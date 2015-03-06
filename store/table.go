// Copyright 2015 stevejiang. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/stevejiang/gotable/api/go/table"
	"github.com/stevejiang/gotable/api/go/table/proto"
	"github.com/stevejiang/gotable/config"
	"github.com/stevejiang/gotable/ctrl"
	"log"
	"os"
	"sync"
)

const (
	zopScoreUp = uint64(0x8000000000000000)
)

// AdminDB keys
const (
	KeyFullSyncEnd = "full-sync-end"
	KeyIncrSyncEnd = "incr-sync-end"
)

type PkgArgs struct {
	Cmd  uint8
	DbId uint8
	Seq  uint64
	Pkg  []byte
}

type Authorize interface {
	// Check whether dbId is authorized
	IsAuth(dbId uint8) bool

	// If dbId is already authorized, SetAuth keeps this infomation
	SetAuth(dbId uint8)
}

type Table struct {
	db    *DB
	tl    *TableLock
	rwMtx sync.RWMutex // stop write to NewIterator

	mtx     sync.Mutex // protects following
	authPwd []string
}

func NewTable(tableDir string) *Table {
	os.MkdirAll(tableDir, os.ModeDir|os.ModePerm)

	tbl := new(Table)
	tbl.tl = NewTableLock()

	tbl.db = NewTableDB()
	err := tbl.db.Open(tableDir, true)
	if err != nil {
		log.Println("Open DB failed: ", err)
		return nil
	}

	return tbl
}

func (tbl *Table) GetRWMutex() *sync.RWMutex {
	return &tbl.rwMtx
}

func (tbl *Table) SetPassword(dbId uint8, password string) {
	tbl.mtx.Lock()
	if tbl.authPwd == nil {
		tbl.authPwd = make([]string, 256)
	}
	tbl.authPwd[dbId] = password
	tbl.mtx.Unlock()
}

func (tbl *Table) Auth(req *PkgArgs, au Authorize) []byte {
	var in proto.PkgOneOp
	_, err := in.Decode(req.Pkg)
	if err != nil {
		in.CtrlFlag &^= 0xFF // Clear all ctrl flags
		in.SetErrCode(table.EcDecodeFail)
		return replyHandle(&in)
	}

	in.ErrCode = 0
	var authDB uint8

	var already bool
	if au.IsAuth(proto.AdminDbId) {
		authDB = proto.AdminDbId
		already = true
	} else if in.DbId != proto.AdminDbId && au.IsAuth(in.DbId) {
		authDB = in.DbId
		already = true
	}
	if already {
		return replyHandle(&in)
	}

	password := string(in.RowKey)

	tbl.mtx.Lock()
	// Admin password
	if tbl.authPwd == nil || tbl.authPwd[proto.AdminDbId] == password {
		authDB = proto.AdminDbId
	} else {
		// Selected DB password
		if len(password) > 0 && tbl.authPwd[in.DbId] == password {
			authDB = in.DbId
		} else {
			in.SetErrCode(table.EcAuthFailed)
		}
	}
	tbl.mtx.Unlock()

	// Success
	if in.ErrCode == 0 {
		in.DbId = authDB
		au.SetAuth(authDB)
	}

	return replyHandle(&in)
}

func (tbl *Table) getKV(srOpt *SnapReadOptions, zop bool, dbId uint8,
	kv *proto.KeyValue) error {
	kv.CtrlFlag &^= 0xFF // Clear all ctrl flags

	var rawColSpace uint8 = proto.ColSpaceDefault
	if zop {
		rawColSpace = proto.ColSpaceScore2
	}
	var rawKey = getRawKey(dbId, kv.TableId, rawColSpace, kv.RowKey, kv.ColKey)

	var err error
	kv.Value, err = tbl.db.Get(srOpt, rawKey)
	if err != nil {
		kv.SetErrCode(table.EcReadFail)
		return err
	} else if kv.Value == nil {
		// Key not exist
		kv.SetErrCode(table.EcNotExist)
	} else {
		// Key exists, set CtrlValue to make sure replied Value is not nil
		kv.CtrlFlag |= proto.CtrlValue
		kv.Value, kv.Score = parseRawValue(kv.Value)
		if kv.Score != 0 {
			kv.CtrlFlag |= proto.CtrlScore
		}
	}

	if kv.Cas > 1 {
		var rawKey = getRawKey(dbId, kv.TableId, rawColSpace, kv.RowKey, kv.ColKey)

		var lck = tbl.tl.GetLock(rawKey)
		lck.Lock()
		kv.SetCas(lck.NewCas(rawKey))
		lck.Unlock()
	}

	return nil
}

func (tbl *Table) setKV(wb *WriteBatch, zop bool, dbId uint8,
	kv *proto.KeyValue, wa *WriteAccess) error {
	kv.CtrlFlag &^= 0xFF // Clear all ctrl flags

	if len(kv.RowKey) == 0 {
		kv.SetErrCode(table.EcInvRowKey)
		return nil
	}
	if len(kv.Value) > proto.MaxValueLen {
		kv.SetErrCode(table.EcInvValue)
		return nil
	}

	var rawColSpace uint8 = proto.ColSpaceDefault
	if zop {
		rawColSpace = proto.ColSpaceScore2
	}

	var rawKey = getRawKey(dbId, kv.TableId, rawColSpace, kv.RowKey, kv.ColKey)
	var lck = tbl.tl.GetLock(rawKey)
	lck.Lock()
	defer lck.Unlock()
	if !wa.replication {
		if kv.Cas != 0 {
			var cas = lck.GetCas(rawKey)
			if cas != kv.Cas {
				kv.SetErrCode(table.EcCasNotMatch)
				return nil
			}
		}
		lck.ClearCas(rawKey)
	}

	var err error
	if zop {
		if wb == nil {
			wb = tbl.db.NewWriteBatch()
			defer wb.Close()
		}

		var oldVal []byte
		var oldScore int64
		oldVal, err = tbl.db.Get(nil, rawKey)
		if err != nil {
			kv.SetErrCode(table.EcReadFail)
			return err
		} else if oldVal == nil {
			// Key not exist
		} else {
			// Key exists, set CtrlValue to make sure replied Value is not nil
			kv.CtrlFlag |= proto.CtrlValue
			oldVal, oldScore = parseRawValue(oldVal)
			var scoreKey = getRawKey(dbId, kv.TableId, proto.ColSpaceScore1,
				kv.RowKey, newScoreColKey(oldScore, kv.ColKey))
			tbl.db.Del(scoreKey, wb)
		}

		tbl.db.Put(rawKey, getRawValue(kv.Value, kv.Score), wb)

		var scoreKey = getRawKey(dbId, kv.TableId, proto.ColSpaceScore1,
			kv.RowKey, newScoreColKey(kv.Score, kv.ColKey))
		tbl.db.Put(scoreKey, getRawValue(kv.Value, 0), wb)

		// Reply old value and score
		kv.Value = oldVal
		kv.SetScore(oldScore)

		err = tbl.db.Commit(wb)
		if err != nil {
			kv.SetErrCode(table.EcWriteFail)
			return err
		}
	} else {
		err = tbl.db.Put(rawKey, getRawValue(kv.Value, kv.Score), nil)
		if err != nil {
			kv.SetErrCode(table.EcWriteFail)
			return err
		}
	}

	return nil
}

func (tbl *Table) setSyncKV(zop bool, dbId uint8, kv *proto.KeyValue) error {
	kv.CtrlFlag &^= 0xFF // Clear all ctrl flags

	var rawColSpace uint8 = proto.ColSpaceDefault
	if zop {
		rawColSpace = proto.ColSpaceScore2
	}

	var rawKey = getRawKey(dbId, kv.TableId, rawColSpace, kv.RowKey, kv.ColKey)

	if zop {
		var wb = tbl.db.NewWriteBatch()
		defer wb.Close()

		tbl.db.Put(rawKey, getRawValue(kv.Value, kv.Score), wb)

		var scoreKey = getRawKey(dbId, kv.TableId, proto.ColSpaceScore1,
			kv.RowKey, newScoreColKey(kv.Score, kv.ColKey))
		tbl.db.Put(scoreKey, getRawValue(kv.Value, 0), wb)

		var err = tbl.db.Commit(wb)
		if err != nil {
			kv.SetErrCode(table.EcWriteFail)
			return err
		}
	} else {
		var err = tbl.db.Put(rawKey, getRawValue(kv.Value, kv.Score), nil)
		if err != nil {
			kv.SetErrCode(table.EcWriteFail)
			return err
		}
	}

	return nil
}

func (tbl *Table) delKV(wb *WriteBatch, zop bool, dbId uint8,
	kv *proto.KeyValue, wa *WriteAccess) error {
	kv.CtrlFlag &^= 0xFF // Clear all ctrl flags

	var rawColSpace uint8 = proto.ColSpaceDefault
	if zop {
		rawColSpace = proto.ColSpaceScore2
	}

	var rawKey = getRawKey(dbId, kv.TableId, rawColSpace, kv.RowKey, kv.ColKey)
	var lck = tbl.tl.GetLock(rawKey)
	lck.Lock()
	defer lck.Unlock()
	if !wa.replication {
		if kv.Cas != 0 {
			var cas = lck.GetCas(rawKey)
			if cas != kv.Cas {
				kv.SetErrCode(table.EcCasNotMatch)
				return nil
			}
		}
		lck.ClearCas(rawKey)
	}

	var err error
	if zop {
		if wb == nil {
			wb = tbl.db.NewWriteBatch()
			defer wb.Close()
		}

		var oldVal []byte
		var oldScore int64
		oldVal, err = tbl.db.Get(nil, rawKey)
		if err != nil {
			kv.SetErrCode(table.EcReadFail)
			return err
		} else if oldVal == nil {
			// Key not exist
		} else if oldVal != nil {
			// Key exists
			kv.CtrlFlag |= proto.CtrlValue
			oldVal, oldScore = parseRawValue(oldVal)

			var scoreKey = getRawKey(dbId, kv.TableId, proto.ColSpaceScore1,
				kv.RowKey, newScoreColKey(oldScore, kv.ColKey))
			tbl.db.Del(scoreKey, wb)
		}

		tbl.db.Del(rawKey, wb)

		// Reply old value and score
		kv.Value = oldVal
		kv.SetScore(oldScore)

		err = tbl.db.Commit(wb)
		if err != nil {
			kv.SetErrCode(table.EcWriteFail)
			return err
		}
	} else {
		err = tbl.db.Del(rawKey, nil)
		if err != nil {
			kv.SetErrCode(table.EcWriteFail)
			return err
		}
	}

	return nil
}

func (tbl *Table) incrKV(wb *WriteBatch, zop bool, dbId uint8,
	kv *proto.KeyValue, wa *WriteAccess) error {
	kv.CtrlFlag &^= 0xFF // Clear all ctrl flags

	if len(kv.RowKey) == 0 {
		kv.SetErrCode(table.EcInvRowKey)
		return nil
	}

	var rawColSpace uint8 = proto.ColSpaceDefault
	if zop {
		rawColSpace = proto.ColSpaceScore2
	}

	var rawKey = getRawKey(dbId, kv.TableId, rawColSpace, kv.RowKey, kv.ColKey)
	var lck = tbl.tl.GetLock(rawKey)
	lck.Lock()
	defer lck.Unlock()
	if !wa.replication {
		if kv.Cas != 0 {
			var cas = lck.GetCas(rawKey)
			if cas != kv.Cas {
				kv.SetErrCode(table.EcCasNotMatch)
				return nil
			}
		}
		lck.ClearCas(rawKey)
	}

	var err error
	var newScore = kv.Score
	var oldVal []byte
	var oldScore int64
	if zop {
		if wb == nil {
			wb = tbl.db.NewWriteBatch()
			defer wb.Close()
		}

		oldVal, err = tbl.db.Get(nil, rawKey)
		if err != nil {
			kv.SetErrCode(table.EcReadFail)
			return err
		} else if oldVal != nil {
			oldVal, oldScore = parseRawValue(oldVal)
			newScore += oldScore

			var scoreKey = getRawKey(dbId, kv.TableId, proto.ColSpaceScore1,
				kv.RowKey, newScoreColKey(oldScore, kv.ColKey))
			tbl.db.Del(scoreKey, wb)
		}

		tbl.db.Put(rawKey, getRawValue(oldVal, newScore), wb)

		var scoreKey = getRawKey(dbId, kv.TableId, proto.ColSpaceScore1,
			kv.RowKey, newScoreColKey(newScore, kv.ColKey))
		tbl.db.Put(scoreKey, getRawValue(oldVal, 0), wb)

		kv.Value = oldVal
		kv.CtrlFlag |= proto.CtrlValue
		kv.SetScore(newScore)

		err = tbl.db.Commit(wb)
		if err != nil {
			kv.SetErrCode(table.EcWriteFail)
			return err
		}
	} else {
		oldVal, err = tbl.db.Get(nil, rawKey)
		if err != nil {
			kv.SetErrCode(table.EcReadFail)
			return err
		} else if oldVal != nil {
			oldVal, oldScore = parseRawValue(oldVal)
			newScore += oldScore
		}

		kv.Value = oldVal
		kv.CtrlFlag |= proto.CtrlValue
		kv.SetScore(newScore)

		err = tbl.db.Put(rawKey, getRawValue(oldVal, newScore), nil)
		if err != nil {
			kv.SetErrCode(table.EcWriteFail)
			return err
		}
	}

	return nil
}

func (tbl *Table) Get(req *PkgArgs, au Authorize) []byte {
	var in proto.PkgOneOp
	_, err := in.Decode(req.Pkg)
	if err != nil {
		in.ErrCode = table.EcDecodeFail
	}
	if in.ErrCode == 0 && in.DbId == proto.AdminDbId {
		in.ErrCode = table.EcInvDbId
	}
	if in.ErrCode == 0 && !au.IsAuth(in.DbId) {
		in.ErrCode = table.EcNoPrivilege
	}

	if in.ErrCode == 0 {
		zop := (in.PkgFlag&proto.FlagZop != 0)
		err = tbl.getKV(nil, zop, in.DbId, &in.KeyValue)
		if err != nil {
			log.Printf("getKV failed: %s\n", err)
		}
	} else {
		in.CtrlFlag &^= 0xFF // Clear all ctrl flags
		in.CtrlFlag |= proto.CtrlErrCode
	}

	return replyHandle(&in)
}

func (tbl *Table) MGet(req *PkgArgs, au Authorize) []byte {
	var in proto.PkgMultiOp
	_, err := in.Decode(req.Pkg)
	if err != nil {
		in.ErrCode = table.EcDecodeFail
	}
	if in.ErrCode == 0 && in.DbId == proto.AdminDbId {
		in.ErrCode = table.EcInvDbId
	}
	if in.ErrCode == 0 && !au.IsAuth(in.DbId) {
		in.ErrCode = table.EcNoPrivilege
	}

	if in.ErrCode == 0 {
		var srOpt = tbl.db.NewSnapReadOptions()
		defer srOpt.Release()
		zop := (in.PkgFlag&proto.FlagZop != 0)
		for i := 0; i < len(in.Kvs); i++ {
			err = tbl.getKV(srOpt, zop, in.DbId, &in.Kvs[i])
			if err != nil {
				log.Printf("getKV failed: %s\n", err)
				break
			}
		}
	} else {
		in.Kvs = nil
	}

	return replyHandle(&in)
}

func (tbl *Table) Sync(req *PkgArgs) ([]byte, bool) {
	var in proto.PkgOneOp
	_, err := in.Decode(req.Pkg)
	if err == nil {
		// Sync full DB, including DB 0
		zop := (in.PkgFlag&proto.FlagZop != 0)
		tbl.rwMtx.RLock()
		err = tbl.setSyncKV(zop, in.DbId, &in.KeyValue)
		tbl.rwMtx.RUnlock()

		if err == nil {
			return nil, true
		} else {
			log.Printf("setRawKV failed: %s\n", err)
		}
	} else {
		in = proto.PkgOneOp{}
		in.Cmd = req.Cmd
		in.DbId = req.DbId
		in.Seq = req.Seq
		in.SetErrCode(table.EcDecodeFail)
	}

	return replyHandle(&in), table.EcOk == in.ErrCode
}

func (tbl *Table) Set(req *PkgArgs, au Authorize, wa *WriteAccess) ([]byte, bool) {
	var in proto.PkgOneOp
	_, err := in.Decode(req.Pkg)
	if err != nil {
		in.ErrCode = table.EcDecodeFail
	}
	if in.ErrCode == 0 && in.DbId == proto.AdminDbId {
		in.ErrCode = table.EcInvDbId
	}
	if in.ErrCode == 0 && !au.IsAuth(in.DbId) {
		in.ErrCode = table.EcNoPrivilege
	}

	if in.ErrCode == 0 {
		zop := (in.PkgFlag&proto.FlagZop != 0)
		tbl.rwMtx.RLock()
		err = tbl.setKV(nil, zop, in.DbId, &in.KeyValue, wa)
		tbl.rwMtx.RUnlock()

		if err != nil {
			log.Printf("setKV failed: %s\n", err)
		}
	} else {
		in.CtrlFlag &^= 0xFF // Clear all ctrl flags
		in.CtrlFlag |= proto.CtrlErrCode
	}

	return replyHandle(&in), table.EcOk == in.ErrCode
}

func (tbl *Table) MSet(req *PkgArgs, au Authorize, wa *WriteAccess) ([]byte, bool) {
	var in proto.PkgMultiOp
	_, err := in.Decode(req.Pkg)
	if err != nil {
		in.ErrCode = table.EcDecodeFail
	}
	if in.ErrCode == 0 && in.DbId == proto.AdminDbId {
		in.ErrCode = table.EcInvDbId
	}
	if in.ErrCode == 0 && !au.IsAuth(in.DbId) {
		in.ErrCode = table.EcNoPrivilege
	}

	if in.ErrCode == 0 {
		var wb = tbl.db.NewWriteBatch()
		defer wb.Close()
		zop := (in.PkgFlag&proto.FlagZop != 0)
		tbl.rwMtx.RLock()
		for i := 0; i < len(in.Kvs); i++ {
			err = tbl.setKV(wb, zop, in.DbId, &in.Kvs[i], wa)
			if err != nil {
				log.Printf("setKV failed: %s\n", err)
				break
			}
		}
		tbl.rwMtx.RUnlock()
	} else {
		in.Kvs = nil
	}

	return replyHandle(&in), table.EcOk == in.ErrCode
}

func (tbl *Table) Del(req *PkgArgs, au Authorize, wa *WriteAccess) ([]byte, bool) {
	var in proto.PkgOneOp
	_, err := in.Decode(req.Pkg)
	if err != nil {
		in.ErrCode = table.EcDecodeFail
	}
	if in.ErrCode == 0 && in.DbId == proto.AdminDbId {
		in.ErrCode = table.EcInvDbId
	}
	if in.ErrCode == 0 && !au.IsAuth(in.DbId) {
		in.ErrCode = table.EcNoPrivilege
	}

	if in.ErrCode == 0 {
		zop := (in.PkgFlag&proto.FlagZop != 0)
		tbl.rwMtx.RLock()
		err = tbl.delKV(nil, zop, in.DbId, &in.KeyValue, wa)
		tbl.rwMtx.RUnlock()

		if err != nil {
			log.Printf("delKV failed: %s\n", err)
		}
	} else {
		in.CtrlFlag &^= 0xFF // Clear all ctrl flags
		in.CtrlFlag |= proto.CtrlErrCode
	}

	return replyHandle(&in), table.EcOk == in.ErrCode
}

func (tbl *Table) MDel(req *PkgArgs, au Authorize, wa *WriteAccess) ([]byte, bool) {
	var in proto.PkgMultiOp
	_, err := in.Decode(req.Pkg)
	if err != nil {
		in.ErrCode = table.EcDecodeFail
	}
	if in.ErrCode == 0 && in.DbId == proto.AdminDbId {
		in.ErrCode = table.EcInvDbId
	}
	if in.ErrCode == 0 && !au.IsAuth(in.DbId) {
		in.ErrCode = table.EcNoPrivilege
	}

	if in.ErrCode == 0 {
		var wb = tbl.db.NewWriteBatch()
		defer wb.Close()
		zop := (in.PkgFlag&proto.FlagZop != 0)
		tbl.rwMtx.RLock()
		for i := 0; i < len(in.Kvs); i++ {
			err = tbl.delKV(wb, zop, in.DbId, &in.Kvs[i], wa)
			if err != nil {
				log.Printf("delKV failed: %s\n", err)
				break
			}
		}
		tbl.rwMtx.RUnlock()
	} else {
		in.Kvs = nil
	}

	return replyHandle(&in), table.EcOk == in.ErrCode
}

func (tbl *Table) Incr(req *PkgArgs, au Authorize, wa *WriteAccess) ([]byte, bool) {
	var in proto.PkgOneOp
	_, err := in.Decode(req.Pkg)
	if err != nil {
		in.ErrCode = table.EcDecodeFail
	}
	if in.ErrCode == 0 && in.DbId == proto.AdminDbId {
		in.ErrCode = table.EcInvDbId
	}
	if in.ErrCode == 0 && !au.IsAuth(in.DbId) {
		in.ErrCode = table.EcNoPrivilege
	}

	if in.ErrCode == 0 {
		zop := (in.PkgFlag&proto.FlagZop != 0)
		tbl.rwMtx.RLock()
		err = tbl.incrKV(nil, zop, in.DbId, &in.KeyValue, wa)
		tbl.rwMtx.RUnlock()

		if err != nil {
			log.Printf("incrKV failed: %s\n", err)
		}
	} else {
		in.CtrlFlag &^= 0xFF // Clear all ctrl flags
		in.CtrlFlag |= proto.CtrlErrCode
	}

	return replyHandle(&in), table.EcOk == in.ErrCode
}

func (tbl *Table) MIncr(req *PkgArgs, au Authorize, wa *WriteAccess) ([]byte, bool) {
	var in proto.PkgMultiOp
	_, err := in.Decode(req.Pkg)
	if err != nil {
		in.ErrCode = table.EcDecodeFail
	}
	if in.ErrCode == 0 && in.DbId == proto.AdminDbId {
		in.ErrCode = table.EcInvDbId
	}
	if in.ErrCode == 0 && !au.IsAuth(in.DbId) {
		in.ErrCode = table.EcNoPrivilege
	}

	if in.ErrCode == 0 {
		var wb = tbl.db.NewWriteBatch()
		defer wb.Close()
		zop := (in.PkgFlag&proto.FlagZop != 0)
		tbl.rwMtx.RLock()
		for i := 0; i < len(in.Kvs); i++ {
			err = tbl.incrKV(wb, zop, in.DbId, &in.Kvs[i], wa)
			if err != nil {
				log.Printf("incrKV failed: %s\n", err)
				break
			}
		}
		tbl.rwMtx.RUnlock()
	} else {
		in.Kvs = nil
	}

	return replyHandle(&in), table.EcOk == in.ErrCode
}

func iterMove(it *Iterator, asc bool) {
	if asc {
		it.Next()
	} else {
		it.Prev()
	}
}

func (tbl *Table) zScanSortScore(in *proto.PkgScanReq, out *proto.PkgScanResp) {
	var it = tbl.db.NewIterator(true)
	defer it.Close()

	var scanAsc = (in.PkgFlag&proto.FlagAscending != 0)
	var startSeek = (in.PkgFlag&proto.FlagStart != 0)
	var scanColSpace uint8 = proto.ColSpaceScore1
	if scanAsc {
		if startSeek {
			// Seek to the first element
			it.Seek(getRawKey(in.DbId, in.TableId, scanColSpace,
				in.RowKey, nil))
		} else {
			it.Seek(getRawKey(in.DbId, in.TableId, scanColSpace,
				in.RowKey, newScoreColKey(in.Score, in.ColKey)))
		}
	} else {
		if startSeek {
			// Seek to the last element
			it.Seek(getRawKey(in.DbId, in.TableId, scanColSpace+1,
				in.RowKey, nil))
		} else {
			it.Seek(getRawKey(in.DbId, in.TableId, scanColSpace,
				in.RowKey, newScoreColKey(in.Score, in.ColKey)))
		}
		if !it.Valid() {
			it.SeekToLast()
		}
	}

	out.PkgFlag |= proto.FlagEnd
	var first = true
	var scanNum = int(in.Num)
	for i := 0; it.Valid() && i < scanNum+1; iterMove(it, scanAsc) {
		_, dbId, tableId, colSpace, rowKey, colKey := parseRawKey(it.Key())
		if dbId != in.DbId || tableId != in.TableId ||
			colSpace != scanColSpace || bytes.Compare(rowKey, in.RowKey) != 0 {
			if first {
				first = false
				continue
			} else {
				break
			}
		}

		if first {
			first = false
		}

		if len(colKey) < 8 {
			continue //  skip invalid record
		}
		zColKey, zScore := parseZColKey(colKey)
		if !startSeek {
			if scanAsc {
				if zScore < in.Score {
					continue
				}
				if zScore == in.Score && bytes.Compare(zColKey, in.ColKey) <= 0 {
					continue
				}
			} else {
				if zScore > in.Score {
					continue
				}
				if zScore == in.Score && bytes.Compare(zColKey, in.ColKey) >= 0 {
					continue
				}
			}
		}

		if i < scanNum {
			var kv proto.KeyValue
			kv.TableId = in.TableId
			kv.RowKey = in.RowKey
			kv.ColKey = zColKey
			kv.Value, _ = parseRawValue(it.Value())
			kv.Score = zScore
			if kv.Value != nil {
				kv.CtrlFlag |= proto.CtrlValue
			}
			if kv.Score != 0 {
				kv.CtrlFlag |= proto.CtrlScore
			}
			out.Kvs = append(out.Kvs, kv)
		} else {
			out.PkgFlag &^= proto.FlagEnd
			break
		}
		i++
	}
}

func (tbl *Table) Scan(req *PkgArgs, au Authorize) []byte {
	var out proto.PkgScanResp
	out.Cmd = req.Cmd
	out.DbId = req.DbId
	out.Seq = req.Seq

	var in proto.PkgScanReq
	_, err := in.Decode(req.Pkg)
	if err != nil {
		return errorHandle(&out, table.EcDecodeFail)
	}

	out.PkgFlag = in.PkgFlag

	if in.DbId == proto.AdminDbId {
		return errorHandle(&out, table.EcInvDbId)
	}

	if !au.IsAuth(in.DbId) {
		return errorHandle(&out, table.EcNoPrivilege)
	}

	if in.ColSpace == proto.ColSpaceScore1 {
		tbl.zScanSortScore(&in, &out)
		return replyHandle(&out)
	}

	var scanColSpace uint8 = proto.ColSpaceDefault
	if in.ColSpace == proto.ColSpaceScore2 {
		scanColSpace = proto.ColSpaceScore2
	}
	var it = tbl.db.NewIterator(true)
	defer it.Close()

	var scanAsc = (in.PkgFlag&proto.FlagAscending != 0)
	var startSeek = (in.PkgFlag&proto.FlagStart != 0)
	if scanAsc {
		if startSeek {
			// Seek to the first element
			it.Seek(getRawKey(in.DbId, in.TableId, scanColSpace, in.RowKey, nil))
		} else {
			it.Seek(getRawKey(in.DbId, in.TableId, scanColSpace,
				in.RowKey, in.ColKey))
		}
	} else {
		if startSeek {
			// Seek to the last element
			it.Seek(getRawKey(in.DbId, in.TableId, scanColSpace+1, in.RowKey, nil))
		} else {
			it.Seek(getRawKey(in.DbId, in.TableId, scanColSpace,
				in.RowKey, in.ColKey))
		}
		if !it.Valid() {
			it.SeekToLast()
		}
	}

	out.PkgFlag |= proto.FlagEnd
	var first = true
	var scanNum = int(in.Num)
	for i := 0; it.Valid() && i < scanNum+1; iterMove(it, scanAsc) {
		_, dbId, tableId, colSpace, rowKey, colKey := parseRawKey(it.Key())
		if dbId != in.DbId || tableId != in.TableId ||
			colSpace != scanColSpace || bytes.Compare(rowKey, in.RowKey) != 0 {
			if first {
				first = false
				continue
			} else {
				break
			}
		}

		if first {
			first = false
		}

		if !startSeek {
			if scanAsc {
				if bytes.Compare(colKey, in.ColKey) <= 0 {
					continue
				}
			} else {
				if bytes.Compare(colKey, in.ColKey) >= 0 {
					continue
				}
			}
		}

		if i < scanNum {
			var kv proto.KeyValue
			kv.TableId = in.TableId
			kv.RowKey = in.RowKey
			kv.ColKey = colKey
			kv.Value, kv.Score = parseRawValue(it.Value())
			if kv.Value != nil {
				kv.CtrlFlag |= proto.CtrlValue
			}
			if kv.Score != 0 {
				kv.CtrlFlag |= proto.CtrlScore
			}

			out.Kvs = append(out.Kvs, kv)
		} else {
			out.PkgFlag &^= proto.FlagEnd
			break
		}
		i++
	}

	return replyHandle(&out)
}

func (tbl *Table) Dump(req *PkgArgs, au Authorize) []byte {
	var out proto.PkgDumpResp
	out.Cmd = req.Cmd
	out.DbId = req.DbId
	out.Seq = req.Seq

	var in proto.PkgDumpReq
	_, err := in.Decode(req.Pkg)
	if err != nil {
		return errorHandle(&out, table.EcDecodeFail)
	}

	out.StartUnitId = in.StartUnitId
	out.EndUnitId = in.EndUnitId
	out.LastUnitId = in.StartUnitId
	out.PkgFlag = in.PkgFlag
	out.PkgFlag &^= (proto.FlagUnitStart | proto.FlagEnd)

	if in.DbId == proto.AdminDbId {
		return errorHandle(&out, table.EcInvDbId)
	}

	if !au.IsAuth(in.DbId) {
		return errorHandle(&out, table.EcNoPrivilege)
	}

	var onlyOneTable = (out.PkgFlag&proto.FlagOneTable != 0)
	var it = tbl.db.NewIterator(false)
	defer it.Close()

	if len(in.RowKey) == 0 {
		it.Seek(getRawUnitKey(in.StartUnitId, in.DbId, in.TableId))
	} else {
		var rawColKey = in.ColKey
		if in.ColSpace == proto.ColSpaceScore1 {
			rawColKey = newScoreColKey(in.Score, in.ColKey)
		}

		it.Seek(getRawKey(in.DbId, in.TableId, in.ColSpace, in.RowKey, rawColKey))
		if it.Valid() {
			_, dbId, tableId, colSpace, rowKey, colKey := parseRawKey(it.Key())
			if dbId == in.DbId && tableId == in.TableId &&
				bytes.Compare(rowKey, in.RowKey) == 0 &&
				colSpace == in.ColSpace &&
				bytes.Compare(colKey, rawColKey) == 0 {
				it.Next()
			}
		}
	}

	const maxScanNum = 20
	const maxTryUnitNum = 10
	var triedUnitNum = 0
	for it.Valid() && len(out.Kvs) < maxScanNum {
		unitId, dbId, tableId, colSpace, rowKey, colKey := parseRawKey(it.Key())
		if unitId < in.StartUnitId || unitId > in.EndUnitId {
			out.PkgFlag |= proto.FlagEnd
			break
		}

		var misMatch bool
		var nextUnitTableId uint8
		// Dump only the selected TableId?
		if onlyOneTable {
			if dbId != in.DbId || tableId != in.TableId {
				misMatch = true
				nextUnitTableId = in.TableId
			}
		} else {
			if dbId != in.DbId {
				misMatch = true
			}
		}
		if misMatch {
			if triedUnitNum >= maxTryUnitNum {
				break
			}
			var nextUnitId = out.LastUnitId + 1
			if nextUnitId < unitId && unitId <= in.EndUnitId {
				nextUnitId = unitId
			}
			if nextUnitId <= in.EndUnitId {
				triedUnitNum++
				out.LastUnitId = nextUnitId
				out.PkgFlag |= proto.FlagUnitStart
				SeekToUnit(it, nextUnitId, in.DbId, nextUnitTableId)
				continue
			} else {
				out.PkgFlag |= proto.FlagEnd
				break
			}
		}

		if colSpace == proto.ColSpaceScore2 {
			it.Seek(getRawKey(dbId, tableId, colSpace+1, rowKey, nil))
			continue // No need to dup dump
		}

		var kv proto.KeyValue
		if colSpace != proto.ColSpaceScore1 {
			kv.ColKey = colKey
			kv.Value, kv.Score = parseRawValue(it.Value())
		} else {
			if len(colKey) < 8 {
				it.Next()
				continue // Skip invalid record
			}
			kv.Score = int64(binary.BigEndian.Uint64(colKey) - zopScoreUp)
			kv.ColKey = colKey[8:]
			kv.Value, _ = parseRawValue(it.Value())
		}

		kv.TableId = tableId
		kv.RowKey = rowKey
		kv.SetColSpace(colSpace)
		if kv.Value != nil {
			kv.CtrlFlag |= proto.CtrlValue
		}
		if kv.Score != 0 {
			kv.CtrlFlag |= proto.CtrlScore
		}

		out.Kvs = append(out.Kvs, kv)
		out.LastUnitId = unitId
		out.PkgFlag &^= proto.FlagUnitStart
		it.Next()
	}

	if !it.Valid() {
		out.PkgFlag |= proto.FlagEnd
	}

	var pkg = make([]byte, out.Length())
	_, err = out.Encode(pkg)
	if err != nil {
		log.Fatalf("Encode failed: %s\n", err)
	}
	return pkg
}

func (tbl *Table) DeleteUnit(unitId uint16) error {
	const maxDelNum = 1000000
	var count, endTimes int
	for {
		var it = tbl.db.NewIterator(false)
		it.Seek(getRawUnitKey(unitId, 0, 0))
		for count = 0; it.Valid() && count < maxDelNum; it.Next() {
			curUnitId := parseRawKeyUnitId(it.Key())
			if curUnitId != unitId {
				break
			}
			err := tbl.db.Del(it.Key(), nil)
			if err != nil {
				it.Close()
				return err
			}
			count++
		}
		it.Close()

		if count < maxDelNum {
			endTimes++
		}
		if endTimes > 1 {
			if count > 0 {
				return errors.New("the deleting unit still has new data written")
			}
			return nil
		}
	}

	return nil
}

func (tbl *Table) NewIterator(fillCache bool) *Iterator {
	return tbl.db.NewIterator(fillCache)
}

func SeekAndCopySyncPkg(it *Iterator, p *proto.PkgOneOp) (uint16, bool) {
	p.PkgFlag &^= 0xFF
	p.CtrlFlag &^= 0xFF

	unitId, dbId, tableId, colSpace, rowKey, colKey := parseRawKey(it.Key())

	switch colSpace {
	case proto.ColSpaceDefault:
		value, score := parseRawValue(it.Value())
		p.SetValue(value)
		p.SetScore(score)
	case proto.ColSpaceScore1:
		it.Seek(getRawKey(dbId, tableId, colSpace+1, rowKey, nil))
		if !it.Valid() {
			return unitId, false
		}
		return SeekAndCopySyncPkg(it, p)
	case proto.ColSpaceScore2:
		value, score := parseRawValue(it.Value())
		p.SetValue(value)
		p.SetScore(score)
		p.PkgFlag |= proto.FlagZop
	}

	p.DbId = dbId
	p.TableId = tableId
	p.SetColSpace(colSpace)
	p.RowKey = rowKey
	p.ColKey = colKey

	return unitId, true
}

func SeekToUnit(it *Iterator, unitId uint16, dbId, tableId uint8) {
	it.Seek(getRawUnitKey(unitId, dbId, tableId))
}

func getRawUnitKey(unitId uint16, dbId, tableId uint8) []byte {
	// wUnitId+cDbId+cTableId
	var rawKey = make([]byte, 4)
	binary.BigEndian.PutUint16(rawKey, unitId)
	rawKey[2] = dbId
	rawKey[3] = tableId

	return rawKey
}

func getRawKey(dbId, tableId, colSpace uint8, rowKey, colKey []byte) []byte {
	var unitId = ctrl.GetUnitId(dbId, tableId, rowKey)

	// wUnitId+cDbId+cTableId+cKeyLen+sRowKey+colSpace+sColKey
	var rowKeyLen = len(rowKey)
	var rawLen = 6 + rowKeyLen + len(colKey)
	var rawKey = make([]byte, rawLen)
	binary.BigEndian.PutUint16(rawKey, unitId)
	rawKey[2] = dbId
	rawKey[3] = tableId
	rawKey[4] = uint8(rowKeyLen)
	copy(rawKey[5:], rowKey)
	rawKey[5+rowKeyLen] = colSpace
	copy(rawKey[6+rowKeyLen:], colKey)

	return rawKey
}

func parseRawKey(rawKey []byte) (unitId uint16, dbId, tableId, colSpace uint8,
	rowKey, colKey []byte) {
	unitId = binary.BigEndian.Uint16(rawKey)
	dbId = rawKey[2]
	tableId = rawKey[3]
	var keyLen = rawKey[4]
	var colTypePos = 5 + int(keyLen)
	rowKey = rawKey[5:colTypePos]
	colSpace = rawKey[colTypePos]
	colKey = rawKey[(colTypePos + 1):]
	return
}

func parseRawKeyUnitId(rawKey []byte) uint16 {
	return binary.BigEndian.Uint16(rawKey)
}

func parseZColKey(colKey []byte) ([]byte, int64) {
	if len(colKey) < 8 {
		return nil, 0
	}

	var score = int64(binary.BigEndian.Uint64(colKey) - zopScoreUp)
	return colKey[8:], score
}

func parseRawValue(value []byte) ([]byte, int64) {
	if len(value) == 0 {
		return nil, 0
	}

	var scoreLen = int(value[0] & 0xF)

	if len(value) >= scoreLen+1 {
		var score int64
		switch scoreLen {
		case 1:
			score = int64(int8(value[1]))
		case 2:
			score = int64(int16(binary.BigEndian.Uint16(value[1:])))
		case 4:
			score = int64(int32(binary.BigEndian.Uint32(value[1:])))
		case 8:
			score = int64(binary.BigEndian.Uint64(value[1:]))
		}
		return value[scoreLen+1:], score
	} else {
		return nil, 0
	}
}

func getRawValue(value []byte, score int64) []byte {
	if score == 0 {
		var r = make([]byte, len(value)+1)
		r[0] = 0
		copy(r[1:], value)
		return r
	}

	if score >= -0x80 && score < 0x80 {
		var r = make([]byte, len(value)+2)
		r[0] = 1
		r[1] = uint8(score)
		copy(r[2:], value)
		return r
	}

	if score >= -0x8000 && score < 0x8000 {
		var r = make([]byte, len(value)+3)
		r[0] = 2
		binary.BigEndian.PutUint16(r[1:], uint16(score))
		copy(r[3:], value)
		return r
	}

	if score >= -0x80000000 && score < 0x80000000 {
		var r = make([]byte, len(value)+5)
		r[0] = 4
		binary.BigEndian.PutUint32(r[1:], uint32(score))
		copy(r[5:], value)
		return r
	}

	var r = make([]byte, len(value)+9)
	r[0] = 8
	binary.BigEndian.PutUint64(r[1:], uint64(score))
	copy(r[9:], value)
	return r
}

func newScoreColKey(score int64, colKey []byte) []byte {
	var col = make([]byte, 8+len(colKey))
	binary.BigEndian.PutUint64(col, uint64(score)+zopScoreUp)
	copy(col[8:], colKey)
	return col
}

func errorHandle(out proto.PkgResponse, errCode int8) []byte {
	out.SetErrCode(errCode)
	var pkg = make([]byte, out.Length())
	_, err := out.Encode(pkg)
	if err != nil {
		log.Fatalf("Encode failed: %s\n", err)
	}
	return pkg
}

func replyHandle(out proto.PkgResponse) []byte {
	var pkg = make([]byte, out.Length())
	_, err := out.Encode(pkg)
	if err != nil {
		log.Fatalf("Encode failed: %s\n", err)
	}
	return pkg
}

type WriteAccess struct {
	replication bool // Replication slaver
	hasMaster   bool
	migration   bool
	unitId      uint16
}

func NewWriteAccess(replication bool, mc *config.MasterConfig) *WriteAccess {
	hasMaster, migration, unitId := mc.GetMasterUnit()
	return &WriteAccess{replication, hasMaster, migration, unitId}
}

// Do we have right to write this key?
func (m *WriteAccess) CheckKey(dbId, tableId uint8, rowKey []byte) bool {
	if m.replication {
		return true // Accept all replication data
	}

	if !m.hasMaster {
		return true
	}

	if m.migration {
		return m.unitId != ctrl.GetUnitId(dbId, tableId, rowKey)
	} else {
		return false
	}
}

// Do we have right to write this unit?
func (m *WriteAccess) CheckUnit(unitId uint16) bool {
	if m.replication {
		return true // Accept all replication data
	}

	if !m.hasMaster {
		return true
	}

	if m.migration {
		return m.unitId != unitId
	} else {
		return false
	}
}

// return false: no right to write!
// return true: may have right to write, need to check key or unit again
func (m *WriteAccess) Check() bool {
	if m.replication {
		return true // Accept all replication data
	}

	if m.hasMaster && !m.migration {
		return false
	}

	return true
}
