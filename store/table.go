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
	"github.com/stevejiang/gotable/ctrl"
	"log"
	"os"
	"sync"
)

const (
	zopScoreUp = uint64(0x8000000000000000)
)

// AdminDB keys, reserved tableId=0(no migration on this table)
const (
	KeyFullSyncEnd    = "full-sync-end"
	KeyIncrSyncEnd    = "incr-sync-end"
	KeySyncLogMissing = "sync-log-missing"
)

const (
	kNoCompression     = 0x0
	kSnappyCompression = 0x1
	kZlibCompression   = 0x2
	kBZip2Compression  = 0x3
	kLZ4Compression    = 0x4
	kLZ4HCCompression  = 0x5
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

func NewTable(tableDir string, maxOpenFiles int,
	writeBufSize int, cacheSize int64, compression string) *Table {
	os.MkdirAll(tableDir, os.ModeDir|os.ModePerm)

	var comp int = kNoCompression
	switch compression {
	case "no":
		comp = kNoCompression
	case "snappy":
		comp = kSnappyCompression
	case "zlib":
		comp = kZlibCompression
	case "bzip2":
		comp = kBZip2Compression
	case "lz4":
		comp = kLZ4Compression
	case "lz4hc":
		comp = kLZ4HCCompression
	default:
		compression = "no"
	}

	tbl := new(Table)
	tbl.tl = NewTableLock()

	tbl.db = NewDB()
	err := tbl.db.Open(tableDir, true, maxOpenFiles, writeBufSize, cacheSize, comp)
	if err != nil {
		log.Println("Open DB failed: ", err)
		return nil
	}

	log.Printf("Open DB with maxOpenFiles %d, writeBufSize %dMB, cacheSize %dMB, "+
		"compression(%s, %d)\n",
		maxOpenFiles, writeBufSize/1048576, cacheSize/1048576, compression, comp)

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

func (tbl *Table) getKV(rOpt *ReadOptions, zop bool, dbId uint8,
	kv *proto.KeyValue, wa *WriteAccess) error {
	kv.CtrlFlag &^= 0xFF // Clear all ctrl flags

	if kv.Cas > 0 && !wa.CheckKey(dbId, kv.TableId, kv.RowKey) {
		kv.SetErrCode(table.EcSlaverCas)
		return nil
	}

	var rawColSpace uint8 = proto.ColSpaceDefault
	if zop {
		rawColSpace = proto.ColSpaceScore2
	}
	var rawKey = getRawKey(dbId, kv.TableId, rawColSpace, kv.RowKey, kv.ColKey)

	var err error
	kv.Value, err = tbl.db.Get(rOpt, rawKey)
	if err != nil {
		kv.SetErrCode(table.EcReadFail)
		return err
	} else if kv.Value == nil {
		// Key not exist
		kv.SetErrCode(table.EcNotExist)
	} else {
		// Key exists
		kv.Value, kv.Score = parseRawValue(kv.Value)
		if len(kv.Value) > 0 {
			kv.CtrlFlag |= proto.CtrlValue
		}
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
	if !wa.CheckKey(dbId, kv.TableId, kv.RowKey) {
		kv.SetErrCode(table.EcWriteSlaver)
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
	if !wa.replication && kv.Cas != 0 {
		var cas = lck.GetCas(rawKey)
		if cas != kv.Cas {
			kv.SetErrCode(table.EcCasNotMatch)
			return nil
		}
	}
	lck.ClearCas(rawKey)

	var err error
	if zop {
		if wb == nil {
			wb = tbl.db.NewWriteBatch()
			defer wb.Destroy()
		}

		var oldVal []byte
		var oldScore int64
		oldVal, err = tbl.db.Get(nil, rawKey)
		if err != nil {
			kv.SetErrCode(table.EcReadFail)
			return err
		} else if oldVal != nil {
			// Key exists
			oldVal, oldScore = parseRawValue(oldVal)
			var scoreKey = getRawKey(dbId, kv.TableId, proto.ColSpaceScore1,
				kv.RowKey, newScoreColKey(oldScore, kv.ColKey))
			tbl.db.Del(scoreKey, wb)
		}

		tbl.db.Put(rawKey, getRawValue(kv.Value, kv.Score), wb)

		var scoreKey = getRawKey(dbId, kv.TableId, proto.ColSpaceScore1,
			kv.RowKey, newScoreColKey(kv.Score, kv.ColKey))
		tbl.db.Put(scoreKey, getRawValue(kv.Value, 0), wb)

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

	kv.SetValue(nil)
	kv.SetScore(0)

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
		defer wb.Destroy()

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

	if len(kv.RowKey) == 0 {
		kv.SetErrCode(table.EcInvRowKey)
		return nil
	}
	if !wa.CheckKey(dbId, kv.TableId, kv.RowKey) {
		kv.SetErrCode(table.EcWriteSlaver)
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
	if !wa.replication && kv.Cas != 0 {
		var cas = lck.GetCas(rawKey)
		if cas != kv.Cas {
			kv.SetErrCode(table.EcCasNotMatch)
			return nil
		}
	}
	lck.ClearCas(rawKey)

	var err error
	if zop {
		if wb == nil {
			wb = tbl.db.NewWriteBatch()
			defer wb.Destroy()
		}

		var oldVal []byte
		var oldScore int64
		oldVal, err = tbl.db.Get(nil, rawKey)
		if err != nil {
			kv.SetErrCode(table.EcReadFail)
			return err
		} else if oldVal != nil {
			// Key exists
			oldVal, oldScore = parseRawValue(oldVal)
			var scoreKey = getRawKey(dbId, kv.TableId, proto.ColSpaceScore1,
				kv.RowKey, newScoreColKey(oldScore, kv.ColKey))
			tbl.db.Del(scoreKey, wb)
		}

		tbl.db.Del(rawKey, wb)

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

	kv.SetValue(nil)
	kv.SetScore(0)

	return nil
}

func (tbl *Table) incrKV(wb *WriteBatch, zop bool, dbId uint8,
	kv *proto.KeyValue, wa *WriteAccess) error {
	kv.CtrlFlag &^= 0xFF // Clear all ctrl flags

	if len(kv.RowKey) == 0 {
		kv.SetErrCode(table.EcInvRowKey)
		return nil
	}
	if !wa.CheckKey(dbId, kv.TableId, kv.RowKey) {
		kv.SetErrCode(table.EcWriteSlaver)
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
	if !wa.replication && kv.Cas != 0 {
		var cas = lck.GetCas(rawKey)
		if cas != kv.Cas {
			kv.SetErrCode(table.EcCasNotMatch)
			return nil
		}
	}
	lck.ClearCas(rawKey)

	var err error
	var newScore = kv.Score
	var oldVal []byte
	var oldScore int64
	if zop {
		if wb == nil {
			wb = tbl.db.NewWriteBatch()
			defer wb.Destroy()
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

		kv.SetValue(oldVal)
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

		kv.SetValue(oldVal)
		kv.SetScore(newScore)

		err = tbl.db.Put(rawKey, getRawValue(oldVal, newScore), nil)
		if err != nil {
			kv.SetErrCode(table.EcWriteFail)
			return err
		}
	}

	return nil
}

func (tbl *Table) Get(req *PkgArgs, au Authorize, wa *WriteAccess) []byte {
	var in proto.PkgOneOp
	if checkOneOp(&in, req, au) {
		zop := (in.PkgFlag&proto.FlagZop != 0)
		err := tbl.getKV(nil, zop, in.DbId, &in.KeyValue, wa)
		if err != nil {
			log.Printf("getKV failed: %s\n", err)
		}
	}

	return replyHandle(&in)
}

func (tbl *Table) MGet(req *PkgArgs, au Authorize, wa *WriteAccess) []byte {
	var in proto.PkgMultiOp
	if checkMultiOp(&in, req, au) {
		var rOpt = tbl.db.NewReadOptions(true)
		defer rOpt.Destroy()
		zop := (in.PkgFlag&proto.FlagZop != 0)
		for i := 0; i < len(in.Kvs); i++ {
			err := tbl.getKV(rOpt, zop, in.DbId, &in.Kvs[i], wa)
			if err != nil {
				log.Printf("getKV failed: %s\n", err)
				break
			}
		}
	}

	return replyMulti(&in)
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
		in.CtrlFlag &^= 0xFF // Clear all ctrl flags
		in.SetErrCode(table.EcDecodeFail)
	}

	return replyHandle(&in), table.EcOk == in.ErrCode
}

func (tbl *Table) Set(req *PkgArgs, au Authorize, wa *WriteAccess) ([]byte, bool) {
	var in proto.PkgOneOp
	if checkOneOp(&in, req, au) {
		zop := (in.PkgFlag&proto.FlagZop != 0)
		tbl.rwMtx.RLock()
		err := tbl.setKV(nil, zop, in.DbId, &in.KeyValue, wa)
		tbl.rwMtx.RUnlock()

		if err != nil {
			log.Printf("setKV failed: %s\n", err)
		}
	}

	return replyHandle(&in), table.EcOk == in.ErrCode
}

func (tbl *Table) MSet(req *PkgArgs, au Authorize, wa *WriteAccess) ([]byte, bool) {
	var in proto.PkgMultiOp
	if checkMultiOp(&in, req, au) {
		var wb = tbl.db.NewWriteBatch()
		defer wb.Destroy()
		zop := (in.PkgFlag&proto.FlagZop != 0)
		tbl.rwMtx.RLock()
		for i := 0; i < len(in.Kvs); i++ {
			err := tbl.setKV(wb, zop, in.DbId, &in.Kvs[i], wa)
			if err != nil {
				log.Printf("setKV failed: %s\n", err)
				break
			}
		}
		tbl.rwMtx.RUnlock()
	}

	return replyMulti(&in), table.EcOk == in.ErrCode
}

func (tbl *Table) Del(req *PkgArgs, au Authorize, wa *WriteAccess) ([]byte, bool) {
	var in proto.PkgOneOp
	if checkOneOp(&in, req, au) {
		zop := (in.PkgFlag&proto.FlagZop != 0)
		tbl.rwMtx.RLock()
		err := tbl.delKV(nil, zop, in.DbId, &in.KeyValue, wa)
		tbl.rwMtx.RUnlock()

		if err != nil {
			log.Printf("delKV failed: %s\n", err)
		}
	}

	return replyHandle(&in), table.EcOk == in.ErrCode
}

func (tbl *Table) MDel(req *PkgArgs, au Authorize, wa *WriteAccess) ([]byte, bool) {
	var in proto.PkgMultiOp
	if checkMultiOp(&in, req, au) {
		var wb = tbl.db.NewWriteBatch()
		defer wb.Destroy()
		zop := (in.PkgFlag&proto.FlagZop != 0)
		tbl.rwMtx.RLock()
		for i := 0; i < len(in.Kvs); i++ {
			err := tbl.delKV(wb, zop, in.DbId, &in.Kvs[i], wa)
			if err != nil {
				log.Printf("delKV failed: %s\n", err)
				break
			}
		}
		tbl.rwMtx.RUnlock()
	}

	return replyMulti(&in), table.EcOk == in.ErrCode
}

func (tbl *Table) Incr(req *PkgArgs, au Authorize, wa *WriteAccess) ([]byte, bool) {
	var in proto.PkgOneOp
	if checkOneOp(&in, req, au) {
		zop := (in.PkgFlag&proto.FlagZop != 0)
		tbl.rwMtx.RLock()
		err := tbl.incrKV(nil, zop, in.DbId, &in.KeyValue, wa)
		tbl.rwMtx.RUnlock()

		if err != nil {
			log.Printf("incrKV failed: %s\n", err)
		}
	}

	return replyHandle(&in), table.EcOk == in.ErrCode
}

func (tbl *Table) MIncr(req *PkgArgs, au Authorize, wa *WriteAccess) ([]byte, bool) {
	var in proto.PkgMultiOp
	if checkMultiOp(&in, req, au) {
		var wb = tbl.db.NewWriteBatch()
		defer wb.Destroy()
		zop := (in.PkgFlag&proto.FlagZop != 0)
		tbl.rwMtx.RLock()
		for i := 0; i < len(in.Kvs); i++ {
			err := tbl.incrKV(wb, zop, in.DbId, &in.Kvs[i], wa)
			if err != nil {
				log.Printf("incrKV failed: %s\n", err)
				break
			}
		}
		tbl.rwMtx.RUnlock()
	}

	return replyMulti(&in), table.EcOk == in.ErrCode
}

func iterMove(it *Iterator, asc bool) {
	if asc {
		it.Next()
	} else {
		it.Prev()
	}
}

func (tbl *Table) zScanSortScore(in *proto.PkgScanReq, out *proto.PkgScanResp) {
	var it = tbl.db.NewIterator(nil)
	defer it.Destroy()

	var scanAsc = (in.PkgFlag&proto.FlagScanAsc != 0)
	var startSeek = (in.PkgFlag&proto.FlagScanKeyStart != 0)
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

	out.PkgFlag |= proto.FlagScanEnd
	var first = true
	var scanNum = int(in.Num)
	var pkgLen = proto.HeadSize + 1000
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
			if len(kv.Value) > 0 {
				kv.CtrlFlag |= proto.CtrlValue
			}
			if kv.Score != 0 {
				kv.CtrlFlag |= proto.CtrlScore
			}
			out.Kvs = append(out.Kvs, kv)

			pkgLen += kv.Length()
			if pkgLen > proto.MaxPkgLen/2 {
				out.PkgFlag &^= proto.FlagScanEnd
				break
			}
		} else {
			out.PkgFlag &^= proto.FlagScanEnd
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
	n, err := in.Decode(req.Pkg)
	if err != nil || n != len(req.Pkg) {
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
	var it = tbl.db.NewIterator(nil)
	defer it.Destroy()

	var scanAsc = (in.PkgFlag&proto.FlagScanAsc != 0)
	var startSeek = (in.PkgFlag&proto.FlagScanKeyStart != 0)
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

	out.PkgFlag |= proto.FlagScanEnd
	var first = true
	var scanNum = int(in.Num)
	var pkgLen = proto.HeadSize + 1000
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
			if len(kv.Value) > 0 {
				kv.CtrlFlag |= proto.CtrlValue
			}
			if kv.Score != 0 {
				kv.CtrlFlag |= proto.CtrlScore
			}

			out.Kvs = append(out.Kvs, kv)

			pkgLen += kv.Length()
			if pkgLen > proto.MaxPkgLen/2 {
				out.PkgFlag &^= proto.FlagScanEnd
				break
			}
		} else {
			out.PkgFlag &^= proto.FlagScanEnd
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
	n, err := in.Decode(req.Pkg)
	if err != nil || n != len(req.Pkg) {
		return errorHandle(&out, table.EcDecodeFail)
	}

	out.StartUnitId = in.StartUnitId
	out.EndUnitId = in.EndUnitId
	out.LastUnitId = in.StartUnitId
	out.PkgFlag = in.PkgFlag
	out.PkgFlag &^= (proto.FlagDumpUnitStart | proto.FlagDumpEnd)

	if in.DbId == proto.AdminDbId {
		return errorHandle(&out, table.EcInvDbId)
	}

	if !au.IsAuth(in.DbId) {
		return errorHandle(&out, table.EcNoPrivilege)
	}

	var onlyOneTable = (out.PkgFlag&proto.FlagDumpTable != 0)
	var rOpt = tbl.db.NewReadOptions(false)
	rOpt.SetFillCache(false)
	defer rOpt.Destroy()
	var it = tbl.db.NewIterator(rOpt)
	defer it.Destroy()

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

	const maxScanNum = 1000
	const maxTryUnitNum = 10
	var triedUnitNum = 0
	var pkgLen = proto.HeadSize + 1000
	for it.Valid() && len(out.Kvs) < maxScanNum {
		unitId, dbId, tableId, colSpace, rowKey, colKey := parseRawKey(it.Key())
		if unitId < in.StartUnitId || unitId > in.EndUnitId {
			out.PkgFlag |= proto.FlagDumpEnd
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
				out.PkgFlag |= proto.FlagDumpUnitStart
				SeekToUnit(it, nextUnitId, in.DbId, nextUnitTableId)
				continue
			} else {
				out.PkgFlag |= proto.FlagDumpEnd
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
		if len(kv.Value) > 0 {
			kv.CtrlFlag |= proto.CtrlValue
		}
		if kv.Score != 0 {
			kv.CtrlFlag |= proto.CtrlScore
		}

		out.Kvs = append(out.Kvs, kv)
		out.LastUnitId = unitId
		out.PkgFlag &^= proto.FlagDumpUnitStart

		pkgLen += kv.Length()
		if pkgLen > proto.MaxPkgLen/2 {
			out.PkgFlag &^= proto.FlagDumpEnd
			break
		}

		it.Next()
	}

	if !it.Valid() {
		out.PkgFlag |= proto.FlagDumpEnd
	}

	var pkg = make([]byte, out.Length())
	_, err = out.Encode(pkg)
	if err != nil {
		log.Fatalf("Encode failed: %s\n", err)
	}
	return pkg
}

func (tbl *Table) DeleteUnit(unitId uint16) error {
	var rOpt = tbl.db.NewReadOptions(false)
	rOpt.SetFillCache(false)
	defer rOpt.Destroy()

	const maxDelNum = 1000000
	var count, endTimes int
	for {
		var it = tbl.db.NewIterator(rOpt)
		it.Seek(getRawUnitKey(unitId, 0, 0))
		for count = 0; it.Valid() && count < maxDelNum; it.Next() {
			curUnitId, dbId, tableId := parseRawKeyUnitId(it.Key())
			if curUnitId != unitId {
				break
			}
			if dbId == proto.AdminDbId && tableId == 0 {
				continue // Reserved admin table
			}
			err := tbl.db.Del(it.Key(), nil)
			if err != nil {
				it.Destroy()
				return err
			}
			count++
		}
		it.Destroy()

		if count < maxDelNum {
			endTimes++
		}
		if endTimes > 1 {
			if count > 0 {
				return errors.New("the unit deleting still has new data written")
			}
			return nil
		}
	}

	return nil
}

func (tbl *Table) HasUnitData(unitId uint16) bool {
	var rOpt = tbl.db.NewReadOptions(false)
	rOpt.SetFillCache(false)
	defer rOpt.Destroy()
	var it = tbl.db.NewIterator(rOpt)
	defer it.Destroy()

	for it.Seek(getRawUnitKey(unitId, 0, 0)); it.Valid(); it.Next() {
		curUnitId, dbId, tableId := parseRawKeyUnitId(it.Key())
		if curUnitId != unitId {
			break
		}
		if dbId == proto.AdminDbId && tableId == 0 {
			continue // Reserved admin table
		}
		return true
	}
	return false
}

func (tbl *Table) NewIterator(fillCache bool) *Iterator {
	if fillCache {
		return tbl.db.NewIterator(nil)
	} else {
		var rOpt = tbl.db.NewReadOptions(false)
		rOpt.SetFillCache(false)
		defer rOpt.Destroy()
		return tbl.db.NewIterator(rOpt)
	}
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

func parseRawKeyUnitId(rawKey []byte) (unitId uint16, dbId, tableId uint8) {
	return binary.BigEndian.Uint16(rawKey), rawKey[2], rawKey[3]
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

func checkOneOp(in *proto.PkgOneOp, req *PkgArgs, au Authorize) bool {
	n, err := in.Decode(req.Pkg)
	if err != nil || n != len(req.Pkg) {
		in.ErrCode = table.EcDecodeFail
	}
	if in.ErrCode == 0 && in.DbId == proto.AdminDbId {
		in.ErrCode = table.EcInvDbId
	}
	if in.ErrCode == 0 && !au.IsAuth(in.DbId) {
		in.ErrCode = table.EcNoPrivilege
	}

	if in.ErrCode != 0 {
		in.CtrlFlag &^= 0xFF // Clear all ctrl flags
		in.CtrlFlag |= proto.CtrlErrCode
	}

	return in.ErrCode == 0
}

func checkMultiOp(in *proto.PkgMultiOp, req *PkgArgs, au Authorize) bool {
	n, err := in.Decode(req.Pkg)
	if err != nil || n != len(req.Pkg) {
		in.ErrCode = table.EcDecodeFail
	}
	if in.ErrCode == 0 && in.DbId == proto.AdminDbId {
		in.ErrCode = table.EcInvDbId
	}
	if in.ErrCode == 0 && !au.IsAuth(in.DbId) {
		in.ErrCode = table.EcNoPrivilege
	}

	if in.ErrCode != 0 {
		in.Kvs = nil
	}

	return in.ErrCode == 0
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

func replyMulti(out *proto.PkgMultiOp) []byte {
	var pkgLen = out.Length()
	if pkgLen > proto.MaxPkgLen {
		out.Kvs = nil
		out.SetErrCode(table.EcInvPkgLen)
		pkgLen = out.Length()
	}

	var pkg = make([]byte, pkgLen)
	_, err := out.Encode(pkg)
	if err != nil {
		log.Fatalf("Encode failed: %s\n", err)
	}
	return pkg
}
