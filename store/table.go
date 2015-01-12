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
	"github.com/stevejiang/gotable/api/go/table"
	"github.com/stevejiang/gotable/api/go/table/proto"
	"log"
	"sync"
)

const (
	zopScoreUp = uint64(0x8000000000000000)
)

type PkgArgs struct {
	Cmd  uint8
	DbId uint8
	Seq  uint64
	Pkg  []byte
}

type Table struct {
	db    *DB
	tl    *TableLock
	rwMtx *sync.RWMutex
}

func NewTable(rwMtx *sync.RWMutex, tableDir string) *Table {
	tbl := new(Table)
	tbl.tl = NewTableLock()
	tbl.rwMtx = rwMtx

	tbl.db = NewTableDB()
	err := tbl.db.Open(tableDir, true)
	if err != nil {
		log.Println("Open DB failed: ", err)
		return nil
	}

	return tbl
}

func (tbl *Table) getKV(srOpt *SnapReadOptions, dbId uint8, kv *proto.KeyValueCtrl) error {
	var err error
	var zop = (kv.ColSpace == proto.ColSpaceScore1 ||
		kv.ColSpace == proto.ColSpaceScore2)
	var rawColSpace uint8 = proto.ColSpaceDefault
	if zop {
		rawColSpace = proto.ColSpaceScore2
	}

	if zop {
		if srOpt == nil {
			// Use the same snapshot for multiple reads get consistency data
			srOpt = tbl.db.NewSnapReadOptions()
			defer srOpt.Release()
		}
		var rawKey = GetRawKey(dbId, kv.TableId, proto.ColSpaceScore2,
			kv.RowKey, kv.ColKey)
		kv.Value, err = tbl.db.Get(srOpt, rawKey)
		if err != nil {
			kv.CtrlFlag |= proto.CtrlErrCode
			kv.ErrCode = table.EcodeReadFailed
			return err
		} else if kv.Value == nil {
			kv.CtrlFlag |= proto.CtrlErrCode
			kv.ErrCode = table.EcodeNotExist
		} else {
			_, kv.Score = ParseRawValue(kv.Value)
			rawKey = GetRawKey(dbId, kv.TableId, proto.ColSpaceScore1,
				kv.RowKey, newScoreColKey(kv.Score, kv.ColKey))
			kv.Value, err = tbl.db.Get(srOpt, rawKey)
			if err != nil {
				kv.CtrlFlag |= proto.CtrlErrCode
				kv.ErrCode = table.EcodeReadFailed
				return err
			} else if kv.Value == nil {
				kv.CtrlFlag |= proto.CtrlErrCode
				kv.ErrCode = table.EcodeNotExist
			} else {
				kv.Value, _ = ParseRawValue(kv.Value)
				if kv.Value != nil {
					kv.CtrlFlag |= proto.CtrlValue
				}
				if kv.Score != 0 {
					kv.CtrlFlag |= proto.CtrlScore
				}
			}
		}
	} else {
		var rawKey = GetRawKey(dbId, kv.TableId, proto.ColSpaceDefault,
			kv.RowKey, kv.ColKey)
		kv.Value, err = tbl.db.Get(srOpt, rawKey)
		if err != nil {
			kv.CtrlFlag |= proto.CtrlErrCode
			kv.ErrCode = table.EcodeReadFailed
			return err
		} else if kv.Value == nil {
			kv.CtrlFlag |= proto.CtrlErrCode
			kv.ErrCode = table.EcodeNotExist
		} else {
			kv.Value, kv.Score = ParseRawValue(kv.Value)
			if kv.Value != nil {
				kv.CtrlFlag |= proto.CtrlValue
			}
			if kv.Score != 0 {
				kv.CtrlFlag |= proto.CtrlScore
			}
		}
	}

	if kv.Cas != 0 {
		var rawKey = GetRawKey(dbId, kv.TableId, rawColSpace, kv.RowKey, kv.ColKey)

		var lck = tbl.tl.GetLock(rawKey)
		lck.Lock()
		defer lck.Unlock()

		kv.Cas = lck.NewCas(rawKey)
		kv.CtrlFlag |= proto.CtrlCas
	}

	return nil
}

func (tbl *Table) setKV(wb *WriteBatch, dbId uint8, kv *proto.KeyValueCtrl) error {
	var err error
	var zop = (kv.ColSpace == proto.ColSpaceScore1 ||
		kv.ColSpace == proto.ColSpaceScore2)
	var rawColSpace uint8 = proto.ColSpaceDefault
	if zop {
		rawColSpace = proto.ColSpaceScore2
	}

	var rawKey = GetRawKey(dbId, kv.TableId, rawColSpace, kv.RowKey, kv.ColKey)
	var lck = tbl.tl.GetLock(rawKey)
	lck.Lock()
	defer lck.Unlock()
	if kv.Cas != 0 {
		var cas = lck.GetCas(rawKey)
		if cas != kv.Cas {
			kv.CtrlFlag |= proto.CtrlErrCode
			kv.CtrlFlag &^= (proto.CtrlCas | proto.CtrlValue | proto.CtrlScore)
			kv.ErrCode = table.EcodeCasNotMatch
			return nil
		}
	}
	lck.ClearCas(rawKey)

	if zop {
		if wb == nil {
			wb = tbl.db.NewWriteBatch()
			defer wb.Close()
		}

		var oldVal []byte
		oldVal, err = tbl.db.Get(nil, rawKey)
		if err != nil {
			kv.CtrlFlag |= proto.CtrlErrCode
			kv.ErrCode = table.EcodeReadFailed
			return err
		} else if oldVal != nil {
			_, oldScore := ParseRawValue(oldVal)
			var scoreKey = GetRawKey(dbId, kv.TableId, proto.ColSpaceScore1,
				kv.RowKey, newScoreColKey(oldScore, kv.ColKey))
			tbl.db.Del(scoreKey, wb)
		}

		tbl.db.Put(rawKey, GetRawValue(nil, kv.Score), wb)

		var scoreKey = GetRawKey(dbId, kv.TableId, proto.ColSpaceScore1,
			kv.RowKey, newScoreColKey(kv.Score, kv.ColKey))
		tbl.db.Put(scoreKey, GetRawValue(kv.Value, 0), wb)

		err = tbl.db.Commit(wb)
		if err != nil {
			kv.CtrlFlag |= proto.CtrlErrCode
			kv.ErrCode = table.EcodeWriteFailed
			return err
		}

		kv.CtrlFlag &^= (proto.CtrlValue | proto.CtrlScore)
	} else {
		err = tbl.db.Put(rawKey, GetRawValue(kv.Value, kv.Score), nil)
		if err != nil {
			kv.CtrlFlag |= proto.CtrlErrCode
			kv.ErrCode = table.EcodeWriteFailed
			return err
		}

		kv.CtrlFlag &^= (proto.CtrlValue | proto.CtrlScore)
	}

	return nil
}

func (tbl *Table) setRawKV(dbId uint8, kv *proto.KeyValueCtrl) error {
	var rawKey = GetRawKey(dbId, kv.TableId, kv.ColSpace, kv.RowKey, kv.ColKey)

	var err = tbl.db.Put(rawKey, kv.Value, nil)
	if err != nil {
		kv.CtrlFlag |= proto.CtrlErrCode
		kv.ErrCode = table.EcodeWriteFailed
		return err
	}

	kv.CtrlFlag &^= (proto.CtrlValue | proto.CtrlScore)

	return nil
}

func (tbl *Table) delKV(wb *WriteBatch, dbId uint8, kv *proto.KeyValueCtrl) error {
	var err error
	var zop = (kv.ColSpace == proto.ColSpaceScore1 ||
		kv.ColSpace == proto.ColSpaceScore2)
	var rawColSpace uint8 = proto.ColSpaceDefault
	if zop {
		rawColSpace = proto.ColSpaceScore2
	}

	var rawKey = GetRawKey(dbId, kv.TableId, rawColSpace, kv.RowKey, kv.ColKey)
	var lck = tbl.tl.GetLock(rawKey)
	lck.Lock()
	defer lck.Unlock()
	if kv.Cas != 0 {
		var cas = lck.GetCas(rawKey)
		if cas != kv.Cas {
			kv.CtrlFlag |= proto.CtrlErrCode
			kv.CtrlFlag &^= (proto.CtrlCas | proto.CtrlValue | proto.CtrlScore)
			kv.ErrCode = table.EcodeCasNotMatch
			return nil
		}
	}
	lck.ClearCas(rawKey)

	if zop {
		if wb == nil {
			wb = tbl.db.NewWriteBatch()
			defer wb.Close()
		}

		var oldVal []byte
		oldVal, err = tbl.db.Get(nil, rawKey)
		if err != nil {
			kv.CtrlFlag |= proto.CtrlErrCode
			kv.ErrCode = table.EcodeReadFailed
			return err
		} else if oldVal != nil {
			var oldScore int64
			oldVal, oldScore = ParseRawValue(oldVal)

			var scoreKey = GetRawKey(dbId, kv.TableId, proto.ColSpaceScore1,
				kv.RowKey, newScoreColKey(oldScore, kv.ColKey))
			tbl.db.Del(scoreKey, wb)
		}

		tbl.db.Del(rawKey, wb)

		kv.CtrlFlag &^= (proto.CtrlValue | proto.CtrlScore)

		err = tbl.db.Commit(wb)
		if err != nil {
			kv.CtrlFlag |= proto.CtrlErrCode
			kv.ErrCode = table.EcodeWriteFailed
			return err
		}
	} else {
		err = tbl.db.Del(rawKey, nil)
		if err != nil {
			kv.CtrlFlag |= proto.CtrlErrCode
			kv.ErrCode = table.EcodeWriteFailed
			return err
		}

		kv.CtrlFlag &^= (proto.CtrlValue | proto.CtrlScore)
	}

	return nil
}

func (tbl *Table) incrKV(wb *WriteBatch, dbId uint8, kv *proto.KeyValueCtrl) error {
	var err error
	var zop = (kv.ColSpace == proto.ColSpaceScore1 ||
		kv.ColSpace == proto.ColSpaceScore2)
	var rawColSpace uint8 = proto.ColSpaceDefault
	if zop {
		rawColSpace = proto.ColSpaceScore2
	}

	var rawKey = GetRawKey(dbId, kv.TableId, rawColSpace, kv.RowKey, kv.ColKey)
	var lck = tbl.tl.GetLock(rawKey)
	lck.Lock()
	defer lck.Unlock()
	if kv.Cas != 0 {
		var cas = lck.GetCas(rawKey)
		if cas != kv.Cas {
			kv.CtrlFlag |= proto.CtrlErrCode
			kv.CtrlFlag &^= (proto.CtrlCas | proto.CtrlValue | proto.CtrlScore)
			kv.ErrCode = table.EcodeCasNotMatch
			return nil
		}
	}
	lck.ClearCas(rawKey)

	var newScore = kv.Score
	var oldVal []byte
	if zop {
		if wb == nil {
			wb = tbl.db.NewWriteBatch()
			defer wb.Close()
		}

		oldVal, err = tbl.db.Get(nil, rawKey)
		if err != nil {
			kv.CtrlFlag |= proto.CtrlErrCode
			kv.ErrCode = table.EcodeReadFailed
			return err
		} else if oldVal != nil {
			_, oldScore := ParseRawValue(oldVal)
			newScore += oldScore

			var scoreKey = GetRawKey(dbId, kv.TableId, proto.ColSpaceScore1,
				kv.RowKey, newScoreColKey(oldScore, kv.ColKey))
			oldVal, err = tbl.db.Get(nil, scoreKey)
			if err != nil {
				kv.CtrlFlag |= proto.CtrlErrCode
				kv.ErrCode = table.EcodeReadFailed
				return err
			} else if oldVal != nil {
				oldVal, _ = ParseRawValue(oldVal)
			}

			tbl.db.Del(scoreKey, wb)
		}

		tbl.db.Put(rawKey, GetRawValue(nil, newScore), wb)

		var scoreKey = GetRawKey(dbId, kv.TableId, proto.ColSpaceScore1,
			kv.RowKey, newScoreColKey(newScore, kv.ColKey))
		tbl.db.Put(scoreKey, GetRawValue(oldVal, 0), wb)

		kv.Value = oldVal
		kv.Score = newScore
		if kv.Value != nil {
			kv.CtrlFlag |= proto.CtrlValue
		}
		if kv.Score != 0 {
			kv.CtrlFlag |= proto.CtrlScore
		}

		err = tbl.db.Commit(wb)
		if err != nil {
			kv.CtrlFlag |= proto.CtrlErrCode
			kv.ErrCode = table.EcodeWriteFailed
			return err
		}
	} else {
		oldVal, err = tbl.db.Get(nil, rawKey)
		if err != nil {
			kv.CtrlFlag |= proto.CtrlErrCode
			kv.ErrCode = table.EcodeReadFailed
			return err
		} else if oldVal != nil {
			var oldScore int64
			oldVal, oldScore = ParseRawValue(oldVal)
			newScore += oldScore
		}

		err = tbl.db.Put(rawKey, GetRawValue(oldVal, newScore), nil)
		if err != nil {
			kv.CtrlFlag |= proto.CtrlErrCode
			kv.ErrCode = table.EcodeWriteFailed
			return err
		}

		kv.Value = oldVal
		kv.Score = newScore
		if kv.Value != nil {
			kv.CtrlFlag |= proto.CtrlValue
		}
		if kv.Score != 0 {
			kv.CtrlFlag |= proto.CtrlScore
		}
	}

	return nil
}

func (tbl *Table) Get(req *PkgArgs) []byte {
	var in proto.PkgOneOp
	_, err := in.Decode(req.Pkg)
	if err == nil {
		if in.DbId != 0 {
			err = tbl.getKV(nil, in.DbId, &in.KeyValueCtrl)
			if err != nil {
				log.Printf("getKV failed: %s\n", err)
			}
		} else {
			in.ErrCode = table.EcodeInvalidDbId
			in.CtrlFlag &^= (proto.CtrlValue | proto.CtrlScore)
		}
	} else {
		in.ErrCode = table.EcodeDecodeFailed
		in.CtrlFlag &^= (proto.CtrlValue | proto.CtrlScore)
	}

	pkg, _, err := in.Encode(nil)
	if err != nil {
		log.Fatalf("Encode failed: %s\n", err)
	}
	return pkg
}

func (tbl *Table) MGet(req *PkgArgs) []byte {
	var in proto.PkgMultiOp
	_, err := in.Decode(req.Pkg)
	if err == nil {
		if in.DbId != 0 {
			var srOpt = tbl.db.NewSnapReadOptions()
			defer srOpt.Release()
			for i := 0; i < len(in.Kvs); i++ {
				err = tbl.getKV(srOpt, in.DbId, &in.Kvs[i])
				if err != nil {
					log.Printf("getKV failed: %s\n", err)
					break
				}
			}
		} else {
			in.ErrCode = table.EcodeInvalidDbId
			in.Kvs = nil
		}
	} else {
		in.ErrCode = table.EcodeDecodeFailed
		in.Kvs = nil
	}

	pkg, _, err := in.Encode(nil)
	if err != nil {
		log.Fatalf("Encode failed: %s\n", err)
	}
	return pkg
}

func (tbl *Table) Sync(req *PkgArgs) ([]byte, bool) {
	var in proto.PkgOneOp
	_, err := in.Decode(req.Pkg)
	if err == nil {
		// Sync full DB, including DB 0
		tbl.rwMtx.RLock()
		err = tbl.setRawKV(in.DbId, &in.KeyValueCtrl)
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
		in.ErrCode = table.EcodeDecodeFailed
	}

	pkg, _, err := in.Encode(nil)
	if err != nil {
		log.Fatalf("Encode failed: %s\n", err)
	}
	return pkg, table.EcodeOk == in.ErrCode
}

func (tbl *Table) Set(req *PkgArgs) ([]byte, bool) {
	var in proto.PkgOneOp
	_, err := in.Decode(req.Pkg)
	if err == nil {
		if in.DbId != 0 {
			tbl.rwMtx.RLock()
			err = tbl.setKV(nil, in.DbId, &in.KeyValueCtrl)
			tbl.rwMtx.RUnlock()

			if err != nil {
				log.Printf("setKV failed: %s\n", err)
			}
		} else {
			in.ErrCode = table.EcodeInvalidDbId
			in.CtrlFlag &^= (proto.CtrlValue | proto.CtrlScore)
		}
	} else {
		in.ErrCode = table.EcodeDecodeFailed
		in.CtrlFlag &^= (proto.CtrlValue | proto.CtrlScore)
	}

	pkg, _, err := in.Encode(nil)
	if err != nil {
		log.Fatalf("Encode failed: %s\n", err)
	}
	return pkg, table.EcodeOk == in.ErrCode
}

func (tbl *Table) MSet(req *PkgArgs) ([]byte, bool) {
	var in proto.PkgMultiOp
	_, err := in.Decode(req.Pkg)
	if err == nil {
		if in.DbId != 0 {
			var wb = tbl.db.NewWriteBatch()
			defer wb.Close()
			tbl.rwMtx.RLock()
			for i := 0; i < len(in.Kvs); i++ {
				err = tbl.setKV(wb, in.DbId, &in.Kvs[i])
				if err != nil {
					log.Printf("setKV failed: %s\n", err)
					break
				}
			}
			tbl.rwMtx.RUnlock()
		} else {
			in.ErrCode = table.EcodeInvalidDbId
			in.Kvs = nil
		}
	} else {
		in.ErrCode = table.EcodeDecodeFailed
		in.Kvs = nil
	}

	pkg, _, err := in.Encode(nil)
	if err != nil {
		log.Fatalf("Encode failed: %s\n", err)
	}
	return pkg, table.EcodeOk == in.ErrCode
}

func (tbl *Table) Del(req *PkgArgs) ([]byte, bool) {
	var in proto.PkgOneOp
	_, err := in.Decode(req.Pkg)
	if err == nil {
		if in.DbId != 0 {
			tbl.rwMtx.RLock()
			err = tbl.delKV(nil, in.DbId, &in.KeyValueCtrl)
			tbl.rwMtx.RUnlock()

			if err != nil {
				log.Printf("delKV failed: %s\n", err)
			}
		} else {
			in.ErrCode = table.EcodeInvalidDbId
			in.CtrlFlag &^= (proto.CtrlValue | proto.CtrlScore)
		}
	} else {
		in.ErrCode = table.EcodeDecodeFailed
		in.CtrlFlag &^= (proto.CtrlValue | proto.CtrlScore)
	}

	pkg, _, err := in.Encode(nil)
	if err != nil {
		log.Fatalf("Encode failed: %s\n", err)
	}
	return pkg, table.EcodeOk == in.ErrCode
}

func (tbl *Table) MDel(req *PkgArgs) ([]byte, bool) {
	var in proto.PkgMultiOp
	_, err := in.Decode(req.Pkg)
	if err == nil {
		if in.DbId != 0 {
			var wb = tbl.db.NewWriteBatch()
			defer wb.Close()
			tbl.rwMtx.RLock()
			for i := 0; i < len(in.Kvs); i++ {
				err = tbl.delKV(wb, in.DbId, &in.Kvs[i])
				if err != nil {
					log.Printf("delKV failed: %s\n", err)
					break
				}
			}
			tbl.rwMtx.RUnlock()
		} else {
			in.ErrCode = table.EcodeInvalidDbId
			in.Kvs = nil
		}
	} else {
		in.ErrCode = table.EcodeDecodeFailed
		in.Kvs = nil
	}

	pkg, _, err := in.Encode(nil)
	if err != nil {
		log.Fatalf("Encode failed: %s\n", err)
	}
	return pkg, table.EcodeOk == in.ErrCode
}

func (tbl *Table) Incr(req *PkgArgs) ([]byte, bool) {
	var in proto.PkgOneOp
	_, err := in.Decode(req.Pkg)
	if err == nil {
		if in.DbId != 0 {
			tbl.rwMtx.RLock()
			err = tbl.incrKV(nil, in.DbId, &in.KeyValueCtrl)
			tbl.rwMtx.RUnlock()

			if err != nil {
				log.Printf("incrKV failed: %s\n", err)
			}
		} else {
			in.ErrCode = table.EcodeInvalidDbId
			in.CtrlFlag &^= (proto.CtrlValue | proto.CtrlScore)
		}
	} else {
		in.ErrCode = table.EcodeDecodeFailed
		in.CtrlFlag &^= (proto.CtrlValue | proto.CtrlScore)
	}

	pkg, _, err := in.Encode(nil)
	if err != nil {
		log.Fatalf("Encode failed: %s\n", err)
	}
	return pkg, table.EcodeOk == in.ErrCode
}

func (tbl *Table) MIncr(req *PkgArgs) ([]byte, bool) {
	var in proto.PkgMultiOp
	_, err := in.Decode(req.Pkg)
	if err == nil {
		if in.DbId != 0 {
			var wb = tbl.db.NewWriteBatch()
			defer wb.Close()
			tbl.rwMtx.RLock()
			for i := 0; i < len(in.Kvs); i++ {
				err = tbl.incrKV(wb, in.DbId, &in.Kvs[i])
				if err != nil {
					log.Printf("incrKV failed: %s\n", err)
					break
				}
			}
			tbl.rwMtx.RUnlock()
		} else {
			in.ErrCode = table.EcodeInvalidDbId
			in.Kvs = nil
		}
	} else {
		in.ErrCode = table.EcodeDecodeFailed
		in.Kvs = nil
	}

	pkg, _, err := in.Encode(nil)
	if err != nil {
		log.Fatalf("Encode failed: %s\n", err)
	}
	return pkg, table.EcodeOk == in.ErrCode
}

func iterMove(it *Iterator, direction uint8) {
	if direction == 0 {
		it.Next()
	} else {
		it.Prev()
	}
}

func (tbl *Table) zScanSortScore(in *proto.PkgScanReq, out *proto.PkgScanResp) {
	var it = tbl.db.NewIterator(true)
	defer it.Close()

	var scanColSpace uint8 = proto.ColSpaceScore1
	it.Seek(GetRawKey(in.DbId, in.TableId, scanColSpace,
		in.RowKey, newScoreColKey(in.Score, in.ColKey)))
	var first = true
	for i := 0; it.Valid() && i < int(in.Num); iterMove(it, in.Direction) {
		_, dbId, tableId, colSpace, rowKey, colKey := ParseRawKey(it.Key())
		if dbId != in.DbId || tableId != in.TableId ||
			colSpace != scanColSpace || bytes.Compare(rowKey, in.RowKey) != 0 {
			if first {
				first = false
				continue
			} else {
				break
			}
		}

		var zScore int64
		var zColKey []byte
		if len(colKey) >= 8 {
			zScore = int64(binary.BigEndian.Uint64(colKey) - zopScoreUp)
			zColKey = colKey[8:]
			if in.Direction == 0 {
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
		} else {
			break
		}

		var kv proto.KeyValueCtrl
		kv.TableId = in.TableId
		kv.RowKey = in.RowKey
		kv.ColKey = zColKey
		kv.Value, _ = ParseRawValue(it.Value())
		kv.Score = zScore
		if kv.Value != nil {
			kv.CtrlFlag |= proto.CtrlValue
		}
		if kv.Score != 0 {
			kv.CtrlFlag |= proto.CtrlScore
		}
		out.Kvs = append(out.Kvs, kv)
		i++
	}
}

func (tbl *Table) Scan(req *PkgArgs) []byte {
	var out proto.PkgScanResp
	out.Cmd = req.Cmd
	out.DbId = req.DbId
	out.Seq = req.Seq

	var in proto.PkgScanReq
	_, err := in.Decode(req.Pkg)
	if err != nil {
		out.ErrCode = table.EcodeDecodeFailed
		pkg, _, err := out.Encode(nil)
		if err != nil {
			log.Fatalf("Encode failed: %s\n", err)
		}
		return pkg
	}

	if in.DbId == 0 {
		out.ErrCode = table.EcodeInvalidDbId
		pkg, _, err := out.Encode(nil)
		if err != nil {
			log.Fatalf("Encode failed: %s\n", err)
		}
		return pkg
	}

	if in.ColSpace == proto.ColSpaceScore1 {
		tbl.zScanSortScore(&in, &out)
		pkg, _, err := out.Encode(nil)
		if err != nil {
			log.Fatalf("Encode failed: %s\n", err)
		}
		return pkg
	}

	var it = tbl.db.NewIterator(true)
	defer it.Close()

	var scanColSpace uint8 = proto.ColSpaceDefault
	it.Seek(GetRawKey(in.DbId, in.TableId, scanColSpace, in.RowKey, in.ColKey))
	var first = true
	for i := 0; it.Valid() && i < int(in.Num); iterMove(it, in.Direction) {
		_, dbId, tableId, colSpace, rowKey, colKey := ParseRawKey(it.Key())
		if dbId != in.DbId || tableId != in.TableId ||
			colSpace != scanColSpace || bytes.Compare(rowKey, in.RowKey) != 0 {
			if first {
				first = false
				continue
			} else {
				break
			}
		}

		if in.Direction == 0 {
			if bytes.Compare(colKey, in.ColKey) <= 0 {
				continue
			}
		} else {
			if bytes.Compare(colKey, in.ColKey) >= 0 {
				continue
			}
		}

		var kv proto.KeyValueCtrl
		kv.TableId = in.TableId
		kv.RowKey = in.RowKey
		kv.ColKey = colKey
		kv.Value, kv.Score = ParseRawValue(it.Value())
		if kv.Value != nil {
			kv.CtrlFlag |= proto.CtrlValue
		}
		if kv.Score != 0 {
			kv.CtrlFlag |= proto.CtrlScore
		}
		out.Kvs = append(out.Kvs, kv)
		i++
	}

	pkg, _, err := out.Encode(nil)
	if err != nil {
		log.Fatalf("Encode failed: %s\n", err)
	}
	return pkg
}

func (tbl *Table) NewIterator(fillCache bool) *Iterator {
	return tbl.db.NewIterator(fillCache)
}

func GetRawKey(dbId, tableId, colSpace uint8, rowKey, colKey []byte) []byte {
	var unitId = proto.GetUnitId(dbId, tableId, rowKey)

	// wUnitId+cDbId+cTableId+cKeyLen+sRowKey+colSpace+sColKey
	var rowKeyLen = len(rowKey)
	var rawLen = 6 + rowKeyLen + len(colKey)
	var rawKey = make([]byte, rawLen, rawLen)
	binary.BigEndian.PutUint16(rawKey, unitId)
	rawKey[2] = dbId
	rawKey[3] = tableId
	rawKey[4] = uint8(rowKeyLen)
	copy(rawKey[5:], rowKey)
	rawKey[5+rowKeyLen] = colSpace
	copy(rawKey[6+rowKeyLen:], colKey)

	return rawKey
}

func ParseRawKey(rawKey []byte) (unitId uint16, dbId, tableId, colSpace uint8,
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

func ParseRawValue(value []byte) ([]byte, int64) {
	if value == nil {
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

func GetRawValue(value []byte, score int64) []byte {
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
