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

package proto

import (
	"encoding/binary"
)

const (
	CtrlDbIdExt  = 0x1
	CtrlErrCode  = 0x2
	CtrlCas      = 0x4
	CtrlColSpace = 0x8
	CtrlValue    = 0x10
	CtrlScore    = 0x20
)

const (
	ColSpaceDefault = 0 // Default space
	ColSpaceScore1  = 1 // rowKey+score+colKey => value
	ColSpaceScore2  = 2 // rowKey+colKey => score+value
)

type KeyValue struct {
	TableId uint8
	RowKey  []byte
	ColKey  []byte
	Value   []byte // default: nil if missing
	Score   int64  // default: 0 if missing
	Cas     uint32 // default: 0 if missing
}

// KeyValueCtrl=cCtrlFlag+[cDbIdExt]+cTableId+[cErrCode]+[dwCas]
//             +[cColSpace]+cRowKeyLen+sRowKey+wColKeyLen+sColKey
//             +[dwValueLen+sValue]+[ddwScore]
type KeyValueCtrl struct {
	CtrlFlag uint8
	DbIdExt  uint8 // default: 0 if missing
	ErrCode  uint8 // default: 0 if missing
	ColSpace uint8 // default: 0 if missing
	KeyValue
}

// Get, Set, Del, GetSet, ZGet, ZSet, ZGetSet, Sync
// PKG = HEAD+KeyValueCtrl
type PkgOneOp struct {
	PkgHead
	KeyValueCtrl
}

// MGet, MSet, MDel, MZGet, MZSet, MZDel
// PKG = HEAD+cErrCode+wNum+KeyValueCtrl[wNum]
type PkgMultiOp struct {
	ErrCode uint8 // default: 0 if missing
	PkgHead
	Kvs []KeyValueCtrl
}

const (
	ScopeTableId = 0 // Dump TableId
	ScopeDbId    = 1 // Dump DbId
	ScopeFullDB  = 2 // Dump Full DB
)

// Scan, ZScan
// PKG = HEAD+cDirection+wNum+KeyValueCtrl
type PkgScanReq struct {
	Direction uint8 // 0: Ascending  order; 1: Descending  order
	Num       uint16
	PkgHead
	KeyValueCtrl
}

// Scan, ZScan
// PKG = HEAD+cErrCode+cDirection+cEnd+wNum+KeyValueCtrl[wNum]
type PkgScanResp struct {
	Direction uint8
	End       uint8 // 0: Not end yet; 1: Has scan to end, stop now
	ErrCode   uint8 // default: 0 if missing
	PkgHead
	Kvs []KeyValueCtrl
}

// Dump
// PKG = HEAD+cScope+wUnitId+KeyValueCtrl
type PkgDumpReq struct {
	Scope  uint8  // Only for Dump (0: TableId; 1: DbId; 2: full DB)
	UnitId uint16 // Dump Unit ID start point
	PkgHead
	KeyValueCtrl
}

// Dump
// PKG = HEAD+cErrCode+cScope+cEnd+wUnitId+wNum+KeyValueCtrl[wNum]
type PkgDumpResp struct {
	Scope   uint8
	End     uint8  // 0: Not end yet; 1: Has dump to end, stop now
	ErrCode uint8  // default: 0 if missing
	UnitId  uint16 // Last dump Unit ID
	PkgHead
	Kvs []KeyValueCtrl
}

func (kv *KeyValueCtrl) Length() int {
	// KeyValueCtrl=cCtrlFlag+[cDbIdExt]+cTableId+[cErrCode]+[dwCas]
	//             +[cColSpace]+cRowKeyLen+sRowKey+wColKeyLen+sColKey
	//             +[dwValueLen+sValue]+[ddwScore]
	var n = 2
	if kv.CtrlFlag&CtrlDbIdExt != 0 {
		n += 1
	}
	if kv.CtrlFlag&CtrlErrCode != 0 {
		n += 1
	}
	if kv.CtrlFlag&CtrlCas != 0 {
		n += 4
	}
	if kv.CtrlFlag&CtrlColSpace != 0 {
		n += 1
	}
	n += 1 + len(kv.RowKey) + 2 + len(kv.ColKey)
	if kv.CtrlFlag&CtrlValue != 0 {
		n += 4 + len(kv.Value)
	}
	if kv.CtrlFlag&CtrlScore != 0 {
		n += 8
	}
	return n
}

func (kv *KeyValueCtrl) Encode(pkg []byte) (int, error) {
	if len(pkg) < kv.Length() {
		return 0, ErrPkgLen
	}
	if len(kv.RowKey) > MaxUint8 {
		return 0, ErrRowKeyLen
	}
	if len(kv.ColKey) > MaxUint16 {
		return 0, ErrColKeyLen
	}
	if len(kv.Value) > MaxValueLen {
		return 0, ErrValueLen
	}

	var n int
	pkg[n] = kv.CtrlFlag
	n += 1
	if kv.CtrlFlag&CtrlDbIdExt != 0 {
		pkg[n] = kv.DbIdExt
		n += 1
	}
	pkg[n] = kv.TableId
	n += 1
	if kv.CtrlFlag&CtrlErrCode != 0 {
		pkg[n] = kv.ErrCode
		n += 1
	}
	if kv.CtrlFlag&CtrlCas != 0 {
		binary.BigEndian.PutUint32(pkg[n:], kv.Cas)
		n += 4
	}
	if kv.CtrlFlag&CtrlColSpace != 0 {
		pkg[n] = kv.ColSpace
		n += 1
	}

	pkg[n] = uint8(len(kv.RowKey))
	n += 1
	copy(pkg[n:], kv.RowKey)
	n += len(kv.RowKey)

	binary.BigEndian.PutUint16(pkg[n:], uint16(len(kv.ColKey)))
	n += 2
	copy(pkg[n:], kv.ColKey)
	n += len(kv.ColKey)

	if kv.CtrlFlag&CtrlValue != 0 {
		binary.BigEndian.PutUint32(pkg[n:], uint32(len(kv.Value)))
		n += 4
		copy(pkg[n:], kv.Value)
		n += len(kv.Value)
	}
	if kv.CtrlFlag&CtrlScore != 0 {
		binary.BigEndian.PutUint64(pkg[n:], uint64(kv.Score))
		n += 8
	}
	return n, nil
}

func (kv *KeyValueCtrl) Decode(pkg []byte) (int, error) {
	var pkgLen = len(pkg)
	var n int
	if n+1 > pkgLen {
		return n, ErrPkgLen
	}
	kv.CtrlFlag = pkg[n]
	n += 1

	if kv.CtrlFlag&CtrlDbIdExt != 0 {
		if n+1 > pkgLen {
			return n, ErrPkgLen
		}
		kv.DbIdExt = pkg[n]
		n += 1
	}

	if n+1 > pkgLen {
		return n, ErrPkgLen
	}
	kv.TableId = pkg[n]
	n += 1

	if kv.CtrlFlag&CtrlErrCode != 0 {
		if n+1 > pkgLen {
			return n, ErrPkgLen
		}
		kv.ErrCode = pkg[n]
		n += 1
	}
	if kv.CtrlFlag&CtrlCas != 0 {
		if n+4 > pkgLen {
			return n, ErrPkgLen
		}
		kv.Cas = binary.BigEndian.Uint32(pkg[n:])
		n += 4
	}
	if kv.CtrlFlag&CtrlColSpace != 0 {
		if n+1 > pkgLen {
			return n, ErrPkgLen
		}
		kv.ColSpace = pkg[n]
		n += 1
	}

	if n+1 > pkgLen {
		return n, ErrPkgLen
	}
	var rowKeyLen = int(pkg[n])
	n += 1
	if n+rowKeyLen+2 > pkgLen {
		return n, ErrPkgLen
	}
	if rowKeyLen > 0 {
		kv.RowKey = pkg[n : n+rowKeyLen]
		n += rowKeyLen
	} else {
		kv.RowKey = nil
	}

	var colKeyLen = int(binary.BigEndian.Uint16(pkg[n:]))
	n += 2
	if n+colKeyLen > pkgLen {
		return n, ErrPkgLen
	}
	if colKeyLen > 0 {
		kv.ColKey = pkg[n : n+colKeyLen]
		n += colKeyLen
	} else {
		kv.ColKey = nil
	}

	if kv.CtrlFlag&CtrlValue != 0 {
		if n+4 > pkgLen {
			return n, ErrPkgLen
		}
		var valueLen = int(binary.BigEndian.Uint32(pkg[n:]))
		n += 4
		if valueLen > MaxValueLen {
			return n, ErrValueLen
		}
		if n+valueLen > pkgLen {
			return n, ErrPkgLen
		}
		if valueLen > 0 {
			kv.Value = pkg[n : n+valueLen]
			n += valueLen
		} else {
			kv.Value = nil
		}
	}
	if kv.CtrlFlag&CtrlScore != 0 {
		if n+8 > pkgLen {
			return n, ErrPkgLen
		}
		kv.Score = int64(binary.BigEndian.Uint64(pkg[n:]))
		n += 8
	}
	return n, nil
}

func (p *PkgOneOp) Length() int {
	// PKG = HEAD+KeyValueCtrl
	return HeadSize + p.KeyValueCtrl.Length()
}

func (p *PkgOneOp) SetErrCode(errCode uint8) {
	p.ErrCode = errCode
}

func (p *PkgOneOp) Encode(pkg []byte) ([]byte, int, error) {
	var pkgLen = p.Length()
	if pkg == nil {
		pkg = make([]byte, pkgLen)
	}

	var n, m int
	if len(pkg) < pkgLen {
		return pkg, n, ErrPkgLen
	}

	p.PkgLen = uint32(pkgLen)
	var err = p.EncodeHead(pkg)
	if err != nil {
		return pkg, n, err
	}
	n += HeadSize

	m, err = p.KeyValueCtrl.Encode(pkg[n:])
	if err != nil {
		return pkg, n, err
	}
	n += m

	return pkg, n, nil
}

func (p *PkgOneOp) Decode(pkg []byte) (int, error) {
	var n, m int
	var err = p.DecodeHead(pkg)
	if err != nil {
		return n, err
	}
	n += HeadSize

	m, err = p.KeyValueCtrl.Decode(pkg[n:])
	if err != nil {
		return n, err
	}
	n += m

	return n, nil
}

func (p *PkgMultiOp) Length() int {
	// PKG = HEAD+cErrCode+wNum+KeyValueCtrl[wNum]
	var n = HeadSize + 3
	for i := 0; i < len(p.Kvs); i++ {
		n += p.Kvs[i].Length()
	}
	return n
}

func (p *PkgMultiOp) SetErrCode(errCode uint8) {
	p.ErrCode = errCode
}

func (p *PkgMultiOp) Encode(pkg []byte) ([]byte, int, error) {
	var numKvs = len(p.Kvs)
	if numKvs > MaxUint16 {
		return nil, 0, ErrKvArrayLen
	}

	var pkgLen = p.Length()
	if pkg == nil {
		pkg = make([]byte, pkgLen)
	}

	var n, m int
	if len(pkg) < pkgLen {
		return pkg, n, ErrPkgLen
	}

	p.PkgLen = uint32(pkgLen)
	var err = p.EncodeHead(pkg)
	if err != nil {
		return pkg, n, err
	}
	n += HeadSize

	pkg[n] = p.ErrCode
	n += 1
	binary.BigEndian.PutUint16(pkg[n:], uint16(numKvs))
	n += 2

	for i := 0; i < numKvs; i++ {
		m, err = p.Kvs[i].Encode(pkg[n:])
		if err != nil {
			return pkg, n, err
		}
		n += m
	}

	return pkg, n, nil
}

func (p *PkgMultiOp) Decode(pkg []byte) (int, error) {
	var pkgLen = len(pkg)
	var n, m int
	var err = p.DecodeHead(pkg)
	if err != nil {
		return n, err
	}
	n += HeadSize

	if n+3 > pkgLen {
		return n, ErrPkgLen
	}
	p.ErrCode = pkg[n]
	n += 1
	var numKvs = int(binary.BigEndian.Uint16(pkg[n:]))
	n += 2

	p.Kvs = make([]KeyValueCtrl, numKvs)
	for i := 0; i < numKvs; i++ {
		m, err = p.Kvs[i].Decode(pkg[n:])
		if err != nil {
			return n, err
		}
		n += m
	}

	return n, nil
}

func (p *PkgScanReq) Length() int {
	// PKG = HEAD+cDirection+wNum+KeyValueCtrl
	return HeadSize + 3 + p.KeyValueCtrl.Length()
}

func (p *PkgScanReq) Encode(pkg []byte) ([]byte, int, error) {
	var pkgLen = p.Length()
	if pkg == nil {
		pkg = make([]byte, pkgLen)
	}

	var n, m int
	if len(pkg) < pkgLen {
		return pkg, n, ErrPkgLen
	}

	p.PkgLen = uint32(pkgLen)
	var err = p.EncodeHead(pkg)
	if err != nil {
		return pkg, n, err
	}
	n += HeadSize

	pkg[n] = p.Direction
	n += 1
	binary.BigEndian.PutUint16(pkg[n:], p.Num)
	n += 2

	m, err = p.KeyValueCtrl.Encode(pkg[n:])
	if err != nil {
		return pkg, n, err
	}
	n += m

	return pkg, n, nil
}

func (p *PkgScanReq) Decode(pkg []byte) (int, error) {
	var pkgLen = len(pkg)
	var n, m int
	var err = p.DecodeHead(pkg)
	if err != nil {
		return n, err
	}
	n += HeadSize

	if n+3 > pkgLen {
		return n, ErrPkgLen
	}

	p.Direction = pkg[n]
	n += 1
	p.Num = binary.BigEndian.Uint16(pkg[n:])
	n += 2

	m, err = p.KeyValueCtrl.Decode(pkg[n:])
	if err != nil {
		return n, err
	}
	n += m

	return n, nil
}

func (p *PkgScanResp) Length() int {
	// PKG = HEAD+cErrCode+cDirection+cEnd+wNum+KeyValueCtrl[wNum]
	var n = HeadSize + 5
	for i := 0; i < len(p.Kvs); i++ {
		n += p.Kvs[i].Length()
	}
	return n
}

func (p *PkgScanResp) SetErrCode(errCode uint8) {
	p.ErrCode = errCode
}

func (p *PkgScanResp) Encode(pkg []byte) ([]byte, int, error) {
	var numKvs = len(p.Kvs)
	if numKvs > MaxUint16 {
		return nil, 0, ErrKvArrayLen
	}

	var pkgLen = p.Length()
	if pkg == nil {
		pkg = make([]byte, pkgLen)
	}

	var n, m int
	if len(pkg) < pkgLen {
		return pkg, n, ErrPkgLen
	}

	p.PkgLen = uint32(pkgLen)
	var err = p.EncodeHead(pkg)
	if err != nil {
		return pkg, n, err
	}
	n += HeadSize

	pkg[n] = p.ErrCode
	n += 1
	pkg[n] = p.Direction
	n += 1
	pkg[n] = p.End
	n += 1
	binary.BigEndian.PutUint16(pkg[n:], uint16(numKvs))
	n += 2

	for i := 0; i < numKvs; i++ {
		m, err = p.Kvs[i].Encode(pkg[n:])
		if err != nil {
			return pkg, n, err
		}
		n += m
	}

	return pkg, n, nil
}

func (p *PkgScanResp) Decode(pkg []byte) (int, error) {
	var pkgLen = len(pkg)
	var n, m int
	var err = p.DecodeHead(pkg)
	if err != nil {
		return n, err
	}
	n += HeadSize

	if n+5 > pkgLen {
		return n, ErrPkgLen
	}
	p.ErrCode = pkg[n]
	n += 1
	p.Direction = pkg[n]
	n += 1
	p.End = pkg[n]
	n += 1
	var numKvs = int(binary.BigEndian.Uint16(pkg[n:]))
	n += 2

	p.Kvs = make([]KeyValueCtrl, numKvs)
	for i := 0; i < numKvs; i++ {
		m, err = p.Kvs[i].Decode(pkg[n:])
		if err != nil {
			return n, err
		}
		n += m
	}

	return n, nil
}

func (p *PkgDumpReq) Length() int {
	// PKG = HEAD+cScope+wUnitId+KeyValueCtrl
	return HeadSize + 3 + p.KeyValueCtrl.Length()
}

func (p *PkgDumpReq) Encode(pkg []byte) ([]byte, int, error) {
	var pkgLen = p.Length()
	if pkg == nil {
		pkg = make([]byte, pkgLen)
	}

	var n, m int
	if len(pkg) < pkgLen {
		return pkg, n, ErrPkgLen
	}

	p.PkgLen = uint32(pkgLen)
	var err = p.EncodeHead(pkg)
	if err != nil {
		return pkg, n, err
	}
	n += HeadSize

	pkg[n] = p.Scope
	n += 1
	binary.BigEndian.PutUint16(pkg[n:], p.UnitId)
	n += 2

	m, err = p.KeyValueCtrl.Encode(pkg[n:])
	if err != nil {
		return pkg, n, err
	}
	n += m

	return pkg, n, nil
}

func (p *PkgDumpReq) Decode(pkg []byte) (int, error) {
	var pkgLen = len(pkg)
	var n, m int
	var err = p.DecodeHead(pkg)
	if err != nil {
		return n, err
	}
	n += HeadSize

	if n+3 > pkgLen {
		return n, ErrPkgLen
	}

	p.Scope = pkg[n]
	n += 1
	p.UnitId = binary.BigEndian.Uint16(pkg[n:])
	n += 2

	m, err = p.KeyValueCtrl.Decode(pkg[n:])
	if err != nil {
		return n, err
	}
	n += m

	return n, nil
}

func (p *PkgDumpResp) Length() int {
	// PKG = HEAD+cErrCode+cScope+cEnd+wUnitId+wNum+KeyValueCtrl[wNum]
	var n = HeadSize + 7
	for i := 0; i < len(p.Kvs); i++ {
		n += p.Kvs[i].Length()
	}
	return n
}

func (p *PkgDumpResp) SetErrCode(errCode uint8) {
	p.ErrCode = errCode
}

func (p *PkgDumpResp) Encode(pkg []byte) ([]byte, int, error) {
	var numKvs = len(p.Kvs)
	if numKvs > MaxUint16 {
		return nil, 0, ErrKvArrayLen
	}

	var pkgLen = p.Length()
	if pkg == nil {
		pkg = make([]byte, pkgLen)
	}

	var n, m int
	if len(pkg) < pkgLen {
		return pkg, n, ErrPkgLen
	}

	p.PkgLen = uint32(pkgLen)
	var err = p.EncodeHead(pkg)
	if err != nil {
		return pkg, n, err
	}
	n += HeadSize

	pkg[n] = p.ErrCode
	n += 1
	pkg[n] = p.Scope
	n += 1
	pkg[n] = p.End
	n += 1
	binary.BigEndian.PutUint16(pkg[n:], p.UnitId)
	n += 2
	binary.BigEndian.PutUint16(pkg[n:], uint16(numKvs))
	n += 2

	for i := 0; i < numKvs; i++ {
		m, err = p.Kvs[i].Encode(pkg[n:])
		if err != nil {
			return pkg, n, err
		}
		n += m
	}

	return pkg, n, nil
}

func (p *PkgDumpResp) Decode(pkg []byte) (int, error) {
	var pkgLen = len(pkg)
	var n, m int
	var err = p.DecodeHead(pkg)
	if err != nil {
		return n, err
	}
	n += HeadSize

	if n+7 > pkgLen {
		return n, ErrPkgLen
	}
	p.ErrCode = pkg[n]
	n += 1
	p.Scope = pkg[n]
	n += 1
	p.End = pkg[n]
	n += 1
	p.UnitId = binary.BigEndian.Uint16(pkg[n:])
	n += 2
	var numKvs = int(binary.BigEndian.Uint16(pkg[n:]))
	n += 2

	p.Kvs = make([]KeyValueCtrl, numKvs)
	for i := 0; i < numKvs; i++ {
		m, err = p.Kvs[i].Decode(pkg[n:])
		if err != nil {
			return n, err
		}
		n += m
	}

	return n, nil
}
