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

package table

import (
	"github.com/stevejiang/gotable/api/go/table/proto"
)

type GetArgs struct {
	TableId uint8
	RowKey  []byte
	ColKey  []byte
	Cas     uint32
}

type GetReply struct {
	ErrCode int8
	TableId uint8
	RowKey  []byte
	ColKey  []byte
	Value   []byte
	Score   int64
	Cas     uint32
}

type SetArgs struct {
	TableId uint8
	RowKey  []byte
	ColKey  []byte
	Value   []byte
	Score   int64
	Cas     uint32
}

type SetReply struct {
	ErrCode int8
	TableId uint8
	RowKey  []byte
	ColKey  []byte
}

type IncrArgs struct {
	TableId uint8
	RowKey  []byte
	ColKey  []byte
	Score   int64
	Cas     uint32
}

type IncrReply struct {
	ErrCode int8
	TableId uint8
	RowKey  []byte
	ColKey  []byte
	Value   []byte
	Score   int64
}

type DelArgs GetArgs
type DelReply SetReply

type MGetArgs []GetArgs
type MSetArgs []SetArgs
type MDelArgs []DelArgs
type MIncrArgs []IncrArgs

type multiArgs interface {
	length() int
	toKV(kv []proto.KeyValue)
}

func (a MGetArgs) length() int {
	return len(a)
}

func (a MGetArgs) toKV(kv []proto.KeyValue) {
	for i := 0; i < len(a); i++ {
		kv[i].TableId = a[i].TableId
		kv[i].RowKey = a[i].RowKey
		kv[i].ColKey = a[i].ColKey
		kv[i].SetCas(a[i].Cas)
	}
}

func (a MSetArgs) length() int {
	return len(a)
}

func (a MSetArgs) toKV(kv []proto.KeyValue) {
	for i := 0; i < len(a); i++ {
		kv[i].TableId = a[i].TableId
		kv[i].RowKey = a[i].RowKey
		kv[i].ColKey = a[i].ColKey
		kv[i].SetCas(a[i].Cas)
		kv[i].SetScore(a[i].Score)
		kv[i].SetValue(a[i].Value)
	}
}

func (a MDelArgs) length() int {
	return len(a)
}

func (a MDelArgs) toKV(kv []proto.KeyValue) {
	for i := 0; i < len(a); i++ {
		kv[i].TableId = a[i].TableId
		kv[i].RowKey = a[i].RowKey
		kv[i].ColKey = a[i].ColKey
		kv[i].SetCas(a[i].Cas)
	}
}

func (a MIncrArgs) length() int {
	return len(a)
}

func (a MIncrArgs) toKV(kv []proto.KeyValue) {
	for i := 0; i < len(a); i++ {
		kv[i].TableId = a[i].TableId
		kv[i].RowKey = a[i].RowKey
		kv[i].ColKey = a[i].ColKey
		kv[i].SetCas(a[i].Cas)
		kv[i].SetScore(a[i].Score)
	}
}

var emptyBytes = make([]byte, 0)

func copyBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	if len(in) == 0 {
		return emptyBytes
	}
	var out = make([]byte, len(in))
	copy(out, in)
	return out
}

func (a *MGetArgs) Add(tableId uint8, rowKey, colKey []byte, cas uint32) {
	*a = append(*a, GetArgs{tableId, rowKey, colKey, cas})
}

func (a *MSetArgs) Add(tableId uint8, rowKey, colKey, value []byte, score int64, cas uint32) {
	*a = append(*a, SetArgs{tableId, rowKey, colKey, value, score, cas})
}

func (a *MDelArgs) Add(tableId uint8, rowKey, colKey []byte, cas uint32) {
	*a = append(*a, DelArgs{tableId, rowKey, colKey, cas})
}

func (a *MIncrArgs) Add(tableId uint8, rowKey, colKey []byte, score int64, cas uint32) {
	*a = append(*a, IncrArgs{tableId, rowKey, colKey, score, cas})
}

type scanContext struct {
	tableId      uint8
	rowKey       []byte
	zop          bool
	asc          bool // true: Ascending  order; false: Descending  order
	orderByScore bool // true: Score+ColKey; false: ColKey
	num          int  // Max number of scan reply records
}

type ScanKV struct {
	ColKey []byte
	Value  []byte
	Score  int64
}

type ScanReply struct {
	TableId uint8
	RowKey  []byte
	Kvs     []ScanKV
	End     bool // false: Not end yet; true: Scan to end, stop now

	ctx scanContext
}

type dumpContext struct {
	oneTable    bool   // Never change during dump
	tableId     uint8  // Never change during dump
	startUnitId uint16 // Never change during dump
	endUnitId   uint16 // Never change during dump
	lastUnitId  uint16 // The last unit ID tried to dump
	unitStart   bool   // Next dump start from new UnitId
}

type DumpKV struct {
	TableId  uint8
	ColSpace uint8
	RowKey   []byte
	ColKey   []byte
	Value    []byte
	Score    int64
}

type DumpReply struct {
	Kvs []DumpKV
	End bool // false: Not end yet; true: Has scan to end, stop now

	ctx dumpContext
}
