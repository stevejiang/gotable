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

type OneArgs struct {
	Cas uint32
	*proto.KeyValue
}

type OneReply struct {
	ErrCode uint8  // Error Code Replied
	Cas     uint32 // Only meaningful for GET/ZGET/MGET/ZMGET
	*proto.KeyValue
}

type MultiArgs struct {
	Args []OneArgs
}

type MultiReply struct {
	Reply []OneReply
}

func (a *MultiArgs) AddGetArgs(tableId uint8, rowKey, colKey []byte, cas uint32) {
	a.Args = append(a.Args,
		OneArgs{cas, &proto.KeyValue{tableId, rowKey, colKey, nil, 0}})
}

func (a *MultiArgs) AddDelArgs(tableId uint8, rowKey, colKey []byte, cas uint32) {
	a.Args = append(a.Args,
		OneArgs{cas, &proto.KeyValue{tableId, rowKey, colKey, nil, 0}})
}

func (a *MultiArgs) AddSetArgs(tableId uint8, rowKey, colKey, value []byte,
	score int64, cas uint32) {
	a.Args = append(a.Args,
		OneArgs{cas, &proto.KeyValue{tableId, rowKey, colKey, value, score}})
}

func (a *MultiArgs) AddIncrArgs(tableId uint8, rowKey, colKey []byte,
	score int64, cas uint32) {
	a.Args = append(a.Args,
		OneArgs{cas, &proto.KeyValue{tableId, rowKey, colKey, nil, score}})
}

type scanContext struct {
	zop          bool
	asc          bool // true: Ascending  order; false: Descending  order
	orderByScore bool // true: Score+ColKey; false: ColKey
	num          int  // Max number of scan reply records
}

type ScanReply struct {
	Reply []*proto.KeyValue
	End   bool // false: Not end yet; true: Scan to end, stop now

	ctx scanContext
}

type dumpContext struct {
	oneTable    bool   // Never change during dump
	dbId        uint8  // Never change during dump
	tableId     uint8  // Never change during dump
	startUnitId uint16 // Never change during dump
	endUnitId   uint16 // Never change during dump
	lastUnitId  uint16 // The last unit ID tried to dump
	lastUnitRec bool   // Is last record in lastUnitId? false: No; true: Yes
}

type DumpRecord struct {
	ColSpace uint8
	*proto.KeyValue
}

type DumpReply struct {
	Reply []DumpRecord
	End   bool // false: Not end yet; true: Has scan to end, stop now

	ctx dumpContext
}
