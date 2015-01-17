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

type OneArgs proto.KeyValue

type OneReply struct {
	ErrCode uint8 // Error code replied
	*proto.KeyValue
}

type MultiArgs struct {
	Args []OneArgs
}

type MultiReply struct {
	Reply []OneReply
}

func NewGetArgs(tableId uint8, rowKey, colKey []byte, cas uint32) *OneArgs {
	return &OneArgs{tableId, rowKey, colKey, nil, 0, cas}
}

func NewSetArgs(tableId uint8, rowKey, colKey, value []byte, score int64,
	cas uint32) *OneArgs {
	return &OneArgs{tableId, rowKey, colKey, value, score, cas}
}

func NewDelArgs(tableId uint8, rowKey, colKey []byte, cas uint32) *OneArgs {
	return &OneArgs{tableId, rowKey, colKey, nil, 0, cas}
}

func NewIncrArgs(tableId uint8, rowKey, colKey []byte, score int64,
	cas uint32) *OneArgs {
	return &OneArgs{tableId, rowKey, colKey, nil, score, cas}
}

type scanContext struct {
	asc          bool // true: Ascending  order; false: Descending  order
	orderByScore bool // true: Score+ColKey; false: ColKey
	num          int  // Max number of scan reply records
}

type ScanReply struct {
	Reply []OneReply
	End   bool // false: Not end yet; true: Scan to end, stop now

	ctx scanContext
}

// Dump Scope
const (
	ScopeTableId = iota // Dump only the specified TableId
	ScopeDbId           // Dump only the specified DbId
	ScopeFullDB         // Dump the entire database
)

type dumpContext struct {
	scope   uint8 // Dump Scope
	unitId  uint16
	dbId    uint8
	tableId uint8
}

type DumpRecord struct {
	DbId     uint8 // DbId of current record
	ColSpace uint8 // ColSpace of current record
	*proto.KeyValue
}

type DumpReply struct {
	Reply []DumpRecord
	End   bool // false: Not end yet; true: Has scan to end, stop now

	ctx dumpContext
}
