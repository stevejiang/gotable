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
	ErrCode uint8
	*proto.KeyValue
}

type MultiArgs struct {
	Args []OneArgs
}

type MultiReply struct {
	Reply []OneReply
}

type ScanArgs struct {
	Num       uint16 // Max number of scan reply records
	Direction uint8
	OneArgs
}

type ScanReply struct {
	Direction uint8
	End       uint8 // 0: has more records; 1: no more records
	Reply     []OneReply
}

type DumpArgs struct {
	Scope uint8 // 1: dump TableId; 1: dump DbId; 2: Dump full DB
	OneArgs
}

type DumpRecord struct {
	DbId     uint8
	ColSpace uint8
	*proto.KeyValue
}

type DumpReply struct {
	Scope uint8
	End   uint8 // 0: has more records; 1: no more records
	Reply []DumpRecord
}
