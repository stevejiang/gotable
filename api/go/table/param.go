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
