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
	"errors"
	"github.com/stevejiang/gotable/api/go/table/proto"
	"github.com/stevejiang/gotable/ctrl"
	"strconv"
)

// Connection Context to GoTable server.
// It's safe to use in multiple goroutines.
type Context struct {
	cli  *Client
	dbId uint8
}

type Call struct {
	Done  chan *Call  // Reply channel
	ctx   interface{} // Request context
	err   error
	pkg   []byte
	seq   uint64
	cmd   uint8
	ready bool // Ready to invoke Reply?
}

// Get the underling connection Client of the Context.
func (c *Context) Client() *Client {
	return c.cli
}

func (c *Context) Auth(password string) error {
	if c.cli.isAuthorized(c.dbId) {
		return nil
	}

	var args = OneArgs{0, &proto.KeyValue{0, []byte(password), nil, nil, 0}}
	call, err := c.goOneOp(false, args, proto.CmdAuth, nil)
	if err != nil {
		return err
	}

	r, err := (<-call.Done).Reply()
	if err != nil {
		return err
	}

	t := r.(*OneReply)
	if t.ErrCode != 0 {
		return ErrAuthFailed
	}
	return nil
}

func (c *Context) Ping() error {
	call, err := c.GoPing(nil)
	if err != nil {
		return err
	}

	_, err = (<-call.Done).Reply()
	return err
}

func (c *Context) Get(tableId uint8, rowKey, colKey []byte,
	cas uint32) (*OneReply, error) {
	call, err := c.GoGet(tableId, rowKey, colKey, cas, nil)
	return doOneReply(call, err)
}

func (c *Context) ZGet(tableId uint8, rowKey, colKey []byte,
	cas uint32) (*OneReply, error) {
	call, err := c.GoZGet(tableId, rowKey, colKey, cas, nil)
	return doOneReply(call, err)
}

func (c *Context) Set(tableId uint8, rowKey, colKey, value []byte, score int64,
	cas uint32) (*OneReply, error) {
	call, err := c.GoSet(tableId, rowKey, colKey, value, score, cas, nil)
	return doOneReply(call, err)
}

func (c *Context) ZSet(tableId uint8, rowKey, colKey, value []byte, score int64,
	cas uint32) (*OneReply, error) {
	call, err := c.GoZSet(tableId, rowKey, colKey, value, score, cas, nil)
	return doOneReply(call, err)
}

func (c *Context) Del(tableId uint8, rowKey, colKey []byte,
	cas uint32) (*OneReply, error) {
	call, err := c.GoDel(tableId, rowKey, colKey, cas, nil)
	return doOneReply(call, err)
}

func (c *Context) ZDel(tableId uint8, rowKey, colKey []byte,
	cas uint32) (*OneReply, error) {
	call, err := c.GoZDel(tableId, rowKey, colKey, cas, nil)
	return doOneReply(call, err)
}

func (c *Context) Incr(tableId uint8, rowKey, colKey []byte, score int64,
	cas uint32) (*OneReply, error) {
	call, err := c.GoIncr(tableId, rowKey, colKey, score, cas, nil)
	return doOneReply(call, err)
}

func (c *Context) ZIncr(tableId uint8, rowKey, colKey []byte, score int64,
	cas uint32) (*OneReply, error) {
	call, err := c.GoZIncr(tableId, rowKey, colKey, score, cas, nil)
	return doOneReply(call, err)
}

func (c *Context) MGet(args *MultiArgs) (*MultiReply, error) {
	call, err := c.GoMGet(args, nil)
	return doMultiReply(call, err)
}

func (c *Context) ZmGet(args *MultiArgs) (*MultiReply, error) {
	call, err := c.GoZmGet(args, nil)
	return doMultiReply(call, err)
}

func (c *Context) MSet(args *MultiArgs) (*MultiReply, error) {
	call, err := c.GoMSet(args, nil)
	return doMultiReply(call, err)
}

func (c *Context) ZmSet(args *MultiArgs) (*MultiReply, error) {
	call, err := c.GoZmSet(args, nil)
	return doMultiReply(call, err)
}

func (c *Context) MDel(args *MultiArgs) (*MultiReply, error) {
	call, err := c.GoMDel(args, nil)
	return doMultiReply(call, err)
}

func (c *Context) ZmDel(args *MultiArgs) (*MultiReply, error) {
	call, err := c.GoZmDel(args, nil)
	return doMultiReply(call, err)
}

func (c *Context) MIncr(args *MultiArgs) (*MultiReply, error) {
	call, err := c.GoMIncr(args, nil)
	return doMultiReply(call, err)
}

func (c *Context) ZmIncr(args *MultiArgs) (*MultiReply, error) {
	call, err := c.GoZmIncr(args, nil)
	return doMultiReply(call, err)
}

func (c *Context) Scan(tableId uint8, rowKey, colKey []byte,
	asc bool, num int) (*ScanReply, error) {
	call, err := c.GoScan(tableId, rowKey, colKey, asc, num, nil)
	return doScanReply(call, err)
}

func (c *Context) ScanStart(tableId uint8, rowKey []byte,
	asc bool, num int) (*ScanReply, error) {
	call, err := c.GoScanStart(tableId, rowKey, asc, num, nil)
	return doScanReply(call, err)
}

func (c *Context) ZScan(tableId uint8, rowKey, colKey []byte, score int64,
	asc, orderByScore bool, num int) (*ScanReply, error) {
	call, err := c.GoZScan(tableId, rowKey, colKey, score, asc, orderByScore,
		num, nil)
	return doScanReply(call, err)
}

func (c *Context) ZScanStart(tableId uint8, rowKey []byte,
	asc, orderByScore bool, num int) (*ScanReply, error) {
	call, err := c.GoZScanStart(tableId, rowKey, asc, orderByScore, num, nil)
	return doScanReply(call, err)
}

// Scan/ZScan more records.
func (c *Context) ScanMore(last *ScanReply) (*ScanReply, error) {
	if last.End || len(last.Reply) == 0 {
		return nil, ErrScanEnded
	}
	var r = last.Reply[len(last.Reply)-1]
	var call *Call
	var err error
	if last.ctx.zop {
		call, err = c.GoZScan(r.TableId, r.RowKey, r.ColKey, r.Score,
			last.ctx.asc, last.ctx.orderByScore, last.ctx.num, nil)
	} else {
		call, err = c.GoScan(r.TableId, r.RowKey, r.ColKey,
			last.ctx.asc, last.ctx.num, nil)
	}
	return doScanReply(call, err)
}

// Dump start from the pivot record.
// If oneTable is true, only dump the selected table.
// If oneTable is false, dump current DB(dbId).
// The pivot record itself is excluded from the reply.
func (c *Context) Dump(oneTable bool, tableId, colSpace uint8,
	rowKey, colKey []byte, score int64,
	startUnitId, endUnitId uint16) (*DumpReply, error) {
	call, err := c.goDump(oneTable, tableId, colSpace, rowKey, colKey,
		score, startUnitId, endUnitId, nil)
	if err != nil {
		return nil, err
	}

	r, err := (<-call.Done).Reply()
	if err != nil {
		return nil, err
	}

	var t = r.(*DumpReply)
	if t.End || len(t.Reply) > 0 {
		return t, nil
	}

	return c.DumpMore(t)
}

// Dump the selected Table.
func (c *Context) DumpTable(tableId uint8) (*DumpReply, error) {
	return c.Dump(true, tableId, 0, nil, nil, 0, 0, 65535)
}

// Dump current DB(dbId in Context).
func (c *Context) DumpDB() (*DumpReply, error) {
	return c.Dump(false, 0, 0, nil, nil, 0, 0, 65535)
}

// Dump more records.
func (c *Context) DumpMore(last *DumpReply) (*DumpReply, error) {
	if last.End {
		return nil, ErrScanEnded
	}

	var t = last
	for {
		var rec *DumpRecord
		var lastUnitId = t.ctx.lastUnitId
		if t.ctx.unitStart {
			lastUnitId += 1
			rec = &DumpRecord{0, &proto.KeyValue{0, nil, nil, nil, 0}}
			if t.ctx.oneTable {
				rec.TableId = t.ctx.tableId
			}
		} else {
			rec = &t.Reply[len(t.Reply)-1]
		}

		call, err := c.goDump(t.ctx.oneTable, rec.TableId, rec.ColSpace,
			rec.RowKey, rec.ColKey, rec.Score, lastUnitId, t.ctx.endUnitId, nil)
		if err != nil {
			return nil, err
		}

		r, err := (<-call.Done).Reply()
		if err != nil {
			return nil, err
		}

		t = r.(*DumpReply)
		if t.End || len(t.Reply) > 0 {
			return t, nil
		}
	}

	return nil, ErrScanEnded
}

// Get, Set, Del, Incr, ZGet, ZSet, ZDel, ZIncr
func (c *Context) goOneOp(zop bool, args OneArgs, cmd uint8,
	done chan *Call) (*Call, error) {
	call := c.cli.newCall(cmd, done)
	if call.err != nil {
		return call, call.err
	}

	var p proto.PkgOneOp
	p.Seq = call.seq
	p.DbId = c.dbId
	p.Cmd = call.cmd
	p.TableId = args.TableId
	p.RowKey = args.RowKey
	p.ColKey = args.ColKey

	p.SetCas(args.Cas)
	p.SetScore(args.Score)
	p.SetValue(args.Value)

	// ZGet, ZSet, ZDel, ZIncr
	if zop {
		p.PkgFlag |= proto.FlagZop
	}

	call.pkg = make([]byte, p.Length())
	_, err := p.Encode(call.pkg)
	if err != nil {
		c.cli.errCall(call, err)
		return call, err
	}

	// put request pkg to sending channel
	c.cli.sending <- call

	return call, nil
}

func (c *Context) GoPing(done chan *Call) (*Call, error) {
	return c.goOneOp(false, OneArgs{0, &proto.KeyValue{}}, proto.CmdPing, done)
}

func (c *Context) GoGet(tableId uint8, rowKey, colKey []byte, cas uint32,
	done chan *Call) (*Call, error) {
	var args = OneArgs{cas, &proto.KeyValue{tableId, rowKey, colKey, nil, 0}}
	return c.goOneOp(false, args, proto.CmdGet, done)
}

func (c *Context) GoZGet(tableId uint8, rowKey, colKey []byte, cas uint32,
	done chan *Call) (*Call, error) {
	var args = OneArgs{cas, &proto.KeyValue{tableId, rowKey, colKey, nil, 0}}
	return c.goOneOp(true, args, proto.CmdGet, done)
}

func (c *Context) GoSet(tableId uint8, rowKey, colKey, value []byte, score int64,
	cas uint32, done chan *Call) (*Call, error) {
	var args = OneArgs{cas, &proto.KeyValue{tableId, rowKey, colKey, value, score}}
	return c.goOneOp(false, args, proto.CmdSet, done)
}

func (c *Context) GoZSet(tableId uint8, rowKey, colKey, value []byte, score int64,
	cas uint32, done chan *Call) (*Call, error) {
	var args = OneArgs{cas, &proto.KeyValue{tableId, rowKey, colKey, value, score}}
	return c.goOneOp(true, args, proto.CmdSet, done)
}

func (c *Context) GoDel(tableId uint8, rowKey, colKey []byte,
	cas uint32, done chan *Call) (*Call, error) {
	var args = OneArgs{cas, &proto.KeyValue{tableId, rowKey, colKey, nil, 0}}
	return c.goOneOp(false, args, proto.CmdDel, done)
}

func (c *Context) GoZDel(tableId uint8, rowKey, colKey []byte,
	cas uint32, done chan *Call) (*Call, error) {
	var args = OneArgs{cas, &proto.KeyValue{tableId, rowKey, colKey, nil, 0}}
	return c.goOneOp(true, args, proto.CmdDel, done)
}

func (c *Context) GoIncr(tableId uint8, rowKey, colKey []byte, score int64,
	cas uint32, done chan *Call) (*Call, error) {
	var args = OneArgs{cas, &proto.KeyValue{tableId, rowKey, colKey, nil, score}}
	return c.goOneOp(false, args, proto.CmdIncr, done)
}

func (c *Context) GoZIncr(tableId uint8, rowKey, colKey []byte, score int64,
	cas uint32, done chan *Call) (*Call, error) {
	var args = OneArgs{cas, &proto.KeyValue{tableId, rowKey, colKey, nil, score}}
	return c.goOneOp(true, args, proto.CmdIncr, done)
}

// MGet, MSet, MDel, MIncr, ZMGet, ZMSet, ZMDel, ZMIncr
func (c *Context) goMultiOp(zop bool, args *MultiArgs, cmd uint8,
	done chan *Call) (*Call, error) {
	call := c.cli.newCall(cmd, done)
	if call.err != nil {
		return call, call.err
	}

	var p proto.PkgMultiOp
	p.Seq = call.seq
	p.DbId = c.dbId
	p.Cmd = call.cmd

	// ZMGet, ZMSet, ZMDel, ZMIncr
	if zop {
		p.PkgFlag |= proto.FlagZop
	}

	p.Kvs = make([]proto.KeyValueCtrl, len(args.Args))
	for i := 0; i < len(args.Args); i++ {
		p.Kvs[i].TableId = args.Args[i].TableId
		p.Kvs[i].RowKey = args.Args[i].RowKey
		p.Kvs[i].ColKey = args.Args[i].ColKey

		p.Kvs[i].SetCas(args.Args[i].Cas)
		p.Kvs[i].SetScore(args.Args[i].Score)
		p.Kvs[i].SetValue(args.Args[i].Value)
	}

	call.pkg = make([]byte, p.Length())
	_, err := p.Encode(call.pkg)
	if err != nil {
		c.cli.errCall(call, err)
		return call, err
	}

	c.cli.sending <- call

	return call, nil
}

func (c *Context) GoMGet(args *MultiArgs, done chan *Call) (*Call, error) {
	return c.goMultiOp(false, args, proto.CmdMGet, done)
}

func (c *Context) GoZmGet(args *MultiArgs, done chan *Call) (*Call, error) {
	return c.goMultiOp(true, args, proto.CmdMGet, done)
}

func (c *Context) GoMSet(args *MultiArgs, done chan *Call) (*Call, error) {
	return c.goMultiOp(false, args, proto.CmdMSet, done)
}

func (c *Context) GoZmSet(args *MultiArgs, done chan *Call) (*Call, error) {
	return c.goMultiOp(true, args, proto.CmdMSet, done)
}

func (c *Context) GoMDel(args *MultiArgs, done chan *Call) (*Call, error) {
	return c.goMultiOp(false, args, proto.CmdMDel, done)
}

func (c *Context) GoZmDel(args *MultiArgs, done chan *Call) (*Call, error) {
	return c.goMultiOp(true, args, proto.CmdMDel, done)
}

func (c *Context) GoMIncr(args *MultiArgs, done chan *Call) (*Call, error) {
	return c.goMultiOp(false, args, proto.CmdMIncr, done)
}

func (c *Context) GoZmIncr(args *MultiArgs, done chan *Call) (*Call, error) {
	return c.goMultiOp(true, args, proto.CmdMIncr, done)
}

func (c *Context) goScan(zop bool, tableId uint8, rowKey, colKey []byte,
	score int64, start, asc, orderByScore bool, num int,
	done chan *Call) (*Call, error) {
	call := c.cli.newCall(proto.CmdScan, done)
	if call.err != nil {
		return call, call.err
	}

	if num < 1 {
		c.cli.errCall(call, ErrInvScanNum)
		return call, call.err
	}

	var p proto.PkgScanReq
	p.Seq = call.seq
	p.DbId = c.dbId
	p.Cmd = call.cmd
	if asc {
		p.PkgFlag |= proto.FlagAscending
	}
	if start {
		p.PkgFlag |= proto.FlagStart
	}
	p.Num = uint16(num)
	p.TableId = tableId
	p.RowKey = rowKey
	p.ColKey = colKey

	// ZScan
	if zop {
		p.PkgFlag |= proto.FlagZop
		p.SetScore(score)
		if orderByScore {
			p.SetColSpace(proto.ColSpaceScore1)
		} else {
			p.SetColSpace(proto.ColSpaceScore2)
		}
	}

	call.pkg = make([]byte, p.Length())
	_, err := p.Encode(call.pkg)
	if err != nil {
		c.cli.errCall(call, err)
		return call, err
	}

	call.ctx = scanContext{zop, asc, orderByScore, num}
	c.cli.sending <- call

	return call, nil
}

func (c *Context) GoScan(tableId uint8, rowKey, colKey []byte,
	asc bool, num int, done chan *Call) (*Call, error) {
	return c.goScan(false, tableId, rowKey, colKey, 0,
		false, asc, false, num, done)
}

func (c *Context) GoScanStart(tableId uint8, rowKey []byte,
	asc bool, num int, done chan *Call) (*Call, error) {
	return c.goScan(false, tableId, rowKey, nil, 0,
		true, asc, false, num, done)
}

func (c *Context) GoZScan(tableId uint8, rowKey, colKey []byte, score int64,
	asc, orderByScore bool, num int, done chan *Call) (*Call, error) {
	return c.goScan(true, tableId, rowKey, colKey, score,
		false, asc, orderByScore, num, done)
}

func (c *Context) GoZScanStart(tableId uint8, rowKey []byte,
	asc, orderByScore bool, num int, done chan *Call) (*Call, error) {
	return c.goScan(true, tableId, rowKey, nil, 0,
		true, asc, orderByScore, num, done)
}

func (c *Context) goDump(oneTable bool, tableId, colSpace uint8,
	rowKey, colKey []byte, score int64, startUnitId, endUnitId uint16,
	done chan *Call) (*Call, error) {
	call := c.cli.newCall(proto.CmdDump, done)
	if call.err != nil {
		return call, call.err
	}

	var p proto.PkgDumpReq
	p.Seq = call.seq
	p.DbId = c.dbId
	p.Cmd = call.cmd
	if oneTable {
		p.PkgFlag |= proto.FlagOneTable
	}
	p.StartUnitId = startUnitId
	p.EndUnitId = endUnitId
	p.TableId = tableId
	p.RowKey = rowKey
	p.ColKey = colKey
	p.SetColSpace(colSpace)
	p.SetScore(score)

	call.pkg = make([]byte, p.Length())
	_, err := p.Encode(call.pkg)
	if err != nil {
		c.cli.errCall(call, err)
		return call, err
	}

	call.ctx = dumpContext{oneTable, c.dbId, tableId,
		startUnitId, endUnitId, startUnitId, false}

	c.cli.sending <- call

	return call, nil
}

// Internal control command.
// SlaveOf can change the replication settings of a slave on the fly.
func (c *Context) SlaveOf(mis []ctrl.MasterInfo) error {
	call := c.cli.newCall(proto.CmdSlaveOf, nil)
	if call.err != nil {
		return call.err
	}

	var p ctrl.PkgSlaveOf
	p.Mis = mis

	pkg, err := ctrl.NewEncoder().Encode(call.cmd, c.dbId, call.seq, &p)
	if err != nil {
		c.cli.errCall(call, err)
		return err
	}

	call.pkg = pkg
	c.cli.sending <- call

	r, err := (<-call.Done).Reply()
	if err != nil {
		return err
	}

	t := r.(*ctrl.PkgSlaveOf)
	if t.ErrMsg != "" {
		return errors.New(t.ErrMsg)
	}
	return nil
}

func doOneReply(call *Call, err error) (*OneReply, error) {
	if err != nil {
		return nil, err
	}

	r, err := (<-call.Done).Reply()
	if err != nil {
		return nil, err
	}
	return r.(*OneReply), nil
}

func doMultiReply(call *Call, err error) (*MultiReply, error) {
	if err != nil {
		return nil, err
	}

	r, err := (<-call.Done).Reply()
	if err != nil {
		return nil, err
	}
	return r.(*MultiReply), nil
}

func doScanReply(call *Call, err error) (*ScanReply, error) {
	if err != nil {
		return nil, err
	}

	r, err := (<-call.Done).Reply()
	if err != nil {
		return nil, err
	}
	return r.(*ScanReply), nil
}

func (call *Call) Reply() (interface{}, error) {
	if call.err != nil {
		return nil, call.err
	}

	if !call.ready {
		return nil, ErrCallNotReady
	}

	switch call.cmd {
	case proto.CmdAuth:
		fallthrough
	case proto.CmdPing:
		fallthrough
	case proto.CmdIncr:
		fallthrough
	case proto.CmdDel:
		fallthrough
	case proto.CmdSet:
		fallthrough
	case proto.CmdGet:
		var p proto.PkgOneOp
		_, err := p.Decode(call.pkg)
		if err != nil {
			call.err = err
			return nil, call.err
		}
		return &OneReply{p.ErrCode, p.Cas, &p.KeyValue}, nil

	case proto.CmdMIncr:
		fallthrough
	case proto.CmdMDel:
		fallthrough
	case proto.CmdMSet:
		fallthrough
	case proto.CmdMGet:
		var p proto.PkgMultiOp
		_, err := p.Decode(call.pkg)
		if err != nil {
			call.err = err
			return nil, call.err
		}

		if p.ErrCode != 0 {
			return nil, errors.New("error code " + strconv.Itoa(int(p.ErrCode)))
		}

		var r MultiReply
		r.Reply = make([]OneReply, len(p.Kvs))
		for i := 0; i < len(p.Kvs); i++ {
			r.Reply[i].ErrCode = p.Kvs[i].ErrCode
			r.Reply[i].KeyValue = &p.Kvs[i].KeyValue
		}
		return &r, nil

	case proto.CmdScan:
		var p proto.PkgScanResp
		_, err := p.Decode(call.pkg)
		if err != nil {
			call.err = err
			return nil, call.err
		}

		if p.ErrCode != 0 {
			return nil, errors.New("error code " + strconv.Itoa(int(p.ErrCode)))
		}

		var r ScanReply
		r.ctx = call.ctx.(scanContext)
		r.End = (p.PkgFlag&proto.FlagEnd != 0)
		r.Reply = make([]*proto.KeyValue, len(p.Kvs))
		for i := 0; i < len(p.Kvs); i++ {
			r.Reply[i] = &p.Kvs[i].KeyValue
		}
		return &r, nil

	case proto.CmdDump:
		var p proto.PkgDumpResp
		_, err := p.Decode(call.pkg)
		if err != nil {
			call.err = err
			return nil, call.err
		}

		if p.ErrCode != 0 {
			return nil, errors.New("error code " + strconv.Itoa(int(p.ErrCode)))
		}

		var r DumpReply
		r.ctx = call.ctx.(dumpContext)
		r.ctx.lastUnitId = p.LastUnitId
		r.ctx.unitStart = (p.PkgFlag&proto.FlagUnitStart != 0)
		r.End = (p.PkgFlag&proto.FlagEnd != 0)
		r.Reply = make([]DumpRecord, len(p.Kvs))
		for i := 0; i < len(p.Kvs); i++ {
			r.Reply[i].ColSpace = p.Kvs[i].ColSpace
			r.Reply[i].KeyValue = &p.Kvs[i].KeyValue
		}
		return &r, nil

	case proto.CmdSlaveOf:
		var p ctrl.PkgSlaveOf
		err := ctrl.NewDecoder().Decode(call.pkg, nil, &p)
		if err != nil {
			call.err = err
			return nil, call.err
		}
		return &p, nil
	}

	return nil, ErrUnknownCmd
}
