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

	call, err := c.goOneOp(false, proto.CmdAuth, 0, []byte(password), nil, nil, 0, 0, nil)
	if err != nil {
		return err
	}

	_, err = (<-call.Done).Reply()
	return err
}

func (c *Context) Ping() error {
	call, err := c.GoPing(nil)
	if err != nil {
		return err
	}

	_, err = (<-call.Done).Reply()
	return err
}

// Get value&score of the key in default column space.
// Parameter cas is Compare And Switch, 2 means read data on master and return
// a new cas, 1 means read data on master machine but without a new cas, 0 means
// read data on any machine without a new cas. On cluter mode, routing to master
// machine is automatically, but on a normal master/slaver mode it should be done
// manually. If cas 1&2 sent to a slaver machine, error will be returned.
// Return value nil means key not exist.
func (c *Context) Get(tableId uint8, rowKey, colKey []byte, cas uint32) (
	value []byte, score int64, casReply uint32, err error) {
	return replyGet(c.GoGet(tableId, rowKey, colKey, cas, nil))
}

// Get value&score of the key in "Z" sorted score column space.
// Request and return parameters have the same meaning as the Get API.
func (c *Context) ZGet(tableId uint8, rowKey, colKey []byte, cas uint32) (
	value []byte, score int64, newCas uint32, err error) {
	return replyGet(c.GoZGet(tableId, rowKey, colKey, cas, nil))
}

func (c *Context) Set(tableId uint8, rowKey, colKey, value []byte, score int64,
	cas uint32) error {
	return replySet(c.GoSet(tableId, rowKey, colKey, value, score, cas, nil))
}

func (c *Context) ZSet(tableId uint8, rowKey, colKey, value []byte, score int64,
	cas uint32) error {
	return replySet(c.GoZSet(tableId, rowKey, colKey, value, score, cas, nil))
}

func (c *Context) Del(tableId uint8, rowKey, colKey []byte,
	cas uint32) error {
	return replySet(c.GoDel(tableId, rowKey, colKey, cas, nil))
}

func (c *Context) ZDel(tableId uint8, rowKey, colKey []byte,
	cas uint32) error {
	return replySet(c.GoZDel(tableId, rowKey, colKey, cas, nil))
}

func (c *Context) Incr(tableId uint8, rowKey, colKey []byte, score int64,
	cas uint32) (newValue []byte, newScore int64, err error) {
	return replyIncr(c.GoIncr(tableId, rowKey, colKey, score, cas, nil))
}

func (c *Context) ZIncr(tableId uint8, rowKey, colKey []byte, score int64,
	cas uint32) (newValue []byte, newScore int64, err error) {
	return replyIncr(c.GoZIncr(tableId, rowKey, colKey, score, cas, nil))
}

func (c *Context) MGet(args MGetArgs) ([]GetReply, error) {
	call, err := c.GoMGet(args, nil)
	if err != nil {
		return nil, err
	}

	r, err := (<-call.Done).Reply()
	if err != nil {
		return nil, err
	}
	return r.([]GetReply), nil
}

func (c *Context) ZmGet(args MGetArgs) ([]GetReply, error) {
	call, err := c.GoZmGet(args, nil)
	if err != nil {
		return nil, err
	}

	r, err := (<-call.Done).Reply()
	if err != nil {
		return nil, err
	}
	return r.([]GetReply), nil
}

func (c *Context) MSet(args MSetArgs) ([]SetReply, error) {
	call, err := c.GoMSet(args, nil)
	if err != nil {
		return nil, err
	}

	r, err := (<-call.Done).Reply()
	if err != nil {
		return nil, err
	}
	return r.([]SetReply), nil
}

func (c *Context) ZmSet(args MSetArgs) ([]SetReply, error) {
	call, err := c.GoZmSet(args, nil)
	if err != nil {
		return nil, err
	}

	r, err := (<-call.Done).Reply()
	if err != nil {
		return nil, err
	}
	return r.([]SetReply), nil
}

func (c *Context) MDel(args MDelArgs) ([]DelReply, error) {
	call, err := c.GoMDel(args, nil)
	if err != nil {
		return nil, err
	}

	r, err := (<-call.Done).Reply()
	if err != nil {
		return nil, err
	}
	return r.([]DelReply), nil
}

func (c *Context) ZmDel(args MDelArgs) ([]DelReply, error) {
	call, err := c.GoZmDel(args, nil)
	if err != nil {
		return nil, err
	}

	r, err := (<-call.Done).Reply()
	if err != nil {
		return nil, err
	}
	return r.([]DelReply), nil
}

func (c *Context) MIncr(args MIncrArgs) ([]IncrReply, error) {
	call, err := c.GoMIncr(args, nil)
	if err != nil {
		return nil, err
	}

	r, err := (<-call.Done).Reply()
	if err != nil {
		return nil, err
	}
	return r.([]IncrReply), nil
}

func (c *Context) ZmIncr(args MIncrArgs) ([]IncrReply, error) {
	call, err := c.GoZmIncr(args, nil)
	if err != nil {
		return nil, err
	}

	r, err := (<-call.Done).Reply()
	if err != nil {
		return nil, err
	}
	return r.([]IncrReply), nil
}

func (c *Context) Scan(tableId uint8, rowKey, colKey []byte,
	asc bool, num int) (ScanReply, error) {
	return replyScan(c.GoScan(tableId, rowKey, colKey, asc, num, nil))
}

func (c *Context) ScanStart(tableId uint8, rowKey []byte,
	asc bool, num int) (ScanReply, error) {
	return replyScan(c.GoScanStart(tableId, rowKey, asc, num, nil))
}

func (c *Context) ZScan(tableId uint8, rowKey, colKey []byte, score int64,
	asc, orderByScore bool, num int) (ScanReply, error) {
	return replyScan(c.GoZScan(tableId, rowKey, colKey, score,
		asc, orderByScore, num, nil))
}

func (c *Context) ZScanStart(tableId uint8, rowKey []byte,
	asc, orderByScore bool, num int) (ScanReply, error) {
	return replyScan(c.GoZScanStart(tableId, rowKey,
		asc, orderByScore, num, nil))
}

// Scan/ZScan more records.
func (c *Context) ScanMore(last ScanReply) (ScanReply, error) {
	if last.End || len(last.Kvs) == 0 {
		return ScanReply{}, ErrScanEnded
	}
	var r = last.Kvs[len(last.Kvs)-1]
	var call *Call
	var err error
	if last.ctx.zop {
		call, err = c.GoZScan(r.TableId, r.RowKey, r.ColKey, r.Score,
			last.ctx.asc, last.ctx.orderByScore, last.ctx.num, nil)
	} else {
		call, err = c.GoScan(r.TableId, r.RowKey, r.ColKey,
			last.ctx.asc, last.ctx.num, nil)
	}
	return replyScan(call, err)
}

// Dump start from the pivot record.
// If oneTable is true, only dump the selected table.
// If oneTable is false, dump current DB(dbId).
// The pivot record itself is excluded from the reply.
func (c *Context) Dump(oneTable bool, tableId, colSpace uint8,
	rowKey, colKey []byte, score int64,
	startUnitId, endUnitId uint16) (DumpReply, error) {
	call, err := c.goDump(oneTable, tableId, colSpace, rowKey, colKey,
		score, startUnitId, endUnitId, nil)
	if err != nil {
		return DumpReply{}, err
	}

	r, err := (<-call.Done).Reply()
	if err != nil {
		return DumpReply{}, err
	}

	var t = r.(DumpReply)
	if t.End || len(t.Kvs) > 0 {
		return t, nil
	}

	return c.DumpMore(t)
}

// Dump the selected Table.
func (c *Context) DumpTable(tableId uint8) (DumpReply, error) {
	return c.Dump(true, tableId, 0, nil, nil, 0, 0, 65535)
}

// Dump current DB(dbId in Context).
func (c *Context) DumpDB() (DumpReply, error) {
	return c.Dump(false, 0, 0, nil, nil, 0, 0, 65535)
}

// Dump more records.
func (c *Context) DumpMore(last DumpReply) (DumpReply, error) {
	if last.End {
		return DumpReply{}, ErrScanEnded
	}

	var t = last
	for {
		var rec DumpKV
		var lastUnitId = t.ctx.lastUnitId
		if t.ctx.unitStart {
			lastUnitId += 1
			if t.ctx.oneTable {
				rec.TableId = t.ctx.tableId
			}
		} else {
			rec = t.Kvs[len(t.Kvs)-1]
		}

		call, err := c.goDump(t.ctx.oneTable, rec.TableId, rec.ColSpace,
			rec.RowKey, rec.ColKey, rec.Score, lastUnitId, t.ctx.endUnitId, nil)
		if err != nil {
			return DumpReply{}, err
		}

		r, err := (<-call.Done).Reply()
		if err != nil {
			return DumpReply{}, err
		}

		t = r.(DumpReply)
		if t.End || len(t.Kvs) > 0 {
			return t, nil
		}
	}

	return DumpReply{}, ErrScanEnded
}

// Get, Set, Del, Incr, ZGet, ZSet, ZDel, ZIncr
func (c *Context) goOneOp(zop bool, cmd, tableId uint8,
	rowKey, colKey, value []byte, score int64, cas uint32,
	done chan *Call) (*Call, error) {
	call := c.cli.newCall(cmd, done)
	if call.err != nil {
		return call, call.err
	}

	var p proto.PkgOneOp
	p.Seq = call.seq
	p.DbId = c.dbId
	p.Cmd = call.cmd
	p.TableId = tableId
	p.RowKey = rowKey
	p.ColKey = colKey

	p.SetCas(cas)
	p.SetScore(score)
	p.SetValue(value)

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
	return c.goOneOp(false, proto.CmdPing, 0, nil, nil, nil, 0, 0, done)
}

func (c *Context) GoGet(tableId uint8, rowKey, colKey []byte, cas uint32,
	done chan *Call) (*Call, error) {
	return c.goOneOp(false, proto.CmdGet, tableId, rowKey, colKey, nil, 0, cas, done)
}

func (c *Context) GoZGet(tableId uint8, rowKey, colKey []byte, cas uint32,
	done chan *Call) (*Call, error) {
	return c.goOneOp(true, proto.CmdGet, tableId, rowKey, colKey, nil, 0, cas, done)
}

func (c *Context) GoSet(tableId uint8, rowKey, colKey, value []byte, score int64,
	cas uint32, done chan *Call) (*Call, error) {
	return c.goOneOp(false, proto.CmdSet, tableId, rowKey, colKey, value, score, cas, done)
}

func (c *Context) GoZSet(tableId uint8, rowKey, colKey, value []byte, score int64,
	cas uint32, done chan *Call) (*Call, error) {
	return c.goOneOp(true, proto.CmdSet, tableId, rowKey, colKey, value, score, cas, done)
}

func (c *Context) GoDel(tableId uint8, rowKey, colKey []byte,
	cas uint32, done chan *Call) (*Call, error) {
	return c.goOneOp(false, proto.CmdDel, tableId, rowKey, colKey, nil, 0, cas, done)
}

func (c *Context) GoZDel(tableId uint8, rowKey, colKey []byte,
	cas uint32, done chan *Call) (*Call, error) {
	return c.goOneOp(true, proto.CmdDel, tableId, rowKey, colKey, nil, 0, cas, done)
}

func (c *Context) GoIncr(tableId uint8, rowKey, colKey []byte, score int64,
	cas uint32, done chan *Call) (*Call, error) {
	return c.goOneOp(false, proto.CmdIncr, tableId, rowKey, colKey, nil, score, cas, done)
}

func (c *Context) GoZIncr(tableId uint8, rowKey, colKey []byte, score int64,
	cas uint32, done chan *Call) (*Call, error) {
	return c.goOneOp(true, proto.CmdIncr, tableId, rowKey, colKey, nil, score, cas, done)
}

// MGet, MSet, MDel, MIncr, ZMGet, ZMSet, ZMDel, ZMIncr
func (c *Context) goMultiOp(zop bool, args multiArgs, cmd uint8,
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

	p.Kvs = make([]proto.KeyValue, args.length())
	args.toKV(p.Kvs)

	call.pkg = make([]byte, p.Length())
	_, err := p.Encode(call.pkg)
	if err != nil {
		c.cli.errCall(call, err)
		return call, err
	}

	c.cli.sending <- call

	return call, nil
}

func (c *Context) GoMGet(args []GetArgs, done chan *Call) (*Call, error) {
	return c.goMultiOp(false, MGetArgs(args), proto.CmdMGet, done)
}

func (c *Context) GoZmGet(args []GetArgs, done chan *Call) (*Call, error) {
	return c.goMultiOp(true, MGetArgs(args), proto.CmdMGet, done)
}

func (c *Context) GoMSet(args []SetArgs, done chan *Call) (*Call, error) {
	return c.goMultiOp(false, MSetArgs(args), proto.CmdMSet, done)
}

func (c *Context) GoZmSet(args []SetArgs, done chan *Call) (*Call, error) {
	return c.goMultiOp(true, MSetArgs(args), proto.CmdMSet, done)
}

func (c *Context) GoMDel(args []DelArgs, done chan *Call) (*Call, error) {
	return c.goMultiOp(false, MDelArgs(args), proto.CmdMDel, done)
}

func (c *Context) GoZmDel(args []DelArgs, done chan *Call) (*Call, error) {
	return c.goMultiOp(true, MDelArgs(args), proto.CmdMDel, done)
}

func (c *Context) GoMIncr(args []IncrArgs, done chan *Call) (*Call, error) {
	return c.goMultiOp(false, MIncrArgs(args), proto.CmdMIncr, done)
}

func (c *Context) GoZmIncr(args []IncrArgs, done chan *Call) (*Call, error) {
	return c.goMultiOp(true, MIncrArgs(args), proto.CmdMIncr, done)
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

	call.ctx = dumpContext{oneTable, tableId,
		startUnitId, endUnitId, startUnitId, false}

	c.cli.sending <- call

	return call, nil
}

// Internal control command.
// SlaveOf can change the replication settings of a slave on the fly.
func (c *Context) SlaveOf(host string) error {
	call := c.cli.newCall(proto.CmdSlaveOf, nil)
	if call.err != nil {
		return call.err
	}

	var p ctrl.PkgSlaveOf
	p.ClientReq = true
	p.MasterAddr = host

	pkg, err := ctrl.Encode(call.cmd, c.dbId, call.seq, &p)
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

// Internal control command.
// Migrate moves one unit data to another server on the fly.
func (c *Context) Migrate(host string, unitId uint16) error {
	call := c.cli.newCall(proto.CmdMigrate, nil)
	if call.err != nil {
		return call.err
	}

	var p ctrl.PkgMigrate
	p.ClientReq = true
	p.MasterAddr = host
	p.UnitId = unitId

	pkg, err := ctrl.Encode(call.cmd, c.dbId, call.seq, &p)
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

	t := r.(*ctrl.PkgMigrate)
	if t.ErrMsg != "" {
		return errors.New(t.ErrMsg)
	}
	return nil
}

// Internal control command.
// SlaverStatus reads migration/slaver status.
func (c *Context) SlaverStatus(migration bool, unitId uint16) (int, error) {
	call := c.cli.newCall(proto.CmdSlaverSt, nil)
	if call.err != nil {
		return ctrl.NotSlaver, call.err
	}

	var p ctrl.PkgSlaverStatus
	p.Migration = migration
	p.UnitId = unitId

	pkg, err := ctrl.Encode(call.cmd, c.dbId, call.seq, &p)
	if err != nil {
		c.cli.errCall(call, err)
		return ctrl.NotSlaver, call.err
	}

	call.pkg = pkg
	c.cli.sending <- call

	r, err := (<-call.Done).Reply()
	if err != nil {
		return ctrl.NotSlaver, call.err
	}

	t := r.(*ctrl.PkgSlaverStatus)
	if t.ErrMsg != "" {
		return ctrl.NotSlaver, errors.New(t.ErrMsg)
	}
	return t.Status, nil
}

// Internal control command.
// DelUnit deletes one unit data.
func (c *Context) DelUnit(unitId uint16) error {
	call := c.cli.newCall(proto.CmdDelUnit, nil)
	if call.err != nil {
		return call.err
	}

	var p ctrl.PkgDelUnit
	p.UnitId = unitId

	pkg, err := ctrl.Encode(call.cmd, c.dbId, call.seq, &p)
	if err != nil {
		c.cli.errCall(call, err)
		return call.err
	}

	call.pkg = pkg
	c.cli.sending <- call

	r, err := (<-call.Done).Reply()
	if err != nil {
		return call.err
	}

	t := r.(*ctrl.PkgDelUnit)
	if t.ErrMsg != "" {
		return errors.New(t.ErrMsg)
	}
	return nil
}

func replyGet(call *Call, err error) ([]byte, int64, uint32, error) {
	if err != nil {
		return nil, 0, 0, err
	}

	r, err := (<-call.Done).Reply()
	if err != nil {
		return nil, 0, 0, err
	}

	a := r.(GetReply)
	var value []byte
	if a.Value != nil {
		value = make([]byte, len(a.Value))
		copy(value, a.Value)
	}

	return value, a.Score, a.Cas, nil
}

func replySet(call *Call, err error) error {
	if err != nil {
		return err
	}

	_, err = (<-call.Done).Reply()
	return err
}

func replyIncr(call *Call, err error) ([]byte, int64, error) {
	if err != nil {
		return nil, 0, err
	}

	r, err := (<-call.Done).Reply()
	if err != nil {
		return nil, 0, err
	}

	var value []byte
	var a = r.(IncrReply)
	if a.Value != nil {
		value = make([]byte, len(a.Value))
		copy(value, a.Value)
	}

	return value, a.Score, nil
}

func replyScan(call *Call, err error) (ScanReply, error) {
	if err != nil {
		return ScanReply{}, err
	}

	r, err := (<-call.Done).Reply()
	if err != nil {
		return ScanReply{}, err
	}
	return r.(ScanReply), nil
}

func (call *Call) Reply() (interface{}, error) {
	if call.err != nil {
		return nil, call.err
	}

	if !call.ready {
		return nil, ErrCallNotReady
	}

	if proto.CmdAuth == call.cmd ||
		proto.CmdPing == call.cmd ||
		proto.CmdIncr == call.cmd ||
		proto.CmdDel == call.cmd ||
		proto.CmdSet == call.cmd ||
		proto.CmdGet == call.cmd {
		var p proto.PkgOneOp
		_, err := p.Decode(call.pkg)
		if err != nil {
			call.err = err
			return nil, call.err
		}
		if p.ErrCode < 0 {
			return nil, getErr(p.ErrCode)
		}
		switch call.cmd {
		case proto.CmdAuth:
			return nil, nil
		case proto.CmdPing:
			return nil, nil
		case proto.CmdIncr:
			return IncrReply{p.ErrCode, p.TableId, copyBytes(p.RowKey),
				copyBytes(p.ColKey), copyBytes(p.Value), p.Score}, nil
		case proto.CmdDel:
			return nil, nil
		case proto.CmdSet:
			return nil, nil
		case proto.CmdGet:
			return GetReply{p.ErrCode, p.TableId, copyBytes(p.RowKey),
				copyBytes(p.ColKey), copyBytes(p.Value), p.Score, p.Cas}, nil
		}
	}

	if proto.CmdMIncr == call.cmd ||
		proto.CmdMDel == call.cmd ||
		proto.CmdMSet == call.cmd ||
		proto.CmdMGet == call.cmd {
		var p proto.PkgMultiOp
		_, err := p.Decode(call.pkg)
		if err != nil {
			call.err = err
			return nil, call.err
		}

		if p.ErrCode < 0 {
			return nil, getErr(p.ErrCode)
		}

		switch call.cmd {
		case proto.CmdMIncr:
			var r = make([]IncrReply, len(p.Kvs))
			for i := 0; i < len(r); i++ {
				r[i] = IncrReply{p.Kvs[i].ErrCode, p.Kvs[i].TableId,
					copyBytes(p.Kvs[i].RowKey), copyBytes(p.Kvs[i].ColKey),
					copyBytes(p.Kvs[i].Value), p.Kvs[i].Score}
			}
			return r, nil
		case proto.CmdMDel:
			var r = make([]DelReply, len(p.Kvs))
			for i := 0; i < len(r); i++ {
				r[i] = DelReply{p.Kvs[i].ErrCode, p.Kvs[i].TableId,
					copyBytes(p.Kvs[i].RowKey), copyBytes(p.Kvs[i].ColKey)}
			}
			return r, nil
		case proto.CmdMSet:
			var r = make([]SetReply, len(p.Kvs))
			for i := 0; i < len(r); i++ {
				r[i] = SetReply{p.Kvs[i].ErrCode, p.Kvs[i].TableId,
					copyBytes(p.Kvs[i].RowKey), copyBytes(p.Kvs[i].ColKey)}
			}
			return r, nil
		case proto.CmdMGet:
			var r = make([]GetReply, len(p.Kvs))
			for i := 0; i < len(r); i++ {
				r[i] = GetReply{p.Kvs[i].ErrCode, p.Kvs[i].TableId,
					copyBytes(p.Kvs[i].RowKey), copyBytes(p.Kvs[i].ColKey),
					copyBytes(p.Kvs[i].Value), p.Kvs[i].Score, p.Kvs[i].Cas}
			}
			return r, nil
		}
	}

	switch call.cmd {
	case proto.CmdScan:
		var p proto.PkgScanResp
		_, err := p.Decode(call.pkg)
		if err != nil {
			call.err = err
			return nil, call.err
		}

		if p.ErrCode < 0 {
			return nil, getErr(p.ErrCode)
		}

		var r ScanReply
		r.ctx = call.ctx.(scanContext)
		r.End = (p.PkgFlag&proto.FlagEnd != 0)
		r.Kvs = make([]ScanKV, len(p.Kvs))
		for i := 0; i < len(p.Kvs); i++ {
			r.Kvs[i] = ScanKV{p.Kvs[i].TableId, copyBytes(p.Kvs[i].RowKey),
				copyBytes(p.Kvs[i].ColKey), copyBytes(p.Kvs[i].Value), p.Kvs[i].Score}
		}
		return r, nil

	case proto.CmdDump:
		var p proto.PkgDumpResp
		_, err := p.Decode(call.pkg)
		if err != nil {
			call.err = err
			return nil, call.err
		}

		if p.ErrCode < 0 {
			return nil, getErr(p.ErrCode)
		}

		var r DumpReply
		r.ctx = call.ctx.(dumpContext)
		r.ctx.lastUnitId = p.LastUnitId
		r.ctx.unitStart = (p.PkgFlag&proto.FlagUnitStart != 0)
		r.End = (p.PkgFlag&proto.FlagEnd != 0)
		r.Kvs = make([]DumpKV, len(p.Kvs))
		for i := 0; i < len(p.Kvs); i++ {
			r.Kvs[i] = DumpKV{p.Kvs[i].TableId, p.Kvs[i].ColSpace,
				copyBytes(p.Kvs[i].RowKey), copyBytes(p.Kvs[i].ColKey),
				copyBytes(p.Kvs[i].Value), p.Kvs[i].Score}
		}
		return r, nil
	}

	switch call.cmd {
	case proto.CmdSlaveOf:
		return call.replyInnerCtrl(&ctrl.PkgSlaveOf{})
	case proto.CmdMigrate:
		return call.replyInnerCtrl(&ctrl.PkgMigrate{})
	case proto.CmdSlaverSt:
		return call.replyInnerCtrl(&ctrl.PkgSlaverStatus{})
	case proto.CmdDelUnit:
		return call.replyInnerCtrl(&ctrl.PkgDelUnit{})
	}

	return nil, ErrUnknownCmd
}

func (call *Call) replyInnerCtrl(p interface{}) (interface{}, error) {
	err := ctrl.Decode(call.pkg, nil, p)
	if err != nil {
		call.err = err
		return nil, call.err
	}
	return p, nil
}
