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

// Connection Context to GoTable server.
// It's NOT safe to use in multiple goroutines.
type Context struct {
	cli  *Client
	dbId uint8
}

type Call struct {
	Error error
	Done  chan *Call

	pkg []byte
	seq uint64
	cmd uint8
}

// Get the underling connection Client of the Context.
func (c *Context) Client() *Client {
	return c.cli
}

func (c *Context) Use(dbId uint8) {
	c.dbId = dbId
}

func (c *Context) Ping() error {
	call := c.GoPing(nil)
	if call.Error != nil {
		return call.Error
	}

	_, err := c.ParseReply(<-call.Done)
	return err
}

func (c *Context) Get(args *OneArgs) (*OneReply, error) {
	call := c.GoGet(args, nil)
	if call.Error != nil {
		return nil, call.Error
	}

	r, err := c.ParseReply(<-call.Done)
	if err != nil {
		return nil, err
	}
	return r.(*OneReply), nil
}

func (c *Context) ZGet(args *OneArgs) (*OneReply, error) {
	call := c.GoZGet(args, nil)
	if call.Error != nil {
		return nil, call.Error
	}

	r, err := c.ParseReply(<-call.Done)
	if err != nil {
		return nil, err
	}
	return r.(*OneReply), nil
}

func (c *Context) Set(args *OneArgs) (*OneReply, error) {
	call := c.GoSet(args, nil)
	if call.Error != nil {
		return nil, call.Error
	}

	r, err := c.ParseReply(<-call.Done)
	if err != nil {
		return nil, err
	}
	return r.(*OneReply), nil
}

func (c *Context) ZSet(args *OneArgs) (*OneReply, error) {
	call := c.GoZSet(args, nil)
	if call.Error != nil {
		return nil, call.Error
	}

	r, err := c.ParseReply(<-call.Done)
	if err != nil {
		return nil, err
	}
	return r.(*OneReply), nil
}

func (c *Context) Del(args *OneArgs) (*OneReply, error) {
	call := c.GoDel(args, nil)
	if call.Error != nil {
		return nil, call.Error
	}

	r, err := c.ParseReply(<-call.Done)
	if err != nil {
		return nil, err
	}
	return r.(*OneReply), nil
}

func (c *Context) ZDel(args *OneArgs) (*OneReply, error) {
	call := c.GoZDel(args, nil)
	if call.Error != nil {
		return nil, call.Error
	}

	r, err := c.ParseReply(<-call.Done)
	if err != nil {
		return nil, err
	}
	return r.(*OneReply), nil
}

func (c *Context) Incr(args *OneArgs) (*OneReply, error) {
	call := c.GoIncr(args, nil)
	if call.Error != nil {
		return nil, call.Error
	}

	r, err := c.ParseReply(<-call.Done)
	if err != nil {
		return nil, err
	}
	return r.(*OneReply), nil
}

func (c *Context) ZIncr(args *OneArgs) (*OneReply, error) {
	call := c.GoZIncr(args, nil)
	if call.Error != nil {
		return nil, call.Error
	}

	r, err := c.ParseReply(<-call.Done)
	if err != nil {
		return nil, err
	}
	return r.(*OneReply), nil
}

func (c *Context) MGet(args *MultiArgs) (*MultiReply, error) {
	call := c.GoMGet(args, nil)
	if call.Error != nil {
		return nil, call.Error
	}

	r, err := c.ParseReply(<-call.Done)
	if err != nil {
		return nil, err
	}
	return r.(*MultiReply), nil
}

func (c *Context) ZmGet(args *MultiArgs) (*MultiReply, error) {
	call := c.GoZmGet(args, nil)
	if call.Error != nil {
		return nil, call.Error
	}

	r, err := c.ParseReply(<-call.Done)
	if err != nil {
		return nil, err
	}
	return r.(*MultiReply), nil
}

func (c *Context) MSet(args *MultiArgs) (*MultiReply, error) {
	call := c.GoMSet(args, nil)
	if call.Error != nil {
		return nil, call.Error
	}

	r, err := c.ParseReply(<-call.Done)
	if err != nil {
		return nil, err
	}
	return r.(*MultiReply), nil
}

func (c *Context) ZmSet(args *MultiArgs) (*MultiReply, error) {
	call := c.GoZmSet(args, nil)
	if call.Error != nil {
		return nil, call.Error
	}

	r, err := c.ParseReply(<-call.Done)
	if err != nil {
		return nil, err
	}
	return r.(*MultiReply), nil
}

func (c *Context) MDel(args *MultiArgs) (*MultiReply, error) {
	call := c.GoMDel(args, nil)
	if call.Error != nil {
		return nil, call.Error
	}

	r, err := c.ParseReply(<-call.Done)
	if err != nil {
		return nil, err
	}
	return r.(*MultiReply), nil
}

func (c *Context) ZmDel(args *MultiArgs) (*MultiReply, error) {
	call := c.GoZmDel(args, nil)
	if call.Error != nil {
		return nil, call.Error
	}

	r, err := c.ParseReply(<-call.Done)
	if err != nil {
		return nil, err
	}
	return r.(*MultiReply), nil
}

func (c *Context) MIncr(args *MultiArgs) (*MultiReply, error) {
	call := c.GoMIncr(args, nil)
	if call.Error != nil {
		return nil, call.Error
	}

	r, err := c.ParseReply(<-call.Done)
	if err != nil {
		return nil, err
	}
	return r.(*MultiReply), nil
}

func (c *Context) ZmIncr(args *MultiArgs) (*MultiReply, error) {
	call := c.GoZmIncr(args, nil)
	if call.Error != nil {
		return nil, call.Error
	}

	r, err := c.ParseReply(<-call.Done)
	if err != nil {
		return nil, err
	}
	return r.(*MultiReply), nil
}

func (c *Context) Scan(args *ScanArgs) (*ScanReply, error) {
	call := c.GoScan(args, nil)
	if call.Error != nil {
		return nil, call.Error
	}

	r, err := c.ParseReply(<-call.Done)
	if err != nil {
		return nil, err
	}
	return r.(*ScanReply), nil
}

func (c *Context) ZScan(args *ScanArgs) (*ScanReply, error) {
	call := c.GoZScan(args, nil)
	if call.Error != nil {
		return nil, call.Error
	}

	r, err := c.ParseReply(<-call.Done)
	if err != nil {
		return nil, err
	}
	return r.(*ScanReply), nil
}

// Get, Set, Del, Incr, ZGet, ZSet, ZDel, ZIncr
func (c *Context) goOneOp(zop bool, args *OneArgs, cmd uint8, done chan *Call) *Call {
	call := c.cli.newCall(cmd, done)
	if call.Error != nil {
		return call
	}

	var p proto.PkgOneOp
	p.Seq = call.seq
	p.DbId = c.dbId
	p.Cmd = call.cmd
	p.TableId = args.TableId
	p.RowKey = args.RowKey
	p.ColKey = args.ColKey

	if args.Cas != 0 {
		p.Cas = args.Cas
		p.CtrlFlag |= proto.CtrlCas
	}
	if proto.CmdSet == cmd || proto.CmdIncr == cmd {
		if args.Score != 0 {
			p.Score = args.Score
			p.CtrlFlag |= proto.CtrlScore
		}
	}
	if proto.CmdSet == cmd {
		if args.Value != nil {
			p.Value = args.Value
			p.CtrlFlag |= proto.CtrlValue
		}
	}

	// ZGet, ZSet, ZDel, ZIncr
	if zop {
		p.ColSpace = proto.ColSpaceScore1
		p.CtrlFlag |= proto.CtrlColSpace
	}

	var err error
	call.pkg, _, err = p.Encode(nil)
	if err != nil {
		c.cli.errCall(call, err)
		return call
	}

	// put request pkg to sending channel
	c.cli.sending <- call

	return call
}

func (c *Context) GoPing(done chan *Call) *Call {
	var oa OneArgs
	return c.goOneOp(false, &oa, proto.CmdPing, done)
}

func (c *Context) GoGet(args *OneArgs, done chan *Call) *Call {
	return c.goOneOp(false, args, proto.CmdGet, done)
}

func (c *Context) GoZGet(args *OneArgs, done chan *Call) *Call {
	return c.goOneOp(true, args, proto.CmdGet, done)
}

func (c *Context) GoSet(args *OneArgs, done chan *Call) *Call {
	return c.goOneOp(false, args, proto.CmdSet, done)
}

func (c *Context) GoZSet(args *OneArgs, done chan *Call) *Call {
	return c.goOneOp(true, args, proto.CmdSet, done)
}

func (c *Context) GoDel(args *OneArgs, done chan *Call) *Call {
	return c.goOneOp(false, args, proto.CmdDel, done)
}

func (c *Context) GoZDel(args *OneArgs, done chan *Call) *Call {
	return c.goOneOp(true, args, proto.CmdDel, done)
}

func (c *Context) GoIncr(args *OneArgs, done chan *Call) *Call {
	return c.goOneOp(false, args, proto.CmdIncr, done)
}

func (c *Context) GoZIncr(args *OneArgs, done chan *Call) *Call {
	return c.goOneOp(true, args, proto.CmdIncr, done)
}

// MGet, MSet, MDel, MIncr, ZMGet, ZMSet, ZMDel, ZMIncr
func (c *Context) goMultiOp(zop bool, args *MultiArgs, cmd uint8, done chan *Call) *Call {
	call := c.cli.newCall(cmd, done)
	if call.Error != nil {
		return call
	}

	var p proto.PkgMultiOp
	p.Seq = call.seq
	p.DbId = c.dbId
	p.Cmd = call.cmd

	p.Kvs = make([]proto.KeyValueCtrl, len(args.Args))
	for i := 0; i < len(args.Args); i++ {
		p.Kvs[i].TableId = args.Args[i].TableId
		p.Kvs[i].RowKey = args.Args[i].RowKey
		p.Kvs[i].ColKey = args.Args[i].ColKey

		if args.Args[i].Cas != 0 {
			p.Kvs[i].Cas = args.Args[i].Cas
			p.Kvs[i].CtrlFlag |= proto.CtrlCas
		}
		if proto.CmdMSet == cmd || proto.CmdIncr == cmd {
			if args.Args[i].Score != 0 {
				p.Kvs[i].Score = args.Args[i].Score
				p.Kvs[i].CtrlFlag |= proto.CtrlScore
			}
		}
		if proto.CmdMSet == cmd {
			if args.Args[i].Value != nil {
				p.Kvs[i].Value = args.Args[i].Value
				p.Kvs[i].CtrlFlag |= proto.CtrlValue
			}
		}

		// ZMGet, ZMSet, ZMDel, ZMIncr
		if zop {
			p.Kvs[i].ColSpace = proto.ColSpaceScore1
			p.Kvs[i].CtrlFlag |= proto.CtrlColSpace
		}
	}

	var err error
	call.pkg, _, err = p.Encode(nil)
	if err != nil {
		c.cli.errCall(call, err)
		return call
	}

	c.cli.sending <- call

	return call
}

func (c *Context) GoMGet(args *MultiArgs, done chan *Call) *Call {
	return c.goMultiOp(false, args, proto.CmdMGet, done)
}

func (c *Context) GoZmGet(args *MultiArgs, done chan *Call) *Call {
	return c.goMultiOp(true, args, proto.CmdMGet, done)
}

func (c *Context) GoMSet(args *MultiArgs, done chan *Call) *Call {
	return c.goMultiOp(false, args, proto.CmdMSet, done)
}

func (c *Context) GoZmSet(args *MultiArgs, done chan *Call) *Call {
	return c.goMultiOp(true, args, proto.CmdMSet, done)
}

func (c *Context) GoMDel(args *MultiArgs, done chan *Call) *Call {
	return c.goMultiOp(false, args, proto.CmdMDel, done)
}

func (c *Context) GoZmDel(args *MultiArgs, done chan *Call) *Call {
	return c.goMultiOp(true, args, proto.CmdMDel, done)
}

func (c *Context) GoMIncr(args *MultiArgs, done chan *Call) *Call {
	return c.goMultiOp(false, args, proto.CmdMIncr, done)
}

func (c *Context) GoZmIncr(args *MultiArgs, done chan *Call) *Call {
	return c.goMultiOp(true, args, proto.CmdMIncr, done)
}

func (c *Context) goScan(zop bool, args *ScanArgs, done chan *Call) *Call {
	call := c.cli.newCall(proto.CmdScan, done)
	if call.Error != nil {
		return call
	}

	var p proto.PkgScanReq
	p.Seq = call.seq
	p.DbId = c.dbId
	p.Cmd = call.cmd
	p.Num = args.Num
	p.Direction = args.Direction
	p.TableId = args.TableId
	p.RowKey = args.RowKey
	p.ColKey = args.ColKey

	// ZScan
	if zop {
		if args.Score != 0 {
			p.Score = args.Score
			p.CtrlFlag |= proto.CtrlScore
		}
		p.ColSpace = proto.ColSpaceScore1
		p.CtrlFlag |= proto.CtrlColSpace
	}

	var err error
	call.pkg, _, err = p.Encode(nil)
	if err != nil {
		c.cli.errCall(call, err)
		return call
	}

	c.cli.sending <- call

	return call
}

func (c *Context) GoScan(args *ScanArgs, done chan *Call) *Call {
	return c.goScan(false, args, done)
}

func (c *Context) GoZScan(args *ScanArgs, done chan *Call) *Call {
	return c.goScan(true, args, done)
}

func (c *Context) ParseReply(call *Call) (interface{}, error) {
	if call.Error != nil {
		return nil, call.Error
	}

	switch call.cmd {
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
			call.Error = err
			return nil, call.Error
		}

		if !isNormalErrorCode(p.ErrCode) {
			call.Error = newErrorCode(p.ErrCode)
			return nil, call.Error
		}

		return &OneReply{p.ErrCode, &p.KeyValue}, nil

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
			call.Error = err
			return nil, call.Error
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
			call.Error = err
			return nil, call.Error
		}

		var r ScanReply
		r.Direction = p.Direction
		r.End = p.End
		r.Reply = make([]OneReply, len(p.Kvs))
		for i := 0; i < len(p.Kvs); i++ {
			r.Reply[i].ErrCode = p.Kvs[i].ErrCode
			r.Reply[i].KeyValue = &p.Kvs[i].KeyValue
		}
		return &r, nil
	}

	return nil, ErrUnknownCmd
}
