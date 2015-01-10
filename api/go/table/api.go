package table

import (
	"bufio"
	"errors"
	"github.com/stevejiang/gotable/api/go/table/proto"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
)

var (
	ErrCasNotMatch = errors.New("cas is not match")
	ErrShutdown    = errors.New("connection is shut down")
	ErrUnknownCmd  = errors.New("unknown cmd")

	ErrClosedPool  = errors.New("connection pool is closed")
	ErrInvalidTag  = errors.New("invalid tag id")
	ErrNoValidAddr = errors.New("no valid address")
)

const (
	EcodeOk          = 0  // Success, normal case
	EcodeNotExist    = 1  // Key not exist, normal case
	EcodeCasNotMatch = 10 // CAS not match, get new CAS and try again

	EcodeTempReadFail  = 11 // Temporary fail, retry may fix this
	EcodeTempWriteFail = 12 // Temporary fail, retry may fix this

	EcodeDecodeFailed = 21 // Decode request PKG failed
	EcodeReadFailed   = 22
	EcodeWriteFailed  = 23
)

type Client struct {
	p       *Pool
	c       net.Conn
	r       *bufio.Reader
	sending chan *Call

	mtx      sync.Mutex // protects following
	seq      uint64
	pending  map[uint64]*Call
	dbId     uint8
	closing  bool // user has called Close
	shutdown bool // server has told us to stop
}

type Call struct {
	Error error
	Done  chan *Call

	pkg []byte
	seq uint64
	cmd uint8
}

func NewClient(conn net.Conn) *Client {
	var c = new(Client)
	c.c = conn
	c.r = bufio.NewReader(conn)
	c.sending = make(chan *Call, 128)
	c.pending = make(map[uint64]*Call)

	go c.recv()
	go c.send()

	return c
}

func newPoolClient(network, address string, pool *Pool) *Client {
	c, err := Dial(network, address)
	if err != nil {
		return nil
	}

	c.p = pool
	return c
}

func Dial(network, address string) (*Client, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	return NewClient(conn), nil
}

func (c *Client) Close() error {
	if c.p == nil {
		return c.doClose()
	} else {
		return c.p.put(c)
	}
}

func (c *Client) doClose() error {
	c.mtx.Lock()
	if c.closing {
		c.mtx.Unlock()
		return ErrShutdown
	}
	c.closing = true
	c.mtx.Unlock()

	close(c.sending)
	var err = c.c.Close()

	var p = c.p
	if p != nil {
		p.remove(c)
	}

	return err
}

func (c *Client) Use(dbId uint8) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.dbId = dbId
}

func (c *Client) Get(args *OneArgs) (*OneReply, error) {
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

func (c *Client) ZGet(args *OneArgs) (*OneReply, error) {
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

func (c *Client) Set(args *OneArgs) (*OneReply, error) {
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

func (c *Client) ZSet(args *OneArgs) (*OneReply, error) {
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

func (c *Client) Del(args *OneArgs) (*OneReply, error) {
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

func (c *Client) ZDel(args *OneArgs) (*OneReply, error) {
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

func (c *Client) Incr(args *OneArgs) (*OneReply, error) {
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

func (c *Client) ZIncr(args *OneArgs) (*OneReply, error) {
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

func (c *Client) MGet(args *MultiArgs) (*MultiReply, error) {
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

func (c *Client) ZmGet(args *MultiArgs) (*MultiReply, error) {
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

func (c *Client) MSet(args *MultiArgs) (*MultiReply, error) {
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

func (c *Client) ZmSet(args *MultiArgs) (*MultiReply, error) {
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

func (c *Client) MDel(args *MultiArgs) (*MultiReply, error) {
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

func (c *Client) ZmDel(args *MultiArgs) (*MultiReply, error) {
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

func (c *Client) MIncr(args *MultiArgs) (*MultiReply, error) {
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

func (c *Client) ZmIncr(args *MultiArgs) (*MultiReply, error) {
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

func (c *Client) Scan(args *ScanArgs) (*ScanReply, error) {
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

func (c *Client) ZScan(args *ScanArgs) (*ScanReply, error) {
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
func (c *Client) goOneOp(zop bool, args *OneArgs, cmd uint8, done chan *Call) *Call {
	call := c.newCall(cmd, done)
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
		c.errCall(call, err)
		return call
	}

	// put request pkg to sending channel
	c.sending <- call

	return call
}

func (c *Client) GoGet(args *OneArgs, done chan *Call) *Call {
	return c.goOneOp(false, args, proto.CmdGet, done)
}

func (c *Client) GoZGet(args *OneArgs, done chan *Call) *Call {
	return c.goOneOp(true, args, proto.CmdGet, done)
}

func (c *Client) GoSet(args *OneArgs, done chan *Call) *Call {
	return c.goOneOp(false, args, proto.CmdSet, done)
}

func (c *Client) GoZSet(args *OneArgs, done chan *Call) *Call {
	return c.goOneOp(true, args, proto.CmdSet, done)
}

func (c *Client) GoDel(args *OneArgs, done chan *Call) *Call {
	return c.goOneOp(false, args, proto.CmdDel, done)
}

func (c *Client) GoZDel(args *OneArgs, done chan *Call) *Call {
	return c.goOneOp(true, args, proto.CmdDel, done)
}

func (c *Client) GoIncr(args *OneArgs, done chan *Call) *Call {
	return c.goOneOp(false, args, proto.CmdIncr, done)
}

func (c *Client) GoZIncr(args *OneArgs, done chan *Call) *Call {
	return c.goOneOp(true, args, proto.CmdIncr, done)
}

// MGet, MSet, MDel, MIncr, ZMGet, ZMSet, ZMDel, ZMIncr
func (c *Client) goMultiOp(zop bool, args *MultiArgs, cmd uint8, done chan *Call) *Call {
	call := c.newCall(cmd, done)
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
		c.errCall(call, err)
		return call
	}

	c.sending <- call

	return call
}

func (c *Client) GoMGet(args *MultiArgs, done chan *Call) *Call {
	return c.goMultiOp(false, args, proto.CmdMGet, done)
}

func (c *Client) GoZmGet(args *MultiArgs, done chan *Call) *Call {
	return c.goMultiOp(true, args, proto.CmdMGet, done)
}

func (c *Client) GoMSet(args *MultiArgs, done chan *Call) *Call {
	return c.goMultiOp(false, args, proto.CmdMSet, done)
}

func (c *Client) GoZmSet(args *MultiArgs, done chan *Call) *Call {
	return c.goMultiOp(true, args, proto.CmdMSet, done)
}

func (c *Client) GoMDel(args *MultiArgs, done chan *Call) *Call {
	return c.goMultiOp(false, args, proto.CmdMDel, done)
}

func (c *Client) GoZmDel(args *MultiArgs, done chan *Call) *Call {
	return c.goMultiOp(true, args, proto.CmdMDel, done)
}

func (c *Client) GoMIncr(args *MultiArgs, done chan *Call) *Call {
	return c.goMultiOp(false, args, proto.CmdMIncr, done)
}

func (c *Client) GoZmIncr(args *MultiArgs, done chan *Call) *Call {
	return c.goMultiOp(true, args, proto.CmdMIncr, done)
}

func (c *Client) goScan(zop bool, args *ScanArgs, done chan *Call) *Call {
	call := c.newCall(proto.CmdScan, done)
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
		c.errCall(call, err)
		return call
	}

	c.sending <- call

	return call
}

func (c *Client) GoScan(args *ScanArgs, done chan *Call) *Call {
	return c.goScan(false, args, done)
}

func (c *Client) GoZScan(args *ScanArgs, done chan *Call) *Call {
	return c.goScan(true, args, done)
}

func (c *Client) ParseReply(call *Call) (interface{}, error) {
	if call.Error != nil {
		return nil, call.Error
	}

	switch call.cmd {
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

func (c *Client) recv() {
	var headBuf = make([]byte, proto.HeadSize)
	var head proto.PkgHead

	var pkg []byte
	var err error
	for err == nil {
		pkg, err = proto.ReadPkg(c.r, headBuf, &head, nil)
		if err != nil {
			break
		}

		var call *Call
		var ok bool

		c.mtx.Lock()
		if call, ok = c.pending[head.Seq]; ok {
			delete(c.pending, head.Seq)
		}
		c.mtx.Unlock()

		if call != nil {
			call.pkg = pkg
			call.done()
		}
	}

	// Terminate pending calls.
	c.mtx.Lock()
	c.shutdown = true
	if err == io.EOF {
		if c.closing {
			err = ErrShutdown
		} else {
			err = io.ErrUnexpectedEOF
		}
	}
	for _, call := range c.pending {
		call.Error = err
		call.done()
	}
	c.mtx.Unlock()

	c.doClose()
}

func (c *Client) send() {
	var err error
	for {
		select {
		case call, ok := <-c.sending:
			if !ok {
				return
			}

			if err == nil {
				_, err = c.c.Write(call.pkg)
				if err != nil {
					c.mtx.Lock()
					c.shutdown = true
					c.mtx.Unlock()
				}
			}
		}
	}
}

func (call *Call) done() {
	select {
	case call.Done <- call:
		// ok
	default:
		// We don't want to block here.  It is the caller's responsibility to make
		// sure the channel has enough buffer space. See comment in Go().
		log.Println("gotable: discarding Call reply due to insufficient Done chan capacity")
	}
}

func (c *Client) newCall(cmd uint8, done chan *Call) *Call {
	var call = new(Call)
	call.cmd = cmd
	if done == nil {
		done = make(chan *Call, 2)
	} else {
		if cap(done) == 0 {
			log.Panic("gotable: done channel is unbuffered")
		}
	}
	call.Done = done

	c.mtx.Lock()
	if c.shutdown || c.closing {
		c.mtx.Unlock()
		c.errCall(call, ErrShutdown)
		return call
	}
	c.seq += 1
	call.seq = c.seq
	c.pending[call.seq] = call
	c.mtx.Unlock()

	return call
}

func (c *Client) errCall(call *Call, err error) {
	call.Error = err

	if call.seq > 0 {
		c.mtx.Lock()
		delete(c.pending, call.seq)
		c.mtx.Unlock()
	}

	call.done()
}

func isNormalErrorCode(code uint8) bool {
	// <10: normal cases
	return code < 10
}

func newErrorCode(code uint8) error {
	if EcodeCasNotMatch == code {
		return ErrCasNotMatch
	}
	return errors.New("error code " + strconv.Itoa(int(code)))
}
