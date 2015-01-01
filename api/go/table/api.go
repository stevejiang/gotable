package table

import (
	"bufio"
	"errors"
	"github.com/stevejiang/gotable/proto"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
)

var (
	ErrNotExist = errors.New("key is not found")
	ErrShutdown = errors.New("connection is shut down")
)

type Client struct {
	c       net.Conn
	r       *bufio.Reader
	headBuf []byte
	head    proto.PkgHead

	sending chan *Call

	mtx      sync.Mutex // protects following
	seq      uint64
	pending  map[uint64]*Call
	closing  bool // user has called Close
	shutdown bool // server has told us to stop
}

type Call struct {
	Pkg   []byte
	Error error
	seq   uint64
	Done  chan *Call
}

func NewClient(conn net.Conn) *Client {
	var c = new(Client)
	c.c = conn
	c.r = bufio.NewReader(conn)
	c.headBuf = make([]byte, proto.HeadSize)
	c.sending = make(chan *Call, 128)
	c.pending = make(map[uint64]*Call)

	go c.recv()
	go c.send()

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
	c.mtx.Lock()
	if c.closing {
		c.mtx.Unlock()
		return ErrShutdown
	}
	c.closing = true
	c.mtx.Unlock()
	return nil
}

func (c *Client) Get(rowKey, colKey []byte) ([]byte, error) {
	call := c.GoGet(rowKey, colKey)
	if call.Error != nil {
		return nil, call.Error
	}

	return c.GetCall(<-call.Done)
}

func (c *Client) Set(rowKey, colKey, value []byte) error {
	call := c.GoSet(rowKey, colKey, value)
	if call.Error != nil {
		return call.Error
	}

	return c.SetCall(<-call.Done)
}

func (c *Client) Del(rowKey, colKey []byte) error {
	return nil
}

func (c *Client) newCall() *Call {
	var call = new(Call)
	call.Done = make(chan *Call, 1)

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

func (c *Client) GoGet(rowKey, colKey []byte) *Call {
	call := c.newCall()
	if call.Error != nil {
		return call
	}

	var req proto.PkgCmdGetReq
	req.Seq = call.seq
	req.Cmd = proto.CmdGet
	req.RowKey = rowKey
	req.ColKey = colKey

	_, err := req.Encode(&call.Pkg)
	if err != nil {
		c.errCall(call, err)
		return call
	}

	// put request pkg to sending channel
	c.sending <- call

	return call
}

func (c *Client) GetCall(call *Call) ([]byte, error) {
	var resp proto.PkgCmdGetResp
	resp.Decode(call.Pkg)

	if resp.ErrCode != proto.EcodeOk {
		call.Error = newErrorCode(resp.ErrCode)
		return nil, call.Error
	}

	return resp.Value, nil
}

func (c *Client) GoSet(rowKey, colKey, value []byte) *Call {
	call := c.newCall()
	if call.Error != nil {
		return call
	}

	var req proto.PkgCmdPutReq
	req.Seq = call.seq
	req.Cmd = proto.CmdPut
	req.RowKey = rowKey
	req.ColKey = colKey
	req.Value = value

	_, err := req.Encode(&call.Pkg)
	if err != nil {
		c.errCall(call, err)
		return call
	}

	c.sending <- call

	return call
}

func (c *Client) SetCall(call *Call) error {
	var resp proto.PkgCmdPutResp
	resp.Decode(call.Pkg)

	if resp.ErrCode != proto.EcodeOk {
		return newErrorCode(resp.ErrCode)
	}

	return nil
}

func (c *Client) recv() {
	var pkg []byte
	var err error
	for err == nil {
		pkg, err = proto.ReadPkg(c.r, c.headBuf, &c.head, nil)
		if err != nil {
			break
		}

		var call *Call
		var ok bool

		c.mtx.Lock()
		if call, ok = c.pending[c.head.Seq]; ok {
			delete(c.pending, c.head.Seq)
		}
		c.mtx.Unlock()

		call.Pkg = pkg
		call.done()
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
				_, err = c.c.Write(call.Pkg)
			}

			if err != nil {
				c.mtx.Lock()
				c.shutdown = true
				delete(c.pending, call.seq)
				c.mtx.Unlock()

				call.Error = err
				call.done()
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

func newErrorCode(code uint8) error {
	if code == proto.EcodeNotExist {
		return ErrNotExist
	}
	return errors.New("error code " + strconv.Itoa(int(code)))
}
