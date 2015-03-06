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

// The official client API of GoTable. GoTable is a high performance
// NoSQL database.
package table

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/stevejiang/gotable/api/go/table/proto"
	"github.com/stevejiang/gotable/util"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
)

const Version = "0.1" // GoTable version

var (
	ErrShutdown     = errors.New("connection is shut down")
	ErrCallNotReady = errors.New("call not ready to reply")
	ErrClosedPool   = errors.New("connection pool is closed")
	ErrNoValidAddr  = errors.New("no valid address")
)

var (
	ErrCasNotMatch = initErr(EcCasNotMatch, "cas not match")
	ErrTempFail    = initErr(EcTempFail, "temporary failed")
	ErrUnknownCmd  = initErr(EcUnknownCmd, "unknown cmd")
	ErrAuthFailed  = initErr(EcAuthFailed, "authorize failed")
	ErrNoPrivilege = initErr(EcNoPrivilege, "no access privilege")
	ErrWriteSlaver = initErr(EcWriteSlaver, "can not write slaver directly")
	ErrReadFail    = initErr(EcReadFail, "read failed")
	ErrWriteFail   = initErr(EcWriteFail, "write failed")
	ErrDecodeFail  = initErr(EcDecodeFail, "decode request pkg failed")
	ErrInvDbId     = initErr(EcInvDbId, "can not use admin db")
	ErrInvRowKey   = initErr(EcInvRowKey, "row key length out of range")
	ErrInvValue    = initErr(EcInvValue, "value length out of range")
	ErrInvScanNum  = initErr(EcInvScanNum, "scan request number out of range")
	ErrScanEnded   = initErr(EcScanEnded, "already scan/dump to end")
)

// GoTable Error Code List
const (
	EcNotExist    = 1   // Key NOT exist
	EcOk          = 0   // Success
	EcCasNotMatch = -50 // CAS not match, get new CAS and try again
	EcTempFail    = -51 // Temporary failed, retry may fix this
	EcUnknownCmd  = -60 // Unknown cmd
	EcAuthFailed  = -61 // Authorize failed
	EcNoPrivilege = -62 // No access privilege
	EcWriteSlaver = -63 // Can NOT write slaver directly
	EcReadFail    = -64 // Read failed
	EcWriteFail   = -65 // Write failed
	EcDecodeFail  = -66 // Decode request PKG failed
	EcInvDbId     = -67 // Invalid DB ID (cannot be 255)
	EcInvRowKey   = -68 // RowKey length should be [1 ~ 255]
	EcInvValue    = -69 // Value length should be [0 ~ 512KB]
	EcInvScanNum  = -70 // Scan request number out of range
	EcScanEnded   = -71 // Already scan/dump to end
)

var tableErrors = make([]error, 256)

func initErr(code int8, msg string) error {
	tableErrors[int(code)+128] = fmt.Errorf("%s (%d)", msg, code)
	return tableErrors[int(code)+128]
}

func getErr(code int8) error {
	if tableErrors[int(code)+128] != nil {
		return tableErrors[int(code)+128]
	} else {
		return errors.New("error code " + strconv.Itoa(int(code)))
	}
}

// A Client is a connection to GoTable server.
// It's safe to use in multiple goroutines.
type Client struct {
	p       *Pool
	c       net.Conn
	r       *bufio.Reader
	sending chan *Call

	mtx      sync.Mutex // protects following
	authBM   *util.BitMap
	seq      uint64
	pending  map[uint64]*Call
	closing  bool // user has called Close
	shutdown bool // server has told us to stop
}

// Create a new connection Client to GoTable server.
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

// Dial connects to the address on the named network of GoTable server.
//
// Known networks are "tcp", "tcp4" (IPv4-only), "tcp6" (IPv6-only),
// and "unix".
// For TCP networks, addresses have the form host:port.
// For Unix networks, the address must be a file system path.
//
// It returns a connection Client to GoTable server.
func Dial(network, address string) (*Client, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	return NewClient(conn), nil
}

// Create a new client Context with selected dbId.
// All operations on the Context use the selected dbId.
func (c *Client) NewContext(dbId uint8) *Context {
	return &Context{c, dbId}
}

// Close the connection.
func (c *Client) Close() error {
	if c.p == nil {
		return c.doClose()
	} else {
		c.mtx.Lock()
		if c.shutdown {
			c.mtx.Unlock()
			return c.doClose()
		}
		c.mtx.Unlock()
		return nil
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

	if c.p != nil {
		c.p.remove(c)
	}

	return err
}

// Test whether already authorized.
func (c *Client) isAuthorized(dbId uint8) bool {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.authBM == nil {
		return false
	}

	if c.authBM.Get(proto.AdminDbId) {
		return true
	}

	return dbId != proto.AdminDbId && c.authBM.Get(uint(dbId))
}

// Cache authorize result. When authorizing again, return directly.
func (c *Client) cachAuth(pkg []byte) {
	var one proto.PkgOneOp
	_, err := one.Decode(pkg)
	if err == nil && one.ErrCode == 0 {
		c.mtx.Lock()
		if c.authBM == nil {
			c.authBM = util.NewBitMap(256 / 8)
		}
		c.authBM.Set(uint(one.DbId))
		c.mtx.Unlock()
	}
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

		if proto.CmdAuth == head.Cmd {
			c.cachAuth(pkg)
		}

		if call != nil {
			call.pkg = pkg
			call.ready = true
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
		call.err = err
		call.ready = true
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
	if call.ready {
		select {
		case call.Done <- call:
			// ok
		default:
			// We don't want to block here.  It is the caller's responsibility to make
			// sure the channel has enough buffer space. See comment in Go().
			log.Println("discarding reply due to insufficient Done chan capacity")
		}
	}
}

func (c *Client) newCall(cmd uint8, done chan *Call) *Call {
	var call = new(Call)
	call.cmd = cmd
	if done == nil {
		done = make(chan *Call, 4)
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
	call.err = err

	if call.seq > 0 {
		c.mtx.Lock()
		delete(c.pending, call.seq)
		c.mtx.Unlock()
	}

	call.done()
}
