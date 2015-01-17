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
	"io"
	"log"
	"net"
	"strconv"
	"sync"
)

const Version = "0.1" // GoTable version

var (
	ErrShutdown    = errors.New("connection is shut down")
	ErrUnknownCmd  = errors.New("unknown cmd")
	ErrClosedPool  = errors.New("connection pool is closed")
	ErrInvalidTag  = errors.New("invalid tag id")
	ErrNoValidAddr = errors.New("no valid address")
)

var (
	ErrCasNotMatch = initErrCode(EcodeCasNotMatch, "cas not match")
	ErrTempFail    = initErrCode(EcodeTempFail, "temporary failed")

	ErrReadFail    = initErrCode(EcodeReadFail, "read failed")
	ErrWriteFail   = initErrCode(EcodeWriteFail, "write failed")
	ErrDecodeFail  = initErrCode(EcodeDecodeFail, "decode request pkg failed")
	ErrNoPrivilege = initErrCode(EcodeNoPrivilege, "no access privilege")
	ErrInvDbId     = initErrCode(EcodeInvDbId, "invalid DB id")
	ErrInvScope    = initErrCode(EcodeInvScope, "invalid dump scope")
	ErrInvRowKey   = initErrCode(EcodeInvRowKey, "invalid rowkey")
	ErrInvScanNum  = initErrCode(EcodeInvScanNum, "scan number out of range")
	ErrScanEnded   = errors.New("already scan/dump to end")
)

// GoTable Error Code List
const (
	EcodeOk       = 0 // Success, normal case
	EcodeNotExist = 1 // Key not exist, normal case

	EcodeCasNotMatch = 11 // CAS not match, get new CAS and try again
	EcodeTempFail    = 12 // Temporary failed, retry may fix this

	EcodeReadFail    = 21 // Read failed
	EcodeWriteFail   = 22 // Write failed
	EcodeDecodeFail  = 23 // Decode request PKG failed
	EcodeNoPrivilege = 24 // No access privilege
	EcodeInvDbId     = 25 // Invalid DB ID (cannot be 0)
	EcodeInvColSpace = 26 // Invalid Col Space
	EcodeInvScope    = 27 // Invalid Dump Scope
	EcodeInvRowKey   = 28 // Invalid RowKey (cannot be empty)
	EcodeInvScanNum  = 29 // Scan number out of range
)

// A Client is a connection to GoTable server.
// It's safe to use in multiple goroutines.
type Client struct {
	p       *Pool
	c       net.Conn
	r       *bufio.Reader
	sending chan *Call

	mtx      sync.Mutex // protects following
	seq      uint64
	pending  map[uint64]*Call
	closing  bool // user has called Close
	shutdown bool // server has told us to stop
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

// Create a new client Context.
func (c *Client) NewContext(dbId uint8) *Context {
	return &Context{c, dbId}
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
		call.err = err
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

const (
	maxErrNormalCase = 10
	maxErrTableCode  = 64
)

var errTalbe = make([]error, maxErrTableCode)

func isNormalErrorCode(code uint8) bool {
	// normal cases, not failure
	return code < maxErrNormalCase
}

func initErrCode(code uint8, msg string) error {
	if code < maxErrTableCode {
		if errTalbe[code] == nil {
			errTalbe[code] = errors.New(fmt.Sprintf("%s (%d)", msg, code))
		}
		return errTalbe[code]
	}
	return newErrCode(code)
}

func newErrCode(code uint8) error {
	if code < maxErrTableCode {
		if errTalbe[code] != nil {
			return errTalbe[code]
		}
	}
	return errors.New("error code (" + strconv.Itoa(int(code)) + ")")
}
