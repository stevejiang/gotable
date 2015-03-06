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

package server

import (
	"bufio"
	"github.com/stevejiang/gotable/api/go/table/proto"
	"github.com/stevejiang/gotable/store"
	"github.com/stevejiang/gotable/util"
	"log"
	"net"
	"sync"
	"sync/atomic"
)

const (
	ClientTypeNormal = iota
	ClientTypeMaster
	ClientTypeSlaver
)

type Request struct {
	Cli *Client
	store.PkgArgs
}

type Response store.PkgArgs

type RequestChan struct {
	WriteReqChan chan *Request
	ReadReqChan  chan *Request
	SyncReqChan  chan *Request
	DumpReqChan  chan *Request
	CtrlReqChan  chan *Request
}

type Client struct {
	c           net.Conn
	r           *bufio.Reader
	respChan    chan *Response
	authEnabled bool

	// atomic
	closed  uint32
	cliType uint32

	// protects following
	mtx      sync.RWMutex
	authBM   *util.BitMap
	shutdown bool
}

func NewClient(conn net.Conn, authEnabled bool) *Client {
	var c = new(Client)
	c.c = conn
	c.r = bufio.NewReader(conn)
	c.respChan = make(chan *Response, 64)
	c.authEnabled = authEnabled
	atomic.StoreUint32(&c.cliType, ClientTypeNormal)
	return c
}

func (c *Client) AddResp(resp *Response) {
	if !c.IsClosed() {
		defer func() {
			recover()
		}()
		c.respChan <- resp
	}
}

func (c *Client) Close() {
	if !c.IsClosed() {
		atomic.AddUint32(&c.closed, 1)

		c.mtx.Lock()
		if !c.shutdown {
			c.shutdown = true
			c.mtx.Unlock()
		} else {
			c.mtx.Unlock()
			return
		}

		c.c.Close()
		close(c.respChan)

		//log.Printf("Close client %p\n", c)
	}
}

func (c *Client) LocalAddr() net.Addr {
	return c.c.LocalAddr()
}

func (c *Client) RemoteAddr() net.Addr {
	return c.c.RemoteAddr()
}

func (c *Client) IsClosed() bool {
	return atomic.LoadUint32(&c.closed) > 0
}

func (c *Client) SetClientType(cliType uint32) {
	atomic.StoreUint32(&c.cliType, cliType)
}

func (c *Client) ClientType() uint32 {
	return atomic.LoadUint32(&c.cliType)
}

// Check whether dbId is authencated.
// If dbId is 0, it means admin privilege.
func (c *Client) IsAuth(dbId uint8) bool {
	if c == nil || !c.authEnabled {
		return true
	}

	c.mtx.RLock()

	if c.authBM == nil {
		c.mtx.RUnlock()
		return false
	}

	if c.authBM.Get(0) {
		c.mtx.RUnlock()
		return true
	}

	if dbId != 0 && c.authBM.Get(uint(dbId)) {
		c.mtx.RUnlock()
		return true
	}

	c.mtx.RUnlock()
	return false
}

func (c *Client) SetAuth(dbId uint8) {
	if c != nil && c.authEnabled {
		c.mtx.Lock()
		if c.authBM == nil {
			c.authBM = util.NewBitMap(256 / 8)
		}

		c.authBM.Set(uint(dbId))
		c.mtx.Unlock()
	}
}

func (c *Client) GoRecvRequest(ch *RequestChan) {
	var headBuf = make([]byte, proto.HeadSize)
	var head proto.PkgHead
	for {
		pkg, err := proto.ReadPkg(c.r, headBuf, &head, nil)
		if err != nil {
			//log.Printf("ReadPkg failed: %s\n", err)
			c.Close()
			return
		}

		//log.Printf("recv(%s): [0x%X\t%d\t%d]\n",
		//	c.c.RemoteAddr(), head.Cmd, head.DbId, head.Seq)

		var req = Request{c, store.PkgArgs{head.Cmd, head.DbId, head.Seq, pkg}}

		switch head.Cmd {
		case proto.CmdAuth:
			fallthrough
		case proto.CmdPing:
			fallthrough
		case proto.CmdScan:
			fallthrough
		case proto.CmdMGet:
			fallthrough
		case proto.CmdGet:
			ch.ReadReqChan <- &req
		case proto.CmdMIncr:
			fallthrough
		case proto.CmdMDel:
			fallthrough
		case proto.CmdMSet:
			fallthrough
		case proto.CmdIncr:
			fallthrough
		case proto.CmdDel:
			fallthrough
		case proto.CmdSet:
			if ClientTypeNormal == c.ClientType() {
				ch.WriteReqChan <- &req
			} else {
				ch.SyncReqChan <- &req
			}
		case proto.CmdSyncSt:
			fallthrough
		case proto.CmdSync:
			if ClientTypeNormal != c.ClientType() {
				ch.SyncReqChan <- &req
			}
		case proto.CmdDump:
			ch.DumpReqChan <- &req
		case proto.CmdMigStatus:
			fallthrough
		case proto.CmdMigrate:
			fallthrough
		case proto.CmdSlaveOf:
			ch.CtrlReqChan <- &req
		default:
			log.Printf("Invalid cmd 0x%X\n", head.Cmd)
			c.Close()
			return
		}
	}
}

func (c *Client) GoSendResponse() {
	var err error
	for {
		select {
		case resp, ok := <-c.respChan:
			if !ok {
				//log.Printf("channel closed %p\n", c)
				return
			}

			if err == nil && !c.IsClosed() {
				//log.Printf("send(%s): [0x%X\t%d\t%d]\n",
				//	c.c.RemoteAddr(), resp.Cmd, resp.DbId, resp.Seq)
				_, err = c.c.Write(resp.Pkg)
				if err != nil {
					c.Close()
				}
			}
		}
	}
}
