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
	ReadReqChan  chan *Request
	WriteReqChan chan *Request
	SyncReqChan  chan *Request
}

type Client struct {
	cliType  int
	c        net.Conn
	r        *bufio.Reader
	respChan chan *Response

	// atomic
	closed uint32

	// protects following
	mtx      sync.Mutex
	ms       *master
	shutdown bool
}

func NewClient(conn net.Conn) *Client {
	cli := new(Client)
	cli.cliType = ClientTypeNormal
	cli.c = conn
	cli.r = bufio.NewReader(conn)
	cli.respChan = make(chan *Response, 64)
	return cli
}

func (cli *Client) AddResp(resp *Response) {
	if !cli.IsClosed() {
		defer recover()
		cli.respChan <- resp
	}
}

func (cli *Client) Close() {
	if !cli.IsClosed() {
		atomic.AddUint32(&cli.closed, 1)

		cli.mtx.Lock()
		if !cli.shutdown {
			cli.shutdown = true
			cli.mtx.Unlock()
		} else {
			cli.mtx.Unlock()
			return
		}

		cli.c.Close()

		if cli.ms != nil {
			cli.ms.Close()
			cli.ms = nil
		}

		defer recover()
		close(cli.respChan)
	}
}

func (cli *Client) IsClosed() bool {
	return atomic.LoadUint32(&cli.closed) > 0
}

func (cli *Client) SetMaster(ms *master) {
	cli.mtx.Lock()
	defer cli.mtx.Unlock()
	cli.ms = ms
}

func (cli *Client) GoReadRequest(ch *RequestChan) {
	var headBuf = make([]byte, proto.HeadSize)
	var head proto.PkgHead
	for {
		pkg, err := proto.ReadPkg(cli.r, headBuf, &head, nil)
		if err != nil {
			log.Printf("ReadPkg failed: %s\n", err)
			cli.Close()
			return
		}

		//log.Printf("recv(%s): [%d\t%d\t%d]\n",
		//	cli.c.RemoteAddr(), head.Cmd, head.DbId, head.Seq)

		var req = Request{cli, store.PkgArgs{head.Cmd, head.DbId, head.Seq, pkg}}

		switch head.Cmd {
		case proto.CmdMaster:
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
		case proto.CmdSync:
			fallthrough
		case proto.CmdSet:
			ch.WriteReqChan <- &req
		default:
			cli.Close()
			log.Printf("Invalid cmd %x\n", head.Cmd)
			return
		}
	}
}

func (cli *Client) GoSendResponse() {
	var err error
	for {
		select {
		case resp, ok := <-cli.respChan:
			if !ok {
				log.Printf("channel closed %p\n", cli)
				return
			}

			if err == nil && !cli.IsClosed() {
				//log.Printf("send(%s): [%d\t%d\t%d]\n",
				//	cli.c.RemoteAddr(), resp.Cmd, resp.DbId, resp.Seq)
				_, err = cli.c.Write(resp.Pkg)
				if err != nil {
					cli.Close()
				}
			}
		}
	}
}
