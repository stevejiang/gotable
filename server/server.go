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
	"fmt"
	"github.com/stevejiang/gotable/api/go/table/proto"
	"github.com/stevejiang/gotable/binlog"
	"github.com/stevejiang/gotable/ctrl"
	"github.com/stevejiang/gotable/store"
	"log"
	"net"
	"os"
	"runtime"
	"sync"
)

type Server struct {
	tbl     *store.Table
	reqChan *RequestChan
	bin     *binlog.BinLog
	rwMtx   sync.RWMutex
}

func NewServer(dbname string) *Server {
	var binlogDir = fmt.Sprintf("%s/binlog", dbname)
	var tableDir = fmt.Sprintf("%s/table", dbname)
	os.MkdirAll(binlogDir, os.ModeDir|os.ModePerm)
	os.MkdirAll(tableDir, os.ModeDir|os.ModePerm)

	srv := new(Server)

	srv.tbl = store.NewTable(&srv.rwMtx, tableDir)
	if srv.tbl == nil {
		return nil
	}

	srv.reqChan = new(RequestChan)
	srv.reqChan.ReadReqChan = make(chan *Request, 10000)
	srv.reqChan.WriteReqChan = make(chan *Request, 10000)

	srv.bin = binlog.NewBinLog(binlogDir)

	return srv
}

func (srv *Server) sendResp(write, ok bool, req *Request, resp *Response) {
	switch req.Cli.cliType {
	case ClientTypeNormal:
		req.Cli.AddResp(resp)
		if write && ok {
			srv.bin.AddRequest(&binlog.Request{0, 0, req.Pkg})
		}
	case ClientTypeSlaver:
		if write && ok {
			var masterId uint16 = 1
			srv.bin.AddRequest(&binlog.Request{masterId, req.Seq, req.Pkg})
		}
		if !ok && resp.Pkg != nil {
			req.Cli.AddResp(resp)
		}
	}
}

func (srv *Server) ping(req *Request) {
	var resp = Response(req.PkgArgs)

	srv.sendResp(false, true, req, &resp)
}

func (srv *Server) get(req *Request) {
	var resp = Response(req.PkgArgs)
	resp.Pkg = srv.tbl.Get(&req.PkgArgs)

	srv.sendResp(false, true, req, &resp)
}

func (srv *Server) set(req *Request) {
	switch req.Cli.cliType {
	case ClientTypeSlaver:
		fallthrough
	case ClientTypeNormal:
		var resp = Response(req.PkgArgs)
		var ok bool
		resp.Pkg, ok = srv.tbl.Set(&req.PkgArgs)
		srv.sendResp(true, ok, req, &resp)
	case ClientTypeMaster:
		log.Printf("Slaver set failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) del(req *Request) {
	switch req.Cli.cliType {
	case ClientTypeSlaver:
		fallthrough
	case ClientTypeNormal:
		var resp = Response(req.PkgArgs)
		var ok bool
		resp.Pkg, ok = srv.tbl.Del(&req.PkgArgs)
		srv.sendResp(true, ok, req, &resp)
	case ClientTypeMaster:
		log.Printf("Slaver del failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) incr(req *Request) {
	switch req.Cli.cliType {
	case ClientTypeSlaver:
		fallthrough
	case ClientTypeNormal:
		var resp = Response(req.PkgArgs)
		var ok bool
		resp.Pkg, ok = srv.tbl.Incr(&req.PkgArgs)
		srv.sendResp(true, ok, req, &resp)
	case ClientTypeMaster:
		log.Printf("Slaver incr failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) mGet(req *Request) {
	var resp = Response(req.PkgArgs)
	resp.Pkg = srv.tbl.MGet(&req.PkgArgs)

	srv.sendResp(false, true, req, &resp)
}

func (srv *Server) mSet(req *Request) {
	switch req.Cli.cliType {
	case ClientTypeSlaver:
		fallthrough
	case ClientTypeNormal:
		var resp = Response(req.PkgArgs)
		var ok bool
		resp.Pkg, ok = srv.tbl.MSet(&req.PkgArgs)
		srv.sendResp(true, ok, req, &resp)
	case ClientTypeMaster:
		log.Printf("Slaver mSet failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) mDel(req *Request) {
	switch req.Cli.cliType {
	case ClientTypeSlaver:
		fallthrough
	case ClientTypeNormal:
		var resp = Response(req.PkgArgs)
		var ok bool
		resp.Pkg, ok = srv.tbl.MDel(&req.PkgArgs)
		srv.sendResp(true, ok, req, &resp)
	case ClientTypeMaster:
		log.Printf("Slaver mDel failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) mIncr(req *Request) {
	switch req.Cli.cliType {
	case ClientTypeSlaver:
		fallthrough
	case ClientTypeNormal:
		var resp = Response(req.PkgArgs)
		var ok bool
		resp.Pkg, ok = srv.tbl.MIncr(&req.PkgArgs)
		srv.sendResp(true, ok, req, &resp)
	case ClientTypeMaster:
		log.Printf("Slaver mIncr failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) scan(req *Request) {
	var resp = Response(req.PkgArgs)
	resp.Pkg = srv.tbl.Scan(&req.PkgArgs)

	srv.sendResp(false, true, req, &resp)
}

func (srv *Server) sync(req *Request) {
	switch req.Cli.cliType {
	case ClientTypeSlaver:
		var resp = Response(req.PkgArgs)
		var ok bool
		resp.Pkg, ok = srv.tbl.Sync(&req.PkgArgs)

		srv.sendResp(true, ok, req, &resp)
		if !ok && resp.Pkg != nil {
			req.Cli.AddResp(&resp) // Reply Error msg
		}
	case ClientTypeNormal:
		log.Printf("User cannot send SYNC command\n")
	case ClientTypeMaster:
		log.Printf("Slaver sync failed: [%d, %d]\n", req.DbId, req.Seq)
		req.Cli.Close() // stop full sync
	}
}

func (srv *Server) newMaster(req *Request) {
	var in ctrl.PkgCmdMasterReq
	in.Decode(req.Pkg)

	req.Cli.cliType = ClientTypeMaster
	var startLogSeq = in.LastSeq

	log.Printf("receive a slave connection from %s, startLogSeq=%d\n",
		req.Cli.c.RemoteAddr(), startLogSeq)

	srv.rwMtx.Lock()
	var ms = newMaster(req.Cli, srv.bin, startLogSeq)
	srv.rwMtx.Unlock()

	srv.bin.RegisterMonitor(ms)
	go ms.goAsync(srv.tbl, &srv.rwMtx)
}

func (srv *Server) doProcess(req *Request) {
	switch req.Cmd {
	case proto.CmdPing:
		srv.ping(req)
	case proto.CmdGet:
		srv.get(req)
	case proto.CmdSet:
		srv.set(req)
	case proto.CmdDel:
		srv.del(req)
	case proto.CmdIncr:
		srv.incr(req)
	case proto.CmdMGet:
		srv.mGet(req)
	case proto.CmdMSet:
		srv.mSet(req)
	case proto.CmdMDel:
		srv.mDel(req)
	case proto.CmdMIncr:
		srv.mIncr(req)
	case proto.CmdScan:
		srv.scan(req)
	case proto.CmdSync:
		srv.sync(req)
	case proto.CmdMaster:
		srv.newMaster(req)
	}
}

func (srv *Server) processRead() {
	for {
		select {
		case req := <-srv.reqChan.ReadReqChan:
			if !req.Cli.IsClosed() {
				srv.doProcess(req)
			}
		}
	}
}

func (srv *Server) processWrite() {
	for {
		select {
		case req := <-srv.reqChan.WriteReqChan:
			if !req.Cli.IsClosed() {
				srv.doProcess(req)
			}
		}
	}
}

func Run(dbName, host, masterHost string) {
	var srv = NewServer(dbName)
	if srv == nil {
		log.Println("Failed to create new server.")
		return
	}

	var totalProcNum = runtime.NumCPU() * 2
	var writeProcNum = totalProcNum / 4
	var readProcNum = totalProcNum - writeProcNum
	if writeProcNum < 2 {
		writeProcNum = 2
	}

	for i := 0; i < readProcNum; i++ {
		go srv.processRead()
	}

	for i := 0; i < writeProcNum; i++ {
		go srv.processWrite()
	}

	go srv.bin.GoWriteBinLog()

	link, err := net.Listen("tcp", host)
	if err != nil {
		log.Println("listen error: ", err)
	}

	log.Printf("Listen TCP started! readProcNum=%d, writeProcNum=%d\n",
		readProcNum, writeProcNum)

	if masterHost != "" {
		var slv = newSlaver(masterHost, srv.reqChan, srv.bin)
		go slv.goConnectToMaster()
	}

	for {
		if c, err := link.Accept(); err == nil {
			log.Println(c.RemoteAddr().(*net.TCPAddr))

			cli := NewClient(c)
			//runtime.SetFinalizer(cli, func(cli *client) {
			//	log.Printf("finalized %p\n", cli)
			//})
			go cli.GoReadRequest(srv.reqChan)
			go cli.GoSendResponse()
		}
	}
}
