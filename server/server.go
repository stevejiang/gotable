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
	var tableDir = fmt.Sprintf("%s/table", dbname)
	var binlogDir = fmt.Sprintf("%s/binlog", dbname)

	srv := new(Server)
	srv.tbl = store.NewTable(&srv.rwMtx, tableDir)
	if srv.tbl == nil {
		return nil
	}

	srv.reqChan = new(RequestChan)
	srv.reqChan.ReadReqChan = make(chan *Request, 1000)
	srv.reqChan.WriteReqChan = make(chan *Request, 1000)
	srv.reqChan.SyncReqChan = make(chan *Request, 64)
	srv.reqChan.DumpReqChan = make(chan *Request, 16)
	srv.reqChan.CtrlReqChan = make(chan *Request, 16)

	srv.bin = binlog.NewBinLog(binlogDir)

	return srv
}

func (srv *Server) sendResp(write, ok bool, req *Request, resp *Response) {
	switch req.Cli.ClientType() {
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
	switch req.Cli.ClientType() {
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
	switch req.Cli.ClientType() {
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
	switch req.Cli.ClientType() {
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
	switch req.Cli.ClientType() {
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
	switch req.Cli.ClientType() {
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
	switch req.Cli.ClientType() {
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
	switch req.Cli.ClientType() {
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

func (srv *Server) dump(req *Request) {
	switch req.Cli.ClientType() {
	case ClientTypeNormal:
		var resp = Response(req.PkgArgs)
		resp.Pkg = srv.tbl.Dump(&req.PkgArgs)
		srv.sendResp(false, true, req, &resp)
	case ClientTypeSlaver:
		log.Printf("Master cannot send DUMP command\n")
	case ClientTypeMaster:
		log.Printf("Slaver cannot send DUMP command\n")
	}
}

func (srv *Server) newMaster(de *ctrl.Decoder, en *ctrl.Encoder, req *Request) {
	switch req.Cli.ClientType() {
	case ClientTypeSlaver:
		var pm ctrl.PkgMaster
		err := de.Decode(req.Pkg, nil, &pm)
		req.Cli.Close()
		if err != nil {
			log.Printf("Failed to Decode pkg: %s\n", err)
			return
		}
		log.Printf("Master failed with msg: %s\n", err)
	case ClientTypeNormal:
		var pm ctrl.PkgMaster
		err := de.Decode(req.Pkg, nil, &pm)
		if err != nil {
			log.Printf("Failed to Decode pkg: %s\n", err)
			pm = ctrl.PkgMaster{}
			pm.ErrMsg = fmt.Sprintf("decode failed %s", err)
			var resp = Response(req.PkgArgs)
			resp.Pkg, err = en.Encode(req.Cmd, req.DbId, req.Seq, &pm)
			if err == nil {
				srv.sendResp(false, true, req, &resp)
			}
			return
		}

		req.Cli.SetClientType(ClientTypeMaster) // switch client type
		var startLogSeq = pm.LastSeq

		log.Printf("Receive a slave connection from %s, startLogSeq=%d\n",
			req.Cli.c.RemoteAddr(), startLogSeq)

		srv.rwMtx.Lock()
		var ms = newMaster(req.Cli, srv.bin, startLogSeq)
		srv.rwMtx.Unlock()

		srv.bin.RegisterMonitor(ms)
		go ms.goAsync(srv.tbl, &srv.rwMtx)
	case ClientTypeMaster:
		log.Printf("Already in master status, cannot create master again!\n")
		req.Cli.Close()
	}
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

func (srv *Server) processSync() {
	for {
		select {
		case req := <-srv.reqChan.SyncReqChan:
			if !req.Cli.IsClosed() {
				switch req.Cmd {
				case proto.CmdSet:
					srv.set(req)
				case proto.CmdDel:
					srv.del(req)
				case proto.CmdIncr:
					srv.incr(req)
				case proto.CmdMSet:
					srv.mSet(req)
				case proto.CmdMDel:
					srv.mDel(req)
				case proto.CmdMIncr:
					srv.mIncr(req)
				case proto.CmdSync:
					srv.sync(req)
				}
			}
		}
	}
}

func (srv *Server) processDump() {
	for {
		select {
		case req := <-srv.reqChan.DumpReqChan:
			if !req.Cli.IsClosed() {
				switch req.Cmd {
				case proto.CmdDump:
					srv.dump(req)
				}
			}
		}
	}
}

func (srv *Server) processCtrl() {
	var de = ctrl.NewDecoder()
	var en = ctrl.NewEncoder()
	for {
		select {
		case req := <-srv.reqChan.CtrlReqChan:
			if !req.Cli.IsClosed() {
				switch req.Cmd {
				case proto.CmdMaster:
					srv.newMaster(de, en, req)
				}
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
	var syncProcNum = 1 // Use 1 goroutine to make sure data consistency

	for i := 0; i < readProcNum; i++ {
		go srv.processRead()
	}
	for i := 0; i < writeProcNum; i++ {
		go srv.processWrite()
	}
	for i := 0; i < syncProcNum; i++ {
		go srv.processSync()
	}
	go srv.processDump()
	go srv.processCtrl()

	go srv.bin.GoWriteBinLog()

	link, err := net.Listen("tcp", host)
	if err != nil {
		log.Println("listen error: ", err)
	}

	log.Printf("GoTable started on (%s)! read: %d, write: %d, sync: %d\n",
		host, readProcNum, writeProcNum, syncProcNum)

	if masterHost != "" {
		var slv = newSlaver(masterHost, srv.reqChan, srv.bin)
		go slv.goConnectToMaster()
	}

	for {
		if c, err := link.Accept(); err == nil {
			log.Printf("New connection %s\n", c.RemoteAddr())

			cli := NewClient(c)
			go cli.GoReadRequest(srv.reqChan)
			go cli.GoSendResponse()
		}
	}
}
