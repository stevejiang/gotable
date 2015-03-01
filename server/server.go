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
	"github.com/stevejiang/gotable/api/go/table"
	"github.com/stevejiang/gotable/api/go/table/proto"
	"github.com/stevejiang/gotable/binlog"
	"github.com/stevejiang/gotable/config"
	"github.com/stevejiang/gotable/ctrl"
	"github.com/stevejiang/gotable/store"
	"log"
	"net"
	"runtime"
	"sync"
)

const (
	slaverInitMode = iota + 1
	slaverCleanMode
	slaverReadyMode
)

type Server struct {
	tbl     *store.Table
	bin     *binlog.BinLog
	mc      *config.MasterConfig
	reqChan *RequestChan

	rwMtx     sync.RWMutex // protects following
	slaveMode int
	sls       []*slaver
}

func NewServer(conf *config.Config) *Server {
	var tableDir = fmt.Sprintf("%s/table", conf.Db.Data)
	var binlogDir = fmt.Sprintf("%s/binlog", conf.Db.Data)
	var configDir = fmt.Sprintf("%s/config", conf.Db.Data)

	srv := new(Server)
	srv.tbl = store.NewTable(tableDir)
	if srv.tbl == nil {
		return nil
	}

	srv.bin = binlog.NewBinLog(binlogDir,
		conf.Bin.MemSize*1024*1024, conf.Bin.KeepNum)
	if srv.bin == nil {
		return nil
	}

	srv.mc = config.NewMasterConfig(configDir)
	if srv.mc == nil {
		return nil
	}

	srv.reqChan = new(RequestChan)
	srv.reqChan.WriteReqChan = make(chan *Request, 1024)
	srv.reqChan.ReadReqChan = make(chan *Request, 1024)
	srv.reqChan.SyncReqChan = make(chan *Request, 64)
	srv.reqChan.DumpReqChan = make(chan *Request, 16)
	srv.reqChan.CtrlReqChan = make(chan *Request, 16)

	return srv
}

func (srv *Server) isMaster() bool {
	srv.rwMtx.RLock()
	isMaster := (srv.slaveMode == 0)
	srv.rwMtx.RUnlock()

	return isMaster
}

func (srv *Server) sendResp(write, ok bool, req *Request, resp *Response) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}

	switch cliType {
	case ClientTypeNormal:
		if req.Cli != nil {
			req.Cli.AddResp(resp)
		}
		if write && ok {
			srv.bin.AddRequest(&binlog.Request{0, 0, req.Pkg})
		}
	case ClientTypeSlaver:
		if write && ok {
			var masterId uint16 = 1
			srv.bin.AddRequest(&binlog.Request{masterId, req.Seq, req.Pkg})
		}
		if req.Cli != nil && !ok && resp.Pkg != nil {
			req.Cli.AddResp(resp)
		}
	}
}

func (srv *Server) replyOneOp(req *Request, errCode int8) {
	var out proto.PkgOneOp
	out.Cmd = req.Cmd
	out.DbId = req.DbId
	out.Seq = req.Seq
	out.ErrCode = errCode
	if out.ErrCode != 0 {
		out.CtrlFlag |= proto.CtrlErrCode
	}

	var pkg = make([]byte, out.Length())
	_, err := out.Encode(pkg)
	if err != nil {
		log.Fatalf("Encode failed: %s\n", err)
	}

	var resp = Response(req.PkgArgs)
	resp.Pkg = pkg
	srv.sendResp(false, true, req, &resp)
}

func (srv *Server) replyMultiOp(req *Request, errCode int8) {
	var out proto.PkgMultiOp
	out.Cmd = req.Cmd
	out.DbId = req.DbId
	out.Seq = req.Seq
	out.ErrCode = errCode

	var pkg = make([]byte, out.Length())
	_, err := out.Encode(pkg)
	if err != nil {
		log.Fatalf("Encode failed: %s\n", err)
	}

	var resp = Response(req.PkgArgs)
	resp.Pkg = pkg
	srv.sendResp(false, true, req, &resp)
}

func (srv *Server) auth(req *Request) {
	var resp = Response(req.PkgArgs)
	resp.Pkg = srv.tbl.Auth(&req.PkgArgs, req.Cli)
	srv.sendResp(false, true, req, &resp)
}

func (srv *Server) ping(req *Request) {
	var resp = Response(req.PkgArgs)
	srv.sendResp(false, true, req, &resp)
}

func (srv *Server) get(req *Request) {
	var resp = Response(req.PkgArgs)
	resp.Pkg = srv.tbl.Get(&req.PkgArgs, req.Cli)
	srv.sendResp(false, true, req, &resp)
}

func (srv *Server) set(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var replication = (ClientTypeSlaver == cliType)
	switch cliType {
	case ClientTypeNormal:
		if !srv.isMaster() {
			srv.replyOneOp(req, table.EcWriteSlaver)
			return
		}
		fallthrough
	case ClientTypeSlaver:
		var resp = Response(req.PkgArgs)
		var ok bool
		resp.Pkg, ok = srv.tbl.Set(&req.PkgArgs, req.Cli, replication)
		srv.sendResp(true, ok, req, &resp)
	case ClientTypeMaster:
		log.Printf("Slaver SET failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) del(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var replication = (ClientTypeSlaver == cliType)
	switch cliType {
	case ClientTypeNormal:
		if !srv.isMaster() {
			srv.replyOneOp(req, table.EcWriteSlaver)
			return
		}
		fallthrough
	case ClientTypeSlaver:
		var resp = Response(req.PkgArgs)
		var ok bool
		resp.Pkg, ok = srv.tbl.Del(&req.PkgArgs, req.Cli, replication)
		srv.sendResp(true, ok, req, &resp)
	case ClientTypeMaster:
		log.Printf("Slaver DEL failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) incr(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var replication = (ClientTypeSlaver == cliType)
	switch cliType {
	case ClientTypeNormal:
		if !srv.isMaster() {
			srv.replyOneOp(req, table.EcWriteSlaver)
			return
		}
		fallthrough
	case ClientTypeSlaver:
		var resp = Response(req.PkgArgs)
		var ok bool
		resp.Pkg, ok = srv.tbl.Incr(&req.PkgArgs, req.Cli, replication)
		srv.sendResp(true, ok, req, &resp)
	case ClientTypeMaster:
		log.Printf("Slaver INCR failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) mGet(req *Request) {
	var resp = Response(req.PkgArgs)
	resp.Pkg = srv.tbl.MGet(&req.PkgArgs, req.Cli)
	srv.sendResp(false, true, req, &resp)
}

func (srv *Server) mSet(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var replication = (ClientTypeSlaver == cliType)
	switch cliType {
	case ClientTypeNormal:
		if !srv.isMaster() {
			srv.replyMultiOp(req, table.EcWriteSlaver)
			return
		}
		fallthrough
	case ClientTypeSlaver:
		var resp = Response(req.PkgArgs)
		var ok bool
		resp.Pkg, ok = srv.tbl.MSet(&req.PkgArgs, req.Cli, replication)
		srv.sendResp(true, ok, req, &resp)
	case ClientTypeMaster:
		log.Printf("Slaver MSET failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) mDel(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var replication = (ClientTypeSlaver == cliType)
	switch cliType {
	case ClientTypeNormal:
		if !srv.isMaster() {
			srv.replyMultiOp(req, table.EcWriteSlaver)
			return
		}
		fallthrough
	case ClientTypeSlaver:
		var resp = Response(req.PkgArgs)
		var ok bool
		resp.Pkg, ok = srv.tbl.MDel(&req.PkgArgs, req.Cli, replication)
		srv.sendResp(true, ok, req, &resp)
	case ClientTypeMaster:
		log.Printf("Slaver MDEL failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) mIncr(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var replication = (ClientTypeSlaver == cliType)
	switch cliType {
	case ClientTypeNormal:
		if !srv.isMaster() {
			srv.replyMultiOp(req, table.EcWriteSlaver)
			return
		}
		fallthrough
	case ClientTypeSlaver:
		var resp = Response(req.PkgArgs)
		var ok bool
		resp.Pkg, ok = srv.tbl.MIncr(&req.PkgArgs, req.Cli, replication)
		srv.sendResp(true, ok, req, &resp)
	case ClientTypeMaster:
		log.Printf("Slaver MINCR failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) scan(req *Request) {
	var resp = Response(req.PkgArgs)
	resp.Pkg = srv.tbl.Scan(&req.PkgArgs, req.Cli)
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
	var resp = Response(req.PkgArgs)
	resp.Pkg = srv.tbl.Dump(&req.PkgArgs, req.Cli)
	srv.sendResp(false, true, req, &resp)
}

func (srv *Server) replyMaster(en *ctrl.Encoder, req *Request, msg string) {
	var err error
	pm := ctrl.PkgMaster{}
	pm.ErrMsg = msg
	var resp = Response(req.PkgArgs)
	resp.Pkg, err = en.Encode(req.Cmd, req.DbId, req.Seq, &pm)
	if err == nil {
		srv.sendResp(false, true, req, &resp)
	}
}

func (srv *Server) newMaster(de *ctrl.Decoder, en *ctrl.Encoder, req *Request) {
	var err error
	if !req.Cli.IsAuth(0) {
		log.Printf("Not authorized!\n")
		srv.replyMaster(en, req, "no priviledge")
		return
	}

	switch req.Cli.ClientType() {
	case ClientTypeNormal:
		var pm ctrl.PkgMaster
		err = de.Decode(req.Pkg, nil, &pm)
		if err != nil {
			log.Printf("Failed to Decode pkg: %s\n", err)
			srv.replyMaster(en, req, fmt.Sprintf("decode failed %s", err))
			return
		}

		req.Cli.SetClientType(ClientTypeMaster) // switch client type

		log.Printf("Receive a slave connection from %s, lastSeq=%d\n",
			req.Cli.c.RemoteAddr(), pm.LastSeq)
		for i := 0; i < len(pm.Urs); i++ {
			log.Printf("Unit Range %d: [%d-%d]\n",
				i, pm.Urs[i].Start, pm.Urs[i].End)
		}

		var ms = newMaster(pm.SlaveAddr, pm.LastSeq, pm.Urs, req.Cli, srv.bin)
		srv.bin.RegisterMonitor(ms)
		go ms.goAsync(srv.tbl)
	case ClientTypeSlaver:
		var pm ctrl.PkgMaster
		err = de.Decode(req.Pkg, nil, &pm)
		req.Cli.Close()
		if err != nil {
			log.Printf("Failed to Decode pkg: %s\n", err)
			return
		}
		log.Printf("Master failed with msg: %s\n", pm.ErrMsg)
	case ClientTypeMaster:
		log.Printf("Already in master status, cannot create master again!\n")
		req.Cli.Close()
	}
}

func (srv *Server) replySlaveOf(en *ctrl.Encoder, req *Request, msg string) {
	var err error
	ps := ctrl.PkgSlaveOf{}
	ps.ErrMsg = msg
	var resp = Response(req.PkgArgs)
	resp.Pkg, err = en.Encode(req.Cmd, req.DbId, req.Seq, &ps)
	if err == nil {
		srv.sendResp(false, true, req, &resp)
	}
}

func (srv *Server) slaveOf(de *ctrl.Decoder, en *ctrl.Encoder, req *Request) {
	var err error
	if !req.Cli.IsAuth(0) {
		log.Printf("Not authorized!\n")
		srv.replySlaveOf(en, req, "no priviledge")
		return
	}

	switch req.Cli.ClientType() {
	case ClientTypeNormal:
		var ps ctrl.PkgSlaveOf
		err = de.Decode(req.Pkg, nil, &ps)
		if err != nil {
			log.Printf("Failed to Decode pkg: %s\n", err)
			srv.replySlaveOf(en, req, fmt.Sprintf("decode failed %s", err))
			return
		}

		for i := 0; i < len(ps.Mis); i++ {
			if len(ps.Mis[i].Urs) == 0 {
				ps.Mis[i].Urs = append(ps.Mis[i].Urs,
					ctrl.UnitRange{0, uint16(ctrl.TotalUnitNum - 1)})
			}
		}

		err = srv.mc.Set(ps.Mis)
		if err != nil {
			log.Printf("Failed to set config: %s\n", err)
			srv.replySlaveOf(en, req, fmt.Sprintf("set config failed %s", err))
			return
		}

		srv.rwMtx.Lock()
		sls := srv.sls
		srv.sls = nil
		if len(ps.Mis) == 0 {
			srv.slaveMode = 0
		} else {
			srv.slaveMode = slaverInitMode
		}
		srv.rwMtx.Unlock()

		for i := 0; i < len(sls); i++ {
			sls[i].Close()
		}

		go srv.connectToMaster(ps.Mis)

		srv.replySlaveOf(en, req, "") // Success

	case ClientTypeSlaver:
		fallthrough
	case ClientTypeMaster:
		log.Println("Invalid client type for SlaveOf command")
		req.Cli.Close()
	}
}

func (srv *Server) connectToMaster(mis []ctrl.MasterInfo) {
	// Remove old data
	//TODO

	// Connect to master
	var sls []*slaver
	for i := 0; i < len(mis); i++ {
		var slv = newSlaver(mis[i], srv.reqChan, srv.bin)
		go slv.goConnectToMaster()
		sls = append(sls, slv)
	}

	srv.rwMtx.Lock()
	srv.sls = sls
	srv.rwMtx.Unlock()
}

func (srv *Server) doProcess(req *Request) {
	switch req.Cmd {
	case proto.CmdAuth:
		srv.auth(req)
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
				case proto.CmdSlaveOf:
					srv.slaveOf(de, en, req)
				}
			}
		}
	}
}

func Run(conf *config.Config) {
	var srv = NewServer(conf)
	if srv == nil {
		log.Fatalln("Failed to create new server.")
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

	log.Printf("Goroutine distribution: read %d, write %d, sync %d, %s\n",
		readProcNum, writeProcNum, syncProcNum, "dump 1, ctrl 1")

	link, err := net.Listen("tcp", conf.Db.Host)
	if err != nil {
		log.Fatalln("Listen failed:", err)
	}

	log.Printf("GoTable started on %s\n", conf.Db.Host)

	mis := srv.mc.GetAllMaster()
	if len(mis) > 0 {
		srv.connectToMaster(mis)
	}

	var authEnabled bool
	if conf.Auth.AdminPwd != "" {
		authEnabled = true
		srv.tbl.SetPassword(proto.AdminDbId, conf.Auth.AdminPwd)
	}

	for {
		if c, err := link.Accept(); err == nil {
			//log.Printf("New connection %s\n", c.RemoteAddr())

			cli := NewClient(c, authEnabled)
			go cli.GoRecvRequest(srv.reqChan)
			go cli.GoSendResponse()
		}
	}
}
