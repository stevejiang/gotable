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
	"github.com/stevejiang/gotable/util"
	"log"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Server struct {
	tbl     *store.Table
	bin     *binlog.BinLog
	conf    *config.Config
	mc      *config.MasterConfig
	reqChan *RequestChan

	// Atomic
	closed uint32

	rwMtx     sync.RWMutex // protects following
	slv       *slave
	readyTime time.Time
}

func NewServer(conf *config.Config) *Server {
	var tableDir = TableDirName(conf)
	var binlogDir = BinLogDirName(conf)
	var configDir = ConfigDirName(conf)

	mc := config.NewMasterConfig(configDir)
	if mc == nil {
		return nil
	}

	err := clearSlaveOldData(conf, mc)
	if err != nil {
		return nil
	}

	srv := new(Server)
	srv.conf = conf
	srv.mc = mc
	srv.tbl = store.NewTable(tableDir, getMaxOpenFiles(),
		conf.Db.WriteBufSize, conf.Db.CacheSize, conf.Db.Compression)
	if srv.tbl == nil {
		return nil
	}

	srv.bin = binlog.NewBinLog(binlogDir,
		conf.Bin.MemSize*1024*1024, conf.Bin.KeepNum)
	if srv.bin == nil {
		return nil
	}

	srv.reqChan = new(RequestChan)
	srv.reqChan.WriteReqChan = make(chan *Request, 1024)
	srv.reqChan.ReadReqChan = make(chan *Request, 1024)
	srv.reqChan.SyncReqChan = make(chan *Request, 64)
	srv.reqChan.DumpReqChan = make(chan *Request, 16)
	srv.reqChan.CtrlReqChan = make(chan *Request, 16)

	var maxProcs = conf.Db.MaxCpuNum
	if maxProcs <= 0 {
		maxProcs = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(maxProcs)

	log.Printf("NumCPU %d, GOMAXPROCS %d\n", runtime.NumCPU(), maxProcs)

	var totalProcNum = maxProcs * 2
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
	go srv.processSync() // Use 1 goroutine to make sure data consistency
	go srv.processDump()
	go srv.processCtrl()

	log.Printf("Goroutine distribution: read %d, write %d, %s\n",
		readProcNum, writeProcNum, "sync 1, dump 1, ctrl 1")

	return srv
}

func (srv *Server) Start() {
	var authEnabled bool
	if srv.conf.Auth.AdminPwd != "" {
		authEnabled = true
		srv.tbl.SetPassword(proto.AdminDbId, srv.conf.Auth.AdminPwd)
	}

	// Normal slave, reconnect to master
	hasMaster, migration, _ := srv.mc.GetMasterUnit()
	if hasMaster && !migration {
		lastSeq, valid := srv.bin.GetMasterSeq()
		if !valid {
			srv.mc.SetStatus(ctrl.SlaveNeedClear)
			// Any better solution?
			log.Fatalf("Slave lastSeq %d is out of sync, please clear old data! "+
				"(Restart may fix this issue)", lastSeq)
		}

		srv.bin.AsSlave()
		srv.connectToMaster(srv.mc)
	}

	link, err := net.Listen(srv.conf.Db.Network, srv.conf.Db.Address)
	if err != nil {
		log.Fatalln("Listen failed:", err)
	}

	log.Printf("GoTable %s started on %s://%s\n",
		table.Version, srv.conf.Db.Network, srv.conf.Db.Address)

	for {
		if c, err := link.Accept(); err == nil {
			//log.Printf("New connection %s\t%s\n", c.RemoteAddr(), c.LocalAddr())

			cli := NewClient(c, authEnabled)
			go cli.GoRecvRequest(srv.reqChan, nil)
			go cli.GoSendResponse()
		}
	}
}

// Stop server and exit
func (srv *Server) Close() {
	if !srv.IsClosed() {
		atomic.AddUint32(&srv.closed, 1)

		// Flush data to file system
		_, chanLen := srv.bin.GetLogSeqChanLen()
		for i := 0; chanLen != 0 && i < 5000; i++ {
			time.Sleep(time.Millisecond)
			_, chanLen = srv.bin.GetLogSeqChanLen()
		}

		time.Sleep(time.Millisecond * 50)

		srv.bin.Close()
		//srv.tbl.Close()
		os.Exit(0)
	}
}

func (srv *Server) IsClosed() bool {
	return atomic.LoadUint32(&srv.closed) > 0
}

func TableDirName(conf *config.Config) string {
	return fmt.Sprintf("%s/table", conf.Db.Data)
}

func BinLogDirName(conf *config.Config) string {
	return fmt.Sprintf("%s/binlog", conf.Db.Data)
}

func ConfigDirName(conf *config.Config) string {
	return fmt.Sprintf("%s/config", conf.Db.Data)
}

func BackupDirName(conf *config.Config) string {
	return fmt.Sprintf("%s/backup", conf.Db.Data)
}

func clearSlaveOldData(conf *config.Config, mc *config.MasterConfig) error {
	var err error
	m := mc.GetMaster()
	// Normal slave
	if len(m.MasterAddr) > 0 && !m.Migration {
		// Check whether need to clear old data
		if m.Status != ctrl.SlaveNeedClear && m.Status != ctrl.SlaveClear {
			return nil
		}

		err = mc.SetStatus(ctrl.SlaveClear)
		if err != nil {
			return err
		}

		// Move binlog & table to backup in case you need the old data
		var backupDir = BackupDirName(conf)
		err = os.RemoveAll(backupDir)
		if err != nil {
			return err
		}

		err = os.MkdirAll(backupDir, os.ModeDir|os.ModePerm)
		if err != nil {
			return err
		}

		var binlogDir = BinLogDirName(conf)
		log.Println("Move directory binlog to backup")
		err = os.Rename(binlogDir, backupDir+"/binlog")
		if err != nil {
			return err
		}

		var tableDir = TableDirName(conf)
		log.Println("Move directory table to backup")
		err = os.Rename(tableDir, backupDir+"/table")
		if err != nil {
			return err
		}

		err = mc.SetStatus(ctrl.SlaveInit)
		if err != nil {
			return err
		}
	}

	return nil
}

func getMaxOpenFiles() int {
	var rlim syscall.Rlimit
	var err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim)
	if err == nil && rlim.Cur < rlim.Max {
		rlim.Cur = rlim.Max
		syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlim)
	}
	var maxOpenFiles = 1024
	if maxOpenFiles < int(rlim.Cur) {
		maxOpenFiles = int(rlim.Cur)
	}
	return maxOpenFiles
}

func (srv *Server) sendResp(write bool, req *Request, pkg []byte) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()

		if pkg != nil {
			req.Cli.AddResp(pkg)
		}
	}

	switch cliType {
	case ClientTypeNormal:
		if write {
			srv.bin.AddRequest(&binlog.Request{0, req.Pkg})
		}
	case ClientTypeSlave:
		if write {
			srv.bin.AddRequest(&binlog.Request{req.Seq, req.Pkg})
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

	srv.sendResp(false, req, pkg)
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

	srv.sendResp(false, req, pkg)
}

func (srv *Server) auth(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}

	switch cliType {
	case ClientTypeNormal:
		var pkg = srv.tbl.Auth(&req.PkgArgs, req.Cli)
		srv.sendResp(false, req, pkg)
	case ClientTypeSlave:
		var in proto.PkgOneOp
		_, err := in.Decode(req.Pkg)
		if err != nil {
			log.Printf("Decode failed for auth reply(%s), close slave!\n", err)
		} else if in.ErrCode != 0 {
			log.Printf("Auth failed (%d), close slave!\n", in.ErrCode)
		}
		if err != nil || in.ErrCode != 0 {
			if req.Slv != nil {
				req.Slv.Close()
			} else {
				req.Cli.Close()
			}
			return
		}
		// Slave auth succeed
		if req.Slv != nil {
			err = req.Slv.SendSlaveOfToMaster()
			if err != nil {
				log.Printf("SendSlaveOfToMaster failed(%s), close slave!\n", err)
				req.Slv.Close()
			}
		}
	case ClientTypeMaster:
		log.Printf("Invalid client type %d for AUTH cmd, close now!\n", cliType)
		req.Cli.Close()
	}
}

func (srv *Server) ping(req *Request) {
	srv.sendResp(false, req, req.Pkg)
}

func (srv *Server) get(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var wa = store.NewWriteAccess(ClientTypeSlave == cliType, srv.mc)

	var pkg = srv.tbl.Get(&req.PkgArgs, req.Cli, wa)
	srv.sendResp(false, req, pkg)
}

func (srv *Server) set(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var wa = store.NewWriteAccess(ClientTypeSlave == cliType, srv.mc)
	switch cliType {
	case ClientTypeNormal:
		if !wa.Check() {
			srv.replyOneOp(req, table.EcWriteSlave)
			return
		}
		pkg, ok := srv.tbl.Set(&req.PkgArgs, req.Cli, wa)
		srv.sendResp(ok, req, pkg)
	case ClientTypeSlave:
		pkg, ok := srv.tbl.Set(&req.PkgArgs, req.Cli, wa)
		if ok {
			srv.sendResp(ok, req, nil)
		} else {
			srv.sendResp(ok, req, pkg)
		}
	case ClientTypeMaster:
		log.Printf("Slave SET failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) del(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var wa = store.NewWriteAccess(ClientTypeSlave == cliType, srv.mc)
	switch cliType {
	case ClientTypeNormal:
		if !wa.Check() {
			srv.replyOneOp(req, table.EcWriteSlave)
			return
		}
		pkg, ok := srv.tbl.Del(&req.PkgArgs, req.Cli, wa)
		srv.sendResp(ok, req, pkg)
	case ClientTypeSlave:
		pkg, ok := srv.tbl.Del(&req.PkgArgs, req.Cli, wa)
		if ok {
			srv.sendResp(ok, req, nil)
		} else {
			srv.sendResp(ok, req, pkg)
		}
	case ClientTypeMaster:
		log.Printf("Slave DEL failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) incr(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var wa = store.NewWriteAccess(ClientTypeSlave == cliType, srv.mc)
	switch cliType {
	case ClientTypeNormal:
		if !wa.Check() {
			srv.replyOneOp(req, table.EcWriteSlave)
			return
		}
		pkg, ok := srv.tbl.Incr(&req.PkgArgs, req.Cli, wa)
		srv.sendResp(ok, req, pkg)
	case ClientTypeSlave:
		pkg, ok := srv.tbl.Incr(&req.PkgArgs, req.Cli, wa)
		if ok {
			srv.sendResp(ok, req, nil)
		} else {
			srv.sendResp(ok, req, pkg)
		}
	case ClientTypeMaster:
		log.Printf("Slave INCR failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) mGet(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var wa = store.NewWriteAccess(ClientTypeSlave == cliType, srv.mc)

	var pkg = srv.tbl.MGet(&req.PkgArgs, req.Cli, wa)
	srv.sendResp(false, req, pkg)
}

func (srv *Server) mSet(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var wa = store.NewWriteAccess(ClientTypeSlave == cliType, srv.mc)
	switch cliType {
	case ClientTypeNormal:
		if !wa.Check() {
			srv.replyMultiOp(req, table.EcWriteSlave)
			return
		}
		pkg, ok := srv.tbl.MSet(&req.PkgArgs, req.Cli, wa)
		srv.sendResp(ok, req, pkg)
	case ClientTypeSlave:
		pkg, ok := srv.tbl.MSet(&req.PkgArgs, req.Cli, wa)
		if ok {
			srv.sendResp(ok, req, nil)
		} else {
			srv.sendResp(ok, req, pkg)
		}
	case ClientTypeMaster:
		log.Printf("Slave MSET failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) mDel(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var wa = store.NewWriteAccess(ClientTypeSlave == cliType, srv.mc)
	switch cliType {
	case ClientTypeNormal:
		if !wa.Check() {
			srv.replyMultiOp(req, table.EcWriteSlave)
			return
		}
		pkg, ok := srv.tbl.MDel(&req.PkgArgs, req.Cli, wa)
		srv.sendResp(ok, req, pkg)
	case ClientTypeSlave:
		pkg, ok := srv.tbl.MDel(&req.PkgArgs, req.Cli, wa)
		if ok {
			srv.sendResp(ok, req, nil)
		} else {
			srv.sendResp(ok, req, pkg)
		}
	case ClientTypeMaster:
		log.Printf("Slave MDEL failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) mIncr(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var wa = store.NewWriteAccess(ClientTypeSlave == cliType, srv.mc)
	switch cliType {
	case ClientTypeNormal:
		if !wa.Check() {
			srv.replyMultiOp(req, table.EcWriteSlave)
			return
		}
		pkg, ok := srv.tbl.MIncr(&req.PkgArgs, req.Cli, wa)
		srv.sendResp(ok, req, pkg)
	case ClientTypeSlave:
		pkg, ok := srv.tbl.MIncr(&req.PkgArgs, req.Cli, wa)
		if ok {
			srv.sendResp(ok, req, nil)
		} else {
			srv.sendResp(ok, req, pkg)
		}
	case ClientTypeMaster:
		log.Printf("Slave MINCR failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) scan(req *Request) {
	var pkg = srv.tbl.Scan(&req.PkgArgs, req.Cli)
	srv.sendResp(false, req, pkg)
}

func (srv *Server) sync(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	switch cliType {
	case ClientTypeSlave:
		pkg, ok := srv.tbl.Sync(&req.PkgArgs)
		if ok {
			srv.sendResp(ok, req, nil)
		} else {
			srv.sendResp(ok, req, pkg)
		}
	case ClientTypeNormal:
		log.Printf("User cannot send SYNC command, close now!\n")
		req.Cli.Close()
	case ClientTypeMaster:
		log.Printf("Slave SYNC failed: [%d, %d], close now!\n", req.DbId, req.Seq)
		req.Cli.Close()
	}
}

func (srv *Server) syncStatus(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	switch cliType {
	case ClientTypeSlave:
		var in proto.PkgOneOp
		_, err := in.Decode(req.Pkg)
		if err != nil || len(in.RowKey) == 0 {
			return
		}

		rowKey := string(in.RowKey)
		switch rowKey {
		case store.KeyFullSyncEnd:
			srv.mc.SetStatus(ctrl.SlaveIncrSync)
			log.Printf("Switch sync status to SlaveIncrSync\n")
		case store.KeyIncrSyncEnd:
			var st = srv.mc.Status()
			srv.mc.SetStatus(ctrl.SlaveReady)

			var now = time.Now()
			srv.rwMtx.Lock()
			var lastTime = srv.readyTime
			srv.readyTime = now
			srv.rwMtx.Unlock()

			if st != ctrl.SlaveReady || now.Sub(lastTime).Seconds() > 120 {
				log.Printf("Switch sync status to SlaveReady")
			}
		case store.KeySyncLogMissing:
			srv.mc.SetStatus(ctrl.SlaveNeedClear)
			lastSeq, _ := srv.bin.GetMasterSeq()
			// Any better solution?
			log.Fatalf("Slave lastSeq %d is out of sync, please clear old data! "+
				"(Restart may fix this issue)", lastSeq)
		}

		if req.Seq > 0 {
			in.RowKey = nil // Set it as an empty OP
			req.Pkg = make([]byte, in.Length())
			in.Encode(req.Pkg)
			srv.sendResp(true, req, nil)
		}
	case ClientTypeNormal:
		log.Printf("User cannot send SYNCST command\n")
	case ClientTypeMaster:
		log.Printf("Slave SYNCST failed: [%d, %d]\n", req.DbId, req.Seq)
		req.Cli.Close()
	}
}

func (srv *Server) dump(req *Request) {
	var pkg = srv.tbl.Dump(&req.PkgArgs, req.Cli)
	srv.sendResp(false, req, pkg)
}

// Normal master
func (srv *Server) newNormalMaster(req *Request, p *ctrl.PkgSlaveOf) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	switch cliType {
	case ClientTypeNormal:
		if !req.Cli.IsAuth(proto.AdminDbId) {
			log.Printf("Not authorized!\n")
			srv.replySlaveOf(req, "no priviledge")
			return
		}

		req.Cli.SetClientType(ClientTypeMaster) // switch client type

		log.Printf("Receive a slave connection from %s (%s), lastSeq=%d\n",
			p.SlaveAddr, req.Cli.c.RemoteAddr(), p.LastSeq)

		ms := NewMaster(p.SlaveAddr, p.LastSeq, false, 0, req.Cli, srv.bin)
		go ms.GoAsync(srv.tbl)
	case ClientTypeSlave:
		// Get response from master
		log.Printf("Master failed(%s), close slave!\n", p.ErrMsg)
		if req.Slv != nil {
			req.Slv.Close()
		} else {
			req.Cli.Close()
		}
	case ClientTypeMaster:
		log.Printf("Already master, cannot create master again, close now!\n")
		req.Cli.Close()
	}
}

// Migration master
func (srv *Server) newMigMaster(req *Request, p *ctrl.PkgMigrate) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	switch cliType {
	case ClientTypeNormal:
		if !req.Cli.IsAuth(proto.AdminDbId) {
			log.Printf("Not authorized!\n")
			srv.replyMigrate(req, "no priviledge")
			return
		}

		req.Cli.SetClientType(ClientTypeMaster) // switch client type

		log.Printf("Receive a migration slave connection from %s(%s)\n",
			req.Cli.c.RemoteAddr(), p.SlaveAddr)

		ms := NewMaster(p.SlaveAddr, 0, true, p.UnitId, req.Cli, srv.bin)
		go ms.GoAsync(srv.tbl)
	case ClientTypeSlave:
		// Get response from master
		log.Printf("Master failed(%s), close slave!\n", p.ErrMsg)
		if req.Slv != nil {
			req.Slv.Close()
		} else {
			req.Cli.Close()
		}
	case ClientTypeMaster:
		log.Printf("Already master, cannot create master again, close now!\n")
		req.Cli.Close()
	}
}

func (srv *Server) replySlaveOf(req *Request, msg string) {
	ps := ctrl.PkgSlaveOf{}
	ps.ErrMsg = msg
	pkg, err := ctrl.Encode(req.Cmd, req.DbId, req.Seq, &ps)
	if err == nil {
		srv.sendResp(false, req, pkg)
	}
}

func (srv *Server) replyMigrate(req *Request, msg string) {
	ps := ctrl.PkgMigrate{}
	ps.ErrMsg = msg
	pkg, err := ctrl.Encode(req.Cmd, req.DbId, req.Seq, &ps)
	if err == nil {
		srv.sendResp(false, req, pkg)
	}
}

func sameAddress(masterAddr, slaveAddr string, req *Request) bool {
	if masterAddr == slaveAddr {
		return true
	}

	var mAddr = util.GetRealAddr(masterAddr, req.Cli.LocalAddr().String())
	var sAddr = util.GetRealAddr(slaveAddr, req.Cli.LocalAddr().String())

	return mAddr == sAddr || mAddr == req.Cli.LocalAddr().String()
}

func (srv *Server) slaveOf(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}

	var p ctrl.PkgSlaveOf
	var err = ctrl.Decode(req.Pkg, nil, &p)
	if err == nil && !p.ClientReq {
		srv.newNormalMaster(req, &p)
		return
	}

	switch cliType {
	case ClientTypeNormal:
		if err != nil {
			log.Printf("Failed to Decode pkg: %s\n", err)
			srv.replySlaveOf(req, fmt.Sprintf("decode failed(%s)", err))
			return
		}

		if !req.Cli.IsAuth(proto.AdminDbId) {
			log.Printf("Not authorized!\n")
			srv.replySlaveOf(req, "no priviledge")
			return
		}

		if len(p.SlaveAddr) == 0 {
			p.SlaveAddr = req.Cli.LocalAddr().String()
		}
		if sameAddress(p.MasterAddr, p.SlaveAddr, req) {
			log.Printf("Master and slave addresses are the same!\n")
			srv.replyMigrate(req, "master and slave addresses are the same")
			return
		}

		err = srv.mc.SetMaster(p.MasterAddr, p.SlaveAddr)
		if err != nil {
			log.Printf("Failed to set config: %s\n", err)
			srv.replySlaveOf(req, fmt.Sprintf("set config failed(%s)", err))
			return
		}

		srv.rwMtx.Lock()
		slv := srv.slv
		srv.slv = nil
		srv.rwMtx.Unlock()

		if len(p.MasterAddr) > 0 {
			if slv != nil {
				slv.Close()
			}
			srv.bin.AsSlave()
			srv.connectToMaster(srv.mc)
		} else {
			if slv != nil {
				go slv.DelayClose()
			}
			srv.bin.AsMaster()
		}

		srv.replySlaveOf(req, "") // Success
	case ClientTypeSlave:
		fallthrough
	case ClientTypeMaster:
		if err != nil {
			log.Printf("Failed to Decode pkg: %s\n", err)
		} else {
			log.Println("Invalid client type %d for SlaveOf command, close now!",
				cliType)
		}
		req.Cli.Close()
	}
}

func (srv *Server) migrate(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}

	var p ctrl.PkgMigrate
	var err = ctrl.Decode(req.Pkg, nil, &p)
	if err == nil && !p.ClientReq {
		srv.newMigMaster(req, &p)
		return
	}

	switch cliType {
	case ClientTypeNormal:
		if err != nil {
			log.Printf("Failed to Decode pkg: %s\n", err)
			srv.replyMigrate(req, fmt.Sprintf("decode failed(%s)", err))
			return
		}

		if !req.Cli.IsAuth(proto.AdminDbId) {
			log.Printf("Not authorized!\n")
			srv.replyMigrate(req, "no priviledge")
			return
		}

		if len(p.SlaveAddr) == 0 {
			p.SlaveAddr = req.Cli.LocalAddr().String()
		}
		if sameAddress(p.MasterAddr, p.SlaveAddr, req) {
			log.Printf("Master and slave addresses are the same!\n")
			srv.replyMigrate(req, "master and slave addresses are the same")
			return
		}

		err = srv.mc.SetMigration(p.MasterAddr, p.SlaveAddr, p.UnitId)
		if err != nil {
			log.Printf("Failed to update migration config: %s\n", err)
			srv.replyMigrate(req,
				fmt.Sprintf("update migration config failed(%s)", err))
			return
		}

		srv.rwMtx.Lock()
		slv := srv.slv
		srv.slv = nil
		srv.rwMtx.Unlock()

		if len(p.MasterAddr) > 0 {
			if slv != nil {
				slv.Close()
			}
			if srv.tbl.HasUnitData(p.UnitId) {
				err = srv.mc.SetStatus(ctrl.SlaveNeedClear)
				if err != nil {
					log.Printf("Failed to set migration status: %s\n", err)
					srv.replyMigrate(req,
						fmt.Sprintf("failed to set migration status(%s)", err))
					return
				}
			} else {
				srv.connectToMaster(srv.mc)
			}
		} else {
			if slv != nil {
				go slv.DelayClose()
			}
			srv.bin.AsMaster()
		}

		srv.replyMigrate(req, "") // Success
	case ClientTypeSlave:
		fallthrough
	case ClientTypeMaster:
		if err != nil {
			log.Printf("Failed to Decode pkg: %s\n", err)
		} else {
			log.Println("Invalid client type %d for Migrate command, close now!",
				cliType)
		}
		req.Cli.Close()
	}
}

func (srv *Server) connectToMaster(mc *config.MasterConfig) {
	var slv = NewSlave(srv.reqChan, srv.bin, mc, srv.conf.Auth.AdminPwd)

	srv.rwMtx.Lock()
	srv.slv = slv
	srv.rwMtx.Unlock()

	go slv.GoConnectToMaster()
}

func (srv *Server) slaveStatus(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}

	switch cliType {
	case ClientTypeNormal:
		var p ctrl.PkgSlaveStatus
		var err = ctrl.Decode(req.Pkg, nil, &p)
		p.ErrMsg = ""
		if err != nil {
			p.ErrMsg = fmt.Sprintf("decode failed %s", err)
		} else {
			m := srv.mc.GetMaster()
			if len(m.MasterAddr) > 0 {
				if p.Migration {
					if !m.Migration {
						p.ErrMsg = fmt.Sprintf("check migration status on normal slave")
					} else if m.UnitId != p.UnitId {
						p.ErrMsg = fmt.Sprintf("unit id mismatch (%d, %d)",
							m.UnitId, p.UnitId)
					}
				} else {
					if m.Migration {
						p.ErrMsg = fmt.Sprintf("check normal slave status on migration")
					}
				}
			}
			if len(p.ErrMsg) == 0 {
				p.Status = m.Status
			}
		}

		pkg, err := ctrl.Encode(req.Cmd, req.DbId, req.Seq, &p)
		if err == nil {
			srv.sendResp(false, req, pkg)
		}
	case ClientTypeSlave:
		fallthrough
	case ClientTypeMaster:
		log.Println("Invalid client type %d for MigStatus command, close now!",
			cliType)
		req.Cli.Close()
	}
}

func (srv *Server) deleteMigrationUnit(unitId uint16, m config.MasterInfo) error {
	var match bool
	if len(m.MasterAddr) > 0 && m.Migration && m.UnitId == unitId {
		match = true
	}

	var err error
	if match {
		err = srv.mc.SetStatus(ctrl.SlaveClear)
		if err != nil {
			return err
		}
	}

	err = srv.tbl.DeleteUnit(unitId)
	if err != nil {
		return err
	}

	if match {
		// Set as NotSlave, need a new Migrate command
		err = srv.mc.SetStatus(ctrl.NotSlave)
		if err != nil {
			return err
		}
	}

	return nil
}

func (srv *Server) deleteUnit(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}

	switch cliType {
	case ClientTypeNormal:
		var p ctrl.PkgDelUnit
		var err = ctrl.Decode(req.Pkg, nil, &p)
		p.ErrMsg = ""
		if err != nil {
			p.ErrMsg = fmt.Sprintf("decode failed %s", err)
		} else {
			err = srv.deleteMigrationUnit(p.UnitId, srv.mc.GetMaster())
			if err != nil {
				p.ErrMsg = fmt.Sprintf("delete unit failed %s", err)
			}
		}

		pkg, err := ctrl.Encode(req.Cmd, req.DbId, req.Seq, &p)
		if err == nil {
			srv.sendResp(false, req, pkg)
		}
	case ClientTypeSlave:
		fallthrough
	case ClientTypeMaster:
		log.Println("Invalid client type %d for DelUnit command, close now!",
			cliType)
		req.Cli.Close()
	}
}

func (srv *Server) processRead() {
	for {
		select {
		case req := <-srv.reqChan.ReadReqChan:
			if srv.IsClosed() {
				continue
			}
			if !req.Cli.IsClosed() {
				switch req.Cmd {
				case proto.CmdAuth:
					srv.auth(req)
				case proto.CmdPing:
					srv.ping(req)
				case proto.CmdGet:
					srv.get(req)
				case proto.CmdMGet:
					srv.mGet(req)
				case proto.CmdScan:
					srv.scan(req)
				}
			}
		}
	}
}

func (srv *Server) processWrite() {
	for {
		select {
		case req := <-srv.reqChan.WriteReqChan:
			if srv.IsClosed() {
				continue
			}
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
				}
			}
		}
	}
}

func (srv *Server) processSync() {
	for {
		select {
		case req := <-srv.reqChan.SyncReqChan:
			if srv.IsClosed() {
				continue
			}
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
				case proto.CmdSyncSt:
					srv.syncStatus(req)
				}
			}
		}
	}
}

func (srv *Server) processDump() {
	for {
		select {
		case req := <-srv.reqChan.DumpReqChan:
			if srv.IsClosed() {
				continue
			}
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
	for {
		select {
		case req := <-srv.reqChan.CtrlReqChan:
			if srv.IsClosed() {
				continue
			}
			if !req.Cli.IsClosed() {
				switch req.Cmd {
				case proto.CmdSlaveOf:
					srv.slaveOf(req)
				case proto.CmdMigrate:
					srv.migrate(req)
				case proto.CmdSlaveSt:
					srv.slaveStatus(req)
				case proto.CmdDelUnit:
					srv.deleteUnit(req)
				}
			}
		}
	}
}
