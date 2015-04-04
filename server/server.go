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
	"os"
	"runtime"
	"sync"
	"syscall"
)

type Server struct {
	tbl     *store.Table
	bin     *binlog.BinLog
	conf    *config.Config
	mc      *config.MasterConfig
	reqChan *RequestChan

	rwMtx sync.RWMutex // protects following
	slv   *slaver
}

func NewServer(conf *config.Config) *Server {
	var tableDir = TableDirName(conf)
	var binlogDir = BinLogDirName(conf)
	var configDir = ConfigDirName(conf)

	mc := config.NewMasterConfig(configDir)
	if mc == nil {
		return nil
	}

	err := clearSlaverOldData(conf, mc)
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

	return srv
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

func clearSlaverOldData(conf *config.Config, mc *config.MasterConfig) error {
	var err error
	m := mc.GetMaster()
	// Normal slaver
	if len(m.MasterAddr) > 0 && !m.Migration {
		// Check whether need to clear old data
		if m.Status != ctrl.SlaverNeedClear && m.Status != ctrl.SlaverClear {
			return nil
		}

		err = mc.SetStatus(ctrl.SlaverClear)
		if err != nil {
			return err
		}

		var binlogDir = BinLogDirName(conf)
		log.Printf("Delete dir %s\n", binlogDir)
		err = os.RemoveAll(binlogDir)
		if err != nil {
			return err
		}

		var tableDir = TableDirName(conf)
		log.Printf("Delete dir %s\n", tableDir)
		err = os.RemoveAll(tableDir)
		if err != nil {
			return err
		}

		err = mc.SetStatus(ctrl.SlaverInit)
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
	case ClientTypeSlaver:
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
	case ClientTypeSlaver:
		var in proto.PkgOneOp
		_, err := in.Decode(req.Pkg)
		if err != nil {
			log.Printf("Decode failed for auth reply(%s), close slaver!\n", err)
		} else if in.ErrCode != 0 {
			log.Printf("Auth failed (%d), close slaver!\n", in.ErrCode)
		}
		if err != nil || in.ErrCode != 0 {
			if req.Slv != nil {
				req.Slv.Close()
			} else {
				req.Cli.Close()
			}
			return
		}
		// Slaver auth succeed
		if req.Slv != nil {
			err = req.Slv.SendSlaveOfToMaster()
			if err != nil {
				log.Printf("SendSlaveOfToMaster failed(%s), close slaver!\n", err)
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
	var wa = store.NewWriteAccess(ClientTypeSlaver == cliType, srv.mc)

	var pkg = srv.tbl.Get(&req.PkgArgs, req.Cli, wa)
	srv.sendResp(false, req, pkg)
}

func (srv *Server) set(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var wa = store.NewWriteAccess(ClientTypeSlaver == cliType, srv.mc)
	switch cliType {
	case ClientTypeNormal:
		if !wa.Check() {
			srv.replyOneOp(req, table.EcWriteSlaver)
			return
		}
		pkg, ok := srv.tbl.Set(&req.PkgArgs, req.Cli, wa)
		srv.sendResp(ok, req, pkg)
	case ClientTypeSlaver:
		pkg, ok := srv.tbl.Set(&req.PkgArgs, req.Cli, wa)
		if ok {
			srv.sendResp(ok, req, nil)
		} else {
			srv.sendResp(ok, req, pkg)
		}
	case ClientTypeMaster:
		log.Printf("Slaver SET failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) del(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var wa = store.NewWriteAccess(ClientTypeSlaver == cliType, srv.mc)
	switch cliType {
	case ClientTypeNormal:
		if !wa.Check() {
			srv.replyOneOp(req, table.EcWriteSlaver)
			return
		}
		pkg, ok := srv.tbl.Del(&req.PkgArgs, req.Cli, wa)
		srv.sendResp(ok, req, pkg)
	case ClientTypeSlaver:
		pkg, ok := srv.tbl.Del(&req.PkgArgs, req.Cli, wa)
		if ok {
			srv.sendResp(ok, req, nil)
		} else {
			srv.sendResp(ok, req, pkg)
		}
	case ClientTypeMaster:
		log.Printf("Slaver DEL failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) incr(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var wa = store.NewWriteAccess(ClientTypeSlaver == cliType, srv.mc)
	switch cliType {
	case ClientTypeNormal:
		if !wa.Check() {
			srv.replyOneOp(req, table.EcWriteSlaver)
			return
		}
		pkg, ok := srv.tbl.Incr(&req.PkgArgs, req.Cli, wa)
		srv.sendResp(ok, req, pkg)
	case ClientTypeSlaver:
		pkg, ok := srv.tbl.Incr(&req.PkgArgs, req.Cli, wa)
		if ok {
			srv.sendResp(ok, req, nil)
		} else {
			srv.sendResp(ok, req, pkg)
		}
	case ClientTypeMaster:
		log.Printf("Slaver INCR failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) mGet(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var wa = store.NewWriteAccess(ClientTypeSlaver == cliType, srv.mc)

	var pkg = srv.tbl.MGet(&req.PkgArgs, req.Cli, wa)
	srv.sendResp(false, req, pkg)
}

func (srv *Server) mSet(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var wa = store.NewWriteAccess(ClientTypeSlaver == cliType, srv.mc)
	switch cliType {
	case ClientTypeNormal:
		if !wa.Check() {
			srv.replyMultiOp(req, table.EcWriteSlaver)
			return
		}
		pkg, ok := srv.tbl.MSet(&req.PkgArgs, req.Cli, wa)
		srv.sendResp(ok, req, pkg)
	case ClientTypeSlaver:
		pkg, ok := srv.tbl.MSet(&req.PkgArgs, req.Cli, wa)
		if ok {
			srv.sendResp(ok, req, nil)
		} else {
			srv.sendResp(ok, req, pkg)
		}
	case ClientTypeMaster:
		log.Printf("Slaver MSET failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) mDel(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var wa = store.NewWriteAccess(ClientTypeSlaver == cliType, srv.mc)
	switch cliType {
	case ClientTypeNormal:
		if !wa.Check() {
			srv.replyMultiOp(req, table.EcWriteSlaver)
			return
		}
		pkg, ok := srv.tbl.MDel(&req.PkgArgs, req.Cli, wa)
		srv.sendResp(ok, req, pkg)
	case ClientTypeSlaver:
		pkg, ok := srv.tbl.MDel(&req.PkgArgs, req.Cli, wa)
		if ok {
			srv.sendResp(ok, req, nil)
		} else {
			srv.sendResp(ok, req, pkg)
		}
	case ClientTypeMaster:
		log.Printf("Slaver MDEL failed: [%d, %d]\n", req.DbId, req.Seq)
	}
}

func (srv *Server) mIncr(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	var wa = store.NewWriteAccess(ClientTypeSlaver == cliType, srv.mc)
	switch cliType {
	case ClientTypeNormal:
		if !wa.Check() {
			srv.replyMultiOp(req, table.EcWriteSlaver)
			return
		}
		pkg, ok := srv.tbl.MIncr(&req.PkgArgs, req.Cli, wa)
		srv.sendResp(ok, req, pkg)
	case ClientTypeSlaver:
		pkg, ok := srv.tbl.MIncr(&req.PkgArgs, req.Cli, wa)
		if ok {
			srv.sendResp(ok, req, nil)
		} else {
			srv.sendResp(ok, req, pkg)
		}
	case ClientTypeMaster:
		log.Printf("Slaver MINCR failed: [%d, %d]\n", req.DbId, req.Seq)
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
	case ClientTypeSlaver:
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
		log.Printf("Slaver SYNC failed: [%d, %d], close now!\n", req.DbId, req.Seq)
		req.Cli.Close()
	}
}

func (srv *Server) syncStatus(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}
	switch cliType {
	case ClientTypeSlaver:
		var in proto.PkgOneOp
		_, err := in.Decode(req.Pkg)
		if err != nil || len(in.RowKey) == 0 {
			return
		}

		rowKey := string(in.RowKey)
		switch rowKey {
		case store.KeyFullSyncEnd:
			srv.mc.SetStatus(ctrl.SlaverIncrSync)
			log.Printf("Switch sync status to SlaverIncrSync\n")
		case store.KeyIncrSyncEnd:
			srv.mc.SetStatus(ctrl.SlaverReady)
			log.Printf("Switch sync status to SlaverReady")
		case store.KeySyncLogMissing:
			srv.mc.SetStatus(ctrl.SlaverNeedClear)
			lastSeq, _ := srv.bin.GetMasterSeq()
			// Any better solution?
			log.Fatalf("Slaver lastSeq %d is out of sync, please clear old data! "+
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
		log.Printf("Slaver SYNCST failed: [%d, %d]\n", req.DbId, req.Seq)
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

		log.Printf("Receive a slave connection from %s, lastSeq=%d\n",
			req.Cli.c.RemoteAddr(), p.LastSeq)

		ms := NewMaster(p.SlaverAddr, p.LastSeq, false, 0, req.Cli, srv.bin)
		go ms.GoAsync(srv.tbl)
	case ClientTypeSlaver:
		// Get response from master
		log.Printf("Master failed(%s), close slaver!\n", p.ErrMsg)
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
			req.Cli.c.RemoteAddr(), p.SlaverAddr)

		ms := NewMaster(p.SlaverAddr, 0, true, p.UnitId, req.Cli, srv.bin)
		go ms.GoAsync(srv.tbl)
	case ClientTypeSlaver:
		// Get response from master
		log.Printf("Master failed(%s), close slaver!\n", p.ErrMsg)
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

		if len(p.SlaverAddr) == 0 {
			p.SlaverAddr = req.Cli.LocalAddr().String()
		}
		if p.MasterAddr == p.SlaverAddr {
			log.Printf("Master and slaver address are the same!\n")
			srv.replyMigrate(req, "master and slaver address are the same")
			return
		}

		err = srv.mc.SetMaster(p.MasterAddr, p.SlaverAddr)
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
			srv.bin.AsSlaver()
			srv.connectToMaster(srv.mc)
		} else {
			if slv != nil {
				go slv.DelayClose()
			}
			srv.bin.AsMaster()
		}

		srv.replySlaveOf(req, "") // Success
	case ClientTypeSlaver:
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

		if len(p.SlaverAddr) == 0 {
			p.SlaverAddr = req.Cli.LocalAddr().String()
		}
		if p.MasterAddr == p.SlaverAddr {
			log.Printf("Master and slaver address are the same!\n")
			srv.replyMigrate(req, "master and slaver address are the same")
			return
		}

		err = srv.mc.SetMigration(p.MasterAddr, p.SlaverAddr, p.UnitId)
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
				err = srv.mc.SetStatus(ctrl.SlaverNeedClear)
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
	case ClientTypeSlaver:
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
	var slv = NewSlaver(srv.reqChan, srv.bin, mc, srv.conf.Auth.AdminPwd)
	go slv.GoConnectToMaster()

	srv.rwMtx.Lock()
	srv.slv = slv
	srv.rwMtx.Unlock()
}

func (srv *Server) slaverStatus(req *Request) {
	var cliType uint32 = ClientTypeNormal
	if req.Cli != nil {
		cliType = req.Cli.ClientType()
	}

	switch cliType {
	case ClientTypeNormal:
		var p ctrl.PkgSlaverStatus
		var err = ctrl.Decode(req.Pkg, nil, &p)
		p.ErrMsg = ""
		if err != nil {
			p.ErrMsg = fmt.Sprintf("decode failed %s", err)
		} else {
			m := srv.mc.GetMaster()
			if len(m.MasterAddr) > 0 {
				if p.Migration {
					if !m.Migration {
						p.ErrMsg = fmt.Sprintf("check migration status on normal slaver")
					} else if m.UnitId != p.UnitId {
						p.ErrMsg = fmt.Sprintf("unit id mismatch (%d, %d)",
							m.UnitId, p.UnitId)
					}
				} else {
					if m.Migration {
						p.ErrMsg = fmt.Sprintf("check normal slaver status on migration")
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
	case ClientTypeSlaver:
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
		err = srv.mc.SetStatus(ctrl.SlaverClear)
		if err != nil {
			return err
		}
	}

	err = srv.tbl.DeleteUnit(unitId)
	if err != nil {
		return err
	}

	if match {
		// Set as NotSlaver, need a new Migrate command
		err = srv.mc.SetStatus(ctrl.NotSlaver)
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
	case ClientTypeSlaver:
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
			if !req.Cli.IsClosed() {
				switch req.Cmd {
				case proto.CmdSlaveOf:
					srv.slaveOf(req)
				case proto.CmdMigrate:
					srv.migrate(req)
				case proto.CmdSlaverSt:
					srv.slaverStatus(req)
				case proto.CmdDelUnit:
					srv.deleteUnit(req)
				}
			}
		}
	}
}

func Run(conf *config.Config) {
	log.SetFlags(log.Flags() | log.Lshortfile)

	var srv = NewServer(conf)
	if srv == nil {
		log.Fatalln("Failed to create new server.")
		return
	}

	go srv.bin.GoWriteBinLog()

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
	go srv.processSync() // Use 1 goroutine to make sure data consistency
	go srv.processDump()
	go srv.processCtrl()

	log.Printf("Goroutine distribution: read %d, write %d, %s\n",
		readProcNum, writeProcNum, "sync 1, dump 1, ctrl 1")

	link, err := net.Listen(conf.Db.Network, conf.Db.Address)
	if err != nil {
		log.Fatalln("Listen failed:", err)
	}

	log.Printf("GoTable %s started on %s://%s\n",
		table.Version, conf.Db.Network, conf.Db.Address)

	// Normal slaver, reconnect to master
	hasMaster, migration, _ := srv.mc.GetMasterUnit()
	if hasMaster && !migration {
		lastSeq, valid := srv.bin.GetMasterSeq()
		if !valid {
			srv.mc.SetStatus(ctrl.SlaverNeedClear)
			// Any better solution?
			log.Fatalf("Slaver lastSeq %d is out of sync, please clear old data! "+
				"(Restart may fix this issue)", lastSeq)
		}

		srv.bin.AsSlaver()
		srv.connectToMaster(srv.mc)
	}

	var authEnabled bool
	if conf.Auth.AdminPwd != "" {
		authEnabled = true
		srv.tbl.SetPassword(proto.AdminDbId, conf.Auth.AdminPwd)
	}

	for {
		if c, err := link.Accept(); err == nil {
			//log.Printf("New connection %s\t%s\n", c.RemoteAddr(), c.LocalAddr())

			cli := NewClient(c, authEnabled)
			go cli.GoRecvRequest(srv.reqChan, nil)
			go cli.GoSendResponse()
		}
	}
}
