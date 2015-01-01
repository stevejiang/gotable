package server

import (
	"fmt"
	"github.com/stevejiang/gotable/binlog"
	"github.com/stevejiang/gotable/proto"
	"github.com/stevejiang/gotable/store"
	"log"
	"net"
	"os"
	"runtime"
	"sync"
)

type Server struct {
	db      *store.TableDB
	reqChan *RequestChan
	bin     *binlog.BinLog
	rwMtx   sync.RWMutex
}

func NewServer(dbname string) *Server {
	var binlogDir = fmt.Sprintf("%s/binlog", dbname)
	var rocksdbDir = fmt.Sprintf("%s/rocksdb", dbname)
	os.MkdirAll(binlogDir, os.ModeDir|os.ModePerm)
	os.MkdirAll(rocksdbDir, os.ModeDir|os.ModePerm)

	srv := new(Server)

	srv.db = store.NewTableDB()
	err := srv.db.Open(rocksdbDir, true)
	if err != nil {
		log.Println("Open failed: ", err)
		return nil
	}

	srv.reqChan = new(RequestChan)
	srv.reqChan.ReadReqChan = make(chan *request, 10000)
	srv.reqChan.WriteReqChan = make(chan *request, 10000)

	srv.bin = binlog.NewBinLog(binlogDir)

	return srv
}

func (srv *Server) addResp(write bool, req *request, resp *response) {
	switch req.cli.cliType {
	case ClientTypeNormal:
		req.cli.AddResp(resp)
		if write {
			srv.bin.AddRequest(&binlog.Request{0, 0, req.pkg})
		}
	case ClientTypeSlaver:
		if write {
			var masterId uint16 = 1
			srv.bin.AddRequest(&binlog.Request{masterId, req.seq, req.pkg})
		}
	}
}

func (srv *Server) ping(req *request) {
	var resp = new(response)
	resp.cmd = req.cmd
	resp.seq = req.seq
	resp.pkg = req.pkg

	srv.addResp(false, req, resp)
}

func (srv *Server) get(req *request) {
	var err error
	var in proto.PkgCmdGetReq
	in.Decode(req.pkg)

	var out proto.PkgCmdGetResp
	out.Value, err = srv.db.Get(in.DbId, store.TableKey{in.TableId, in.RowKey,
		store.ColSpaceDefault, in.ColKey})
	if err != nil {
		out.ErrCode = proto.EcodeReadDbFailed
		log.Printf("Read DB failed: %s\n", err)
	}

	if out.Value == nil {
		out.ErrCode = proto.EcodeNotExist
	}

	out.PkgHead = in.PkgHead
	out.DbId = in.DbId
	out.TableId = in.TableId
	out.RowKey = in.RowKey
	out.ColKey = in.ColKey

	var resp = new(response)
	resp.cmd = req.cmd
	resp.seq = req.seq
	out.Encode(&resp.pkg)

	srv.addResp(false, req, resp)
}

func (srv *Server) put(req *request) {
	var in proto.PkgCmdPutReq
	in.Decode(req.pkg)

	srv.rwMtx.RLock()
	var err = srv.db.Put(in.DbId, store.TableKey{in.TableId, in.RowKey,
		store.ColSpaceDefault, in.ColKey}, in.Value)
	srv.rwMtx.RUnlock()

	var out proto.PkgCmdPutResp
	if err != nil {
		out.ErrCode = proto.EcodeWriteDbFailed
		log.Printf("Write DB failed: %s\n", err)
	}

	out.PkgHead = in.PkgHead
	out.DbId = in.DbId
	out.TableId = in.TableId
	out.RowKey = in.RowKey
	out.ColKey = in.ColKey

	var resp = new(response)
	resp.cmd = req.cmd
	resp.seq = req.seq
	out.Encode(&resp.pkg)

	srv.addResp(true, req, resp)
}

func (srv *Server) newMaster(req *request) {
	var in proto.PkgCmdMasterReq
	in.Decode(req.pkg)

	req.cli.cliType = ClientTypeMaster
	var startLogSeq = in.LastSeq

	log.Printf("receive a slave connection from %s, startLogSeq=%d\n",
		req.cli.c.RemoteAddr(), startLogSeq)

	srv.rwMtx.Lock()
	var ms = newMaster(req.cli, srv.bin, startLogSeq)
	srv.rwMtx.Unlock()

	srv.bin.RegisterMonitor(ms)
	go ms.goAsync(srv.db, &srv.rwMtx)
}

func (srv *Server) doProcess(req *request) {
	switch req.cmd {
	case proto.CmdPing:
		srv.ping(req)
	case proto.CmdGet:
		srv.get(req)
	case proto.CmdPut:
		srv.put(req)
	case proto.CmdMaster:
		srv.newMaster(req)
	}
}

func (srv *Server) processRead() {
	for {
		select {
		case req := <-srv.reqChan.ReadReqChan:
			if !req.cli.IsClosed() {
				srv.doProcess(req)
			}
		}
	}
}

func (srv *Server) processWrite() {
	for {
		select {
		case req := <-srv.reqChan.WriteReqChan:
			if !req.cli.IsClosed() {
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
