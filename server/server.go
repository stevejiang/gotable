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

func (srv *Server) addResp(write bool, req *Request, resp *Response) {
	switch req.Cli.cliType {
	case ClientTypeNormal:
		req.Cli.AddResp(resp)
		if write {
			srv.bin.AddRequest(&binlog.Request{0, 0, req.Pkg})
		}
	case ClientTypeSlaver:
		if write {
			var masterId uint16 = 1
			srv.bin.AddRequest(&binlog.Request{masterId, req.Seq, req.Pkg})
		}
	}
}

func (srv *Server) ping(req *Request) {
	var resp = new(Response)
	resp.Cmd = req.Cmd
	resp.Seq = req.Seq
	resp.Pkg = req.Pkg

	srv.addResp(false, req, resp)
}

func (srv *Server) get(req *Request) {
	var resp = Response(req.PkgArgs)
	resp.Pkg = srv.tbl.Get(&req.PkgArgs)

	srv.addResp(false, req, &resp)
}

func (srv *Server) set(req *Request) {
	var resp = Response(req.PkgArgs)
	resp.Pkg = srv.tbl.Set(&req.PkgArgs)

	srv.addResp(true, req, &resp)
}

func (srv *Server) del(req *Request) {
	var resp = Response(req.PkgArgs)
	resp.Pkg = srv.tbl.Del(&req.PkgArgs)

	srv.addResp(true, req, &resp)
}

func (srv *Server) mget(req *Request) {
	var resp = Response(req.PkgArgs)
	resp.Pkg = srv.tbl.Mget(&req.PkgArgs)

	srv.addResp(false, req, &resp)
}

func (srv *Server) mset(req *Request) {
	var resp = Response(req.PkgArgs)
	resp.Pkg = srv.tbl.Mset(&req.PkgArgs)

	srv.addResp(true, req, &resp)
}

func (srv *Server) mdel(req *Request) {
	var resp = Response(req.PkgArgs)
	resp.Pkg = srv.tbl.Mdel(&req.PkgArgs)

	srv.addResp(true, req, &resp)
}

func (srv *Server) scan(req *Request) {
	var resp = Response(req.PkgArgs)
	resp.Pkg = srv.tbl.Scan(&req.PkgArgs)

	srv.addResp(false, req, &resp)
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
	case proto.CmdMGet:
		srv.mget(req)
	case proto.CmdMSet:
		srv.mset(req)
	case proto.CmdMDel:
		srv.mdel(req)
	case proto.CmdScan:
		srv.scan(req)
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
