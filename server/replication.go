package server

import (
	"github.com/stevejiang/gotable/api/go/table/proto"
	"github.com/stevejiang/gotable/binlog"
	"github.com/stevejiang/gotable/ctrl"
	"github.com/stevejiang/gotable/store"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type slaver struct {
	masterHost string
	reqChan    *RequestChan
	bin        *binlog.BinLog
}

func newSlaver(masterHost string, reqChan *RequestChan, bin *binlog.BinLog) *slaver {
	var slv = new(slaver)
	slv.masterHost = masterHost
	slv.reqChan = reqChan
	slv.bin = bin

	return slv
}

func (slv *slaver) goConnectToMaster() {
	var cli *Client
	for {
		// Connect to master host
		if c, err := net.Dial("tcp", slv.masterHost); err == nil {
			cli = NewClient(c)
			cli.cliType = ClientTypeSlaver

			go cli.GoReadRequest(slv.reqChan)
			go cli.GoSendResponse()

			var one ctrl.PkgCmdMasterReq
			one.Cmd = proto.CmdMaster

			one.LastSeq = slv.bin.GetMasterSeq(1) //TODO
			log.Printf("Connect to master with start log seq %d\n", one.LastSeq)

			var resp = &Response{one.Cmd, 0, 0, nil}
			one.Encode(&resp.Pkg)
			cli.AddResp(resp)

			for !cli.IsClosed() {
				time.Sleep(1e9)
			}
		} else {
			log.Printf("connect to master %s failed, sleep 1 second and try again.\n",
				slv.masterHost)
			time.Sleep(1e9)
		}
	}
}

type master struct {
	syncChan    chan struct{}
	cli         *Client
	closed      uint32
	startLogSeq uint64
	bin         *binlog.BinLog
}

func newMaster(cli *Client, bin *binlog.BinLog, startLogSeq uint64) *master {
	var ms = new(master)
	ms.syncChan = make(chan struct{}, 20)
	ms.cli = cli
	ms.startLogSeq = startLogSeq
	ms.bin = bin

	ms.cli.SetMaster(ms)

	return ms
}

func (ms *master) doClose() {
	ms.bin.RemoveMonitor(ms)

	ms.cli = nil
	ms.bin = nil
}

func (ms *master) Close() {
	if !ms.IsClosed() {
		atomic.AddUint32(&ms.closed, 1)
		ms.NewLogComming()
	}
}

func (ms *master) IsClosed() bool {
	return atomic.LoadUint32(&ms.closed) > 0
}

func (ms *master) NewLogComming() {
	if len(ms.syncChan)*2 < cap(ms.syncChan) {
		ms.syncChan <- struct{}{}
	}
}

func (ms *master) goAsync(tbl *store.Table, rwMtx *sync.RWMutex) {
	var lastSeq uint64
	if ms.startLogSeq > 0 {
		lastSeq = ms.startLogSeq
		log.Printf("already full asynced. startLogSeq=%d\n", ms.startLogSeq)
	} else {
		// Stop write globally
		rwMtx.Lock()
		var valid bool
		for lastSeq, valid = ms.bin.GetLastLogSeq(); !valid; {
			log.Println("Stop write globally for 1ms")
			time.Sleep(1e6)
			lastSeq, valid = ms.bin.GetLastLogSeq()
		}
		var iter = tbl.NewIterator(false)
		rwMtx.Unlock()

		// Full sync
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			_, dbId, key := iter.Key()
			value := iter.Value()

			var one proto.PkgOneOp
			one.Cmd = proto.CmdSync
			one.DbId = dbId
			one.TableId = key.TableId
			one.ColSpace = key.ColSpace
			one.RowKey = key.RowKey
			one.ColKey = key.ColKey
			one.Value = value
			one.CtrlFlag |= (proto.CtrlColSpace | proto.CtrlValue)

			pkg, _, _ := one.Encode(nil)
			ms.cli.AddResp(&Response{pkg[1], dbId, 0, pkg})
		}

		iter.Close()

		log.Println("full async finished")
	}

	log.Printf("start incremental sync. lastSeq=%d\n", lastSeq)

	ms.NewLogComming()

	var lastResp *Response
	var reader *binlog.Reader
	var tick = time.Tick(2e8)
	for {
		select {
		case _, ok := <-ms.syncChan:
			if !ok || ms.IsClosed() {
				if reader != nil {
					reader.Close()
				}
				ms.doClose()
				log.Printf("master sync channel closed %p\n", ms)
				return
			}

			if reader == nil {
				reader = binlog.NewReader(ms.bin, lastSeq)
			}
			if reader == nil {
				ms.doClose()
				log.Printf("master sync channel closed %p\n", ms)
				return
			}

			for !ms.IsClosed() {
				var pkg = reader.Next()
				if pkg == nil {
					break
				}

				var head proto.PkgHead
				var err = head.DecodeHead(pkg)
				if err != nil {
					break
				}

				lastResp = &Response{head.Cmd, head.DbId, head.Seq, pkg}
				ms.cli.AddResp(lastResp)

				//log.Printf("sync seq=%d\n", lastResp.seq)
			}

		case <-tick:
			ms.NewLogComming()
			if lastResp != nil {
				log.Printf("sync seq=%d\n", lastResp.Seq)
				lastResp = nil
			}
		}
	}
}
