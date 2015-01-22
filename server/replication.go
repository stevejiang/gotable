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
			cli.SetClientType(ClientTypeSlaver)

			go cli.GoReadRequest(slv.reqChan)
			go cli.GoSendResponse()

			var en = ctrl.NewEncoder()
			var pm ctrl.PkgMaster
			pm.LastSeq = slv.bin.GetMasterSeq(1) //TODO
			log.Printf("Connect to master with start log seq %d\n", pm.LastSeq)

			var resp = &Response{proto.CmdMaster, 0, 0, nil}
			resp.Pkg, err = en.Encode(proto.CmdMaster, 0, 0, &pm)
			if err != nil {
				log.Printf("Fatal Encode error: %s\n", err)
				return
			}
			cli.AddResp(resp)

			for !cli.IsClosed() {
				time.Sleep(time.Second)
			}
		} else {
			log.Printf("Connect to master %s failed, sleep 1 second and try again.\n",
				slv.masterHost)
			time.Sleep(time.Second)
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

func (ms *master) fullSync(tbl *store.Table, rwMtx *sync.RWMutex) uint64 {
	var lastSeq uint64
	if ms.startLogSeq > 0 {
		lastSeq = ms.startLogSeq
		log.Println("Already full asynced")
	} else {
		// Stop write globally
		rwMtx.Lock()
		var valid bool
		for lastSeq, valid = ms.bin.GetLastLogSeq(); !valid; {
			log.Println("Stop write globally for 1ms")
			time.Sleep(1e6)
			lastSeq, valid = ms.bin.GetLastLogSeq()
		}
		var it = tbl.NewIterator(false)
		rwMtx.Unlock()

		defer it.Close()

		// Full sync
		var hasRecord = false
		var one proto.PkgOneOp
		one.Cmd = proto.CmdSync
		for it.SeekToFirst(); it.Valid(); it.Next() {
			if hasRecord {
				var pkg = make([]byte, one.Length())
				one.Encode(pkg)
				ms.cli.AddResp(&Response{one.Cmd, one.DbId, one.Seq, pkg})
			}

			store.SetSyncPkg(it, &one)
			hasRecord = true

			if ms.cli.IsClosed() {
				return lastSeq
			}
		}

		if hasRecord {
			one.Seq = lastSeq
			var pkg = make([]byte, one.Length())
			one.Encode(pkg)
			ms.cli.AddResp(&Response{one.Cmd, one.DbId, one.Seq, pkg})
		}

		log.Println("Full async finished")
	}

	return lastSeq
}

func (ms *master) goAsync(tbl *store.Table, rwMtx *sync.RWMutex) {
	var lastSeq = ms.fullSync(tbl, rwMtx)
	if ms.cli.IsClosed() {
		log.Println("Master-slaver connection is closed, stop sync!")
		return
	}

	log.Printf("Start incremental sync, lastSeq=%d\n", lastSeq)

	ms.NewLogComming()

	var lastResp *Response
	var reader *binlog.Reader
	var tick = time.Tick(time.Second)
	for {
		select {
		case _, ok := <-ms.syncChan:
			if !ok || ms.IsClosed() {
				if reader != nil {
					reader.Close()
				}
				ms.doClose()
				log.Printf("Master sync channel closed %p\n", ms)
				return
			}

			if reader == nil {
				reader = binlog.NewReader(ms.bin, lastSeq)
			}
			if reader == nil {
				ms.doClose()
				log.Printf("Master sync channel closed %p\n", ms)
				return
			}

			for !ms.IsClosed() {
				var pkg = reader.Next()
				if pkg == nil {
					break
				}

				var head proto.PkgHead
				_, err := head.Decode(pkg)
				if err != nil {
					break
				}

				lastResp = &Response{head.Cmd, head.DbId, head.Seq, pkg}
				ms.cli.AddResp(lastResp)
			}

		case <-tick:
			if ms.IsClosed() {
				if reader != nil {
					reader.Close()
				}
				ms.doClose()
				log.Printf("Master sync channel closed %p\n", ms)
				return
			}

			ms.NewLogComming()
			if lastResp != nil {
				//log.Printf("Sync seq=%d\n", lastResp.Seq)
				lastResp = nil
			}
		}
	}
}
