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
	"github.com/stevejiang/gotable/config"
	"github.com/stevejiang/gotable/ctrl"
	"github.com/stevejiang/gotable/store"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type slaver struct {
	reqChan  *RequestChan
	bin      *binlog.BinLog
	mc       *config.MasterConfig
	mi       config.MasterInfo
	adminPwd string

	mtx    sync.Mutex // protects following
	cli    *Client
	closed bool
}

func NewSlaver(reqChan *RequestChan, bin *binlog.BinLog,
	mc *config.MasterConfig, adminPwd string) *slaver {
	var slv = new(slaver)
	slv.reqChan = reqChan
	slv.bin = bin
	slv.mc = mc
	slv.mi = mc.GetMaster()
	slv.adminPwd = adminPwd

	return slv
}

func (slv *slaver) Close() {
	var cli *Client
	slv.mtx.Lock()
	if !slv.closed {
		slv.closed = true
		cli = slv.cli
	}
	slv.mtx.Unlock()

	if cli != nil {
		cli.Close()
	}
}

func (slv *slaver) DelayClose() {
	log.Printf("Delay close slaver %p\n", slv)
	time.Sleep(time.Second * 2)
	slv.Close()
}

func (slv *slaver) IsClosed() bool {
	slv.mtx.Lock()
	closed := slv.closed
	slv.mtx.Unlock()
	return closed
}

func (slv *slaver) GoConnectToMaster() {
	slv.doConnectToMaster()

	slv.cli = nil
	slv.bin = nil
	slv.reqChan = nil
	slv.mc = nil
}

func (slv *slaver) doConnectToMaster() {
	for {
		if slv.IsClosed() {
			return
		}

		c, err := net.Dial("tcp", slv.mi.MasterAddr)
		if err != nil {
			log.Printf("Connect to master %s failed, sleep 1 second and try again.\n",
				slv.mi.MasterAddr)
			time.Sleep(time.Second)
			continue
		}

		cli := NewClient(c, false)
		slv.mtx.Lock()
		slv.cli = cli
		slv.mtx.Unlock()
		if slv.IsClosed() {
			return
		}

		cli.SetClientType(ClientTypeSlaver)

		go cli.GoRecvRequest(slv.reqChan, slv)
		go cli.GoSendResponse()

		if len(slv.adminPwd) == 0 {
			err = slv.SendSlaveOfToMaster()
			if err != nil {
				log.Printf("SendSlaveOfToMaster failed(%s), close slaver!", err)
				slv.Close()
				return
			}
		} else {
			err = slv.SendAuthToMaster()
			if err != nil {
				log.Printf("SendAuthToMaster failed(%s), close slaver!", err)
				slv.Close()
				return
			}
		}

		for !cli.IsClosed() {
			time.Sleep(time.Second)
		}
	}
}

func (slv *slaver) SendSlaveOfToMaster() error {
	var cli = slv.cli
	if cli == nil {
		return nil
	}

	var err error
	var pkg []byte
	if slv.mi.Migration {
		var p ctrl.PkgMigrate
		p.ClientReq = false
		p.MasterAddr = slv.mi.MasterAddr
		p.SlaverAddr = slv.mi.SlaverAddr
		p.UnitId = slv.mi.UnitId

		pkg, err = ctrl.Encode(proto.CmdMigrate, 0, 0, &p)
		if err != nil {
			return err
		}
	} else {
		lastSeq, valid := slv.bin.GetMasterSeq()
		if !valid {
			slv.mc.SetStatus(ctrl.SlaverNeedClear)
			// Any better solution?
			log.Fatalf("Slaver has old data with lastSeq %d, please clear first! "+
				"(Restart may fix this issue)", lastSeq)
		}

		var p ctrl.PkgSlaveOf
		p.ClientReq = false
		p.MasterAddr = slv.mi.MasterAddr
		p.SlaverAddr = slv.mi.SlaverAddr
		p.LastSeq = lastSeq
		log.Printf("Connect to master %s with lastSeq %d\n",
			slv.mi.MasterAddr, p.LastSeq)

		pkg, err = ctrl.Encode(proto.CmdSlaveOf, 0, 0, &p)
		if err != nil {
			return err
		}
	}

	cli.AddResp(pkg)
	return slv.mc.SetStatus(ctrl.SlaverFullSync)
}

func (slv *slaver) SendAuthToMaster() error {
	var cli = slv.cli
	if cli == nil {
		return nil
	}
	if len(slv.adminPwd) == 0 {
		return nil
	}

	var p proto.PkgOneOp
	p.DbId = proto.AdminDbId
	p.Cmd = proto.CmdAuth
	p.RowKey = []byte(slv.adminPwd)

	var pkg = make([]byte, p.Length())
	_, err := p.Encode(pkg)
	if err != nil {
		return err
	}

	cli.AddResp(pkg)
	return nil
}

type master struct {
	syncChan  chan struct{}
	cli       *Client
	bin       *binlog.BinLog
	reader    *binlog.Reader
	slaveAddr string
	lastSeq   uint64
	migration bool   // true: Migration; false: Normal master/slaver
	unitId    uint16 // Only meaningful for migration

	// atomic
	closed uint32
}

func NewMaster(slaveAddr string, lastSeq uint64, migration bool, unitId uint16,
	cli *Client, bin *binlog.BinLog) *master {
	var ms = new(master)
	ms.syncChan = make(chan struct{}, 20)
	ms.cli = cli
	ms.bin = bin
	ms.reader = nil
	ms.slaveAddr = slaveAddr
	ms.lastSeq = lastSeq
	ms.migration = migration
	if migration {
		ms.unitId = unitId
	} else {
		ms.unitId = ctrl.TotalUnitNum
	}
	ms.bin.RegisterMonitor(ms)

	return ms
}

func (ms *master) doClose() {
	atomic.AddUint32(&ms.closed, 1)

	cli := ms.cli
	if cli != nil {
		cli.Close()
	}

	bin := ms.bin
	if bin != nil {
		bin.RemoveMonitor(ms)
	}

	reader := ms.reader
	if reader != nil {
		reader.Close()
	}

	ms.cli = nil
	ms.bin = nil
	ms.reader = nil

	log.Printf("Master sync to slaver %s is closed\n", ms.slaveAddr)
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

func (ms *master) syncStatus(key string, lastSeq uint64) {
	var p proto.PkgOneOp
	p.Cmd = proto.CmdSyncSt
	p.DbId = proto.AdminDbId
	p.Seq = lastSeq
	p.RowKey = []byte(key)
	var pkg = make([]byte, p.Length())
	p.Encode(pkg)
	ms.cli.AddResp(pkg)
}

func (ms *master) fullSync(tbl *store.Table) uint64 {
	var lastSeq uint64
	if ms.lastSeq > 0 {
		lastSeq = ms.lastSeq
		if ms.migration {
			log.Printf("Already full migrated to %s unitId %d\n",
				ms.slaveAddr, ms.unitId)
		} else {
			log.Printf("Already full synced to %s\n", ms.slaveAddr)
		}
		return lastSeq
	}

	// Stop write globally
	rwMtx := tbl.GetRWMutex()
	rwMtx.Lock()
	var valid bool
	for lastSeq, valid = ms.bin.GetLastLogSeq(); !valid; {
		log.Println("Stop write globally for 1ms")
		time.Sleep(time.Millisecond)
		lastSeq, valid = ms.bin.GetLastLogSeq()
	}
	var it = tbl.NewIterator(false)
	rwMtx.Unlock()

	defer it.Destroy()

	// Full sync
	var one proto.PkgOneOp
	one.Cmd = proto.CmdSync
	for it.SeekToFirst(); it.Valid(); it.Next() {
		unitId, ok := store.SeekAndCopySyncPkg(it, &one)
		if !ok {
			break
		}
		if ms.migration && ms.unitId != unitId {
			if ms.unitId < unitId {
				break
			} else if ms.unitId > unitId {
				store.SeekToUnit(it, ms.unitId, 0, 0)
				if !it.Valid() {
					break
				}
				unitId, ok = store.SeekAndCopySyncPkg(it, &one)
				if !ok || ms.unitId != unitId {
					break
				}
			}
		}

		if ms.cli.IsClosed() {
			return lastSeq
		}

		one.Seq = 0
		var pkg = make([]byte, one.Length())
		one.Encode(pkg)
		ms.cli.AddResp(pkg)
	}

	// Tell slaver full sync finished
	if ms.migration {
		ms.syncStatus(store.KeyFullSyncEnd, 0)
		log.Printf("Full migration to %s unitId %d finished\n",
			ms.slaveAddr, ms.unitId)
	} else {
		ms.syncStatus(store.KeyFullSyncEnd, lastSeq)
		log.Printf("Full sync to %s finished\n", ms.slaveAddr)
	}

	return lastSeq
}

func (ms *master) GoAsync(tbl *store.Table) {
	var lastSeq = ms.fullSync(tbl)
	if ms.cli.IsClosed() {
		log.Println("Master-slaver connection is closed, stop sync!")
		return
	}

	if ms.migration {
		log.Printf("Start incremental migration to %s unitId %d, lastSeq=%d\n",
			ms.slaveAddr, ms.unitId, lastSeq)
	} else {
		log.Printf("Start incremental sync to %s, lastSeq=%d",
			ms.slaveAddr, lastSeq)
	}

	ms.NewLogComming()

	var isReady bool
	var readyCount int64
	var head proto.PkgHead
	var tick = time.Tick(time.Second)
	for {
		select {
		case _, ok := <-ms.syncChan:
			if !ok || ms.IsClosed() || ms.cli.IsClosed() {
				ms.doClose()
				return
			}

			if ms.reader == nil {
				ms.reader = binlog.NewReader(ms.bin, lastSeq)
			}
			if ms.reader == nil {
				ms.doClose()
				return
			}

			for !ms.IsClosed() && !ms.cli.IsClosed() {
				var pkg = ms.reader.Next()
				if pkg == nil {
					if readyCount%60 == 0 {
						ms.syncStatus(store.KeyIncrSyncEnd, 0)
						readyCount++
					} else {
						isReady = true
					}
					break
				}

				pkg, err := ms.convertMigPkg(pkg, &head)
				if err != nil {
					break
				}
				if pkg == nil {
					continue
				}

				ms.cli.AddResp(pkg)
			}

		case <-tick:
			if ms.IsClosed() || ms.cli.IsClosed() {
				ms.doClose()
				return
			}

			if isReady {
				readyCount++
			} else {
				isReady = false
			}

			ms.NewLogComming()
		}
	}
}

func (ms *master) convertMigPkg(pkg []byte, head *proto.PkgHead) ([]byte, error) {
	_, err := head.Decode(pkg)
	if err != nil {
		return nil, err
	}

	if !ms.migration {
		return pkg, nil
	}

	switch head.Cmd {
	case proto.CmdIncr:
		fallthrough
	case proto.CmdDel:
		fallthrough
	case proto.CmdSync:
		fallthrough
	case proto.CmdSet:
		var p proto.PkgOneOp
		_, err = p.Decode(pkg)
		if err != nil {
			return nil, err
		}
		if ms.unitId == ctrl.GetUnitId(p.DbId, p.TableId, p.RowKey) {
			return pkg, nil
		} else {
			return nil, nil
		}
	case proto.CmdMIncr:
		fallthrough
	case proto.CmdMDel:
		fallthrough
	case proto.CmdMSet:
		var p proto.PkgMultiOp
		_, err = p.Decode(pkg)
		if err != nil {
			return nil, err
		}
		var kvs []proto.KeyValue
		for i := 0; i < len(p.Kvs); i++ {
			if ms.unitId == ctrl.GetUnitId(p.DbId, p.Kvs[i].TableId, p.Kvs[i].RowKey) {
				kvs = append(kvs, p.Kvs[i])
			}
		}
		if len(kvs) == 0 {
			return nil, nil
		} else {
			p.Kvs = kvs
			pkg = make([]byte, p.Length())
			_, err = p.Encode(pkg)
			if err != nil {
				return nil, err
			}
			return pkg, nil
		}
	}

	return nil, nil
}
