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

package binlog

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/stevejiang/gotable/api/go/table/proto"
	"github.com/stevejiang/gotable/util"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

// Monitor is a BinLog monitor.
type Monitor interface {
	// NewLogComming tells the monitor there is new binlog written.
	NewLogComming()
}

const (
	memBinLogSize  = 1024 * 1024 * 8
	keepBinFileNum = 5
	MinNormalSeq   = uint64(1000000000000000000)
)

type fileInfo struct {
	Idx    uint64
	MinSeq uint64
	MaxSeq uint64
	Done   bool
}

type readerSeq struct {
	seq uint64
}

type Request struct {
	MasterSeq uint64
	Pkg       []byte
}

type BinLog struct {
	dir     string
	memSize int
	keepNum int
	reqChan chan *Request

	binFile *os.File
	binBufW *bufio.Writer

	mtx       sync.Mutex // The following variables are protected by mtx
	hasMaster bool       // Has master or not
	msChanged bool       // Whether monitors changed
	monitors  []Monitor

	fileIdx uint64
	logSeq  uint64

	memlog  []byte
	usedLen int

	infos []*fileInfo  // All binlog files
	rseqs []*readerSeq // Most recent reader seq
}

func NewBinLog(dir string, memSize, keepNum int) *BinLog {
	err := os.MkdirAll(dir, os.ModeDir|os.ModePerm)
	if err != nil {
		log.Printf("Invalid binlog directory (%s): %s\n", dir, err)
		return nil
	}

	var bin = new(BinLog)
	bin.dir = dir

	bin.reqChan = make(chan *Request, 10000)

	bin.hasMaster = false // No master
	bin.msChanged = true
	bin.logSeq = MinNormalSeq // Master server seq start from here
	bin.memlog = make([]byte, memBinLogSize)

	err = bin.init()
	if err != nil {
		log.Printf("Init binlog failed: %s\n", err)
		return nil
	}

	log.Printf("Last binlog: fileIdx %d, logSeq %d\n", bin.fileIdx, bin.logSeq)

	return bin
}

func (bin *BinLog) init() error {
	idxs, err := bin.loadAllFilesIndex()
	if err != nil {
		return err
	}

	err = bin.loadAndFixSeqFiles(idxs)
	if err != nil {
		return err
	}

	if len(bin.infos) > 0 {
		bin.fileIdx = bin.infos[len(bin.infos)-1].Idx
		bin.logSeq = bin.infos[len(bin.infos)-1].MaxSeq
	}

	return nil
}

func (bin *BinLog) RegisterMonitor(m Monitor) {
	bin.mtx.Lock()
	bin.monitors = append(bin.monitors, m)
	bin.msChanged = true
	bin.mtx.Unlock()
}

func (bin *BinLog) RemoveMonitor(m Monitor) {
	bin.mtx.Lock()
	for index, tmp := range bin.monitors {
		if tmp == m {
			copy(bin.monitors[index:], bin.monitors[index+1:])
			bin.monitors = bin.monitors[:len(bin.monitors)-1]
			break
		}
	}
	bin.mtx.Unlock()
}

func (bin *BinLog) AsMaster() {
	bin.mtx.Lock()
	bin.hasMaster = false
	if bin.logSeq < MinNormalSeq {
		bin.logSeq = MinNormalSeq
	}
	bin.mtx.Unlock()
}

func (bin *BinLog) AsSlaver() {
	bin.mtx.Lock()
	bin.hasMaster = true
	bin.logSeq = 0
	bin.mtx.Unlock()
}

func (bin *BinLog) GetLogSeq() uint64 {
	bin.mtx.Lock()
	seq := bin.logSeq
	bin.mtx.Unlock()
	return seq
}

// Only for master/slaver mode
func (bin *BinLog) GetMasterSeq() (masterSeq uint64, valid bool) {
	masterSeq = 0
	bin.mtx.Lock()
	if len(bin.infos) > 0 {
		masterSeq = bin.infos[len(bin.infos)-1].MaxSeq
	}
	bin.mtx.Unlock()

	valid = true
	if masterSeq > 0 && masterSeq < MinNormalSeq {
		valid = false
	}
	return
}

func (bin *BinLog) GetLastLogSeq() (lastLogSeq uint64, valid bool) {
	bin.mtx.Lock()
	lastLogSeq, valid = bin.logSeq, len(bin.reqChan) == 0
	bin.mtx.Unlock()
	return
}

func (bin *BinLog) AddRequest(req *Request) {
	bin.reqChan <- req
}

func (bin *BinLog) GetBinFileName(fileIdx uint64) string {
	return fmt.Sprintf("%s/%06d.bin", bin.dir, fileIdx)
}

func (bin *BinLog) GetSeqFileName(fileIdx uint64) string {
	return fmt.Sprintf("%s/%06d.seq", bin.dir, fileIdx)
}

func (bin *BinLog) GoWriteBinLog() {
	var lastReq *Request
	var tick = time.Tick(1e9)
	var ms []Monitor
	for {
		select {
		case req, ok := <-bin.reqChan:
			if !ok {
				log.Printf("binlog channel closed %p\n", bin)
				return
			}

			bin.mtx.Lock()
			if bin.hasMaster && req.MasterSeq > 0 {
				bin.logSeq = req.MasterSeq
			} else {
				bin.logSeq++
			}
			if bin.msChanged {
				bin.msChanged = false
				ms = make([]Monitor, len(bin.monitors))
				copy(ms, bin.monitors)
			}
			bin.mtx.Unlock()

			for _, ms := range ms {
				ms.NewLogComming()
			}

			proto.OverWriteSeq(req.Pkg, bin.logSeq)
			bin.doWrite(req, bin.logSeq)

			lastReq = req

		case <-tick:
			if bin.binBufW != nil {
				bin.binBufW.Flush()
			}
			if lastReq != nil {
				log.Printf("Write binlog: seq=%d, masterSeq=%d\n",
					bin.logSeq, lastReq.MasterSeq)
				lastReq = nil
			}
		}
	}
}

func (bin *BinLog) doWrite(req *Request, logSeq uint64) error {
	if bin.usedLen+len(req.Pkg) > len(bin.memlog) {
		if bin.binFile != nil {
			bin.binBufW.Flush()
			bin.binBufW = nil
			bin.binFile.Close()
			bin.binFile = nil

			bin.mtx.Lock()
			bin.usedLen = 0
			bin.infos[len(bin.infos)-1].Done = true
			var fi = *bin.infos[len(bin.infos)-1]
			var delIdxs = bin.selectDelBinLogFiles()
			bin.mtx.Unlock()

			bin.writeSeqFile(&fi)
			bin.deleteOldBinLogFiles(delIdxs)
		}
	}

	var err error
	if bin.binFile == nil {
		bin.mtx.Lock()
		bin.fileIdx++
		bin.mtx.Unlock()

		var name = bin.GetBinFileName(bin.fileIdx)
		bin.binFile, err = os.Create(name)
		if err != nil {
			log.Printf("create file failed: (%s) %s\n", name, err)
			return err
		}

		bin.binBufW = bufio.NewWriter(bin.binFile)

		bin.mtx.Lock()
		bin.infos = append(bin.infos, &fileInfo{bin.fileIdx, logSeq, 0, false})
		bin.mtx.Unlock()
	}

	copy(bin.memlog[bin.usedLen:], req.Pkg)
	bin.binBufW.Write(req.Pkg)

	bin.mtx.Lock()
	bin.usedLen += len(req.Pkg)
	bin.infos[len(bin.infos)-1].MaxSeq = logSeq
	bin.mtx.Unlock()

	return nil
}

func (bin *BinLog) selectDelBinLogFiles() []uint64 {
	var delIdxs []uint64

	var minSeq = bin.logSeq
	for _, rs := range bin.rseqs {
		if minSeq > rs.seq {
			minSeq = rs.seq
		}
	}
	var delPivot = -1
	for i := 0; i < len(bin.infos)-keepBinFileNum; i++ {
		if bin.infos[i].MaxSeq < minSeq {
			delPivot = i
		}
	}
	for i := 0; i <= delPivot; i++ {
		delIdxs = append(delIdxs, bin.infos[i].Idx)
	}
	if delPivot > -1 {
		bin.infos = bin.infos[delPivot+1:]
	}

	return delIdxs
}

func (bin *BinLog) deleteOldBinLogFiles(idxs []uint64) {
	for _, idx := range idxs {
		os.Remove(bin.GetBinFileName(idx))
		os.Remove(bin.GetSeqFileName(idx))
	}
}

func (bin *BinLog) loadAllFilesIndex() ([]uint64, error) {
	dir, err := os.Open(bin.dir)
	if err != nil {
		return nil, err
	}

	fi, err := dir.Readdir(-1)
	if err != nil {
		return nil, err
	}

	var idxs []uint64
	for _, f := range fi {
		if f.IsDir() {
			continue
		}

		var idx uint64
		n, err := fmt.Sscanf(f.Name(), "%d.bin", &idx)
		if err != nil || n != 1 {
			continue
		}

		idxs = append(idxs, idx)
	}

	sort.Sort(util.Uint64Slice(idxs))

	return idxs, nil
}

func (bin *BinLog) writeSeqFile(fi *fileInfo) error {
	var name = bin.GetSeqFileName(fi.Idx)
	file, err := os.Create(name)
	if err != nil {
		return err
	}
	defer file.Close()

	en := json.NewEncoder(file)

	err = en.Encode(fi)
	if err != nil {
		return err
	}

	return nil
}

func (bin *BinLog) fixSeqFile(idx uint64) (fileInfo, error) {
	var fi fileInfo
	var name = bin.GetBinFileName(idx)
	file, err := os.Open(name)
	if err != nil {
		return fi, err
	}

	var r = bufio.NewReader(file)
	var headBuf = make([]byte, proto.HeadSize)
	var head proto.PkgHead
	for {
		_, err := proto.ReadPkg(r, headBuf, &head, nil)
		if err != nil {
			break
		}
		if fi.MinSeq == 0 {
			fi.MinSeq = head.Seq
			fi.Idx = idx
			fi.Done = true
		}
		if fi.MaxSeq < head.Seq {
			fi.MaxSeq = head.Seq
		}
	}

	file.Close()

	if fi.Idx == 0 {
		os.Remove(bin.GetBinFileName(idx))
		os.Remove(bin.GetSeqFileName(idx))
		return fi, fmt.Errorf("no record in bin file id %d", idx)
	}

	return fi, bin.writeSeqFile(&fi)
}

func (bin *BinLog) loadAndFixSeqFiles(idxs []uint64) error {
	for _, idx := range idxs {
		var needFix bool
		var name = bin.GetSeqFileName(idx)
		file, err := os.Open(name)
		if err != nil {
			if os.IsNotExist(err) {
				needFix = true
			} else {
				return err
			}
		}

		var fi fileInfo
		if !needFix {
			de := json.NewDecoder(file)
			err = de.Decode(&fi)
			if err != nil {
				needFix = true
			}
			file.Close()
		}

		if needFix {
			fi, err = bin.fixSeqFile(idx)
		}

		if err == nil && fi.Idx > 0 {
			fi.Done = true
			bin.infos = append(bin.infos, &fi)
		}
	}

	return nil
}
