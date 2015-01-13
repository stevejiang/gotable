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
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/stevejiang/gotable/api/go/table/proto"
	"github.com/stevejiang/gotable/util"
	"io"
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
)

const (
	seqFileHeadSize = 32
	seqRecordOne    = 0x1 // One-2-one record of binlog seq
	seqRecordAll    = 0x2 // Summary record of master seq
)

type seqFileHead struct {
	minSeq  uint64
	maxSeq  uint64
	tailPos uint64
	tailNum uint16
}

type fileInfo struct {
	idx    uint64
	minSeq uint64
	maxSeq uint64
	done   bool
}

type readerSeq struct {
	seq uint64
}

type Request struct {
	MasterId  uint16
	MasterSeq uint64
	Pkg       []byte
}

type BinLog struct {
	dir     string
	reqChan chan *Request

	binFile *os.File
	binBufW *bufio.Writer
	seqFile *os.File
	seqBufW *bufio.Writer

	mtx       sync.Mutex // The following variables are protected by mtx
	msChanged bool
	monitors  []Monitor

	seqHead   seqFileHead
	masterSeq map[uint16]uint64 //master id => last master binlog seq

	fileIdx    uint64
	logSeq     uint64
	lastLogSeq uint64

	memlog  []byte
	usedLen int

	infos []*fileInfo  // All binlog files
	rseqs []*readerSeq // Most recent reader seq
}

func NewBinLog(dir string) *BinLog {
	var bin = new(BinLog)
	bin.dir = dir
	bin.reqChan = make(chan *Request, 10000)

	bin.msChanged = true
	bin.logSeq = 1000000 // start from here
	bin.memlog = make([]byte, memBinLogSize)

	err := bin.init()
	if err != nil {
		log.Fatalf("init binlog failed: %s\n", err)
	}

	log.Printf("Last binlog: fileIdx %d, logSeq %d\n", bin.fileIdx, bin.logSeq)

	return bin
}

func (bin *BinLog) init() error {
	idxs, err := bin.loadAllFilesIndex()
	if err != nil {
		return err
	}

	err = bin.loadAndFixHeadAndTail(idxs)
	if err != nil {
		return err
	}

	if len(bin.infos) > 0 {
		bin.fileIdx = bin.infos[len(bin.infos)-1].idx
		bin.logSeq = bin.infos[len(bin.infos)-1].maxSeq
		bin.lastLogSeq = bin.logSeq + uint64(len(bin.reqChan))
	}

	var delIdxs []uint64
	if len(bin.infos) > keepBinFileNum {
		for i := 0; i < len(bin.infos)-keepBinFileNum; i++ {
			delIdxs = append(delIdxs, bin.infos[i].idx)
		}
	}

	bin.deleteOldBinLogFiles(delIdxs)

	return nil
}

func (bin *BinLog) RegisterMonitor(m Monitor) {
	bin.mtx.Lock()
	defer bin.mtx.Unlock()

	bin.monitors = append(bin.monitors, m)
	bin.msChanged = true
}

func (bin *BinLog) RemoveMonitor(m Monitor) {
	bin.mtx.Lock()
	defer bin.mtx.Unlock()

	for index, tmp := range bin.monitors {
		if tmp == m {
			copy(bin.monitors[index:], bin.monitors[index+1:])
			bin.monitors = bin.monitors[:len(bin.monitors)-1]
			log.Println("Remove one monitor from monitors")
			break
		}
	}
}

func (bin *BinLog) GetMasterSeq(masterId uint16) uint64 {
	bin.mtx.Lock()
	defer bin.mtx.Unlock()

	if v, ok := bin.masterSeq[masterId]; ok {
		return v
	} else {
		return 0
	}
}

func (bin *BinLog) DropMaster(masterId uint16) {
	bin.mtx.Lock()
	defer bin.mtx.Unlock()

	delete(bin.masterSeq, masterId)
}

func (bin *BinLog) GetLogSeq() uint64 {
	bin.mtx.Lock()
	defer bin.mtx.Unlock()
	return bin.logSeq
}

func (bin *BinLog) GetLastLogSeq() (lastLogSeq uint64, valid bool) {
	bin.mtx.Lock()
	defer bin.mtx.Unlock()
	return bin.lastLogSeq, len(bin.reqChan)*2 < cap(bin.reqChan)
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

func (bin *BinLog) GetSeqFileTmpName(fileIdx uint64) string {
	return fmt.Sprintf("%s/%06d.seq.tmp", bin.dir, fileIdx)
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
			bin.logSeq++
			bin.lastLogSeq = bin.logSeq + uint64(len(bin.reqChan))
			if bin.msChanged {
				bin.msChanged = false
				ms = make([]Monitor, len(bin.monitors))
				copy(ms, bin.monitors)
			}
			bin.mtx.Unlock()

			for _, ms := range ms {
				ms.NewLogComming()
			}

			proto.OverwriteSeq(req.Pkg, bin.logSeq)
			bin.doWrite(req, bin.logSeq)

			lastReq = req

		case <-tick:
			if bin.binBufW != nil {
				bin.binBufW.Flush()
			}
			if bin.seqBufW != nil {
				bin.seqBufW.Flush()
			}
			if lastReq != nil {
				log.Printf("Write binlog: seq=%d, mId=%d, mSeq=%d\n",
					bin.logSeq, lastReq.MasterId, lastReq.MasterSeq)
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

			if bin.seqFile != nil {
				bin.seqBufW.Flush()
				bin.seqBufW = nil

				var mSeq = make(map[uint16]uint64)
				bin.mtx.Lock()
				for k, v := range bin.masterSeq {
					mSeq[k] = v
				}
				var minSeq = bin.seqHead.minSeq
				var maxSeq = bin.seqHead.maxSeq
				bin.seqHead = seqFileHead{}
				bin.mtx.Unlock()

				bin.writeSeqFileHeadAndTail(mSeq, minSeq, maxSeq)
				bin.seqFile.Close()
				bin.seqFile = nil
			}

			bin.mtx.Lock()
			bin.usedLen = 0
			bin.infos[len(bin.infos)-1].done = true
			var delIdxs = bin.selectDelBinLogFiles()
			bin.mtx.Unlock()

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

		name = bin.GetSeqFileName(bin.fileIdx)
		bin.seqFile, err = os.Create(name)
		if err != nil {
			log.Printf("create file failed: (%s) %s\n", name, err)
			return err
		}

		bin.binBufW = bufio.NewWriter(bin.binFile)
		bin.seqBufW = bufio.NewWriterSize(bin.seqFile, 1024)

		bin.seqBufW.Write(make([]byte, seqFileHeadSize))

		bin.mtx.Lock()
		bin.infos = append(bin.infos, &fileInfo{bin.fileIdx, logSeq, 0, false})
		bin.seqHead.minSeq = logSeq
		bin.mtx.Unlock()
	}

	copy(bin.memlog[bin.usedLen:], req.Pkg)
	bin.binBufW.Write(req.Pkg)
	bin.writeSeqFileRecord(req, logSeq)

	bin.mtx.Lock()
	bin.usedLen += len(req.Pkg)
	bin.infos[len(bin.infos)-1].maxSeq = logSeq
	bin.seqHead.maxSeq = logSeq
	if req.MasterId > 0 {
		if bin.masterSeq[req.MasterId] < req.MasterSeq {
			bin.masterSeq[req.MasterId] = req.MasterSeq
		}
	}
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
		if bin.infos[i].maxSeq < minSeq {
			delPivot = i
		}
	}
	for i := 0; i <= delPivot; i++ {
		delIdxs = append(delIdxs, bin.infos[i].idx)
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

func (bin *BinLog) writeSeqFileRecord(req *Request, logSeq uint64) {
	var oneRecord = make([]byte, 19)
	oneRecord[0] = byte(seqRecordOne)
	binary.BigEndian.PutUint16(oneRecord[1:], req.MasterId)
	binary.BigEndian.PutUint64(oneRecord[3:], req.MasterSeq)
	binary.BigEndian.PutUint64(oneRecord[11:], logSeq)
	bin.seqBufW.Write(oneRecord)
}

func (bin *BinLog) writeSeqFileHeadAndTail(mSeq map[uint16]uint64,
	minSeq, maxSeq uint64) error {

	tailPos, err := bin.seqFile.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}

	var head = make([]byte, seqFileHeadSize)
	var tail bytes.Buffer

	//tail
	tailNum := uint16(len(mSeq))
	var tmp = make([]byte, 11)
	for id, seq := range mSeq {
		tmp[0] = byte(seqRecordAll)
		binary.BigEndian.PutUint16(tmp[1:], id)
		binary.BigEndian.PutUint64(tmp[3:], seq)
		tail.Write(tmp)
	}

	// head
	binary.BigEndian.PutUint64(head, minSeq)
	binary.BigEndian.PutUint64(head[8:], maxSeq)
	binary.BigEndian.PutUint64(head[16:], uint64(tailPos))
	binary.BigEndian.PutUint16(head[24:], tailNum)

	_, err = bin.seqFile.Write(tail.Bytes())
	if err != nil {
		return err
	}

	_, err = bin.seqFile.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}

	_, err = bin.seqFile.Write(head)
	if err != nil {
		return err
	}

	return nil
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

func (bin *BinLog) loadAndFixHeadAndTail(idxs []uint64) error {
	var mSeq = make(map[uint16]uint64)

	for _, idx := range idxs {
		var name = bin.GetSeqFileName(idx)
		f, err := os.Open(name)
		if err != nil {
			if os.IsNotExist(err) {
				// skip the idx
				continue
			} else {
				return err
			}
		}

		var head = make([]byte, seqFileHeadSize)
		_, err = f.Read(head)
		if err != nil {
			f.Close()
			// skip the idx
			continue
		}

		var minSeq = binary.BigEndian.Uint64(head)
		var maxSeq = binary.BigEndian.Uint64(head[8:])
		var tailPos = binary.BigEndian.Uint64(head[16:])
		var tailNum = binary.BigEndian.Uint16(head[24:])

		var failed = false
		if tailPos > 0 {
			f.Seek(int64(tailPos), os.SEEK_SET)

			var r = bufio.NewReader(f)
			var oneRecord = make([]byte, 11)
			var tmpSeq = make(map[uint16]uint64)
			for {
				_, err := io.ReadFull(r, oneRecord)
				if err != nil {
					if err == io.EOF || err == io.ErrUnexpectedEOF {
						break
					} else {
						f.Close()
						return err
					}
				}

				if oneRecord[0] != byte(seqRecordAll) {
					break
				}

				var masterId = binary.BigEndian.Uint16(oneRecord[1:])
				var masterSeq = binary.BigEndian.Uint64(oneRecord[3:])
				tmpSeq[masterId] = masterSeq
			}

			if int(tailNum) == len(tmpSeq) {
				f.Close()
				mSeq = tmpSeq
				log.Printf("Succeed to parse seq tail\n")
			} else {
				f.Seek(seqFileHeadSize, os.SEEK_SET)
				failed = true
				log.Printf("Failed to parse seq tail! try scan seq file\n")
			}
		}

		// Fix seq file
		if tailPos == 0 || failed {
			var r = bufio.NewReader(f)
			var oneRecord = make([]byte, 19)
			var first = true
			for {
				_, err := io.ReadFull(r, oneRecord)
				if err != nil {
					if err == io.EOF || err == io.ErrUnexpectedEOF {
						break
					} else {
						f.Close()
						return err
					}
				}

				if oneRecord[0] != byte(seqRecordOne) {
					break
				}

				var masterId = binary.BigEndian.Uint16(oneRecord[1:])
				var masterSeq = binary.BigEndian.Uint64(oneRecord[3:])
				var logSeq = binary.BigEndian.Uint64(oneRecord[11:])

				if masterId > 0 && masterSeq > 0 && mSeq[masterId] < masterSeq {
					mSeq[masterId] = masterSeq
				}

				maxSeq = logSeq
				if first {
					first = false
					minSeq = logSeq
				}

				//log.Printf("%d\t%d\t%d\n", logSeq, masterId, masterSeq)
			}

			f.Close()

			var tmpName = bin.GetSeqFileTmpName(idx)
			bin.seqFile, err = os.Create(tmpName)
			if err != nil {
				return err
			}

			log.Printf("Fix seq file (%s): %v\n", name, mSeq)
			_, err = bin.seqFile.Write(make([]byte, seqFileHeadSize))
			if err != nil {
				bin.seqFile.Close()
				bin.seqFile = nil
				return err
			}

			err = bin.writeSeqFileHeadAndTail(mSeq, minSeq, maxSeq)
			bin.seqFile.Close()
			bin.seqFile = nil
			if err != nil {
				return err
			}

			err = os.Rename(tmpName, name)
			if err != nil {
				return err
			}
		}

		bin.infos = append(bin.infos, &fileInfo{idx, minSeq, maxSeq, true})
	}

	bin.masterSeq = mSeq
	return nil
}
