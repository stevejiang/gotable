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
	"errors"
	"github.com/stevejiang/gotable/api/go/table/proto"
	"log"
	"os"
)

var (
	ErrLogMissing = errors.New("log file missing")
	ErrUnexpected = errors.New("unexpected binlog reader error")
)

type Reader struct {
	bin       *BinLog
	curFile   *os.File
	curBufR   *bufio.Reader
	curMemPos int // -1: means read from file; >=0: means read from memory buffer
	curInfo   fileInfo

	// temp variable
	headBuf []byte
	head    proto.PkgHead

	// protected by bin.mtx
	rseq *readerSeq
}

func NewReader(bin *BinLog) *Reader {
	var r = new(Reader)
	r.bin = bin
	r.curMemPos = -1
	r.rseq = new(readerSeq)
	r.headBuf = make([]byte, proto.HeadSize)

	return r
}

func (r *Reader) Close() {
	if r.curFile != nil {
		r.curFile.Close()
		r.curFile = nil
	}

	r.bin.mtx.Lock()
	defer r.bin.mtx.Unlock()
	for index, tmp := range r.bin.rseqs {
		if tmp == r.rseq {
			copy(r.bin.rseqs[index:], r.bin.rseqs[index+1:])
			r.bin.rseqs = r.bin.rseqs[:len(r.bin.rseqs)-1]
			break
		}
	}

	r.bin = nil
	r.curBufR = nil
	r.curMemPos = -1
	r.curInfo.Idx = 0
	r.rseq = nil
}

func (r *Reader) Init(lastSeq uint64) error {
	var err error
	r.bin.mtx.Lock()

	r.bin.rseqs = append(r.bin.rseqs, r.rseq)
	r.rseq.seq = lastSeq

	var index = -1
	for i, f := range r.bin.infos {
		index = i
		if lastSeq < f.MinSeq {
			break
		}

		if f.MaxSeq >= lastSeq || f.MaxSeq == 0 {
			break
		}
	}

	if index < 0 {
		r.bin.mtx.Unlock() // unlock immediately
		if lastSeq > 0 && lastSeq != MinNormalSeq {
			return ErrLogMissing
		}
		r.curMemPos = -1
		r.curInfo.Idx = 0
		return nil
	} else {
		r.curInfo = *r.bin.infos[index]
		if lastSeq > 0 && lastSeq != MinNormalSeq && r.curInfo.MinSeq > lastSeq+1 {
			r.bin.mtx.Unlock() // unlock immediately
			return ErrLogMissing
		}
	}

	r.rseq.seq = r.curInfo.MinSeq

	if r.curInfo.Done {
		r.bin.mtx.Unlock() // unlock immediately

		var name = r.bin.GetBinFileName(r.curInfo.Idx)
		r.curFile, err = os.Open(name)
		if err != nil {
			log.Printf("open file failed: (%s) %s\n", name, err)
			return err
		}

		if r.curBufR == nil {
			r.curBufR = bufio.NewReader(r.curFile)
		} else {
			r.curBufR.Reset(r.curFile)
		}

		if r.curInfo.MinSeq < lastSeq+1 {
			var pkgBuf = make([]byte, 4096)
			for {
				_, err = proto.ReadPkg(r.curBufR, r.headBuf, &r.head, pkgBuf)
				if err != nil {
					return err
				}

				if r.head.Seq >= lastSeq {
					break
				}
			}
		}
	} else {
		defer r.bin.mtx.Unlock() // wait until func finished

		if r.curInfo.Idx != r.bin.fileIdx {
			log.Printf("invalid file index (%d, %d)\n", r.curInfo.Idx, r.bin.fileIdx)
			return ErrUnexpected
		}
		r.curMemPos = 0

		if r.curInfo.MinSeq < lastSeq+1 {
			for {
				if r.curMemPos+proto.HeadSize > r.bin.usedLen {
					return ErrUnexpected
				}

				_, err = r.head.Decode(r.bin.memlog[r.curMemPos:])
				if err != nil {
					return ErrUnexpected
				}

				if r.curMemPos+int(r.head.PkgLen) > r.bin.usedLen {
					return ErrUnexpected
				}

				r.curMemPos += int(r.head.PkgLen)
				if r.head.Seq >= lastSeq {
					break
				}
			}
		}
	}

	return nil
}

func (r *Reader) nextFile() []byte {
	if r.curFile != nil {
		r.curFile.Close()
		r.curFile = nil
	}

	r.curMemPos = -1

	var find bool
	var curIdx = r.curInfo.Idx

	r.bin.mtx.Lock()
	var memFileIdx = r.bin.fileIdx
	var infos = r.bin.infos
	if curIdx > 0 {
		for i := len(infos) - 1; i >= 0; i-- {
			if infos[i].Idx <= curIdx {
				break
			}
			if infos[i].Idx == curIdx+1 || (i > 0 && infos[i-1].Idx == curIdx) {
				r.curInfo = *infos[i]
				find = true
				break
			}
		}
	} else if len(infos) > 0 {
		r.curInfo = *infos[0]
		find = true
	}
	if find {
		r.rseq.seq = r.curInfo.MinSeq
	}
	r.bin.mtx.Unlock()

	if !find {
		return nil
	}

	var err error
	if r.curInfo.Done {
		var name = r.bin.GetBinFileName(r.curInfo.Idx)
		r.curFile, err = os.Open(name)
		if err != nil {
			log.Printf("open file failed: (%s) %s\n", name, err)
			return nil
		}

		if r.curBufR == nil {
			r.curBufR = bufio.NewReader(r.curFile)
		} else {
			r.curBufR.Reset(r.curFile)
		}
	} else {
		if r.curInfo.Idx != memFileIdx {
			log.Printf("invalid file index (%d, %d)\n", r.curInfo.Idx, memFileIdx)
			return nil
		}
		r.curMemPos = 0
	}

	return r.next()
}

func (r *Reader) next() []byte {
	var err error
	if r.curFile != nil {
		pkg, err := proto.ReadPkg(r.curBufR, r.headBuf, &r.head, nil)
		if err != nil {
			return r.nextFile()
		}

		return pkg
	} else if r.curMemPos >= 0 {
		r.bin.mtx.Lock()
		if r.curInfo.Idx != r.bin.fileIdx {
			for _, f := range r.bin.infos {
				if f.Idx == r.curInfo.Idx {
					r.curInfo = *f
					break
				}
			}
			r.bin.mtx.Unlock() // unlock immediately

			var name = r.bin.GetBinFileName(r.curInfo.Idx)
			r.curFile, err = os.Open(name)
			if err != nil {
				log.Printf("open file failed: (%s) %s\n", name, err)
				return nil
			}

			if r.curBufR == nil {
				r.curBufR = bufio.NewReader(r.curFile)
			} else {
				r.curBufR.Reset(r.curFile)
			}

			r.curFile.Seek(int64(r.curMemPos), 0)
			r.curMemPos = -1
			return r.next()
		}

		defer r.bin.mtx.Unlock() // wait to finish

		if r.curMemPos+proto.HeadSize > r.bin.usedLen {
			return nil
		}
		_, err = r.head.Decode(r.bin.memlog[r.curMemPos:])
		if err != nil {
			return nil
		}

		if r.curMemPos+int(r.head.PkgLen) > r.bin.usedLen {
			return nil
		}

		var pkg = make([]byte, int(r.head.PkgLen))
		copy(pkg, r.bin.memlog[r.curMemPos:])

		r.curMemPos += int(r.head.PkgLen)
		return pkg
	} else {
		return r.nextFile()
	}

	return nil
}

func (r *Reader) Next() []byte {
	return r.next()
}
