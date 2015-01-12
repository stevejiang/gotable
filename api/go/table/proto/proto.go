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

package proto

import (
	"bufio"
	"encoding/binary"
	"errors"
)

var (
	ErrPkgLen     = errors.New("invalid pkg length")
	ErrHeadBuf    = errors.New("invalid head buffer size")
	ErrRowKeyLen  = errors.New("invalid row key length")
	ErrColKeyLen  = errors.New("invalid col key length")
	ErrValueLen   = errors.New("invalid value length")
	ErrKvArrayLen = errors.New("invalid key/value array length")
	ErrArrayLen   = errors.New("array or slice length out of range")
)

const (
	// Front Read
	CmdPing = 0x10
	CmdGet  = 0x11
	CmdMGet = 0x12
	CmdScan = 0x13
	CmdDump = 0x14

	// Front Write
	CmdSet   = 0x60
	CmdMSet  = 0x61
	CmdDel   = 0x62
	CmdMDel  = 0x63
	CmdIncr  = 0x64
	CmdMIncr = 0x65

	// Inner SYNC
	CmdSync = 0xB0

	// Inner CTRL
	CmdMaster = 0xE0
)

// cCmd+cDbId+ddwSeq+dwPkgLen+sBody
const (
	HeadSize    = 14
	MaxPkgLen   = 1024 * 1024
	MaxValueLen = 512 * 1024
	MaxUint8    = 255
	MaxUint16   = 65535
)

type PkgEncoding interface {
	DecodeHead(pkg []byte) error
	EncodeHead(pkg []byte) error
	Length() int
	Encode(ppkg *[]byte) (n int, err error)
	Decode(pkg []byte) (n int, err error)
}

type PkgHead struct {
	Cmd    uint8
	DbId   uint8
	Seq    uint64 //normal: request seq; replication: master binlog seq
	PkgLen uint32
}

func (head *PkgHead) DecodeHead(pkg []byte) error {
	if len(pkg) < HeadSize {
		return ErrPkgLen
	}

	// cCmd+cDbId+ddwSeq+dwPkgLen+sBody
	head.Cmd = pkg[0]
	head.DbId = pkg[1]
	head.Seq = binary.BigEndian.Uint64(pkg[2:])
	head.PkgLen = binary.BigEndian.Uint32(pkg[10:])

	return nil
}

func (head *PkgHead) EncodeHead(pkg []byte) error {
	if len(pkg) < HeadSize {
		return ErrPkgLen
	}

	// cCmd+cDbId+ddwSeq+dwPkgLen+sBody
	pkg[0] = head.Cmd
	pkg[1] = head.DbId
	binary.BigEndian.PutUint64(pkg[2:], head.Seq)
	binary.BigEndian.PutUint32(pkg[10:], head.PkgLen)

	return nil
}

func OverwriteSeq(pkg []byte, seq uint64) error {
	if len(pkg) < HeadSize {
		return ErrPkgLen
	}

	binary.BigEndian.PutUint64(pkg[2:], seq)
	return nil
}

func ReadPkg(r *bufio.Reader, headBuf []byte, head *PkgHead, pkgBuf []byte) (pkg []byte, err error) {
	if len(headBuf) != HeadSize {
		return nil, ErrHeadBuf
	}

	if head == nil {
		head = new(PkgHead)
	}

	var readLen int
	for {
		n, err := r.Read(headBuf[readLen:])
		if err != nil {
			return nil, err
		}

		readLen += n

		if readLen < HeadSize {
			continue
		}

		err = head.DecodeHead(headBuf)
		if err != nil {
			return nil, err
		}

		var pkgLen = int(head.PkgLen)
		if pkgLen > MaxPkgLen {
			return nil, ErrPkgLen
		}

		if cap(pkgBuf) < pkgLen {
			pkg = make([]byte, pkgLen)
		} else {
			pkg = pkgBuf[:pkgLen]
		}

		copy(pkg, headBuf)

		for readLen < pkgLen {
			n, err = r.Read(pkg[readLen:])
			if err != nil {
				return nil, err
			}
			readLen += n
		}

		return pkg, nil
	}
}
