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
	// Front CTRL
	CmdAuth = 0x9

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
	CmdSync   = 0xB0 // Sync data
	CmdSyncSt = 0xB1 // Sync status

	// Inner CTRL
	CmdSlaveOf   = 0xD0
	CmdMigrate   = 0xD1 // Start/Stop migration
	CmdMigStatus = 0xD2 // Get migration status
	CmdDelUnit   = 0xD3 // Delete unit data
)

const (
	AdminDbId    = 255
	HeadSize     = 14
	MaxUint8     = 255
	MaxUint16    = 65535
	MaxRowKeyLen = 255
	MaxValueLen  = 512 * 1024
	MaxPkgLen    = 1024 * 1024
)

type PkgEncoding interface {
	Length() int
	Encode(pkg []byte) (int, error)
	Decode(pkg []byte) (int, error)
}

type PkgResponse interface {
	PkgEncoding
	SetErrCode(errCode int8)
}

// cCmd+cDbId+ddwSeq+dwPkgLen+sBody
type PkgHead struct {
	Cmd    uint8
	DbId   uint8
	Seq    uint64 //normal: request seq; replication: master binlog seq
	PkgLen uint32
}

func (head *PkgHead) Decode(pkg []byte) (int, error) {
	if len(pkg) < HeadSize {
		return 0, ErrPkgLen
	}

	head.Cmd = pkg[0]
	head.DbId = pkg[1]
	head.Seq = binary.BigEndian.Uint64(pkg[2:])
	head.PkgLen = binary.BigEndian.Uint32(pkg[10:])

	return HeadSize, nil
}

func (head *PkgHead) Encode(pkg []byte) (int, error) {
	if len(pkg) < HeadSize {
		return 0, ErrPkgLen
	}

	pkg[0] = head.Cmd
	pkg[1] = head.DbId
	binary.BigEndian.PutUint64(pkg[2:], head.Seq)
	binary.BigEndian.PutUint32(pkg[10:], head.PkgLen)

	return HeadSize, nil
}

func OverWriteLen(pkg []byte, pkgLen int) {
	binary.BigEndian.PutUint32(pkg[10:], uint32(pkgLen))
}

func OverWriteSeq(pkg []byte, seq uint64) {
	binary.BigEndian.PutUint64(pkg[2:], seq)
}

func ReadPkg(r *bufio.Reader, headBuf []byte, head *PkgHead,
	pkgBuf []byte) (pkg []byte, err error) {
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

		_, err = head.Decode(headBuf)
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
