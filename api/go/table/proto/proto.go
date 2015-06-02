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
	ErrPkgLen     = errors.New("pkg length out of range")
	ErrHeadCrc    = errors.New("pkg head crc mismatch")
	ErrRowKeyLen  = errors.New("row key length out of range")
	ErrColKeyLen  = errors.New("col key length out of range")
	ErrKvArrayLen = errors.New("key/value array length out of range")
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
	CmdSlaveOf = 0xD0
	CmdMigrate = 0xD1 // Start/Stop migration
	CmdSlaveSt = 0xD2 // Get migration/slave status
	CmdDelSlot = 0xD3 // Delete slot data
)

const (
	AdminDbId   = 255
	HeadSize    = 15
	MaxUint8    = 255
	MaxUint16   = 65535
	MaxValueLen = 1024 * 1024     // 1MB
	MaxPkgLen   = 1024 * 1024 * 2 // 2MB
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

// cCrc+cCmd+cDbId+ddwSeq+dwPkgLen+sBody
type PkgHead struct {
	Crc    uint8 // Head CRC
	Cmd    uint8
	DbId   uint8
	Seq    uint64 // normal: request seq; replication: master binlog seq
	PkgLen uint32
}

func (head *PkgHead) Decode(pkg []byte) (int, error) {
	if len(pkg) < HeadSize {
		return 0, ErrPkgLen
	}

	head.Crc = CalHeadCrc(pkg)
	if head.Crc != pkg[0] {
		return 0, ErrHeadCrc
	}

	head.Cmd = pkg[1]
	head.DbId = pkg[2]
	head.Seq = binary.BigEndian.Uint64(pkg[3:])
	head.PkgLen = binary.BigEndian.Uint32(pkg[11:])

	return HeadSize, nil
}

func (head *PkgHead) Encode(pkg []byte) (int, error) {
	if len(pkg) < HeadSize {
		return 0, ErrPkgLen
	}

	pkg[1] = head.Cmd
	pkg[2] = head.DbId
	binary.BigEndian.PutUint64(pkg[3:], head.Seq)
	binary.BigEndian.PutUint32(pkg[11:], head.PkgLen)
	pkg[0] = CalHeadCrc(pkg)

	return HeadSize, nil
}

func CalHeadCrc(pkg []byte) uint8 {
	var crc int8 = 10
	for i := 1; i < HeadSize; i++ {
		crc += int8(pkg[i])
	}
	return uint8(crc)
}

func OverWriteLen(pkg []byte, pkgLen int) {
	binary.BigEndian.PutUint32(pkg[11:], uint32(pkgLen))
	pkg[0] = CalHeadCrc(pkg)
}

func OverWriteSeq(pkg []byte, seq uint64) {
	binary.BigEndian.PutUint64(pkg[3:], seq)
	pkg[0] = CalHeadCrc(pkg)
}

func ReadPkg(r *bufio.Reader, headBuf []byte, head *PkgHead,
	pkgBuf []byte) (pkg []byte, err error) {
	if len(headBuf) != HeadSize {
		headBuf = make([]byte, HeadSize)
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
