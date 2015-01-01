package proto

import (
	"bufio"
	"encoding/binary"
	"errors"
)

var (
	ErrPkgLen    = errors.New("invalid pkg length")
	ErrPkgStx    = errors.New("invalid stx")
	ErrHeadBuf   = errors.New("invalid head buffer size")
	ErrRowKeyLen = errors.New("invalid row key length")
	ErrColKeyLen = errors.New("invalid col key length")
	ErrArrayLen  = errors.New("array or slice length is too large")
)

const (
	EcodeOk            = 0 // Success
	EcodeNotExist      = 1 // Key not exist
	EcodeReadDbFailed  = 11
	EcodeWriteDbFailed = 12
)

const (
	// Front Read
	CmdPing = 0x10
	CmdGet  = 0x11
	CmdMGet = 0x12

	// Front Write
	CmdPut  = 0x60
	CmdMPut = 0x61

	// Inner Write
	CmdSync = 0xB0

	// Inner CTRL
	CmdMaster = 0xE0
)

// cStx+cCmd+cDbId+ddwSeq+dwPkgLen+sBody
const (
	stx          = 0xFF
	HeadSize     = 15
	MaxPkgLen    = 1024 * 1024
	MaxRowKeyLen = 255
	MaxColKeyLen = 65535
	MaxUint8     = 255
	MaxUint16    = 65535
)

type PkgEncoding interface {
	DecodeHead(pkg []byte) error
	EncodeHead(pkg []byte) error
	Length() int
	Encode(ppkg *[]byte) (n int, err error)
	Decode(pkg []byte) (n int, err error)
}

type PkgHead struct {
	stx    uint8
	Cmd    uint8
	DbId   uint8
	Seq    uint64 //normal: request seq; replication: master binlog seq
	PkgLen uint32
}

func (head *PkgHead) DecodeHead(pkg []byte) error {
	if len(pkg) < HeadSize {
		return ErrPkgLen
	}

	if pkg[0] != stx {
		return ErrPkgStx
	}

	// cStx+cCmd+cDbId+ddwSeq+dwPkgLen+sBody
	head.stx = pkg[0]
	head.Cmd = pkg[1]
	head.DbId = pkg[2]
	head.Seq = binary.BigEndian.Uint64(pkg[3:])
	head.PkgLen = binary.BigEndian.Uint32(pkg[11:])

	return nil
}

func (head *PkgHead) EncodeHead(pkg []byte) error {
	if len(pkg) < HeadSize {
		return ErrPkgLen
	}

	// cStx+cCmd+cDbId+ddwSeq+dwPkgLen+sBody
	pkg[0] = stx
	pkg[1] = head.Cmd
	pkg[2] = head.DbId
	binary.BigEndian.PutUint64(pkg[3:], head.Seq)
	binary.BigEndian.PutUint32(pkg[11:], head.PkgLen)

	return nil
}

func OverwriteSeq(pkg []byte, seq uint64) error {
	if len(pkg) < HeadSize {
		return ErrPkgLen
	}

	binary.BigEndian.PutUint64(pkg[3:], seq)
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
