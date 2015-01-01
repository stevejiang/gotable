package proto

import (
	"encoding/binary"
)

// PKG = HEAD+cDbId+cTableId+cErrCode+cRowKeyLen+sRowKey+wColKeyLen+sColKey
type PkgKey struct {
	PkgHead
	DbId    uint8
	TableId uint8
	ErrCode uint8
	RowKey  []byte
	ColKey  []byte
}

// PKG = HEAD+cDbId+cTableId+cErrCode+cRowKeyLen+sRowKey+wColKeyLen+sColKey+dwValueLen+sValue
type PkgKeyValue struct {
	PkgKey
	Value []byte
}

type PkgCmdGetReq struct {
	PkgKey
}

type PkgCmdGetResp struct {
	PkgKeyValue
}

type PkgCmdPutReq struct {
	PkgKeyValue
}

type PkgCmdPutResp struct {
	PkgKey
}

func (p *PkgKey) Length() int {
	// PKG = HEAD+cDbId+cTableId+cErrCode+cRowKeyLen+sRowKey+wColKeyLen+sColKey
	return HeadSize + 4 + len(p.RowKey) + 2 + len(p.ColKey)
}

func (p *PkgKey) Encode(ppkg *[]byte) (n int, err error) {
	if len(p.RowKey) > MaxRowKeyLen {
		return 0, ErrRowKeyLen
	}

	if len(p.ColKey) > MaxColKeyLen {
		return 0, ErrColKeyLen
	}

	// PKG = HEAD+cDbId+cTableId+cErrCode+cRowKeyLen+sRowKey+wColKeyLen+sColKey
	var pkgLen = p.Length()
	if p.PkgLen < uint32(pkgLen) {
		p.PkgLen = uint32(pkgLen)
	}

	if len(*ppkg) < int(p.PkgLen) {
		*ppkg = make([]byte, int(p.PkgLen))
	}
	var pkg = *ppkg

	err = p.EncodeHead(pkg)
	if err != nil {
		return 0, err
	}

	n = HeadSize
	pkg[n] = p.DbId
	n += 1
	pkg[n] = p.TableId
	n += 1
	pkg[n] = p.ErrCode
	n += 1
	pkg[n] = uint8(len(p.RowKey))
	n += 1
	copy(pkg[n:], p.RowKey)
	n += len(p.RowKey)

	binary.BigEndian.PutUint16(pkg[n:], uint16(len(p.ColKey)))
	n += 2
	copy(pkg[n:], p.ColKey)
	n += len(p.ColKey)

	return n, nil
}

func (p *PkgKey) Decode(pkg []byte) (n int, err error) {
	err = p.DecodeHead(pkg)
	if err != nil {
		return 0, err
	}

	var pkgLen = len(pkg)
	if p.PkgLen > uint32(pkgLen) {
		return 0, ErrPkgLen
	}

	// PKG = HEAD+cDbId+cTableId+cErrCode+cRowKeyLen+sRowKey+wColKeyLen+sColKey
	n = HeadSize
	p.DbId = pkg[n]
	n += 1
	p.TableId = pkg[n]
	n += 1
	p.ErrCode = pkg[n]
	n += 1
	var rowKeyLen = int(pkg[n])
	n += 1
	if n+rowKeyLen > pkgLen {
		return n, ErrPkgLen
	}
	p.RowKey = pkg[n : n+rowKeyLen]
	n += rowKeyLen

	var colKeyLen = int(binary.BigEndian.Uint16(pkg[n:]))
	n += 2
	if n+colKeyLen > pkgLen {
		return n, ErrPkgLen
	}
	p.ColKey = pkg[n : n+colKeyLen]
	n += colKeyLen

	return n, nil
}

func (p *PkgKeyValue) Length() int {
	return p.PkgKey.Length() + 4 + len(p.Value)
}

func (p *PkgKeyValue) Encode(ppkg *[]byte) (n int, err error) {
	// PKG = HEAD+cDbId+cTableId+cErrCode+cRowKeyLen+sRowKey+wColKeyLen+sColKey+dwValueLen+sValue
	var pkgLen = p.Length()
	if p.PkgLen < uint32(pkgLen) {
		p.PkgLen = uint32(pkgLen)
	}

	if len(*ppkg) < int(p.PkgLen) {
		*ppkg = make([]byte, int(p.PkgLen))
	}
	var pkg = *ppkg

	n, err = p.PkgKey.Encode(ppkg)
	if err != nil {
		return n, err
	}

	binary.BigEndian.PutUint32(pkg[n:], uint32(len(p.Value)))
	n += 4
	copy(pkg[n:], p.Value)
	n += len(p.Value)

	return n, nil
}

func (p *PkgKeyValue) Decode(pkg []byte) (n int, err error) {
	n, err = p.PkgKey.Decode(pkg)
	if err != nil {
		return n, err
	}

	var pkgLen = len(pkg)

	var valueLen = int(binary.BigEndian.Uint32(pkg[n:]))
	n += 4
	if n+valueLen > pkgLen {
		return n, ErrPkgLen
	}
	p.Value = pkg[n : n+valueLen]
	n += valueLen

	return n, nil
}
