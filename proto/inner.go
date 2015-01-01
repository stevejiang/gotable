package proto

import (
	"encoding/binary"
)

// PKG = HEAD+cDbId+cTableId+cColType+cRowKeyLen+sRowKey+wColKeyLen+sColKey+dwValueLen+sValue
// Inner request: ErrCode = cColType
type PkgCmdSyncReq struct {
	PkgKeyValue
}

func (p *PkgCmdSyncReq) ColSpace() uint8 {
	return p.ErrCode
}

func (p *PkgCmdSyncReq) SetColSpace(val uint8) {
	p.ErrCode = val
}

// PKG = HEAD+cAddrLen+sSlaveAddr+ddwLastSeq+wUnitNum+awUnitId[wUnitNum]
type PkgCmdMasterReq struct {
	PkgHead
	SlaveAddr []byte
	LastSeq   uint64
	UnitIDs   []uint16
}

// PKG = HEAD+cErrCode+wMsgLen+sErrMsg
type PkgCmdMasterResp struct {
	PkgHead
	ErrCode uint8
	ErrMsg  []byte
}

func (p *PkgCmdMasterReq) Length() int {
	// PKG = HEAD+cAddrLen+sSlaveAddr+ddwLastSeq+wUnitNum+awUnitId[wUnitNum]
	return HeadSize + 1 + len(p.SlaveAddr) + 10 + 2*len(p.UnitIDs)
}

func (p *PkgCmdMasterReq) Encode(ppkg *[]byte) (n int, err error) {
	if len(p.SlaveAddr) > MaxUint8 {
		return 0, ErrArrayLen
	}

	if len(p.UnitIDs) > MaxUint16 {
		return 0, ErrArrayLen
	}

	// PKG = HEAD+cAddrLen+sSlaveAddr+ddwLastSeq+wUnitNum+awUnitId[wUnitNum]
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
	pkg[n] = uint8(len(p.SlaveAddr))
	n += 1
	copy(pkg[n:], p.SlaveAddr)
	n += len(p.SlaveAddr)

	binary.BigEndian.PutUint64(pkg[n:], p.LastSeq)
	n += 8

	binary.BigEndian.PutUint16(pkg[n:], uint16(len(p.UnitIDs)))
	n += 2
	for _, id := range p.UnitIDs {
		binary.BigEndian.PutUint16(pkg[n:], id)
		n += 2
	}

	return n, nil
}

func (p *PkgCmdMasterReq) Decode(pkg []byte) (n int, err error) {
	err = p.DecodeHead(pkg)
	if err != nil {
		return 0, err
	}

	var pkgLen = len(pkg)
	if p.PkgLen > uint32(pkgLen) {
		return 0, ErrPkgLen
	}

	// PKG = HEAD+cAddrLen+sSlaveAddr+ddwLastSeq+wUnitNum+awUnitId[wUnitNum]
	n = HeadSize
	var addrLen = int(pkg[n])
	n += 1
	if n+addrLen > pkgLen {
		return n, ErrPkgLen
	}
	p.SlaveAddr = pkg[n : n+addrLen]
	n += addrLen

	p.LastSeq = binary.BigEndian.Uint64(pkg[n:])
	n += 8

	var unitNum = int(binary.BigEndian.Uint16(pkg[n:]))
	n += 2
	if n+unitNum*2 > pkgLen {
		return n, ErrPkgLen
	}
	for i := 0; i < unitNum; i++ {
		p.UnitIDs = append(p.UnitIDs, binary.BigEndian.Uint16(pkg[n:]))
		n += 2
	}

	return n, nil
}

func (p *PkgCmdMasterResp) Length() int {
	// PKG = HEAD+cErrCode+wMsgLen+sErrMsg
	return HeadSize + 3 + len(p.ErrMsg)
}

func (p *PkgCmdMasterResp) Encode(ppkg *[]byte) (n int, err error) {
	if len(p.ErrMsg) > MaxUint16 {
		return 0, ErrArrayLen
	}

	// PKG = HEAD+cErrCode+wMsgLen+sErrMsg
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
	pkg[n] = p.ErrCode
	n += 1

	binary.BigEndian.PutUint16(pkg[n:], uint16(len(p.ErrMsg)))
	n += 2
	copy(pkg[n:], p.ErrMsg)
	n += len(p.ErrMsg)

	return n, nil
}

func (p *PkgCmdMasterResp) Decode(pkg []byte) (n int, err error) {
	err = p.DecodeHead(pkg)
	if err != nil {
		return 0, err
	}

	var pkgLen = len(pkg)
	if p.PkgLen > uint32(pkgLen) {
		return 0, ErrPkgLen
	}

	// PKG = HEAD+cErrCode+wMsgLen+sErrMsg
	n = HeadSize
	p.ErrCode = pkg[n]
	n += 1
	var msgLen = int(pkg[n])
	n += 1
	if n+msgLen > pkgLen {
		return n, ErrPkgLen
	}
	p.ErrMsg = pkg[n : n+msgLen]
	n += msgLen

	return n, nil
}
