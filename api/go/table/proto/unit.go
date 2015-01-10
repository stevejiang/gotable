package proto

import (
	"hash/crc32"
)

const (
	TotalUnitNum = 8192
)

func GetUnitId(dbId, tableId uint8, rowKey []byte) uint16 {
	var a = crc32.Update(0, crc32.IEEETable, []byte{dbId, tableId})
	return uint16(crc32.Update(a, crc32.IEEETable, rowKey) % TotalUnitNum)
}
