package util

type BitMap struct {
	ab []uint8
}

// NewBitMap returns a new BitMap with size in bytes.
func NewBitMap(size uint) *BitMap {
	bm := new(BitMap)
	bm.ab = make([]uint8, size)

	return bm
}

func (bm *BitMap) Get(index uint) bool {
	if len(bm.ab)*8 > int(index) {
		idx := index / 8
		bit := index % 8

		return bm.ab[idx]&(1<<bit) != 0
	}

	return false
}

func (bm *BitMap) Set(index uint) {
	if len(bm.ab)*8 > int(index) {
		idx := index / 8
		bit := index % 8

		bm.ab[idx] |= (1 << bit)
	}
}

func (bm *BitMap) Clear(index uint) {
	if len(bm.ab)*8 > int(index) {
		idx := index / 8
		bit := index % 8

		bm.ab[idx] &^= (1 << bit)
	}
}
