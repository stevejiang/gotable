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
