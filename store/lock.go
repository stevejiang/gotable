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

package store

import (
	"hash/crc32"
	"sync"
	"time"
)

const (
	dbLockUnitNum = 1024
	rollInterval  = time.Second * 10 // 10 seconds
	minCasValue   = uint32(6600)
	maxCasValue   = uint32(0x80000000)
)

var castagnoliTab = crc32.MakeTable(crc32.Castagnoli)

type UnitLock struct {
	sync.Mutex
	curCas uint32
	curIdx int
	cas    []map[string]uint32 // rawKey => cas
}

type TableLock struct {
	ul []UnitLock
}

func NewTableLock() *TableLock {
	var tl = new(TableLock)
	tl.ul = make([]UnitLock, dbLockUnitNum)

	go tl.GoRollDeamon()
	return tl
}

func (tl *TableLock) GetLock(key []byte) *UnitLock {
	var idx = crc32.Checksum(key, castagnoliTab) % dbLockUnitNum
	return &tl.ul[idx]
}

func (tl *TableLock) GoRollDeamon() {
	var tick = time.Tick(rollInterval)
	for {
		select {
		case <-tick:
			for i := 0; i < len(tl.ul); i++ {
				tl.ul[i].roll()
			}
		}
	}
}

func (u *UnitLock) NewCas(key []byte) uint32 {
	var strKey = string(key)
	if u.cas == nil {
		u.curCas = minCasValue
		u.cas = make([]map[string]uint32, 2)
	}
	if u.cas[u.curIdx] == nil {
		u.cas[u.curIdx] = make(map[string]uint32)
	}

	if rc, ok := u.cas[u.curIdx][strKey]; ok {
		return rc
	}

	var idxBak = (u.curIdx + 1) % 2
	if u.cas[idxBak] != nil {
		if rc, ok := u.cas[idxBak][strKey]; ok {
			return rc
		}
	}

	u.curCas++
	if u.curCas > maxCasValue {
		u.curCas = 1
	}

	u.cas[u.curIdx][strKey] = u.curCas

	return u.curCas
}

func (u *UnitLock) GetCas(key []byte) uint32 {
	var strKey = string(key)
	if u.cas == nil {
		return 0
	}
	if u.cas[u.curIdx] == nil {
		return 0
	}

	if rc, ok := u.cas[u.curIdx][strKey]; ok {
		return rc
	}

	var idxBak = (u.curIdx + 1) % 2
	if u.cas[idxBak] != nil {
		if rc, ok := u.cas[idxBak][strKey]; ok {
			return rc
		}
	}

	return 0
}

func (u *UnitLock) ClearCas(key []byte) {
	if u.cas != nil {
		var strKey = string(key)
		delete(u.cas[u.curIdx], strKey)
		var idxBak = (u.curIdx + 1) % 2
		delete(u.cas[idxBak], strKey)
	}
}

func (u *UnitLock) roll() {
	u.Lock()
	defer u.Unlock()

	if u.cas != nil {
		var idxBak = (u.curIdx + 1) % 2
		u.cas[idxBak] = nil
		u.curIdx = idxBak
	}
}
