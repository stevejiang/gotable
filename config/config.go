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

package config

import (
	"sync"
)

type MasterList struct {
	mtx sync.Mutex
	m1  map[string]uint16
	m2  map[uint16]string
}

func (m *MasterList) Add(mAddr string) uint16 {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if id, ok := m.m1[mAddr]; ok {
		return id
	}

	var i uint16
	for i = 1; i <= 65535; i++ {
		if mAddr, ok := m.m2[i]; !ok {
			m.m1[mAddr] = i
			m.m2[i] = mAddr
			return i
		}
	}

	return 0
}

func (m *MasterList) Get(mAddr string) uint16 {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if id, ok := m.m1[mAddr]; ok {
		return id
	}

	return 0
}

func (m *MasterList) GetId(id uint16) string {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if mAddr, ok := m.m2[id]; ok {
		return mAddr
	}

	return ""
}

func (m *MasterList) Del(mAddr string) uint16 {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if id, ok := m.m1[mAddr]; ok {
		delete(m.m1, mAddr)
		delete(m.m2, id)
		return id
	}

	return 0
}

func (m *MasterList) DelId(id uint16) string {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if mAddr, ok := m.m2[id]; ok {
		delete(m.m1, mAddr)
		delete(m.m2, id)
		return mAddr
	}

	return ""
}

func (m *MasterList) Save() error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	return nil
}
