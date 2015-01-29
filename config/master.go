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
	"encoding/json"
	"fmt"
	"github.com/stevejiang/gotable/ctrl"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

const (
	masterConfigFile = "master.conf"
)

type UnitRangeSlice []ctrl.UnitRange

func (p UnitRangeSlice) Len() int      { return len(p) }
func (p UnitRangeSlice) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p UnitRangeSlice) Less(i, j int) bool {
	if p[i].Start < p[j].Start {
		return true
	} else if p[i].Start > p[j].Start {
		return false
	} else {
		return p[i].End < p[j].End
	}
}

type MasterElem struct {
	Id uint16
	ctrl.MasterInfo
}

type MasterEncoding struct {
	LastTime time.Time
	LastId   uint16
	Ms       []MasterElem
}

type MasterConfig struct {
	dir string

	mtx      sync.RWMutex // protects following
	lastTime time.Time
	lastId   uint16
	m1       map[string]uint16
	m2       map[uint16]ctrl.MasterInfo
}

func NewMasterConfig(dir string) *MasterConfig {
	err := os.MkdirAll(dir, os.ModeDir|os.ModePerm)
	if err != nil {
		log.Printf("Invalid master config directory (%s): %s\n", dir, err)
		return nil
	}

	mc := new(MasterConfig)
	mc.dir = dir
	mc.m1 = make(map[string]uint16)
	mc.m2 = make(map[uint16]ctrl.MasterInfo)

	err = mc.load(fmt.Sprintf("%s/%s", mc.dir, masterConfigFile))
	if err != nil {
		log.Printf("Load master config failed: %s\n", err)
		return nil
	}

	return mc
}

func (mc *MasterConfig) load(confFile string) error {
	file, err := os.Open(confFile)
	if err != nil {
		if os.IsNotExist(err) {
			tmpFile := fmt.Sprintf("%s.tmp", confFile)
			file, err = os.Open(tmpFile)
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
			} else {
				os.Rename(tmpFile, confFile)
			}
		}
		return err
	}
	defer file.Close()

	de := json.NewDecoder(file)

	var me MasterEncoding
	err = de.Decode(&me)
	if err != nil {
		return err
	}

	var m1 = make(map[string]uint16)
	var m2 = make(map[uint16]ctrl.MasterInfo)
	for i := 0; i < len(me.Ms); i++ {
		m2[me.Ms[i].Id] = me.Ms[i].MasterInfo
		m1[me.Ms[i].Host] = me.Ms[i].Id
	}

	mc.mtx.Lock()
	mc.lastTime = me.LastTime
	mc.lastId = me.LastId
	mc.m1 = m1
	mc.m2 = m2
	mc.mtx.Unlock()

	return nil
}

func (mc *MasterConfig) save(lastTime time.Time, lastId uint16,
	mmi map[uint16]ctrl.MasterInfo, fileName string) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	var me MasterEncoding
	me.LastTime = lastTime
	me.LastId = lastId
	for k, v := range mmi {
		me.Ms = append(me.Ms, MasterElem{k, v})
	}

	en := json.NewEncoder(file)

	err = en.Encode(&me)
	if err != nil {
		return err
	}

	err = file.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (mc *MasterConfig) check(mis []ctrl.MasterInfo) error {
	if len(mis) > ctrl.TotalUnitNum {
		return fmt.Errorf("number of masters (%d) out of range", len(mis))
	}

	m1 := make(map[string]struct{})
	var urs UnitRangeSlice
	for i := 0; i < len(mis); i++ {
		if _, ok := m1[mis[i].Host]; ok {
			return fmt.Errorf("duplicate master host %s", mis[i].Host)
		}
		m1[mis[i].Host] = struct{}{}
		urs = append(urs, mis[i].Urs...)
	}

	sort.Sort(urs)
	for i := 0; i < len(urs); i++ {
		if urs[i].Start > urs[i].End {
			return fmt.Errorf("invalid unit range [%d, %d]",
				urs[i].Start, urs[i].End)
		}
		if i > 0 && urs[i].Start <= urs[i-1].End {
			return fmt.Errorf("unit range overlap [%d, %d] and [%d, %d]",
				urs[i-1].Start, urs[i-1].End, urs[i].Start, urs[i].End)
		}
	}

	return nil
}

func (mc *MasterConfig) Set(mis []ctrl.MasterInfo) error {
	err := mc.check(mis)
	if err != nil {
		return err
	}

	mc.mtx.Lock()
	var lastId = mc.lastId
	mc.mtx.Unlock()

	m1 := make(map[string]uint16)
	m2 := make(map[uint16]ctrl.MasterInfo)
	for i := 0; i < len(mis); i++ {
		if lastId < 65535 {
			lastId++
		} else {
			lastId = 1
		}

		m1[mis[i].Host] = lastId
		m2[lastId] = mis[i]
	}

	confFile := fmt.Sprintf("%s/%s", mc.dir, masterConfigFile)
	tmpFile := fmt.Sprintf("%s.tmp", confFile)
	lastTime := time.Now()
	err = mc.save(lastTime, lastId, m2, tmpFile)
	if err != nil {
		return err
	}

	mc.mtx.Lock()
	mc.lastTime = lastTime
	mc.lastId = lastId
	mc.m1 = m1
	mc.m2 = m2
	os.Rename(tmpFile, confFile)
	mc.mtx.Unlock()

	return nil
}

func (mc *MasterConfig) GetId(host string) uint16 {
	mc.mtx.RLock()
	id, ok := mc.m1[host]
	mc.mtx.RUnlock()

	if ok {
		return id
	}

	return 0
}

func (mc *MasterConfig) GetInfo(id uint16) ctrl.MasterInfo {
	mc.mtx.RLock()
	mi, ok := mc.m2[id]
	mc.mtx.RUnlock()

	if ok {
		return mi
	}

	return ctrl.MasterInfo{}
}

func (mc *MasterConfig) GetAllMaster() []ctrl.MasterInfo {
	mc.mtx.RLock()
	var m2 = mc.m2
	mc.mtx.RUnlock()

	var mis []ctrl.MasterInfo
	for _, v := range m2 {
		mis = append(mis, v)
	}

	return mis
}
