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
	"sync"
	"time"
)

const (
	masterConfigFile = "master.conf"
)

type MasterInfo struct {
	MasterAddr string // Master address ip:host
	SlaveAddr  string // This server address ip:host
	Migration  bool   // true: Migration; false: Normal master/slave
	SlotId     uint16 // Only meaningful for migration
	Status     int    // Status of Slave/Migration
}

type MasterEncoding struct {
	HasMaster bool // true: Has master; false: No master/No migration
	MasterInfo
	LastTime time.Time // Last change time
}

type MasterConfig struct {
	dir string

	mtx sync.RWMutex // protects following
	m   MasterEncoding
}

func NewMasterConfig(dir string) *MasterConfig {
	err := os.MkdirAll(dir, os.ModeDir|os.ModePerm)
	if err != nil {
		log.Printf("Invalid master config directory (%s): %s\n", dir, err)
		return nil
	}

	mc := new(MasterConfig)
	mc.dir = dir

	err = mc.load(fmt.Sprintf("%s/%s", mc.dir, masterConfigFile))
	if err != nil {
		log.Printf("Load master config failed: %s\n", err)
		return nil
	}

	mc.mtx.Lock()
	if mc.m.HasMaster {
		if mc.m.Migration {
			// Need to clear old data first
			if mc.m.Status != ctrl.SlaveClear {
				mc.m.Status = ctrl.SlaveNeedClear
			}
		} else {
			if mc.m.Status != ctrl.SlaveNeedClear &&
				mc.m.Status != ctrl.SlaveClear {
				mc.m.Status = ctrl.SlaveInit
			}
		}
	}
	mc.mtx.Unlock()

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

	de := json.NewDecoder(file)

	var m MasterEncoding
	err = de.Decode(&m)
	if err != nil {
		file.Close()
		return err
	}
	file.Close()

	// Migration shouldn't be interrupted.
	// If server restarted, remove migration config.
	// Administrator should send migrate command again.
	if m.HasMaster && m.Migration {
		m.HasMaster = false
		mc.save(&m)
	}

	mc.mtx.Lock()
	mc.m = m
	mc.mtx.Unlock()

	return nil
}

func (mc *MasterConfig) save(m *MasterEncoding) error {
	confFile := fmt.Sprintf("%s/%s", mc.dir, masterConfigFile)
	tmpFile := fmt.Sprintf("%s.tmp", confFile)
	file, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	defer file.Close()

	en := json.NewEncoder(file)

	err = en.Encode(m)
	if err != nil {
		return err
	}

	err = file.Sync()
	if err != nil {
		return err
	}

	mc.mtx.Lock()
	mc.m = *m
	os.Rename(tmpFile, confFile)
	mc.mtx.Unlock()

	return nil
}

func (mc *MasterConfig) SetMaster(masterAddr, slaveAddr string) error {
	mc.mtx.RLock()
	var m = mc.m
	mc.mtx.RUnlock()

	if m.HasMaster && m.Migration {
		return fmt.Errorf("server is under migration and cannot change master/slave")
	}

	if len(masterAddr) > 0 {
		m.HasMaster = true
		m.LastTime = time.Now()
		m.MasterAddr = masterAddr
		m.SlaveAddr = slaveAddr
		m.Migration = false
		m.SlotId = ctrl.TotalSlotNum // Exceed
		m.Status = ctrl.SlaveInit
	} else {
		m.HasMaster = false
		m.LastTime = time.Now()
		m.Status = ctrl.NotSlave
	}

	return mc.save(&m)
}

func (mc *MasterConfig) SetMigration(masterAddr, slaveAddr string, slotId uint16) error {
	mc.mtx.RLock()
	var m = mc.m
	mc.mtx.RUnlock()

	if m.HasMaster {
		if !m.Migration {
			return fmt.Errorf("server is a slave and cannot start/stop migration")
		} else if m.Status == ctrl.SlaveClear {
			return fmt.Errorf("cannot update migration config when clearing data")
		}
	}

	if len(masterAddr) > 0 {
		if m.HasMaster && m.Migration && m.SlotId != slotId {
			return fmt.Errorf("cannot start more than 1 migration")
		}
		if slotId >= ctrl.TotalSlotNum {
			return fmt.Errorf("migrate slot id out of range")
		}

		m.HasMaster = true
		m.LastTime = time.Now()
		m.MasterAddr = masterAddr
		m.SlaveAddr = slaveAddr
		m.Migration = true
		m.SlotId = slotId
		m.Status = ctrl.SlaveInit
	} else {
		m.HasMaster = false
		m.LastTime = time.Now()
		m.Status = ctrl.NotSlave
	}

	return mc.save(&m)
}

func (mc *MasterConfig) SetStatus(status int) error {
	var changed bool
	mc.mtx.Lock()
	if mc.m.HasMaster {
		if status == ctrl.NotSlave {
			mc.m.HasMaster = false
			changed = true
		}
		if mc.m.Status != status {
			mc.m.Status = status
			changed = true
		}
	}
	var m = mc.m
	mc.mtx.Unlock()

	if changed {
		return mc.save(&m)
	} else {
		return nil
	}
}

func (mc *MasterConfig) Status() int {
	var st int = ctrl.NotSlave
	mc.mtx.RLock()
	if mc.m.HasMaster {
		st = mc.m.Status
	}
	mc.mtx.RUnlock()

	return st
}

func (mc *MasterConfig) GetMaster() MasterInfo {
	var m MasterInfo
	mc.mtx.RLock()
	if mc.m.HasMaster {
		m = mc.m.MasterInfo
	}
	mc.mtx.RUnlock()

	if len(m.MasterAddr) == 0 {
		m.Status = ctrl.NotSlave
	}

	return m
}

func (mc *MasterConfig) GetMasterSlot() (bool, bool, uint16) {
	var hasMaster, migration bool
	var slotId uint16
	mc.mtx.RLock()
	if mc.m.HasMaster {
		hasMaster = true
		migration = mc.m.Migration
		slotId = mc.m.SlotId
	}
	mc.mtx.RUnlock()

	return hasMaster, migration, slotId
}
