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

package ctrl

// Slave/Migration status
const (
	NotSlave       = iota // Not a normal slave (also not a migration slave)
	SlaveInit             // Just receive slave or migration command
	SlaveNeedClear        // Slave has old data, need to clear old data
	SlaveClear            // Slave clearing(deleting) old data
	SlaveFullSync         // Doing full sync right now
	SlaveIncrSync         // Doing incremental sync right now
	SlaveReady            // Slave is up to date with master
)

// SlaveOf command pkg
type PkgSlaveOf struct {
	ClientReq  bool   // true: from client api; false: from slave to master
	MasterAddr string // ip:host, no master if emtpy
	SlaveAddr  string // ip:host
	LastSeq    uint64
	ErrMsg     string // error msg, nil means no error
}

// Migrate command pkg.
// When stop migration:
// If all data migrated, switch client requests to new servers.
// If migration failed, wait for delete slot command.
// Steps in cluster mode when migration succeeds:
// 1. new servers switch to normal status (but still hold the master/slave connection)
// 2. switch proxy and client requests routed to new servers
// 3. wait for 10 seconds, and close the master/slave connection
// 4. delete the slot data from old servers
type PkgMigrate struct {
	ClientReq  bool   // true: from client api; false: from slave to master
	MasterAddr string // ip:host, stop migration if empty
	SlaveAddr  string // ip:host
	SlotId     uint16 // The slot to be migrated
	ErrMsg     string // error msg, nil means no error
}

// Get migration/slave status
type PkgSlaveStatus struct {
	Migration bool   // true: Migration status; false: Normal slave status
	SlotId    uint16 // The slot under migration
	Status    int
	ErrMsg    string // error msg, nil means no error
}

// Delete slot data
type PkgDelSlot struct {
	SlotId uint16 // The slot to delete
	ErrMsg string // error msg, nil means no error
}
