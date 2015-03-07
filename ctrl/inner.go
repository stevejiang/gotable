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

// Slaver/Migration status
const (
	NotSlaver       = iota // Not a normal slaver (also not a migration slaver)
	SlaverInit             // Just receive slaver or migration command
	SlaverNeedClear        // Slaver has old data, need to clear old data
	SlaverClear            // Slaver clearing(deleting) old data
	SlaverFullSync         // Doing full sync right now
	SlaverIncrSync         // Doing incremental sync right now
	SlaverReady            // Slaver is up to date with master
)

// SlaveOf command pkg
type PkgSlaveOf struct {
	ClientReq  bool   // true: from client api; false: from slaver to master
	MasterAddr string // ip:host, no master if emtpy
	SlaverAddr string // ip:host
	LastSeq    uint64
	ErrMsg     string // error msg, nil means no error
}

// Migrate command pkg.
// When stop migration:
// If all data migrated, switch client requests to new servers.
// If migration failed, wait for delete unit command.
// Steps in cluster mode when migration succeeds:
// 1. new servers switch to normal status (but still hold the master/slaver connection)
// 2. switch proxy and client requests routed to new servers
// 3. wait for 10 seconds, and close the master/slaver connection
// 4. delete the unit data from old servers
type PkgMigrate struct {
	ClientReq  bool   // true: from client api; false: from slaver to master
	MasterAddr string // ip:host, stop migration if empty
	SlaverAddr string // ip:host
	UnitId     uint16 // The unit to be migrated
	ErrMsg     string // error msg, nil means no error
}

// Get migration/slaver status
type PkgSlaverStatus struct {
	Migration bool   // true: Migration status; false: Normal slaver status
	UnitId    uint16 // The unit under migration
	Status    int
	ErrMsg    string // error msg, nil means no error
}

// Delete unit data
type PkgDelUnit struct {
	UnitId uint16 // The unit to delete
	ErrMsg string // error msg, nil means no error
}
