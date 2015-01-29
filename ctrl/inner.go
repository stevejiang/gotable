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

// Unit Range [Start, End]
type UnitRange struct {
	Start uint16
	End   uint16
}

// Master command pkg
type PkgMaster struct {
	SlaveAddr string
	LastSeq   uint64
	Urs       []UnitRange

	ErrMsg string // error msg, nil means no error
}

type MasterInfo struct {
	Host string // ip:host
	Urs  []UnitRange
}

// SlaveOf command pkg
type PkgSlaveOf struct {
	Mis []MasterInfo

	ErrMsg string // error msg, nil means no error
}
