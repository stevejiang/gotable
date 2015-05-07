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

import (
	"strings"
)

func GetRealAddr(writtenAddr, netAddr string) string {
	var nas = strings.Split(netAddr, ":")
	if len(nas) != 2 || len(nas[0]) == 0 {
		return writtenAddr
	}
	if strings.Index(writtenAddr, "127.0.0.1:") == 0 {
		var was = strings.Split(writtenAddr, ":")
		return nas[0] + ":" + was[1]
	}

	if strings.Index(writtenAddr, ":") == 0 {
		return nas[0] + writtenAddr
	}

	return writtenAddr
}
