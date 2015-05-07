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
	"testing"
)

func TestGetRealAddr(t *testing.T) {
	if GetRealAddr(":6688", "127.0.0.1:12345") != "127.0.0.1:6688" {
		t.Fatalf("RealAddr not match")
	}

	if GetRealAddr(":6688", "10.1.0.25:12345") != "10.1.0.25:6688" {
		t.Fatalf("RealAddr not match")
	}

	if GetRealAddr("127.0.0.1:6688", "10.1.0.25:12345") != "10.1.0.25:6688" {
		t.Fatalf("RealAddr not match")
	}

	if GetRealAddr("127.0.0.1:6688", ":12345") != "127.0.0.1:6688" {
		t.Fatalf("RealAddr not match")
	}

	if GetRealAddr("10.1.0.25:6688", ":12345") != "10.1.0.25:6688" {
		t.Fatalf("RealAddr not match")
	}

	if GetRealAddr("10.1.0.25:6688", "10.1.0.33:12345") != "10.1.0.25:6688" {
		t.Fatalf("RealAddr not match")
	}

	if GetRealAddr("", "10.1.0.33:12345") != "" {
		t.Fatalf("RealAddr not match")
	}
}
