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

func TestBitMap(t *testing.T) {
	var b = NewBitMap(256 / 8)

	if b.Get(1) {
		t.Fatalf("index 1 should be false")
	}

	b.Set(1)
	if !b.Get(1) {
		t.Fatalf("index 1 should be true")
	}

	b.Clear(1)
	if b.Get(1) {
		t.Fatalf("index 1 should be false")
	}

	if b.Get(255) {
		t.Fatalf("index 255 should be false")
	}
	if b.Get(256) {
		t.Fatalf("index 256 should be false")
	}
	if b.Get(260) {
		t.Fatalf("index 260 should be false")
	}

	b.Set(255)
	b.Set(256)
	b.Set(260)
	if !b.Get(255) {
		t.Fatalf("index 255 should be true")
	}
	if b.Get(256) {
		t.Fatalf("index 256 should be false")
	}
	if b.Get(260) {
		t.Fatalf("index 260 should be false")
	}

	b.Clear(255)
	b.Clear(256)
	b.Clear(260)
	if b.Get(255) {
		t.Fatalf("index 255 should be false")
	}
	if b.Get(256) {
		t.Fatalf("index 256 should be false")
	}
	if b.Get(260) {
		t.Fatalf("index 260 should be false")
	}
}
