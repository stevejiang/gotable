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
	"bytes"
	"github.com/stevejiang/gotable/api/go/table"
	"github.com/stevejiang/gotable/api/go/table/proto"
	"os"
	"sync"
	"testing"
)

var testTbl *Table
var testTblOnce sync.Once
var testAuth MockAuth

type MockAuth struct {
}

func (ma MockAuth) IsAuth(dbId uint8) bool {
	return true
}

func (ma MockAuth) SetAuth(dbId uint8) {

}

func getTestTable() *Table {
	f := func() {
		tblDir := "/tmp/test_gotable/table"
		os.RemoveAll(tblDir)
		testTbl = NewTable(tblDir)
	}

	testTblOnce.Do(f)
	return testTbl
}

func TestTable(t *testing.T) {
	if getTestTable() == nil {
		t.Fatalf("Failed to getTestTable")
	}
}

func TestTableSet(t *testing.T) {
	var in proto.PkgOneOp
	in.Cmd = proto.CmdSet
	in.DbId = 1
	in.Seq = 10
	in.TableId = 2
	in.RowKey = []byte("row1")
	in.ColKey = []byte("col1")
	in.Value = []byte("v1")
	in.Score = 30
	in.CtrlFlag |= (proto.CtrlValue | proto.CtrlScore)

	var pkg = make([]byte, in.Length())
	_, err := in.Encode(pkg)
	if err != nil {
		t.Fatalf("Encode failed: ", err)
	}

	pkg, ok := testTbl.Set(&PkgArgs{in.Cmd, in.DbId, in.Seq, pkg}, testAuth, false)
	if !ok {
		t.Fatalf("Table Set failed")
	}

	var out proto.PkgOneOp
	_, err = out.Decode(pkg)
	if err != nil {
		t.Fatalf("Decode failed: ", err)
	}

	if out.ErrCode != 0 {
		t.Fatalf("Failed with ErrCode %d", out.ErrCode)
	}
	if out.Value != nil || out.Score != 0 || out.Cas != 0 {
		t.Fatalf("Invalid default values")
	}
	if out.Seq != in.Seq || out.DbId != in.DbId || out.TableId != in.TableId {
		t.Fatalf("Seq/DbId/TableId mismatch")
	}
	if bytes.Compare(out.RowKey, in.RowKey) != 0 ||
		bytes.Compare(out.ColKey, in.ColKey) != 0 {
		t.Fatalf("RowKey/ColKey mismatch")
	}
}

func TestTableGet(t *testing.T) {
	var in proto.PkgOneOp
	in.Cmd = proto.CmdGet
	in.DbId = 1
	in.TableId = 2
	in.RowKey = []byte("row1")
	in.ColKey = []byte("col1")

	var pkg = make([]byte, in.Length())
	_, err := in.Encode(pkg)
	if err != nil {
		t.Fatalf("Encode failed: ", err)
	}

	pkg = testTbl.Get(&PkgArgs{in.Cmd, in.DbId, in.Seq, pkg}, testAuth)

	var out proto.PkgOneOp
	_, err = out.Decode(pkg)
	if err != nil {
		t.Fatalf("Decode failed: ", err)
	}

	if out.ErrCode != 0 {
		t.Fatalf("Failed with ErrCode %d", out.ErrCode)
	}
	if out.DbId != in.DbId || out.TableId != in.TableId {
		t.Fatalf("DbId/TableId mismatch")
	}
	if bytes.Compare(out.RowKey, in.RowKey) != 0 ||
		bytes.Compare(out.ColKey, in.ColKey) != 0 {
		t.Fatalf("RowKey/ColKey mismatch")
	}
	if bytes.Compare(out.Value, []byte("v1")) != 0 {
		t.Fatalf("Value mismatch: %q", out.Value)
	}
	if out.Score != 30 {
		t.Fatalf("Score mismatch")
	}
	if out.Cas != 0 {
		t.Fatalf("Invalid default values")
	}
}

func TestTableSetCas(t *testing.T) {
	var in proto.PkgOneOp
	in.Cmd = proto.CmdSet
	in.DbId = 1
	in.TableId = 2
	in.RowKey = []byte("row1")
	in.ColKey = []byte("col1")
	in.Value = []byte("v1")
	in.Score = 30
	in.Cas = 600
	in.CtrlFlag |= (proto.CtrlValue | proto.CtrlScore | proto.CtrlCas)

	var pkg = make([]byte, in.Length())
	_, err := in.Encode(pkg)
	if err != nil {
		t.Fatalf("Encode failed: ", err)
	}

	pkg, ok := testTbl.Set(&PkgArgs{in.Cmd, in.DbId, in.Seq, pkg}, testAuth, false)
	if ok {
		t.Fatalf("Table Set should fail")
	}

	var out proto.PkgOneOp
	_, err = out.Decode(pkg)
	if err != nil {
		t.Fatalf("Decode failed: ", err)
	}

	if out.ErrCode != table.EcCasNotMatch {
		t.Fatalf("Should fail with EcCasNotMatch")
	}
	if out.Value != nil || out.Score != 0 || out.Cas != 0 {
		t.Fatalf("Invalid default values")
	}
	if out.DbId != in.DbId || out.TableId != in.TableId {
		t.Fatalf("DbId/TableId mismatch")
	}
	if bytes.Compare(out.RowKey, in.RowKey) != 0 ||
		bytes.Compare(out.ColKey, in.ColKey) != 0 {
		t.Fatalf("RowKey/ColKey mismatch")
	}
}

func TestTableGetCas(t *testing.T) {
	var in proto.PkgOneOp
	in.Cmd = proto.CmdGet
	in.DbId = 1
	in.TableId = 2
	in.RowKey = []byte("row1")
	in.ColKey = []byte("col1")
	in.Cas = 2
	in.CtrlFlag |= proto.CtrlCas

	var pkg = make([]byte, in.Length())
	_, err := in.Encode(pkg)
	if err != nil {
		t.Fatalf("Encode failed: ", err)
	}

	pkg = testTbl.Get(&PkgArgs{in.Cmd, in.DbId, in.Seq, pkg}, testAuth)

	var out proto.PkgOneOp
	_, err = out.Decode(pkg)
	if err != nil {
		t.Fatalf("Decode failed: ", err)
	}

	if out.ErrCode != 0 {
		t.Fatalf("Failed with ErrCode %d", out.ErrCode)
	}
	if bytes.Compare(out.Value, []byte("v1")) != 0 {
		t.Fatalf("Value mismatch: %q", out.Value)
	}
	if out.Score != 30 {
		t.Fatalf("Score mismatch")
	}
	if out.Cas == 0 {
		t.Fatalf("Should return new cas")
	}

	// Set
	in.Cmd = proto.CmdSet
	in.Value = append(out.Value, []byte("-cas")...)
	in.Score = 32
	in.Cas = out.Cas
	in.CtrlFlag |= (proto.CtrlValue | proto.CtrlScore | proto.CtrlCas)

	var pkg1 = make([]byte, in.Length())
	_, err = in.Encode(pkg1)
	if err != nil {
		t.Fatalf("Encode failed: ", err)
	}

	var ok bool
	pkg, ok = testTbl.Set(&PkgArgs{in.Cmd, in.DbId, in.Seq, pkg1}, testAuth, false)
	if !ok {
		t.Fatalf("Table Set failed")
	}

	out = proto.PkgOneOp{}
	_, err = out.Decode(pkg)
	if err != nil {
		t.Fatalf("Decode failed: ", err)
	}

	if out.ErrCode != 0 {
		t.Fatalf("Failed with ErrCode %d", out.ErrCode)
	}

	// Set again
	pkg, ok = testTbl.Set(&PkgArgs{in.Cmd, in.DbId, in.Seq, pkg1}, testAuth, false)
	if ok {
		t.Fatalf("Table Set should fail since cas is cleared")
	}

	// Get
	in.Cmd = proto.CmdGet
	in.Cas = 0
	in.CtrlFlag &^= 0xFF

	pkg = make([]byte, in.Length())
	_, err = in.Encode(pkg)
	if err != nil {
		t.Fatalf("Encode failed: ", err)
	}

	pkg = testTbl.Get(&PkgArgs{in.Cmd, in.DbId, in.Seq, pkg}, testAuth)

	out = proto.PkgOneOp{}
	_, err = out.Decode(pkg)
	if err != nil {
		t.Fatalf("Decode failed: ", err)
	}

	if out.ErrCode != 0 {
		t.Fatalf("Failed with ErrCode %d", out.ErrCode)
	}
	if bytes.Compare(out.Value, []byte("v1-cas")) != 0 {
		t.Fatalf("Value mismatch: %q", out.Value)
	}
	if out.Score != 32 {
		t.Fatalf("Score mismatch")
	}
	if out.Cas != 0 {
		t.Fatalf("Invalid default values")
	}
}
