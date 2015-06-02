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
	"fmt"
	"github.com/stevejiang/gotable/api/go/table"
	"github.com/stevejiang/gotable/api/go/table/proto"
	"github.com/stevejiang/gotable/config"
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
		testTbl = NewTable(tblDir, 1024, 1024*1024, 1024*1024, "snappy")
	}

	testTblOnce.Do(f)
	return testTbl
}

func getTestWA() *WriteAccess {
	return NewWriteAccess(false, &config.MasterConfig{})
}

func myGet(in proto.PkgOneOp, au Authorize, wa *WriteAccess,
	t *testing.T) proto.PkgOneOp {
	var pkg = make([]byte, in.Length())
	_, err := in.Encode(pkg)
	if err != nil {
		t.Fatalf("Encode failed: ", err)
	}

	pkg = testTbl.Get(&PkgArgs{in.Cmd, in.DbId, in.Seq, pkg}, au, wa)

	var out proto.PkgOneOp
	_, err = out.Decode(pkg)
	if err != nil {
		t.Fatalf("Decode failed: ", err)
	}

	if out.ErrCode < 0 {
		t.Fatalf("Failed with ErrCode %d", out.ErrCode)
	}
	if out.DbId != in.DbId || out.Seq != in.Seq || out.TableId != in.TableId {
		t.Fatalf("DbId/Seq/TableId mismatch")
	}
	if bytes.Compare(out.RowKey, in.RowKey) != 0 ||
		bytes.Compare(out.ColKey, in.ColKey) != 0 {
		t.Fatalf("RowKey/ColKey mismatch")
	}
	if in.Cas == 0 && out.Cas != 0 {
		t.Fatalf("Invalid default Cas value")
	}

	return out
}

func mySet(in proto.PkgOneOp, au Authorize, wa *WriteAccess, expected bool,
	t *testing.T) proto.PkgOneOp {
	var pkg = make([]byte, in.Length())
	_, err := in.Encode(pkg)
	if err != nil {
		t.Fatalf("Encode failed: ", err)
	}

	pkg, ok := testTbl.Set(&PkgArgs{in.Cmd, in.DbId, in.Seq, pkg}, au, wa)
	if ok != expected {
		if expected {
			t.Fatalf("Set failed")
		} else {
			t.Fatalf("Set should fail")
		}
	}

	var out proto.PkgOneOp
	_, err = out.Decode(pkg)
	if err != nil {
		t.Fatalf("Decode failed: ", err)
	}

	if expected {
		if out.ErrCode != 0 {
			t.Fatalf("Failed with ErrCode %d", out.ErrCode)
		}
	}
	if len(out.Value) != 0 || out.Score != 0 || out.Cas != 0 {
		t.Fatalf("Invalid default values")
	}
	if out.Seq != in.Seq || out.DbId != in.DbId || out.TableId != in.TableId {
		t.Fatalf("Seq/DbId/TableId mismatch")
	}
	if bytes.Compare(out.RowKey, in.RowKey) != 0 ||
		bytes.Compare(out.ColKey, in.ColKey) != 0 {
		t.Fatalf("RowKey/ColKey mismatch")
	}

	return out
}

func myDel(in proto.PkgOneOp, au Authorize, wa *WriteAccess, expected bool,
	t *testing.T) proto.PkgOneOp {
	var pkg = make([]byte, in.Length())
	_, err := in.Encode(pkg)
	if err != nil {
		t.Fatalf("Encode failed: ", err)
	}

	pkg, ok := testTbl.Del(&PkgArgs{in.Cmd, in.DbId, in.Seq, pkg}, au, wa)
	if ok != expected {
		if expected {
			t.Fatalf("Del failed")
		} else {
			t.Fatalf("Del should fail")
		}
	}

	var out proto.PkgOneOp
	_, err = out.Decode(pkg)
	if err != nil {
		t.Fatalf("Decode failed: ", err)
	}

	if expected {
		if out.ErrCode != 0 {
			t.Fatalf("Failed with ErrCode %d", out.ErrCode)
		}
	}
	if len(out.Value) != 0 || out.Score != 0 || out.Cas != 0 {
		t.Fatalf("Invalid default values")
	}
	if out.Seq != in.Seq || out.DbId != in.DbId || out.TableId != in.TableId {
		t.Fatalf("Seq/DbId/TableId mismatch")
	}
	if bytes.Compare(out.RowKey, in.RowKey) != 0 ||
		bytes.Compare(out.ColKey, in.ColKey) != 0 {
		t.Fatalf("RowKey/ColKey mismatch")
	}

	return out
}

func myIncr(in proto.PkgOneOp, au Authorize, wa *WriteAccess, expected bool,
	t *testing.T) proto.PkgOneOp {
	var pkg = make([]byte, in.Length())
	_, err := in.Encode(pkg)
	if err != nil {
		t.Fatalf("Encode failed: ", err)
	}

	pkg, ok := testTbl.Incr(&PkgArgs{in.Cmd, in.DbId, in.Seq, pkg}, au, wa)
	if ok != expected {
		if expected {
			t.Fatalf("Incr failed")
		} else {
			t.Fatalf("Incr should fail")
		}
	}

	var out proto.PkgOneOp
	_, err = out.Decode(pkg)
	if err != nil {
		t.Fatalf("Decode failed: ", err)
	}

	if expected {
		if out.ErrCode != 0 {
			t.Fatalf("Failed with ErrCode %d", out.ErrCode)
		}
	}
	if out.Cas != 0 {
		t.Fatalf("Invalid default values")
	}
	if out.Seq != in.Seq || out.DbId != in.DbId || out.TableId != in.TableId {
		t.Fatalf("Seq/DbId/TableId mismatch")
	}
	if bytes.Compare(out.RowKey, in.RowKey) != 0 ||
		bytes.Compare(out.ColKey, in.ColKey) != 0 {
		t.Fatalf("RowKey/ColKey mismatch")
	}

	return out
}

func myMGet(in proto.PkgMultiOp, au Authorize, wa *WriteAccess,
	t *testing.T) proto.PkgMultiOp {
	var pkg = make([]byte, in.Length())
	_, err := in.Encode(pkg)
	if err != nil {
		t.Fatalf("Encode failed: ", err)
	}

	pkg = testTbl.MGet(&PkgArgs{in.Cmd, in.DbId, in.Seq, pkg}, au, wa)

	var out proto.PkgMultiOp
	_, err = out.Decode(pkg)
	if err != nil {
		t.Fatalf("Decode failed: ", err)
	}

	if out.ErrCode != 0 {
		t.Fatalf("Failed with ErrCode %d", out.ErrCode)
	}
	if out.DbId != in.DbId || out.Seq != in.Seq {
		t.Fatalf("DbId/Seq mismatch")
	}

	return out
}

func myMSet(in proto.PkgMultiOp, au Authorize, wa *WriteAccess, expected bool,
	t *testing.T) proto.PkgMultiOp {
	var pkg = make([]byte, in.Length())
	_, err := in.Encode(pkg)
	if err != nil {
		t.Fatalf("Encode failed: ", err)
	}

	pkg, ok := testTbl.MSet(&PkgArgs{in.Cmd, in.DbId, in.Seq, pkg}, au, wa)
	if ok != expected {
		if expected {
			t.Fatalf("MSet failed")
		} else {
			t.Fatalf("MSet should fail")
		}
	}

	var out proto.PkgMultiOp
	_, err = out.Decode(pkg)
	if err != nil {
		t.Fatalf("Decode failed: ", err)
	}

	if expected {
		if out.ErrCode != 0 {
			t.Fatalf("Failed with ErrCode %d", out.ErrCode)
		}
	}
	if out.DbId != in.DbId || out.Seq != in.Seq {
		t.Fatalf("DbId/Seq mismatch")
	}

	return out
}

func myMDel(in proto.PkgMultiOp, au Authorize, wa *WriteAccess, expected bool,
	t *testing.T) proto.PkgMultiOp {
	var pkg = make([]byte, in.Length())
	_, err := in.Encode(pkg)
	if err != nil {
		t.Fatalf("Encode failed: ", err)
	}

	pkg, ok := testTbl.MDel(&PkgArgs{in.Cmd, in.DbId, in.Seq, pkg}, au, wa)
	if ok != expected {
		if expected {
			t.Fatalf("MDel failed")
		} else {
			t.Fatalf("MDel should fail")
		}
	}

	var out proto.PkgMultiOp
	_, err = out.Decode(pkg)
	if err != nil {
		t.Fatalf("Decode failed: ", err)
	}

	if expected {
		if out.ErrCode != 0 {
			t.Fatalf("Failed with ErrCode %d", out.ErrCode)
		}
	}
	if out.DbId != in.DbId || out.Seq != in.Seq {
		t.Fatalf("DbId/Seq mismatch")
	}

	return out
}

func myMIncr(in proto.PkgMultiOp, au Authorize, wa *WriteAccess, expected bool,
	t *testing.T) proto.PkgMultiOp {
	var pkg = make([]byte, in.Length())
	_, err := in.Encode(pkg)
	if err != nil {
		t.Fatalf("Encode failed: ", err)
	}

	pkg, ok := testTbl.MIncr(&PkgArgs{in.Cmd, in.DbId, in.Seq, pkg}, au, wa)
	if ok != expected {
		if expected {
			t.Fatalf("MIncr failed")
		} else {
			t.Fatalf("MIncr should fail")
		}
	}

	var out proto.PkgMultiOp
	_, err = out.Decode(pkg)
	if err != nil {
		t.Fatalf("Decode failed: ", err)
	}

	if expected {
		if out.ErrCode != 0 {
			t.Fatalf("Failed with ErrCode %d", out.ErrCode)
		}
	}
	if out.DbId != in.DbId || out.Seq != in.Seq {
		t.Fatalf("DbId/Seq mismatch")
	}

	return out
}

func myScan(in proto.PkgScanReq, au Authorize, t *testing.T) proto.PkgScanResp {
	var pkg = make([]byte, in.Length())
	_, err := in.Encode(pkg)
	if err != nil {
		t.Fatalf("Encode failed: ", err)
	}

	pkg = testTbl.Scan(&PkgArgs{in.Cmd, in.DbId, in.Seq, pkg}, au)

	var out proto.PkgScanResp
	_, err = out.Decode(pkg)
	if err != nil {
		t.Fatalf("Decode failed: ", err)
	}

	if out.ErrCode != 0 {
		t.Fatalf("Failed with ErrCode %d", out.ErrCode)
	}
	if out.DbId != in.DbId || out.Seq != in.Seq {
		t.Fatalf("DbId/Seq mismatch")
	}

	return out
}

func myDump(in proto.PkgDumpReq, au Authorize, t *testing.T) proto.PkgDumpResp {
	var pkg = make([]byte, in.Length())
	_, err := in.Encode(pkg)
	if err != nil {
		t.Fatalf("Encode failed: ", err)
	}

	pkg = testTbl.Dump(&PkgArgs{in.Cmd, in.DbId, in.Seq, pkg}, au)

	var out proto.PkgDumpResp
	_, err = out.Decode(pkg)
	if err != nil {
		t.Fatalf("Decode failed: ", err)
	}

	if out.ErrCode != 0 {
		t.Fatalf("Failed with ErrCode %d", out.ErrCode)
	}
	if out.DbId != in.DbId || out.Seq != in.Seq {
		t.Fatalf("DbId/Seq mismatch")
	}

	return out
}

func getTestKV(tableId uint8, rowKey, colKey, value []byte, score int64, cas uint32) proto.KeyValue {
	var kv proto.KeyValue
	kv.TableId = tableId
	kv.RowKey = rowKey
	kv.ColKey = colKey
	kv.SetValue(value)
	kv.SetScore(score)
	kv.SetCas(cas)
	return kv
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
	in.KeyValue = getTestKV(2, []byte("row1"), []byte("col1"), []byte("v1"), 30, 0)

	mySet(in, testAuth, getTestWA(), true, t)
}

func TestTableGet(t *testing.T) {
	var in proto.PkgOneOp
	in.Cmd = proto.CmdGet
	in.DbId = 1
	in.Seq = 10
	in.KeyValue = getTestKV(2, []byte("row1"), []byte("col1"), nil, 0, 0)

	out := myGet(in, testAuth, getTestWA(), t)
	if bytes.Compare(out.Value, []byte("v1")) != 0 {
		t.Fatalf("Value mismatch: %q", out.Value)
	}
	if out.Score != 30 {
		t.Fatalf("Score mismatch")
	}
}

func TestTableSetCas(t *testing.T) {
	var in proto.PkgOneOp
	in.Cmd = proto.CmdSet
	in.DbId = 1
	in.Seq = 10
	in.KeyValue = getTestKV(2, []byte("row1"), []byte("col1"), []byte("v1"), 30, 600)

	out := mySet(in, testAuth, getTestWA(), false, t)
	if out.ErrCode != table.EcCasNotMatch {
		t.Fatalf("Should fail with EcCasNotMatch")
	}
}

func TestTableGetCas(t *testing.T) {
	var in proto.PkgOneOp
	in.Cmd = proto.CmdGet
	in.DbId = 1
	in.Seq = 10
	in.KeyValue = getTestKV(2, []byte("row1"), []byte("col1"), []byte("v1"), 0, 2)

	out := myGet(in, testAuth, getTestWA(), t)
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
	in.SetValue(append(out.Value, []byte("-cas")...))
	in.SetScore(32)
	in.SetCas(out.Cas)

	mySet(in, testAuth, getTestWA(), true, t)

	// Set again should fail
	mySet(in, testAuth, getTestWA(), false, t)

	// Get
	in.Cmd = proto.CmdGet
	in.Cas = 0
	in.CtrlFlag &^= 0xFF

	out = myGet(in, testAuth, getTestWA(), t)
	if bytes.Compare(out.Value, []byte("v1-cas")) != 0 {
		t.Fatalf("Value mismatch: %q", out.Value)
	}
	if out.Score != 32 {
		t.Fatalf("Score mismatch")
	}
}

func TestTableDel(t *testing.T) {
	var in proto.PkgOneOp
	in.Cmd = proto.CmdSet
	in.DbId = 1
	in.Seq = 10
	in.KeyValue = getTestKV(2, []byte("row1"), []byte("col1"), nil, 0, 0)

	myDel(in, testAuth, getTestWA(), true, t)
}

func TestTableIncr(t *testing.T) {
	var in proto.PkgOneOp
	in.Cmd = proto.CmdGet
	in.DbId = 1
	in.Seq = 10
	in.KeyValue = getTestKV(2, []byte("row1"), []byte("col1"), nil, -21, 0)

	out := myIncr(in, testAuth, getTestWA(), true, t)
	if len(out.Value) != 0 {
		t.Fatalf("Value mismatch: %q", out.Value)
	}
	if out.Score != -21 {
		t.Fatalf("Score mismatch")
	}
}

func TestTableZopSetGet(t *testing.T) {
	var in proto.PkgOneOp
	in.PkgFlag |= proto.FlagZop
	in.Cmd = proto.CmdSet
	in.DbId = 1
	in.Seq = 10
	in.KeyValue = getTestKV(2, []byte("row1"), []byte("col1"), []byte("v1"), 30, 0)

	// ZSET
	mySet(in, testAuth, getTestWA(), true, t)

	// ZGET
	in.Cmd = proto.CmdGet
	out := myGet(in, testAuth, getTestWA(), t)

	if bytes.Compare(out.Value, []byte("v1")) != 0 {
		t.Fatalf("Value mismatch: %q", out.Value)
	}
	if out.Score != 30 {
		t.Fatalf("Score mismatch")
	}
}

func TestTableMSet(t *testing.T) {
	var in proto.PkgMultiOp
	in.Cmd = proto.CmdMSet
	in.DbId = 2
	in.Seq = 20
	in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col0"), []byte("v0"), 10, 0))
	in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col1"), []byte("v1"), 20, 0))
	in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col2"), []byte("v2"), 30, 0))

	out := myMSet(in, testAuth, getTestWA(), true, t)

	if len(out.Kvs) != 3 {
		t.Fatalf("Invalid KV number: %d", len(out.Kvs))
	}

	for i := 0; i < len(out.Kvs); i++ {
		if out.Kvs[i].ErrCode != 0 {
			t.Fatalf("ErrCode is 0")
		}
		if out.Kvs[i].TableId != 2 {
			t.Fatalf("TableId mismatch")
		}
		if bytes.Compare(out.Kvs[i].RowKey, []byte("row1")) != 0 {
			t.Fatalf("RowKey mismatch")
		}
		if bytes.Compare(out.Kvs[i].ColKey, []byte(fmt.Sprintf("col%d", i))) != 0 {
			t.Fatalf("ColKey mismatch")
		}
	}
}

func TestTableMGet(t *testing.T) {
	var in proto.PkgMultiOp
	in.Cmd = proto.CmdMSet
	in.DbId = 2
	in.Seq = 20
	in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col0"), nil, 0, 0))
	in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col1"), nil, 0, 0))
	in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col2"), nil, 0, 0))

	out := myMGet(in, testAuth, getTestWA(), t)

	if len(out.Kvs) != 3 {
		t.Fatalf("Invalid KV number: %d", len(out.Kvs))
	}

	for i := 0; i < len(out.Kvs); i++ {
		if out.Kvs[i].ErrCode != 0 {
			t.Fatalf("ErrCode is 0")
		}
		if out.Kvs[i].TableId != 2 {
			t.Fatalf("TableId mismatch")
		}
		if bytes.Compare(out.Kvs[i].RowKey, []byte("row1")) != 0 {
			t.Fatalf("RowKey mismatch")
		}
		if bytes.Compare(out.Kvs[i].ColKey, []byte(fmt.Sprintf("col%d", i))) != 0 {
			t.Fatalf("ColKey mismatch")
		}
		if out.Kvs[i].Score != int64(i*10+10) {
			t.Fatalf("Score mismatch")
		}
		if bytes.Compare(out.Kvs[i].Value, []byte(fmt.Sprintf("v%d", i))) != 0 {
			t.Fatalf("Value mismatch")
		}
	}
}

func TestTableMIncr(t *testing.T) {
	var in proto.PkgMultiOp
	in.Cmd = proto.CmdMSet
	in.DbId = 2
	in.Seq = 20
	in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col0"), nil, 1, 0))
	in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col1"), nil, 2, 0))
	in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col2"), nil, 3, 0))

	out := myMIncr(in, testAuth, getTestWA(), true, t)

	if len(out.Kvs) != 3 {
		t.Fatalf("Invalid KV number: %d", len(out.Kvs))
	}

	for i := 0; i < len(out.Kvs); i++ {
		if out.Kvs[i].ErrCode != 0 {
			t.Fatalf("ErrCode is 0")
		}
		if out.Kvs[i].TableId != 2 {
			t.Fatalf("TableId mismatch")
		}
		if bytes.Compare(out.Kvs[i].RowKey, []byte("row1")) != 0 {
			t.Fatalf("RowKey mismatch")
		}
		if bytes.Compare(out.Kvs[i].ColKey, []byte(fmt.Sprintf("col%d", i))) != 0 {
			t.Fatalf("ColKey mismatch")
		}
		if out.Kvs[i].Score != int64(i*10+10+i+1) {
			t.Fatalf("Score mismatch")
		}
		if bytes.Compare(out.Kvs[i].Value, []byte(fmt.Sprintf("v%d", i))) != 0 {
			t.Fatalf("Value mismatch")
		}
	}
}

func TestTableMDel(t *testing.T) {
	var in proto.PkgMultiOp
	in.Cmd = proto.CmdMSet
	in.DbId = 2
	in.Seq = 20
	in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col0"), nil, 0, 0))
	in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col1"), nil, 0, 0))
	in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col2"), nil, 0, 0))

	out := myMDel(in, testAuth, getTestWA(), true, t)

	if len(out.Kvs) != 3 {
		t.Fatalf("Invalid KV number: %d", len(out.Kvs))
	}

	for i := 0; i < len(out.Kvs); i++ {
		if out.Kvs[i].ErrCode != 0 {
			t.Fatalf("ErrCode is 0")
		}
		if out.Kvs[i].TableId != 2 {
			t.Fatalf("TableId mismatch")
		}
		if bytes.Compare(out.Kvs[i].RowKey, []byte("row1")) != 0 {
			t.Fatalf("RowKey mismatch")
		}
		if bytes.Compare(out.Kvs[i].ColKey, []byte(fmt.Sprintf("col%d", i))) != 0 {
			t.Fatalf("ColKey mismatch")
		}
	}
}

func TestTableMGetAfterDel(t *testing.T) {
	var in proto.PkgMultiOp
	in.Cmd = proto.CmdMSet
	in.DbId = 2
	in.Seq = 20
	in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col0"), nil, 0, 0))
	in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col1"), nil, 0, 0))
	in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col2"), nil, 0, 0))

	out := myMGet(in, testAuth, getTestWA(), t)

	if len(out.Kvs) != 3 {
		t.Fatalf("Invalid KV number: %d", len(out.Kvs))
	}

	for i := 0; i < len(out.Kvs); i++ {
		if out.Kvs[i].ErrCode != table.EcNotExist {
			t.Fatalf("ErrCode is not EcNotExist")
		}
		if out.Kvs[i].TableId != 2 {
			t.Fatalf("TableId mismatch")
		}
		if bytes.Compare(out.Kvs[i].RowKey, []byte("row1")) != 0 {
			t.Fatalf("RowKey mismatch")
		}
		if bytes.Compare(out.Kvs[i].ColKey, []byte(fmt.Sprintf("col%d", i))) != 0 {
			t.Fatalf("ColKey mismatch")
		}
		if out.Kvs[i].Score != 0 {
			t.Fatalf("Score should be 0")
		}
		if out.Kvs[i].Value != nil {
			t.Fatalf("Value should be nil")
		}
	}
}

func TestTableScan(t *testing.T) {
	// MSET
	{
		var in proto.PkgMultiOp
		in.Cmd = proto.CmdMSet
		in.DbId = 2
		in.Seq = 20
		in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col0"), []byte("v0"), 10, 0))
		in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col1"), []byte("v1"), 20, 0))
		in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col2"), []byte("v2"), 30, 0))
		in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col3"), []byte("v3"), 40, 0))
		in.Kvs = append(in.Kvs, getTestKV(2, []byte("row1"), []byte("col4"), []byte("v4"), 50, 0))

		myMSet(in, testAuth, getTestWA(), true, t)
	}

	// SCAN ASC
	var in proto.PkgScanReq
	in.Cmd = proto.CmdScan
	in.DbId = 2
	in.Seq = 20
	in.Num = 10
	in.TableId = 2
	in.RowKey = []byte("row1")
	in.ColKey = []byte("")
	in.PkgFlag |= proto.FlagScanAsc

	out := myScan(in, testAuth, t)
	if len(out.Kvs) != 5 {
		t.Fatalf("Invalid KV number: %d", len(out.Kvs))
	}
	if out.PkgFlag&proto.FlagScanEnd == 0 {
		t.Fatalf("Scan should end")
	}
	for i := 0; i < len(out.Kvs); i++ {
		if out.Kvs[i].ErrCode != 0 {
			t.Fatalf("ErrCode is 0")
		}
		if out.Kvs[i].TableId != 2 {
			t.Fatalf("TableId mismatch")
		}
		if bytes.Compare(out.Kvs[i].RowKey, []byte("row1")) != 0 {
			t.Fatalf("RowKey mismatch")
		}
		if bytes.Compare(out.Kvs[i].ColKey, []byte(fmt.Sprintf("col%d", i))) != 0 {
			t.Fatalf("ColKey mismatch")
		}
		if out.Kvs[i].Score != int64(i*10+10) {
			t.Fatalf("Score mismatch")
		}
		if bytes.Compare(out.Kvs[i].Value, []byte(fmt.Sprintf("v%d", i))) != 0 {
			t.Fatalf("Value mismatch")
		}
	}

	// SCAN DESC
	in.PkgFlag &^= 0xFF
	in.PkgFlag |= proto.FlagScanKeyStart

	out = myScan(in, testAuth, t)
	if len(out.Kvs) != 5 {
		t.Fatalf("Invalid KV number: %d", len(out.Kvs))
	}
	if out.PkgFlag&proto.FlagScanEnd == 0 {
		t.Fatalf("Scan should end")
	}
	for i := 0; i < len(out.Kvs); i++ {
		idx := 4 - i
		if out.Kvs[i].ErrCode != 0 {
			t.Fatalf("ErrCode is 0")
		}
		if out.Kvs[i].TableId != 2 {
			t.Fatalf("TableId mismatch")
		}
		if bytes.Compare(out.Kvs[i].RowKey, []byte("row1")) != 0 {
			t.Fatalf("RowKey mismatch")
		}
		if bytes.Compare(out.Kvs[i].ColKey, []byte(fmt.Sprintf("col%d", idx))) != 0 {
			t.Fatalf("ColKey mismatch")
		}
		if out.Kvs[i].Score != int64(idx*10+10) {
			t.Fatalf("Score mismatch")
		}
		if bytes.Compare(out.Kvs[i].Value, []byte(fmt.Sprintf("v%d", idx))) != 0 {
			t.Fatalf("Value mismatch")
		}
	}
}

func TestTableZScan(t *testing.T) {
	// MZSET
	{
		var in proto.PkgMultiOp
		in.Cmd = proto.CmdMSet
		in.DbId = 2
		in.Seq = 20
		in.PkgFlag |= proto.FlagZop
		in.Kvs = append(in.Kvs, getTestKV(2, []byte("row2"), []byte("col0"), []byte("v0"), 40, 0))
		in.Kvs = append(in.Kvs, getTestKV(2, []byte("row2"), []byte("col1"), []byte("v1"), 30, 0))
		in.Kvs = append(in.Kvs, getTestKV(2, []byte("row2"), []byte("col2"), []byte("v2"), 20, 0))
		in.Kvs = append(in.Kvs, getTestKV(2, []byte("row2"), []byte("col3"), []byte("v3"), 10, 0))
		in.Kvs = append(in.Kvs, getTestKV(2, []byte("row2"), []byte("col4"), []byte("v4"), 0, 0))

		myMSet(in, testAuth, getTestWA(), true, t)
	}

	// ZSCAN ASC order by SCORE
	var in proto.PkgScanReq
	in.Cmd = proto.CmdScan
	in.DbId = 2
	in.Seq = 20
	in.Num = 10
	in.TableId = 2
	in.RowKey = []byte("row2")
	in.SetScore(-1)
	in.ColKey = []byte("")
	in.PkgFlag |= proto.FlagScanAsc
	in.SetColSpace(proto.ColSpaceScore1) // order by SCORE

	out := myScan(in, testAuth, t)
	if len(out.Kvs) != 5 {
		t.Fatalf("Invalid KV number: %d", len(out.Kvs))
	}
	if out.PkgFlag&proto.FlagScanEnd == 0 {
		t.Fatalf("Scan should end")
	}
	for i := 0; i < len(out.Kvs); i++ {
		idx := 4 - i
		if out.Kvs[i].ErrCode != 0 {
			t.Fatalf("ErrCode is 0")
		}
		if out.Kvs[i].TableId != 2 {
			t.Fatalf("TableId mismatch")
		}
		if bytes.Compare(out.Kvs[i].RowKey, []byte("row2")) != 0 {
			t.Fatalf("RowKey mismatch")
		}
		if bytes.Compare(out.Kvs[i].ColKey, []byte(fmt.Sprintf("col%d", idx))) != 0 {
			t.Fatalf("ColKey mismatch")
		}
		if out.Kvs[i].Score != int64(i*10) {
			t.Fatalf("Score mismatch")
		}
		if bytes.Compare(out.Kvs[i].Value, []byte(fmt.Sprintf("v%d", idx))) != 0 {
			t.Fatalf("Value mismatch")
		}
	}

	// ZSCAN DESC order by SCORE
	in.PkgFlag &^= 0xFF
	in.PkgFlag |= proto.FlagScanKeyStart

	out = myScan(in, testAuth, t)
	if len(out.Kvs) != 5 {
		t.Fatalf("Invalid KV number: %d", len(out.Kvs))
	}
	if out.PkgFlag&proto.FlagScanEnd == 0 {
		t.Fatalf("Scan should end")
	}
	for i := 0; i < len(out.Kvs); i++ {
		idx := 4 - i
		if out.Kvs[i].ErrCode != 0 {
			t.Fatalf("ErrCode is 0")
		}
		if out.Kvs[i].TableId != 2 {
			t.Fatalf("TableId mismatch")
		}
		if bytes.Compare(out.Kvs[i].RowKey, []byte("row2")) != 0 {
			t.Fatalf("RowKey mismatch")
		}
		if bytes.Compare(out.Kvs[i].ColKey, []byte(fmt.Sprintf("col%d", i))) != 0 {
			t.Fatalf("ColKey mismatch")
		}
		if out.Kvs[i].Score != int64(idx*10) {
			t.Fatalf("Score mismatch")
		}
		if bytes.Compare(out.Kvs[i].Value, []byte(fmt.Sprintf("v%d", i))) != 0 {
			t.Fatalf("Value mismatch")
		}
	}
}

func TestTableDump(t *testing.T) {
	// Dump
	var in proto.PkgDumpReq
	in.Cmd = proto.CmdDump
	in.DbId = 2
	in.Seq = 20
	in.TableId = 2
	in.PkgFlag |= proto.FlagDumpSlotStart
	in.StartSlotId = 0
	in.EndSlotId = 65535

	out := myDump(in, testAuth, t)
	if len(out.Kvs) != 10 {
		t.Fatalf("Invalid KV number: %d", len(out.Kvs))
	}
	if out.PkgFlag&proto.FlagDumpEnd == 0 {
		t.Fatalf("Dump should end")
	}
	for i := 0; i < len(out.Kvs); i++ {
		j := i
		idx := 4 - i
		if i >= 5 {
			j = i - 5
			idx = 9 - i
		}

		if out.Kvs[i].ErrCode != 0 {
			t.Fatalf("ErrCode is 0")
		}
		if out.Kvs[i].TableId != 2 {
			t.Fatalf("TableId mismatch")
		}

		if bytes.Compare(out.Kvs[i].RowKey, []byte("row2")) == 0 {
			if bytes.Compare(out.Kvs[i].ColKey, []byte(fmt.Sprintf("col%d", idx))) != 0 {
				t.Fatalf("ColKey mismatch")
			}
			if out.Kvs[i].Score != int64(j*10) {
				t.Fatalf("Score mismatch")
			}
			if bytes.Compare(out.Kvs[i].Value, []byte(fmt.Sprintf("v%d", idx))) != 0 {
				t.Fatalf("Value mismatch")
			}
		} else {
			if bytes.Compare(out.Kvs[i].RowKey, []byte("row1")) != 0 {
				t.Fatalf("RowKey mismatch")
			}
			if bytes.Compare(out.Kvs[i].ColKey, []byte(fmt.Sprintf("col%d", j))) != 0 {
				t.Fatalf("ColKey mismatch")
			}
			if out.Kvs[i].Score != int64(j*10+10) {
				t.Fatalf("Score mismatch")
			}
			if bytes.Compare(out.Kvs[i].Value, []byte(fmt.Sprintf("v%d", j))) != 0 {
				t.Fatalf("Value mismatch")
			}
		}
	}
}
