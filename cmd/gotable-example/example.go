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

package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/stevejiang/gotable/api/go/table"
	"time"
)

var (
	host = flag.String("h", "127.0.0.1:6688", "Server host address ip:port")
)

func main() {
	flag.Parse()

	client, err := table.Dial("tcp", *host)
	if err != nil {
		fmt.Printf("Dial failed: %s\n", err)
		return
	}
	defer client.Close()

	ctx := client.NewContext(0)

	testGet(ctx)
	testMGet(ctx)
	testScan(ctx)
	testZScan(ctx)
	testCas(ctx)
	testBinary(ctx)
	testPing(ctx)
	testDump(ctx)
}

func testGet(tc *table.Context) {
	// set
	_, err := tc.Set(1, []byte("row1"), []byte("col1"), []byte("v01"), 10, 0)
	if err != nil {
		fmt.Printf("Set failed: %s\n", err)
		return
	}

	r, err := tc.Get(1, []byte("row1"), []byte("col1"), 0)
	if err != nil {
		fmt.Printf("Get failed: %s\n", err)
		return
	}
	if r.ErrCode != 0 {
		fmt.Printf("Get failed with unexpected error code: %d\n", r.ErrCode)
		return
	}

	if r.Value == nil {
		fmt.Printf("GET result1: Key not exist\n")
	} else {
		fmt.Printf("GET result1: %q\t%d\n", r.Value, r.Score)
	}

	// delete
	_, err = tc.Del(1, []byte("row1"), []byte("col1"), 0)
	if err != nil {
		fmt.Printf("Del failed: %s\n", err)
		return
	}

	r, err = tc.Get(1, []byte("row1"), []byte("col1"), 0)
	if err != nil {
		fmt.Printf("Get failed: %s\n", err)
		return
	}
	if r.ErrCode != 0 {
		fmt.Printf("Get failed with unexpected error code: %d\n", r.ErrCode)
		return
	}

	if r.Value == nil {
		fmt.Printf("GET result2: Key not exist\n")
	} else {
		fmt.Printf("GET result2: %q\t%d\n", r.Value, r.Score)
	}
}

func testMGet(tc *table.Context) {
	var ma table.MultiArgs
	ma.AddSetArgs(1, []byte("row1"), []byte("col0"), []byte("v00"), 10, 0)
	ma.AddSetArgs(1, []byte("row1"), []byte("col1"), []byte("v01"), 9, 0)
	ma.AddSetArgs(1, []byte("row1"), []byte("col2"), []byte("v02"), 8, 0)
	ma.AddSetArgs(1, []byte("row1"), []byte("col3"), []byte("v03"), 7, 0)
	ma.AddSetArgs(1, []byte("row1"), []byte("col4"), []byte("v04"), 6, 0)
	_, err := tc.MSet(&ma)
	if err != nil {
		fmt.Printf("Mset failed: %s\n", err)
		return
	}

	ma = table.MultiArgs{}
	ma.AddGetArgs(1, []byte("row1"), []byte("col4"), 0)
	ma.AddGetArgs(1, []byte("row1"), []byte("col2"), 0)
	ma.AddGetArgs(1, []byte("row1"), []byte("col1"), 0)
	ma.AddGetArgs(1, []byte("row1"), []byte("col3"), 0)
	r, err := tc.MGet(&ma)
	if err != nil {
		fmt.Printf("Mget failed: %s\n", err)
		return
	}

	fmt.Println("MGET result:")
	for i := 0; i < len(r.Reply); i++ {
		fmt.Printf("[%q\t%q]\t[%d\t%q]\n",
			r.Reply[i].RowKey, r.Reply[i].ColKey,
			r.Reply[i].Score, r.Reply[i].Value)
	}
}

func testScan(tc *table.Context) {
	r, err := tc.Scan(1, []byte("row1"), []byte("col0"), true, 10)
	if err != nil {
		fmt.Printf("Scan failed: %s\n", err)
		return
	}

	fmt.Println("SCAN result:")
	for i := 0; i < len(r.Reply); i++ {
		fmt.Printf("[%q\t%q]\t[%d\t%q]\n",
			r.Reply[i].RowKey, r.Reply[i].ColKey,
			r.Reply[i].Score, r.Reply[i].Value)
	}
	if r.End {
		fmt.Println("SCAN finished!")
	} else {
		fmt.Println("SCAN has more records!")
	}
}

func testZScan(tc *table.Context) {
	_, err := tc.ZSet(1, []byte("row2"), []byte("000"), []byte("v00"), 10, 0)
	if err != nil {
		fmt.Printf("Set failed: %s\n", err)
	}

	var ma table.MultiArgs
	ma.AddSetArgs(1, []byte("row2"), []byte("001"), []byte("v01"), 9, 0)
	ma.AddSetArgs(1, []byte("row2"), []byte("002"), []byte("v02"), 6, 0)
	ma.AddSetArgs(1, []byte("row2"), []byte("003"), []byte("v03"), 7, 0)
	ma.AddSetArgs(1, []byte("row2"), []byte("004"), []byte("v04"), 8, 0)
	ma.AddSetArgs(1, []byte("row2"), []byte("005"), []byte("v05"), -5, 0)
	_, err = tc.ZmSet(&ma)
	if err != nil {
		fmt.Printf("Mset failed: %s\n", err)
		return
	}

	//r, err := tc.ZScan(1, []byte("row2"), nil, 0, true, true, 4)
	r, err := tc.ZScanStart(1, []byte("row2"), true, true, 4)
	if err != nil {
		fmt.Printf("ZScan failed: %s\n", err)
		return
	}

	fmt.Println("ZSCAN result:")
	for {
		for i := 0; i < len(r.Reply); i++ {
			fmt.Printf("[%q\t%q]\t[%d\t%q]\n",
				r.Reply[i].RowKey, r.Reply[i].ColKey,
				r.Reply[i].Score, r.Reply[i].Value)
		}

		if r.End {
			fmt.Println("ZSCAN finished!")
			break
		} else {
			fmt.Println("ZSCAN has more records:")
		}

		r, err = tc.ScanMore(r)
		if err != nil {
			fmt.Printf("ScanMore failed: %s\n", err)
			return
		}
	}
}

func testCas(tc *table.Context) {
	var cas uint32
	for i := 0; i < 1; i++ {
		r, err := tc.Get(1, []byte("row1"), []byte("col1"), 1)
		if err != nil {
			fmt.Printf("Get failed: %s\n", err)
			return
		}

		if i > 0 {
			time.Sleep(time.Second)
		} else {
			cas = r.Cas
		}

		fmt.Printf("  Cas: %d\t(%02d, %d)\n", r.Cas, i, cas)
	}

	_, err := tc.Set(1, []byte("row1"), []byte("col1"), []byte("v20"), 20, cas)
	if err != nil {
		fmt.Printf("Set failed: %s\n", err)
	}

	r, err := tc.Get(1, []byte("row1"), []byte("col1"), 0)
	if err != nil {
		fmt.Printf("Get failed: %s\n", err)
		return
	}

	fmt.Printf("CAS result: %q\t%d\n", r.Value, r.Score)
}

func testBinary(tc *table.Context) {
	var colKey = make([]byte, 4)
	var value = make([]byte, 8)
	binary.BigEndian.PutUint32(colKey, 998365)
	binary.BigEndian.PutUint64(value, 6000000000)
	_, err := tc.Set(1, []byte("row1"), colKey, value, 30, 0)
	if err != nil {
		fmt.Printf("Set failed: %s\n", err)
	}

	r, err := tc.Get(1, []byte("row1"), colKey, 0)
	if err != nil {
		fmt.Printf("Get failed: %s\n", err)
		return
	}

	fmt.Printf("Binary result: %q\t%d\t%d\n",
		r.Value, binary.BigEndian.Uint64(r.Value), r.Score)
}

func testPing(tc *table.Context) {
	start := time.Now()
	err := tc.Ping()
	if err != nil {
		fmt.Printf("Ping failed: %s\n", err)
		return
	}

	elapsed := time.Since(start)

	fmt.Printf("Ping succeed: %.2f ms\n", float64(elapsed)/1e6)
}

func testDump(tc *table.Context) {
	//r, err := tc.DumpDB()
	r, err := tc.DumpTable(1)
	if err != nil {
		fmt.Printf("Dump failed: %s\n", err)
		return
	}

	fmt.Println("Dump result:")
	var idx = 0
	for {
		for _, rw := range r.Reply {
			fmt.Printf("%02d) %d\t%q\t%d\t%q\t%d\t%q\n", idx,
				rw.TableId, rw.RowKey, rw.ColSpace, rw.ColKey, rw.Score, rw.Value)
			idx++
		}

		if r.End {
			break
		}

		r, err = tc.DumpMore(r)
		if err != nil {
			fmt.Printf("DumpMore failed: %s\n", err)
			return
		}
	}
}
