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

	var tc, err = table.Dial("tcp", *host)
	if err != nil {
		fmt.Printf("Dial fialed: %s", err)
		return
	}
	defer tc.Close()

	testGet(tc)
	testMGet(tc)
	testScan(tc)
	testZScan(tc)
	testCas(tc)
	testBinary(tc)

	time.Sleep(time.Millisecond)
}

func testGet(tc *table.Client) {
	// set
	_, err := tc.Set(&table.OneArgs{1, []byte("row1"), []byte("col1"), []byte("v01"), 10, 0})
	if err != nil {
		fmt.Printf("Set fialed: %s\n", err)
		return
	}

	r, err := tc.Get(&table.OneArgs{1, []byte("row1"), []byte("col1"), nil, 0, 0})
	if err != nil {
		fmt.Printf("Get fialed: %s\n", err)
		return
	}

	if r.ErrCode == table.EcodeNotExist {
		fmt.Printf("GET result1: Key not exist\n")
	} else {
		fmt.Printf("GET result1: %q\t%d\n", r.Value, r.Score)
	}

	// delete
	_, err = tc.Del(&table.OneArgs{1, []byte("row1"), []byte("col1"), nil, 10, 0})
	if err != nil {
		fmt.Printf("Del fialed: %s\n", err)
		return
	}

	r, err = tc.Get(&table.OneArgs{1, []byte("row1"), []byte("col1"), nil, 0, 0})
	if err != nil {
		fmt.Printf("Get fialed: %s\n", err)
		return
	}

	if r.ErrCode == table.EcodeNotExist {
		fmt.Printf("GET result2: Key not exist\n")
	} else {
		fmt.Printf("GET result2: %q\t%d\n", r.Value, r.Score)
	}
}

func testMGet(tc *table.Client) {
	var ma table.MultiArgs
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("col0"), []byte("v00"), 10, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("col1"), []byte("v01"), 9, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("col2"), []byte("v02"), 8, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("col3"), []byte("v03"), 7, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("col4"), []byte("v04"), 6, 0})
	_, err := tc.MSet(&ma)
	if err != nil {
		fmt.Printf("Mset fialed: %s\n", err)
		return
	}

	ma = table.MultiArgs{}
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("col1"), nil, 0, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("col2"), nil, 0, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("col3"), nil, 0, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("col4"), nil, 0, 0})
	r, err := tc.MGet(&ma)
	if err != nil {
		fmt.Printf("Mget fialed: %s\n", err)
		return
	}

	fmt.Println("MGET result:")
	for i := 0; i < len(r.Reply); i++ {
		fmt.Printf("[%q\t%q]\t[%d\t%q]\n",
			r.Reply[i].RowKey, r.Reply[i].ColKey,
			r.Reply[i].Score, r.Reply[i].Value)
	}
}

func testScan(tc *table.Client) {
	var sa = table.ScanArgs{10, 0, table.OneArgs{1, []byte("row1"), []byte("col0"), nil, 0, 0}}
	r, err := tc.Scan(&sa)
	if err != nil {
		fmt.Printf("Scan fialed: %s\n", err)
		return
	}

	fmt.Println("SCAN result:")
	for i := 0; i < len(r.Reply); i++ {
		fmt.Printf("[%q\t%q]\t[%d\t%q]\n",
			r.Reply[i].RowKey, r.Reply[i].ColKey,
			r.Reply[i].Score, r.Reply[i].Value)
	}
}

func testZScan(tc *table.Client) {
	_, err := tc.ZSet(&table.OneArgs{1, []byte("row1"), []byte("000"), []byte("v00"), 10, 0})
	if err != nil {
		fmt.Printf("Set fialed: %s\n", err)
	}

	var ma table.MultiArgs
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("001"), []byte("v01"), 9, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("002"), []byte("v02"), 8, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("003"), []byte("v03"), 7, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("004"), []byte("v04"), 6, 0})
	_, err = tc.ZmSet(&ma)
	if err != nil {
		fmt.Printf("Mset fialed: %s\n", err)
		return
	}

	var sa = table.ScanArgs{10, 0, table.OneArgs{1, []byte("row1"), []byte("000"), nil, 6, 0}}
	r, err := tc.ZScan(&sa)
	if err != nil {
		fmt.Printf("Scan fialed: %s\n", err)
		return
	}

	fmt.Println("ZSCAN result:")
	for i := 0; i < len(r.Reply); i++ {
		fmt.Printf("[%q\t%q]\t[%d\t%q]\n",
			r.Reply[i].RowKey, r.Reply[i].ColKey,
			r.Reply[i].Score, r.Reply[i].Value)
	}
}

func testCas(tc *table.Client) {
	var cas uint32
	for i := 0; i < 1; i++ {
		r, err := tc.Get(&table.OneArgs{1, []byte("row1"), []byte("col1"), nil, 0, 1})
		if err != nil {
			fmt.Printf("Get fialed: %s\n", err)
			return
		}

		if i > 0 {
			time.Sleep(time.Second)
		} else {
			cas = r.Cas
		}

		fmt.Printf("  Cas: %d\t(%02d, %d)\n", r.Cas, i, cas)
	}

	_, err := tc.Set(&table.OneArgs{1, []byte("row1"), []byte("col1"), []byte("v20"), 20, cas})
	if err != nil {
		fmt.Printf("Set fialed: %s\n", err)
	}

	r, err := tc.Get(&table.OneArgs{1, []byte("row1"), []byte("col1"), nil, 0, 0})
	if err != nil {
		fmt.Printf("Get fialed: %s\n", err)
		return
	}

	fmt.Printf("CAS result: %q\t%d\n", r.Value, r.Score)
}

func testBinary(tc *table.Client) {
	var colKey = make([]byte, 4)
	var value = make([]byte, 8)
	binary.BigEndian.PutUint32(colKey, 998365)
	binary.BigEndian.PutUint64(value, 6000000000)
	_, err := tc.Set(&table.OneArgs{1, []byte("row1"), colKey, value, 30, 0})
	if err != nil {
		fmt.Printf("Set fialed: %s\n", err)
	}

	r, err := tc.Get(&table.OneArgs{1, []byte("row1"), colKey, nil, 0, 0})
	if err != nil {
		fmt.Printf("Get fialed: %s\n", err)
		return
	}

	fmt.Printf("Binary result: %q\t%d\t%d\n",
		r.Value, binary.BigEndian.Uint64(r.Value), r.Score)
}
