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

	tc := client.NewContext(0)

	testGet(tc)
	testMGet(tc)
	testScan(tc)
	testZScan(tc)
	testCas(tc)
	testBinary(tc)
	testPing(tc)
	testDump(tc)

}

func testGet(tc *table.Context) {
	// set
	err := tc.Set(1, []byte("row1"), []byte("col1"), []byte("v01"), 10, 0)
	if err != nil {
		fmt.Printf("Set failed: %s\n", err)
		return
	}

	value, score, _, err := tc.Get(1, []byte("row1"), []byte("col1"), 0)
	if err != nil {
		fmt.Printf("Get failed: %s\n", err)
		return
	}

	if value == nil {
		fmt.Printf("GET result1: Key not exist\n")
	} else {
		fmt.Printf("GET result1: %q\t%d\n", value, score)
	}

	// delete
	err = tc.Del(1, []byte("row1"), []byte("col1"), 0)
	if err != nil {
		fmt.Printf("Del failed: %s\n", err)
		return
	}

	value, score, _, err = tc.Get(1, []byte("row1"), []byte("col1"), 0)
	if err != nil {
		fmt.Printf("Get failed: %s\n", err)
		return
	}

	if value == nil {
		fmt.Printf("GET result2: Key not exist\n")
	} else {
		fmt.Printf("GET result2: %q\t%d\n", value, score)
	}
}

func testMGet(tc *table.Context) {
	var ma table.MSetArgs
	ma.Add(1, []byte("row1"), []byte("col0"), []byte("v00"), 10, 0)
	ma.Add(1, []byte("row1"), []byte("col1"), []byte("v01"), 9, 0)
	ma.Add(1, []byte("row1"), []byte("col2"), []byte("v02"), 8, 0)
	ma.Add(1, []byte("row1"), []byte("col3"), []byte("v03"), 7, 0)
	ma.Add(1, []byte("row1"), []byte("col4"), []byte("v04"), 6, 0)
	_, err := tc.MSet(ma)
	if err != nil {
		fmt.Printf("Mset failed: %s\n", err)
		return
	}

	var mb table.MGetArgs
	mb.Add(1, []byte("row1"), []byte("col4"), 0)
	mb.Add(1, []byte("row1"), []byte("col2"), 0)
	mb.Add(1, []byte("row1"), []byte("col1"), 0)
	mb.Add(1, []byte("row1"), []byte("col3"), 0)
	mb.Add(1, []byte("row1"), []byte("not"), 0)
	r, err := tc.MGet(mb)
	if err != nil {
		fmt.Printf("Mget failed: %s\n", err)
		return
	}

	fmt.Println("MGET result:")
	for i := 0; i < len(r); i++ {
		if r[i].ErrCode < 0 {
			fmt.Printf("[%q\t%q]\tget failed with error %d!\n",
				r[i].RowKey, r[i].ColKey, r[i].ErrCode)
		} else if r[i].ErrCode > 0 {
			fmt.Printf("[%q\t%q]\tkey not exist!\n",
				r[i].RowKey, r[i].ColKey)
		} else {
			fmt.Printf("[%q\t%q]\t[%d\t%q]\n",
				r[i].RowKey, r[i].ColKey,
				r[i].Score, r[i].Value)
		}
	}
}

func testScan(tc *table.Context) {
	r, err := tc.Scan(1, []byte("row1"), []byte("col0"), true, 10)
	if err != nil {
		fmt.Printf("Scan failed: %s\n", err)
		return
	}

	fmt.Println("SCAN result:")
	for i := 0; i < len(r.Kvs); i++ {
		fmt.Printf("[%q\t%q]\t[%d\t%q]\n",
			r.Kvs[i].RowKey, r.Kvs[i].ColKey,
			r.Kvs[i].Score, r.Kvs[i].Value)
	}
	if r.End {
		fmt.Println("SCAN finished!")
	} else {
		fmt.Println("SCAN has more records!")
	}
}

func testZScan(tc *table.Context) {
	err := tc.ZSet(1, []byte("row2"), []byte("000"), []byte("v00"), 10, 0)
	if err != nil {
		fmt.Printf("ZSet failed: %s\n", err)
		return
	}

	var ma table.MSetArgs
	ma.Add(1, []byte("row2"), []byte("001"), []byte("v01"), 9, 0)
	ma.Add(1, []byte("row2"), []byte("002"), []byte("v02"), 6, 0)
	ma.Add(1, []byte("row2"), []byte("003"), []byte("v03"), 7, 0)
	ma.Add(1, []byte("row2"), []byte("004"), []byte("v04"), 8, 0)
	ma.Add(1, []byte("row2"), []byte("005"), []byte("v05"), -5, 0)
	_, err = tc.ZmSet(ma)
	if err != nil {
		fmt.Printf("ZmSet failed: %s\n", err)
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
		for i := 0; i < len(r.Kvs); i++ {
			fmt.Printf("[%q\t%q]\t[%d\t%q]\n",
				r.Kvs[i].RowKey, r.Kvs[i].ColKey,
				r.Kvs[i].Score, r.Kvs[i].Value)
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
	var value []byte
	var score int64
	var cas, newCas uint32
	var err error
	// Try i < 11 for cas not match
	for i := 0; i < 1; i++ {
		value, score, newCas, err = tc.Get(1, []byte("row1"), []byte("col1"), 2)
		if err != nil {
			fmt.Printf("Get failed: %s\n", err)
			return
		}

		if i > 0 {
			time.Sleep(time.Second)
		} else {
			cas = newCas
		}

		fmt.Printf("\tCas %02d: (%d %d)\t(%s, %d)\n", i, newCas, cas, value, score)
	}

	err = tc.Set(1, []byte("row1"), []byte("col1"),
		[]byte(string(value)+"-cas"), score+20, cas)
	if err != nil {
		fmt.Printf("Set failed: %s\n", err)
	}

	value, score, _, err = tc.Get(1, []byte("row1"), []byte("col1"), 0)
	if err != nil {
		fmt.Printf("Get failed: %s\n", err)
		return
	}

	fmt.Printf("CAS result: %q\t%d\n", value, score)
}

func testBinary(tc *table.Context) {
	var colKey = make([]byte, 4)
	var value = make([]byte, 8)
	binary.BigEndian.PutUint32(colKey, 998365)
	binary.BigEndian.PutUint64(value, 6000000000)
	err := tc.Set(1, []byte("row1"), colKey, value, 30, 0)
	if err != nil {
		fmt.Printf("Set failed: %s\n", err)
	}

	value, score, _, err := tc.Get(1, []byte("row1"), colKey, 0)
	if err != nil {
		fmt.Printf("Get failed: %s\n", err)
		return
	}

	fmt.Printf("Binary result: %q\t%d\t%d\n",
		value, binary.BigEndian.Uint64(value), score)
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
		for _, kv := range r.Kvs {
			fmt.Printf("%02d) %d\t%q\t%d\t%q\t%d\t%q\n", idx,
				kv.TableId, kv.RowKey, kv.ColSpace, kv.ColKey, kv.Score, kv.Value)
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
