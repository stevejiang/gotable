package main

import (
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
	testCas(tc)
	testMget(tc)
	testScan(tc)
	testZscan(tc)

	time.Sleep(time.Millisecond)
}

func testGet(tc *table.Client) {
	_, err := tc.Set(false, &table.OneArgs{1, []byte("row1"), []byte("col1"), []byte("v1"), 10, 0})
	if err != nil {
		fmt.Printf("Set fialed: %s\n", err)
		return
	}

	r, err := tc.Get(false, &table.OneArgs{1, []byte("row1"), []byte("col1"), nil, 0, 0})
	if err != nil {
		fmt.Printf("Get fialed: %s\n", err)
		return
	}

	fmt.Printf("GET result: %q\t%d\n", r.Value, r.Score)
}

func testCas(tc *table.Client) {
	var cas uint32
	for i := 0; i < 1; i++ {
		r, err := tc.Get(false, &table.OneArgs{1, []byte("row1"), []byte("col1"), nil, 0, 1})
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

	_, err := tc.Set(false, &table.OneArgs{1, []byte("row1"), []byte("col1"), []byte("v20"), 20, cas})
	if err != nil {
		fmt.Printf("Set fialed: %s\n", err)
	}

	r, err := tc.Get(false, &table.OneArgs{1, []byte("row1"), []byte("col1"), nil, 0, 0})
	if err != nil {
		fmt.Printf("Get fialed: %s\n", err)
		return
	}

	fmt.Printf("CAS result: %q\t%d\n", r.Value, r.Score)
}

func testMget(tc *table.Client) {
	var ma table.MultiArgs
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("col0"), []byte("v00"), 10, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("col1"), []byte("v01"), 9, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("col2"), []byte("v02"), 8, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("col3"), []byte("v03"), 7, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("col4"), []byte("v04"), 6, 0})
	_, err := tc.Mset(false, &ma)
	if err != nil {
		fmt.Printf("Mset fialed: %s\n", err)
		return
	}

	ma = table.MultiArgs{}
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("col1"), nil, 0, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("col2"), nil, 0, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("col3"), nil, 0, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("col4"), nil, 0, 0})
	r, err := tc.Mget(false, &ma)
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
	r, err := tc.Scan(false, &sa)
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

func testZscan(tc *table.Client) {
	_, err := tc.Set(true, &table.OneArgs{1, []byte("row1"), []byte("000"), []byte("v00"), 10, 0})
	if err != nil {
		fmt.Printf("Set fialed: %s\n", err)
	}

	var ma table.MultiArgs
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("001"), []byte("v01"), 9, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("002"), []byte("v02"), 8, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("003"), []byte("v03"), 7, 0})
	ma.Args = append(ma.Args, table.OneArgs{1, []byte("row1"), []byte("004"), []byte("v04"), 6, 0})
	_, err = tc.Mset(true, &ma)
	if err != nil {
		fmt.Printf("Mset fialed: %s\n", err)
		return
	}

	var sa = table.ScanArgs{10, 0, table.OneArgs{1, []byte("row1"), []byte("000"), nil, 6, 0}}
	r, err := tc.Scan(true, &sa)
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
