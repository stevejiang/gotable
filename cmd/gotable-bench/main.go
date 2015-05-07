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
	"flag"
	"fmt"
	"github.com/stevejiang/gotable/api/go/table"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	address  = flag.String("h", "127.0.0.1:6688", "Server host address ip:port")
	network  = flag.String("N", "tcp", "Server network: tcp, tcp4, tcp6, unix")
	cliNum   = flag.Int("c", 50, "Number of parallel clients")
	reqNum   = flag.Int("n", 100000, "Total number of requests")
	dataSize = flag.Int("d", 8, "Data size of SET/MSET value in bytes")
	testCase = flag.String("t", "set,get", "Test cases: "+
		"set,get,zset,zget,scan,zscan,incr,zincr,mset,zmset,mget,zmget")
	rangeNum    = flag.Int("range", 10, "Scan/MGet/Mset range number")
	histogram   = flag.Int("histogram", 0, "Print histogram of operation timings")
	pipeline    = flag.Int("P", 0, "Pipeline number")
	verbose     = flag.Int("v", 0, "Verbose mode, if enabled it will slow down the test")
	poolNum     = flag.Int("pool", 10, "Max number of pool connections")
	maxProcs    = flag.Int("cpu", runtime.NumCPU(), "Go Max Procs")
	profileport = flag.String("profileport", "", "Profile port, such as 8080")
)

func main() {
	flag.Parse()
	if *maxProcs > 0 {
		runtime.GOMAXPROCS(*maxProcs)
	}

	var cliPool = table.NewPool([]table.Addr{{*network, *address}}, *poolNum)
	client, err := cliPool.Get()
	if err != nil {
		fmt.Printf("Get connection client to host %s://%s failed!\n\n",
			*network, *address)
		cliPool.Close()
		flag.Usage()
		os.Exit(1)
	}
	client.Close()

	var tests = strings.Split(*testCase, ",")
	for _, t := range tests {
		switch t {
		case "get":
			benchGet(cliPool, false)
		case "zget":
			benchGet(cliPool, true)
		case "set":
			benchSet(cliPool, false)
		case "zset":
			benchSet(cliPool, true)
		case "incr":
			benchIncr(cliPool, false)
		case "zincr":
			benchIncr(cliPool, true)
		case "scan":
			benchScan(cliPool, false)
		case "zscan":
			benchScan(cliPool, true)
		case "mset":
			benchMSet(cliPool, false)
		case "zmset":
			benchMSet(cliPool, true)
		case "mget":
			benchMGet(cliPool, false)
		case "zmget":
			benchMGet(cliPool, true)
		default:
			fmt.Printf("Unknown test case: %s\n", t)
		}
	}
}

type OpParam struct {
	c      *table.Context
	done   chan *table.Call
	keyBuf []byte
	value  []byte
}

func NewOpParam(id int, cliPool *table.Pool) *OpParam {
	var p = new(OpParam)
	client, _ := cliPool.Get()
	p.c = client.NewContext(0)

	if *pipeline > 0 {
		p.done = make(chan *table.Call, *pipeline)
	}

	p.keyBuf = make([]byte, 0, 64)
	p.value = make([]byte, *dataSize)
	for i := 0; i < *dataSize; i++ {
		p.value[i] = 'x'
	}

	return p
}

func (p *OpParam) ResetValue(cutLen int) {
	for i := 0; i < cutLen && i < len(p.value); i++ {
		p.value[i] = 'x'
	}
}

func benchmark(cliPool *table.Pool, name string, op func(v int, p *OpParam)) {
	var numChan = make(chan int, 100000)
	go func() {
		var n = *reqNum + 1000
		for i := 1000; i < n; i++ {
			numChan <- i
		}

		close(numChan)
	}()

	var hists []Histogram
	if *histogram != 0 && *pipeline <= 0 {
		hists = make([]Histogram, *cliNum)
	}

	var g sync.WaitGroup
	g.Add(*cliNum)

	start := time.Now()
	for i := 0; i < *cliNum; i++ {
		go benchClient(i, cliPool, name, &g, numChan, start, hists, op)
	}

	g.Wait()

	elapsed := time.Since(start)
	speed := float64(*reqNum) * 1e9 / float64(elapsed)
	fmt.Printf("%-10s : %9.1f op/s\n", name, speed)

	if hists != nil {
		var hist = hists[0]
		for i := 1; i < len(hists); i++ {
			hist.Merge(&hists[i])
		}
		fmt.Printf("Microseconds per op:\n%s\n", hist.ToString())
	}
}

func benchClient(id int, cliPool *table.Pool, name string, g *sync.WaitGroup,
	numChan <-chan int, start time.Time, hists []Histogram, op func(v int, p *OpParam)) {
	defer g.Done()

	var hist *Histogram
	if hists != nil {
		hist = &hists[id]
		hist.Clear()
	}

	var p = NewOpParam(id, cliPool)
	var pipeReqNum int
	var opStart time.Time
	for {
		select {
		case v, ok := <-numChan:
			if !ok {
				if p.done != nil {
					for i := 0; i < pipeReqNum; i++ {
						<-p.done
					}
				}
				p.c.Client().Close()
				return
			}

			if hist != nil {
				opStart = time.Now()
			}

			op(v, p) // Handle OP

			if p.done != nil {
				pipeReqNum++
				if pipeReqNum >= *pipeline {
					for i := 0; i < pipeReqNum; i++ {
						<-p.done
					}
					pipeReqNum = 0
				}
			}

			if hist != nil {
				d := time.Since(opStart)
				hist.Add(float64(d / 1000))
			}

			if v%10000 == 0 && v > 0 {
				elapsed := time.Since(start)
				speed := float64(v+1) * 1e9 / float64(elapsed)
				fmt.Printf("%-10s : %9.1f op/s    \r", name, speed)
			}
		}
	}
}

func benchSet(cliPool *table.Pool, zop bool) {
	var op = func(v int, p *OpParam) {
		key := strconv.AppendInt(p.keyBuf, int64(v), 10)
		var rowKey = key[0 : len(key)-3]
		var colKey = key[len(key)-3:]
		copy(p.value, key)

		set(p.c, p.done, zop, rowKey, colKey, p.value, int64(-v))

		p.ResetValue(len(key))
	}

	if zop {
		benchmark(cliPool, "ZSET", op)
	} else {
		benchmark(cliPool, "SET", op)
	}
}

func benchGet(cliPool *table.Pool, zop bool) {
	var op = func(v int, p *OpParam) {
		key := strconv.AppendInt(p.keyBuf, int64(v), 10)
		var rowKey = key[0 : len(key)-3]
		var colKey = key[len(key)-3:]

		get(p.c, p.done, zop, rowKey, colKey)
	}

	if zop {
		benchmark(cliPool, "ZGET", op)
	} else {
		benchmark(cliPool, "GET", op)
	}
}

func benchIncr(cliPool *table.Pool, zop bool) {
	var op = func(v int, p *OpParam) {
		key := strconv.AppendInt(p.keyBuf, int64(v), 10)
		var rowKey = key[0 : len(key)-3]
		var colKey = key[len(key)-3:]

		incr(p.c, p.done, zop, rowKey, colKey, 1)
	}

	if zop {
		benchmark(cliPool, "ZINCR", op)
	} else {
		benchmark(cliPool, "INCR", op)
	}
}

func benchScan(cliPool *table.Pool, zop bool) {
	var startScore int64
	if zop {
		startScore = -500000000
	}

	var op = func(v int, p *OpParam) {
		key := strconv.AppendInt(p.keyBuf, int64(v), 10)
		var rowKey = key[0 : len(key)-3]

		scan(p.c, p.done, zop, rowKey, nil, startScore, *rangeNum)
	}

	if zop {
		benchmark(cliPool, fmt.Sprintf("ZSCAN %d", *rangeNum), op)
	} else {
		benchmark(cliPool, fmt.Sprintf("SCAN %d", *rangeNum), op)
	}
}

func benchMSet(cliPool *table.Pool, zop bool) {
	var colKeys = make([][]byte, *rangeNum)
	for i := 0; i < *rangeNum; i++ {
		colKeys[i] = []byte(fmt.Sprintf("%03d", i))
	}

	var op = func(v int, p *OpParam) {
		var rowKey = strconv.AppendInt(p.keyBuf, int64(v), 10)
		copy(p.value, rowKey)

		var ma table.MSetArgs
		for i := 0; i < len(colKeys); i++ {
			ma.Add(0, rowKey, colKeys[i], p.value, int64(v*1000+i), 0)
		}

		mSet(p.c, p.done, zop, ma)

		p.ResetValue(len(rowKey))
	}

	if zop {
		benchmark(cliPool, fmt.Sprintf("ZMSET %d", *rangeNum), op)
	} else {
		benchmark(cliPool, fmt.Sprintf("MSET %d", *rangeNum), op)
	}
}

func benchMGet(cliPool *table.Pool, zop bool) {
	var colKeys = make([][]byte, *rangeNum)
	for i := 0; i < *rangeNum; i++ {
		colKeys[i] = []byte(fmt.Sprintf("%03d", i))
	}

	var op = func(v int, p *OpParam) {
		var rowKey = strconv.AppendInt(p.keyBuf, int64(v), 10)

		var ma table.MGetArgs
		for i := 0; i < len(colKeys); i++ {
			ma.Add(0, rowKey, colKeys[i], 0)
		}

		mGet(p.c, p.done, zop, ma)
	}

	if zop {
		benchmark(cliPool, fmt.Sprintf("ZMGET %d", *rangeNum), op)
	} else {
		benchmark(cliPool, fmt.Sprintf("MGET %d", *rangeNum), op)
	}
}

func set(c *table.Context, done chan *table.Call, zop bool,
	rowKey, colKey, value []byte, score int64) {
	var err error
	if zop {
		if done == nil {
			err = c.ZSet(0, rowKey, colKey, value, score, 0)
		} else {
			_, err = c.GoZSet(0, rowKey, colKey, value, score, 0, done)
		}
	} else {
		if done == nil {
			err = c.Set(0, rowKey, colKey, value, score, 0)
		} else {
			_, err = c.GoSet(0, rowKey, colKey, value, score, 0, done)
		}
	}
	if err != nil {
		fmt.Printf("Set failed: %s\n", err)
		os.Exit(1)
	}

	if *verbose != 0 && done == nil {
		fmt.Printf("rowKey: %2s, colKey: %s\n", string(rowKey), string(colKey))
	}
}

func get(c *table.Context, done chan *table.Call, zop bool, rowKey, colKey []byte) {
	var err error
	var value []byte
	var score int64
	if zop {
		if done == nil {
			value, score, _, err = c.ZGet(0, rowKey, colKey, 0)
		} else {
			_, err = c.GoZGet(0, rowKey, colKey, 0, done)
		}
	} else {
		if done == nil {
			value, score, _, err = c.Get(0, rowKey, colKey, 0)
		} else {
			_, err = c.GoGet(0, rowKey, colKey, 0, done)
		}
	}
	if err != nil {
		fmt.Printf("Get failed: %s\n", err)
		os.Exit(1)
	}

	if *verbose != 0 && done == nil {
		fmt.Printf("rowKey: %2s, colKey: %s, value: %s, score:%d\n",
			string(rowKey), string(colKey), string(value), score)
	}
}

func incr(c *table.Context, done chan *table.Call, zop bool,
	rowKey, colKey []byte, score int64) {
	var err error
	var value []byte
	if zop {
		if done == nil {
			value, score, err = c.ZIncr(0, rowKey, colKey, score, 0)
		} else {
			_, err = c.GoZIncr(0, rowKey, colKey, score, 0, done)
		}
	} else {
		if done == nil {
			value, score, err = c.Incr(0, rowKey, colKey, score, 0)
		} else {
			_, err = c.GoIncr(0, rowKey, colKey, score, 0, done)
		}
	}
	if err != nil {
		fmt.Printf("Incr failed: %s\n", err)
		os.Exit(1)
	}

	if *verbose != 0 && done == nil {
		fmt.Printf("rowKey: %2s, colKey: %s, value: %s, score:%d\n",
			string(rowKey), string(colKey), string(value), score)
	}
}

func scan(c *table.Context, done chan *table.Call, zop bool,
	rowKey, colKey []byte, score int64, num int) {
	var err error
	var r table.ScanReply
	if zop {
		if done == nil {
			r, err = c.ZScanPivot(0, rowKey, colKey, score, true, true, num)
		} else {
			_, err = c.GoZScanPivot(0, rowKey, colKey, score, true, true, num, done)
		}
	} else {
		if done == nil {
			r, err = c.ScanPivot(0, rowKey, colKey, true, num)
		} else {
			_, err = c.GoScanPivot(0, rowKey, colKey, true, num, done)
		}
	}
	if err != nil {
		fmt.Printf("Scan failed: %s\n", err)
		os.Exit(1)
	}

	if *verbose != 0 && done == nil {
		for i := 0; i < len(r.Kvs); i++ {
			var one = r.Kvs[i]
			fmt.Printf("%02d) [%q\t%q]\t[%d\t%q]\n", i,
				r.RowKey, one.ColKey, one.Score, one.Value)
		}
	}
}

func mSet(c *table.Context, done chan *table.Call, zop bool, ma table.MSetArgs) {
	var err error
	if zop {
		if done == nil {
			_, err = c.ZmSet(ma)
		} else {
			_, err = c.GoZmSet(ma, done)
		}
	} else {
		if done == nil {
			_, err = c.MSet(ma)
		} else {
			_, err = c.GoMSet(ma, done)
		}
	}
	if err != nil {
		fmt.Printf("MSet failed: %s\n", err)
		os.Exit(1)
	}

	if *verbose != 0 && done == nil {
		for i := 0; i < len(ma); i++ {
			var one = ma[i]
			fmt.Printf("%02d) [%q\t%q]\t[%d\t%q]\n", i,
				one.RowKey, one.ColKey, one.Score, one.Value)
		}
	}
}

func mGet(c *table.Context, done chan *table.Call, zop bool, ma table.MGetArgs) {
	var err error
	var r []table.GetReply
	if zop {
		if done == nil {
			r, err = c.ZmGet(ma)
		} else {
			_, err = c.GoZmGet(ma, done)
		}
	} else {
		if done == nil {
			r, err = c.MGet(ma)
		} else {
			_, err = c.GoMGet(ma, done)
		}
	}
	if err != nil {
		fmt.Printf("MGet failed: %s\n", err)
		os.Exit(1)
	}

	if *verbose != 0 && done == nil {
		for i := 0; i < len(r); i++ {
			var one = r[i]
			fmt.Printf("%02d) [%q\t%q]\t[%d\t%q]\n", i,
				one.RowKey, one.ColKey, one.Score, one.Value)
		}
	}
}
