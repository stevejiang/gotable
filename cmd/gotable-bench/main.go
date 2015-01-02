package main

import (
	"flag"
	"fmt"
	"github.com/stevejiang/gotable/api/go/table"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	host        = flag.String("h", "127.0.0.1:6688", "Server host address ip:port")
	cliNum      = flag.Int("c", 50, "Number of parallel clients")
	reqNum      = flag.Int("n", 100000, "Total number of requests")
	dataSize    = flag.Int("d", 4, "Data size of SET/GET value in bytes")
	testCase    = flag.String("t", "set,get", "Run the comma separated list of tests")
	histogram   = flag.Int("histogram", 0, "Print histogram of operation timings")
	verbose     = flag.Int("v", 0, "Verbose mode, if enabled it will slow down the test")
	poolNum     = flag.Int("pool", 5, "Max number of pool connections")
	maxProcs    = flag.Int("cpu", runtime.NumCPU(), "Go Max Procs")
	profileport = flag.String("profileport", "", "Profile port, such as 8080")
)

var valueData []byte

func main() {
	flag.Parse()
	if *maxProcs > 0 {
		runtime.GOMAXPROCS(*maxProcs)
	}

	var tests = strings.Split(*testCase, ",")
	for _, t := range tests {
		switch t {
		case "get":
			benchGet()
		case "set":
			benchSet()
		default:
			fmt.Printf("Unknown test case: %s\n", t)
		}
	}
}

func benchmark(name string, op func(v int, client *table.Client)) {
	var g sync.WaitGroup
	g.Add(*cliNum)

	var numChan = make(chan int, 10000)
	go func() {
		var n = *reqNum + 1000
		for i := 1000; i < n; i++ {
			numChan <- i
		}

		close(numChan)
	}()

	var cliPool = table.NewPool([]table.Addr{{1, "tcp", *host}}, *poolNum)

	var recordHist = (*histogram != 0)
	var hists = make([]Histogram, *cliNum)

	start := time.Now()
	for i := 0; i < *cliNum; i++ {
		go func(id int) {
			defer g.Done()

			client, _ := cliPool.Get(1)

			var hist = &hists[id]
			hist.Clear()

			var opStart time.Time
			for {
				select {
				case v, ok := <-numChan:
					if !ok {
						client.Close()
						return
					}

					if recordHist {
						opStart = time.Now()
					}

					op(v, client)

					if recordHist {
						d := time.Since(opStart)
						hist.Add(float64(d / 1000))
					}

					if v%10000 == 0 && v > 0 {
						elapsed := time.Since(start)
						speed := float64(v+1) * 1e9 / float64(elapsed)
						fmt.Printf("%-8s : %9.1f op/s\r", name, speed)
					}
				}
			}
		}(i)
	}

	g.Wait()

	elapsed := time.Since(start)
	speed := float64(*reqNum) * 1e9 / float64(elapsed)
	fmt.Printf("%-8s : %9.1f op/s\n", name, speed)

	if recordHist {
		var hist = hists[0]
		for i := 1; i < len(hists); i++ {
			hist.Merge(&hists[i])
		}
		fmt.Printf("Microseconds per op:\n%s\n", hist.ToString())
	}
}

func benchSet() {
	var value = make([]byte, *dataSize)
	for i := 0; i < *dataSize; i++ {
		value[i] = 'x'
	}

	var keyBuf = make([]byte, 0, 64)
	var op = func(v int, c *table.Client) {
		key := strconv.AppendInt(keyBuf, int64(v), 10)
		var rowKey = key[0 : len(key)-3]
		var colKey = key[len(key)-3:]
		copy(value, key)

		set(c, rowKey, colKey, value)

		for i := 0; i < len(key) && i < len(value); i++ {
			value[i] = 'x'
		}
	}

	benchmark("SET", op)
}

func benchGet() {
	var keyBuf = make([]byte, 0, 64)
	var op = func(v int, c *table.Client) {
		key := strconv.AppendInt(keyBuf, int64(v), 10)
		var rowKey = key[0 : len(key)-3]
		var colKey = key[len(key)-3:]

		get(c, rowKey, colKey)
	}

	benchmark("GET", op)
}

func set(c *table.Client, rowKey, colKey, value []byte) {
	err := c.Set(rowKey, colKey, value)
	if err != nil {
		fmt.Printf("Set failed: %s\n", err)
		return
	}

	if *verbose != 0 {
		fmt.Printf("rowKey: %2s, colKey: %s\n", string(rowKey), string(colKey))
	}
}

func get(c *table.Client, rowKey, colKey []byte) {
	value, err := c.Get(rowKey, colKey)
	if err != nil && err != table.ErrNotExist {
		fmt.Printf("Get failed: %s\n", err)
		return
	}

	if *verbose != 0 {
		fmt.Printf("rowKey: %2s, colKey: %s, value: %s\n",
			string(rowKey), string(colKey), string(value))
	}
}
