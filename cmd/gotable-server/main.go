// ctable project main.go
package main

import (
	"flag"
	"fmt"
	"github.com/stevejiang/gotable/server"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
)

var (
	host        = flag.String("h", ":6688", "Server host address ip:port")
	dbname      = flag.String("db", "db", "Database directory name")
	masterhost  = flag.String("m", "", "Master server host address ip:port")
	maxProcs    = flag.Int("cpu", runtime.NumCPU(), "Go Max Procs")
	profileport = flag.Int("profileport", 0, "profile port, such as 8080")
	memprofile  = flag.String("memprofile", "", "write memory profile to this file")
)

func main() {
	flag.Parse()

	if *maxProcs > 0 {
		runtime.GOMAXPROCS(*maxProcs)
	}

	if *profileport > 0 {
		go func() {
			http.ListenAndServe(fmt.Sprintf(":%d", *profileport), nil)
		}()
	}

	go func() {
		var c = make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGUSR1)

		var s = <-c
		fmt.Println("Got signal:", s)

		if *memprofile != "" {
			f, err := os.Create(*memprofile)
			if err != nil {
				fmt.Println("create file failed: ", err)
				return
			}

			pprof.WriteHeapProfile(f)
			f.Close()
		}

		os.Exit(0)
	}()

	server.Run(*dbname, *host, *masterhost)
}
