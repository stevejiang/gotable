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
