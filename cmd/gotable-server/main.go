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
	"github.com/stevejiang/gotable/config"
	"github.com/stevejiang/gotable/server"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
)

func main() {
	var configFile string
	if len(os.Args) > 1 {
		configFile = os.Args[1]
	}

	conf, err := config.Load(configFile)
	if err != nil {
		log.Fatalf("Failed to load config: %s", err)
	}

	var maxProcs = conf.Db.MaxCpuNum
	if maxProcs == 0 {
		maxProcs = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(maxProcs)

	if len(conf.Profile.Host) > 0 {
		log.Printf("Start profile on http://%s/debug/pprof\n", conf.Profile.Host)
		go func() {
			http.ListenAndServe(conf.Profile.Host, nil)
		}()
	}

	go func() {
		var c = make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGUSR1)

		var s = <-c
		log.Println("Get signal:", s)

		if conf.Profile.Memory != "" {
			f, err := os.Create(conf.Profile.Memory)
			if err != nil {
				log.Println("create memory profile file failed:", err)
				return
			}

			pprof.WriteHeapProfile(f)
			f.Close()
		}

		os.Exit(0)
	}()

	server.Run(conf)
}
