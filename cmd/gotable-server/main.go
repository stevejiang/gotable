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
	"runtime/pprof"
	"syscall"
)

func main() {
	log.SetFlags(log.Flags() | log.Lshortfile)

	var configFile string
	if len(os.Args) > 1 {
		configFile = os.Args[1]
	}

	conf, err := config.Load(configFile)
	if err != nil {
		log.Fatalf("Failed to load config: %s", err)
	}

	var srv = server.NewServer(conf)
	if srv == nil {
		log.Fatalln("Failed to create new server!")
	}

	if len(conf.Profile.Host) > 0 {
		log.Printf("Start profile on http://%s/debug/pprof\n", conf.Profile.Host)
		go func() {
			http.ListenAndServe(conf.Profile.Host, nil)
		}()
	}

	go func() {
		var c = make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGUSR1, syscall.SIGINT)

		var s = <-c
		log.Println("Get signal:", s)

		if s == syscall.SIGUSR1 {
			if conf.Profile.Memory != "" {
				f, err := os.Create(conf.Profile.Memory)
				if err != nil {
					log.Println("create memory profile file failed:", err)
					return
				}

				pprof.WriteHeapProfile(f)
				f.Close()
			}
		}

		srv.Close()
	}()

	srv.Start()
}
