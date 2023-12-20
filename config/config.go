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

package config

import (
	"github.com/BurntSushi/toml"
	"log"
)

type Config struct {
	Db      database `toml:"database"`
	Bin     binlog   `toml:"binlog"`
	Auth    auth
	Profile profile
}

type database struct {
	Network      string
	Address      string
	Data         string
	MaxCpuNum    int   `toml:"max_cpu_num"`
	WriteBufSize int   `toml:"write_buffer_size"`
	CacheSize    int64 `toml:"cache_size"`
	Compression  string
}

type binlog struct {
	MemSize int `toml:"memory_size"`
	KeepNum int `toml:"keep_num"`
}

type auth struct {
	AdminPwd string `toml:"admin_password"`
}

type profile struct {
	Memory string
	Host   string
}

func Load(fileName string) (*Config, error) {
	var conf Config
	var err error
	if len(fileName) == 0 {
		log.Println("Use default configuration")
		_, err = toml.Decode(defaultConfig, &conf)
	} else {
		log.Printf("Use configuration file %s\n", fileName)
		_, err = toml.DecodeFile(fileName, &conf)
	}

	if err != nil {
		return nil, err
	}
	return &conf, nil
}

var defaultConfig = `
[database]
network = "tcp"
address = "0.0.0.0:6688"
data = "data"
write_buffer_size = 67108864
cache_size = 67108864
compression = "no"

[binlog]
memory_size = 8
keep_num = 128

`
