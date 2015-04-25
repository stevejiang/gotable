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
	"github.com/GeertJohan/go.linenoise"
	"os"
	"regexp"
	"strings"
)

var (
	address = flag.String("h", "127.0.0.1:6688", "Server host address ip:port")
	network = flag.String("N", "tcp", "Server network: tcp, tcp4, tcp6, unix")
)

func main() {
	flag.Parse()

	var cli = newClient()
	if cli == nil {
		writeln("Failed to create client!\n")
		flag.Usage()
		return
	}

	re, _ := regexp.Compile(`'[^\'\"]*'|[qQ]?"[^\'\"]*"|[^\s\'\"]+`)

	writeln("Welcome to GoTable.")
	//writeHelp()
	for {
		line, err := linenoise.Line(fmt.Sprintf("gotable@%d> ", cli.dbId))
		if err != nil {
			if err == linenoise.KillSignalError {
				os.Exit(0)
			}
			writeln("Unexpected error: %s" + err.Error())
			os.Exit(0)
		}
		fields := fullMatchString(re, line)

		if len(line) == 0 || !notWhiteSpace(line, 0, len(line)) {
			continue
		}

		linenoise.AddHistory(line)

		if len(fields) == 0 {
			writeUnrecognized()
			continue
		}

		var cmd = strings.ToLower(fields[0])
		switch cmd {
		case "ping":
			checkError(cli.ping())
		case "get":
			checkError(cli.get(false, fields[1:]))
		case "set":
			checkError(cli.set(false, fields[1:]))
		case "del":
			checkError(cli.del(false, fields[1:]))
		case "incr":
			checkError(cli.incr(false, fields[1:]))
		case "zget":
			checkError(cli.get(true, fields[1:]))
		case "zset":
			checkError(cli.set(true, fields[1:]))
		case "zdel":
			checkError(cli.del(true, fields[1:]))
		case "zincr":
			checkError(cli.incr(true, fields[1:]))
		case "scan":
			checkError(cli.scan(fields[1:]))
		case "zscan":
			checkError(cli.zscan(fields[1:]))
		case "auth":
			checkError(cli.auth(fields[1:]))
		case "select":
			checkError(cli.use(fields[1:]))
		case "slaveof":
			checkError(cli.slaveOf(fields[1:]))
		case "dump":
			checkError(cli.dump(fields[1:]))

		case "?":
			fallthrough
		case "help":
			writeHelp()
		case "clear":
			linenoise.Clear()
		case "exit":
			fallthrough
		case "q":
			fallthrough
		case "quit":
			quit()
		default:
			writeUnrecognized()
		}
	}
}

func quit() {
	writeln("Bye.")
	writeln("")
	os.Exit(0)
}

func writeln(msg string) {
	fmt.Fprintln(os.Stderr, msg)
}

func writeHelp() {
	writeln("  help                      print help message")
	writeln("  auth <dbId> <password>    authenticate to the database")
	writeln("select <dbId>               select database [0 ~ 254] to use")
	writeln("   set <tableId> <rowKey> <colKey> <value> [score]")
	writeln("                            set key/value in selected database")
	writeln("   get <tableId> <rowKey> <colKey>")
	writeln("                            get key/value in selected database")
	writeln("   del <tableId> <rowKey> <colKey>")
	writeln("                            delete key in selected database")
	writeln("  incr <tableId> <rowKey> <colKey> [score]")
	writeln("                            increase key score in selected database")
	writeln("  zset <tableId> <rowKey> <colKey> <value> [score]")
	writeln("                            zset key/value in selected database")
	writeln("  zget <tableId> <rowKey> <colKey>")
	writeln("                            zget key/value in selected database")
	writeln("  zdel <tableId> <rowKey> <colKey>")
	writeln("                            zdel key in selected database")
	writeln(" zincr <tableId> <rowKey> <colKey> [score]")
	writeln("                            zincr key score in selected database")
	writeln("  scan <tableId> <rowKey> <colKey> [num]")
	writeln("                            scan columns of rowKey in ASC order")
	writeln(" zscan <tableId> <rowKey> <score> <colKey> [num]")
	writeln("                            zscan columns of rowKey in ASC order by score")
	writeln("  dump <dbId> [tableId]     dump the selected database or the table. Fields are:")
	writeln("                            dbId, tableId, rowKey, colSpace, colKey, value, score")
	writeln("slaveof [host]              be slave of master host(ip:port)")
	writeln("  ping                      ping the server")
	writeln(" clear                      clear the screen")
	writeln("  quit                      exit")
	writeln("")
	writeln("Use the arrow up and down keys to walk through history.")
	writeln("")
}

func writeUnrecognized() {
	writeln("Unrecognized command line. Use 'help'.")
}

func checkError(err error) {
	if err != nil {
		writeln("Failed with error: %s" + err.Error())
	}
}

func isWhiteSpace(c byte) bool {
	//\s             whitespace (== [\t\n\f\r ])
	if c != ' ' && c != '\t' && c != '\n' && c != '\f' && c != '\r' {
		return false
	}
	return true
}

func notWhiteSpace(s string, a, b int) bool {
	for j := a; j < b; j++ {
		if !isWhiteSpace(s[j]) {
			return true
		}
	}
	return false
}

func fullMatchString(re *regexp.Regexp, s string) []string {
	var rs = re.FindAllStringIndex(s, -1)
	var cur int
	for _, r := range rs {
		if notWhiteSpace(s, cur, r[0]) {
			return nil
		}
		if cur > 0 && cur == r[0] {
			return nil
		}
		cur = r[1]
	}

	if notWhiteSpace(s, cur, len(s)) {
		return nil
	}

	return re.FindAllString(s, -1)
}
