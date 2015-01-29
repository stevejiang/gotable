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
	host     = flag.String("h", "127.0.0.1:6688", "Server host address ip:port")
	userName = flag.String("u", "", "User name for authentication")
	password = flag.String("p", "", "Password for authentication")
)

func main() {
	flag.Parse()

	var cli = newClient()
	if cli == nil {
		fmt.Printf("Failed to create client!\n")
		flag.Usage()
		return
	}

	re, _ := regexp.Compile(`'[^\'\"]*'|"[^\'\"]*"|[^\s\'\"]+`)

	fmt.Println("Welcome to GoTable.")
	writeHelp()
	for {
		line, err := linenoise.Line(fmt.Sprintf("gotable@%d> ", cli.dbId))
		if err != nil {
			if err == linenoise.KillSignalError {
				quit()
			}
			fmt.Printf("Unexpected error: %s\n", err)
			quit()
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
			fallthrough
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
		case "use":
			checkError(cli.use(fields[1:]))
		case "slaveof":
			checkError(cli.slaveOf(fields[1:]))

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
	fmt.Println("Bye.")
	fmt.Println("")
	os.Exit(0)
}

func writeHelp() {
	fmt.Println(" help                      print this message")
	fmt.Println(" auth <dbId> <password>    authorize access to database")
	fmt.Println("  use <dbId>               use database (1 ~ 255)")
	fmt.Println("  set <tableId> <rowKey> <colKey> <value> [score]")
	fmt.Println("                           set key/value for table in selected database")
	fmt.Println("  get <tableId> <rowKey> <colKey>")
	fmt.Println("                           get key/value for table in selected database")
	fmt.Println("  del <tableId> <rowKey> <colKey>")
	fmt.Println("                           del key for table in selected database")
	fmt.Println(" incr <tableId> <rowKey> <colKey> [score]")
	fmt.Println("                           incr key score for table in selected database")
	fmt.Println(" zset <tableId> <rowKey> <colKey> <value> [score]")
	fmt.Println("                           zset key/value for table in selected database")
	fmt.Println(" zget <tableId> <rowKey> <colKey>")
	fmt.Println("                           zget key/value for table in selected database")
	fmt.Println(" zdel <tableId> <rowKey> <colKey>")
	fmt.Println("                           zdel key for table in selected database")
	fmt.Println("zincr <tableId> <rowKey> <colKey> [score]")
	fmt.Println("                           zincr key score for table in selected database")
	fmt.Println(" scan <tableId> <rowKey> <colKey> [num]")
	fmt.Println("zscan <tableId> <rowKey> <score> <colKey> [num]")
	fmt.Println("slaveof [masterHost]       be slave of master host ip:port")
	fmt.Println("clear                      clear the screen")
	fmt.Println(" quit                      exit")
	fmt.Println("")
	fmt.Println("Use the arrow up and down keys to walk through history.")
	fmt.Println("")
}

func writeUnrecognized() {
	fmt.Println("Unrecognized command. Use 'help'.")
}

func checkError(err error) {
	if err != nil {
		fmt.Printf("Failed with error: %s\n", err)
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
		cur = r[1]
	}

	if notWhiteSpace(s, cur, len(s)) {
		return nil
	}

	return re.FindAllString(s, -1)
}
