// ctable project main.go
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
		line, err := linenoise.Line("prompt> ")
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

		if len(fields) == 0 {
			writeUnrecognized()
			continue
		}

		linenoise.AddHistory(line)

		var cmd = strings.ToLower(fields[0])
		switch cmd {
		case "ping":
			fallthrough
		case "get":
			checkError(cli.get(fields[1:]))
		case "set":
			checkError(cli.set(fields[1:]))
		case "zget":
			checkError(cli.zget(fields[1:]))
		case "zset":
			checkError(cli.zset(fields[1:]))
		case "scan":
			checkError(cli.scan(fields[1:]))
		case "zscan":
			checkError(cli.zscan(fields[1:]))
		case "use":
			checkError(cli.use(fields[1:]))

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
	fmt.Println("help                   print this message")
	fmt.Println("use <databaseId>       use database (0 ~ 200)")
	fmt.Println("set <tableId> <rowKey> <colKey> <value>")
	fmt.Println("                       set key/value for table in selected database")
	fmt.Println("get <tableId> <rowKey> <colKey>")
	fmt.Println("                       get key/value for table in selected database")
	fmt.Println("zset <tableId> <rowKey> <colKey> <value> <score>")
	fmt.Println("                       zset key/value for table in selected database")
	fmt.Println("zget <tableId> <rowKey> <colKey>")
	fmt.Println("                       zget key/value for table in selected database")
	fmt.Println("scan <tableId> <rowKey> <colKey> <num>")
	fmt.Println("zscan <tableId> <rowKey> <score> <colKey> <num>")
	fmt.Println("clear                  clear the screen")
	fmt.Println("quit                   exit")
	fmt.Println("")
	fmt.Println("Use the arrow up and down keys to walk through history.")
	fmt.Println("")
}

func writeUnrecognized() {
	fmt.Println("Unrecognized command. Use 'help'.")
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
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
