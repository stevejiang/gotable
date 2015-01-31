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
	"fmt"
	"github.com/stevejiang/gotable/api/go/table"
	"github.com/stevejiang/gotable/ctrl"
	"strconv"
	"strings"
)

type client struct {
	c    *table.Context
	dbId uint8
}

func newClient() *client {
	var c = new(client)
	cli, err := table.Dial("tcp", *host)
	if err != nil {
		fmt.Println("dial failed: ", err)
		return nil
	}

	c.dbId = 1
	c.c = cli.NewContext(c.dbId)

	return c
}

func (c *client) auth(args []string) error {
	//auth <dbId> <password>
	if len(args) != 2 {
		return fmt.Errorf("invalid number of arguments (%d)", len(args))
	}

	dbId, err := getDatabaseId(args[0])
	if err != nil {
		return err
	}

	password, err := extractString(args[1])
	if err != nil {
		return err
	}

	ctx := c.c.Client().NewContext(dbId)
	err = ctx.Auth(password)
	if err != nil {
		return err
	}

	if dbId != 0 && dbId != c.dbId {
		c.c = ctx
		c.dbId = dbId
	}

	fmt.Println("OK")
	return nil
}

func (c *client) use(args []string) error {
	//use <databaseId>
	if len(args) != 1 {
		return fmt.Errorf("invalid number of arguments (%d)", len(args))
	}

	dbId, err := getDatabaseId(args[0])
	if err != nil {
		return err
	}

	c.dbId = dbId
	c.c = c.c.Client().NewContext(dbId)

	fmt.Println("OK")
	return nil
}

func (c *client) slaveOf(args []string) error {
	//slaveof ["host [start-end]*"]*
	//Examples:
	//slaveof "127.0.0.1:6688"
	//slaveof "127.0.0.1:6688 0-8191"
	//slaveof "127.0.0.1:6688 0-1203 4096-8191" "127.0.0.1:6689 1024-4095"
	var err error
	var mis []ctrl.MasterInfo
	for i := 0; i < len(args); i++ {
		var oneHost string
		oneHost, err = extractQuote(args[i])
		if err != nil {
			if len(args) == 1 {
				oneHost, err = extractString(args[i])
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
		tokens := strings.Split(oneHost, " ")
		if len(tokens) == 0 {
			return fmt.Errorf("empty host")
		}
		idx := strings.Index(tokens[0], ":")
		if idx <= 0 || idx >= len(tokens[0])-1 {
			return fmt.Errorf("invalid host %s", tokens[0])
		}
		if strings.Contains(tokens[0], " \t\r\n") {
			return fmt.Errorf("invalid host %s", tokens[0])
		}

		var mi ctrl.MasterInfo
		mi.Host = tokens[0]
		for j := 1; j < len(tokens); j++ {
			ur := strings.Split(tokens[j], "-")
			if len(ur) != 2 {
				return fmt.Errorf("invalid unit range %s", tokens[j])
			}
			start, err := strconv.Atoi(ur[0])
			if err != nil || start < 0 || start >= ctrl.TotalUnitNum {
				return fmt.Errorf("invalid unit range %s", tokens[j])
			}
			end, err := strconv.Atoi(ur[1])
			if err != nil || end < 0 || end >= ctrl.TotalUnitNum {
				return fmt.Errorf("invalid unit range %s", tokens[j])
			}
			mi.Urs = append(mi.Urs, ctrl.UnitRange{uint16(start), uint16(end)})
		}

		mis = append(mis, mi)
	}

	err = c.c.SlaveOf(mis)
	if err != nil {
		return err
	}

	fmt.Println("OK")
	return nil
}

func (c *client) get(zop bool, args []string) error {
	// get <tableId> <rowKey> <colKey>
	//zget <tableId> <rowKey> <colKey>
	if len(args) != 3 {
		return fmt.Errorf("invalid number of arguments (%d)", len(args))
	}

	tableId, err := getTableId(args[0])
	if err != nil {
		return err
	}

	rowKey, err := extractString(args[1])
	if err != nil {
		return err
	}
	colKey, err := extractString(args[2])
	if err != nil {
		return err
	}

	var r *table.OneReply
	if zop {
		r, err = c.c.ZGet(tableId, []byte(rowKey), []byte(colKey), 0)
	} else {
		r, err = c.c.Get(tableId, []byte(rowKey), []byte(colKey), 0)
	}
	if err != nil {
		return err
	}
	if r.ErrCode != 0 {
		return fmt.Errorf(errCodeMsg(r.ErrCode))
	}

	if r.Value == nil {
		fmt.Println("<nil>")
	} else {
		fmt.Printf("[%d\t%q]\n", r.Score, r.Value)
	}

	return nil
}

func (c *client) set(zop bool, args []string) error {
	// set <tableId> <rowKey> <colKey> <value> [score]
	//zset <tableId> <rowKey> <colKey> <value> [score]
	if len(args) < 4 || len(args) > 5 {
		return fmt.Errorf("invalid number of arguments (%d)", len(args))
	}

	tableId, err := getTableId(args[0])
	if err != nil {
		return err
	}

	rowKey, err := extractString(args[1])
	if err != nil {
		return err
	}
	colKey, err := extractString(args[2])
	if err != nil {
		return err
	}
	value, err := extractString(args[3])
	if err != nil {
		return err
	}
	var score int64
	if len(args) >= 5 {
		score, err = strconv.ParseInt(args[4], 10, 64)
		if err != nil {
			return err
		}
	}

	var r *table.OneReply
	if zop {
		r, err = c.c.ZSet(tableId, []byte(rowKey), []byte(colKey), []byte(value), score, 0)
	} else {
		r, err = c.c.Set(tableId, []byte(rowKey), []byte(colKey), []byte(value), score, 0)
	}
	if err != nil {
		return err
	}
	if r.ErrCode != 0 {
		return fmt.Errorf(errCodeMsg(r.ErrCode))
	}

	fmt.Println("OK")
	return nil
}

func (c *client) del(zop bool, args []string) error {
	// del <tableId> <rowKey> <colKey>
	//zdel <tableId> <rowKey> <colKey>
	if len(args) != 3 {
		return fmt.Errorf("invalid number of arguments (%d)", len(args))
	}

	tableId, err := getTableId(args[0])
	if err != nil {
		return err
	}

	rowKey, err := extractString(args[1])
	if err != nil {
		return err
	}
	colKey, err := extractString(args[2])
	if err != nil {
		return err
	}

	if zop {
		_, err = c.c.ZDel(tableId, []byte(rowKey), []byte(colKey), 0)
	} else {
		_, err = c.c.Del(tableId, []byte(rowKey), []byte(colKey), 0)
	}
	if err != nil {
		return err
	}

	fmt.Println("OK")
	return nil
}

func (c *client) incr(zop bool, args []string) error {
	// incr <tableId> <rowKey> <colKey> [score]
	//zincr <tableId> <rowKey> <colKey> [score]
	if len(args) < 3 || len(args) > 4 {
		return fmt.Errorf("invalid number of arguments (%d)", len(args))
	}

	tableId, err := getTableId(args[0])
	if err != nil {
		return err
	}

	rowKey, err := extractString(args[1])
	if err != nil {
		return err
	}
	colKey, err := extractString(args[2])
	if err != nil {
		return err
	}
	var score int64 = 1
	if len(args) >= 4 {
		score, err = strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			return err
		}
	}

	var r *table.OneReply
	if zop {
		r, err = c.c.ZIncr(tableId, []byte(rowKey), []byte(colKey), score, 0)
	} else {
		r, err = c.c.Incr(tableId, []byte(rowKey), []byte(colKey), score, 0)
	}
	if err != nil {
		return err
	}
	if r.ErrCode != 0 {
		return fmt.Errorf("error code %d", r.ErrCode)
	}

	fmt.Printf("[%d\t%q]\n", r.Score, r.Value)
	return nil
}

func (c *client) scan(args []string) error {
	//scan <tableId> <rowKey> <colKey> [num]
	if len(args) < 3 || len(args) > 4 {
		return fmt.Errorf("invalid number of arguments (%d)", len(args))
	}

	tableId, err := getTableId(args[0])
	if err != nil {
		return err
	}

	rowKey, err := extractString(args[1])
	if err != nil {
		return err
	}
	colKey, err := extractString(args[2])
	if err != nil {
		return err
	}

	var num int64 = 10
	if len(args) >= 4 {
		num, err = strconv.ParseInt(args[3], 10, 16)
		if err != nil {
			return err
		}
	}

	r, err := c.c.Scan(tableId, []byte(rowKey), []byte(colKey), true, int(num))
	if err != nil {
		return err
	}

	if len(r.Reply) == 0 {
		fmt.Println("No record!")
	} else {
		for i := 0; i < len(r.Reply); i++ {
			var one = r.Reply[i]
			fmt.Printf("%2d) [%q\t%q]\t[%d\t%q]\n", i,
				one.RowKey, one.ColKey, one.Score, one.Value)
		}
	}

	return nil
}

func (c *client) zscan(args []string) error {
	//zscan <tableId> <rowKey> <score> <colKey> [num]
	if len(args) < 4 || len(args) > 5 {
		return fmt.Errorf("invalid number of arguments (%d)", len(args))
	}

	tableId, err := getTableId(args[0])
	if err != nil {
		return err
	}

	rowKey, err := extractString(args[1])
	if err != nil {
		return err
	}

	score, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return err
	}
	colKey, err := extractString(args[3])
	if err != nil {
		return err
	}

	var num int64 = 10
	if len(args) >= 5 {
		num, err = strconv.ParseInt(args[4], 10, 16)
		if err != nil {
			return err
		}
	}

	r, err := c.c.ZScan(tableId, []byte(rowKey), []byte(colKey), score,
		true, true, int(num))
	if err != nil {
		return err
	}

	if len(r.Reply) == 0 {
		fmt.Println("No record!")
	} else {
		for i := 0; i < len(r.Reply); i++ {
			var one = r.Reply[i]
			fmt.Printf("%2d) [%q\t%d\t%q]\t[%q]\n", i,
				one.RowKey, one.Score, one.ColKey, one.Value)
		}
	}

	return nil
}

func errCodeMsg(errCode uint8) string {
	switch errCode {
	case table.EcCasNotMatch:
		return "cas not match"
	case table.EcNoPrivilege:
		return "no access privilege"
	default:
		return fmt.Sprintf("error code %d", errCode)
	}
}

func getTableId(arg string) (uint8, error) {
	tableId, err := strconv.Atoi(arg)
	if err != nil {
		return 0, fmt.Errorf("<tableId> %s is not a number", arg)
	}

	if tableId < 0 || tableId > 255 {
		return 0, fmt.Errorf("<tableId> %s is out of range [0 ~ 255]", arg)
	}

	return uint8(tableId), nil
}

func getDatabaseId(arg string) (uint8, error) {
	dbId, err := strconv.Atoi(arg)
	if err != nil {
		return 0, fmt.Errorf("<databaseId> %s is not a number", arg)
	}

	if dbId == 0 {
		return 0, fmt.Errorf("database 0 is reserved for internal use only")
	}

	if dbId < 0 || dbId > 255 {
		return 0, fmt.Errorf("<databaseId> %s is out of range [1 ~ 255]", arg)
	}

	return uint8(dbId), nil
}

func extractString(arg string) (string, error) {
	if arg[0] == '\'' || arg[0] == '"' {
		if len(arg) < 2 {
			return "", fmt.Errorf("invalid string (%s)", arg)
		}

		if arg[0] != arg[len(arg)-1] {
			return "", fmt.Errorf("invalid string (%s)", arg)
		}

		return arg[1 : len(arg)-1], nil
	}

	return arg, nil
}

func extractQuote(arg string) (string, error) {
	if arg[0] != '\'' && arg[0] != '"' {
		return "", fmt.Errorf("not start with quote (%s)", arg)
	}

	if len(arg) < 2 {
		return "", fmt.Errorf("invalid string (%s)", arg)
	}

	if arg[0] != arg[len(arg)-1] {
		return "", fmt.Errorf("invalid string (%s)", arg)
	}

	return arg[1 : len(arg)-1], nil
}
