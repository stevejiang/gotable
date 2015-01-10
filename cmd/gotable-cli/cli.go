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
	"strconv"
)

type client struct {
	c *table.Client
}

func newClient() *client {
	var c = new(client)
	var err error
	c.c, err = table.Dial("tcp", *host)
	if err != nil {
		fmt.Println("dial failed: ", err)
		return nil
	}

	return c
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

	fmt.Printf("OK, current database is %d\n", dbId)
	c.c.Use(dbId)

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

	rowKey, err := extractKey(args[1])
	if err != nil {
		return err
	}
	colKey, err := extractKey(args[2])
	if err != nil {
		return err
	}

	var r *table.OneReply
	var oa = table.OneArgs{tableId, []byte(rowKey), []byte(colKey), nil, 0, 0}
	if zop {
		r, err = c.c.ZGet(&oa)
	} else {
		r, err = c.c.Get(&oa)
	}
	if err != nil {
		return err
	}

	switch r.ErrCode {
	case table.EcodeOk:
		fmt.Printf("[%d\t%q]\n", r.Score, r.Value)
	case table.EcodeNotExist:
		fmt.Println("(nil)")
	default:
		fmt.Printf("<Unexpected error code %d>\n", r.ErrCode)
	}

	return nil
}

func (c *client) set(zop bool, args []string) error {
	// set <tableId> <rowKey> <colKey> <value> [score]
	//zset <tableId> <rowKey> <colKey> <value> [score]
	if len(args) < 4 && len(args) > 5 {
		return fmt.Errorf("invalid number of arguments (%d)", len(args))
	}

	tableId, err := getTableId(args[0])
	if err != nil {
		return err
	}

	rowKey, err := extractKey(args[1])
	if err != nil {
		return err
	}
	colKey, err := extractKey(args[2])
	if err != nil {
		return err
	}
	value, err := extractKey(args[3])
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

	var oa = table.OneArgs{tableId, []byte(rowKey), []byte(colKey), []byte(value), score, 0}
	if zop {
		_, err = c.c.ZSet(&oa)
	} else {
		_, err = c.c.Set(&oa)
	}
	if err != nil {
		return err
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

	rowKey, err := extractKey(args[1])
	if err != nil {
		return err
	}
	colKey, err := extractKey(args[2])
	if err != nil {
		return err
	}

	var oa = table.OneArgs{tableId, []byte(rowKey), []byte(colKey), nil, 0, 0}
	if zop {
		_, err = c.c.ZDel(&oa)
	} else {
		_, err = c.c.Del(&oa)
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
	if len(args) < 3 && len(args) > 4 {
		return fmt.Errorf("invalid number of arguments (%d)", len(args))
	}

	tableId, err := getTableId(args[0])
	if err != nil {
		return err
	}

	rowKey, err := extractKey(args[1])
	if err != nil {
		return err
	}
	colKey, err := extractKey(args[2])
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
	var oa = table.OneArgs{tableId, []byte(rowKey), []byte(colKey), nil, score, 0}
	if zop {
		r, err = c.c.ZIncr(&oa)
	} else {
		r, err = c.c.Incr(&oa)
	}
	if err != nil {
		return err
	}

	switch r.ErrCode {
	case table.EcodeOk:
		fmt.Printf("[%d\t%q]\n", r.Score, r.Value)
	default:
		fmt.Printf("<Unexpected error code %d>\n", r.ErrCode)
	}
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

	rowKey, err := extractKey(args[1])
	if err != nil {
		return err
	}
	colKey, err := extractKey(args[2])
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

	var sa = table.ScanArgs{uint16(num), 0, table.OneArgs{tableId, []byte(rowKey),
		[]byte(colKey), nil, 0, 0}}
	r, err := c.c.Scan(&sa)
	if err != nil {
		return err
	}

	if len(r.Reply) == 0 {
		fmt.Println("no record!")
	} else {
		for i := 0; i < len(r.Reply); i++ {
			var one = &r.Reply[i]
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

	rowKey, err := extractKey(args[1])
	if err != nil {
		return err
	}

	score, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return err
	}
	colKey, err := extractKey(args[3])
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

	var sa = table.ScanArgs{uint16(num), 0, table.OneArgs{tableId, []byte(rowKey),
		[]byte(colKey), nil, score, 0}}
	r, err := c.c.ZScan(&sa)
	if err != nil {
		return err
	}

	if len(r.Reply) == 0 {
		fmt.Println("no record!")
	} else {
		for i := 0; i < len(r.Reply); i++ {
			var one = &r.Reply[i]
			fmt.Printf("%2d) [%q\t%d\t%q]\t[%q]\n", i,
				one.RowKey, one.Score, one.ColKey, one.Value)
		}
	}

	return nil
}

func getTableId(arg string) (uint8, error) {
	tableId, err := strconv.Atoi(arg)
	if err != nil {
		return 0, fmt.Errorf("<tableId> %s is not a number", arg)
	}

	if tableId < 0 || tableId > 200 {
		return 0, fmt.Errorf("<tableId> %s is out of range (0 ~ 200)", arg)
	}

	return uint8(tableId), nil
}

func getDatabaseId(arg string) (uint8, error) {
	dbId, err := strconv.Atoi(arg)
	if err != nil {
		return 0, fmt.Errorf("<databaseId> %s is not a number", arg)
	}

	if dbId < 0 || dbId > 200 {
		return 0, fmt.Errorf("<databaseId> %s is out of range (0 ~ 200)", arg)
	}

	return uint8(dbId), nil
}

func extractKey(arg string) (string, error) {
	if arg[0] == '\'' || arg[0] == '"' {
		if len(arg) < 2 {
			return "", fmt.Errorf("Invalid key (%s)", arg)
		}

		if arg[0] != arg[len(arg)-1] {
			return "", fmt.Errorf("Invalid key (%s)", arg)
		}

		return arg[1 : len(arg)-1], nil
	}

	return arg, nil
}
