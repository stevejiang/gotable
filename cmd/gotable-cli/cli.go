package main

import (
	"bufio"
	"fmt"
	"github.com/stevejiang/gotable/proto"
	"net"
	"strconv"
)

type client struct {
	c       net.Conn
	r       *bufio.Reader
	w       *bufio.Writer
	headBuf []byte
	head    proto.PkgHead
	dbId    uint8
}

func newClient() *client {
	link, err := net.Dial("tcp", *host)
	if err != nil {
		fmt.Println("dial failed: ", err)
		return nil
	}

	return &client{link, bufio.NewReader(link), bufio.NewWriter(link),
		make([]byte, proto.HeadSize), proto.PkgHead{}, 0}
}

func (c *client) do(cmd string, args []string) {
	fmt.Println(args)
}

func (c *client) use(args []string) error {
	//use <databaseId>
	if len(args) != 1 {
		return fmt.Errorf("Invalid number of arguments (%d)", len(args))
	}

	dbId, err := getDatabaseId(args[0])
	if err != nil {
		return err
	}

	fmt.Printf("OK, current database is %d\n", dbId)
	c.dbId = dbId

	return nil
}

func (c *client) get(args []string) error {
	//get <tableId> <rowKey> <colKey>
	if len(args) != 3 {
		return fmt.Errorf("Invalid number of arguments (%d)", len(args))
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

	var req proto.PkgCmdGetReq
	req.Cmd = proto.CmdGet
	req.DbId = c.dbId
	req.TableId = tableId
	req.RowKey = []byte(rowKey)
	req.ColKey = []byte(colKey)

	var pkg []byte
	_, err = req.Encode(&pkg)
	if err != nil {
		return err
	}

	_, err = c.w.Write(pkg)
	if err != nil {
		fmt.Printf("Write failed: %s\n", err)
		return err
	}
	c.w.Flush()

	pkg, err = proto.ReadPkg(c.r, c.headBuf, &c.head, nil)
	if err != nil {
		return err
	}

	var resp proto.PkgCmdGetResp
	resp.Decode(pkg)

	switch resp.ErrCode {
	case proto.EcodeOk:
		fmt.Printf("%q\n", resp.Value)
	case proto.EcodeNotExist:
		fmt.Println("(nil)")
	default:
		return fmt.Errorf("GET failed with error code %d", resp.ErrCode)
	}

	return nil
}

func (c *client) set(args []string) error {
	//set <tableId> <rowKey> <colKey> <value>
	if len(args) != 4 {
		return fmt.Errorf("Invalid number of arguments (%d)", len(args))
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

	var req proto.PkgCmdPutReq
	req.Cmd = proto.CmdPut
	req.DbId = c.dbId
	req.TableId = tableId
	req.RowKey = []byte(rowKey)
	req.ColKey = []byte(colKey)
	req.Value = []byte(value)

	var pkg []byte
	_, err = req.Encode(&pkg)
	if err != nil {
		return err
	}

	_, err = c.w.Write(pkg)
	if err != nil {
		fmt.Printf("Write failed: %s\n", err)
		return err
	}
	c.w.Flush()

	pkg, err = proto.ReadPkg(c.r, c.headBuf, &c.head, nil)
	if err != nil {
		return err
	}

	var resp proto.PkgCmdPutResp
	resp.Decode(pkg)

	if resp.ErrCode != proto.EcodeOk {
		return fmt.Errorf("GET failed with error code %d", resp.ErrCode)
	}

	fmt.Println("OK")

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
