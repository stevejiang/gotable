package server

import (
	"bufio"
	"github.com/stevejiang/gotable/proto"
	"log"
	"net"
	"sync/atomic"
)

const (
	ClientTypeNormal = iota
	ClientTypeMaster
	ClientTypeSlaver
)

type request struct {
	cli *Client
	cmd uint8
	seq uint64
	pkg []byte
}

type response struct {
	cmd uint8
	seq uint64
	pkg []byte
}

type RequestChan struct {
	ReadReqChan  chan *request
	WriteReqChan chan *request
	SyncReqChan  chan *request
}

type Client struct {
	cliType  int
	c        net.Conn
	r        *bufio.Reader
	w        *bufio.Writer
	respChan chan *response
	ms       *master

	//atomic
	closed uint32
}

func NewClient(conn net.Conn) *Client {
	cli := new(Client)
	cli.cliType = ClientTypeNormal
	cli.c = conn
	cli.r = bufio.NewReader(conn)
	cli.w = bufio.NewWriter(conn)
	cli.respChan = make(chan *response, 64)
	return cli
}

func (cli *Client) AddResp(resp *response) {
	if !cli.IsClosed() {
		defer recover()
		cli.respChan <- resp
	}
}

func (cli *Client) Close() {
	if !cli.IsClosed() {
		atomic.AddUint32(&cli.closed, 1)

		cli.c.Close()

		var ms = cli.ms
		if ms != nil {
			ms.Close()
			cli.ms = nil
		}

		defer recover()
		close(cli.respChan)
	}
}

func (cli *Client) IsClosed() bool {
	return atomic.LoadUint32(&cli.closed) > 0
}

func (cli *Client) SetMaster(ms *master) {
	cli.ms = ms
}

func (cli *Client) GoReadRequest(ch *RequestChan) {
	var headBuf = make([]byte, proto.HeadSize)
	var head proto.PkgHead
	for {
		pkg, err := proto.ReadPkg(cli.r, headBuf, &head, nil)
		if err != nil {
			log.Printf("ReadPkg failed: %s\n", err)
			cli.Close()
			return
		}

		var req = request{cli, head.Cmd, head.Seq, pkg}

		switch head.Cmd {
		case proto.CmdMaster:
			fallthrough
		case proto.CmdPing:
			fallthrough
		case proto.CmdGet:
			ch.ReadReqChan <- &req
		case proto.CmdSync:
			fallthrough
		case proto.CmdPut:
			ch.WriteReqChan <- &req
		default:
			cli.Close()
			log.Printf("Invalid cmd %x\n", head.Cmd)
			return
		}
	}
}

func (cli *Client) GoSendResponse() {
	for {
		select {
		case resp, ok := <-cli.respChan:
			if !ok {
				log.Printf("channel closed %p\n", cli)
				return
			}

			if !cli.IsClosed() {
				_, err := cli.w.Write(resp.pkg)
				if err != nil {
					continue
				}
				cli.w.Flush()
			}
		}
	}
}
