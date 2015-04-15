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

package table

import (
	"math/rand"
	"net"
	"sync"
	"time"
)

const (
	statusOk = iota
	statusErr
)

type Addr struct {
	Network string
	Address string
}

type Pool struct {
	as      []Addr // Server address list, never change once assigned
	connNum int

	mtx      sync.Mutex
	status   []int
	cs       []*Client
	lastAddr int // Last as index
	lastCli  int // Last cs index
	closed   bool
}

func NewPool(as []Addr, connNum int) *Pool {
	var p = new(Pool)
	p.connNum = connNum
	p.as = as
	for i := 0; i < len(as); i++ {
		p.status = append(p.status, statusOk)
	}
	p.lastAddr = rand.Int() % len(as)

	go p.goPingDeamon()
	return p
}

func (p *Pool) Get() (*Client, error) {
	p.mtx.Lock()
	if p.closed {
		p.mtx.Unlock()
		return nil, ErrClosedPool
	}

	cliNum := len(p.cs)
	if cliNum >= p.connNum && cliNum > 0 {
		p.lastCli = (p.lastCli + 1) % cliNum
		var c = p.cs[p.lastCli]
		p.mtx.Unlock()
		return c, nil
	}

	defer p.mtx.Unlock()

	var lastAddr = p.lastAddr
	for i := 0; i < len(p.as); i++ {
		lastAddr = (lastAddr + 1) % len(p.as)
		if p.status[lastAddr] == statusOk {
			var addr = p.as[lastAddr]
			c, err := Dial(addr.Network, addr.Address)
			if err != nil {
				p.status[lastAddr] = statusErr
				continue
			}
			c.p = p
			p.cs = append(p.cs, c)
			p.lastAddr = lastAddr
			return c, nil
		}
	}

	return nil, ErrNoValidAddr
}

func (p *Pool) Close() {
	p.mtx.Lock()
	if p.closed {
		p.mtx.Unlock()
		return
	}
	p.closed = true
	var cs = make([]*Client, len(p.cs))
	copy(cs, p.cs)
	p.cs = nil
	p.mtx.Unlock()

	for _, c := range cs {
		if c != nil {
			c.doClose()
		}
	}
}

func (p *Pool) remove(cli *Client) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	if p.closed {
		return
	}

	for i, c := range p.cs {
		if c == cli {
			copy(p.cs[i:], p.cs[i+1:])
			p.cs = p.cs[:len(p.cs)-1]
			return
		}
	}
}

func (p *Pool) goPingDeamon() {
	for !p.closed {
		time.Sleep(time.Second)
		for i := 0; i < len(p.as); i++ {
			p.mtx.Lock()
			var st = p.status[i]
			p.mtx.Unlock()
			if st != statusOk {
				var addr = p.as[i]
				c, err := net.DialTimeout(addr.Network, addr.Address, time.Second)
				if err == nil {
					p.mtx.Lock()
					p.status[i] = statusOk
					p.mtx.Unlock()
					c.Close()
					continue
				}
			}
		}
	}
}
