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
	"log"
	"net"
	"sync"
	"time"
)

const (
	maxTagId = 5
)

const (
	statusOk = iota
	statusErr
)

type Addr struct {
	Tag     uint8
	Network string
	Address string
}

type poolAddr struct {
	tag      uint8
	as       []Addr
	status   []int
	cs       []*Client
	lastAddr int // last as index
	lastCli  int // last cs index
}

type Pool struct {
	connNum int

	mtx    sync.Mutex
	pas    []*poolAddr
	tags   []uint8
	tagIdx int
	closed bool
}

func NewPool(as []Addr, connNum int) *Pool {
	var p = new(Pool)
	p.connNum = connNum
	p.pas = make([]*poolAddr, maxTagId+1)
	for _, t := range as {
		if t.Tag > 0 && t.Tag <= maxTagId {
			p.tags = append(p.tags, t.Tag)

			var a = new(poolAddr)
			a.tag = t.Tag
			a.as = append(a.as, t)
			a.status = append(a.status, statusOk)
			p.pas[t.Tag] = a
		} else {
			log.Printf("Invalid tag %d\n", t.Tag)
			return nil
		}
	}

	go p.goPingDeamon()
	return p
}

func (p *Pool) Get(tag uint8) (*Client, error) {
	if tag > maxTagId {
		return nil, ErrInvalidTag
	}

	p.mtx.Lock()
	if p.closed {
		p.mtx.Unlock()
		return nil, ErrClosedPool
	}

	var reqTag = tag
	if tag == 0 {
		if p.tags == nil {
			p.mtx.Unlock()
			return nil, ErrNoValidAddr
		}
		p.tagIdx = (p.tagIdx + 1) % len(p.tags)
		tag = p.tags[p.tagIdx]
	}

	var a = p.pas[tag]
	if a == nil {
		p.mtx.Unlock()
		return nil, ErrInvalidTag
	}

	if len(a.cs) >= p.connNum && p.connNum > 0 {
		a.lastCli = (a.lastCli + 1) % len(a.cs)
		var c = a.cs[a.lastCli]
		p.mtx.Unlock()
		return c, nil
	}

	var c *Client
	var err error
	var lastAddr = a.lastAddr
	for i := 0; i < len(a.as); i++ {
		lastAddr = (lastAddr + 1) % len(a.as)
		if a.status[lastAddr] == statusOk {
			var addr = a.as[lastAddr]

			c, err = Dial(addr.Network, addr.Address)
			if err != nil {
				a.status[lastAddr] = statusErr
				continue
			}
			c.p = p

			a.cs = append(a.cs, c)
			a.lastAddr = lastAddr

			p.mtx.Unlock()
			//log.Printf("create a new connection (%d, %d)\n", len(a.cs), p.connNum)
			return c, nil
		}
	}

	p.mtx.Unlock()

	if reqTag == 0 {
		// try other tags
		for _, t := range p.pas {
			if t == nil {
				continue
			}
			if t.tag > 0 && t.tag != tag {
				c, err = p.Get(t.tag)
				if err == nil {
					return c, nil
				}
			}
		}
	}

	return nil, ErrNoValidAddr
}

func (p *Pool) Close() {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	if !p.closed {
		p.closed = true
	} else {
		return
	}

	for _, a := range p.pas {
		if a == nil {
			continue
		}
		for _, c := range a.cs {
			if c != nil {
				c.doClose()
				c.p = nil
			}
		}
		a.cs = nil
	}
}

func (p *Pool) remove(cli *Client) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	if p.closed {
		return
	}

	for _, a := range p.pas {
		if a == nil {
			continue
		}
		for i, c := range a.cs {
			if c == cli {
				copy(a.cs[i:], a.cs[i+1:])
				a.cs = a.cs[:len(a.cs)-1]
				return
			}
		}
	}
}

func (p *Pool) put(c *Client) error {
	c.mtx.Lock()
	if c.shutdown {
		c.mtx.Unlock()
		return c.doClose()
	}
	c.mtx.Unlock()
	return nil
}

func (p *Pool) goPingDeamon() {
	for !p.closed {
		time.Sleep(time.Second)
		for _, a := range p.pas {
			if a == nil {
				continue
			}
			for i := 0; i < len(a.as); i++ {
				p.mtx.Lock()
				if a.status[i] != statusOk {
					p.mtx.Unlock()
					var addr = a.as[i]

					c, err := net.Dial(addr.Network, addr.Address)
					if err == nil {
						p.mtx.Lock()
						a.status[i] = statusOk
						p.mtx.Unlock()
						c.Close()
						continue
					}
				}
				p.mtx.Unlock()
			}
		}
	}
}
