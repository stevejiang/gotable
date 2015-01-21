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

package ctrl

import (
	"bytes"
	"encoding/json"
	"github.com/stevejiang/gotable/api/go/table/proto"
)

type Decoder struct {
	de *json.Decoder
	rb bytes.Buffer
}

type Encoder struct {
	en *json.Encoder
	wb bytes.Buffer
}

func NewDecoder() *Decoder {
	var cj = new(Decoder)
	cj.de = json.NewDecoder(&cj.rb)
	return cj
}

func (cj *Decoder) Decode(pkg []byte, head *proto.PkgHead, v interface{}) error {
	if len(pkg) < proto.HeadSize {
		return proto.ErrPkgLen
	}

	var err error
	if head != nil {
		_, err = head.Decode(pkg)
		if err != nil {
			return err
		}
	}

	cj.rb.Reset()
	cj.rb.Write(pkg[proto.HeadSize:])
	err = cj.de.Decode(v)
	if err != nil {
		cj.rb.Reset()
		cj.de = json.NewDecoder(&cj.rb)
		return err
	}
	return nil
}

func NewEncoder() *Encoder {
	var cj = new(Encoder)
	cj.en = json.NewEncoder(&cj.wb)
	return cj
}

func (cj *Encoder) Encode(cmd, dbId uint8, seq uint64, v interface{}) ([]byte, error) {
	cj.wb.Reset()
	var err = cj.en.Encode(v)
	if err != nil {
		cj.wb.Reset()
		cj.en = json.NewEncoder(&cj.wb)
		return nil, err
	}

	var pkgLen = proto.HeadSize + cj.wb.Len()
	var head proto.PkgHead
	head.Cmd = cmd
	head.DbId = dbId
	head.Seq = seq
	head.PkgLen = uint32(pkgLen)

	var pkg = make([]byte, pkgLen)
	_, err = head.Encode(pkg)
	if err != nil {
		return nil, err
	}

	copy(pkg[proto.HeadSize:], cj.wb.Bytes())
	return pkg, nil
}
