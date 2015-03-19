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

#ifndef _GO_TABLE_PROTO_H_
#define _GO_TABLE_PROTO_H_

#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <arpa/inet.h>


#define htonll(x) gotable::switchll((x))
#define ntohll(x) gotable::switchll((x))

namespace gotable {

using std::string;


enum {
	// Front CTRL
	CmdAuth = 0x9,

	// Front Read
	CmdPing = 0x10,
	CmdGet  = 0x11,
	CmdMGet = 0x12,
	CmdScan = 0x13,
	CmdDump = 0x14,

	// Front Write
	CmdSet   = 0x60,
	CmdMSet  = 0x61,
	CmdDel   = 0x62,
	CmdMDel  = 0x63,
	CmdIncr  = 0x64,
	CmdMIncr = 0x65,
};

enum {
	AdminDbId    = 255,
	HeadSize     = 14,
	MaxUint8     = 255,
	MaxUint16    = 65535,
	MaxValueLen  = 1024 * 1024,     // 1MB
	MaxPkgLen    = 1024 * 1024 * 2, // 2MB
};

// cCmd+cDbId+ddwSeq+dwPkgLen+sBody
struct PkgHead {
	uint8_t  cmd;
	uint8_t  dbId;
	uint64_t seq;     //normal: request seq; replication: master binlog seq
	uint32_t pkgLen;

	virtual ~PkgHead() {}
	virtual int length();
	virtual int decode(const char* pkg, int len);
	virtual int encode(char* pkg, int len);
};

void OverWriteLen(char* pkg, int pkgLen);
void OverWriteSeq(char* pkg, uint64_t seq);

int readPkg(int fd, char* buf, int bufLen, PkgHead* head, string& pkg);


inline uint64_t switchll(uint64_t x)
{
#if BYTE_ORDER == BIG_ENDIAN
	return x;
#elif BYTE_ORDER == LITTLE_ENDIAN
	return ((((x) & 0xff00000000000000llu) >> 56) |
			(((x) & 0x00ff000000000000llu) >> 40) |
			(((x) & 0x0000ff0000000000llu) >> 24) |
			(((x) & 0x0000ff0000000000llu) >> 24) |
			(((x) & 0x000000ff00000000llu) >> 8) |
			(((x) & 0x00000000ff000000llu) << 8) |
			(((x) & 0x0000000000ff0000llu) << 24) |
			(((x) & 0x000000000000ff00llu) << 40) |
			(((x) & 0x00000000000000ffllu) << 56));
#else
#error "What kind of system is this?"
#endif
}

inline void putUint16(char* pkg, uint16_t a) {
	*(uint16_t*)pkg = htons(a);
}

inline uint16_t getUint16(const char* pkg) {
	return ntohs(*(uint16_t*)pkg);
}

inline void putUint32(char* pkg, uint32_t a) {
	*(uint32_t*)pkg = htonl(a);
}

inline uint32_t getUint32(const char* pkg) {
	return ntohl(*(uint32_t*)pkg);
}

inline void putUint64(char* pkg, uint64_t a) {
	*(uint64_t*)pkg = htonll(a);
}

inline uint64_t getUint64(const char* pkg) {
	return ntohll(*(uint64_t*)pkg);
}

}
#endif
