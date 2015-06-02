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

#include <unistd.h>
#include <string.h>
#include "proto.h"

namespace gotable {

static int8_t calHeadCrc(const char* pkg) {
	int8_t crc = 10;
	for (int i = 1; i < HeadSize; i++) {
		crc += pkg[i];
	}
	return crc;
}

int PkgHead::length() {
	return HeadSize;
}

int PkgHead::decode(const char* pkg, int len) {
	if(len < HeadSize) {
		return -1;
	}

	crc = calHeadCrc(pkg);
	if (crc != pkg[0]) {
		return -2;
	}
	
	cmd = pkg[1];
	dbId = pkg[2];
	seq = getUint64(pkg+3);
	pkgLen = getUint32(pkg+11);

	return HeadSize;
}

int PkgHead::encode(char* pkg, int len) {
	if(len < HeadSize) {
		return -1;
	}

	pkg[1] = cmd;
	pkg[2] = dbId;
	putUint64(pkg+3, seq);
	putUint32(pkg+11, pkgLen);
	pkg[0] = calHeadCrc(pkg);

	return HeadSize;
}

void overWriteLen(char* pkg, int pkgLen) {
	putUint32(pkg+11, uint32_t(pkgLen));
	pkg[0] = calHeadCrc(pkg);
}

void overWriteSeq(char* pkg, uint64_t seq) {
	putUint64(pkg+3, seq);
	pkg[0] = calHeadCrc(pkg);
}

int readPkg(int fd, char* buf, int bufLen, PkgHead* head, string& pkg) {
	pkg.clear();

	if(buf == NULL || bufLen < HeadSize || head == NULL) {
		return -2;
	}

	int readLen = 0;
	while(true) {
		int n = read(fd, buf+readLen, HeadSize-readLen);
		if (n <= 0) {
			return n;
		}

		readLen += n;

		if(readLen < HeadSize) {
			continue;
		}

		int err = head->decode(buf, HeadSize);
		if(err < 0) {
			return err;
		}

		int pkgLen = int(head->pkgLen);
		if(pkgLen > MaxPkgLen) {
			return -3;
		}

		pkg.append(buf, HeadSize);

		while(readLen < pkgLen) {
			int curLen = pkgLen-readLen;
			if(curLen > bufLen) {
				curLen = bufLen;
			}
			n = read(fd, buf, curLen);
			if (n <= 0) {
				return n;
			}
			readLen += n;

			pkg.append(buf, n);
		}

		return pkgLen;
	}
}

}  // namespace gotable
