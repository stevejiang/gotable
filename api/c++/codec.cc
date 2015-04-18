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

#include <string.h>
#include "proto.h"
#include "codec.h"

namespace gotable {


int KeyValue::length() {
	// KeyValue=cCtrlFlag+cTableId+[cErrCode]+[cColSpace]
	//         +cRowKeyLen+sRowKey+wColKeyLen+sColKey
	//         +[dwValueLen+sValue]+[ddwScore]+[dwCas]
	int n = 2;
	if((ctrlFlag&CtrlErrCode) != 0) {
		n += 1;
	}
	if((ctrlFlag&CtrlColSpace) != 0) {
		n += 1;
	}
	n += 1 + rowKey.size() + 2 + colKey.size();
	if((ctrlFlag&CtrlValue) != 0) {
		n += 4 + value.size();
	}
	if((ctrlFlag&CtrlScore) != 0) {
		n += 8;
	}
	if((ctrlFlag&CtrlCas) != 0) {
		n += 4;
	}
	return n;
}

int KeyValue::decode(const char* pkg, int pkgLen) {
	int n = 0;
	if(n+2 > pkgLen) {
		return -2;
	}
	ctrlFlag = pkg[n];
	n += 1;
	tableId = pkg[n];
	n += 1;

	if((ctrlFlag&CtrlErrCode) != 0) {
		if(n+1 > pkgLen) {
			return -3;
		}
		errCode = pkg[n];
		n += 1;
	} else {
		errCode = 0;
	}
	if((ctrlFlag&CtrlColSpace) != 0) {
		if(n+1 > pkgLen) {
			return -4;
		}
		colSpace = pkg[n];
		n += 1;
	} else {
		colSpace = 0;
	}

	if(n+1 > pkgLen) {
		return -5;
	}
	int rowKeyLen = int(pkg[n]);
	n += 1;
	if(n+rowKeyLen+2 > pkgLen) {
		return -6;
	}
	rowKey = Slice(pkg+n, rowKeyLen);
	n += rowKeyLen;

	int colKeyLen = int(getUint16(pkg+n));
	n += 2;
	if(n+colKeyLen > pkgLen) {
		return -7;
	}
	colKey = Slice(pkg+n, colKeyLen);
	n += colKeyLen;

	if((ctrlFlag&CtrlValue) != 0) {
		if(n+4 > pkgLen) {
			return -8;
		}
		int valueLen = int(getUint32(pkg+n));
		n += 4;
		if(n+valueLen > pkgLen) {
			return -9;
		}
		value = Slice(pkg+n, valueLen);
		n += valueLen;
	} else {
		value.clear();
	}
	if((ctrlFlag&CtrlScore) != 0) {
		if(n+8 > pkgLen) {
			return -10;
		}
		score = int64_t(getUint64(pkg+n));
		n += 8;
	} else {
		score = 0;
	}
	if((ctrlFlag&CtrlCas) != 0) {
		if(n+4 > pkgLen) {
			return -11;
		}
		cas = getUint32(pkg+n);
		n += 4;
	} else {
		cas = 0;
	}
	return n;
}

int KeyValue::encode(char* pkg, int pkgLen) {
	if(pkgLen < length()) {
		return -2;
	}
	if(rowKey.size() > MaxUint8) {
		return -3;
	}
	if(colKey.size() > MaxUint16) {
		return -4;
	}

	int n = 0;
	pkg[n] = ctrlFlag;
	n += 1;
	pkg[n] = tableId;
	n += 1;
	if((ctrlFlag&CtrlErrCode) != 0) {
		pkg[n] = errCode;
		n += 1;
	}
	if((ctrlFlag&CtrlColSpace) != 0) {
		pkg[n] = colSpace;
		n += 1;
	}

	pkg[n] = uint8_t(rowKey.size());
	n += 1;
	memcpy(pkg+n, rowKey.data(), rowKey.size());
	n += rowKey.size();

	putUint16(pkg+n, uint16_t(colKey.size()));
	n += 2;
	memcpy(pkg+n, colKey.data(), colKey.size());
	n += colKey.size();

	if((ctrlFlag&CtrlValue) != 0) {
		putUint32(pkg+n, uint32_t(value.size()));
		n += 4;
		memcpy(pkg+n, value.data(), value.size());
		n += value.size();
	}
	if((ctrlFlag&CtrlScore) != 0) {
		putUint64(pkg+n, uint64_t(score));
		n += 8;
	}
	if((ctrlFlag&CtrlCas) != 0) {
		putUint32(pkg+n, cas);
		n += 4;
	}
	return n;
}

int PkgOneOp::length() {
	// PKG = HEAD+cPkgFlag+KeyValue
	return HeadSize + 1 + KeyValue::length();
}

int PkgOneOp::decode(const char* pkg, int pkgLen) {
	int n= PkgHead::decode(pkg, pkgLen);
	if(n < 0){
		return -2;
	}

	if(n+1 > pkgLen) {
		return -3;
	}
	pkgFlag = pkg[n];
	n += 1;

	int m = KeyValue::decode(pkg+n, pkgLen-n);
	if(m < 0){
		return -4;
	}
	n += m;

	return n;
}

int PkgOneOp::encode(char* pkg, int pkgLen) {
	int n = PkgHead::encode(pkg, pkgLen);
	if(n < 0) {
		return -2;
	}

	if(n+1 > pkgLen) {
		return -3;
	}
	pkg[n] = pkgFlag;
	n += 1;

	int m = KeyValue::encode(pkg+n, pkgLen-n);
	if(m < 0) {
		return -4;
	}
	n += m;

	overWriteLen(pkg, n);
	return n;
}

int PkgMultiOp::length() {
	// PKG = HEAD+cPkgFlag+cErrCode+wNum+KeyValue[wNum]
	int n = HeadSize + 4;
	for(int i = 0; i < kvs.size(); i++) {
		n += kvs[i].length();
	}
	return n;
}

int PkgMultiOp::decode(const char* pkg, int pkgLen) {
	int n = PkgHead::decode(pkg, pkgLen);
	if(n < 0) {
		return -2;
	}

	if(n+4 > pkgLen) {
		return -3;
	}
	pkgFlag = pkg[n];
	n += 1;
	errCode = pkg[n];
	n += 1;
	int numKvs = int(getUint16(pkg+n));
	n += 2;

	kvs.resize(numKvs);
	for(int i = 0; i < numKvs; i++) {
		int m = kvs[i].decode(pkg+n, pkgLen-n);
		if(m < 0) {
			return -4;
		}
		n += m;
	}

	return n;
}

int PkgMultiOp::encode(char* pkg, int pkgLen) {
	int numKvs = kvs.size();
	if(numKvs > MaxUint16) {
		return -2;
	}

	int n = PkgHead::encode(pkg, pkgLen);
	if(n < 0) {
		return -3;
	}

	if(n+4 > pkgLen) {
		return -5;
	}
	pkg[n] = pkgFlag;
	n += 1;
	pkg[n] = errCode;
	n += 1;
	putUint16(pkg+n, uint16_t(numKvs));
	n += 2;

	for(int i = 0; i < numKvs; i++) {
		int m = kvs[i].encode(pkg+n, pkgLen-n);
		if(m < 0) {
			return -6;
		}
		n += m;
	}

	overWriteLen(pkg, n);
	return n;
}

int PkgScanReq::length() {
	// PKG=PkgOneOp+wNum
	return PkgOneOp::length() + 2;
}

int PkgScanReq::decode(const char* pkg, int pkgLen) {
	int n = PkgOneOp::decode(pkg, pkgLen);
	if(n < 0) {
		return -2;
	}

	if(n+2 > pkgLen) {
		return -3;
	}
	num = getUint16(pkg+n);
	n += 2;

	return n;
}

int PkgScanReq::encode(char* pkg, int pkgLen) {
	int n= PkgOneOp::encode(pkg, pkgLen);
	if(n < 0) {
		return -2;
	}

	if(n+2 > pkgLen) {
		return -3;
	}
	putUint16(pkg+n, num);
	n += 2;

	overWriteLen(pkg, n);
	return n;
}

int PkgDumpReq::length() {
	// PKG=PkgOneOp+wStartUnitId+wEndUnitId
	return PkgOneOp::length() + 4;
}

int PkgDumpReq::decode(const char* pkg, int pkgLen) {
	int n = PkgOneOp::decode(pkg, pkgLen);
	if(n < 0) {
		return -2;
	}

	if(n+4 > pkgLen) {
		return -3;
	}
	startUnitId = getUint16(pkg+n);
	n += 2;
	endUnitId = getUint16(pkg+n);
	n += 2;

	return n;
}

int PkgDumpReq::encode(char* pkg, int pkgLen) {
	int n = PkgOneOp::encode(pkg, pkgLen);
	if(n < 0) {
		return -2;
	}

	if(n+4 > pkgLen) {
		return -3;
	}
	putUint16(pkg+n, startUnitId);
	n += 2;
	putUint16(pkg+n, endUnitId);
	n += 2;

	overWriteLen(pkg, n);
	return n;
}

int PkgDumpResp::length() {
	// PKG=PkgMultiOp+wStartUnitId+wEndUnitId+wLastUnitId
	return PkgMultiOp::length() + 6;
}

int PkgDumpResp::decode(const char* pkg, int pkgLen) {
	int n = PkgMultiOp::decode(pkg, pkgLen);
	if(n < 0) {
		return -2;
	}

	if(n+6 > pkgLen) {
		return -3;
	}
	startUnitId = getUint16(pkg+n);
	n += 2;
	endUnitId = getUint16(pkg+n);
	n += 2;
	lastUnitId = getUint16(pkg+n);
	n += 2;

	return n;
}

int PkgDumpResp::encode(char* pkg, int pkgLen) {
	int n = PkgMultiOp::encode(pkg, pkgLen);
	if(n < 0) {
		return -2;
	}

	if(n+6 > pkgLen) {
		return -3;
	}
	putUint16(pkg+n, startUnitId);
	n += 2;
	putUint16(pkg+n, endUnitId);
	n += 2;
	putUint16(pkg+n, lastUnitId);
	n += 2;

	overWriteLen(pkg, n);
	return n;
}

} // namespace gotable
