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

#ifndef _GO_TABLE_CODEC_H_
#define _GO_TABLE_CODEC_H_

#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include "slice.h"
#include "proto.h"

namespace gotable {

using std::string;
using std::vector;

// CtrlFlag
enum {
	CtrlErrCode  = 0x1,  // Response Error Code
	CtrlCas      = 0x2,  // Compare And Switch
	CtrlColSpace = 0x4,
	CtrlValue    = 0x8,
	CtrlScore    = 0x10,
};

enum {
	ColSpaceDefault = 0,  // Default column space
	ColSpaceScore1  = 1,  // rowKey+score+colKey => value
	ColSpaceScore2  = 2,  // rowKey+colKey => value+score
};

// KeyValue=cCtrlFlag+cTableId+[cErrCode]+[cColSpace]
//         +cRowKeyLen+sRowKey+wColKeyLen+sColKey
//         +[dwValueLen+sValue]+[ddwScore]+[dwCas]
struct KeyValue {
	uint8_t  ctrlFlag;
	int8_t   errCode;   // default: 0 if missing
	uint8_t  colSpace;  // default: 0 if missing
	uint8_t  tableId;
	Slice    rowKey;
	Slice    colKey;
	Slice    value;     // default: empty if missing
	int64_t  score;     // default: 0 if missing
	uint32_t cas;       // default: 0 if missing

	KeyValue() : ctrlFlag(0), errCode(0), colSpace(0), tableId(0), rowKey(), colKey(),
			value(), score(0), cas(0) {}

	int length();
	int decode(const char* pkg, int len);
	int encode(char* pkg, int len);

	void setErrCode(int8_t errCode) {
		this->errCode = errCode;
		if(errCode != 0) {
			this->ctrlFlag |= CtrlErrCode;
		} else {
			this->ctrlFlag &= (~CtrlErrCode);
		}
	}

	void setColSpace(uint8_t colSpace) {
		this->colSpace = colSpace;
		if(colSpace != 0) {
			this->ctrlFlag |= CtrlColSpace;
		} else {
			this->ctrlFlag &= (~CtrlColSpace);
		}
	}

	void setCas(uint32_t cas) {
		this->cas = cas;
		if(cas != 0) {
			this->ctrlFlag |= CtrlCas;
		} else {
			this->ctrlFlag &= (~CtrlCas);
		}
	}

	void setValue(const string& value) {
		this->value = value;
		if(value.length() != 0) {
			this->ctrlFlag |= CtrlValue;
		} else {
			this->ctrlFlag &= (~CtrlValue);
		}
	}

	void setScore(int64_t score) {
		this->score = score;
		if(score != 0) {
			this->ctrlFlag |= CtrlScore;
		} else {
			this->ctrlFlag &= (~CtrlScore);
		}
	}
};

// PkgFlag
enum {
	FlagZop       = 0x1,  // if set, it is a "Z" op
	FlagAscending = 0x2,  // if set, Scan in ASC order, else DESC order
	FlagStart     = 0x4,  // if set, Scan start from MIN/MAX key
	FlagEnd       = 0x8,  // if set, Scan/Dump finished, stop now
	FlagOneTable  = 0x10, // if set, Dump only one table, else Dump full DB(dbId)
	FlagUnitStart = 0x20, // if set, Dump start from new UnitId, else from pivot record
};

// Get, Set, Del, GetSet, GetDel, ZGet, ZSet, Sync
// PKG=HEAD+cPkgFlag+KeyValueCtrl
struct PkgOneOp : public PkgHead {
	uint8_t  pkgFlag;
	KeyValue kv;

	PkgOneOp() : pkgFlag(0), kv() {}

	int length();
	int decode(const char* pkg, int len);
	int encode(char* pkg, int len);
};

// MGet, MSet, MDel, MZGet, MZSet, MZDel
// PKG=HEAD+cPkgFlag+cErrCode+wNum+KeyValueCtrl[wNum]
struct PkgMultiOp : public PkgHead {
	uint8_t          pkgFlag;
	int8_t           errCode;
	vector<KeyValue> kvs;

	PkgMultiOp() : pkgFlag(0), errCode(0) {}

	int length();
	int decode(const char* pkg, int len);
	int encode(char* pkg, int len);
};

// Scan, ZScan
// PKG=PkgOneOp+wNum
struct PkgScanReq : public PkgOneOp {
	uint16_t num;

	PkgScanReq() : num(0) {}

	int length();
	int decode(const char* pkg, int len);
	int encode(char* pkg, int len);
};

// Scan, ZScan
typedef PkgMultiOp PkgScanResp;

// Dump
// PKG=PkgOneOp+wStartUnitId+wEndUnitId
struct PkgDumpReq : public PkgOneOp {
	uint16_t startUnitId; // Dump start unit ID (included)
	uint16_t endUnitId;   // Dump finish unit ID (included)

	PkgDumpReq() : startUnitId(0), endUnitId(0) {}

	int length();
	int decode(const char* pkg, int len);
	int encode(char* pkg, int len);
};

// Dump
// PKG=PkgMultiOp+wStartUnitId+wEndUnitId+wLastUnitId
struct PkgDumpResp : public PkgMultiOp {
	uint16_t startUnitId;
	uint16_t endUnitId;
	uint16_t lastUnitId; // Last Unit ID tried to dump

	PkgDumpResp() : startUnitId(0), endUnitId(0), lastUnitId(0) {}

	int length();
	int decode(const char* pkg, int len);
	int encode(char* pkg, int len);
};

}
#endif
