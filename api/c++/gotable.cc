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

#include <stdio.h>
#include <errno.h>
#include <netdb.h>
#include <unistd.h>
#include <strings.h>
#include <string>

#include "codec.h"
#include "gotable.h"

namespace gotable {

using std::string;

static const string EMPTYSTR;

Client::Client(int fd) : closed(false), fd(fd), dbId(0), seq(0), authAdmin(false) {

}

Client::~Client() {
	close();
}

void Client::close() {
	if(!closed) {
		closed = true;
		::close(fd);
	}
}

void Client::select(uint8_t dbId) {
	this->dbId = dbId;
}

uint8_t Client::databaseId() {
	return dbId;
}

Client* Client::Dial(const char* ip, int port) {
	int s, rv;
	char sPort[16];
	struct addrinfo hints, *servinfo, *p;

	snprintf(sPort, sizeof(sPort), "%d", port);
	bzero(&hints, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;

	// Try with IPv6 if no IPv4 address was found. We do it in this order since
	// in a GoTable client you can't afford to test if you have IPv6 connectivity
	// as this would add latency to every connect. Otherwise a more sensible
	// route could be: Use IPv6 if both addresses are available and there is IPv6
	// connectivity.
	if ((rv = getaddrinfo(ip, sPort, &hints, &servinfo)) != 0) {
		hints.ai_family = AF_INET6;
		if ((rv = getaddrinfo(ip, sPort, &hints, &servinfo)) != 0) {
			return NULL;
		}
	}

	for (p = servinfo; p != NULL; p = p->ai_next) {
		if ((s = socket(p->ai_family,p->ai_socktype,p->ai_protocol)) == -1)
			continue;

		if (connect(s, p->ai_addr, p->ai_addrlen) == -1) {
			::close(s);
			continue;
		}

		return new Client(s);
	}

	return NULL;
}

int Client::doOneOp(bool zop, uint8_t cmd, uint8_t tableId,
		const string& rowKey, const string& colKey,
		const string& value, int64_t score, uint32_t cas,
		PkgOneOp* reply, string& pkg) {
	if(closed) {
		return -1;
	}

	seq++;

	PkgOneOp p;
	p.seq = seq;
	p.dbId = dbId;
	p.cmd = cmd;
	p.tableId = tableId;
	p.rowKey = rowKey;
	p.colKey = colKey;

	p.setCas(cas);
	p.setScore(score);
	p.setValue(value);

	// ZGet, ZSet, ZDel, ZIncr
	if(zop) {
		p.pkgFlag |=  FlagZop;
	}

	int pkgLen = p.length();
	if(pkgLen > MaxPkgLen) {
		return EcInvPkgLen;
	}

	pkg.resize(pkgLen);
	int n = p.encode((char*)pkg.data(), pkgLen);
	if(n < 0) {
		return -2;
	}

	// send pkg
	n = 0;
	while(n < pkgLen) {
		int m = write(fd, pkg.data()+n, pkgLen-n);
		if(m < 0) {
			return -3;
		}
		n += m;
	}

	// recv pkg
	PkgHead head;
	n = readPkg(fd, buf, sizeof(buf), &head, pkg);
	if(n < 0) {
		return -4;
	}
	if(n == 0) {
		this->close();
		return -5;
	}
	if(head.seq != seq) {
		this->close();
		return -6;
	}

	// reply
	n = reply->decode(pkg.data(), pkg.size());
	if(n < 0) {
		return -7;
	}

	return 0;
}

static inline void copyArgs(KeyValue& kv, const GetArgs& a) {
	kv.tableId = a.tableId;
	kv.rowKey = a.rowKey;
	kv.colKey = a.colKey;
	kv.setCas(a.cas);
}

static inline void copyArgs(KeyValue& kv, const SetArgs& a) {
	kv.tableId = a.tableId;
	kv.rowKey = a.rowKey;
	kv.colKey = a.colKey;
	kv.setCas(a.cas);
	kv.setScore(a.score);
	kv.setValue(a.value);
}

static inline void copyArgs(KeyValue& kv, const IncrArgs& a) {
	kv.tableId = a.tableId;
	kv.rowKey = a.rowKey;
	kv.colKey = a.colKey;
	kv.setCas(a.cas);
	kv.setScore(a.score);
}

static inline void copyReply(GetReply& r, const KeyValue& kv) {
	r.errCode = kv.errCode;
	r.tableId = kv.tableId;
	r.rowKey.assign(kv.rowKey.data(), kv.rowKey.size());
	r.colKey.assign(kv.colKey.data(), kv.colKey.size());
	r.value.assign(kv.value.data(), kv.value.size());
	r.score = kv.score;
	r.cas = kv.cas;
}

static inline void copyReply(SetReply& r, const KeyValue& kv) {
	r.errCode = kv.errCode;
	r.tableId = kv.tableId;
	r.rowKey.assign(kv.rowKey.data(), kv.rowKey.size());
	r.colKey.assign(kv.colKey.data(), kv.colKey.size());
}

static inline void copyReply(IncrReply& r, const KeyValue& kv) {
	r.errCode = kv.errCode;
	r.tableId = kv.tableId;
	r.rowKey.assign(kv.rowKey.data(), kv.rowKey.size());
	r.colKey.assign(kv.colKey.data(), kv.colKey.size());
	r.value.assign(kv.value.data(), kv.value.size());
	r.score = kv.score;
}

static inline void copyReply(ScanKV& r, const KeyValue& kv) {
	r.colKey.assign(kv.colKey.data(), kv.colKey.size());
	r.value.assign(kv.value.data(), kv.value.size());
	r.score = kv.score;
}

static inline void copyReply(DumpKV& r, const KeyValue& kv) {
	r.tableId = kv.tableId;
	r.colSpace = kv.colSpace;
	r.rowKey.assign(kv.rowKey.data(), kv.rowKey.size());
	r.colKey.assign(kv.colKey.data(), kv.colKey.size());
	r.value.assign(kv.value.data(), kv.value.size());
	r.score = kv.score;
}

template <typename T>
int Client::doMultiOp(bool zop, uint8_t cmd, const vector<T>& args,
		PkgMultiOp* reply, string& pkg) {
	if(closed) {
		return -1;
	}

	seq++;

	PkgMultiOp p;
	p.seq = seq;
	p.dbId = dbId;
	p.cmd = cmd;

	// ZGet, ZSet, ZDel, ZIncr
	if(zop) {
		p.pkgFlag |=  FlagZop;
	}

	p.kvs.resize(args.size());
	for(unsigned i = 0; i < args.size(); i++) {
		copyArgs(p.kvs[i], args[i]);
	}

	int pkgLen = p.length();
	if(pkgLen > MaxPkgLen) {
		return EcInvPkgLen;
	}

	pkg.resize(pkgLen);
	int n = p.encode((char*)pkg.data(), pkgLen);
	if(n < 0) {
		return -2;
	}

	// send pkg
	n = 0;
	while(n < pkgLen) {
		int m = write(fd, pkg.data()+n, pkgLen-n);
		if(m < 0) {
			return -3;
		}
		n += m;
	}

	// recv pkg
	PkgHead head;
	n = readPkg(fd, buf, sizeof(buf), &head, pkg);
	if(n < 0) {
		return -4;
	}
	if(n == 0) {
		this->close();
		return -5;
	}
	if(head.seq != seq) {
		this->close();
		return -6;
	}

	// reply
	n = reply->decode(pkg.data(), pkg.size());
	if(n < 0) {
		return -7;
	}

	return 0;
}

int Client::auth(const char* password) {
	if(authAdmin || setAuth.count(dbId)) {
		return 0;
	}

	string pkg;
	PkgOneOp reply;
	int err = doOneOp(false, CmdPing, 0, password, EMPTYSTR, EMPTYSTR, 0, 0,
			&reply, pkg);
	if(err < 0) {
		return err;
	}
	if(reply.errCode == 0) {
		if(reply.dbId == AdminDbId) {
			authAdmin = true;
		} else {
			setAuth.insert(reply.dbId);
		}
	}
	return reply.errCode;
}

int Client::ping() {
	string pkg;
	PkgOneOp reply;
	int err = doOneOp(false, CmdPing, 0, EMPTYSTR, EMPTYSTR, EMPTYSTR, 0, 0,
			&reply, pkg);
	if(err < 0) {
		return err;
	}
	return reply.errCode;
}

static inline int replyGet(string* value, int64_t* score, uint32_t* cas, PkgOneOp* reply) {
	// failed
	if(reply->errCode < 0) {
		if(value != NULL) {
			value->clear();
		}

		if(score != NULL) {
			*score = 0;
		}
		return reply->errCode;
	}

	if(value != NULL) {
		value->assign(reply->value.data(), reply->value.size());
	}

	if(score != NULL) {
		*score = reply->score;
	}

	if(cas != NULL && *cas > 1) {
		*cas = reply->cas;
	}

	return reply->errCode;
}

int Client::get(uint8_t tableId, const string& rowKey, const string& colKey,
		string* value, int64_t* score, uint32_t* cas) {
	string pkg;
	PkgOneOp reply;
	uint32_t dwCas = (cas != NULL) ? *cas : 0;
	int err = doOneOp(false, CmdGet, tableId, rowKey, colKey, EMPTYSTR, 0, dwCas,
			&reply, pkg);
	if(err < 0) {
		return err;
	}
	return replyGet(value, score, cas, &reply);
}

int Client::zGet(uint8_t tableId, const string& rowKey, const string& colKey,
			string* value, int64_t* score, uint32_t* cas) {
	string pkg;
	PkgOneOp reply;
	uint32_t dwCas = (cas != NULL) ? *cas : 0;
	int err = doOneOp(true, CmdGet, tableId, rowKey, colKey, EMPTYSTR, 0, dwCas,
			&reply, pkg);
	if(err < 0) {
		return err;
	}
	return replyGet(value, score, cas, &reply);
}

int Client::set(uint8_t tableId, const string& rowKey, const string& colKey,
			const string& value, int64_t score, uint32_t cas) {
	string pkg;
	PkgOneOp reply;
	int err = doOneOp(false, CmdSet, tableId, rowKey, colKey, value, score, cas,
			&reply, pkg);
	if(err < 0) {
		return err;
	}
	return reply.errCode;
}

int Client::zSet(uint8_t tableId, const string& rowKey, const string& colKey,
			const string& value, int64_t score, uint32_t cas) {
	string pkg;
	PkgOneOp reply;
	int err = doOneOp(true, CmdSet, tableId, rowKey, colKey, value, score, cas,
			&reply, pkg);
	if(err < 0) {
		return err;
	}
	return reply.errCode;
}

int Client::del(uint8_t tableId, const string& rowKey, const string& colKey, uint32_t cas) {
	string pkg;
	PkgOneOp reply;
	int err = doOneOp(false, CmdDel, tableId, rowKey, colKey, EMPTYSTR, 0, cas,
			&reply, pkg);
	if(err < 0) {
		return err;
	}
	return reply.errCode;
}

int Client::zDel(uint8_t tableId, const string& rowKey, const string& colKey, uint32_t cas) {
	string pkg;
	PkgOneOp reply;
	int err = doOneOp(false, CmdDel, tableId, rowKey, colKey, EMPTYSTR, 0, cas,
			&reply, pkg);
	if(err < 0) {
		return err;
	}
	return reply.errCode;
}

int Client::incr(uint8_t tableId, const string& rowKey, const string& colKey,
			string* value, int64_t* score, uint32_t cas) {
	string pkg;
	PkgOneOp reply;
	int err = doOneOp(false, CmdGet, tableId, rowKey, colKey, EMPTYSTR, 0, cas,
			&reply, pkg);
	if(err < 0) {
		return err;
	}
	return replyGet(value, score, NULL, &reply);
}

int Client::zIncr(uint8_t tableId, const string& rowKey, const string& colKey,
			string* value, int64_t* score, uint32_t cas) {
	string pkg;
	PkgOneOp reply;
	int err = doOneOp(true, CmdGet, tableId, rowKey, colKey, EMPTYSTR, 0, cas,
			&reply, pkg);
	if(err < 0) {
		return err;
	}
	return replyGet(value, score, NULL, &reply);
}

template <typename T>
static inline int replyMulti(vector<T>* reply, const PkgMultiOp& p) {
	if(p.errCode == 0 && reply != NULL) {
		reply->resize(p.kvs.size());
		for(unsigned i = 0; i < p.kvs.size(); i++) {
			copyReply((*reply)[i], p.kvs[i]);
		}
	}
	return p.errCode;
}

int Client::mGet(const vector<GetArgs>& args, vector<GetReply>* reply) {
	string pkg;
	PkgMultiOp p;
	int err = doMultiOp(false, CmdMGet, args, &p, pkg);
	if(err < 0) {
		return err;
	}
	return replyMulti(reply, p);
}

int Client::zmGet(const vector<GetArgs>& args, vector<GetReply>* reply) {
	string pkg;
	PkgMultiOp p;
	int err = doMultiOp(true, CmdMGet, args, &p, pkg);
	if(err < 0) {
		return err;
	}
	return replyMulti(reply, p);
}

int Client::mSet(const vector<SetArgs>& args, vector<SetReply>* reply) {
	string pkg;
	PkgMultiOp p;
	int err = doMultiOp(false, CmdMSet, args, &p, pkg);
	if(err < 0) {
		return err;
	}
	return replyMulti(reply, p);
}

int Client::zmSet(const vector<SetArgs>& args, vector<SetReply>* reply) {
	string pkg;
	PkgMultiOp p;
	int err = doMultiOp(true, CmdMSet, args, &p, pkg);
	if(err < 0) {
		return err;
	}
	return replyMulti(reply, p);
}

int Client::mDel(const vector<DelArgs>& args, vector<DelReply>* reply) {
	string pkg;
	PkgMultiOp p;
	int err = doMultiOp(false, CmdMDel, args, &p, pkg);
	if(err < 0) {
		return err;
	}
	return replyMulti(reply, p);
}

int Client::zmDel(const vector<DelArgs>& args, vector<DelReply>* reply) {
	string pkg;
	PkgMultiOp p;
	int err = doMultiOp(true, CmdMDel, args, &p, pkg);
	if(err < 0) {
		return err;
	}
	return replyMulti(reply, p);
}

int Client::mIncr(const vector<IncrArgs>& args, vector<IncrReply>* reply) {
	string pkg;
	PkgMultiOp p;
	int err = doMultiOp(false, CmdMIncr, args, &p, pkg);
	if(err < 0) {
		return err;
	}
	return replyMulti(reply, p);
}

int Client::zmIncr(const vector<IncrArgs>& args, vector<IncrReply>* reply) {
	string pkg;
	PkgMultiOp p;
	int err = doMultiOp(true, CmdMIncr, args, &p, pkg);
	if(err < 0) {
		return err;
	}
	return replyMulti(reply, p);
}

int Client::doScan(bool zop, uint8_t tableId, const string& rowKey, const string& colKey,
			int64_t score, bool start, bool asc, bool orderByScore, int num,
			ScanReply* reply, PkgScanResp* resp,  string& pkg) {
	if(closed) {
		return -1;
	}

	seq++;

	PkgScanReq p;
	p.seq = seq;
	p.dbId = dbId;
	p.cmd = CmdScan;
	if(asc) {
		p.pkgFlag |= FlagScanAsc;
	}
	if(start) {
		p.pkgFlag |= FlagScanKeyStart;
	}
	p.num = uint16_t(num);
	p.tableId = tableId;
	p.rowKey = rowKey;
	p.colKey = colKey;

	// ZScan
	if(zop) {
		p.pkgFlag |= FlagZop;
		p.setScore(score);
		if(orderByScore) {
			p.setColSpace(ColSpaceScore1);
		} else {
			p.setColSpace(ColSpaceScore2);
		}
	}

	int pkgLen = p.length();
	if(pkgLen > MaxPkgLen) {
		return EcInvPkgLen;
	}

	pkg.resize(pkgLen);
	int n = p.encode((char*)pkg.data(), pkgLen);
	if(n < 0) {
		return -2;
	}

	// send pkg
	n = 0;
	while(n < pkgLen) {
		int m = write(fd, pkg.data()+n, pkgLen-n);
		if(m < 0) {
			return -3;
		}
		n += m;
	}

	// recv pkg
	PkgHead head;
	n = readPkg(fd, buf, sizeof(buf), &head, pkg);
	if(n < 0) {
		return -4;
	}
	if(n == 0) {
		this->close();
		return -5;
	}
	if(head.seq != seq) {
		this->close();
		return -6;
	}

	// reply
	n = resp->decode(pkg.data(), pkg.size());
	if(n < 0) {
		return -7;
	}

	reply->tableId = tableId;
	reply->rowKey = rowKey;

	reply->ctx.zop = zop;
	reply->ctx.asc = asc;
	reply->ctx.orderByScore = orderByScore;
	reply->ctx.num = num;

	return 0;
}

static inline int replyScan(ScanReply* reply, const PkgScanResp& p) {
	if(p.errCode == 0 && reply != NULL) {
		reply->end = (p.pkgFlag&FlagScanEnd) != 0;
		reply->kvs.resize(p.kvs.size());
		for(unsigned i = 0; i < p.kvs.size(); i++) {
			copyReply(reply->kvs[i], p.kvs[i]);
		}
	}
	return p.errCode;
}

int Client::scan(uint8_t tableId, const string& rowKey,
			bool asc, int num, ScanReply* reply) {
	string pkg;
	PkgScanResp p;
	int err = doScan(false, tableId, rowKey, EMPTYSTR, 0, true, asc, false, num,
			reply, &p, pkg);
	if(err < 0) {
		return err;
	}
	return replyScan(reply, p);
}

int Client::scanPivot(uint8_t tableId, const string& rowKey, const string& colKey,
			bool asc, int num, ScanReply* reply) {
	string pkg;
	PkgScanResp p;
	int err = doScan(false, tableId, rowKey, colKey, 0, false, asc, false, num,
			reply, &p, pkg);
	if(err < 0) {
		return err;
	}
	return replyScan(reply, p);
}

int Client::zScan(uint8_t tableId, const string& rowKey,
			bool asc, bool orderByScore, int num, ScanReply* reply) {
	string pkg;
	PkgScanResp p;
	int err = doScan(true, tableId, rowKey, EMPTYSTR, 0, true, asc, orderByScore, num,
			reply, &p, pkg);
	if(err < 0) {
		return err;
	}
	return replyScan(reply, p);
}

int Client::zScanPivot(uint8_t tableId, const string& rowKey, const string& colKey, int64_t score,
			bool asc, bool orderByScore, int num, ScanReply* reply) {
	string pkg;
	PkgScanResp p;
	int err = doScan(true, tableId, rowKey, colKey, score, false, asc, orderByScore, num,
			reply, &p, pkg);
	if(err < 0) {
		return err;
	}
	return replyScan(reply, p);
}

int Client::scanMore(const ScanReply& last, ScanReply* reply) {
	if(last.end || last.kvs.size() == 0) {
		return -10;
	}
	if(reply == NULL) {
		return -11;
	}
	const ScanKV& r = last.kvs[last.kvs.size()-1];
	if(last.ctx.zop) {
		return zScanPivot(last.tableId, last.rowKey, r.colKey, r.score,
			last.ctx.asc, last.ctx.orderByScore, last.ctx.num, reply);
	} else {
		return scanPivot(last.tableId, last.rowKey, r.colKey,
				last.ctx.asc, last.ctx.num, reply);
	}
}

int Client::doDump(bool oneTable, uint8_t tableId, uint8_t colSpace,
		const string& rowKey, const string& colKey, int64_t score,
		uint16_t startUnitId, uint16_t endUnitId,
		DumpReply* reply, PkgDumpResp* resp, string& pkg) {
	if(closed) {
		return -1;
	}

	seq++;

	PkgDumpReq p;
	p.seq = seq;
	p.dbId = dbId;
	p.cmd = CmdDump;
	if(oneTable) {
		p.pkgFlag |= FlagDumpTable;
	}
	p.startUnitId = startUnitId;
	p.endUnitId = endUnitId;
	p.tableId = tableId;
	p.rowKey = rowKey;
	p.colKey = colKey;
	p.setColSpace(colSpace);
	p.setScore(score);

	int pkgLen = p.length();
	if(pkgLen > MaxPkgLen) {
		return EcInvPkgLen;
	}
	
	pkg.resize(pkgLen);
	int n = p.encode((char*)pkg.data(), pkgLen);
	if(n < 0) {
		return -2;
	}

	// send pkg
	n = 0;
	while(n < pkgLen) {
		int m = write(fd, pkg.data()+n, pkgLen-n);
		if(m < 0) {
			return -3;
		}
		n += m;
	}

	// recv pkg
	PkgHead head;
	n = readPkg(fd, buf, sizeof(buf), &head, pkg);
	if(n < 0) {
		return -4;
	}
	if(n == 0) {
		this->close();
		return -5;
	}
	if(head.seq != seq) {
		this->close();
		return -6;
	}

	// reply
	n = resp->decode(pkg.data(), pkg.size());
	if(n < 0) {
		return -7;
	}

	reply->ctx.oneTable = oneTable;
	reply->ctx.tableId = tableId;
	reply->ctx.startUnitId = startUnitId;
	reply->ctx.endUnitId = endUnitId;
	reply->ctx.lastUnitId = resp->lastUnitId;
	reply->ctx.unitStart = (resp->pkgFlag&FlagDumpUnitStart) != 0;

	return 0;
}

int Client::dumpPivot(bool oneTable, uint8_t tableId, uint8_t colSpace,
		const string& rowKey, const string& colKey, int64_t score,
		uint16_t startUnitId, uint16_t endUnitId, DumpReply* reply) {
	string pkg;
	PkgDumpResp p;
	int err = doDump(oneTable, tableId, colSpace, rowKey, colKey, score,
			startUnitId, endUnitId, reply, &p, pkg);
	if(err < 0) {
		return err;
	}

	if(p.errCode == 0 && reply != NULL) {
		reply->end = (p.pkgFlag&FlagDumpEnd) != 0;
		reply->kvs.resize(p.kvs.size());
		for(unsigned i = 0; i < p.kvs.size(); i++) {
			copyReply(reply->kvs[i], p.kvs[i]);
		}
	}
	return p.errCode;
}

int Client::dumpDB(DumpReply* reply) {
	return dumpPivot(false, 0, 0, EMPTYSTR, EMPTYSTR, 0, 0, 65535, reply);
}

int Client::dumpTable(uint8_t tableId, DumpReply* reply) {
	return dumpPivot(true, tableId, 0, EMPTYSTR, EMPTYSTR, 0, 0, 65535, reply);
}

int Client::dumpMore(const DumpReply& last, DumpReply* reply) {
	if(last.end) {
		return -10;
	}
	if(reply == NULL) {
		return -11;
	}

	const DumpReply* t = &last;
	while(true) {
		DumpKV tmp;
		const DumpKV* rec;
		uint16_t lastUnitId = t->ctx.lastUnitId;
		if(t->ctx.unitStart) {
			lastUnitId += 1;
			if(t->ctx.oneTable) {
				tmp.tableId = t->ctx.tableId;
			}
			rec = &tmp;
		} else {
			rec = &t->kvs[t->kvs.size()-1];
		}

		int err = dumpPivot(t->ctx.oneTable, rec->tableId, rec->colSpace,
			rec->rowKey, rec->colKey, rec->score, lastUnitId, t->ctx.endUnitId, reply);
		if(err < 0) {
			return err;
		}

		t = reply;
		if(t->end || t->kvs.size() > 0) {
			return 0;
		}
	}

	return -12;
}

}  // namespace gotable
