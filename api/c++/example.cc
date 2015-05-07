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
#include <stdint.h>
#include <sys/time.h>
#include <unistd.h>
#include <string>
#include <vector>

#include "gotable.h"

using std::string;
using std::vector;
using namespace gotable;

void testGet(Client* cli) {
	// set
	int err = cli->set(1, "row1", "col1", "v01", 10, 0);
	if(err < 0) {
		printf("set failed with error %d\n", err);
		return;
	}

	string value;
	int64_t score;
	err = cli->get(1, "row1", "col1", &value, &score, NULL);
	if(err < 0) {
		printf("get failed with error %d\n", err);
		return;
	}

	if(err > 0) {
		printf("get result1: Key not exist!\n");
	} else {
		printf("get result1: %s\t%lld\n", value.c_str(), (long long)score);
	}

	// delete
	err = cli->del(1, "row1", "col1", 0);
	if(err < 0) {
		printf("del failed with error %d\n", err);
		return;
	}

	err = cli->get(1, "row1", "col1", &value, &score, NULL);
	if(err < 0) {
		printf("get failed with error %d\n", err);
		return;
	}

	if(err > 0) {
		printf("get result2: Key not exist!\n");
	} else {
		printf("get result2: %s\t%lld\n", value.c_str(), (long long)score);
	}
}

void testMGet(Client* cli) {
	vector<SetArgs> sa;
	sa.push_back(SetArgs(1, "row1", "col0", "v00", 10, 0));
	sa.push_back(SetArgs(1, "row1", "col1", "v01", 9, 0));
	sa.push_back(SetArgs(1, "row1", "col2", "v02", 8, 0));
	sa.push_back(SetArgs(1, "row1", "col3", "v03", 7, 0));
	sa.push_back(SetArgs(1, "row1", "col4", "v04", 6, 0));
	int err = cli->mSet(sa, NULL);
	if(err < 0) {
		printf("mSet failed: %d\n", err);
		return;
	}

	vector<GetArgs> ga;
	vector<GetReply> reply;
	ga.push_back(GetArgs(1, "row1", "col4", 0));
	ga.push_back(GetArgs(1, "row1", "col2", 0));
	ga.push_back(GetArgs(1, "row1", "col1", 0));
	ga.push_back(GetArgs(1, "row1", "col3", 0));
	ga.push_back(GetArgs(1, "row1", "not", 0));
	err = cli->mGet(ga, &reply);
	if(err < 0) {
		printf("get failed with error %d\n", err);
		return;
	}

	printf("MGET result:\n");
	for(unsigned i = 0; i < reply.size(); i++) {
		if(reply[i].errCode < 0) {
			printf("[%s\t%s]\tget failed with error %d!\n",
					reply[i].rowKey.c_str(), reply[i].colKey.c_str(), reply[i].errCode);
		} else if(reply[i].errCode > 0) {
			printf("[%s\t%s]\tkey not exist!\n",
					reply[i].rowKey.c_str(), reply[i].colKey.c_str());
		} else {
			printf("[%s\t%s]\t[%lld\t%s]\n",
					reply[i].rowKey.c_str(), reply[i].colKey.c_str(),
					(long long)reply[i].score, reply[i].value.c_str());
		}
	}
}

void testScan(Client* cli) {
	ScanReply reply;
	int err = cli->scan(1, "row1", true, 10, &reply);
	if(err < 0) {
		printf("scan failed: %d\n", err);
		return;
	}

	printf("SCAN result:\n");
	for(unsigned i = 0; i < reply.kvs.size(); i++) {
		printf("[%s\t%s]\t[%lld\t%s]\n",
			reply.rowKey.c_str(), reply.kvs[i].colKey.c_str(),
			(long long)reply.kvs[i].score, reply.kvs[i].value.c_str());
	}
	if(reply.end) {
		printf("SCAN finished!\n");
	} else {
		printf("SCAN has more records!\n");
	}
}

void testZScan(Client* cli) {
	int err = cli->zSet(1, "row2", "000", "v00", 10, 0);
	if(err < 0) {
		printf("zSet failed: %d\n", err);
		return;
	}

	vector<SetArgs> sa;
	sa.push_back(SetArgs(1, "row2", "col1", "v01", 9, 0));
	sa.push_back(SetArgs(1, "row2", "col2", "v02", 6, 0));
	sa.push_back(SetArgs(1, "row2", "col3", "v03", 7, 0));
	sa.push_back(SetArgs(1, "row2", "col4", "v04", 6, 0));
	sa.push_back(SetArgs(1, "row2", "col5", "v05", -5, 0));

	err = cli->zmSet(sa, NULL);
	if(err < 0) {
		printf("zmSet failed: %d\n", err);
		return;
	}

	ScanReply reply;
	err = cli->zScan(1, "row2", true, true, 4, &reply);
	if(err < 0) {
		printf("zScan failed: %d\n", err);
		return;
	}

	printf("ZSCAN result:\n");
	while(true) {
		for(unsigned i = 0; i < reply.kvs.size(); i++) {
			printf("[%s\t%s]\t[%lld\t%s]\n",
				reply.rowKey.c_str(), reply.kvs[i].colKey.c_str(),
				(long long)reply.kvs[i].score, reply.kvs[i].value.c_str());
		}

		if(reply.end) {
			printf("ZSCAN finished!\n");
			break;
		} else {
			printf("ZSCAN has more records:\n");
		}

		err = cli->scanMore(reply, &reply);
		if(err < 0) {
			printf("scanMore failed: %d\n", err);
			return;
		}
	}
}

void testCas(Client* cli) {
	string value;
	int64_t score;
	uint32_t cas = 0;
	int err = 0;
	// Try i < 11 for cas not match
	for(int i = 0; i < 1; i++) {
		uint32_t newCas = 2;
		err = cli->get(1, "row1", "col1", &value, &score, &newCas);
		if(err < 0) {
			printf("get failed: %d\n", err);
			return;
		}

		if(i > 0) {
			sleep(1);
		} else {
			cas = newCas;
		}

		printf("\tCas %02d: (%u %u)\t(%s, %lld)\n", i, newCas, cas, value.c_str(), (long long)score);
	}

	err = cli->set(1, "row1", "col1", value+"-cas", score+20, cas);
	if(err < 0) {
		printf("Set failed: %d\n", err);
	}

	err = cli->get(1, "row1", "col1", &value, &score, 0);
	if(err < 0) {
		printf("get failed: %d\n", err);
		return;
	}

	printf("CAS result: %s\t%lld\n", value.c_str(), (long long)score);
}

void testPing(Client* cli) {
	timeval start, end;
	gettimeofday(&start, NULL);
	int err = cli->ping();
	if(err < 0) {
		printf("ping failed: %d\n", err);
		return;
	}

	gettimeofday(&end, NULL);
	float elapsed = (end.tv_sec-start.tv_sec)*1e6+(end.tv_usec-start.tv_usec);

	printf("Ping succeed: %.2f ms\n", elapsed/1e3);
}

void testDump(Client* cli) {
	DumpReply reply;
	int err = cli->dumpTable(1, &reply);
	if(err < 0) {
		printf("dump failed: %d\n", err);
		return;
	}

	printf("Dump result:\n");
	int idx = 0;
	while(true) {
		for(unsigned i=0; i < reply.kvs.size(); i++) {
			printf("%02u) %d\t%s\t%d\t%s\t%lld\t%s\n", idx,
				reply.kvs[i].tableId, reply.kvs[i].rowKey.c_str(), reply.kvs[i].colSpace,
				reply.kvs[i].colKey.c_str(), (long long)reply.kvs[i].score, reply.kvs[i].value.c_str());
			idx++;
		}

		if(reply.end) {
			break;
		}

		err = cli->dumpMore(reply, &reply);
		if(err < 0) {
			printf("dumpMore failed: %d\n", err);
			return;
		}
	}
}


int main(int argc, char** argv) {
	Client* cli = Client::Dial("127.0.0.1", 6688);
	if(cli == NULL) {
		printf("Failed to connect to gotable server!\n");
		return 1;
	}
	cli->select(1);

	testGet(cli);
	testMGet(cli);
	testScan(cli);
	testZScan(cli);
	testCas(cli);
	testPing(cli);
	testDump(cli);

	delete cli;
	return 0;
}
