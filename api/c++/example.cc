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
#include <string>
#include <vector>

#include "gotable.h"

using std::string;
using std::vector;
using gotable::Client;
using gotable::GetArgs;
using gotable::GetReply;

void testGet(Client* cli) {
	string value;
	int64_t score;
	int err = cli->get(0, "1", "000", &value, &score);
	if(err < 0) {
		printf("get failed with error %d\n", err);
		return;
	}
	if(err > 0) {
		printf("key not exist\n");
	} else {
		printf("value=%s, score=%lld\n", value.c_str(), (long long)score);
	}
}

void testMGet(Client* cli) {
	vector<GetArgs> args;
	vector<GetReply> reply;
	args.push_back(GetArgs(0, "1", "000", 0));
	args.push_back(GetArgs(0, "1", "003", 0));
	args.push_back(GetArgs(0, "1", "not", 0));
	int err = cli->mGet(args, &reply);
	if(err < 0) {
		printf("get failed with error %d\n", err);
		return;
	}
	for(unsigned i = 0; i < reply.size(); i++) {
		if(reply[i].errCode < 0) {
			printf("rowKey=%s, colKey=%s, failed with error %d\n",
					reply[i].rowKey.c_str(), reply[i].colKey.c_str(), reply[i].errCode);
		} else if(reply[i].errCode > 0) {
			printf("rowKey=%s, colKey=%s, key not exist!\n",
					reply[i].rowKey.c_str(), reply[i].colKey.c_str());
		} else {
			printf("rowKey=%s, colKey=%s, value=%s, score=%lld\n",
					reply[i].rowKey.c_str(), reply[i].colKey.c_str(),
					reply[i].value.c_str(), (long long)reply[i].score);
		}
	}
}

int main(int argc, char** argv) {
	Client* cli = Client::Dial("127.0.0.1", 6688);

	testGet(cli);
	testMGet(cli);

	delete cli;
	return 0;
}
