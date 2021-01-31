# GoTable

GoTable is a high performance NoSQL database powered by [Go](http://golang.org/) and [RocksDB](http://rocksdb.org/). It's inspired by BigTable and Redis.

## Features

+ High performance and easy to scale.
+ Powerful set of APIs: GET, SET, DEL, MGET, MSET, MDEL, SCAN, INCR, DUMP and "Z" APIs.
+ Data storage is not limited by RAM.
+ Friendly with SSD.
+ Transaction support with [CAS](http://en.wikipedia.org/wiki/Compare-and-swap) (Compare-And-Swap).
+ Replication.

## Build and Install

To build GoTable, you need to setup [Go](http://golang.org/) environment and gcc with c++11 support, please see the requirement part for details.

	#download GoTable source code
	git clone https://github.com/stevejiang/gotable.git
	
	cd gotable

	#build rocksdb, it will download rocksdb automatically if missing
	sh build_rocksdb.sh
	
	#build GoTable
	make

The GoTable binary files are in $HOME/go/bin directory.

## Requirement

+ Linux or MacOS, 64 bit operating system is the best.
+ Go version >= 1.15
+ g++ version, supports c++11

## Running GoTable

Please add $GOPATH/bin to PATH environment first.
To run GoTable in default configuration just type:

	gotable-server

If you want to provide your gotable.conf, you have to run it using an additional parameter (the path of the configuration file):

	gotable-server /path/to/gotable.conf

## Playing with GoTable

You can use gotable-cli to play with GoTable. Start a gotable-server instance, then in another terminal try the following:

	% gotable-cli 
	gotable@0> set 0 r1 c1 v1
	OK
	gotable@0> get 0 r1 c1
	[0	"v1"]
	gotable@0> incr 0 r1 c1
	[1	"v1"]
	gotable@0> incr 0 r1 c1 4
	[5	"v1"]
	gotable@0> zget 0 r1 c1
	<nil>
	gotable@0> zset 0 r1 c1 va 11
	OK
	gotable@0> zget 0 r1 c1
	[11	"va"]
	gotable@0> set 0 r1 c2 v2 2
	OK
	gotable@0> scan 0 r1 ""
	 0) ["r1"	"c1"]	[5	"v1"]
	 1) ["r1"	"c2"]	[2	"v2"]
	gotable@0> zscan 0 r1 0 ""
	 0) ["r1"	11	"c1"]	["va"]
	gotable@0> zset 0 r1 c2 vb 12
	OK
	gotable@0> zscan 0 r1 0 ""
	 0) ["r1"	11	"c1"]	["va"]
	 1) ["r1"	12	"c2"]	["vb"]
	gotable@0> select 1
	OK
	gotable@1> get 0 r1 c1
	<nil>
	gotable@1> select 0
	OK
	gotable@0> get 0 r1 c1
	[5	"v1"]

## Replication

You can use gotable-cli with command SLAVEOF to change replication settings of a slave on the fly. If a GoTable server is already acting as slave, the command SLAVEOF NO ONE will turn off the replication, turning the GoTable server into a master. In the proper form SLAVEOF host will make the server a slave of another server listening at the specified host(ip:port). GoTable remembers the replication settings, it will reconnect to master automatically when restarted.

	% gotable-cli 
	gotable@0> SLAVEOF 127.0.0.1:6689
	OK
	gotable@0> SLAVEOF
	OK

If a server is already a slave of some master, SLAVEOF host will stop the replication against the old server and start the synchronization against the new one. Old dataset is kept and synchronization starts from the last binlog sequence.

## API Example

+ [Go Example](https://github.com/stevejiang/gotable/blob/master/cmd/gotable-example/example.go)
+ [C++ Example](https://github.com/stevejiang/gotable/blob/master/api/c++/example.cc)

## Data Model

GoTable is constructed with up to 255 DBs, each DB is constructed with up to 256 Tables. The table structure is like the following chart:

	        |-------------------------------------|-------------------------------------|
	        |        Default column space         |    "Z" sorted score column space    |
	        |---------------|---------------|-----|---------------|---------------|-----|
	        |    colKey1    |    colKey2    | ... |     colKey1   |    colKey3    | ... |
	--------|---------------|---------------|-----|---------------|---------------|-----|
	rowKey1 |value11,score11|value12,score12| ... |value13,score13|value14,score14| ... |
	--------|---------------|---------------|-----|---------------|---------------|-----|
	rowKey2 |value21,score21|value22,score22| ... |value23,score23|value24,score24| ... |
	--------|---------------|---------------|-----|---------------|---------------|-----|
	  ...   |              ...                    |              ...                    |

A table can hold unlimited number of rows(rowKey). Each row can have up to millions columns(colKey).
Data sharding is based on rowKey, records with the same rowKey are stored in the same slot. So you should carefully construct rowKey to avoid hot spot issue.

### Default column space

In default column space, all colKeys are stored in ASC order. The APIs GET/SET/DEL/INCR/SCAN take effect in this space. The SCAN API scans records order by colKey in ASC or DESC order for a rowKey.

### "Z" sorted score column space

In "Z" sorted score column space, there are two lists for every rowKey. The first list is like the default column space, all colKeys are sorted in ASC order; the second list is order by score, all colKeys are sorted by "score+colKey" in ASC order. The APIs ZGET/ZSET/ZDEL/ZINCR/ZSCAN take effect in this space. The SCAN API can scan records on the two lists, order by colKey or "score+colKey".

## Performance Benchmark

Benchmark command:

	gotable-bench -t set,zset,get,zget,scan,zscan,incr,zincr -range 10 -n 1000000 -c 100

Benchmark result:

	SET        :  136954.7 op/s    
	ZSET       :  105653.1 op/s    
	GET        :  244484.1 op/s    
	ZGET       :  264541.5 op/s    
	SCAN 10    :  115589.9 op/s    
	ZSCAN 10   :  115073.4 op/s    
	INCR       :  118933.3 op/s    
	ZINCR      :   86478.0 op/s    

If you want to see latency distribution, add "-histogram 1" to the command line:

	gotable-bench -t get -n 1000000 -c 100 -histogram 1

Benchmark latency distribution:

	GET        :  244271.3 op/s    
	Microseconds per op:
	Count: 1000000  Average: 406.6848  StdDev: 189.40
	Min: 64.0000  Median: 384.1120  Max: 3220.0000
	------------------------------------------------------
	[      60,      70 )       5   0.001%   0.001% 
	[      70,      80 )      18   0.002%   0.002% 
	[      80,      90 )      74   0.007%   0.010% 
	[      90,     100 )     249   0.025%   0.035% 
	[     100,     120 )    1588   0.159%   0.193% 
	[     120,     140 )    5490   0.549%   0.742% 
	[     140,     160 )   11924   1.192%   1.935% 
	[     160,     180 )   20065   2.006%   3.941% 
	[     180,     200 )   27430   2.743%   6.684% #
	[     200,     250 )   96658   9.666%  16.350% ##
	[     250,     300 )  119100  11.910%  28.260% ##
	[     300,     350 )  125923  12.592%  40.852% ###
	[     350,     400 )  134082  13.408%  54.261% ###
	[     400,     450 )  141526  14.153%  68.413% ###
	[     450,     500 )  120417  12.042%  80.455% ##
	[     500,     600 )  115526  11.553%  92.008% ##
	[     600,     700 )   30308   3.031%  95.038% #
	[     700,     800 )    9663   0.966%  96.005% 
	[     800,     900 )    6191   0.619%  96.624% 
	[     900,    1000 )    7402   0.740%  97.364% 
	[    1000,    1200 )   15571   1.557%  98.921% 
	[    1200,    1400 )    8483   0.848%  99.769% 
	[    1400,    1600 )    1912   0.191%  99.961% 
	[    1600,    1800 )     286   0.029%  99.989% 
	[    1800,    2000 )      62   0.006%  99.995% 
	[    2000,    2500 )      43   0.004% 100.000% 
	[    2500,    3000 )       3   0.000% 100.000% 
	[    3000,    3500 )       1   0.000% 100.000% 
