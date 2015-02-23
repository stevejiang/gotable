# GoTable

GoTable is a high performance NoSQL database powered by [Go](http://golang.org/) and [RocksDB](http://rocksdb.org/). It's inspired by BigTable and Redis.

## Features

+ High performance and easy to scale.
+ Powerful set of APIs: GET, SET, DEL, MGET, MSET, MDEL, SCAN, INCR, DUMP and "Z" APIs.
+ Data storage is not limited by RAM and friendly with SSD.
+ Transaction support with CAS (compare and save).
+ Replication.
+ Cluster (To be done ...)

## Build and Install

To build GoTable, you need to setup [Go](http://golang.org/) environment and gcc with c++11 support, please see the requirement part for details.

	#download GoTable source code
	go get github.com/stevejiang/gotable
	
	cd $GOPATH/src/github.com/stevejiang/gotable
	
	#build gotable, it will download rocksdb automatically if not found
	make

The make tool uses curl or wget to download rocksdb. If your computer has trouble to connect internet, please manually download and build rocksdb, and then use the following command to build gotable:

	#use prebuilt rocksdb
	make CGO_CFLAGS="-I/path/to/rocksdb/include" CGO_LDFLAGS="-L/path/to/rocksdb"

The GoTable binary files are in $GOPATH/bin directory.

## Requirement

+ Linux or MacOS, 64 bit operating system is the best.
+ Go version >= 1.3
+ gcc version >= 4.8.1

## API Example

+ [Official Example](https://github.com/stevejiang/gotable/blob/master/cmd/gotable-example/example.go)

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
Data sharding is based on rowKey, records with the same rowKey are stored in the same unit. So you should carefully construct rowKey to avoid hot spot issue.

### Default column space

In default column space, all colKeys are stored in ASC order. The APIs GET/SET/DEL/INCR/SCAN take effect in this space. The SCAN API scans records order by colKey in ASC or DESC order for a rowKey.

### "Z" sorted score column space

In "Z" sorted score column space, all colKeys are sorted in ASC order, and also colKeys are sorted by "score" in ASC order. The APIs ZGET/ZSET/ZDEL/ZINCR/ZSCAN take effect in this space. The SCAN API can scan records order by colKey, or order by score+colKey.

## Performance Benchmark

Benchmark command:

	./gotable-bench -t set,get,zset,zget,scan,zscan,incr,zincr -range 10 -n 1000000 -c 100

Benchmark result:

	SET        :   93863.3 op/s   
	GET        :  187564.2 op/s   
	ZSET       :   74422.7 op/s   
	ZGET       :  175590.2 op/s   
	SCAN 10    :   77577.6 op/s   
	ZSCAN 10   :   76823.5 op/s   
	INCR       :   80263.6 op/s   
	ZINCR      :   61873.2 op/s   

If you want to see latency distribution, add "-histogram 1" to the command line:

	./gotable-bench -t get -n 1000000 -c 100 -histogram 1

Benchmark latency distribution:

	GET        :  187564.2 op/s   
	Microseconds per op:
	Count: 1000000  Average: 531.7485  StdDev: 308.49
	Min: 165.0000  Median: 442.7021  Max: 10429.0000
	------------------------------------------------------
	[     160,     180 )       9   0.001%   0.001%
	[     180,     200 )     171   0.017%   0.018%
	[     200,     250 )    8198   0.820%   0.838%
	[     250,     300 )   57623   5.762%   6.600% #
	[     300,     350 )  136801  13.680%  20.280% ###
	[     350,     400 )  170846  17.085%  37.365% ###
	[     400,     450 )  147946  14.795%  52.159% ###
	[     450,     500 )  112429  11.243%  63.402% ##
	[     500,     600 )  135950  13.595%  76.997% ###
	[     600,     700 )   70409   7.041%  84.038% #
	[     700,     800 )   35382   3.538%  87.576% #
	[     800,     900 )   19661   1.966%  89.543%
	[     900,    1000 )   20231   2.023%  91.566%
	[    1000,    1200 )   44357   4.436%  96.001% #
	[    1200,    1400 )   25027   2.503%  98.504% #
	[    1400,    1600 )   10126   1.013%  99.517%
	[    1600,    1800 )    2758   0.276%  99.792%
	[    1800,    2000 )     689   0.069%  99.861%
	[    2000,    2500 )     521   0.052%  99.913%
	[    2500,    3000 )     245   0.025%  99.938%
	[    3000,    3500 )     110   0.011%  99.949%
	[    3500,    4000 )      61   0.006%  99.955%
	[    4000,    4500 )       5   0.001%  99.956%
	[    4500,    5000 )      92   0.009%  99.965%
	[    5000,    6000 )      67   0.007%  99.971%
	[    6000,    7000 )      43   0.004%  99.976%
	[    8000,    9000 )     122   0.012%  99.988%
	[    9000,   10000 )      80   0.008%  99.996%
	[   10000,   12000 )      41   0.004% 100.000%
