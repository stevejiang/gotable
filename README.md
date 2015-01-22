# GoTable

GoTable is a high performance NoSQL database powered by [Go](http://golang.org/) and [RocksDB](http://rocksdb.org/). It's inspired by BigTable/HBase and Redis/LedisDB.

## Features

+ High performance.
+ Small but powerful set of APIs: GET, SET, MGET, MSET, SCAN, INCR, DUMP and "Z" APIs.
+ Data storage is not limited by RAM and friendly with SSD.
+ Transaction support with CAS (compare and save).
+ Replication.
+ Distributed scale design in mind.

## Build and Install

To build GoTable, you need to setup [Go](http://golang.org/) environment and gcc with c++11 support.

	#download GoTable source code
	go get github.com/stevejiang/gotable
	
	cd $GOPATH/src/github.com/stevejiang/gotable
	make
	
The GoTable binary files are in $GOPATH/bin directory.

## Requirement

+ Linux or MacOS
+ Go version >= 1.3
+ Gcc version >= 4.8.1

## API Example

+ [Official Example](https://github.com/stevejiang/gotable/blob/master/cmd/gotable-example/example.go)

## Data Model

GoTable is constructed with up to 255 DBs, each DB is constructed with up to 256 Tables. The structure of each table is something like the following chart:

	        |     default column space      |  "Z" sort score column space  |
	        |-------------------------------|-------------------------------|
	        |    colKey1    |    colKey2    |     colKey1   |    colKey3    |
	--------|---------------|---------------|---------------|---------------|
	rowKey1 |value11,score11|value12,score12|value13,score13|value14,score14|
	--------|-------------------------------|-------------------------------|
	rowKey2 |value21,score21|value22,score22|value23,score23|value24,score24|
	--------|-------------------------------|-------------------------------|
	 ...    |              ...              |              ...              |

A table can holds unlimited number of rows(rowKey). Each row can have up to millions columns(colKey).
Data sharding is based on rowKey. So you should carefully construct rowKey to avoid hot spot issue.
