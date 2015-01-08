#!/bin/sh

DEPS_DIR=/tmp/gotable-deps
#DEPS_DIR=$HOME/workspace/gotable-deps
#DEPS_DIR=$HOME/localws/gotable-deps
DEPS_ULR=https://github.com/stevejiang/gotable-deps/raw/master

ROCKSDB_VER=rocksdb-3.8
ROCKSDB=$DEPS_DIR/$ROCKSDB_VER

if [ ! -d "$DEPS_DIR" ]; then
	mkdir -p $DEPS_DIR;
fi

if [ "$1" = "-dl" ]; then
	tar zxf $ROCKSDB.tar.gz -C $DEPS_DIR
	if [ "$?" != "0" ]; then
		wget --no-check-certificate $DEPS_ULR/$ROCKSDB_VER.tar.gz -O $ROCKSDB.tar.gz
		tar zxf $ROCKSDB.tar.gz -C $DEPS_DIR
	fi
else	
	OUTPUT=$1
	if [ "$OUTPUT" != "" ]; then
		echo "DEPS_DIR=$DEPS_DIR" >> $OUTPUT
		echo "ROCKSDB=$ROCKSDB" >> $OUTPUT
	fi
fi
