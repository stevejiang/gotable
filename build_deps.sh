#!/bin/sh

DEPS_DIR=/tmp/gotable-deps
#DEPS_DIR=$HOME/workspace/gotable-deps
DEPS_ULR=https://github.com/stevejiang/gotable-deps/raw/master

ROCKSDB_VER=rocksdb-3.8

if [ ! -d "$DEPS_DIR" ]; then
	mkdir -p $DEPS_DIR;
fi

if [ "$1" = "-dl" ]; then
	if [ ! -f "$DEPS_DIR/$ROCKSDB_VER.tar.gz" ]; then
		wget --no-check-certificate $DEPS_ULR/$ROCKSDB_VER.tar.gz -O $DEPS_DIR/$ROCKSDB_VER.tar.gz
	fi
else	
	OUTPUT=$1
	if [ "$OUTPUT" != "" ]; then
		echo "DEPS_DIR=$DEPS_DIR" >> $OUTPUT
		echo "ROCKSDB_VER=$ROCKSDB_VER" >> $OUTPUT
	fi
fi
