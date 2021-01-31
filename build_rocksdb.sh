#!/bin/sh

ROCKSDB_ROOT_DIR=`pwd`/../rocksdb
ROCKSDB_VER=6.15.4
ROCKSDB_URL=https://github.com/facebook/rocksdb/archive/v$ROCKSDB_VER.tar.gz
ROCKSDB_DIR=$ROCKSDB_ROOT_DIR/rocksdb-$ROCKSDB_VER

command_exists() {
	command -v "$@" > /dev/null 2>&1
}

download_rocksdb() {
    tar zxf $ROCKSDB_DIR.tar.gz -C $ROCKSDB_ROOT_DIR 2>/dev/null
    if [ "$?" != "0" ]; then
		if command_exists curl; then
			curl -L $ROCKSDB_URL -o $ROCKSDB_DIR.tar.gz
		elif command_exists wget; then
			wget --no-check-certificate $ROCKSDB_URL -O $ROCKSDB_DIR.tar.gz
		else
			echo "curl and wget are not installed, download rocksdb failed!"
       		exit 1
		fi
        tar zxf $ROCKSDB_DIR.tar.gz -C $ROCKSDB_ROOT_DIR
    fi
    if [ "$?" != "0" ]; then
        echo "download rocksdb failed!"
        exit 2
    fi
}

if [ ! -d "$ROCKSDB_ROOT_DIR" ]; then
    mkdir -p $ROCKSDB_ROOT_DIR
fi

make -C $ROCKSDB_DIR static_lib
if [ "$?" != "0" ]; then
    rm -rf $ROCKSDB_DIR
    download_rocksdb

    make -C $ROCKSDB_DIR static_lib
    if [ "$?" != "0" ]; then
        echo "build rocksdb failed!"
        exit 3
    fi
fi

cp $ROCKSDB_DIR/make_config.mk .
echo "ROCKSDB_DIR=$ROCKSDB_DIR" >> make_config.mk
