#!/bin/sh

DEPS_DIR=/tmp/gotable-deps
#DEPS_DIR=$HOME/workspace/gotable-deps
#DEPS_DIR=$HOME/localws/gotable-deps
DEPS_ULR=https://github.com/stevejiang/gotable-deps/raw/master

ROCKSDB_VER=rocksdb-3.8
ROCKSDB_URL=$DEPS_ULR/$ROCKSDB_VER.tar.gz
ROCKSDB=$DEPS_DIR/$ROCKSDB_VER

if [ ! -d "$DEPS_DIR" ]; then
	mkdir -p $DEPS_DIR
fi

if [ "$1" = "-dl" ]; then
	# Download rocksdb
	rm -rf $ROCKSDB
	tar zxf $ROCKSDB.tar.gz -C $DEPS_DIR 2>/dev/null
	if [ "$?" != "0" ]; then
		curl -L $ROCKSDB_URL -o $ROCKSDB.tar.gz
		if [ "$?" != "0" ]; then
			wget --no-check-certificate $ROCKSDB_URL -O $ROCKSDB.tar.gz
		fi
		tar zxf $ROCKSDB.tar.gz -C $DEPS_DIR
	fi
	if [ "$?" != "0" ]; then
		echo "Download rocksdb failed!"
		exit 1
	fi
	exit 0
fi


# Generate build flags
CROSS_COMPILE=
COMMON_FLAGS=
PLATFORM_LDFLAGS=

OUTPUT=$1
if test -z "$OUTPUT"; then
  echo "usage: $0 <output-filename>" >&2
  exit 1
fi

# Delete existing output, if it exists
rm -f "$OUTPUT"
touch "$OUTPUT"

if test -z "$CC"; then
   CC=cc
fi

if test -z "$CXX"; then
    CXX=g++
fi


if [ "$CROSS_COMPILE" = "true" ]; then
    # Cross-compiling; do not try any compilation tests.
    true
else
    # Test whether Snappy library is installed
    # http://code.google.com/p/snappy/
    $CXX $CFLAGS -x c++ - -o /dev/null 2>/dev/null  <<EOF
      #include <snappy.h>
      int main() {}
EOF
    if [ "$?" = 0 ]; then
        COMMON_FLAGS="$COMMON_FLAGS -DSNAPPY"
        PLATFORM_LDFLAGS="$PLATFORM_LDFLAGS -lsnappy"
    fi

    # Test whether zlib library is installed
    $CXX $CFLAGS $COMMON_FLAGS -x c++ - -o /dev/null 2>/dev/null  <<EOF
      #include <zlib.h>
      int main() {}
EOF
    if [ "$?" = 0 ]; then
        COMMON_FLAGS="$COMMON_FLAGS -DZLIB"
        PLATFORM_LDFLAGS="$PLATFORM_LDFLAGS -lz"
    fi

    # Test whether bzip library is installed
    $CXX $CFLAGS $COMMON_FLAGS -x c++ - -o /dev/null 2>/dev/null  <<EOF
      #include <bzlib.h>
      int main() {}
EOF
    if [ "$?" = 0 ]; then
        COMMON_FLAGS="$COMMON_FLAGS -DBZIP2"
        PLATFORM_LDFLAGS="$PLATFORM_LDFLAGS -lbz2"
    fi

    # Test whether lz4 library is installed
    $CXX $CFLAGS $COMMON_FLAGS -x c++ - -o /dev/null 2>/dev/null  <<EOF
      #include <lz4.h>
      #include <lz4hc.h>
      int main() {}
EOF
    if [ "$?" = 0 ]; then
        COMMON_FLAGS="$COMMON_FLAGS -DLZ4"
        PLATFORM_LDFLAGS="$PLATFORM_LDFLAGS -llz4"
    fi

    # Test whether numa is available
    $CXX $CFLAGS -x c++ - -o /dev/null -lnuma 2>/dev/null  <<EOF
      #include <numa.h>
      #inlcude <numaif.h>
      int main() {}
EOF
    if [ "$?" = 0 ]; then
        COMMON_FLAGS="$COMMON_FLAGS -DNUMA"
        PLATFORM_LDFLAGS="$PLATFORM_LDFLAGS -lnuma"
    fi

    # Test whether tcmalloc is available
    $CXX $CFLAGS -x c++ - -o /dev/null -ltcmalloc 2>/dev/null  <<EOF
      int main() {}
EOF
    if [ "$?" = 0 ]; then
        PLATFORM_LDFLAGS="$PLATFORM_LDFLAGS -ltcmalloc"
    fi
fi


echo "PLATFORM_LDFLAGS=$PLATFORM_LDFLAGS" >> $OUTPUT
echo "COMMON_FLAGS=$COMMON_FLAGS" >> $OUTPUT
echo "DEPS_DIR=$DEPS_DIR" >> $OUTPUT
echo "ROCKSDB=$ROCKSDB" >> $OUTPUT
