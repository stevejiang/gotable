# please run build_rocksdb.sh first
include make_config.mk

export CGO_CFLAGS=-g -O2 -DNDEBUG -I$(ROCKSDB_DIR)/include
export CGO_LDFLAGS=-L$(ROCKSDB_DIR) -lrocksdb $(PLATFORM_LDFLAGS)

all:
	go install --ldflags '-extldflags "-static-libstdc++"' ./...

test:
	go test -v --ldflags '-extldflags "-static-libstdc++"' ./...

clean:
	go clean -i ./...
