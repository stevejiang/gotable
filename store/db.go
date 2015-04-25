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

package store

// #include <rocksdb/c.h>
// #include <stdlib.h>
import "C"

import (
	"errors"
	"unsafe"
)

type DB struct {
	db    *C.rocksdb_t
	opt   *C.rocksdb_options_t
	rOpt  *C.rocksdb_readoptions_t // fill cache by default
	wOpt  *C.rocksdb_writeoptions_t
	cache *C.rocksdb_cache_t
	fp    *C.rocksdb_filterpolicy_t
}

type Iterator struct {
	it *C.rocksdb_iterator_t
}

type ReadOptions struct {
	rOpt *C.rocksdb_readoptions_t
	snap *C.rocksdb_snapshot_t
	db   *C.rocksdb_t
}

type WriteBatch struct {
	batch *C.rocksdb_writebatch_t
}

func NewDB() *DB {
	db := new(DB)

	return db
}

func (db *DB) Close() {
	if db.db != nil {
		C.rocksdb_close(db.db)
		db.db = nil

		if db.opt != nil {
			C.rocksdb_options_destroy(db.opt)
		}
		if db.rOpt != nil {
			C.rocksdb_readoptions_destroy(db.rOpt)
		}
		if db.wOpt != nil {
			C.rocksdb_writeoptions_destroy(db.wOpt)
		}
		if db.cache != nil {
			C.rocksdb_cache_destroy(db.cache)
		}
		if db.fp != nil {
			C.rocksdb_filterpolicy_destroy(db.fp)
		}
	}
}

func (db *DB) Open(name string, createIfMissing bool, maxOpenFiles int,
	writeBufSize int, cacheSize int64, compression int) error {
	db.opt = C.rocksdb_options_create()
	C.rocksdb_options_set_create_if_missing(db.opt, boolToUchar(createIfMissing))
	C.rocksdb_options_set_write_buffer_size(db.opt, C.size_t(writeBufSize))
	C.rocksdb_options_set_max_open_files(db.opt, C.int(maxOpenFiles))
	C.rocksdb_options_set_compression(db.opt, C.int(compression))

	var block_options = C.rocksdb_block_based_options_create()
	if cacheSize > 0 {
		db.cache = C.rocksdb_cache_create_lru(C.size_t(cacheSize))
		C.rocksdb_block_based_options_set_block_cache(block_options, db.cache)
	} else {
		C.rocksdb_block_based_options_set_no_block_cache(block_options, 1)
	}
	db.fp = C.rocksdb_filterpolicy_create_bloom(10)
	C.rocksdb_block_based_options_set_filter_policy(block_options, db.fp)

	C.rocksdb_options_set_block_based_table_factory(db.opt, block_options)

	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	var errStr *C.char
	db.db = C.rocksdb_open(db.opt, cname, &errStr)
	if errStr != nil {
		defer C.free(unsafe.Pointer(errStr))
		return errors.New(C.GoString(errStr))
	}

	db.rOpt = C.rocksdb_readoptions_create()
	db.wOpt = C.rocksdb_writeoptions_create()

	return nil
}

func (db *DB) Put(rawKey, value []byte, wb *WriteBatch) error {
	var ck, cv *C.char
	if len(rawKey) > 0 {
		ck = (*C.char)(unsafe.Pointer(&rawKey[0]))
	}
	if len(value) > 0 {
		cv = (*C.char)(unsafe.Pointer(&value[0]))
	}

	if wb == nil {
		var errStr *C.char
		C.rocksdb_put(db.db, db.wOpt, ck, C.size_t(len(rawKey)), cv,
			C.size_t(len(value)), &errStr)
		if errStr != nil {
			defer C.free(unsafe.Pointer(errStr))
			return errors.New(C.GoString(errStr))
		}
	} else {
		C.rocksdb_writebatch_put(wb.batch, ck, C.size_t(len(rawKey)), cv,
			C.size_t(len(value)))
	}

	return nil
}

func (db *DB) Get(opt *ReadOptions, rawKey []byte) ([]byte, error) {
	var ck = (*C.char)(unsafe.Pointer(&rawKey[0]))

	var rOpt = db.rOpt
	if opt != nil && opt.rOpt != nil {
		rOpt = opt.rOpt
	}

	var errStr *C.char
	var vallen C.size_t
	var cv = C.rocksdb_get(db.db, rOpt, ck, C.size_t(len(rawKey)), &vallen, &errStr)

	var err error
	if errStr != nil {
		defer C.free(unsafe.Pointer(errStr))
		err = errors.New(C.GoString(errStr))
	}

	if cv != nil {
		defer C.free(unsafe.Pointer(cv))
		return C.GoBytes(unsafe.Pointer(cv), C.int(vallen)), err
	}

	return nil, err
}

func (db *DB) Del(rawKey []byte, wb *WriteBatch) error {
	var ck = (*C.char)(unsafe.Pointer(&rawKey[0]))

	if wb == nil {
		var errStr *C.char
		C.rocksdb_delete(db.db, db.wOpt, ck, C.size_t(len(rawKey)), &errStr)
		if errStr != nil {
			defer C.free(unsafe.Pointer(errStr))
			return errors.New(C.GoString(errStr))
		}
	} else {
		C.rocksdb_writebatch_delete(wb.batch, ck, C.size_t(len(rawKey)))
	}

	return nil
}

func (db *DB) NewReadOptions(createSnapshot bool) *ReadOptions {
	var opt = new(ReadOptions)
	opt.rOpt = C.rocksdb_readoptions_create()
	if createSnapshot {
		opt.snap = C.rocksdb_create_snapshot(db.db)
		C.rocksdb_readoptions_set_snapshot(opt.rOpt, opt.snap)
		opt.db = db.db
	}
	return opt
}

func (opt *ReadOptions) SetFillCache(fillCache bool) {
	C.rocksdb_readoptions_set_fill_cache(opt.rOpt, boolToUchar(fillCache))
}

func (opt *ReadOptions) Destroy() {
	if opt.rOpt != nil {
		C.rocksdb_readoptions_destroy(opt.rOpt)
		opt.rOpt = nil
	}

	if opt.snap != nil {
		C.rocksdb_release_snapshot(opt.db, opt.snap)
		opt.snap = nil
		opt.db = nil
	}
}

func (db *DB) NewWriteBatch() *WriteBatch {
	return &WriteBatch{C.rocksdb_writebatch_create()}
}

func (db *DB) Commit(wb *WriteBatch) error {
	if wb != nil && wb.batch != nil {
		var errStr *C.char
		C.rocksdb_write(db.db, db.wOpt, wb.batch, &errStr)
		C.rocksdb_writebatch_clear(wb.batch)
		if errStr != nil {
			defer C.free(unsafe.Pointer(errStr))
			return errors.New(C.GoString(errStr))
		}
	}

	return nil
}

func (wb *WriteBatch) Destroy() {
	if wb.batch != nil {
		C.rocksdb_writebatch_destroy(wb.batch)
		wb.batch = nil
	}
}

func (db *DB) NewIterator(opt *ReadOptions) *Iterator {
	var rOpt = db.rOpt
	if opt != nil && opt.rOpt != nil {
		rOpt = opt.rOpt
	}

	var iter = new(Iterator)
	iter.it = C.rocksdb_create_iterator(db.db, rOpt)

	return iter
}

func (iter *Iterator) Destroy() {
	if iter.it != nil {
		C.rocksdb_iter_destroy(iter.it)
		iter.it = nil
	}
}

func (iter *Iterator) SeekToFirst() {
	C.rocksdb_iter_seek_to_first(iter.it)
}

func (iter *Iterator) SeekToLast() {
	C.rocksdb_iter_seek_to_last(iter.it)
}

func (iter *Iterator) Seek(key []byte) {
	var ck *C.char
	if len(key) > 0 {
		ck = (*C.char)(unsafe.Pointer(&key[0]))
	}
	C.rocksdb_iter_seek(iter.it, ck, C.size_t(len(key)))
}

func (iter *Iterator) Next() {
	C.rocksdb_iter_next(iter.it)
}

func (iter *Iterator) Prev() {
	C.rocksdb_iter_prev(iter.it)
}

func (iter *Iterator) Valid() bool {
	return C.rocksdb_iter_valid(iter.it) != 0
}

func (iter *Iterator) Key() []byte {
	var keyLen C.size_t
	var ck = C.rocksdb_iter_key(iter.it, &keyLen)
	return C.GoBytes(unsafe.Pointer(ck), C.int(keyLen))
}

func (iter *Iterator) Value() []byte {
	var valueLen C.size_t
	var value = C.rocksdb_iter_value(iter.it, &valueLen)
	return C.GoBytes(unsafe.Pointer(value), C.int(valueLen))
}

func boolToUchar(b bool) C.uchar {
	if b {
		return 1
	} else {
		return 0
	}
}
