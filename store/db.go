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
	"fmt"
	"unsafe"
)

type DB struct {
	db   *C.rocksdb_t
	opt  *C.rocksdb_options_t
	rOpt *C.rocksdb_readoptions_t
	wOpt *C.rocksdb_writeoptions_t
}

type Iterator struct {
	iter *C.rocksdb_iterator_t
}

type SnapReadOptions struct {
	rOpt *C.rocksdb_readoptions_t
	snap *C.rocksdb_snapshot_t
	db   *C.rocksdb_t
}

type WriteBatch struct {
	batch *C.rocksdb_writebatch_t
}

func NewTableDB() *DB {
	db := new(DB)

	return db
}

func (db *DB) Close() {
	if db.db != nil {
		C.rocksdb_close(db.db)
		db.db = nil

		C.rocksdb_options_destroy(db.opt)
		C.rocksdb_readoptions_destroy(db.rOpt)
		C.rocksdb_writeoptions_destroy(db.wOpt)
	}
}

func (db *DB) Open(name string, createIfMissing bool) error {
	var errStr *C.char

	db.opt = C.rocksdb_options_create()
	C.rocksdb_options_set_create_if_missing(db.opt,
		boolToUchar(createIfMissing))
	C.rocksdb_options_set_write_buffer_size(db.opt, 1024*1024*64)

	var block_cache = C.rocksdb_cache_create_lru(1024 * 1024 * 64)
	var block_cache_compressed = C.rocksdb_cache_create_lru(1024 * 1024 * 64)
	var block_based_table_options = C.rocksdb_block_based_options_create()
	C.rocksdb_block_based_options_set_block_cache_compressed(
		block_based_table_options, block_cache_compressed)
	C.rocksdb_block_based_options_set_block_cache(
		block_based_table_options, block_cache)
	C.rocksdb_options_set_block_based_table_factory(
		db.opt, block_based_table_options)

	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	db.db = C.rocksdb_open(db.opt, cname, &errStr)
	if errStr != nil {
		defer C.free(unsafe.Pointer(errStr))
		return fmt.Errorf(C.GoString(errStr))
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
		C.rocksdb_put(db.db, db.wOpt, ck, C.size_t(len(rawKey)), cv, C.size_t(len(value)),
			&errStr)
		if errStr != nil {
			defer C.free(unsafe.Pointer(errStr))
			return fmt.Errorf(C.GoString(errStr))
		}
	} else {
		C.rocksdb_writebatch_put(wb.batch, ck, C.size_t(len(rawKey)), cv, C.size_t(len(value)))
	}

	return nil
}

func (db *DB) Get(opt *SnapReadOptions, rawKey []byte) ([]byte, error) {
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
		err = fmt.Errorf(C.GoString(errStr))
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
			return fmt.Errorf(C.GoString(errStr))
		}
	} else {
		C.rocksdb_writebatch_delete(wb.batch, ck, C.size_t(len(rawKey)))
	}

	return nil
}

func (db *DB) NewSnapReadOptions() *SnapReadOptions {
	var opt = new(SnapReadOptions)
	opt.rOpt = C.rocksdb_readoptions_create()
	opt.snap = C.rocksdb_create_snapshot(db.db)
	C.rocksdb_readoptions_set_snapshot(opt.rOpt, opt.snap)
	opt.db = db.db
	return opt
}

// Release snapshot
func (opt *SnapReadOptions) Release() {
	if opt.rOpt != nil {
		C.rocksdb_readoptions_destroy(opt.rOpt)
		opt.rOpt = nil
	}

	if opt.snap != nil {
		C.rocksdb_release_snapshot(opt.db, opt.snap)
		opt.snap = nil
	}

	opt.db = nil
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
			return fmt.Errorf(C.GoString(errStr))
		}
	}

	return nil
}

func (wb *WriteBatch) Close() {
	if wb.batch != nil {
		C.rocksdb_writebatch_destroy(wb.batch)
		wb.batch = nil
	}
}

func (db *DB) NewIterator(fillCache bool) *Iterator {
	var iter = new(Iterator)
	var scanOpt = C.rocksdb_readoptions_create()
	defer C.rocksdb_readoptions_destroy(scanOpt)

	C.rocksdb_readoptions_set_fill_cache(scanOpt, boolToUchar(fillCache))
	iter.iter = C.rocksdb_create_iterator(db.db, scanOpt)

	return iter
}

func (db *DB) NewIteratorSnap(opt *SnapReadOptions) *Iterator {
	var iter = new(Iterator)

	if opt != nil {
		iter.iter = C.rocksdb_create_iterator(db.db, opt.rOpt)
	} else {
		return db.NewIterator(true)
	}

	return iter
}

func (iter *Iterator) Close() {
	if iter.iter != nil {
		C.rocksdb_iter_destroy(iter.iter)
		iter.iter = nil
	}
}

func (iter *Iterator) SeekToFirst() {
	C.rocksdb_iter_seek_to_first(iter.iter)
}

func (iter *Iterator) SeekToLast() {
	C.rocksdb_iter_seek_to_last(iter.iter)
}

func (iter *Iterator) Seek(key []byte) {
	var ck *C.char
	if len(key) > 0 {
		ck = (*C.char)(unsafe.Pointer(&key[0]))
	}
	C.rocksdb_iter_seek(iter.iter, ck, C.size_t(len(key)))
}

func (iter *Iterator) Next() {
	C.rocksdb_iter_next(iter.iter)
}

func (iter *Iterator) Prev() {
	C.rocksdb_iter_prev(iter.iter)
}

func (iter *Iterator) Valid() bool {
	return C.rocksdb_iter_valid(iter.iter) != 0
}

/*
func (iter *Iterator) Key() (unitId uint16, dbId uint8, key TableKey) {
	var keyLen C.size_t
	var ck = C.rocksdb_iter_key(iter.iter, &keyLen)
	var rawKey = C.GoBytes(unsafe.Pointer(ck), C.int(keyLen))
	return ParseRawKey(rawKey)
}
*/

func (iter *Iterator) Key() []byte {
	var keyLen C.size_t
	var ck = C.rocksdb_iter_key(iter.iter, &keyLen)
	return C.GoBytes(unsafe.Pointer(ck), C.int(keyLen))
}

func (iter *Iterator) Value() []byte {
	var valueLen C.size_t
	var value = C.rocksdb_iter_value(iter.iter, &valueLen)
	return C.GoBytes(unsafe.Pointer(value), C.int(valueLen))
}

func boolToUchar(b bool) C.uchar {
	if b {
		return 1
	} else {
		return 0
	}
}
