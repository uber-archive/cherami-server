// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package manyrocks

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pborman/uuid"
	"github.com/tecbot/gorocksdb"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	s "github.com/uber/cherami-server/storage"

	"github.com/sirupsen/logrus"
)

const rocksdbNotExistError = "does not exist (create_if_missing is false)"

type (
	// Opts are the options passed into creating a new instance of ManyRocks
	Opts struct {
		BaseDir string
		// MaxOpenFilesPerExtent int
		// LRUCacheSize          int
	}

	// ManyRocks implements the storage.StoreManager interface
	ManyRocks struct {
		opts   *Opts
		cache  *gorocksdb.Cache
		logger bark.Logger

		sync.RWMutex

		// deletedExtents map mainly tracks the extents that have been deleted;
		// value true means deleted, false means it is just created (more than the
		// meaning of NOT deleted). If an extent is not being created in this current
		// session, it won't show in the map with value to false. This helps to
		// prevent race condition where we do not have the folder of the extent
		// and it could be created at any moment; for a just created extent, the only
		// way to set it to deleted (value as true) is when it is being dropped.
		deletedExtents map[string]bool
	}

	// Rock implements the storage.ExtentStore interface; it uses
	// a separate RocksDB instance for each extent.
	Rock struct {
		store *ManyRocks

		id         s.ExtentUUID
		path       string
		keyPattern s.KeyPattern
		notify     s.NotifyFunc
		purgeAddr  s.Address // address upto which a purge has been received
		deleted    int32

		// rocksdb stuff
		opts      *gorocksdb.Options
		bbtOpts   *gorocksdb.BlockBasedTableOptions
		db        *gorocksdb.DB
		readOpts  *gorocksdb.ReadOptions
		writeOpts *gorocksdb.WriteOptions
	}
)

var (
	errExtentDoesNotExist = errors.New("error extent does not exist")
	errOpenFailed         = errors.New("error opening extent")
	errUnsupported        = errors.New("unsupported feature")
	errSeekFailed         = errors.New("seek failed")
	errInvalidKey         = errors.New("invalid key")
	errInvalidAddr        = errors.New("invalid address")
	errPurged             = errors.New("key/address below purge level")
	errPutFailed          = errors.New("error writing to store")
	errGetFailed          = errors.New("error reading from store")
	errGetKeyFailed       = errors.New("error reading key")
)

// New creates and initializes a ManyRocks object
func New(opts *Opts, log bark.Logger) (*ManyRocks, error) {

	if err := os.MkdirAll(opts.BaseDir, 0777); err != nil {
		log.Errorf("ManyRocks.New() failed: %v", err)
		return nil, errOpenFailed
	}

	return &ManyRocks{
		opts:           opts,
		logger:         log,
		cache:          gorocksdb.NewLRUCache(8 << 30), // 8 GiB shared LRU cache
		deletedExtents: make(map[string]bool),
	}, nil
}

// OpenExtentDB gets a handle to the raw extent DB
func OpenExtentDB(id s.ExtentUUID, path string) (*Rock, error) {

	// setup RocksDB options
	opts := gorocksdb.NewDefaultOptions()

	opts.SetCreateIfMissing(false)

	db, err := gorocksdb.OpenDb(opts, path)
	if err != nil {
		return nil, err
	}

	// setup read/write options used with IO
	readOpts := gorocksdb.NewDefaultReadOptions()

	writeOpts := gorocksdb.NewDefaultWriteOptions()

	return &Rock{
		id:         id,
		path:       path,
		keyPattern: s.IncreasingKeys,
		notify:     func(key s.Key, addr s.Address) {},
		db:         db,
		opts:       opts,
		readOpts:   readOpts,
		writeOpts:  writeOpts,
		store: &ManyRocks{
			logger: bark.NewLoggerFromLogrus(logrus.StandardLogger()),
		},
	}, nil
}

// CloseExtentDB closes the handle to the raw extent DB
func (t *Rock) CloseExtentDB() {

	t.writeOpts.Destroy()
	t.readOpts.Destroy()
	t.db.Close()
	t.opts.Destroy()
	t.notify = nil
}

// getDBPath returns the base-dir to use for the DB
func (t *ManyRocks) getDBPath(id s.ExtentUUID) string {
	return fmt.Sprintf("%s/%v", t.opts.BaseDir, id) // NB: 'baseDir' should already created
}

// OpenExtent opens an extent, adhering to the storage.ExtentStore interface
func (t *ManyRocks) OpenExtent(id s.ExtentUUID, keyPattern s.KeyPattern, notify s.NotifyFunc, failIfNotExist bool) (s.ExtentStore, error) {

	path := t.getDBPath(id) // create path to db

	if failIfNotExist {
		if t.isExtentDeleted(id) {
			t.logger.WithFields(bark.Fields{
				common.TagDbPath: path,
				common.TagExt:    common.FmtExt(id.String()),
			}).Error(`Extent does not exist`)
			return nil, errExtentDoesNotExist
		}

		_, err := os.Stat(path)
		if err != nil && os.IsNotExist(err) {
			t.logger.WithFields(bark.Fields{
				common.TagDbPath: path,
				common.TagExt:    common.FmtExt(id.String()),
			}).Error(`Extent does not exist`)

			t.setExtentDeleted(id, false)

			return nil, errExtentDoesNotExist
		}
	}

	// ManyRocks can be used with "random" and "increasing" key patterns
	if keyPattern != s.RandomKeys && keyPattern != s.IncreasingKeys {
		t.logger.Errorf("unsupported keyPattern: %v", keyPattern)
		return nil, errUnsupported
	}

	// setup RocksDB options
	opts := gorocksdb.NewDefaultOptions()

	// handle fail-if-not-exist appropriately
	opts.SetCreateIfMissing(!failIfNotExist)

	opts.IncreaseParallelism(runtime.NumCPU())

	bbtOpts := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbtOpts.SetBlockCache(t.cache)
	opts.SetBlockBasedTableFactory(bbtOpts)

	// Control log files
	opts.SetKeepLogFileNum(5)

	// dump stats every minute
	opts.EnableStatistics()
	opts.SetStatsDumpPeriodSec(60)

	// since we are using one instance of RocksDB per extent,
	// limit the number of open files we use per instance.
	opts.SetMaxOpenFiles(64)

	db, err := gorocksdb.OpenDb(opts, path)
	if err != nil {
		t.logger.WithFields(bark.Fields{
			common.TagDbPath: path,
			common.TagExt:    common.FmtExt(id.String()),
			common.TagErr:    err,
		}).Error(`NewRocksDB() failed`)

		// cleanup allocations
		bbtOpts.Destroy()
		opts.Destroy()

		if failIfNotExist && strings.Contains(err.Error(), rocksdbNotExistError) {
			t.setExtentDeleted(id, false)
			return nil, errExtentDoesNotExist
		}

		return nil, errOpenFailed
	}

	// setup read/write options used with IO
	readOpts := gorocksdb.NewDefaultReadOptions()
	// readOpts.SetFillCache(false) // don't fill cache on reads

	writeOpts := gorocksdb.NewDefaultWriteOptions()
	// writeOpts.SetSync(true) // sync every write
	// writeOpts.DisableWAL(true) // disable WAL -> data loss on crash

	// if 'notify' callback is not specified, use a no-op
	// function as placeholder; this avoids the need to do
	// a nil-check in the write-path every time this is
	// called .. for the uncommon case of an nil notify func
	if notify == nil {
		notify = func(key s.Key, addr s.Address) {}
	}

	if !failIfNotExist {
		t.setExtentCreated(id)
	}

	return &Rock{
		id:         id,
		path:       path,
		keyPattern: keyPattern,
		notify:     notify,
		db:         db,
		opts:       opts,
		bbtOpts:    bbtOpts,
		readOpts:   readOpts,
		writeOpts:  writeOpts,
		store:      t,
	}, nil
}

// ListExtents returns a list of extents available on the store
func (t *ManyRocks) ListExtents() (extents []s.ExtentUUID, err error) {

	dir, err := os.Open(t.opts.BaseDir)
	if err != nil {
		return nil, err
	}

	defer dir.Close()

	names, err := dir.Readdirnames(0)

	if err == nil {

		extents = make([]s.ExtentUUID, len(names))

		for i, x := range names {
			extents[i] = s.ExtentUUID(x)
		}
	}

	return
}

// GetExtentInfo returns information about an extent
func (t *ManyRocks) GetExtentInfo(id s.ExtentUUID) (info *s.ExtentInfo, err error) {

	path := t.getDBPath(id) // get path to db

	dir, err := os.Open(path)
	if err != nil {
		return nil, errOpenFailed
	}

	defer dir.Close()

	dirStat, err := dir.Stat()

	if err != nil {
		return nil, errOpenFailed
	}

	modified := dirStat.ModTime().UnixNano() // get modified time

	stat, ok := dirStat.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, errOpenFailed
	}

	// NB: on OSX, the Stat_t does not seem to contain the 'Ctim' member
	// created := time.Unix(int64(stat.Ctim.Sec), int64(stat.Ctim.Nsec)).UnixNano()

	// NB: for some reason, the computation using 'dirStat.Blksize' does not
	// seem to match up with the filesize; it looks like the 'dirStat.Blocks'
	// were computed using a '512' block size!
	// size := dirStat.Blocks * dirStat.Blksize
	size := stat.Blocks * 512

	files, err := dir.Readdir(0)

	if err != nil {
		return nil, errOpenFailed
	}

	for _, f := range files {

		// see comment above
		// size += f.Sys().(*syscall.Stat_t).Blocks * f.Sys().(*syscall.Stat_t).Blksize
		size += f.Sys().(*syscall.Stat_t).Blocks * 512
	}

	return &s.ExtentInfo{Size: size, Modified: modified}, nil
}

// setExtentDeleted set the map entry to deleted status; if the command is
// from db drop action, it is delete for sure so go ahead to set the map;
// if it is due to folder check error or db open errors, we need to make
// sure that we are not aware of the creation; in another word, if the extent
// is just created, there is no point to set the entry to deleted status
// due to empty folder.
func (t *ManyRocks) setExtentDeleted(id s.ExtentUUID, isDeleteAction bool) {
	t.Lock()
	defer t.Unlock()
	if !isDeleteAction {
		// Extent was created in this Storage session
		if deleted, ok := t.deletedExtents[string(id)]; ok && !deleted {
			return
		}
	}
	t.deletedExtents[string(id)] = true
}

func (t *ManyRocks) setExtentCreated(id s.ExtentUUID) {
	t.Lock()
	defer t.Unlock()
	t.deletedExtents[string(id)] = false
}

func (t *ManyRocks) isExtentDeleted(id s.ExtentUUID) bool {
	t.RLock()
	defer t.RUnlock()
	deleted, ok := t.deletedExtents[string(id)]
	return ok && deleted
}

// DeleteExtent marks the extent store for deletion. When the handle is closed
// this would delete the entire extent.
func (t *Rock) DeleteExtent() {

	// mark for delete on close
	atomic.StoreInt32(&t.deleted, 1)

	// since we have updated the extent's 'purgeAddr', notify all
	// listeners there's a "change" in the extent content
	t.notify(s.InvalidKey, s.InvalidAddr)
}

// Close cleans up this interface. Deregisters the notification channel automatically.
func (t *Rock) Close() {

	t.writeOpts.Destroy()
	t.readOpts.Destroy()
	t.db.Close()
	t.bbtOpts.Destroy()
	t.opts.Destroy()

	t.notify = nil

	// if this was marked for delete-on-close, then delete underlying DB
	if atomic.LoadInt32(&t.deleted) > 0 {

		opts := gorocksdb.NewDefaultOptions() // use default opts

		// delete the underlying DB
		if err := gorocksdb.DestroyDb(t.path, opts); err != nil {
			t.store.logger.WithFields(bark.Fields{
				common.TagDbPath: t.path,
				common.TagExt:    common.FmtExt(t.id.String()),
				common.TagErr:    err,
			}).Error(`DestroyDb failed`)
		} else {
			t.store.logger.WithFields(bark.Fields{
				common.TagDbPath: t.path,
				common.TagExt:    common.FmtExt(t.id.String()),
			}).Info(`DestroyDb successful`)

			t.store.setExtentDeleted(t.id, true)
		}

		opts.Destroy() // free-up allocs
	}
}

// getPurgeAddr: return extent's purgeAddr
func (t *Rock) getPurgeAddr() s.Address {
	return s.Address(atomic.LoadUint64((*uint64)(&t.purgeAddr)))
}

// setPurgeAddr: set extent's purgeAddr
func (t *Rock) setPurgeAddr(purgeAddr s.Address) {
	atomic.StoreUint64((*uint64)(&t.purgeAddr), uint64(purgeAddr))
}

// casPurgeAddr: compare and swap extent's purgeAddr atomically
func (t *Rock) casPurgeAddr(oldAddr s.Address, newAddr s.Address) bool {
	return atomic.CompareAndSwapUint64((*uint64)(&t.purgeAddr), uint64(oldAddr), uint64(newAddr))
}

// serializeAddr serializes the 64-bit address into a byte slice
func (t *Rock) serializeAddr(addr s.Address) (bs []byte) {
	bs = make([]byte, 8)
	binary.BigEndian.PutUint64(bs, uint64(addr))
	return
}

// deserializeAddr reads in a 64-bit address from a byte slice
func (t *Rock) deserializeAddr(buf []byte) (addr s.Address) {
	return s.Address(binary.BigEndian.Uint64(buf))
}

// addrFromKey converts from key to address
func addrFromKey(key s.Key) s.Address {
	return s.Address(key)
}

// keyFromAddr converts from address to key
func keyFromAddr(addr s.Address) s.Key {
	return s.Key(addr)
}

// seek returns the address of the message greater than or equal to given key
func (t *Rock) seek(addr s.Address) (nextAddr s.Address, nextKey s.Key, err error) {

	// FIXME: try using RocksDB's "tailing" iterator
	it := t.db.NewIterator(t.readOpts)
	defer it.Close()

	// try and seek to the given addr
	it.Seek(t.serializeAddr(addr))

	if !it.Valid() { // indicates no more messages
		return s.EOX, s.InvalidKey, nil
	}

	addrSlice := it.Key()
	nextAddr = t.deserializeAddr(addrSlice.Data())
	addrSlice.Free()

	nextKey, err = keyFromAddr(nextAddr), it.Err()

	return
}

// Put inserts message into the extent against the given 'key' and returns its 'address'
func (t *Rock) Put(key s.Key, val s.Value) (addr s.Address, err error) {

	if key == s.InvalidKey {
		addr, err = s.InvalidAddr, errInvalidKey
		t.store.logger.WithFields(bark.Fields{`id`: t.id, `key`: key, `valLength`: len(val), `addr`: addr, common.TagErr: err}).Error(`Rock.Put with invalid key`)
		return
	}

	// the key is the 'address'
	addr = addrFromKey(key)

	// do quick check to see if this is below the 'purgeAddr'. Note: this is just a best effort to
	// make sure we aren't writing to below the 'purgeAddr' mark; if there is a race between this Put
	// and a concurrent Purge request, we could potentially 'purge' this message before being read.
	if purgeAddr := t.getPurgeAddr(); purgeAddr != 0 && addr <= purgeAddr {

		t.store.logger.WithFields(bark.Fields{`id`: t.id, `key`: key, `valLength`: len(val), common.TagErr: err}).Error(`Rock.Put error`)
		addr, err = s.InvalidAddr, errPurged
		return
	}

	// store message
	err = t.db.Put(t.writeOpts, t.serializeAddr(addr), []byte(val))

	if err != nil {
		t.store.logger.WithFields(bark.Fields{`id`: t.id, `addr`: addr, `valLength`: len(val), common.TagErr: err}).Error(`Rock.Put error`)
		addr, err = s.InvalidAddr, errPutFailed
		return
	}

	// notify all listeners there's a new message available to read
	// TODO: when used with 'IncreasingKeys' coalesce multiple notify
	// calls and sent notification less often to improve write perf
	t.notify(key, addr)

	// This log line evaluation (even not printed) takes 10% CPU
	// log.WithFields(log.Fields{`id`: t.id,  `key`: key,  `valLength`: len(val),  `addr`: addr,}).Debug(`Rock.Put()`)
	return
}

// Sync ensures the latest key is flushed to disk.
func (t *Rock) Sync() {
}

// Get retrieves the message corresponding to the given address
func (t *Rock) Get(addr s.Address) (key s.Key, val s.Value, nextAddr s.Address, nextKey s.Key, err error) {

	// do quick check to see if this is below the 'purgeAddr'. Note: this is just a best effort to
	// "hide" purged messages; if there is a race between this Get and a concurrent Purge request,
	// we could make available potentially 'purged' messages.
	if addr <= t.getPurgeAddr() {

		t.store.logger.WithFields(bark.Fields{`id`: t.id, `addr`: addr, `key`: key, `0`: 0, `nextAddr`: nextAddr, `nextKey`: nextKey, common.TagErr: err}).Debug(`Rock.Get ERROR`)
		key, val, nextAddr, nextKey, err = s.InvalidKey, nil, s.InvalidAddr, s.InvalidKey, errPurged
		return
	}

	// retrieve message
	valSlice, err := t.db.Get(t.readOpts, t.serializeAddr(addr))

	if err != nil {
		t.store.logger.WithFields(bark.Fields{`id`: t.id, `addr`: addr, `key`: key, `0`: 0, `nextAddr`: nextAddr, `nextKey`: nextKey, common.TagErr: err}).Error(`Rock.Get ERROR`)
		return
	}

	// the 'key' is the address
	key = keyFromAddr(addr)

	val = make([]byte, valSlice.Size())
	copy(val, valSlice.Data())
	valSlice.Free()

	// find the 'next' address
	nextAddr, nextKey, err = t.seek(addr + 1)

	// log.WithFields(log.Fields{`id`: t.id,  `addr`: addr,  `key`: key,  `valLength`: len(val),  `nextAddr`: nextAddr,  `nextKey`: nextKey,  common.TagErr: err,}).Debug(`Rock.Get`) // #perfdisable
	return
}

// GetMany is like Get, but returns a batch of max 'numMsgs' between [addr, endAddr)
func (t *Rock) GetMany(addr s.Address, numMsgs int32, endAddr s.Address) (msgs []s.KeyValue, nextAddr s.Address, nextKey s.Key, err error) {

	if purgeAddr := t.getPurgeAddr(); addr <= purgeAddr {
		if addr, _, err = t.seek(purgeAddr + 1); err != nil {
			nextAddr, nextKey, err = s.InvalidAddr, s.InvalidKey, errPurged
			return
		}
	}

	// readOpts := t.readOpts
	// readOpts.SetFillCache(false) // don't fill cache
	// it := t.db.NewIterator(readOpts)

	it := t.db.NewIterator(t.readOpts)

	for it.Seek(t.serializeAddr(addr)); it.Valid(); it.Next() {

		addrSlice := it.Key()
		addr = t.deserializeAddr(addrSlice.Data())
		addrSlice.Free()

		// collect until we have 'numMsgs' or we find a message
		// with an address greater than the provided 'endAddr'
		if numMsgs--; numMsgs < 0 || addr >= endAddr {
			break
		}

		valSlice := it.Value()
		val := make([]byte, valSlice.Size())
		copy(val, valSlice.Data())
		valSlice.Free()

		msgs = append(msgs, s.KeyValue{Key: keyFromAddr(addr), Value: val})
	}

	// find where we left off and set nextKey/nextAddr
	if it.Valid() {

		addrSlice := it.Key()
		nextAddr = t.deserializeAddr(addrSlice.Data())
		addrSlice.Free()

		nextKey = keyFromAddr(nextAddr)

	} else {

		nextAddr, nextKey, err = s.EOX, s.InvalidKey, nil
	}

	it.Close()

	return
}

// Next returns the address of the the message immediately following the one at the given address
func (t *Rock) Next(addr s.Address) (nextAddr s.Address, nextKey s.Key, err error) {

	// do quick check to see if this is below the 'purgeAddr'. Note: this is just a best effort to
	// "hide" purged messages; if there is a race between this Next and a concurrent Purge request,
	// we could potentially return the address of a 'purged' message.

	if addr <= t.getPurgeAddr() {
		addr = t.getPurgeAddr()
	}

	nextAddr, nextKey, err = t.seek(addr + 1)
	// log.WithFields(log.Fields{`id`: t.id,  `addr`: addr,  `nextAddr`: nextAddr,  `nextKey`: nextKey,  common.TagErr: err,}).Debug(`Rock.Next`) // #perfdisable
	return
}

// SeekCeiling returns the address of the message for the given 'key' or the one following it
func (t *Rock) SeekCeiling(ceilKey s.Key) (addr s.Address, key s.Key, err error) {

	// FIXME: try using RocksDB's "tailing" iterator
	it := t.db.NewIterator(t.readOpts)
	defer it.Close()

	// try and seek to the given 'ceilKey'
	it.Seek(t.serializeAddr(addrFromKey(ceilKey)))

	if !it.Valid() { // indicates no more messages
		return s.EOX, s.InvalidKey, nil
	}

	addrSlice := it.Key()
	addr = t.deserializeAddr(addrSlice.Data())
	addrSlice.Free()

	key, err = keyFromAddr(addr), it.Err()

	if err == nil && addr <= t.getPurgeAddr() {
		addr, key, err = t.seek(t.getPurgeAddr() + 1)
	}

	return
}

// SeekFloor returns the address of the message less than or equal to the given key
func (t *Rock) SeekFloor(floorKey s.Key) (addr s.Address, key s.Key, err error) {

	// FIXME: try using RocksDB's "tailing" iterator
	it := t.db.NewIterator(t.readOpts)
	defer it.Close()

	// try and seek to the given 'floorKey'
	it.Seek(t.serializeAddr(addrFromKey(floorKey)))

	if it.Valid() {

		addrSlice := it.Key()
		addr = t.deserializeAddr(addrSlice.Data())
		addrSlice.Free()

		key, err = keyFromAddr(addr), it.Err()

		if key == floorKey {

			if err == nil && addr <= t.getPurgeAddr() {
				addr, key, err = s.MinAddr, s.InvalidKey, nil
			}

			return
		}

		// we found a key that wasn't equal, which means it was
		// was greater than 'floorKey', since that's the default
		// behaviour of it.Seek(); so move iterator one backwards
		// to find the actual 'floor'
		it.Prev()

	} else {

		// this indicates that there were no messages greater than
		// or equal to 'floorKey'; which means the 'floor' of the
		// given key is the last key in the extent; so seek-to-last.
		it.SeekToLast()
	}

	if it.Valid() {

		addrSlice := it.Key()
		addr = t.deserializeAddr(addrSlice.Data())
		addrSlice.Free()

		key, err = keyFromAddr(addr), it.Err()

		if err == nil && addr <= t.getPurgeAddr() {
			addr, key, err = s.MinAddr, s.InvalidKey, nil
		}

		return
	}

	// !it.Valid() implies there was no previous key, so return
	// MinAddr (ie, before even the first valid key)
	return s.MinAddr, s.InvalidKey, nil
}

// SeekFirst returns the address and key of the first message available
func (t *Rock) SeekFirst() (addr s.Address, key s.Key, err error) {

	// FIXME: try using RocksDB's "tailing" iterator
	it := t.db.NewIterator(t.readOpts)
	defer it.Close()

	// seek to the first key
	it.SeekToFirst()

	if !it.Valid() { // indicates no messages in extent
		addr, key, err = s.EOX, s.InvalidKey, nil
		return
	}

	addrSlice := it.Key()
	addr = t.deserializeAddr(addrSlice.Data())
	addrSlice.Free()

	key, err = keyFromAddr(addr), it.Err()

	if err == nil && addr <= t.getPurgeAddr() {
		addr, key, err = t.seek(t.getPurgeAddr() + 1)
	}

	return
}

// SeekLast returns the address and key of the last message available
func (t *Rock) SeekLast() (addr s.Address, key s.Key, err error) {

	// FIXME: try using RocksDB's "tailing" iterator
	it := t.db.NewIterator(t.readOpts)
	defer it.Close()

	// seek to the last key
	it.SeekToLast()

	if !it.Valid() { // indicates no messages in extent
		addr, key, err = s.EOX, s.InvalidKey, nil
		return
	}

	addrSlice := it.Key()
	addr = t.deserializeAddr(addrSlice.Data())
	addrSlice.Free()

	key, err = keyFromAddr(addr), it.Err()

	if err == nil && addr <= t.getPurgeAddr() {
		addr, key, err = s.EOX, s.InvalidKey, nil
	}

	return
}

// GetKey returns the key of the message at the given address. In a sense, it is the inverse
func (t *Rock) GetKey(getKeyAddr s.Address) (key s.Key, err error) {

	if getKeyAddr <= t.getPurgeAddr() {
		return s.InvalidKey, errPurged
	}

	// seek to given address using it as the "key" and validate it
	addr, key, err := t.seek(getKeyAddr)

	if err != nil {
		key, err = s.InvalidKey, errGetKeyFailed
		return
	}

	// if the found address does not equal to 'getKeyAddr', that
	// means we could not find a message with 'getKeyAddr'.
	if addr != getKeyAddr {
		key, err = s.InvalidKey, errInvalidAddr
		return
	}

	return
}

// Purge deletes all messages whose address is less than or equal
// to the given address.
func (t *Rock) Purge(purgeAddr s.Address) (nextAddr s.Address, nextKey s.Key, err error) {

	// check to see if this is a "redundant" Purge request; ie, if the address
	// to purge to is less than what we have previously accepted.

	curPurgeAddr := t.getPurgeAddr()

	for purgeAddr > curPurgeAddr {

		// compare-and-swap in new address, and if we succeed go ahead
		// and actually delete the messages from the extent ..
		if t.casPurgeAddr(curPurgeAddr, purgeAddr) {

			// since we have updated the extent's 'purgeAddr', notify all listeners
			// there's a "change" in the extent content
			t.notify(s.InvalidKey, s.InvalidAddr)

			type dbgInfo struct {
				filename string
				maxAddr  s.Address
				deleted  bool
				duration time.Duration
			}

			var dbg []*dbgInfo

			t0 := time.Now()

			// run through all the SSTs and delete those whose range of
			// keys are entirely less than the 'purgeAddr'

			for _, file := range t.db.GetLiveFilesMetaData() {

				di := &dbgInfo{filename: file.Name}

				if maxAddr := t.deserializeAddr(file.LargestKey); maxAddr <= purgeAddr {

					t0d := time.Now()
					t.db.DeleteFile(file.Name)
					di.duration = time.Since(t0d)
					di.maxAddr = maxAddr
					di.deleted = true

				} else {
					di.deleted = false
				}

				dbg = append(dbg, di)
			}

			if duration := time.Since(t0); duration > 2*time.Second {

				session := uuid.New()

				t.store.logger.WithFields(bark.Fields{
					`purgeAddr`:    purgeAddr,
					`curPurgeAddr`: curPurgeAddr,
					common.TagExt:  t.id.String(),
					`duration`:     duration,
					`session`:      session,
					`num-files`:    len(dbg),
				}).Info(`Purge took too long!`)

				for _, di := range dbg {

					if di.deleted {
						t.store.logger.WithFields(bark.Fields{
							common.TagExt: t.id.String(),
							`filename`:    di.filename,
							`maxAddr`:     di.maxAddr,
							`deleted`:     di.deleted,
							`duration`:    di.duration,
							`session`:     session,
						}).Info(`RocksDB.DeleteFile`)
					} else {
						t.store.logger.WithFields(bark.Fields{
							common.TagExt: t.id.String(),
							`filename`:    di.filename,
							`deleted`:     di.deleted,
							`session`:     session,
						}).Info(`RocksDB.DeleteFile`)
					}
				}
			}

			return t.seek(purgeAddr + 1)
		}

		// we lost the CAS race; reload and retry
		curPurgeAddr = t.getPurgeAddr()
	}

	// return the next available address, after curPurgeAddr
	return t.seek(curPurgeAddr + 1)
}
