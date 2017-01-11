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

package controllerhost

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
)

const (
	lockStateUnlocked = iota
	lockStateLocked
)

const initialLocksCap = 16

type (
	// LockMgr represents the API for
	// any implementation that manages
	// a set of locks, where each lock
	// is identified by a string key
	LockMgr interface {
		// TryLock attempts to acquire the lock
		// identified by the given key. If it
		// cannot acquire the lock within the
		// timeout, returns false. On success,
		// returns true
		TryLock(key string, timeout time.Duration) bool
		// Unlock releases the lock identified
		// by the key
		Unlock(key string)
		// Size returns the number of
		// keys currently held by the
		// lockmgr
		Size() int64
	}

	// lockEntry is the actual lock
	// corresponding to a key
	lockEntry struct {
		state    int           // locked or unlocked
		nWaiting int           // number of go-routines waiting for lock
		sigCh    chan struct{} // only valid when state is locked
	}

	hashBucket struct {
		sync.Mutex
		locks map[string]*lockEntry
	}

	// lockMgrImpl is an implementation of
	// LockMgr that uses a fixed size hash table
	// for storing the locks (identified by
	// string key)
	lockMgrImpl struct {
		mask    uint32
		buckets []hashBucket
		hashfn  common.HashFunc
		size    int64
		logger  bark.Logger
	}
)

var errInvalidSize = errors.New("Size must be a power of two")
var errNilHashFunc = errors.New("Hash function cannot be nil")

// NewLockMgr creates and returns a new instance of LockMgr.
// The returned lockMgr uses a hashtable of hash tables for
// maintaining the mapping from (key -> lock). The first level
// table is fixed size and each entry in this table contains
// a lock, which needs to be acquired to get access to
// the second level hashtable. The second level hashtable maps
// the key to lock and grows dynamically.
//
//
// @param concurrency
//       Number of entries in the first level table. This directly
//  translates into the number of concurrent threads that can be
//  using the lockMgr at any given point of time. Ideally, this
//  number MUST be the number of cores.
// @param hashfn
//     The hash function to index onto the first level table
// @param logger
//      Logger to use
func NewLockMgr(concurrency int, hashfn common.HashFunc, logger bark.Logger) (LockMgr, error) {

	if !isPowerOfTwo(concurrency) {
		return nil, errInvalidSize
	}

	if hashfn == nil {
		return nil, errNilHashFunc
	}

	return &lockMgrImpl{
		mask:    uint32(concurrency - 1),
		buckets: make([]hashBucket, concurrency),
		hashfn:  hashfn,
		logger:  logger,
	}, nil
}

// TryLock attempts to acquire the lock identified by key
func (lockMgr *lockMgrImpl) TryLock(key string, timeout time.Duration) bool {

	bucket := lockMgr.hash(key)

	bucket.Lock()

	entry := lockMgr.getOrCreateEntry(bucket, key)
	if entry.state == lockStateUnlocked && entry.nWaiting == 0 {
		entry.state = lockStateLocked
		bucket.Unlock()
		return true
	}
	if timeout < time.Millisecond {
		bucket.Unlock()
		return false
	}

	// add ourselves to the waitQ and wait
	// until signaled by the lock holder.
	lockMgr.addToWaitQ(entry)
	bucket.Unlock()
	// sleep until woken up by signal or timeout
	lockMgr.sleepOnWaitQ(entry, timeout)

	var status bool

	bucket.Lock()
	entry.nWaiting--
	// with the bucket lock acquired, inspect
	// the lock state. If its locked, we timed
	// out. If not, we got woken up through
	// sigCh or we timed out at the same
	// instant an UnLock happened.
	if entry.state == lockStateUnlocked {
		entry.state = lockStateLocked
		status = true
		// Since we may have timed out
		// at the exact same instant a
		// sigCh entry was added, its
		// necessary to drain the sigCh.
		select {
		case <-entry.sigCh:
		default:
		}
	}
	bucket.Unlock()

	return status
}

// Unlock releases a previously acquired lock
func (lockMgr *lockMgrImpl) Unlock(key string) {

	bucket := lockMgr.hash(key)

	bucket.Lock()
	defer bucket.Unlock()

	entry, ok := bucket.locks[key]
	if !ok || entry.state == lockStateUnlocked {
		lockMgr.logger.WithField("key", key).Fatal("Unexpected Unlock()")
	}
	if entry.nWaiting == 0 {
		lockMgr.deleteEntry(bucket, key, entry)
		return
	}

	entry.state = lockStateUnlocked

	select {
	// try to wake up one waiter
	case entry.sigCh <- struct{}{}:
	default:
		// sigCh is a buffered channel of size=1 and we verified
		// nWaiting > 0 above, so there should atleast be one
		// waiter waiting  (regardless of whether it timed out or not)
		// Given that, we should be *never* blocked on this channel
		lockMgr.logger.WithField("nWaiting", entry.nWaiting).Fatal("Blocked on sigCh")
	}
}

func (lockMgr *lockMgrImpl) Size() int64 {
	return atomic.LoadInt64(&lockMgr.size)
}

func (lockMgr *lockMgrImpl) getOrCreateEntry(bucket *hashBucket, key string) *lockEntry {
	lockMgr.lazyInitBucket(bucket)
	entry, ok := bucket.locks[key]
	if !ok {
		entry = &lockEntry{}
		bucket.locks[key] = entry
		atomic.AddInt64(&lockMgr.size, 1)
	}
	return entry
}

func (lockMgr *lockMgrImpl) deleteEntry(bucket *hashBucket, key string, entry *lockEntry) {
	entry.state = lockStateUnlocked
	delete(bucket.locks, key)
	atomic.AddInt64(&lockMgr.size, -1)
}

func (lockMgr *lockMgrImpl) sleepOnWaitQ(entry *lockEntry, timeout time.Duration) {
	timer := common.NewTimer(timeout)
	select {
	case <-entry.sigCh:
	case <-timer.Chan():
	}
	timer.Stop()
}

func (lockMgr *lockMgrImpl) addToWaitQ(entry *lockEntry) {
	entry.nWaiting++
	if entry.sigCh == nil {
		entry.sigCh = make(chan struct{}, 1)
	}
}

func (lockMgr *lockMgrImpl) lazyInitBucket(bucket *hashBucket) {
	if bucket.locks != nil {
		return
	}
	bucket.locks = make(map[string]*lockEntry, initialLocksCap)
}

func (lockMgr *lockMgrImpl) hash(key string) *hashBucket {
	hash := lockMgr.hashfn(key) & lockMgr.mask
	return &lockMgr.buckets[hash]
}

func isPowerOfTwo(size int) bool {
	return (size & (size - 1)) == 0
}
