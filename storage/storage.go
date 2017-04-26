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

// Package storage defines the low-level store (and store manager) interface
package storage

import (
	"strconv"

	"github.com/pborman/uuid"
)

// TODO:
// - define/formalize return error codes

// ExtentUUID is identifies the extent
type ExtentUUID uuid.UUID

// KeyPattern is an enum indicating the pattern of the keys
// that will be used to store values into the extent.
type KeyPattern int

const (
	_ KeyPattern = iota

	// RandomKeys indicates that the keys could be in completely
	// random order.
	RandomKeys

	// IncreasingKeys indicates that the keys used to store
	// values will be in strictly increasing order; ie, each key
	// associatd with a value will be strictly greater than that
	// used for the previous write.
	IncreasingKeys
)

// NotifyFunc is the callback used to notify when new messages are available
// to read. It includes the "key" of the newly available message and its
// address. For extents with "RandomKeys" pattern, this callback will be
// called for every single message. For those with "IncreasingKeys" pattern,
// the callback may *not* be called for every message -- when the callback is
// called, the provided key would be the "high-water mark" of the available
// values. NotifyFunc is also called when messages in an extent have been deleted;
// in this case it would be passed a key equal to 'InvalidKey'.
type NotifyFunc func(key Key, addr Address)

// ExtentInfo contains information about an extent
type ExtentInfo struct {
	Size     int64 // estimated size of extent on disk
	Modified int64 // time extent was modified (in unix-nano)
}

// StoreManager is the storage interface implemented by the low-level stores
// (currently, "Chunky" (append-only) and "Rockstor" (primarily, for indexed
// but can be used for append-only)
type StoreManager interface {

	// OpenExtent opens DB and initializes the state for a given extent.
	// The extent is opened so it is available for read or write.
	// (FIXME: This should probably take in some manager-specific
	// configuration parameters, as an empty struct/interface?)
	OpenExtent(id ExtentUUID, pattern KeyPattern, notify NotifyFunc, failIfNotExist bool) (ExtentStore, error)

	// ListExtents returns list of available extents
	ListExtents() ([]ExtentUUID, error)

	// GetExtentInfo returns information about an extent
	GetExtentInfo(id ExtentUUID) (*ExtentInfo, error)
}

// OpenExtentDB gets a handle to the raw extent DB
// func OpenExtentDB(id ExtentUUID, path string) (ExtentStore, error)

// -- EXTENT STORE -- //
// The extent-store is designed and expected to be used concurrently by
// at most one-writer and any number of readers.

// Key to associate the message with
//    This is a 64-bit value created by StoreHost:
//    - for timer-queues, this could be a combination of seq-num and delivery
//        time (enqueue-time + delay) to ensure that the key is unique.
//    - for append-only streams, this could be a combination of enqueue-time
//        & seq-num (the key needs to be inserted in increasing order for
//        'chunky' for the "Seek()" operation to work efficiently)
type Key uint64

func (k Key) String() string {
	return strconv.FormatUint(uint64(k), 16)
}

// Value is an arbitrary sized byte array (payload of the "message")
type Value []byte

// KeyValue key-value pair
type KeyValue struct {
	Key   Key
	Value Value
}

// Address used to refer to a message.
//    This is a 64-bit value that can be thought of as an 'internal key' that
//    is specific to the low-level storage) that can be used to retrieve the
//     message faster/more optimally than with the "key". The "Seek(..)" call
//    could be used to translate the actual message Key to an Address.
type Address uint64

func (a Address) String() string {
	return strconv.FormatUint(uint64(a), 16)
}

// MinAddr is the smallest valid address for a message
const MinAddr Address = 0

// EOX indicates the End Of eXtent (no more messages in the extent); this value
// needs to be larger than any valid address
const EOX Address = (1 << 64) - 2

// MaxAddr is equal to EOX, and is the largest valid address + 1
const MaxAddr Address = (1 << 64) - 2

// InvalidAddr is a special value used to return an address that is "invalid"
const InvalidAddr Address = (1 << 64) - 1

// InvalidKey indicates an invalid (or unknown) key
const InvalidKey Key = (1 << 64) - 1

// ExtentStore is the interface used to read/write messages for an extent
type ExtentStore interface {

	// Put inserts message into the extent against the (optional) 'key'
	// Args:
	//     key: key to associate with value
	//          - for timer-queues, this is a combination of the delivery
	//          time (enqueue-time + delay) and seqNum
	//          - for append-only, this could just be the msg seq-num
	//          (either way, the assumption is the key is _unique_ (does
	//          not overwrite))
	//     val: byte array of message payload; StoreHost is expected to
	//        serialize the message
	//
	// Returns:
	//    addr: address of the message that was just inserted.
	//        this is returned only if available;  this could be
	//        InvalidAddr, in case it isnt' readily available for
	//        consumption (like in chunky)
	//    err: error
	Put(key Key, val Value) (addr Address, err error)

	Sync()

	// Get takes in the "address" of the message to read
	// Args:
	//    addr: Address of the message to retrieve
	//
	// Returns:
	//    key: key of the message
	//    val: value (payload) of the message
	//    next: Address of the very next message (if available)
	//    err: error, if any
	Get(addr Address) (key Key, val Value, nextAddr Address, nextKey Key, err error)

	// GetMany returns a batch of max 'numMsgs' between [addr, maxAddr)
	// Args:
	//    numMsgs: max number of messages to return
	//    addr: Address of the first message to retrieve
	//     maxAddr: upper bound of the address for messages to be returned;
	//    can be 'EOX' for to get all messages starting from 'addr'.
	//
	// Returns:
	//    msgs: array of key-value pair of messages in successive order
	//        of addresses
	//    nextAddr: Address of the very next message that follows the last
	//        one returned in 'msgs'
	//    nextKey: Key of the very next message that follows the last one
	//        returned in 'msgs'
	//    err: error, if any
	GetMany(addr Address, numMsgs int32, maxAddr Address) (msgs []KeyValue, nextAddr Address, nextKey Key, err error)

	// Next returns the address of the next message (in address order)
	// Args:
	//    addr: Address of the message whose 'next' is being queried. This
	//        could refer to an address that does not exist, in which case
	//        the expectation is to return the address of the first
	//        message whose address is greater than the given address.
	//
	// Returns:
	//    nextAddr: Address of the message after 'addr'
	//    nextKey: Key of the next message (corresponding to nextAddr)
	//     err: error, if any
	Next(addr Address) (nextAddr Address, nextKey Key, err error)

	// Prev: returns the address of the previous message (by address)
	// Args:
	//    addr: Address of the message whose 'prev' is being queried
	//
	// Returns:
	//    prevAddr: Address of the message before given 'addr'
	//    prevKey: Key of the message before given 'addr'
	//     err: error, if any
	// Prev(addr Address) (prevAddress, prevKey Key, err error) // TODO

	// SeekCeiling returns the address of the message for the given 'key' or the
	// first one following it (basically message with key ">=" given key).
	// If there are no such key exists, it would return an error, with
	// 'addr == EOX' and 'retKey == InvalidKey'.
	// Args:
	//    ceilKey: key of the message to 'ceil' to
	//
	// Returns:
	//    addr: address of the message whose key is equal or greater than
	//          the specified key
	//    key:  key of the message at 'addr' (could be equal to 'key' or
	//          greater than it)
	//    err:  error, if any
	//
	SeekCeiling(ceilKey Key) (addr Address, key Key, err error)

	// SeekFloor returns the address of the message for the given 'key' or the
	// first one following it (basically message with key "<=" given key).
	// If there are no such key exists, it would return an error, with
	// 'addr == MinAddr' and 'retKey == InvalidKey'.
	// Args:
	//    floorKey: key of the message to find 'floor' for
	//
	// Returns:
	//    addr: address of the message whose key is equal or less than
	//          the specified key
	//    key:  key of the message at 'addr' (could be equal to 'key' or
	//          less than it)
	//    err:  error, if any
	//
	SeekFloor(floorKey Key) (addr Address, key Key, err error)

	// SeekFirst returns the address and key of the first message available
	// Args:
	//    none
	//
	// Returns:
	//    addr: address of the first message
	//    key: key of the first message
	//    err: error, if any
	SeekFirst() (addr Address, key Key, err error)

	// SeekLast returns the address and key of the last message available
	// Args:
	//    none
	//
	// Returns:
	//    addr: address of the last message
	//    key: key of the last message
	//    err: error, if any
	SeekLast() (addr Address, key Key, err error)

	// GetKey returns the key of the message at the given address. In a
	// sense, it is the inverse of the 'Seek' operation, in that it finds
	// the key from the address.
	// Args:
	//    addr: the message whose key is sought
	//
	// Returns:
	//    key: key of the message
	//    err: error, if any
	//
	GetKey(addr Address) (key Key, err error)

	// Purge deletes all messages whose address is less than or equal to the
	// given address (including the message with the given address). Once a
	// purge is accepted, no messages less than or equal to the given
	// address should be visible
	// Args:
	//    addr: address upto which all the messages are to be purged
	//
	// Returns:
	//    nextAddr: the address of the next available message, if exists
	//    nextKey: the key of the next available message, if exists
	//    err: error, if any
	Purge(endAddr Address) (nextAddr Address, nextKey Key, err error)

	// DeleteExtent deletes all messages and data associated with the extent
	// when the extent is closed. In other words, marks this extent for delete
	// on close.
	DeleteExtent()

	// Close does necessary clean-up to close this extent
	// Args:
	//    none
	//
	// Returns:
	//     none
	Close()

	// CloseExtentDB is closes the handle when opened using OpenExtentDB
	// Args:
	//    none
	//
	// Returns:
	//    none
	CloseExtentDB()
}

// -- Misc utility functions -- //

// String implements the Stringer interface for ExtentUUID
func (t ExtentUUID) String() string {
	return uuid.UUID(t).String()
}

// String implements the Stringer interface for KeyPattern
func (t KeyPattern) String() string {
	switch t {
	case RandomKeys:
		return "RandomKeys"
	case IncreasingKeys:
		return "IncreasingKeys"
	default:
		return "[unknown key pattern]"
	}
}
