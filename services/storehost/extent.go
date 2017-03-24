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

package storehost

import (
	"sync/atomic"
	"time"

	"github.com/uber/cherami-server/services/storehost/load"
	"github.com/uber/cherami-server/storage"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

// ExtentObj is allocated one per "open" to an extent
type ExtentObj struct {
	ext    *extentContext
	intent OpenIntent

	notifyC chan<- struct{} // change notification channel

	// the following stores the current read "offset" for this handle
	// within the extent. this is used and updated by ReadMsg, SeekMsg,
	// NextMsg, HasNextMsg, etc.
	addr    storage.Address // address of the last read message
	next    storage.Address // address of the next message
	nextKey storage.Key     // key associated with the next message
}

func newExtentObj(ext *extentContext, intent OpenIntent) (x *ExtentObj) {
	return &ExtentObj{ext: ext, intent: intent}
}

// Delete marks the extent to be deleted when the last reference goes away
func (x *ExtentObj) Delete() {
	x.ext.delete()
}

// Close closes this handle to the extent
func (x *ExtentObj) Close() {
	x.ext.xMgr.closeExtent(x.ext, x.intent, nil)
}

// Mode returns this extent's mode (ie, AppendOnly or TimerQueue)
func (x *ExtentObj) Mode() Mode {
	return x.ext.mode
}

// TODO TODO TODO
// the following are 'low-level' methods that should eventually be deprecated/removed once
// we implement the 'high-level' extent APIs (ex: WriteMsg, ReadMsg, Seal, etc) and move
// all the storehost Thrift APIs to use them. we have these around tempoararily to help
// with the transition.
// in other words, everything below here should eventually go away .. or at least not be
// referenced by the rest of the storehost code.

// extentLock gets the extent-wide lock exclusive
func (x *ExtentObj) extentLock() {
	x.ext.Lock()
}

func (x *ExtentObj) extentUnlock() {
	x.ext.Unlock()
}

// extentRLock gets the extent-wide lock shared
func (x *ExtentObj) extentRLock() {
	x.ext.RLock()
}

func (x *ExtentObj) extentRUnlock() {
	x.ext.RUnlock()
}

// wrappers that modify the {first,last,seal}SeqNum
func (x *ExtentObj) getFirstSeqNum() int64 {
	return atomic.LoadInt64(&x.ext.firstSeqNum)
}

func (x *ExtentObj) setFirstSeqNum(firstSeqNum int64) {
	atomic.StoreInt64(&x.ext.firstSeqNum, firstSeqNum)
}

func (x *ExtentObj) setFirstMsg(firstAddr, firstSeqNum, firstTimestamp int64) {
	atomic.StoreInt64(&x.ext.firstAddr, firstAddr)
	atomic.StoreInt64(&x.ext.firstSeqNum, firstSeqNum)
	atomic.StoreInt64(&x.ext.firstTimestamp, firstTimestamp)
}

func (x *ExtentObj) getLastSeqNum() int64 {
	return x.ext.lastSeqNum // should be called with extentLock held
}

func (x *ExtentObj) setLastSeqNum(lastSeqNum int64) {
	x.ext.lastSeqNum = lastSeqNum // should be called with extentLock held
}

// setLastMsg sets the addr, seqnum, timestamp of the last message;
// should be called with the extentLock held.
func (x *ExtentObj) setLastMsg(lastAddr, lastSeqNum, lastTimestamp int64) {
	x.ext.lastAddr = lastAddr
	x.ext.lastSeqNum = lastSeqNum
	x.ext.lastTimestamp = lastTimestamp
}

func (x *ExtentObj) getSealSeqNum() int64 {
	return atomic.LoadInt64(&x.ext.sealSeqNum)
}

func (x *ExtentObj) setSealSeqNum(sealSeqNum int64) {
	atomic.StoreInt64(&x.ext.sealSeqNum, sealSeqNum) // save sealSeqNum into extInfo
}

// wrappers that call into the mode-specific callbacks
func (x *ExtentObj) messageVisibilityTime(msg *store.AppendMessage) (visibilityTime int64) {
	return x.ext.messageVisibilityTime(msg)
}

func (x *ExtentObj) constructKey(visibilityTime int64, seqNum int64) storage.Key {
	return x.ext.constructKey(visibilityTime, seqNum)
}

func (x *ExtentObj) deconstructKey(key storage.Key) (visibilityTime int64, seqNum int64) {
	vt, sn := x.ext.deconstructKey(key)
	return int64(vt), int64(sn)
}

func (x *ExtentObj) maxSeqNum() (seqNum int64) {
	return int64(x.ext.maxSeqNum())
}

func (x *ExtentObj) constructSealExtentKey(seqNum int64) storage.Key {
	return x.ext.constructSealExtentKey(int64(seqNum))
}

func (x *ExtentObj) deconstructSealExtentKey(key storage.Key) (seqNum int64) {
	return int64(x.ext.deconstructSealExtentKey(key))
}

func (x *ExtentObj) isSealExtentKey(key storage.Key) bool {
	return x.ext.isSealExtentKey(key)
}

// wrappers to low-level store interface
func (x *ExtentObj) storePut(key storage.Key, val storage.Value) (addr storage.Address, err error) {

	t0 := time.Now().UnixNano()

	addr, err = x.ext.store.Put(key, val)

	if err != nil {

		tX := time.Now().UnixNano()

		x.ext.extMetrics.Add(load.ExtentMetricWriteLatency, tX-t0)

		x.ext.extMetrics.Increment(load.ExtentMetricMsgsWritten)
		x.ext.xMgr.hostMetrics.Increment(load.HostMetricMsgsWritten)
		x.ext.extMetrics.Add(load.ExtentMetricBytesWritten, int64(len(val)))
		x.ext.xMgr.hostMetrics.Add(load.HostMetricBytesWritten, int64(len(val)))
	}

	return
}

func (x *ExtentObj) storeSync() {
	x.ext.store.Sync()
}

func (x *ExtentObj) storeGet(addr storage.Address) (key storage.Key, val storage.Value, nextAddr storage.Address, nextKey storage.Key, err error) {

	t0 := time.Now().UnixNano()

	key, val, nextAddr, nextKey, err = x.ext.store.Get(addr)

	if err != nil {

		tX := time.Now().UnixNano()

		x.ext.extMetrics.Add(load.ExtentMetricReadLatency, tX-t0)

		x.ext.extMetrics.Increment(load.ExtentMetricMsgsRead)
		x.ext.xMgr.hostMetrics.Increment(load.HostMetricMsgsRead)
		x.ext.extMetrics.Add(load.ExtentMetricBytesRead, int64(len(val)))
		x.ext.xMgr.hostMetrics.Add(load.HostMetricBytesRead, int64(len(val)))
	}

	return
}

func (x *ExtentObj) storeGetMany(addr storage.Address, numMsgs int32, maxAddr storage.Address) (msgs []storage.KeyValue, nextAddr storage.Address, nextKey storage.Key, err error) {
	return x.ext.store.GetMany(addr, numMsgs, maxAddr)
}

func (x *ExtentObj) storeNext(addr storage.Address) (nextAddr storage.Address, nextKey storage.Key, err error) {
	return x.ext.store.Next(addr)
}

func (x *ExtentObj) storeSeekCeiling(ceilKey storage.Key) (addr storage.Address, key storage.Key, err error) {
	return x.ext.store.SeekCeiling(ceilKey)
}

func (x *ExtentObj) storeSeekFloor(floorKey storage.Key) (addr storage.Address, key storage.Key, err error) {
	return x.ext.store.SeekFloor(floorKey)
}

func (x *ExtentObj) storeSeekFirst() (addr storage.Address, key storage.Key, err error) {
	return x.ext.store.SeekFirst()
}

func (x *ExtentObj) storeSeekLast() (addr storage.Address, key storage.Key, err error) {
	return x.ext.store.SeekLast()
}

func (x *ExtentObj) storeGetKey(addr storage.Address) (key storage.Key, err error) {
	return x.ext.store.GetKey(addr)
}

func (x *ExtentObj) storePurge(endAddr storage.Address) (nextAddr storage.Address, nextKey storage.Key, err error) {
	return x.ext.store.Purge(endAddr)
}

// listen registers a 'notification channel' that will be notified
// of any newly available messages to read
func (x *ExtentObj) listen(notifyC chan<- struct{}) {
	x.ext.listen(notifyC)
}

// unlisten deregisters the "notification channel" on the extent
func (x *ExtentObj) unlisten(notifyC chan<- struct{}) {
	x.ext.unlisten(notifyC)
}
