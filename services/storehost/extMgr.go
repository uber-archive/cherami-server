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
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/storehost/load"
	"github.com/uber/cherami-server/storage"
	"github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

type (
	// ExtentManager contains the map of all open extent-contexts
	ExtentManager struct {

		// lock, primarily used to synchronize the 'extents' map (below)
		sync.RWMutex

		// extent context for every extent (really 'map[uuid.UUID]*extentContext')
		extents map[string]*extentContext

		// number of open extents
		numExtents int64
		// - incremented/decremented atomically

		// storeMgr is the interface to backend storage
		storeMgr storage.StoreManager

		//logger
		logger bark.Logger

		// metrics client
		m3Client metrics.Client

		// hostMetrics refers to metrics that are
		// aggregated at host level and reported
		// to the controller
		hostMetrics *load.HostMetrics

		// factory for creating load reporters for each extent
		loadReporterFactory common.LoadReporterDaemonFactory
	}

	// extentContext: contains objects and methods global to an extent.
	// this includes information shared between various opens to the same
	// extent; ie, only of these objects is initialized per open extent
	extentContext struct {

		// this is the "extentLock" that is primarily intended for use to synchronize
		// with SealExtent (used by both SealExtent and OpenAppendStream routines).
		// It is also used to protect beginSeqNum, lastSeqNum, sealSeqNum, and other
		// members of the extentContext, as well; refer comments.
		sync.RWMutex

		// the extent UUID
		id uuid.UUID // the extent UUID

		// mode: {AppendOnly, TimerQueue, Log}
		mode Mode

		// these are used to abstract the difference in behavior between the various
		// modes (ie, AppendOnly vs TimerQueue)
		modeSpecificCallbacks

		// store is the 'handle' to the low-level extent-store
		store storage.ExtentStore

		// xMgr is a pointer to the ExtentManager
		xMgr *ExtentManager

		// ref-count on this structure
		ref uint32
		//  - incrementing ref should be done atomically with 'extent-lock' held
		//   to to ensure the structure doesn't get torn down (when ref == 0)
		//  - decrementing the ref can be done atomically (without locks); but when
		//    ref is 0, the 'extentLock' needs to be acquired and the ref re-checked
		//    before initiating clean-up/tear-down

		// the following are used to indicate whether the extent has been
		// initialized, deleted or closed respectively.
		initialized, deleted, closed bool
		// - updates require extent-lock exclusive
		// - reads at least require extent-lock shared
		//   NB: all of these variables change to 'true' only once during the
		//   lifetime of the extentContext

		// indicates if this extent was previously written to
		previouslyOpenedForWrite int32
		// - updated atomically with extent-lock at least shared

		// indicates if this extent has already been opened for replication
		openedForReplication int32
		// - updated atomically with extent-lock at least shared

		// list of listeners for changes to this extent
		listeners []chan<- struct{}
		// - updates need to hold the notify-lock exclusive
		// - reads need to hold the notify-lock shared

		// notifyLock is used to protect the list of listeners, above
		notifyLock sync.RWMutex

		// beginSeqNum: int64 that contains the seqNum of the first message
		// that could ever be consumed. It may or may not be 'available' on a
		// timer queue.
		beginSeqNum int64

		// lastSeqNum: int64 that contains the seqNum of last message
		// that was written to this extent (rename to 'maxSeqNum'?)
		//  - "-1" indicates this is unknown or has not been updated.
		//  - otherwise, indicates the seqnum of last message
		lastSeqNum int64
		//  - reads/writes need to hold the 'extentLock' shared/exclusive.
		//  - this is updated by the "OpenAppendStream" go-routine for each msg
		// after it has written to storage; and this value is read by SealExtent
		// to decide whether it is safe to seal the extent at a given seal-
		// seqnum.

		// sealSeqNum: int64 that indicates the seqnum at which this is sealed
		//  - "-1" indicates this is unknown or has not been updated.
		//  - "MaxInt64" indicates that the extent is not sealed.
		//  - otherwise, indicates the seqNum at which the extent is sealed
		sealSeqNum int64
		//  - writes need to hold 'extentLock' and modify atomically.
		//  - reads (that do not hold 'extentLock') need to it atomically.
		//  - this is written to by SealExtent and is read by both routines in
		//  OpenAppendStream and OpenReadStream to ensure we don't read/write
		//  beyond the seal seqnum.

		// LoadReporter job for reporting store extent metric
		loadReporter common.LoadReporterDaemon

		// various metrics tracked real-time to report for load balancing
		// NB: these metrics are cleared out every time they are reported
		extMetrics *load.ExtentMetrics
		// unix nanos when the last report happened
		lastLoadReportedTime int64
	}
)

// OpenIntent is used to indicate what the particular call to OpenExtent is intended for
type OpenIntent int

const (
	_ OpenIntent = iota

	// OpenIntentAppendStream is used by OpenAppendStream
	OpenIntentAppendStream

	// OpenIntentReadStream is used by OpenReadStream
	OpenIntentReadStream

	// OpenIntentSealExtent is used by SealExtent
	OpenIntentSealExtent

	// OpenIntentGetAddressFromTimestamp is used by GetAddressFromTimestamp
	OpenIntentGetAddressFromTimestamp

	// OpenIntentGetExtentInfo is used by GetExtentInfo
	OpenIntentGetExtentInfo

	// OpenIntentPurgeMessages is used by PurgeMessages
	OpenIntentPurgeMessages

	// OpenIntentReplicateExtent is used by ReplicateExtent
	OpenIntentReplicateExtent
)

func (t OpenIntent) String() string {

	switch t {
	case OpenIntentAppendStream:
		return "OpenIntentAppendStream"
	case OpenIntentReadStream:
		return "OpenIntentReadStream"
	case OpenIntentSealExtent:
		return "OpenIntentSealExtent"
	case OpenIntentGetAddressFromTimestamp:
		return "OpenIntentGetAddressFromTimestamp"
	case OpenIntentGetExtentInfo:
		return "OpenIntentGetExtentInfo"
	case OpenIntentPurgeMessages:
		return "OpenIntentPurgeMessages"
	case OpenIntentReplicateExtent:
		return "OpenIntentReplicateExtent"
	default:
		return fmt.Sprintf("Invalid OpenIntent: %d", t)
	}
}

const (
	// storeExtentLoadReportingInterval is the interval store host load is reported to controller
	storeExtentLoadReportingInterval = 2 * time.Second
)

// NewExtentManager initializes and returns a new 'extent manager'
func NewExtentManager(storeMgr storage.StoreManager, m3Client metrics.Client, hostMetrics *load.HostMetrics, log bark.Logger) (xMgr *ExtentManager) {

	return &ExtentManager{
		storeMgr:    storeMgr,
		extents:     make(map[string]*extentContext),
		logger:      log,
		m3Client:    m3Client,
		hostMetrics: hostMetrics,
	}
}

// define error 'constants'
var (
	errClosePending  = errors.New("extent pending close")
	errDeletePending = errors.New("extent pending delete")
)

// OpenExtent opens DB and initializes state for a given extent and returns a 'handle' to it
func (xMgr *ExtentManager) OpenExtent(id uuid.UUID, mode Mode, intent OpenIntent) (x *ExtentObj, err error) {

retryOpen:
	// get global lock exclusive (reading/writing 'extents' map)
	xMgr.Lock()

	ext, exists := xMgr.extents[string(id)]
	if exists {

		err = ext.reference()

	} else {

		ext = &extentContext{
			id:                   id,
			mode:                 mode,
			xMgr:                 xMgr,
			ref:                  1,
			initialized:          false,
			closed:               false,
			deleted:              false,
			extMetrics:           load.NewExtentMetrics(),
			lastLoadReportedTime: time.Now().UnixNano(),
		}

		xMgr.extents[string(id)] = ext

		err = nil
	}

	// drop xMgr global lock, while we initialize/prepare extent
	xMgr.Unlock()

	switch err {
	case nil:

		// if we just created the extentContext, update metrics
		if !exists {
			numExtents := atomic.AddInt64(&xMgr.numExtents, 1)
			xMgr.m3Client.UpdateGauge(metrics.ExtentManagerScope, metrics.StorageOpenExtents, numExtents) // metrics
			xMgr.logger.WithField("context", fmt.Sprintf("ext=%v numExtents=%d", id, numExtents)).
				Info("ExtMgr: extent opened")
		}

		// no errors -> move on

	case errClosePending:
		// if the extent is pending close, wait a while a while for
		// it to be torn down and try again
		time.Sleep(20 * time.Millisecond)
		goto retryOpen

	case errDeletePending:
		// if the extent has been marked for deletion, fail this request
		return nil, err

	default:
		// any other errors, fail
		return nil, err
	}

	// now we have a reference on the extent, now 'prepare' it for open
	if err = ext.prepareForOpen(intent); err != nil {

		// on failure call 'closeExtent' to remove reference and do any
		// necessary clean-up (including updating the 'extents' map)
		xMgr.closeExtent(ext, intent)

		return nil, err
	}

	return newExtentObj(ext, intent), nil // return a new extentObj for the extent
}

// closeExtent decrements the ref-count on the extent; if it falls to zero, this
// does necessary clean-up, including closing the underlying DB.
func (xMgr *ExtentManager) closeExtent(ext *extentContext, intent OpenIntent) {

	// update metrics
	switch intent {
	case OpenIntentAppendStream:
		ext.extMetrics.Decrement(load.ExtentMetricNumWriteConns)
		xMgr.hostMetrics.Decrement(load.HostMetricNumWriteConns)
	case OpenIntentReadStream:
		ext.extMetrics.Decrement(load.ExtentMetricNumReadConns)
		xMgr.hostMetrics.Decrement(load.HostMetricNumReadConns)
	case OpenIntentReplicateExtent:
		// mark as not open for replication
		atomic.StoreInt32(&ext.openedForReplication, 0)
	}

	// dereference, and if this was the last reference, remove from map
	if ext.dereference() {

		// get exclusive lock, since we could potentially be deleting from the map
		xMgr.Lock()
		delete(xMgr.extents, string(ext.id)) // remove from map
		xMgr.Unlock()

		numExtents := atomic.AddInt64(&xMgr.numExtents, -1)
		xMgr.m3Client.UpdateGauge(metrics.ExtentManagerScope, metrics.StorageOpenExtents, numExtents) // metrics
		xMgr.logger.WithField("context", fmt.Sprintf("ext=%v numExtents=%d", ext.id, numExtents)).
			Info("ExtMgr: extent closed")

		return
	}

	// there still are active references; nothing to be done ..
	return
}

// ListExtents returns list of extents on store
func (xMgr *ExtentManager) ListExtents() (extentIDs []string, err error) {

	extents, err := xMgr.storeMgr.ListExtents()

	if len(extents) > 0 {

		extentIDs = make([]string, len(extents))

		for i, x := range extents {
			extentIDs[i] = string(x)
		}
	}

	return
}

// ExtentInfo contains basic information about the extent
type ExtentInfo struct {
	Size     int64
	Modified int64
}

// GetExtentInfo returns info about an extent
func (xMgr *ExtentManager) GetExtentInfo(extentID string) (info *ExtentInfo, err error) {

	xInfo, err := xMgr.storeMgr.GetExtentInfo(storage.ExtentUUID(extentID))

	if err != nil {
		return nil, err
	}

	return &ExtentInfo{Size: xInfo.Size, Modified: xInfo.Modified}, nil
}

// IsExtentOpenedForReplication checks whether an extent is already opened for replication
func (xMgr *ExtentManager) IsExtentOpenedForReplication(extentID string) bool {
	xMgr.RLock()
	defer xMgr.RUnlock()

	extentUUID := uuid.Parse(extentID)
	ext, exists := xMgr.extents[string(extentUUID)]
	if exists {
		return atomic.LoadInt32(&ext.openedForReplication) > 0
	}
	return false
}

type seqNumSnapshot struct {
	snapshotTime int64
	beginSeqNum  int64
	lastSeqNum   int64
}

// getAllExtentsSeqNumSnapshot returns a snapshot of seqnums for all currently
// active extents, used by 'queueMonitor'.
// TODO: we need to update this mechanism a bit .. in order to better handle
// extents that might come and go in between queueMonitor's queries.
func (xMgr *ExtentManager) getAllExtentsSeqNumSnapshot() map[string]*seqNumSnapshot {

	xMgr.RLock()
	defer xMgr.RUnlock()

	snapshot := make(map[string]*seqNumSnapshot, len(xMgr.extents))

	for _, ext := range xMgr.extents {
		ext.RLock()
		snapshot[uuid.UUID.String(ext.id)] = &seqNumSnapshot{
			snapshotTime: time.Now().UnixNano(),
			beginSeqNum:  ext.getBeginSeqNum(),
			lastSeqNum:   ext.getLastSeqNum(),
		}
		ext.RUnlock()
	}

	return snapshot
}

// reference adds a reference to the extentContext
func (ext *extentContext) reference() (err error) {

	// get extent-lock shared while we get a reference on the
	// extent-context to ensure this doesn't get deleted/closed
	ext.RLock()
	defer ext.RUnlock()

	// check if extent has already been marked for deletion
	if ext.deleted {
		return errDeletePending
	}

	if ext.closed {
		return errClosePending
	}

	// atomically add reference; this would prevent the extent
	// from being closed/torn-down
	atomic.AddUint32(&ext.ref, 1)

	return nil // success
}

// dereference drops a reference on extentContext; if the reference goes
// down to zero, it returns 'true' to indicate that it has been torn-down
func (ext *extentContext) dereference() (closed bool) {

	// atomically remove reference; if the reference goes down to zero,
	// tear down the in-memory context and clean-up things
	if atomic.AddUint32(&ext.ref, ^uint32(0)) == 0 {

		ext.Lock()

		// double-check reference with exclusive lock, in case another
		// thread got a reference before we acquired the lock, or it
		// referenced/dereferenced in that time and is already tearing
		// down this extent
		if atomic.LoadUint32(&ext.ref) != 0 || ext.closed {

			ext.Unlock()

			// another thread got a reference
			return false
		}

		// mark extent as "closed" under exclusive lock; this ensures
		// no-one can get a reference until this is completely torn-down
		ext.closed = true

		ext.Unlock()

		// assert(len(ext.listeners) == 0) //

		// do any necessary clean-up of extent-context structures

		if ext.store != nil {

			// close the handle to the extent store; if the extent was
			// marked for deletion, it would be taken care of by the
			// underlying store as part of this handle closure
			ext.store.Close()
			ext.store = nil
		}

		// shutdown reporting load for this extent
		if ext.loadReporter != nil {
			ext.loadReporter.Stop()
		}

		return true
	}

	return false
}

func (ext *extentContext) getBeginSeqNum() int64 {
	return atomic.LoadInt64(&ext.beginSeqNum)
}

func (ext *extentContext) getLastSeqNum() int64 {
	return ext.lastSeqNum // should be called with extentLock held
}

// prepareForOpen is called once per "open" for an extent; it calls into
// 'initialize' the extentContext if it hasn't already been.
func (ext *extentContext) prepareForOpen(intent OpenIntent) (err error) {

	// get shared lock
	ext.RLock()

	if !ext.initialized {

		// drop lock, since 'initialize' will need it exclusive
		ext.RUnlock()

		err = ext.initialize(intent)

		if err != nil {
			return err
		}

		// re-acquire lock shared, post initialization
		ext.RLock()
	}

	defer ext.RUnlock() // remember to release lock before leaving

	// check if the 'intent' is okay
	switch intent {
	case OpenIntentAppendStream:
		// prevent opening an extent for write that was previously opened for write
		if !atomic.CompareAndSwapInt32(&ext.previouslyOpenedForWrite, 0, 1) {
			return fmt.Errorf("extent previously opened for write")
		}
	case OpenIntentReplicateExtent:
		// prevent opening an extent for write that is already open for replication
		if !atomic.CompareAndSwapInt32(&ext.openedForReplication, 0, 1) {
			return fmt.Errorf("extent already open for replication")
		}
	}

	// update metrics
	switch intent {
	case OpenIntentAppendStream:
		ext.extMetrics.Increment(load.ExtentMetricNumWriteConns)
		ext.xMgr.hostMetrics.Increment(load.HostMetricNumWriteConns)
	case OpenIntentReadStream:
		ext.extMetrics.Increment(load.ExtentMetricNumReadConns)
		ext.xMgr.hostMetrics.Increment(load.HostMetricNumReadConns)
	}

	return nil
}

// failIfNotExist computes the 'failIfNotExist' argument to be used with the
// OpenExtent call based on the open-intent
func failIfNotExist(intent OpenIntent, logger bark.Logger) bool {

	switch intent {
	case OpenIntentAppendStream:
		fallthrough
	case OpenIntentReadStream:
		fallthrough
	case OpenIntentSealExtent:
		fallthrough
	case OpenIntentReplicateExtent:
		// create if it does not exist
		return false

	case OpenIntentGetAddressFromTimestamp:
		fallthrough
	case OpenIntentGetExtentInfo:
		fallthrough
	case OpenIntentPurgeMessages:
		// fail if it does not exist
		return true
	}

	logger.WithField(`intent`, intent).Error(`unrecognized OpenIntent`)
	return true
}

// initialize is called once per extent
func (ext *extentContext) initialize(intent OpenIntent) (err error) {

	// extent lock held exclusive during initialization
	ext.Lock()
	defer ext.Unlock()

	// double-check with exclusive lock
	if !ext.initialized {

		// initialize mode-specific callbacks and keyPattern (to use with OpenExtent)
		var keyPattern storage.KeyPattern

		switch ext.mode {
		case AppendOnly:
			fallthrough
		case Log:
			ext.modeSpecificCallbacks = getAppendQueueCallbacks(appendQueueSeqNumBits)
			keyPattern = storage.IncreasingKeys

		case TimerQueue:
			ext.modeSpecificCallbacks = getTimerQueueCallbacks(timerQueueSeqNumBits)
			keyPattern = storage.RandomKeys

		default:
			return fmt.Errorf("unknown mode: %d", ext.mode)
		}

		// open extent-store and remember handle
		ext.store, err = ext.xMgr.storeMgr.OpenExtent(
			storage.ExtentUUID(ext.id),
			keyPattern,
			func(storage.Key, storage.Address) { ext.notify() },
			failIfNotExist(intent, ext.xMgr.logger),
		)

		if err != nil {
			return fmt.Errorf("OpenExtent failed: %v", err)
		}

		// -- read and initialize {seal,begin,last}-seqnum -- //

		// start by assuming this is an empty extent
		emptyExtent := true

		// read in seal-seqnum
		if ext.sealSeqNum, err = ext.readSealSeqNum(); err != nil {

			ext.store.Close()
			ext.store = nil
			return fmt.Errorf("error reading seal-seqnum: %v", err)

		} else if ext.sealSeqNum != math.MaxInt64 {

			emptyExtent = false
		}

		// read in begin-seqnum
		if ext.beginSeqNum, err = ext.readBeginSeqNum(); err != nil {

			ext.store.Close()
			ext.store = nil
			return fmt.Errorf("error reading begin-seqnum: %v", err)

		} else if ext.beginSeqNum != -1 {

			emptyExtent = false
		}

		// read in last-seqnum
		if ext.lastSeqNum, err = ext.readLastSeqNum(); err != nil {

			ext.store.Close()
			ext.store = nil
			return fmt.Errorf("error reading last-seqnum: %v", err)

		} else if ext.lastSeqNum != -1 {

			emptyExtent = false
		}

		// if the extent is not empty, then it was previously opened for write
		if !emptyExtent {
			ext.previouslyOpenedForWrite = 1
		}

		if ext.xMgr.loadReporterFactory != nil {
			ext.loadReporter = ext.xMgr.loadReporterFactory.CreateReporter(storeExtentLoadReportingInterval, ext, ext.xMgr.logger)
			ext.loadReporter.Start()
		}

		ext.initialized = true // mark as initialized
	}

	return nil
}

// delete: marks this extent for deletion; the extent actually gets deleted
// when the last reference goes away.
func (ext *extentContext) delete() {

	// mark extent as deleted under exclusive lock; this ensures any
	// further attempts to 'reference' this extent will fail.
	ext.Lock()
	ext.deleted = true
	ext.Unlock()

	// mark underlying extent to be deleted on close
	ext.store.DeleteExtent()
}

func (ext *extentContext) readBeginSeqNum() (beginSeqNum int64, err error) {

	// FIXME: for timer-queues, this would return the seqNum of the first
	// _available_ message to deliver, and *not* the first enqueued message.

	_, key, err := ext.store.SeekFirst()

	if err == nil && key != storage.InvalidKey {
		_, beginSeqNum = ext.deconstructKey(key)
	} else {
		beginSeqNum = -1
	}

	return
}

func (ext *extentContext) readLastSeqNum() (lastSeqNum int64, err error) {

	// FIXME: for timer-queues, this would return the seqNum of the last
	// _available_ message to deliver, and *not* the last enqueued message.

	_, key, err := ext.store.SeekLast()

	if err == nil && key != storage.InvalidKey {
		_, lastSeqNum = ext.deconstructKey(key)
	} else {
		lastSeqNum = -1
	}

	return
}

func (ext *extentContext) readSealSeqNum() (sealSeqNum int64, err error) {

	// FIXME: we could potentially have multiple "SealExtentKey" markers
	// in an extent, since we allow re-sealing an extent with a lower
	// seqNum. for an _ordered_ key store (like 'rockstor'), the following
	// "ext.SeekCeiling()" for a SealExtentKey with "0" seqNum, will yield the
	// SealExtentKey with the lowest seqNum. But for an append-only store
	// like 'chunky', we need to seek to the end ("ext.SeekLast()") and find
	// the last key, which should have the most recent (and ideally with
	// the least seqNum) SealExtentKey.

	sealExtentKey := ext.constructSealExtentKey(0)

	_, key, err := ext.store.SeekCeiling(sealExtentKey)

	// if this is a SealExtentKey, extract and return the seqNum
	if err == nil && ext.isSealExtentKey(key) {
		sealSeqNum = int64(ext.deconstructSealExtentKey(key))
	} else {
		// sealSeqNum of 'MaxInt64' -> not sealed
		sealSeqNum = math.MaxInt64
	}

	return
}

// listen returns a 'notification channel' that will be notified of any new writes on the extent
func (ext *extentContext) listen(notifyC chan<- struct{}) {

	ext.notifyLock.Lock()
	defer ext.notifyLock.Unlock()

	// the notification channel should be buffered, else
	// it might miss out on notifications, since they are
	// sent non-blockingly
	// assert( cap(notifyC) > 0 )

	ext.listeners = append(ext.listeners, notifyC)

	return
}

// unlisten deregisters the "notification channel" on the extent
func (ext *extentContext) unlisten(notifyC chan<- struct{}) {

	// get lock exclusive, to modify listener's list
	ext.notifyLock.Lock()
	defer ext.notifyLock.Unlock()

	list := ext.listeners
	for i, c := range list {
		if c == notifyC {
			// delete i-th elem from slice
			list[i] = list[len(list)-1]
			ext.listeners = list[:len(list)-1]
			return
		}
	}

	// not found (-> already deregistered?); ignore
	return
}

// notify notifies all listener channels registered for the extent
func (ext *extentContext) notify() {

	// FIXME: currently, this callback is called literally on every write
	// of a message; this should therefore be optimized to be lockless
	ext.notifyLock.RLock()
	defer ext.notifyLock.RUnlock()

	for _, notifyC := range ext.listeners {
		// use non-blocking send to avoid the possibility of a slow reader to
		// potentially block/stall the writer/other readers on the extent
		select {
		case notifyC <- struct{}{}:
			// sent/buffered on channel
		default:
			// channel "full" -> drop and move on
		}
	}
}

// Report is used for reporting Destination Extent specific load to controller
func (ext *extentContext) Report(reporter common.LoadReporter) {

	if ext.loadReporter == nil {
		return
	}

	now := time.Now().UnixNano()
	intervalSecs := (now - ext.lastLoadReportedTime) / int64(time.Second)
	if intervalSecs == 0 {
		return
	}

	// we have been called in to report load metrics to the controller;
	// collect metrics to create the report and reset them.
	numberOfConnections := ext.extMetrics.Get(load.ExtentMetricNumWriteConns) + ext.extMetrics.Get(load.ExtentMetricNumReadConns)

	var incomingMsgs, incomingBytes, writeMsgLatency int64

	incomingMsgs = ext.extMetrics.GetAndReset(load.ExtentMetricMsgsWritten)
	incomingBytes = ext.extMetrics.GetAndReset(load.ExtentMetricBytesWritten)
	if incomingMsgs != 0 {
		// compute average latency
		writeMsgLatency = ext.extMetrics.GetAndReset(load.ExtentMetricWriteLatency) / incomingMsgs
	}

	var outgoingMsgs, outgoingBytes, readMsgsLatency int64

	outgoingMsgs = ext.extMetrics.GetAndReset(load.ExtentMetricMsgsRead)
	outgoingBytes = ext.extMetrics.GetAndReset(load.ExtentMetricBytesRead)
	if outgoingMsgs != 0 {
		// compute average latency
		readMsgsLatency = ext.extMetrics.GetAndReset(load.ExtentMetricReadLatency) / outgoingMsgs
	}

	extStatus := shared.ExtentStatus_OPEN

	sealSeq := atomic.LoadInt64(&ext.sealSeqNum)
	if sealSeq != seqNumNotSealed {
		extStatus = shared.ExtentStatus_SEALED
	}

	reporter.ReportStoreExtentMetric(ext.id.String(), controller.StoreExtentMetrics{
		NumberOfConnections:     common.Int64Ptr(numberOfConnections),
		IncomingMessagesCounter: common.Int64Ptr(incomingMsgs / intervalSecs),  // report msgsPerSec
		IncomingBytesCounter:    common.Int64Ptr(incomingBytes / intervalSecs), // report bytesPerSec
		WriteMessageLatency:     common.Int64Ptr(writeMsgLatency),
		OutgoingMessagesCounter: common.Int64Ptr(outgoingBytes / intervalSecs), // report msgsPerSec
		OutgoingBytesCounter:    common.Int64Ptr(outgoingBytes / intervalSecs), // report bytesPerSec
		ReadMessageLatency:      common.Int64Ptr(readMsgsLatency),
		ExtentStatus:            &extStatus,
	})

	ext.lastLoadReportedTime = now
}
