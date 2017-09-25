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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-client-go/common/backoff"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/cache"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/tchannel-go/thrift"
)

type (
	// extentStateMonitor is a background daemon that
	// iterates over all the extents managed by a
	// cherami cluster and triggers state changes,
	// if needed based on the current state of the
	// cluster.
	extentStateMonitor struct {
		started int32
		context *Context
		// cache of store to extentInfo, entries
		// are added when store heartbeats extent
		// info to the controller (through the
		// metrics reporting thrift API). This
		// cache will only keep track of open and
		// sealed extents
		ll           bark.Logger
		storeExtents cache.Cache
		rateLimiter  common.TokenBucket
		shutdownC    chan struct{}
		shutdownWG   sync.WaitGroup
		queueDepth   *queueDepthCalculator
		mi           *mIterator

		// stats gathered during one iteration
		// of the extentMon loop. All of these
		// stats will be reset at the beginning
		// of a new iteration.
		loopStats struct {
			nExtentsByStatus    [maxExtentStates]int64
			nDLQExtentsByStatus [maxExtentStates]int64
			nCGExtentsByStatus  [maxCGExtentStates]int64
		}
	}

	extentCacheEntry struct {
		status shared.ExtentStatus
	}

	consumerGroupInfo struct {
		desc        *shared.ConsumerGroupDescription
		outputHosts map[string]struct{}
	}
)

var (
	maxExtentDownEventsPerSec = 5

	// IntervalBtwnScans is the time that the scanner will sleep between scans. It is exported to allow tests to modify it.
	IntervalBtwnScans   = time.Minute
	intervalBtwnRetries = time.Millisecond * 500
	// arbitrary, but needs to be short lived
	// - cache only needs to keep track of working set
	// - large TTL can lead to stale information
	extentCacheTTL        = time.Minute
	extentCacheInitialCap = 1024
	// ~640K, if it gets bigger than 10k,
	// likely to have lock contention due
	// to the big lock in the lru cache
	extentCacheMaxSize = 10000
)

// maxExtentStats gives the maximum number of unique states
// that an extent can be during its lifetime
const maxExtentStates = int(shared.ExtentStatus_DELETED) + 1

// maxCGExtentStates gives the maximum number of unique states
// that a consumer group extent can be during its lifetime
const maxCGExtentStates = int(shared.ConsumerGroupExtentStatus_DELETED) + 1

// newExtentStateMonitor creates and returns a new instance
// of extentStateMonitor. extentStateMonitor is a background
// daemon that serves as the last resort for moving OPEN extents
// to SEALED state.
func newExtentStateMonitor(context *Context) *extentStateMonitor {
	monitor := new(extentStateMonitor)
	monitor.context = context
	monitor.ll = context.log.WithField(`module`, `extentMon`)
	monitor.shutdownC = make(chan struct{})
	opts := &cache.Options{}
	opts.InitialCapacity = extentCacheInitialCap
	opts.TTL = extentCacheTTL
	monitor.storeExtents = cache.New(extentCacheMaxSize, opts)
	monitor.rateLimiter = common.NewTokenBucket(maxExtentDownEventsPerSec, common.NewRealTimeSource())
	monitor.queueDepth = newQueueDepthCalculator(monitor.context)
	monitor.mi = newMIterator(context, monitor.queueDepth, newDlqMonitor(context))
	return monitor
}

func (monitor *extentStateMonitor) Start() {
	if !atomic.CompareAndSwapInt32(&monitor.started, 0, 1) {
		return
	}
	monitor.shutdownWG.Add(1)
	go monitor.run()
	monitor.ll.Info("ExtentStateMonitor started")
}

func (monitor *extentStateMonitor) Stop() {
	close(monitor.shutdownC)
	if !common.AwaitWaitGroup(&monitor.shutdownWG, time.Second) {
		monitor.ll.Error("Timed out waiting for ExtentStateMonitor to stop")
		return
	}
	monitor.ll.Error("ExtentStateMonitor stopped")
}

// RecvStoreExtentHeartbeat receives a heartbeat coming
// out of a store host for an extent.
func (monitor *extentStateMonitor) RecvStoreExtentHeartbeat(hostID string, extID string, status shared.ExtentStatus) {
	key := buildExtentCacheKey(hostID, extID)
	monitor.storeExtents.Put(key, &extentCacheEntry{status: status})
}

func (monitor *extentStateMonitor) isPrimary() bool {
	context := monitor.context
	hi, err := context.rpm.FindHostForKey(common.ControllerServiceName, common.ControllerServiceName)
	if err != nil {
		return false
	}
	return (hi.UUID == context.hostID)
}

// waitUntilBootstrap waits for ringpop to bootstrap and
// discover cherami peers. Since extent health is depended
// upon the health of input/store nodes responsible for
// the extent, this wait is needed to prevent us from
// pre-maturely triggering sealing of all extents thinking
// that all hosts are unhealthy.
func (monitor *extentStateMonitor) waitUntilBootstrap() {

	var done = false
	var context = monitor.context
	var services = []string{common.InputServiceName, common.OutputServiceName, common.StoreServiceName}

	for !monitor.isShutdown() && !done {

		done = true
		monitor.sleep(intervalBtwnRetries)

		for _, svc := range services {
			hosts, err := context.rpm.GetHosts(svc)
			if err != nil || len(hosts) < 1 {
				done = false
				if err != nil {
					monitor.ll.WithField(common.TagErr, err).Error(`RingPop GetHosts failed`)
				} else if len(hosts) < 1 {
					monitor.ll.Warn(`No peers found; waiting for bootstrap...`)
				}
				break
			}
		}
	}

	monitor.ll.Info("ExtentStateMonitor bootstrap complete")
}

// run is the main loop for the monitor
//
// Every iteration looks like
//
//  list all destinations
//  for each dst
//      for each open_extent
//           trigger seal if needed
func (monitor *extentStateMonitor) run() {
	var sleepTime time.Duration

	monitor.waitUntilBootstrap()

shutdown:
	for !monitor.isShutdown() {

		sleepTime = IntervalBtwnScans

		if !monitor.isPrimary() {
			monitor.ll.Info("ExtentStateMonitor won't run, controller is not primary")
			monitor.sleep(30 * time.Second)
			continue shutdown
		}

		monitor.ll.Info("ExtentStateMonitor beginning to scan all extents")
		monitor.resetLoopStats()
		monitor.runIterator()
		monitor.emitLoopStats()
		monitor.ll.Info("ExtentStateMonitor done with scanning all extents")
		monitor.sleep(sleepTime)
	}

	monitor.shutdownWG.Done()
}

// runIterator is the main loop that iterates over
// all the destinations, destinationExtents, consumer
// groups and all consumer group extents
func (monitor *extentStateMonitor) runIterator() {

	monitor.mi.publishEvent(eIterStart, nil)

	dests := monitor.listDestinations()

	for _, dstDesc := range dests {

		if monitor.isShutdown() {
			return
		}

		if dstDesc.GetStatus() == shared.DestinationStatus_DELETED {
			// We only care about destinations that are in DELETING status,
			// DELETED indicates all clean up has been done and we forgot
			// about the destination, skip over them.
			continue
		}

		if common.IsDLQDestinationPath(dstDesc.GetPath()) {
			monitor.handleDestination(dstDesc) // local handler
			// dlq destinations are handled as part of the
			// consumer group for the iterator
			continue
		}

		// Invoke other handlers before local handler.
		// Why? When the destination is in DELETING state, local handler
		// would go ahead and delete all consumer groups for that destination,
		// so other handlers won't get a chance for cleanup
		monitor.mi.publishEvent(eDestStart, dstDesc)
		monitor.mi.publishEvent(eExtentIterStart, nil)
		monitor.iterateDestinationExtents(dstDesc)
		monitor.mi.publishEvent(eExtentIterEnd, nil)

		consgroups, err := monitor.listConsumerGroups(dstDesc.GetDestinationUUID())
		if err != nil {
			monitor.mi.publishEvent(eDestEnd, dstDesc)
			continue
		}

		monitor.mi.publishEvent(eCnsmIterStart, nil)

		for _, cgDesc := range consgroups {

			if monitor.isShutdown() {
				break
			}

			// skip 'deleted' consumer-groups, but include those in 'deleting' state
			if cgDesc.GetStatus() == shared.ConsumerGroupStatus_DELETED {
				continue
			}

			monitor.mi.publishEvent(eCnsmStart, cgDesc)
			monitor.mi.publishEvent(eCnsmExtentIterStart, nil)
			monitor.iterateConsumerGroupExtents(dstDesc, cgDesc)
			monitor.mi.publishEvent(eCnsmExtentIterEnd, nil)
			monitor.mi.publishEvent(eCnsmEnd, cgDesc)
		}

		monitor.mi.publishEvent(eCnsmIterEnd, nil)
		monitor.mi.publishEvent(eDestEnd, dstDesc)

		// now go over the dlq desitnation for
		// each of the consumer groups
		for _, cgDesc := range consgroups {
			if monitor.isShutdown() {
				break
			}
			if cgDesc.GetStatus() == shared.ConsumerGroupStatus_DELETED {
				continue
			}
			monitor.iterateDLQDestination(cgDesc)
		}

		monitor.handleDestination(dstDesc) // local handler
	}

	monitor.mi.publishEvent(eIterEnd, nil)
}

func (monitor *extentStateMonitor) iterateDestinationExtents(dstDesc *shared.DestinationDescription) {

	var err error
	var context = monitor.context
	var extents []*metadata.DestinationExtent

	isDLQ := common.IsDLQDestinationPath(dstDesc.GetPath())

	// Note that there may be duplicates in these multiple passes, but because these are in chronological order, we should not
	// miss an extent.

extentIter:
	for _, status := range []shared.ExtentStatus{
		shared.ExtentStatus_OPEN,
		shared.ExtentStatus_SEALED,
		shared.ExtentStatus_CONSUMED,
	} {

		filterBy := []shared.ExtentStatus{status}
		extents, err = context.mm.ListDestinationExtentsByStatus(dstDesc.GetDestinationUUID(), filterBy)
		if err != nil {
			monitor.ll.WithFields(bark.Fields{
				common.TagDst:  dstDesc.GetDestinationUUID(),
				common.TagErr:  err,
				`filterStatus`: status}).
				Error(`ListDestinationExtentsByStatus failed`)
			if monitor.sleep(intervalBtwnRetries) {
				break extentIter
			}
			continue extentIter
		}

		for _, extent := range extents {
			monitor.handleDestinationExtent(dstDesc, isDLQ, extent) // local handler
			monitor.mi.publishEvent(eExtent, extent)
		}
	}
}

// processConsumerGroups publishes all non-deleted consumer groups and consumer group extents for mIterator
func (monitor *extentStateMonitor) iterateConsumerGroupExtents(dstDesc *shared.DestinationDescription, cgDesc *shared.ConsumerGroupDescription) {

	dstID := dstDesc.GetDestinationUUID()
	cgID := cgDesc.GetConsumerGroupUUID()
	for _, status := range []shared.ConsumerGroupExtentStatus{
		shared.ConsumerGroupExtentStatus_OPEN,
		shared.ConsumerGroupExtentStatus_CONSUMED,
	} {

		filterBy := []shared.ConsumerGroupExtentStatus{status}
		extents, err := monitor.context.mm.ListExtentsByConsumerGroup(dstID, cgID, filterBy)
		if err != nil {
			monitor.ll.WithFields(bark.Fields{
				common.TagErr:  err,
				common.TagDst:  dstID,
				common.TagCnsm: cgID,
				`statusFilter`: status,
			}).Error(`ListExtentsByConsumerGroup failed`)
			continue
		}

	nextExtent:
		for _, ext := range extents {
			if ext.GetStatus() == shared.ConsumerGroupExtentStatus_DELETED {
				continue nextExtent
			}
			monitor.mi.publishEvent(eCnsmExtent, ext)
			monitor.loopStats.nCGExtentsByStatus[int(ext.GetStatus())]++
		}
	}
}

func (monitor *extentStateMonitor) iterateDLQDestination(cgDesc *shared.ConsumerGroupDescription) {

	dstID := cgDesc.GetDeadLetterQueueDestinationUUID()
	if len(dstID) == 0 {
		return
	}

	dstDesc, err := monitor.context.mm.ReadDestination(dstID, "")
	if err != nil || dstDesc.GetStatus() == shared.DestinationStatus_DELETED {
		return
	}

	monitor.mi.publishEvent(eDestStart, dstDesc)
	monitor.mi.publishEvent(eExtentIterStart, nil)
	monitor.iterateDestinationExtents(dstDesc)
	monitor.mi.publishEvent(eExtentIterEnd, nil)
	monitor.mi.publishEvent(eCnsmIterStart, nil)
	monitor.mi.publishEvent(eCnsmStart, cgDesc)
	monitor.mi.publishEvent(eCnsmEnd, cgDesc)
	monitor.mi.publishEvent(eCnsmIterEnd, nil)
	monitor.mi.publishEvent(eDestEnd, dstDesc)
}

// handleDestination is the local handler for each destination
func (monitor *extentStateMonitor) handleDestination(dstDesc *shared.DestinationDescription) {
	if dstDesc.GetStatus() == shared.DestinationStatus_DELETING {
		// When a destination is in DELETING state, we need to make
		// sure all of the consumer groups tied to that destination
		// are first DELETED. This involves marking the metadata state as deleted
		// and notifying the output hosts to unload the extents.
		// We leave the consumer group extent status as is (i.e. OPEN/CONSUMED)
		// Once all the consumer groups as marked as DELETED, retentionMgr
		// will start deleting the extents from store. It will then ultimately
		// move the destination status to DELETED.
		monitor.context.resultCache.Delete(dstDesc.GetDestinationUUID()) // clear the cache that gives out publish endpoints
		consgroups, err := monitor.listConsumerGroups(dstDesc.GetDestinationUUID())
		if err == nil {
			for _, cgDesc := range consgroups {
				monitor.deleteConsumerGroup(dstDesc, cgDesc)
			}
		}
	}
}

func (monitor *extentStateMonitor) deleteConsumerGroup(dstDesc *shared.DestinationDescription, cgDesc *shared.ConsumerGroupDescription) {

	if cgDesc.GetStatus() == shared.ConsumerGroupStatus_DELETING ||
		cgDesc.GetStatus() == shared.ConsumerGroupStatus_DELETED {
		return
	}

	context := monitor.context
	dstID := dstDesc.GetDestinationUUID()
	cgID := cgDesc.GetConsumerGroupUUID()

	filterBy := []shared.ConsumerGroupExtentStatus{shared.ConsumerGroupExtentStatus_OPEN}
	extents, e := context.mm.ListExtentsByConsumerGroupLite(dstID, cgID, filterBy)
	if e != nil {
		monitor.ll.WithFields(bark.Fields{
			common.TagErr:  e,
			common.TagDst:  dstID,
			common.TagCnsm: cgID,
			`statusFilter`: filterBy[0],
		}).Error(`ListExtentsByConsumerGroupLite failed`)
		// if we cannot list extents, we wont be
		// able to find the output hosts to notify.
		// lets try next time
		return
	}

	// Update the metadata state before notifying the output hosts. This is
	// because, updating the consumer group status is a Quorum operation and
	// will fail in a minority partition. We don't want to sit in a tight
	// loop notifying output hosts in a minority partition.
	e = context.mm.DeleteConsumerGroup(dstDesc.GetDestinationUUID(), cgDesc.GetConsumerGroupName())
	if e != nil {
		context.m3Client.IncCounter(metrics.ExtentMonitorScope, metrics.ControllerErrMetadataUpdateCounter)
		monitor.ll.WithFields(bark.Fields{
			common.TagDst:  common.FmtDst(dstID),
			common.TagCnsm: common.FmtCnsm(cgID),
			common.TagErr:  e,
		}).Warn("Failed to update consumer group status to DELETED")
		return
	}

	monitor.ll.WithFields(bark.Fields{
		common.TagDstPth: common.FmtDstPth(dstDesc.GetPath()),
		common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
		common.TagCnsPth: common.FmtCnsPth(cgDesc.GetConsumerGroupName()),
		common.TagCnsm:   common.FmtCnsm(cgDesc.GetConsumerGroupUUID()),
	}).Info("ExtentMon: ConsumerGroup DELETED")

	context.resultCache.Delete(cgID) // clear the cache that gives out output addrs

	// send notifications to output hosts to unload the extents.
	// The notification is a best effort, if it times out
	// after retries, then we go ahead.

	outputHosts := make(map[string]struct{}, 4)
	for _, ext := range extents {
		if ext.GetStatus() != shared.ConsumerGroupExtentStatus_OPEN {
			continue
		}
		outputHosts[ext.GetOutputHostUUID()] = struct{}{}
	}

	// best effort at notifying the output hosts
	monitor.notifyOutputHosts(dstID, cgID, outputHosts, notifyCGDeleted, cgID)
}

// handleDestinationExtent is the local handler for every destination extent
func (monitor *extentStateMonitor) handleDestinationExtent(dstDesc *shared.DestinationDescription, isDLQ bool, extent *metadata.DestinationExtent) {

	monitor.loopStats.nExtentsByStatus[int(extent.GetStatus())]++
	if isDLQ {
		monitor.loopStats.nDLQExtentsByStatus[int(extent.GetStatus())]++
	}

	context := monitor.context

	switch extent.GetStatus() {
	case shared.ExtentStatus_OPEN:
		if !common.IsRemoteZoneExtent(extent.GetOriginZone(), context.localZone) &&
			(dstDesc.GetStatus() == shared.DestinationStatus_DELETING ||
				!monitor.isExtentHealthy(dstDesc, extent)) {
			// rate limit extent seals to limit the
			// amount of work generated during a given
			// interval
			if !monitor.rateLimiter.Consume(1, 2*time.Second) {
				return
			}
			addExtentDownEvent(context, 0, dstDesc.GetDestinationUUID(), extent.GetExtentUUID())
		}
	default:
		// from a store perspective, an extent is either sealed or
		// not sealed, it doesn't have any other states, make sure
		// all stores for this extent have the state as SEALd and
		// if not, fix the out of sync ones. Do this check only
		// after some minimum time since the last status update
		// to allow for everything to catch up.
		lastUpdateTime := time.Unix(0, extent.GetStatusUpdatedTimeMillis()*int64(time.Millisecond))
		if time.Since(lastUpdateTime) > 2*extentCacheTTL {
			monitor.fixOutOfSyncStoreExtents(dstDesc.GetDestinationUUID(), extent)
		}
	}

	if common.IsRemoteZoneExtent(extent.GetOriginZone(), context.localZone) {
		monitor.handleRemoteZoneDestinationExtent(dstDesc, extent)
	}
}

// handleRemoteZoneDestinationExtent is the local handler for remote destination extent
// this handler kicks off an event if the primary store for the extent is down
func (monitor *extentStateMonitor) handleRemoteZoneDestinationExtent(dstDesc *shared.DestinationDescription, extent *metadata.DestinationExtent) {
	context := monitor.context
	// handle remote zone extent replication job failures
	var failedStores []string
	for _, storeID := range extent.GetStoreUUIDs() {
		state, duration := context.failureDetector.GetHostState(common.StoreServiceName, storeID)
		switch state {
		case dfddHostStateUnknown:
			failedStores = append(failedStores, storeID)
		case dfddHostStateDown:
			if duration >= maxHostRestartDuration {
				failedStores = append(failedStores, storeID)
			}
		}
	}
	if len(failedStores) > 0 {
		stats, err := context.mm.ReadExtentStats(dstDesc.GetDestinationUUID(), extent.GetExtentUUID())
		if err != nil {
			return
		}
		for _, s := range failedStores {
			if s == stats.GetExtent().GetRemoteExtentPrimaryStore() {
				event := NewRemoteExtentPrimaryStoreDownEvent(s, extent.GetExtentUUID())
				context.eventPipeline.Add(event)
			}
		}
	}
}

// fixOutOfSyncStoreExtents checks if all stores have the same view of
// the extent status and if not, issues a SEAL to the out of sync
// store to bring it up to speed
// For a remote zone extent, store will mark a store extent as sealed
// only after it gets the sealed marker from replication
// So we don't need to sync the status for remote zone extent
func (monitor *extentStateMonitor) fixOutOfSyncStoreExtents(dstID string, extent *metadata.DestinationExtent) {
	if common.IsRemoteZoneExtent(extent.GetOriginZone(), monitor.context.localZone) {
		return
	}

	for _, storeh := range extent.GetStoreUUIDs() {

		key := buildExtentCacheKey(storeh, extent.GetExtentUUID())
		entry, ok := monitor.storeExtents.Get(key).(*extentCacheEntry)
		if !ok {
			continue
		}
		if entry.status == shared.ExtentStatus_OPEN {
			// store out of sync, re-seal this extent
			addStoreExtentStatusOutOfSyncEvent(monitor.context, dstID, extent.GetExtentUUID(), storeh)
			monitor.ll.WithFields(bark.Fields{
				common.TagDst:  common.FmtDst(dstID),
				common.TagExt:  common.FmtExt(extent.GetExtentUUID()),
				common.TagStor: common.FmtStor(storeh),
			}).Warn("Extent status out of sync on store; expected=SEALED; found=OPEN")
		}
	}
}

// invalidateStoreExtentCache deletes the cached entry
// corresponding to the (store,extentID). This cache is
// currently used to determine if store thinks the extent
// is open or not. When an extent is sealed, this must be
// invalidated to prevent the extentmon from thinking that
// the store is out of sync with metadata.
func (monitor *extentStateMonitor) invalidateStoreExtentCache(storeID string, extentID string) {
	key := buildExtentCacheKey(storeID, extentID)
	monitor.storeExtents.Delete(key)
}

func (monitor *extentStateMonitor) notifyOutputHosts(dstID, cgID string, outputHosts map[string]struct{}, reason string, reasonContext string) {

	context := monitor.context

	for hostID := range outputHosts {
		update := &admin.ConsumerGroupUpdatedNotification{
			ConsumerGroupUUID: common.StringPtr(cgID),
			Type:              common.AdminNotificationTypePtr(admin.NotificationType_ALL),
		}

		req := &admin.ConsumerGroupsUpdatedRequest{
			UpdateUUID: common.StringPtr(uuid.New()),
			Updates:    []*admin.ConsumerGroupUpdatedNotification{update},
		}

		addr, err := context.rpm.ResolveUUID(common.OutputServiceName, hostID)
		if err != nil {
			context.m3Client.IncCounter(metrics.ExtentMonitorScope, metrics.ControllerErrResolveUUIDCounter)
			monitor.ll.WithFields(bark.Fields{`uuid`: hostID, common.TagErr: err}).Debug(`Cannot send notification, failed to resolve outputhost uuid`)
			continue
		}

		adminClient, err := common.CreateOutputHostAdminClient(context.channel, addr)
		if err != nil {
			context.m3Client.IncCounter(metrics.ExtentMonitorScope, metrics.ControllerErrCreateTChanClientCounter)
			monitor.ll.WithField(common.TagErr, err).Error(`Failed to create output host client`)
			continue
		}

		updateOp := func() error {
			ctx, cancel := thrift.NewContext(thriftCallTimeout)
			defer cancel()
			return adminClient.ConsumerGroupsUpdated(ctx, req)
		}

		monitor.ll.WithFields(bark.Fields{
			common.TagCnsm:       common.FmtCnsm(cgID),
			common.TagDst:        common.FmtDst(dstID),
			`reason`:             reason,
			`context`:            reasonContext,
			common.TagOut:        common.FmtIn(hostID),
			common.TagUpdateUUID: req.GetUpdateUUID(),
		}).Info("ConsumerGroupUpdatedNotification: Sending notification to outputhost")

		// best effort, if the notification fails, its not the end of the
		// world. Output hosts polls metadata every minute to refresh its state
		err = backoff.Retry(updateOp, notificationRetryPolicy(), common.IsRetryableTChanErr)
		if err != nil {
			monitor.ll.WithFields(bark.Fields{
				common.TagCnsm:       common.FmtCnsm(cgID),
				common.TagDst:        common.FmtDst(dstID),
				`reason`:             reason,
				`context`:            reasonContext,
				common.TagOut:        common.FmtOut(hostID),
				common.TagUpdateUUID: req.GetUpdateUUID(),
				`hostaddr`:           addr,
				common.TagErr:        err,
			}).Error("ConsumerGroupUpdatedNotification: Failed to send notification to output")
		}
	}
}

func (monitor *extentStateMonitor) listDestinations() []*shared.DestinationDescription {
	var err error
	var quit bool
	var context = monitor.context
	var result []*shared.DestinationDescription

	for !quit {
		result, err = context.mm.ListDestinationsByUUID()
		if err == nil {
			break
		}
		monitor.ll.WithField(common.TagErr, err).Error(`ListDestinationsByUUID failed`)
		quit = monitor.sleep(intervalBtwnRetries)
	}

	return result
}

func (monitor *extentStateMonitor) listConsumerGroups(dstID string) ([]*shared.ConsumerGroupDescription, error) {
	cgdSlice, err := monitor.context.mm.ListConsumerGroupsByDstID(dstID)
	if err != nil {
		monitor.ll.WithFields(bark.Fields{
			common.TagErr: err,
			common.TagDst: dstID,
		}).Error(`ListConsumerGroupsByDstID failed`)
	}
	return cgdSlice, err
}

func (monitor *extentStateMonitor) isExtentHealthy(dstDesc *shared.DestinationDescription, extent *metadata.DestinationExtent) bool {

	context := monitor.context
	dstID := dstDesc.GetDestinationUUID()

	if !isInputHealthy(context, extent) {
		monitor.ll.WithFields(bark.Fields{
			common.TagDst: common.FmtDst(dstID),
			common.TagExt: common.FmtExt(extent.GetExtentUUID()),
			common.TagIn:  common.FmtIn(extent.GetInputHostUUID()),
		}).Info("ExtentMon: Found extent with unhealthy input")
		return false
	}
	if !areExtentStoresHealthy(context, extent) {
		monitor.ll.WithFields(bark.Fields{
			common.TagDst: common.FmtDst(dstID),
			common.TagExt: common.FmtExt(extent.GetExtentUUID()),
			common.TagIn:  common.FmtIn(extent.GetInputHostUUID()),
			`storeids`:    extent.GetStoreUUIDs(),
		}).Info("ExtentMon: Found extent with unhealthy store")
		return false
	}

	return true
}

func (monitor *extentStateMonitor) resetLoopStats() {
	monitor.loopStats.nExtentsByStatus = [maxExtentStates]int64{}
	monitor.loopStats.nDLQExtentsByStatus = [maxExtentStates]int64{}
	monitor.loopStats.nCGExtentsByStatus = [maxCGExtentStates]int64{}
}

func (monitor *extentStateMonitor) emitLoopStats() {
	stats := &monitor.loopStats
	m3Client := monitor.context.m3Client
	m3Client.AddCounter(metrics.ExtentMonitorScope, metrics.ControllerNumOpenExtents, stats.nExtentsByStatus[int(shared.ExtentStatus_OPEN)])
	m3Client.AddCounter(metrics.ExtentMonitorScope, metrics.ControllerNumSealedExtents, stats.nExtentsByStatus[int(shared.ExtentStatus_SEALED)])
	m3Client.AddCounter(metrics.ExtentMonitorScope, metrics.ControllerNumConsumedExtents, stats.nExtentsByStatus[int(shared.ExtentStatus_CONSUMED)])
	m3Client.AddCounter(metrics.ExtentMonitorScope, metrics.ControllerNumOpenDLQExtents, stats.nDLQExtentsByStatus[int(shared.ExtentStatus_OPEN)])
	m3Client.AddCounter(metrics.ExtentMonitorScope, metrics.ControllerNumSealedDLQExtents, stats.nDLQExtentsByStatus[int(shared.ExtentStatus_SEALED)])
	m3Client.AddCounter(metrics.ExtentMonitorScope, metrics.ControllerNumConsumedDLQExtents, stats.nDLQExtentsByStatus[int(shared.ExtentStatus_CONSUMED)])
	m3Client.AddCounter(metrics.ExtentMonitorScope, metrics.ControllerNumOpenCGExtents, stats.nCGExtentsByStatus[int(shared.ConsumerGroupExtentStatus_OPEN)])
	m3Client.AddCounter(metrics.ExtentMonitorScope, metrics.ControllerNumConsumedCGExtents, stats.nCGExtentsByStatus[int(shared.ConsumerGroupExtentStatus_CONSUMED)])
}

func (monitor *extentStateMonitor) isShutdown() bool {
	select {
	case <-monitor.shutdownC:
		return true
	default:
		return false
	}
}

func (monitor *extentStateMonitor) sleep(d time.Duration) bool {
	select {
	case <-monitor.shutdownC:
		return true
	case <-time.After(d):
		return false
	}
}

func buildExtentCacheKey(hostID string, extID string) string {
	// concat of first 8 bytes of hostID and all of extID
	return strings.Join([]string{hostID[:18], extID}, ".")
}
