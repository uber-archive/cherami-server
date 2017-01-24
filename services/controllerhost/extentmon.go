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
		*queueDepthCalculator
		mi *mIterator
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
	monitor.queueDepthCalculator = newQueueDepthCalculator(monitor)
	monitor.mi = newMIterator(context)
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

		if !monitor.isPrimary() {
			monitor.ll.Info("ExtentStateMonitor won't run, controller is not primary")
			monitor.sleep(30 * time.Second)
			continue shutdown
		}

		monitor.ll.Info("ExtentStateMonitor beginning to scan all extents")

		sleepTime = IntervalBtwnScans

		dests := monitor.listDestinations()
		if monitor.isShutdown() {
			break shutdown
		}

		monitor.mi.publishEvent(eIterStart, nil)

	readLoop:
		for _, dstDesc := range dests {
			if dstDesc.GetStatus() == shared.DestinationStatus_DELETED {
				// We only care about destinations that are in DELETING status,
				// DELETED indicates all clean up has been done and we forgot
				// about the destination, skip over them. TODO add a secondary
				// index on destination status to skip over the DELETED ones.
				continue readLoop
			}

			// Invoke queueDepthCalculator before invoking processDestination.
			// Why? Because when the destination is in DELETING state, processDestination
			// would go ahead and delete all consumer groups for that destination, so
			// queueDepth would never get the opportunity to reset the backlog guages to zero.
			monitor.queueDepthCalculator.processDestination(dstDesc, false)

			monitor.mi.publishEvent(eDestStart, dstDesc)
			monitor.processDestination(dstDesc)
			monitor.mi.publishEvent(eDestEnd, dstDesc)
		}

		monitor.mi.publishEvent(eIterEnd, nil)

		monitor.ll.Info("ExtentStateMonitor done with scanning all extents")
		monitor.sleep(sleepTime)
	}

	monitor.shutdownWG.Done()
}

func (monitor *extentStateMonitor) processDestination(dstDesc *shared.DestinationDescription) {

	var err error
	var context = monitor.context
	var stats []*shared.ExtentStats

	monitor.mi.publishEvent(eExtentIterStart, nil)

	// Note that there may be duplicates in these multiple passes, but because these are in chronological order, we should not
	// miss an extent. OPEN is already done above.

extentIter:
	for _, status := range []shared.ExtentStatus{
		shared.ExtentStatus_OPEN,
		shared.ExtentStatus_SEALED,
		shared.ExtentStatus_CONSUMED,
		shared.ExtentStatus_DELETED,
	} {
		filterBy := []shared.ExtentStatus{status}
		stats, err = context.mm.ListExtentsByDstIDStatus(dstDesc.GetDestinationUUID(), filterBy)
		if err != nil {
			monitor.ll.WithFields(bark.Fields{
				common.TagDst:  dstDesc.GetDestinationUUID(),
				common.TagErr:  err,
				`filterStatus`: status}).
				Error(`ListExtentsByDstIDStatus failed`)
			if monitor.sleep(intervalBtwnRetries) {
				break extentIter
			}
			continue extentIter
		}

		monitor.processExtents(dstDesc, stats)
	}

	monitor.mi.publishEvent(eExtentIterEnd, nil)

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
		monitor.deleteConsumerGroups(dstDesc)
	}

	monitor.publishConsumerGroups(dstDesc)
}

func (monitor *extentStateMonitor) processExtents(dstDesc *shared.DestinationDescription, stats []*shared.ExtentStats) {

	for _, stat := range stats {

		extent := stat.GetExtent()

		monitor.mi.publishEvent(eExtent, stat)

		switch stat.GetStatus() {
		case shared.ExtentStatus_OPEN:
			if !common.IsRemoteZoneExtent(extent.GetOriginZone(), monitor.context.localZone) && (dstDesc.GetStatus() == shared.DestinationStatus_DELETING || !monitor.isExtentHealthy(dstDesc, extent)) {
				// rate limit extent seals to limit the
				// amount of work generated during a given
				// interval
				if !monitor.rateLimiter.Consume(1, 2*time.Second) {
					continue
				}
				addExtentDownEvent(monitor.context, 0, extent.GetDestinationUUID(), extent.GetExtentUUID())
			}
		default:
			// from a store perspective, an extent is either sealed or
			// not sealed, it doesn't have any other states, make sure
			// all stores for this extent have the state as SEALd and
			// if not, fix the out of sync ones. Do this check only
			// after some minimum time since the last status update
			// to allow for everything to catch up.
			lastUpdateTime := time.Unix(0, stat.GetStatusUpdatedTimeMillis()*int64(time.Millisecond))
			if time.Since(lastUpdateTime) > 2*extentCacheTTL {
				monitor.fixOutOfSyncStoreExtents(dstDesc.GetDestinationUUID(), extent)
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
func (monitor *extentStateMonitor) fixOutOfSyncStoreExtents(dstID string, extent *shared.Extent) {
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

func (monitor *extentStateMonitor) deleteConsumerGroups(dstDesc *shared.DestinationDescription) {
	if monitor.isShutdown() {
		return
	}

	dstID := dstDesc.GetDestinationUUID()

	consgroups, err := monitor.listConsumerGroups(dstDesc.GetDestinationUUID())
	if err != nil {
		return
	}

	var context = monitor.context
	var cgInfoMap = make(map[string]*consumerGroupInfo, len(consgroups))

	// Go over all the non-deleted consumer groups and send
	// notifications to output hosts to unload the extents.
	// The notification is a best effort, if it times out
	// after retries, then we go ahead.
	for _, cg := range consgroups {

		if cg.GetStatus() == shared.ConsumerGroupStatus_DELETED {
			continue
		}

		cgID := cg.GetConsumerGroupUUID()

		filterBy := []metadata.ConsumerGroupExtentStatus{metadata.ConsumerGroupExtentStatus_OPEN}
		extents, e := context.mm.ListExtentsByConsumerGroup(dstID, cgID, filterBy)
		if e != nil {
			monitor.ll.WithFields(bark.Fields{
				common.TagErr:  e,
				common.TagDst:  dstID,
				common.TagCnsm: cgID,
				`statusFilter`: filterBy[0],
			}).Error(`ListExtentsByConsumerGroup failed`)
			// if we cannot list extents, we wont be
			// able to find the output hosts to notify.
			// lets try next time
			continue
		}

		// Update the metadata state before notifying the output hosts. This is
		// because, updating the consumer group status is a Quorum operation and
		// will fail in a minority partition. We don't want to sit in a tight
		// loop notifying output hosts in a minority partition.
		e = context.mm.DeleteConsumerGroup(dstDesc.GetDestinationUUID(), cg.GetConsumerGroupName())
		if e != nil {
			context.m3Client.IncCounter(metrics.ExtentMonitorScope, metrics.ControllerErrMetadataUpdateCounter)
			monitor.ll.WithFields(bark.Fields{
				common.TagDst:  common.FmtDst(dstID),
				common.TagCnsm: common.FmtCnsm(cgID),
				common.TagErr:  e,
			}).Warn("Failed to update consumer group status to DELETED")
			continue
		}

		monitor.ll.WithFields(bark.Fields{
			common.TagDstPth: common.FmtDstPth(dstDesc.GetPath()),
			common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
			common.TagCnsPth: common.FmtCnsPth(cg.GetConsumerGroupName()),
			common.TagCnsm:   common.FmtCnsm(cg.GetConsumerGroupUUID()),
		}).Info("ExtentMon: ConsumerGroup DELETED")

		context.resultCache.Delete(cgID) // clear the cache that gives out output addrs

		for _, ext := range extents {

			if ext.GetStatus() != metadata.ConsumerGroupExtentStatus_OPEN {
				continue
			}

			cgInfo, ok := cgInfoMap[cg.GetConsumerGroupUUID()]

			if !ok {
				cgInfo = &consumerGroupInfo{}
				cgInfo.desc = cg
				cgInfo.outputHosts = make(map[string]struct{}, 2)
				cgInfoMap[cgID] = cgInfo
			}

			cgInfo.outputHosts[ext.GetOutputHostUUID()] = struct{}{}
		}

		// best effort at notifying the output hosts
		monitor.notifyOutputHosts(cgInfoMap, notifyCGDeleted, cg.GetConsumerGroupUUID())
	}
}

// publishConsumerGroups publishes all non-deleted consumer groups and consumer group extents for mIterator
func (monitor *extentStateMonitor) publishConsumerGroups(dstDesc *shared.DestinationDescription) {
	if monitor.isShutdown() {
		return
	}

	monitor.mi.publishEvent(eCnsmIterStart, nil)
	defer monitor.mi.publishEvent(eCnsmIterEnd, nil)

	dstID := dstDesc.GetDestinationUUID()

	consgroups, err := monitor.listConsumerGroups(dstDesc.GetDestinationUUID())
	if err != nil {
		return
	}

nextConsGroup:
	for _, cg := range consgroups {
		if monitor.isShutdown() {
			break nextConsGroup
		}

		if cg.GetStatus() == shared.ConsumerGroupStatus_DELETED {
			continue nextConsGroup
		}

		monitor.mi.publishEvent(eCnsmStart, cg)
		monitor.mi.publishEvent(eCnsmExtentIterStart, nil)

		cgID := cg.GetConsumerGroupUUID()
		for _, status := range []metadata.ConsumerGroupExtentStatus{
			metadata.ConsumerGroupExtentStatus_OPEN,
			metadata.ConsumerGroupExtentStatus_CONSUMED,
			//metadata.ConsumerGroupExtentStatus_DELETED,
		} {
			filterBy := []metadata.ConsumerGroupExtentStatus{status}
			extents, e := monitor.context.mm.ListExtentsByConsumerGroup(dstID, cgID, filterBy)
			if e == nil {
			nextExtent:
				for _, ext := range extents {
					if ext.GetStatus() == metadata.ConsumerGroupExtentStatus_DELETED { // This shouldn't happen, but deleteConsumerGroups thinks it can happen...
						continue nextExtent
					}
					monitor.mi.publishEvent(eCnsmExtent, ext)
				}
			} else {
				monitor.ll.WithFields(bark.Fields{
					common.TagErr:  e,
					common.TagDst:  dstID,
					common.TagCnsm: cgID,
					`statusFilter`: status,
				}).Error(`ListExtentsByConsumerGroup failed`)
			}
		}
		monitor.mi.publishEvent(eCnsmExtentIterEnd, nil)
		monitor.mi.publishEvent(eCnsmEnd, nil)
	}
}

func (monitor *extentStateMonitor) notifyOutputHosts(cgInfoMap map[string]*consumerGroupInfo, reason string, reasonContext string) {

	context := monitor.context

	for cgID, cgInfo := range cgInfoMap {
		for hostID := range cgInfo.outputHosts {
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
				common.TagDst:        common.FmtDst(cgInfo.desc.GetDestinationUUID()),
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
					common.TagDst:        common.FmtDst(cgInfo.desc.GetDestinationUUID()),
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

func (monitor *extentStateMonitor) isExtentHealthy(dstDesc *shared.DestinationDescription, extent *shared.Extent) bool {

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
