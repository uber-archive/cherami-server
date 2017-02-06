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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/controllerhost/load"
	c "github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"

	storeGen "github.com/uber/cherami-thrift/.generated/go/store"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"
)

const (
	// T471438, ADDR_SEAL is being written into replica_stats. We ignore small negative 26-bit 2's-compliment numbers,
	// which are larger than this constant in signed 64-bit space
	storageMetaValuesLimit = int64(1<<26) - (1 << 4)

	// QueueDepthTabulationString can be added to a destination or CG owner email to request queue depth tabulation
	// Note that Google allows something like this: gbailey+queueDepthTabulation@uber.com
	// The above is still a valid email and will be delivered to gbailey@uber.com
	QueueDepthTabulationString = `queueDepthTabulation`

	gaftTimeout    = 2 * time.Second // getAddressFromTimestamp timeout
	gaftCacheLimit = 1 << 20         // We expect cache entries to take less than 2^7 bytes. 2^27 = 128MB. 27-7 = 20. This means that our cache should be less than 128MB
	gaftSealed     = common.SequenceNumber(-1)
)

// QueueDepthCacheEntry is a cache structure for testing queue depth
type QueueDepthCacheEntry struct {
	// Time is the cache entry time
	Time common.UnixNanoTime
	// BacklogAvailable is the available backlog
	BacklogAvailable int64
	// BacklogUnavailable is the unavailable backlog (only useful for timer queues)
	BacklogUnavailable int64
	// BacklogInflight is the in flight message count
	BacklogInflight int64
	// BacklogDLQ is the number of messages in DLQ
	BacklogDLQ int64
}

// QueueDepthCacheJSONFields is the json fields for QueueDepthCacheEntry
type QueueDepthCacheJSONFields struct {
	CacheTime          common.UnixNanoTime `json:"cache_time,omitempty"`
	BacklogAvailable   int64               `json:"backlog_available"`
	BacklogUnavailable int64               `json:"backlog_unavailable"`
	BacklogInflight    int64               `json:"backlog_inflight"`
	BacklogDLQ         int64               `json:"backlog_dlq"`
}

type extentCoverMap map[extentID]struct{}

//var testQueueDepth bool
var queueDepthCacheLk sync.Mutex
var queueDepthCache = make(map[string]QueueDepthCacheEntry) // key is CG UUID
var queueDepthCacheLimit = 1 << 19

type gaftKey struct {
	extentUUID string
	timestamp  int64
}

var gaftCache = make(map[gaftKey]common.SequenceNumber)

type queueDepthCalculator struct {
	*extentStateMonitor
	ll                      bark.Logger
	destinationExtentsCache replicaStatsMRUCache // MRU cache of extent replica stats

	// Cover maps are used to find 'dangling' extents (destination extents with no corresponding consumer group extent)
	// Each of these extents may also contribute to backlog, often the majority of backlog, depending on the startFrom time
	dstCoverMap extentCoverMap // Map containing all of the extent UUIDs for the destination
	dlqCoverMap extentCoverMap // Map containing all of the extent UUIDs for the DLQ destination, as applicable
}

func newQueueDepthCalculator(m *extentStateMonitor) *queueDepthCalculator {
	return &queueDepthCalculator{
		extentStateMonitor:      m,
		destinationExtentsCache: *newReplicaStatsMRUCache(m.context.mm, m.context.log.WithField(`module`, `ReplicaStatsMRUCache`), queueDepthCacheLimit),
		ll:          m.context.log.WithField(`module`, `queueDepth`),
		dstCoverMap: make(extentCoverMap),
		dlqCoverMap: make(extentCoverMap),
	}
}

// Makes a copy of the cover map that is safe to mutate
func (qdc *queueDepthCalculator) getCoverMap(dlq bool) (r extentCoverMap) {
	orig := qdc.dstCoverMap
	if dlq {
		orig = qdc.dlqCoverMap
	}

	r = make(extentCoverMap, len(orig))
	for ext := range orig {
		r[ext] = struct{}{}
	}
	return r
}

func (qdc *queueDepthCalculator) processDestination(dstDesc *shared.DestinationDescription, dlqCache bool) {
	var err error
	var extents []*metadata.DestinationExtent
	var context = qdc.context

	// We don't process metrics for DLQ destinations directly. They are processed only as part of the parent consumer group
	if common.IsDLQDestination(dstDesc) && !dlqCache {
		return
	}

	coverMap := &qdc.dstCoverMap
	if dlqCache {
		coverMap = &qdc.dlqCoverMap
	}

	// Clear the coverMap
	*coverMap = make(extentCoverMap)

	filter := []shared.ExtentStatus{shared.ExtentStatus_OPEN, shared.ExtentStatus_SEALED} // CONSUMED should be ignorable for queue depth
	extents, err = context.mm.ListDestinationExtentsByStatus(dstDesc.GetDestinationUUID(), filter)
	if err != nil {
		qdc.ll.WithField(common.TagErr, err).WithField(common.TagDst, dstDesc.GetDestinationUUID()).Error(`QueueDepth: ListDestinationExtentsByStatus failed`)
		qdc.sleep(intervalBtwnRetries) // We can ignore the return value, since we are returning anyway.
		return
	}

	for _, ext := range extents {
		extID := extentID(ext.GetExtentUUID())
		// This updates the consumer group visibility for DLQ merge and purge, and also our store UUIDs for re-replication
		qdc.destinationExtentsCache.putPlaceholders(extID, ext.GetConsumerGroupVisibility(), ext.GetStoreUUIDs())
		(*coverMap)[extentID(ext.GetExtentUUID())] = struct{}{}
	}

	if !dlqCache { // Don't process queue depth for DLQ CGs, just populate the DLQ coverMap and extent cache
		qdc.processConsumerGroups(dstDesc)
	}
}

func (qdc *queueDepthCalculator) processConsumerGroups(dstDesc *shared.DestinationDescription) {
	cgs := qdc.listConsumerGroups(dstDesc.GetDestinationUUID())
	if qdc.isShutdown() {
		return
	}

	for _, cgDesc := range cgs {
		// Report zero metrics for deleted CG or DST
		if cgDesc.GetStatus() == shared.ConsumerGroupStatus_DELETED ||
			dstDesc.GetStatus() == shared.DestinationStatus_DELETING ||
			dstDesc.GetStatus() == shared.DestinationStatus_DELETED {
			qdc.reportMetrics(cgDesc, dstDesc, 0, 0, 0, 0, backlogProgessInfinity, 0)
			continue
		}

		// Load the DLQ extents for this consumer group
		dlqDestDesc := shared.DestinationDescription{}
		dlqDestDesc.Path = common.StringPtr(`DLQ for...`)
		dlqDestDesc.DestinationUUID = cgDesc.DeadLetterQueueDestinationUUID
		if len(cgDesc.GetDeadLetterQueueDestinationUUID()) > 0 {
			qdc.processDestination(&dlqDestDesc, true)
		}

		qdc.processConsumerGroupExtents(dstDesc, cgDesc)
	}
}

func (qdc *queueDepthCalculator) processConsumerGroupExtents(dstDesc *shared.DestinationDescription, cgDesc *shared.ConsumerGroupDescription) {
	const maxResults = 1 << 13 // 16MB (thrift limit) is 2^24. Assume that extents are <= 2KB (2^11). 24 - 11 = 13
	var err error
	var context = qdc.context
	var extents []*metadata.ConsumerGroupExtent

	extents, err = context.mm.ListExtentsByConsumerGroup(dstDesc.GetDestinationUUID(), cgDesc.GetConsumerGroupUUID(), nil)
	if err != nil {
		context.log.WithFields(bark.Fields{
			common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
			common.TagDstPth: common.FmtDstPth(dstDesc.GetPath()),
			common.TagCnsm:   common.FmtCnsm(cgDesc.GetConsumerGroupUUID()),
			common.TagCnsPth: common.FmtCnsPth(cgDesc.GetConsumerGroupName()),
			common.TagErr:    err,
		}).Error(`Error listing consumer group extents`)
		qdc.sleep(intervalBtwnRetries)
		return
	}

	qdc.calculateAndReportMetrics(dstDesc, cgDesc, extents)
}

func (qdc *queueDepthCalculator) listConsumerGroups(dstID string) []*shared.ConsumerGroupDescription {
	var err error
	var context = qdc.context
	var result []*shared.ConsumerGroupDescription

	result, err = context.mm.ListConsumerGroupsByDstID(dstID)
	if err != nil {
		qdc.ll.WithField(common.TagErr, err).WithField(common.TagDst, dstID).Error(`ListConsumerGroupsByDstID failed`)
		qdc.sleep(intervalBtwnRetries)
		return result
	}

	return result
}

func (qdc *queueDepthCalculator) calculateAndReportMetrics(dstDesc *shared.DestinationDescription, cgDesc *shared.ConsumerGroupDescription, extents []*metadata.ConsumerGroupExtent) {
	var storeExtent *compactExtentStats
	var consumerGroupExtent *metadata.ConsumerGroupExtent
	var BacklogUnavailable, BacklogAvailable, BacklogInflight, BacklogDLQ int64
	var extent extentID
	var store storeID
	var coverMap extentCoverMap
	var debugLog bark.Logger

	isTabulationRequested := qdc.isTabulationRequested(cgDesc, dstDesc)
	extrapolatedTime := common.Now() - common.UnixNanoTime(IntervalBtwnScans/2) // Do interpolation by looking back in time for half a reporting period
	if isTabulationRequested {
		debugLog = qdc.ll.WithFields(bark.Fields{
			`time`:           extrapolatedTime,
			common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
			common.TagDstPth: dstDesc.GetPath(),
			common.TagCnsm:   cgDesc.GetConsumerGroupUUID(),
			common.TagCnsPth: cgDesc.GetConsumerGroupName(),
		})
	}

	// This allows extrapolation up to 1 reporting period in either direction, meaning that missing a report shouldn't cause a blip
	maxExtrapolation := common.Seconds(float64(IntervalBtwnScans) / float64(time.Second))

	calc := func() {
		// Calculations that can be performed with only the consumer group extent

		// T505191: Assuming that all consumed extents will have zero backlog is safe, but it may hide some bugs. We do
		// this for now, since there is an issue with dynamic changes to retention not being reflected in outputhost's
		// ack level seq no. This can be removed once we are guaranteed that the ack level address and sequence numbers
		// reported by outputhost are in sync.
		if consumerGroupExtent.GetStatus() != metadata.ConsumerGroupExtentStatus_OPEN {
			if isTabulationRequested {
				qdc.ll.WithFields(bark.Fields{
					`time`:             extrapolatedTime,
					common.TagDst:      common.FmtDst(dstDesc.GetDestinationUUID()),
					common.TagDstPth:   dstDesc.GetPath(),
					common.TagCnsm:     cgDesc.GetConsumerGroupUUID(),
					common.TagCnsPth:   cgDesc.GetConsumerGroupName(),
					common.TagExt:      string(extent),
					common.TagStor:     string(store),
					`cgeStatus`:        consumerGroupExtent.GetStatus(),
					`cgeAckLvlSeq`:     consumerGroupExtent.GetAckLevelSeqNo(),
					`cgeAckLvlSeqRate`: consumerGroupExtent.GetAckLevelSeqNoRate(),
					`cgeWriteTime`:     (extrapolatedTime - common.UnixNanoTime(consumerGroupExtent.GetWriteTime())).ToSecondsFmt(),
				}).Info(`Queue Depth Tabulation (skipping consumed/deleted extent)`)
			}
			delete(coverMap, extent) // mark this extent as done
			return
		}

		ΔInFlight := common.ExtrapolateDifference(
			common.SequenceNumber(consumerGroupExtent.GetReadLevelSeqNo()),
			common.SequenceNumber(consumerGroupExtent.GetAckLevelSeqNo()),
			consumerGroupExtent.GetReadLevelSeqNoRate(),
			consumerGroupExtent.GetAckLevelSeqNoRate(),
			common.UnixNanoTime(consumerGroupExtent.GetWriteTime()),
			common.UnixNanoTime(consumerGroupExtent.GetWriteTime()),
			extrapolatedTime,
			maxExtrapolation)

		BacklogInflight += ΔInFlight

		// Calculations requiring store extent
		delete(coverMap, extent) // mark this extent as done
		storeExtent = qdc.destinationExtentsCache.get(extent, store, debugLog)

		// This happens pretty commonly when extents are churning. There are races where a
		// consumer group extent exists, but hasn't been updated with the connected store id.
		// Hence, the storeExtent may not be possible to retrieve. This is a temporary condition.
		if storeExtent == nil {
			if isTabulationRequested {
				qdc.ll.WithFields(bark.Fields{
					`time`:             extrapolatedTime,
					common.TagDst:      common.FmtDst(dstDesc.GetDestinationUUID()),
					common.TagDstPth:   dstDesc.GetPath(),
					common.TagCnsm:     cgDesc.GetConsumerGroupUUID(),
					common.TagCnsPth:   cgDesc.GetConsumerGroupName(),
					common.TagExt:      string(extent),
					common.TagStor:     string(store),
					`cgeAckLvlSeq`:     consumerGroupExtent.GetAckLevelSeqNo(),
					`cgeAckLvlSeqRate`: consumerGroupExtent.GetAckLevelSeqNoRate(),
					`cgeWriteTime`:     (extrapolatedTime - common.UnixNanoTime(consumerGroupExtent.GetWriteTime())).ToSecondsFmt(),
				}).Info(`Queue Depth Tabulation (missing store extent)`)
			}
			return
		}

		// Depending on status, we may already know that backlogs should be zero, regardless of what the other metadata says
		// Note that the increment to BacklogInflight above should have been zero
		if storeExtent.status == shared.ExtentStatus_ARCHIVED || storeExtent.status == shared.ExtentStatus_DELETED || storeExtent.status == shared.ExtentStatus_CONSUMED {
			BacklogInflight -= ΔInFlight
			if isTabulationRequested {
				qdc.ll.WithFields(bark.Fields{
					`time`:           extrapolatedTime,
					common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
					common.TagDstPth: dstDesc.GetPath(),
					common.TagCnsm:   cgDesc.GetConsumerGroupUUID(),
					common.TagCnsPth: cgDesc.GetConsumerGroupName(),
					common.TagExt:    string(extent),
					common.TagStor:   string(store),
					`extentStatus`:   storeExtent.status,
				}).Info(`Queue Depth Tabulation (skipping deleted store extent)`)
			}
			return
		}

		// T471438 -- Fix meta-values leaked by storehost
		// T520701 -- Fix massive int64 negative values
		var fixed bool
		for _, val := range []*int64{&storeExtent.seqAvailable, &storeExtent.seqBegin} {
			if *val >= storageMetaValuesLimit || *val < -1 {
				*val = 0
				fixed = true
			}
		}
		if fixed {
			qdc.ll.WithFields(bark.Fields{
				`time`:           extrapolatedTime,
				common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
				common.TagDstPth: dstDesc.GetPath(),
				common.TagCnsm:   cgDesc.GetConsumerGroupUUID(),
				common.TagCnsPth: cgDesc.GetConsumerGroupName(),
				common.TagExt:    string(extent),
				common.TagStor:   string(store),
			}).Info(`Queue Depth Temporarily Fixed Replica-Stats`)
		}

		// Skip dangling extents that are excluded by the startFrom time, or modify the consumerGroupExtent to adjust for
		// retention or startFrom
		if !qdc.handleStartFrom(dstDesc, cgDesc, consumerGroupExtent, storeExtent, extent, extrapolatedTime, isTabulationRequested) {
			return
		}

		ba := common.ExtrapolateDifference(
			common.SequenceNumber(storeExtent.seqAvailable),
			common.SequenceNumber(consumerGroupExtent.GetAckLevelSeqNo()),
			storeExtent.seqAvailableRate,
			consumerGroupExtent.GetAckLevelSeqNoRate(),
			common.UnixNanoTime(storeExtent.writeTime),
			common.UnixNanoTime(consumerGroupExtent.GetWriteTime()),
			extrapolatedTime,
			maxExtrapolation)

		bu := int64(0) // Unavailable backlog is only applicable to timer destinations, which are deprecated

		if isTabulationRequested {
			qdc.ll.WithFields(bark.Fields{
				`time`:              extrapolatedTime,
				common.TagDst:       common.FmtDst(dstDesc.GetDestinationUUID()),
				common.TagDstPth:    dstDesc.GetPath(),
				common.TagCnsm:      cgDesc.GetConsumerGroupUUID(),
				common.TagCnsPth:    cgDesc.GetConsumerGroupName(),
				common.TagExt:       string(extent),
				common.TagStor:      string(store),
				`rsAvailSeq`:        storeExtent.seqAvailable,
				`cgeAckLvlSeq`:      consumerGroupExtent.GetAckLevelSeqNo(),
				`rsAvailSeqRate`:    storeExtent.seqAvailableRate,
				`cgeAckLvlSeqRate`:  consumerGroupExtent.GetAckLevelSeqNoRate(),
				`rsWriteTime`:       (extrapolatedTime - common.UnixNanoTime(storeExtent.writeTime)).ToSecondsFmt(),
				`rsCacheTime`:       (extrapolatedTime - common.UnixNanoTime(storeExtent.cacheTime.UnixNano())).ToSecondsFmt(),
				`cgeWriteTime`:      (extrapolatedTime - common.UnixNanoTime(consumerGroupExtent.GetWriteTime())).ToSecondsFmt(),
				`subTotalAvail`:     ba,
				`runningTotalAvail`: ba + BacklogAvailable,
			}).Info(`Queue Depth Tabulation`)
		}

		BacklogAvailable += ba
		BacklogUnavailable += bu
	}

	coverMap = qdc.getCoverMap(false)

	if isTabulationRequested {
		qdc.ll.WithFields(bark.Fields{
			`time`:           extrapolatedTime,
			common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
			common.TagDstPth: common.FmtDstPth(dstDesc.GetPath()),
			common.TagCnsm:   common.FmtCnsm(cgDesc.GetConsumerGroupUUID()),
			common.TagCnsPth: common.FmtCnsPth(cgDesc.GetConsumerGroupName()),
			`extentCount`:    len(coverMap),
			`cgExtentCount`:  len(extents),
		}).Info(`Queue Depth Tabulation, start`)
	}

skipConsumerGroupExtent:
	for _, consumerGroupExtent = range extents {
		store = storeID(consumerGroupExtent.GetConnectedStoreUUID())
		extent = extentID(consumerGroupExtent.GetExtentUUID())

		// Controller doesn't call GetAddressFromTimestamp or check the begin sequence for the CGEs it creates. If that happens, we will
		// overstate backlog for these extents if retention has occurred. To correct for this, ignore these CGEs and allow the dangling
		// processing to fix things.
		if consumerGroupExtent.GetAckLevelSeqNo() == 0 && consumerGroupExtent.GetConnectedStoreUUID() == `` {
			if isTabulationRequested {
				qdc.ll.WithFields(bark.Fields{
					`time`:           extrapolatedTime,
					common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
					common.TagDstPth: common.FmtDstPth(dstDesc.GetPath()),
					common.TagCnsm:   common.FmtCnsm(cgDesc.GetConsumerGroupUUID()),
					common.TagCnsPth: common.FmtCnsPth(cgDesc.GetConsumerGroupName()),
					common.TagExt:    common.FmtExt(string(extent)),
				}).Info(`Queue Depth Tabulation, forcing assigned but unopened extent to dangling`)
			}
			continue skipConsumerGroupExtent
		}
		calc()
	}

	// Handle store extents that weren't covered by consumer group extents
	// Nil CGE is eventually correct. Only discrepancy is that store BeginSeqNo might be non-zero if retention has occurred.
	store = ``
dangling:
	for extent = range coverMap {
		storeExtent = qdc.destinationExtentsCache.get(extent, store, debugLog)
		if storeExtent == nil {
			qdc.ll.WithFields(bark.Fields{
				`time`:           extrapolatedTime,
				common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
				common.TagDstPth: common.FmtDstPth(dstDesc.GetPath()),
				common.TagCnsm:   common.FmtCnsm(cgDesc.GetConsumerGroupUUID()),
				common.TagCnsPth: common.FmtCnsPth(cgDesc.GetConsumerGroupName()),
				common.TagExt:    common.FmtExt(string(extent)),
			}).Warn(`Queue Depth could not get dangling store extent`)
			continue dangling
		}

		if len(storeExtent.consumerGroupVisibility) > 0 &&
			storeExtent.consumerGroupVisibility != cgDesc.GetConsumerGroupUUID() { // Merged extents for other CGs should be skipped
			if isTabulationRequested {
				qdc.ll.WithFields(bark.Fields{
					`time`:           extrapolatedTime,
					common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
					common.TagDstPth: common.FmtDstPth(dstDesc.GetPath()),
					common.TagCnsm:   common.FmtCnsm(cgDesc.GetConsumerGroupUUID()),
					common.TagCnsPth: common.FmtCnsPth(cgDesc.GetConsumerGroupName()),
					common.TagExt:    common.FmtExt(string(extent)),
				}).Info(`Queue Depth Tabulation, skipping other CG's merged extent`)
			}
			continue dangling
		}

		if isTabulationRequested {
			qdc.ll.WithFields(bark.Fields{
				`time`:           extrapolatedTime,
				common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
				common.TagDstPth: common.FmtDstPth(dstDesc.GetPath()),
				common.TagCnsm:   common.FmtCnsm(cgDesc.GetConsumerGroupUUID()),
				common.TagCnsPth: common.FmtCnsPth(cgDesc.GetConsumerGroupName()),
				common.TagExt:    common.FmtExt(string(extent)),
			}).Info(`Queue Depth Tabulation, processing dangling store extent`)
		}
		consumerGroupExtent = &metadata.ConsumerGroupExtent{} // Clear the consumerGroupExtent, since we don't have one for dangling extents
		calc()
	}

	// Handle DLQ backlog
	// Purged extents are identified by CGVisibility
	// Merged extents were part of the original destintation above
	coverMap = qdc.getCoverMap(true)

dlqExtents:
	for extent = range coverMap {
		consumerGroupExtent = &metadata.ConsumerGroupExtent{}
		store = ``
		storeExtent = qdc.destinationExtentsCache.get(extent, store, debugLog)

		if storeExtent == nil {
			qdc.ll.WithFields(bark.Fields{
				`time`:           extrapolatedTime,
				common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
				common.TagDstPth: common.FmtDstPth(dstDesc.GetPath()),
				common.TagCnsm:   common.FmtCnsm(cgDesc.GetConsumerGroupUUID()),
				common.TagCnsPth: common.FmtCnsPth(cgDesc.GetConsumerGroupName()),
				common.TagExt:    common.FmtExt(string(extent)),
			}).Warn(`Queue Depth could not get DLQ store extent`)
			continue dlqExtents
		}

		if len(storeExtent.consumerGroupVisibility) > 0 { // Purging will set the consumer group visibility
			if isTabulationRequested {
				qdc.ll.WithFields(bark.Fields{
					`time`:           extrapolatedTime,
					common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
					common.TagDstPth: common.FmtDstPth(dstDesc.GetPath()),
					common.TagCnsm:   common.FmtCnsm(cgDesc.GetConsumerGroupUUID()),
					common.TagCnsPth: common.FmtCnsPth(cgDesc.GetConsumerGroupName()),
					common.TagExt:    common.FmtExt(string(extent)),
				}).Info(`Queue Depth Tabulation (DLQ), Skipping purged extent`)
			}
			continue dlqExtents
		}

		// T471438 -- Fix meta-values leaked by storehost
		// T520701 -- Fix massive int64 negative values
		var fixed bool
		for _, val := range []*int64{&storeExtent.seqAvailable, &storeExtent.seqBegin} {
			if *val >= storageMetaValuesLimit || *val < -1 {
				*val = 0
				fixed = true
			}
		}
		if fixed {
			qdc.ll.WithFields(bark.Fields{
				`time`:           extrapolatedTime,
				common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
				common.TagDstPth: dstDesc.GetPath(),
				common.TagCnsm:   cgDesc.GetConsumerGroupUUID(),
				common.TagCnsPth: cgDesc.GetConsumerGroupName(),
				common.TagExt:    string(extent),
				common.TagStor:   string(store),
			}).Info(`Queue Depth Temporarily Fixed Replica-Stats`)
		}

		bd := common.ExtrapolateDifference(
			common.SequenceNumber(storeExtent.seqAvailable),
			common.SequenceNumber(storeExtent.seqBegin)+1, // Begin sequence is -1 if no retention has occurred
			storeExtent.seqAvailableRate,
			0,
			common.UnixNanoTime(storeExtent.writeTime),
			common.UnixNanoTime(storeExtent.writeTime),
			extrapolatedTime,
			maxExtrapolation)

		if isTabulationRequested {
			qdc.ll.WithFields(bark.Fields{
				`time`:            extrapolatedTime,
				common.TagDst:     common.FmtDst(dstDesc.GetDestinationUUID()),
				common.TagDstPth:  dstDesc.GetPath(),
				common.TagCnsm:    cgDesc.GetConsumerGroupUUID(),
				common.TagCnsPth:  cgDesc.GetConsumerGroupName(),
				common.TagExt:     string(extent),
				common.TagStor:    string(store),
				`rsAvailSeq`:      storeExtent.seqAvailable,
				`rsAvailSeqRate`:  storeExtent.seqAvailableRate,
				`rsWriteTime`:     (extrapolatedTime - common.UnixNanoTime(storeExtent.writeTime)).ToSecondsFmt(),
				`subTotalDLQ`:     bd,
				`runningTotalDLQ`: bd + BacklogDLQ,
			}).Info(`Queue Depth Tabulation (DLQ)`)
		}

		BacklogDLQ += bd
	}

	progressScore := qdc.measureBacklogProgress(dstDesc, cgDesc, extents, BacklogAvailable)

	qdc.reportMetrics(cgDesc, dstDesc, BacklogUnavailable, BacklogAvailable, BacklogInflight, BacklogDLQ, progressScore, extrapolatedTime)

	if isTabulationRequested {
		qdc.ll.WithFields(bark.Fields{
			`time`:           extrapolatedTime,
			common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
			common.TagDstPth: common.FmtDstPth(dstDesc.GetPath()),
			common.TagCnsm:   common.FmtCnsm(cgDesc.GetConsumerGroupUUID()),
			common.TagCnsPth: common.FmtCnsPth(cgDesc.GetConsumerGroupName()),
		}).Info(`Queue Depth Tabulation, end`)
	}

}

const backlogProgessInfinity = 6666
const minBacklogForCGStallCheck = 1000

func (qdc *queueDepthCalculator) reportStalledMetric(dstDesc *shared.DestinationDescription) bool {
	return !strings.HasPrefix(dstDesc.GetPath(), "/test")
}

func (qdc *queueDepthCalculator) measureBacklogProgress(dstDesc *shared.DestinationDescription, cgDesc *shared.ConsumerGroupDescription, extents []*metadata.ConsumerGroupExtent, backlog int64) int {
	// This CG has backlog, lets identify the
	// extents making progress. If more than
	// 50% are stalled, then emit a metric for
	// alarming
	nOpen := 0
	nStalled := 0
	for _, cge := range extents {
		if cge.GetStatus() == metadata.ConsumerGroupExtentStatus_OPEN {
			nOpen++
			if qdc.isCGExtentStalled(cge) {
				nStalled++
			}
		}
	}

	if nOpen == 0 || backlog < minBacklogForCGStallCheck {
		return backlogProgessInfinity
	}

	if nStalled > 0 && nStalled >= nOpen/2 {
		qdc.ll.WithFields(bark.Fields{
			common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
			common.TagDstPth: dstDesc.GetPath(),
			common.TagCnsm:   cgDesc.GetConsumerGroupUUID(),
			common.TagCnsPth: cgDesc.GetConsumerGroupName(),
			`openExtents`:    nOpen,
			`stalledExtents`: nStalled,
		}).Warn(`ConsumerGroup stalled`)
		return 0
	}

	return nOpen
}

func (qdc *queueDepthCalculator) isCGExtentStalled(cge *metadata.ConsumerGroupExtent) bool {

	context := qdc.context

	cgID := cge.GetConsumerGroupUUID()
	extID := cge.GetExtentUUID()
	hostID := cge.GetOutputHostUUID()

	// Definition of a extent Stall:
	//   * CG has backlog
	//   * Atleast some consumers are connected to the outputhost
	//   * Zero messages are sent to the consumers in the past 5 mins
	//   * CG is not in smart retry mode

	// Get returns an error only when there are insufficient data points
	// for a one-min sum, this typically only for the 1st minute after the
	// ConsumerGroup is loaded by the output host
	smartRetryOn, err := context.loadMetrics.Get(hostID, cgID, load.SmartRetryOn, load.OneMinSum)
	if err != nil || smartRetryOn > 0 {
		return false
	}

	nConns, err := context.loadMetrics.Get(hostID, cgID, load.NumConns, load.FiveMinAvg)
	if err != nil {
		return false
	}

	msgsOut, err := context.loadMetrics.Get(hostID, extID, load.MsgsOutPerSec, load.FiveMinSum)
	if err != nil {
		return false
	}

	return (nConns > 0 && msgsOut == 0)
}

func (qdc *queueDepthCalculator) reportMetrics(cgDesc *shared.ConsumerGroupDescription, dstDesc *shared.DestinationDescription, BacklogUnavailable, BacklogAvailable, BacklogInflight, BacklogDLQ int64, progressScore int, now common.UnixNanoTime) {
	var err error
	var cgTagValue, dstTagValue string
	cgTagValue, err = common.GetTagsFromPath(cgDesc.GetConsumerGroupName())
	if err != nil {
		cgTagValue = metrics.UnknownDirectoryTagValue
	}
	dstTagValue, err = common.GetTagsFromPath(dstDesc.GetPath())
	if err != nil {
		dstTagValue = metrics.UnknownDirectoryTagValue
	}
	tags := map[string]string{
		metrics.ConsumerGroupTagName: cgTagValue,
		metrics.DestinationTagName:   dstTagValue,
	}

	// Emit M3 metrics for per host/consumer group/destination
	qdcM3Client := metrics.NewClientWithTags(qdc.extentStateMonitor.context.m3Client, metrics.Controller, tags)

	qdcM3Client.UpdateGauge(metrics.QueueDepthBacklogCGScope, metrics.ControllerCGBacklogAvailable, BacklogAvailable)
	qdcM3Client.UpdateGauge(metrics.QueueDepthBacklogCGScope, metrics.ControllerCGBacklogInflight, BacklogInflight)
	qdcM3Client.UpdateGauge(metrics.QueueDepthBacklogCGScope, metrics.ControllerCGBacklogDLQ, BacklogDLQ)

	if qdc.reportStalledMetric(dstDesc) {
		qdcM3Client.UpdateGauge(metrics.QueueDepthBacklogCGScope, metrics.ControllerCGBacklogProgress, int64(progressScore))
	}

	if BacklogAvailable != 0 || BacklogUnavailable != 0 || BacklogInflight != 0 || BacklogDLQ != 0 {
		qdc.ll.WithFields(bark.Fields{
			`time`:             now,
			common.TagDst:      common.FmtDst(dstDesc.GetDestinationUUID()),
			common.TagDstPth:   common.FmtDstPth(dstDesc.GetPath()),
			common.TagCnsm:     common.FmtCnsm(cgDesc.GetConsumerGroupUUID()),
			common.TagCnsPth:   common.FmtCnsPth(cgDesc.GetConsumerGroupName()),
			`BacklogAvailable`: BacklogAvailable,
			`BacklogInflight`:  BacklogInflight,
			`BacklogDLQ`:       BacklogDLQ,
			`progressScore`:    progressScore,
		}).Info(`Queue Depth Metrics`)
	}

	queueDepthCacheLk.Lock()
	if len(queueDepthCache) > queueDepthCacheLimit { // Random eviction if the cache has grown too large;
		for k := range queueDepthCache {
			delete(queueDepthCache, k)
			break
		}
	}
	// Cache the queue depth result
	queueDepthCache[cgDesc.GetConsumerGroupUUID()] = QueueDepthCacheEntry{
		Time:               now,
		BacklogAvailable:   BacklogAvailable,
		BacklogUnavailable: BacklogUnavailable,
		BacklogInflight:    BacklogInflight,
		BacklogDLQ:         BacklogDLQ,
	}
	queueDepthCacheLk.Unlock()
}

func (qdc *queueDepthCalculator) isTabulationRequested(cgDesc *shared.ConsumerGroupDescription, dstDesc *shared.DestinationDescription) (tabulationRequested bool) {
	for _, s := range []string{cgDesc.GetOwnerEmail(), dstDesc.GetOwnerEmail()} {
		if strings.Contains(s, QueueDepthTabulationString) {
			return true
		}
	}
	return
}

// getAddressFromTimestampOnStores is a less-than-best-effort caller for GetAddressFromTimestamps. It does no retries.
// NOTE: it should not be called for OPEN extents with a startFrom in the future. That situation requires a cache invalidation strategy that is not implemented.
func getAddressFromTimestampOnStores(context *Context, cgDesc *shared.ConsumerGroupDescription, storeUUIDs []string, extentID string, timestamp int64) common.SequenceNumber {
	var client storeGen.TChanBStore
	var err error
	var ok bool
	var res *storeGen.GetAddressFromTimestampResult_
	var seqNo common.SequenceNumber
	var storeUUID, storeAddr string
	var trace int

	if seqNo, ok = gaftCache[gaftKey{extentUUID: extentID, timestamp: timestamp}]; ok {
		return seqNo
	}

	// These have to go up here, since goto can't skip any declarations
	req := storeGen.NewGetAddressFromTimestampRequest()
	ctx, cancel := thrift.NewContext(gaftTimeout)
	defer cancel()

	// Get the address for any one store
	for _, storeUUID = range storeUUIDs { // TODO: consider picking randomly if this turns out to load stores unevenly
		storeAddr, err = context.rpm.ResolveUUID(common.StoreServiceName, storeUUID)
		if err == nil {
			break
		}
	}
	if err != nil {
		trace = 1
		goto fail
	}

	// Get the thrift connection to the store
	client, err = context.clientFactory.GetThriftStoreClient(storeAddr, storeUUID)
	if err != nil {
		trace = 2
		goto fail
	}
	defer context.clientFactory.ReleaseThriftStoreClient(storeUUID)

	// Setup and execute our request
	req.ExtentUUID = common.StringPtr(extentID)
	req.Timestamp = common.Int64Ptr(timestamp)
	res, err = client.GetAddressFromTimestamp(ctx, req)
	if err != nil {
		trace = 3
		goto fail
	}

	// Interpret the results
	seqNo = common.SequenceNumber(res.GetSequenceNumber())
	// Note: res.GetSealed() means that the extent is sealed AND that this seqNo is the last message in the extent (i.e. timestamp is > end of extent)
	// We still need to check GetSealed() here, since this sealed seqNo may differ among replicas, so we want to ensure that we don't have a small
	// backlog due to a minor discrepancy in the store seal sequence numbers.
	if res.GetSealed() {
		seqNo = gaftSealed
	}

	// Cache the result
	if len(gaftCache) > gaftCacheLimit { // Random eviction if the cache has grown too large; this works fine as long as the working set isn't > 50% of the limit
		for k := range gaftCache {
			delete(gaftCache, k)
			break
		}
	}
	gaftCache[gaftKey{extentUUID: extentID, timestamp: timestamp}] = seqNo

	return seqNo

fail:
	context.log.WithFields(bark.Fields{
		common.TagErr:  err,
		common.TagStor: storeUUID, // don't format, as it could be an empty string
		common.TagExt:  common.FmtExt(extentID),
		common.TagCnsm: common.FmtCnsm(cgDesc.GetConsumerGroupUUID()),
		`timestamp`:    timestamp,
		`storeAddr`:    storeAddr,
		`trace`:        trace,
	}).Error(`Queue Depth: Error getting address for timestamp`)
	return 0 // Returning zero on failures means that we will tend to overstate queue depth
}

// handleStartFrom calls get address from timestamp as necessary to determine if a dangling extent's messages should be totally, partially, or not
// added to the queue depth.
// NOTE: this function may modify cgDesc to alter the calculation
// NOTE: this also handles retention (beginSequence) for dangling extents; the CGE (outputhost) automatically handles retention for non-dangling
// NOTE: single-CG-Visible extents (merged DLQ extents) don't require special calculation here. The messages in the DLQ are guaranteed greater than startFrom
func (qdc *queueDepthCalculator) handleStartFrom(
	dstDesc *shared.DestinationDescription,
	cgDesc *shared.ConsumerGroupDescription,
	consumerGroupExtent *metadata.ConsumerGroupExtent,
	storeExtent *compactExtentStats,
	extentUUID extentID,
	now common.UnixNanoTime,
	isTabulationRequested bool,
) (qualify bool) {
	var startFromSeq common.SequenceNumber
	var trace int // This is just for debugging purposes. It shows the path of 'if's that the function went through before exiting.
	qualify = true
	doGaft := true

	// Don't allow startFrom to be in the future. Future startFrom should have zero queue depth anyway
	if cgDesc.GetStartFrom() > int64(now) {
		trace = 1
		qualify = false
		goto done
	}

	if consumerGroupExtent.AckLevelSeqNo == nil { // This is a dangling extent; CGE doesn't exist yet; we must synthesize a CGE
		switch {
		// No start from means that this extent always qualifies. Only check for beginSequence below.
		// Note that our cherami-cli doesn't allow us to set literal zero for this,
		// so zero startFrom is usually a second after 1970-01-01
		case cgDesc.GetStartFrom() < int64(time.Hour):
			trace = 2
			doGaft = false

		// TODO: Add optimizations based on begin and end enqueue timestamps, when they are populated

		// If our startFrom time is before the extent was created, all messages in this extent qualify. We don't need to call getAddressFromTimestamp
		case storeExtent.createTime != 0 && cgDesc.GetStartFrom() <= int64(storeExtent.createTime): // If the startFrom is before the create time, the extent always qualifies
			trace = 3
			doGaft = false

		// Typical case, we need to call GetAddressFromTimestamp
		default:
			trace = 9
		}

		if doGaft {
			// At this point, startFrom might be in the middle of the extent. We need to call GetAddressFromTimestamp to determine the number of messages
			// after the startFrom point
			startFromSeq = getAddressFromTimestampOnStores(qdc.context, cgDesc, qdc.destinationExtentsCache.getStoreIDs(extentUUID), string(extentUUID), cgDesc.GetStartFrom())
			if startFromSeq == gaftSealed {
				trace += 10
				qualify = false // We include none of the messages in this extent
			} else { // We will include some or all of the messages in this extent
				trace += 20
			}
		}

		// if doGaft == false, this just adjusts for retention, if applicable
		if qualify {
			trace += 100
			consumerGroupExtent.WriteTime = common.Int64Ptr(int64(now))
			consumerGroupExtent.AckLevelSeqNo = common.Int64Ptr(common.MaxInt64(
				storeExtent.seqBegin, // Retention may have removed some messages
				int64(startFromSeq),  // Otherwise, act like we had just opened this extent at startFrom, i.e. don't count messages before startFrom
			))
		}
	}

done:
	if isTabulationRequested {
		qdc.ll.WithFields(bark.Fields{
			`time`:           now,
			common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
			common.TagDstPth: dstDesc.GetPath(),
			common.TagCnsm:   cgDesc.GetConsumerGroupUUID(),
			common.TagCnsPth: cgDesc.GetConsumerGroupName(),
			common.TagExt:    extentUUID,
			`rsBeginSeq`:     storeExtent.seqBegin,
			`startFromSeq`:   startFromSeq,
			`rsAvailSeq`:     storeExtent.seqAvailable,
			`qualify`:        qualify,
			`trace`:          trace,
		}).Info(`Queue Depth Tabulation (StartFrom)`)
	}
	return
}

// GetQueueDepthResult gives a queue depth result for
func GetQueueDepthResult(cgUUID string) (QueueDepthCacheEntry, error) {
	var ok bool
	var qdce = QueueDepthCacheEntry{}
	queueDepthCacheLk.Lock()
	defer queueDepthCacheLk.Unlock()
	// If we get a cache miss here, return nil, error,
	if qdce, ok = queueDepthCache[cgUUID]; ok {
		return qdce, nil
	}
	return qdce, &c.QueueCacheMissError{Message: fmt.Sprintf("queue depth entity cache miss for %s", cgUUID)}
}
