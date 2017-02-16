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

	backlogProgessInfinity    = 6666
	minBacklogForCGStallCheck = 1000

	zeroUUID = "00000000-0000-0000-0000-000000000000"
)

var (
	queueDepthCacheLimit = 1 << 20
	gaftCache            = make(map[gaftKey]common.SequenceNumber)
	// This allows extrapolation up to 1 reporting period in either direction, meaning that missing a report shouldn't cause a blip
	maxExtrapolation = common.Seconds(float64(IntervalBtwnScans) / float64(time.Second))
)

type (
	QueueDepthCache struct {
		sync.RWMutex
		entries  map[string]QueueDepthCacheEntry
		capacity int
	}

	// QueueDepthCacheEntry is a cache structure for testing queue depth
	QueueDepthCacheEntry struct {
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
	QueueDepthCacheJSONFields struct {
		CacheTime        common.UnixNanoTime `json:"cache_time,omitempty"`
		BacklogAvailable int64               `json:"backlog_available"`
		BacklogInflight  int64               `json:"backlog_inflight"`
		BacklogDLQ       int64               `json:"backlog_dlq"`
	}

	queueDepthCalculator struct {
		context                 *Context
		ll                      bark.Logger
		destinationExtentsCache replicaStatsMRUCache // MRU cache of extent replica stats
		queueDepthCache         *QueueDepthCache

		// iter represents the state during a
		// single iteration of a destination and/or
		// consumer group
		iter struct {
			isDLQ   bool // is the current destination a DLQ
			dstDesc *shared.DestinationDescription

			cg struct {
				backlogAvailable      int64
				backlogInflight       int64
				nOpenExtents          int64 // stat for stallness check
				nStalledExtents       int64 // stat for stallness check
				desc                  *shared.ConsumerGroupDescription
				coverMap              extentCoverMap // list of extents that account towards backlog
				extrapolatedTime      common.UnixNanoTime
				isTabulationRequested bool
			}
		}
	}

	gaftKey struct {
		extentUUID string
		timestamp  int64
	}
)

func newQueueDepthCalculator(context *Context) *queueDepthCalculator {
	return &queueDepthCalculator{
		context:                 context,
		destinationExtentsCache: *newReplicaStatsMRUCache(context.mm, context.log.WithField(`module`, `ReplicaStatsMRUCache`)),
		queueDepthCache:         newQueueDepthCache(),
		ll:                      context.log.WithField(`module`, `queueDepth`),
	}
}

// GetQueueDepthResult gives a queue depth result for the given cgUUID
func (qdc *queueDepthCalculator) GetQueueDepthResult(cgUUID string) (QueueDepthCacheEntry, error) {
	if entry, ok := qdc.queueDepthCache.get(cgUUID); ok {
		return entry, nil
	}
	return QueueDepthCacheEntry{}, &c.QueueCacheMissError{Message: fmt.Sprintf("queue depth entity cache miss for %s", cgUUID)}
}

// handleEvent handles an event from the metadata iterator
func (qdc *queueDepthCalculator) handleEvent(e *mIteratorEvent) {
	switch e.t {
	case eDestStart:
		qdc.handleDestinationStart(e.dest)
	case eDestEnd:
		qdc.iter.dstDesc = nil
		qdc.iter.isDLQ = false
	case eExtent:
		qdc.handleDestinationExtent(e.dest, e.extent)
	case eCnsmStart:
		qdc.iter.cg.desc = e.cnsm
		qdc.handleConsumerGroupStart(e.dest, e.cnsm)
	case eCnsmEnd:
		qdc.handleConsumerGroupEnd(e.dest, e.cnsm)
	case eCnsmExtent:
		qdc.handleConsumerGroupExtent(e.dest, e.cnsm, e.cnsmExtent)
	}
}

func (qdc *queueDepthCalculator) handleDestinationStart(dstDesc *shared.DestinationDescription) {
	qdc.iter.dstDesc = dstDesc
	qdc.iter.isDLQ = common.IsDLQDestination(dstDesc)
	qdc.destinationExtentsCache.clear()
}

func (qdc *queueDepthCalculator) handleConsumerGroupStart(dstDesc *shared.DestinationDescription, cgDesc *shared.ConsumerGroupDescription) {
	iter := &qdc.iter
	iter.cg.desc = cgDesc
	iter.cg.isTabulationRequested = qdc.isTabulationRequested(cgDesc, dstDesc)

	if iter.isDLQ && cgDesc.GetDeadLetterQueueDestinationUUID() != dstDesc.GetDestinationUUID() {
		return // we don't compute backlog for dlq consumer groups
	}

	if cgDesc.GetStatus() == shared.ConsumerGroupStatus_DELETED ||
		dstDesc.GetStatus() == shared.DestinationStatus_DELETING ||
		dstDesc.GetStatus() == shared.DestinationStatus_DELETED {
		qdc.reportBacklog(cgDesc, dstDesc, backlogProgessInfinity, 0)
		return
	}

	iter.cg.coverMap = qdc.destinationExtentsCache.getExtentCoverMap()
	iter.cg.extrapolatedTime = common.Now() - common.UnixNanoTime(IntervalBtwnScans/2) // Do interpolation by looking back in time for half a reporting period

	if iter.cg.isTabulationRequested {
		qdc.ll.WithFields(bark.Fields{
			`time`:           iter.cg.extrapolatedTime,
			common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
			common.TagDstPth: common.FmtDstPth(dstDesc.GetPath()),
			common.TagCnsm:   common.FmtCnsm(cgDesc.GetConsumerGroupUUID()),
			common.TagCnsPth: common.FmtCnsPth(cgDesc.GetConsumerGroupName()),
			`extentCount`:    len(iter.cg.coverMap),
		}).Info(`Queue Depth Tabulation, start`)
	}
}

func (qdc *queueDepthCalculator) handleConsumerGroupEnd(dstDesc *shared.DestinationDescription, cgDesc *shared.ConsumerGroupDescription) {
	iter := &qdc.iter
	// iterate over the dstExtents that are not assigned
	// to the consumer group yet and account them towards
	// total backlog
	for extent := range iter.cg.coverMap {
		cge := &metadata.ConsumerGroupExtent{
			ConsumerGroupUUID: common.StringPtr(cgDesc.GetConsumerGroupUUID()),
			ExtentUUID:        common.StringPtr(string(extent)),
			OutputHostUUID:    common.StringPtr(zeroUUID),
		}
		qdc.addExtentBacklog(dstDesc, cgDesc, cge, qdc.makeCGExtentLogger(dstDesc, cgDesc, cge))
	}

	// stallness check, can be performed only after processing all extents
	progressScore := qdc.measureBacklogProgress(dstDesc, cgDesc, iter.cg.backlogAvailable)
	qdc.reportBacklog(cgDesc, dstDesc, progressScore, iter.cg.extrapolatedTime)

	if iter.cg.isTabulationRequested {
		qdc.ll.WithFields(bark.Fields{
			`time`:           iter.cg.extrapolatedTime,
			common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
			common.TagDstPth: common.FmtDstPth(dstDesc.GetPath()),
			common.TagCnsm:   common.FmtCnsm(cgDesc.GetConsumerGroupUUID()),
			common.TagCnsPth: common.FmtCnsPth(cgDesc.GetConsumerGroupName()),
		}).Info(`Queue Depth Tabulation, end`)
	}

	iter.cg.desc = nil
	iter.cg.coverMap = nil
	iter.cg.nOpenExtents = 0
	iter.cg.nStalledExtents = 0
	iter.cg.backlogAvailable = 0
	iter.cg.backlogInflight = 0
}

func (qdc *queueDepthCalculator) handleDestinationExtent(dstDesc *shared.DestinationDescription, extent *metadata.DestinationExtent) {
	if extent.GetStatus() > shared.ExtentStatus_SEALED {
		return
	}
	cache := &qdc.destinationExtentsCache
	extID := extentID(extent.GetExtentUUID())
	cache.putSingleCGVisibility(extID, extent.GetConsumerGroupVisibility())
	for _, store := range extent.GetStoreUUIDs() {
		cache.put(extID, storeID(store), nil) // Add a placeholder so that a store extent may be found if no consumer group extent exists for it
	}
}

func (qdc *queueDepthCalculator) handleConsumerGroupExtent(dstDesc *shared.DestinationDescription, cgDesc *shared.ConsumerGroupDescription, cgExtent *metadata.ConsumerGroupExtent) {

	iter := &qdc.iter
	logger := qdc.makeCGExtentLogger(dstDesc, cgDesc, cgExtent)

	if cgExtent.GetStatus() == metadata.ConsumerGroupExtentStatus_OPEN {
		qdc.iter.cg.nOpenExtents++
		if qdc.isCGExtentStalled(cgExtent) {
			qdc.iter.cg.nStalledExtents++
		}
	}

	// Controller doesn't call GetAddressFromTimestamp or check the begin sequence for the CGEs it creates. If that happens, we will
	// overstate backlog for these extents if retention has occurred. To correct for this, ignore these CGEs and allow the dangling
	// processing to fix things.
	if cgExtent.GetAckLevelSeqNo() == 0 && cgExtent.GetConnectedStoreUUID() == `` {
		if iter.cg.isTabulationRequested {
			logger.Info(`Queue Depth Tabulation, forcing assigned but unopened extent to dangling`)
		}
		return
	}

	qdc.addExtentBacklog(dstDesc, cgDesc, cgExtent, logger)
	delete(iter.cg.coverMap, extentID(cgExtent.GetExtentUUID()))
}

// addExtentBacklog counts the number of un-consumed
// messages in the given extent towards the consumer
// group backlog
func (qdc *queueDepthCalculator) addExtentBacklog(
	dstDesc *shared.DestinationDescription,
	cgDesc *shared.ConsumerGroupDescription,
	cgExtent *metadata.ConsumerGroupExtent,
	logger bark.Logger) {

	extID := extentID(cgExtent.GetExtentUUID())
	connectedStoreID := storeID(cgExtent.GetConnectedStoreUUID())

	iter := &qdc.iter

	if cgExtent.GetStatus() != metadata.ConsumerGroupExtentStatus_OPEN {
		if iter.cg.isTabulationRequested {
			logger.WithFields(bark.Fields{
				common.TagStor:     string(connectedStoreID),
				`cgeStatus`:        cgExtent.GetStatus(),
				`cgeAckLvlSeq`:     cgExtent.GetAckLevelSeqNo(),
				`cgeAckLvlSeqRate`: cgExtent.GetAckLevelSeqNoRate(),
				`cgeWriteTime`:     (iter.cg.extrapolatedTime - common.UnixNanoTime(cgExtent.GetWriteTime())).ToSecondsFmt(),
			}).Info(`Queue Depth Tabulation (skipping consumed/deleted extent)`)
		}
		return
	}

	ΔInFlight := common.ExtrapolateDifference(
		common.SequenceNumber(cgExtent.GetReadLevelSeqNo()),
		common.SequenceNumber(cgExtent.GetAckLevelSeqNo()),
		cgExtent.GetReadLevelSeqNoRate(),
		cgExtent.GetAckLevelSeqNoRate(),
		common.UnixNanoTime(cgExtent.GetWriteTime()),
		common.UnixNanoTime(cgExtent.GetWriteTime()),
		iter.cg.extrapolatedTime,
		maxExtrapolation)

	iter.cg.backlogInflight += ΔInFlight

	// Calculations requiring store extent
	storeExtent := qdc.destinationExtentsCache.get(extID, connectedStoreID)

	// This happens pretty commonly when extents are churning. There are races where a
	// consumer group extent exists, but hasn't been updated with the connected store id.
	// Hence, the storeExtent may not be possible to retrieve. This is a temporary condition.
	if storeExtent == nil {
		if iter.cg.isTabulationRequested {
			logger.WithFields(bark.Fields{
				common.TagStor:     string(connectedStoreID),
				`cgeAckLvlSeq`:     cgExtent.GetAckLevelSeqNo(),
				`cgeAckLvlSeqRate`: cgExtent.GetAckLevelSeqNoRate(),
				`cgeWriteTime`:     (iter.cg.extrapolatedTime - common.UnixNanoTime(cgExtent.GetWriteTime())).ToSecondsFmt(),
			}).Info(`Queue Depth Tabulation (missing store extent)`)
		}
		return
	}

	// Depending on status, we may already know that backlogs should be zero, regardless of what the other metadata says
	// Note that the increment to BacklogInflight above should have been zero
	if storeExtent.GetStatus() >= shared.ExtentStatus_CONSUMED {
		iter.cg.backlogInflight -= ΔInFlight
		if iter.cg.isTabulationRequested {
			logger.WithFields(bark.Fields{
				common.TagStor: string(connectedStoreID),
				`extentStatus`: storeExtent.GetStatus(),
			}).Info(`Queue Depth Tabulation (skipping deleted store extent)`)
		}
		return
	}

	if qdc.isMergedDLQExtentForOtherCG(storeExtent, cgDesc) || qdc.isPurgedDLQExtent(storeExtent) {
		if iter.cg.isTabulationRequested {
			logger.Info(`Queue Depth Tabulation, skipping other other CG's merged and/or purged dlq extent`)
		}
		return
	}

	rs := storeExtent.GetReplicaStats()[0]

	// T471438 -- Fix meta-values leaked by storehost
	// T520701 -- Fix massive int64 negative values
	var fixed bool
	for _, val := range []*int64{rs.AvailableSequence, rs.BeginSequence, rs.LastSequence} {
		if val != nil && (*val >= storageMetaValuesLimit || *val < -1) {
			*val = 0
			fixed = true
		}
	}
	if fixed {
		logger.WithFields(bark.Fields{
			common.TagStor: string(connectedStoreID),
			`cachedStore`:  rs.GetStoreUUID(),
		}).Info(`Queue Depth Temporarily Fixed Replica-Stats`)
	}

	// Skip dangling extents that are excluded by the startFrom time, or modify the consumerGroupExtent to adjust for
	// retention or startFrom
	if !qdc.handleStartFrom(dstDesc, cgDesc, cgExtent, storeExtent, rs, logger) {
		return
	}

	iter.cg.backlogAvailable += qdc.computeBacklog(cgExtent, rs, string(connectedStoreID), logger)
}

func (qdc *queueDepthCalculator) computeBacklog(cgExtent *metadata.ConsumerGroupExtent, rs *shared.ExtentReplicaStats, storeID string, logger bark.Logger) int64 {

	var backlog int64
	var iter = &qdc.iter

	switch qdc.iter.isDLQ {
	case true:
		backlog = common.ExtrapolateDifference(
			common.SequenceNumber(rs.GetLastSequence()),
			common.SequenceNumber(rs.GetBeginSequence())+1, // Begin sequence is -1 if no retention has occurred
			rs.GetLastSequenceRate(),
			0,
			common.UnixNanoTime(rs.GetWriteTime()),
			common.UnixNanoTime(rs.GetWriteTime()),
			iter.cg.extrapolatedTime,
			maxExtrapolation)
	case false:
		backlog = common.ExtrapolateDifference(
			common.SequenceNumber(rs.GetAvailableSequence()),
			common.SequenceNumber(cgExtent.GetAckLevelSeqNo()),
			rs.GetAvailableSequenceRate(),
			cgExtent.GetAckLevelSeqNoRate(),
			common.UnixNanoTime(rs.GetWriteTime()),
			common.UnixNanoTime(cgExtent.GetWriteTime()),
			iter.cg.extrapolatedTime,
			maxExtrapolation)
	}

	if iter.cg.isTabulationRequested {
		logger.WithFields(bark.Fields{
			common.TagStor:      storeID,
			`rsStore`:           rs.GetStoreUUID(),
			`rsAvailSeq`:        rs.GetAvailableSequence(),
			`rsLastSeq`:         rs.GetLastSequence(),
			`rsLastSeqRate`:     rs.GetLastSequenceRate(),
			`cgeAckLvlSeq`:      cgExtent.GetAckLevelSeqNo(),
			`rsAvailSeqRate`:    rs.GetAvailableSequenceRate(),
			`cgeAckLvlSeqRate`:  cgExtent.GetAckLevelSeqNoRate(),
			`rsWriteTime`:       (iter.cg.extrapolatedTime - common.UnixNanoTime(rs.GetWriteTime())).ToSecondsFmt(),
			`cgeWriteTime`:      (iter.cg.extrapolatedTime - common.UnixNanoTime(cgExtent.GetWriteTime())).ToSecondsFmt(),
			`subTotalAvail`:     backlog,
			`runningTotalAvail`: backlog + iter.cg.backlogAvailable,
		}).Info(`Queue Depth Tabulation`)
	}

	return backlog
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
	storeExtent *shared.ExtentStats,
	rs *shared.ExtentReplicaStats,
	logger bark.Logger,
) (qualify bool) {

	if qdc.iter.isDLQ {
		return true // skip over dlq extents
	}

	var startFromSeq common.SequenceNumber
	var trace int // This is just for debugging purposes. It shows the path of 'if's that the function went through before exiting.
	qualify = true
	doGaft := true
	createTime := storeExtent.GetCreatedTimeMillis() * 1000 * 1000

	now := qdc.iter.cg.extrapolatedTime
	isTabulationRequested := qdc.iter.cg.isTabulationRequested

	// Don't allow startFrom to be in the future. Future startFrom should have zero queue depth anyway
	if cgDesc.GetStartFrom() > int64(now) {
		trace = 1
		qualify = false
		goto done
	}

	if consumerGroupExtent.AckLevelSeqNo == nil { // This is a dangling extent; CGE doesn't exist yet; we must synthesize a CGE

		// Evaluate these time-based thresholds in reverse-chronological order
		// rs.GetEndTime >= rs.GetLastEnqueueTimeUtc >= rs.GetBeginTime >= rs.GetBeginEnqueueTimeUtc >= createTime

		switch {
		// No start from means that this extent always qualifies. Only check for beginSequence below.
		case cgDesc.GetStartFrom() == 0:
			trace = 2
			doGaft = false

		// Startfrom is greater than the last firing time for the extent, ignore this extent for now.
		// We haven't cached, so if EndTime change we will re-evaluate
		case rs.GetEndTime() != 0 && cgDesc.GetStartFrom() > rs.GetEndTime():
			trace = 3
			qualify = false
			goto done

		// If we don't have a last firing time (i.e. non-timer destinations), check if the startFrom is greater than the last enqueue time
		// If so, ignore this extent
		case rs.GetEndTime() == 0 && rs.GetLastEnqueueTimeUtc() != 0 && cgDesc.GetStartFrom() > rs.GetLastEnqueueTimeUtc():
			trace = 4
			qualify = false
			goto done

		// These two mean that we will have some part of this extent, and we need to do getAddressFromTimestamp to determine how much
		case rs.GetBeginTime() != 0 && cgDesc.GetStartFrom() >= rs.GetBeginTime():
			trace = 5
		case rs.GetBeginTime() == 0 && rs.GetBeginEnqueueTimeUtc() != 0 && cgDesc.GetStartFrom() >= rs.GetBeginEnqueueTimeUtc():
			trace = 6

		// TODO: we can avoid doing getAddressFromTimestampOnStores() below if we have the begin times and we know startFrom is below them

		// If we don't have the begin times, we can tell that we should have some messages if startFrom is > createTime
		case rs.GetBeginTime() == 0 && rs.GetBeginEnqueueTimeUtc() == 0 && cgDesc.GetStartFrom() > createTime:
			trace = 7

		// If our startFrom time is before the extent was created, all messages in this extent qualify. We don't need to call getAddressFromTimestamp
		case createTime != 0 && cgDesc.GetStartFrom() <= createTime: // If the startFrom is before the create time, the extent always qualifies
			trace = 8
			doGaft = false

		// Everything should be handled up above. Default to calling GetAddressFromTimestamp
		default:
			trace = 9
			if isTabulationRequested {
				logger.WithFields(bark.Fields{
					`rsBeginSeq`:     rs.GetBeginSequence(),
					`rsEndtime`:      rs.GetEndTime(),
					`rsLastEnqueue`:  rs.GetLastEnqueueTimeUtc(),
					`rsBeginTime`:    rs.GetBeginTime(),
					`rsBeginEnqueue`: rs.GetBeginEnqueueTimeUtc(),
					`createTime`:     createTime,
					`startFrom`:      cgDesc.GetStartFrom(),
				}).Warn(`Queue Depth Tabulation (StartFrom) Unhandled case`)
			}
		}

		if doGaft {
			// At this point, startFrom might be in the middle of the extent. We need to call GetAddressFromTimestamp to determine the number of messages
			// after the startFrom point
			startFromSeq = getAddressFromTimestampOnStores(qdc.context, cgDesc, storeExtent.GetExtent().GetStoreUUIDs(), storeExtent.GetExtent().GetExtentUUID(), cgDesc.GetStartFrom())
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
				rs.GetBeginSequence(), // Retention may have removed some messages
				int64(startFromSeq),   // Otherwise, act like we had just opened this extent at startFrom, i.e. don't count messages before startFrom
			))
		}
	}

done:
	if isTabulationRequested {
		logger.WithFields(bark.Fields{
			`rsBeginSeq`:   rs.GetBeginSequence(),
			`startFromSeq`: startFromSeq,
			`rsAvailSeq`:   rs.GetAvailableSequence(),
			`qualify`:      qualify,
			`trace`:        trace,
		}).Info(`Queue Depth Tabulation (StartFrom)`)
	}
	return
}

func (qdc *queueDepthCalculator) measureBacklogProgress(dstDesc *shared.DestinationDescription, cgDesc *shared.ConsumerGroupDescription, backlog int64) int {
	// This CG has backlog, lets identify the
	// extents making progress. If more than
	// 50% are stalled, then emit a metric for
	// alarming
	nOpen := qdc.iter.cg.nOpenExtents
	nStalled := qdc.iter.cg.nStalledExtents

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

	return int(nOpen)
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

func (qdc *queueDepthCalculator) reportStalledMetric(dstDesc *shared.DestinationDescription) bool {
	return !strings.HasPrefix(dstDesc.GetPath(), "/test")
}

func (qdc *queueDepthCalculator) reportNormalBacklog(metricsClient metrics.Client, cgDesc *shared.ConsumerGroupDescription, dstDesc *shared.DestinationDescription, progressScore int, now common.UnixNanoTime) {
	metricsClient.UpdateGauge(metrics.QueueDepthBacklogCGScope, metrics.ControllerCGBacklogAvailable, qdc.iter.cg.backlogAvailable)
	metricsClient.UpdateGauge(metrics.QueueDepthBacklogCGScope, metrics.ControllerCGBacklogInflight, qdc.iter.cg.backlogInflight)
	if qdc.reportStalledMetric(dstDesc) {
		metricsClient.UpdateGauge(metrics.QueueDepthBacklogCGScope, metrics.ControllerCGBacklogProgress, int64(progressScore))
	}

	entry, _ := qdc.queueDepthCache.get(cgDesc.GetConsumerGroupUUID())
	entry.BacklogAvailable = qdc.iter.cg.backlogAvailable
	entry.BacklogInflight = qdc.iter.cg.backlogInflight
	qdc.queueDepthCache.put(cgDesc.GetConsumerGroupUUID(), &entry)
}

func (qdc *queueDepthCalculator) reportDLQBacklog(metricsClient metrics.Client, cgDesc *shared.ConsumerGroupDescription, dstDesc *shared.DestinationDescription, now common.UnixNanoTime) {
	metricsClient.UpdateGauge(metrics.QueueDepthBacklogCGScope, metrics.ControllerCGBacklogDLQ, qdc.iter.cg.backlogAvailable)
	entry, _ := qdc.queueDepthCache.get(cgDesc.GetConsumerGroupUUID())
	entry.BacklogDLQ = qdc.iter.cg.backlogAvailable
	qdc.queueDepthCache.put(cgDesc.GetConsumerGroupUUID(), &entry)
}

func (qdc *queueDepthCalculator) reportBacklog(cgDesc *shared.ConsumerGroupDescription, dstDesc *shared.DestinationDescription, progressScore int, now common.UnixNanoTime) {
	metricsClient := qdc.makeMetricsClient(cgDesc)
	if qdc.iter.isDLQ {
		qdc.reportDLQBacklog(metricsClient, cgDesc, dstDesc, now)
	} else {
		qdc.reportNormalBacklog(metricsClient, cgDesc, dstDesc, progressScore, now)
	}

	if qdc.iter.cg.backlogAvailable != 0 || qdc.iter.cg.backlogInflight != 0 {
		qdc.ll.WithFields(bark.Fields{
			`time`:             now,
			common.TagDst:      common.FmtDst(dstDesc.GetDestinationUUID()),
			common.TagDstPth:   common.FmtDstPth(dstDesc.GetPath()),
			common.TagCnsm:     common.FmtCnsm(cgDesc.GetConsumerGroupUUID()),
			common.TagCnsPth:   common.FmtCnsPth(cgDesc.GetConsumerGroupName()),
			`BacklogAvailable`: qdc.iter.cg.backlogAvailable,
			`BacklogInflight`:  qdc.iter.cg.backlogInflight,
			`progressScore`:    progressScore,
		}).Info(`Queue Depth Metrics`)
	}
}

func (qdc *queueDepthCalculator) isMergedDLQExtentForOtherCG(extent *shared.ExtentStats, cgDesc *shared.ConsumerGroupDescription) bool {
	return len(extent.GetConsumerGroupVisibility()) > 0 &&
		extent.GetConsumerGroupVisibility() != cgDesc.GetConsumerGroupUUID()
}

func (qdc *queueDepthCalculator) isPurgedDLQExtent(extent *shared.ExtentStats) bool {
	return len(extent.GetConsumerGroupVisibility()) > 0 && qdc.iter.isDLQ
}

func (qdc *queueDepthCalculator) isTabulationRequested(cgDesc *shared.ConsumerGroupDescription, dstDesc *shared.DestinationDescription) (tabulationRequested bool) {
	for _, s := range []string{cgDesc.GetOwnerEmail(), dstDesc.GetOwnerEmail()} {
		if strings.Contains(s, QueueDepthTabulationString) {
			return true
		}
	}
	return
}

func (qdc *queueDepthCalculator) makeMetricsClient(cgDesc *shared.ConsumerGroupDescription) metrics.Client {
	cgTagValue, tagErr := common.GetTagsFromPath(cgDesc.GetConsumerGroupName())
	if tagErr != nil {
		cgTagValue = metrics.UnknownDirectoryTagValue
	}
	tags := map[string]string{
		metrics.ConsumerGroupTagName: cgTagValue,
	}
	return metrics.NewClientWithTags(qdc.context.m3Client, metrics.Controller, tags)
}

func (qdc *queueDepthCalculator) makeCGExtentLogger(dstDesc *shared.DestinationDescription, cgDesc *shared.ConsumerGroupDescription, cgExtent *metadata.ConsumerGroupExtent) bark.Logger {
	logger := qdc.ll
	if qdc.iter.cg.isTabulationRequested {
		logger = qdc.ll.WithFields(bark.Fields{
			`time`:           qdc.iter.cg.extrapolatedTime,
			common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
			common.TagDstPth: common.FmtDstPth(dstDesc.GetPath()),
			common.TagCnsm:   common.FmtCnsm(cgDesc.GetConsumerGroupUUID()),
			common.TagCnsPth: common.FmtCnsPth(cgDesc.GetConsumerGroupName()),
			common.TagExt:    common.FmtExt(cgExtent.GetExtentUUID()),
		})
	}
	return logger
}

func newQueueDepthCache() *QueueDepthCache {
	return &QueueDepthCache{
		capacity: queueDepthCacheLimit,
		entries:  make(map[string]QueueDepthCacheEntry),
	}
}

func (cache *QueueDepthCache) get(key string) (QueueDepthCacheEntry, bool) {
	cache.RLock()
	defer cache.RUnlock()
	if entry, ok := cache.entries[key]; ok {
		return entry, ok
	}
	return QueueDepthCacheEntry{}, false
}

func (cache *QueueDepthCache) put(key string, value *QueueDepthCacheEntry) {
	cache.Lock()
	defer cache.Unlock()
	if len(cache.entries) > cache.capacity { // Random eviction if the cache has grown too large;
		for k := range cache.entries {
			delete(cache.entries, k)
			break
		}
	}
	cache.entries[key] = *value
}
