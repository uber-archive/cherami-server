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
	"math"
	"math/rand"
	"strings"
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
	gaftCache = make(map[gaftKey]common.SequenceNumber)
)

type (
	extentID string
	storeID  string

	queueDepthCalculator struct {
		context         *Context
		ll              bark.Logger
		queueDepthCache *QueueDepthCache

		// iter represents the state during a
		// single iteration of a destination and/or
		// consumer group
		iter struct {
			isDLQ                    bool // is the current destination a DLQ
			dstDesc                  *shared.DestinationDescription
			dstExtents               map[extentID]*metadata.DestinationExtent // set of extents for the current dst
			storeExtentMetadataCache *storeExtentMetadataCache

			cg struct {
				now                   common.UnixNanoTime
				backlogAvailable      int64
				backlogInflight       int64
				nOpenExtents          int64 // stat for stallness check
				nStalledExtents       int64 // stat for stallness check
				desc                  *shared.ConsumerGroupDescription
				coverMap              map[extentID]struct{} // list of extents that account towards backlog
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
		context:         context,
		queueDepthCache: newQueueDepthCache(),
		ll:              context.log.WithField(`module`, `queueDepth`),
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

	// ignore Kafka destinations; we do not support queue-depth for Kafka destinations
	if e.dest != nil && e.dest.GetType() == shared.DestinationType_KAFKA {
		return
	}

	switch e.t {
	case eDestStart:
		qdc.handleDestinationStart(e.dest)
	case eDestEnd:
		qdc.handleDestinationEnd(e.dest)
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
	qdc.iter.dstExtents = make(map[extentID]*metadata.DestinationExtent, 16)
	qdc.iter.storeExtentMetadataCache = newStoreExtentMetadataCache(qdc.context.mm, qdc.ll)
}

func (qdc *queueDepthCalculator) handleDestinationEnd(dstDesc *shared.DestinationDescription) {
	qdc.iter.dstDesc = nil
	qdc.iter.isDLQ = false
	qdc.iter.storeExtentMetadataCache = nil
	qdc.iter.dstExtents = nil
}

func (qdc *queueDepthCalculator) handleConsumerGroupStart(dstDesc *shared.DestinationDescription, cgDesc *shared.ConsumerGroupDescription) {
	iter := &qdc.iter
	iter.cg.desc = cgDesc
	iter.cg.isTabulationRequested = qdc.isTabulationRequested(cgDesc, dstDesc)

	if iter.isDLQ && cgDesc.GetDeadLetterQueueDestinationUUID() != dstDesc.GetDestinationUUID() {
		return // we don't compute backlog for dlq consumer groups
	}

	iter.cg.now = common.Now()
	iter.cg.coverMap = qdc.getExtentCoverMap()

	if iter.cg.isTabulationRequested {
		qdc.ll.WithFields(bark.Fields{
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

	// for deleting/deleted dest/CG, report zero backlog
	if cgDesc.GetStatus() == shared.ConsumerGroupStatus_DELETING ||
		cgDesc.GetStatus() == shared.ConsumerGroupStatus_DELETED ||
		dstDesc.GetStatus() == shared.DestinationStatus_DELETING ||
		dstDesc.GetStatus() == shared.DestinationStatus_DELETED {

		qdc.reportBacklog(cgDesc, dstDesc, backlogProgessInfinity, 0)

	} else {

		// iterate over the dstExtents that are not assigned
		// to the consumer group yet and account them towards
		// total backlog
		for extent := range iter.cg.coverMap {
			dstExtent, _ := iter.dstExtents[extent] // if lookup fails, code is broken
			storeUUIDs := dstExtent.GetStoreUUIDs()
			if len(storeUUIDs) == 0 {
				continue // should never happen
			}
			cge := &shared.ConsumerGroupExtent{
				ConsumerGroupUUID:  common.StringPtr(cgDesc.GetConsumerGroupUUID()),
				ExtentUUID:         common.StringPtr(string(extent)),
				OutputHostUUID:     common.StringPtr(zeroUUID),
				ConnectedStoreUUID: common.StringPtr(storeUUIDs[rand.Intn(len(storeUUIDs))]), // pick a random store for backlog computation
			}
			qdc.addExtentBacklog(dstDesc, cgDesc, dstExtent, cge, qdc.makeCGExtentLogger(dstDesc, cgDesc, cge))
		}

		// stallness check, can be performed only after processing all extents
		progressScore := qdc.measureBacklogProgress(dstDesc, cgDesc, iter.cg.backlogAvailable)
		qdc.reportBacklog(cgDesc, dstDesc, progressScore, iter.cg.now)
	}

	if iter.cg.isTabulationRequested {
		qdc.ll.WithFields(bark.Fields{
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

	if dstDesc.GetStatus() == shared.DestinationStatus_DELETING ||
		dstDesc.GetStatus() == shared.DestinationStatus_DELETED ||
		extent.GetStatus() > shared.ExtentStatus_SEALED {
		return
	}
	qdc.iter.dstExtents[extentID(extent.GetExtentUUID())] = extent
}

func (qdc *queueDepthCalculator) handleConsumerGroupExtent(dstDesc *shared.DestinationDescription, cgDesc *shared.ConsumerGroupDescription, cgExtent *shared.ConsumerGroupExtent) {

	// skip deleted/deleting dest/CG
	if cgDesc.GetStatus() == shared.ConsumerGroupStatus_DELETING ||
		cgDesc.GetStatus() == shared.ConsumerGroupStatus_DELETED ||
		dstDesc.GetStatus() == shared.DestinationStatus_DELETING ||
		dstDesc.GetStatus() == shared.DestinationStatus_DELETED {

		return
	}

	iter := &qdc.iter
	logger := qdc.makeCGExtentLogger(dstDesc, cgDesc, cgExtent)

	if cgExtent.GetStatus() == shared.ConsumerGroupExtentStatus_OPEN {
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

	dstExtent, ok := iter.dstExtents[extentID(cgExtent.GetExtentUUID())]
	if !ok {
		return // cgExtent not in dstExtent, must never happen
	}

	qdc.addExtentBacklog(dstDesc, cgDesc, dstExtent, cgExtent, logger)
	delete(iter.cg.coverMap, extentID(cgExtent.GetExtentUUID()))
}

// addExtentBacklog counts the number of un-consumed
// messages in the given extent towards the consumer
// group backlog
func (qdc *queueDepthCalculator) addExtentBacklog(
	dstDesc *shared.DestinationDescription,
	cgDesc *shared.ConsumerGroupDescription,
	dstExtent *metadata.DestinationExtent,
	cgExtent *shared.ConsumerGroupExtent,
	logger bark.Logger) {

	extID := extentID(cgExtent.GetExtentUUID())
	connectedStoreID := storeID(cgExtent.GetConnectedStoreUUID())

	iter := &qdc.iter

	if cgExtent.GetStatus() != shared.ConsumerGroupExtentStatus_OPEN {
		if iter.cg.isTabulationRequested {
			logger.WithFields(bark.Fields{
				common.TagStor:     string(connectedStoreID),
				`cgeStatus`:        cgExtent.GetStatus(),
				`cgeAckLvlSeq`:     cgExtent.GetAckLevelSeqNo(),
				`cgeAckLvlSeqRate`: cgExtent.GetAckLevelSeqNoRate(),
				`cgeWriteTime`:     common.UnixNanoTime(cgExtent.GetWriteTime()).ToSecondsFmt(),
			}).Info(`Queue Depth Tabulation (skipping consumed/deleted extent)`)
		}
		return
	}

	if !qdc.isExtentBelongsToCG(dstExtent, cgDesc) || qdc.isPurgedDLQExtent(dstExtent) {
		if iter.cg.isTabulationRequested {
			logger.Info(`Queue Depth Tabulation, skipping other other CG's merged and/or purged dlq extent`)
		}
		return
	}

	ΔInFlight := common.MaxInt64(0, cgExtent.GetReadLevelSeqNo()-cgExtent.GetAckLevelSeqNo())
	iter.cg.backlogInflight += ΔInFlight

	// Calculations requiring store extent
	storeExtentMetadata := iter.storeExtentMetadataCache.get(connectedStoreID, extID)

	if storeExtentMetadata == nil {
		// Can only happen due to cassandra's eventual consistency
		// nature. Without store metadata, we can't compute backlog
		if iter.cg.isTabulationRequested {
			logger.WithFields(bark.Fields{
				common.TagStor:     string(connectedStoreID),
				`cgeAckLvlSeq`:     cgExtent.GetAckLevelSeqNo(),
				`cgeAckLvlSeqRate`: cgExtent.GetAckLevelSeqNoRate(),
				`cgeWriteTime`:     common.UnixNanoTime(cgExtent.GetWriteTime()).ToSecondsFmt(),
			}).Info(`Queue Depth Tabulation (missing store extent)`)
		}
		return
	}

	// Skip dangling extents that are excluded by the startFrom time, or modify the consumerGroupExtent to adjust for
	// retention or startFrom
	if !qdc.handleStartFrom(dstDesc, cgDesc, dstExtent, cgExtent, storeExtentMetadata, logger) {
		return
	}

	iter.cg.backlogAvailable += qdc.computeBacklog(cgDesc, cgExtent, storeExtentMetadata, string(connectedStoreID), logger)
}

func (qdc *queueDepthCalculator) computeBacklog(cgDesc *shared.ConsumerGroupDescription, cgExtent *shared.ConsumerGroupExtent, storeMetadata *storeExtentMetadata, storeID string, logger bark.Logger) int64 {

	var backlog int64 // backlog defaults to '0'
	var iter = &qdc.iter

	if qdc.iter.isDLQ {
		// update backog, only if the begin/first seqnums are available.
		// {begin,last}Sequence of:
		// 0 -> extent created, but store has not reported metrics yet (no updates on store).
		// MaxInt64 -> extent loaded on store, but no msgs actually written yet; so store
		// 		reported begin/last seq as 'unknown'.
		if storeMetadata.lastSequence != 0 && storeMetadata.lastSequence != math.MaxInt64 &&
			storeMetadata.beginSequence != 0 && storeMetadata.beginSequence != math.MaxInt64 {
			backlog = storeMetadata.lastSequence - storeMetadata.beginSequence + 1
		}
	} else {
		// update backlog, only if there is an available seqnum. see comment above for interpretation
		// of "0" and "MaxInt64".
		if storeMetadata.availableSequence != 0 && storeMetadata.availableSequence != math.MaxInt64 {
			backlog = storeMetadata.availableSequence - cgExtent.GetAckLevelSeqNo()
		}
	}

	if iter.cg.isTabulationRequested {
		logger.WithFields(bark.Fields{
			common.TagStor:      storeID,
			`rsStore`:           storeMetadata.storeID,
			`rsAvailSeq`:        storeMetadata.availableSequence,
			`rsBeginSeq`:        storeMetadata.beginSequence,
			`rsLastSeq`:         storeMetadata.lastSequence,
			`rsLastSeqRate`:     storeMetadata.lastSequenceRate,
			`cgeAckLvlSeq`:      cgExtent.GetAckLevelSeqNo(),
			`rsAvailSeqRate`:    storeMetadata.availableSequenceRate,
			`cgeAckLvlSeqRate`:  cgExtent.GetAckLevelSeqNoRate(),
			`rsWriteTime`:       storeMetadata.writeTime.ToSecondsFmt(),
			`cgeWriteTime`:      common.UnixNanoTime(cgExtent.GetWriteTime()).ToSecondsFmt(),
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
	dstExtent *metadata.DestinationExtent,
	consumerGroupExtent *shared.ConsumerGroupExtent,
	storeMetadata *storeExtentMetadata,
	logger bark.Logger,
) (qualify bool) {

	if qdc.iter.isDLQ {
		return true // skip over dlq extents
	}

	var startFromSeq common.SequenceNumber
	var trace int // This is just for debugging purposes. It shows the path of 'if's that the function went through before exiting.
	qualify = true
	doGaft := true
	createTime := storeMetadata.createdTime

	now := qdc.iter.cg.now
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
		case storeMetadata.endTime != 0 && cgDesc.GetStartFrom() > storeMetadata.endTime:
			trace = 3
			qualify = false
			goto done

		// If we don't have a last firing time (i.e. non-timer destinations), check if the startFrom is greater than the last enqueue time
		// If so, ignore this extent
		case storeMetadata.endTime == 0 && storeMetadata.lastEnqueueTimeUtc != 0 && cgDesc.GetStartFrom() > storeMetadata.lastEnqueueTimeUtc:
			trace = 4
			qualify = false
			goto done

		// These two mean that we will have some part of this extent, and we need to do getAddressFromTimestamp to determine how much
		case storeMetadata.beginTime != 0 && cgDesc.GetStartFrom() >= storeMetadata.beginTime:
			trace = 5
		case storeMetadata.beginTime == 0 && storeMetadata.beginEnqueueTimeUtc != 0 && cgDesc.GetStartFrom() >= storeMetadata.beginEnqueueTimeUtc:
			trace = 6

		// TODO: we can avoid doing getAddressFromTimestampOnStores() below if we have the begin times and we know startFrom is below them

		// If we don't have the begin times, we can tell that we should have some messages if startFrom is > createTime
		case storeMetadata.beginTime == 0 && storeMetadata.beginEnqueueTimeUtc == 0 && cgDesc.GetStartFrom() > int64(createTime):
			trace = 7

		// If our startFrom time is before the extent was created, all messages in this extent qualify. We don't need to call getAddressFromTimestamp
		case createTime != 0 && cgDesc.GetStartFrom() <= int64(createTime): // If the startFrom is before the create time, the extent always qualifies
			trace = 8
			doGaft = false

		// Everything should be handled up above. Default to calling GetAddressFromTimestamp
		default:
			trace = 9
			if isTabulationRequested {
				logger.WithFields(bark.Fields{
					`rsBeginSeq`:     storeMetadata.beginSequence,
					`rsEndtime`:      storeMetadata.endTime,
					`rsLastEnqueue`:  storeMetadata.lastEnqueueTimeUtc,
					`rsBeginTime`:    storeMetadata.beginTime,
					`rsBeginEnqueue`: storeMetadata.beginEnqueueTimeUtc,
					`createTime`:     createTime,
					`startFrom`:      cgDesc.GetStartFrom(),
				}).Warn(`Queue Depth Tabulation (StartFrom) Unhandled case`)
			}
		}

		if doGaft {
			// At this point, startFrom might be in the middle of the extent. We need to call GetAddressFromTimestamp to determine the number of messages
			// after the startFrom point
			startFromSeq = getAddressFromTimestampOnStores(qdc.context, cgDesc, dstExtent.GetStoreUUIDs(), dstExtent.GetExtentUUID(), cgDesc.GetStartFrom())
			if startFromSeq == gaftSealed {
				trace += 10
				qualify = false // We include none of the messages in this extent
			} else { // We will include some or all of the messages in this extent
				trace += 20
			}
		}

		// if doGaft == false, this just adjusts for retention, if applicable
		if qualify {
			consumerGroupExtent.WriteTime = common.Int64Ptr(int64(now))

			// retention may have purged some messages, account for that
			if storeMetadata.beginSequence != math.MaxInt64 {

				trace += 100

				// the 'ack-level' corresponds to message that has already been read (ie 'acked');
				// so reduce one from the 'beginSequence' to since that msg has not been read.
				consumerGroupExtent.AckLevelSeqNo = common.Int64Ptr(common.MaxInt64(
					storeMetadata.beginSequence-1, // Retention may have removed some messages
					int64(startFromSeq),           // Otherwise, act like we had just opened this extent at startFrom, i.e. don't count messages before startFrom
				))

			} else {

				trace += 200

				// if the extent is empty, then assume the 'ack-level' is at the beginning (ie, seqnum 0)
				consumerGroupExtent.AckLevelSeqNo = common.Int64Ptr(0)
			}
		}
	}

done:
	if isTabulationRequested {
		logger.WithFields(bark.Fields{
			`rsBeginSeq`:   storeMetadata.beginSequence,
			`startFromSeq`: startFromSeq,
			`rsAvailSeq`:   storeMetadata.availableSequence,
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

func (qdc *queueDepthCalculator) isCGExtentStalled(cge *shared.ConsumerGroupExtent) bool {

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

// getExtentCoverMap returns a copy of all the extent
// ids for the currently iterated destination
func (qdc *queueDepthCalculator) getExtentCoverMap() map[extentID]struct{} {
	result := make(map[extentID]struct{}, len(qdc.iter.dstExtents))
	for extID := range qdc.iter.dstExtents {
		result[extID] = struct{}{}
	}
	return result
}

func (qdc *queueDepthCalculator) isExtentBelongsToCG(extent *metadata.DestinationExtent, cgDesc *shared.ConsumerGroupDescription) bool {
	return len(extent.GetConsumerGroupVisibility()) == 0 ||
		extent.GetConsumerGroupVisibility() == cgDesc.GetConsumerGroupUUID()
}

func (qdc *queueDepthCalculator) isPurgedDLQExtent(extent *metadata.DestinationExtent) bool {
	return len(extent.GetConsumerGroupVisibility()) > 0 && qdc.iter.isDLQ
}

func (qdc *queueDepthCalculator) isTabulationRequested(cgDesc *shared.ConsumerGroupDescription, dstDesc *shared.DestinationDescription) (tabulationRequested bool) {
	options := cgDesc.GetOptions()
	if enabled, ok := options[common.FlagEnableQueueDepthTabulation]; ok && enabled == "true" {
		return true
	}

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

func (qdc *queueDepthCalculator) makeCGExtentLogger(dstDesc *shared.DestinationDescription, cgDesc *shared.ConsumerGroupDescription, cgExtent *shared.ConsumerGroupExtent) bark.Logger {
	logger := qdc.ll
	if qdc.iter.cg.isTabulationRequested {
		logger = qdc.ll.WithFields(bark.Fields{
			common.TagDst:    common.FmtDst(dstDesc.GetDestinationUUID()),
			common.TagDstPth: common.FmtDstPth(dstDesc.GetPath()),
			common.TagCnsm:   common.FmtCnsm(cgDesc.GetConsumerGroupUUID()),
			common.TagCnsPth: common.FmtCnsPth(cgDesc.GetConsumerGroupName()),
			common.TagExt:    common.FmtExt(cgExtent.GetExtentUUID()),
		})
	}
	return logger
}
