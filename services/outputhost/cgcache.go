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

package outputhost

import (
	"sync"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"

	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/outputhost/load"
)

type (
	// cacheMsg is the message written to the cg cache
	// it has the actual message and a connection ID
	cacheMsg struct {
		msg    *cherami.ConsumerMessage
		connID int
	}

	// consumerGroupCache holds all the extents for this consumer group
	consumerGroupCache struct {
		// extMutex is the lock for protecting the extentCache
		extMutex sync.RWMutex

		// cachedCGDesc is the cached consumer group description from the last time we updated it
		//
		// We need the below 3 fields to handle reconfigure:
		//
		// destUUID holds the UUID for the corresponding destination
		// startFrom is the time to start this CG from
		// lockTimeout is the message timeout for this consumer group
		cachedCGDesc shared.ConsumerGroupDescription

		// cachedTime is the last time that the consumer group description was cached; TODO: invalidate after some time?
		cachedTime common.UnixNanoTime

		// destPath is the original destination path, which is required for a metadataClient.ReadConsumerGroup call;
		// TODO: update metadata client to support ReadCG by UUID
		destPath string

		// currID is the current connection ID
		currID int

		// msgsCh is the channel which is used to deliver message to the client connections
		msgsCh chan *cherami.ConsumerMessage

		// msgsRedeliveryCh is the channel which is used to re-deliver message to the client connections
		msgsRedeliveryCh chan *cherami.ConsumerMessage

		// priorityMsgsRedeliveryCh delivers high-priority messages to the client connections. Used for good message injection in smart retries
		priorityMsgsRedeliveryCh chan *cherami.ConsumerMessage

		// msgCacheCh is the channel which gets the messages to be added to the msg cache
		msgCacheCh chan cacheMsg

		// msgCacheRedeliveredCh is the channel which gets the messages after redelivery
		msgCacheRedeliveredCh chan cacheMsg

		// ackMsgCh is the channel which is used to make sure the msgCache is updated, when we get an ACK
		ackMsgCh chan timestampedAckID

		// nackMsgCh is the same as ackMsgCh, but will cause an immediately delivery to the DLQ as well as acknowledgement
		nackMsgCh chan timestampedAckID

		// notifyConsCloseCh is the channel used to get closed client connections
		notifyConsCloseCh chan int

		// notifyReplicaCloseCh is the channel used to get closed streams to replicas
		notifyReplicaCloseCh chan string

		// connections is the map of connectionID to all the client connections on this CG
		connections map[int]*consConnection

		// extCache is the map of all extentUUIDs to the corresponding extCache object on this CG
		extentCache map[string]*extentCache

		// msgDeliveryCache is the cache used to hold all the messages for redelivery
		msgDeliveryCache *cgMsgCache

		// closeChannel is used to stop the cache management routine during shutdown
		closeChannel chan struct{}

		// shutdownWG is the waitgroup to make sure the shutdown process properly waits for all go routines to complete
		shutdownWG *sync.WaitGroup

		// metaClient is the client to the metadata
		metaClient metadata.TChanMetadataService

		// tClients is the client factory interface
		tClients common.ClientFactory

		// wsConnector takes care of establishing connection via websocket stream
		wsConnector common.WSConnector

		// outputHostUUID is the UUID of this outputhost
		outputHostUUID string

		// logger is the tag based logger used for logging
		logger bark.Logger

		// notifyUnloadCh is a write-only channel to let the outputhost know that this cache is unloaded
		notifyUnloadCh chan<- string

		//consumerM3Client for metrics per consumer group
		consumerM3Client metrics.Client

		//m3Client for metrics per consumer group
		m3Client metrics.Client

		// notifier is the interface which notifies connections about potential throttling
		notifier Notifier

		// creditNotifyCh is the channel to send credits to extents, which is used by msgCache
		creditNotifyCh chan int32

		// creditRequestCh is the channel used to request credits for an extent
		creditRequestCh chan string

		// lastDisconnectTime is the time the last consumer got disconnected
		lastDisconnectTime time.Time

		// sessionID is the 16 bit session identifier for this host
		sessionID uint16

		// ackMgrLoadCh is the channel used to notify the outputhost when an
		// ackMgr is loaded
		ackMgrLoadCh chan<- ackMgrLoadMsg

		// ackMgrUnloadCh is the channel used to notify the outputhost to
		// remove an ackMgr from the map
		ackMgrUnloadCh chan<- uint32

		// ackIDGenerator is the ackIDGenerator interface for this host
		ackIDGen common.HostAckIDGenerator

		// loadReporterFactory is the factory that vends load reporters
		loadReporterFactory common.LoadReporterDaemonFactory

		// loadReporter to report metrics to controller
		loadReporter common.LoadReporterDaemon

		// hostMetrics represents the host level load metrics reported to controller
		hostMetrics *load.HostMetrics
		// cgMetrics represents consgroup level load metrics reported to controller
		cgMetrics *load.CGMetrics
	}
)

// getAddressCtxTimeout is the timeout for the GetAddressFromTimestampCall
const getAddressCtxTimeout = 60 * time.Second

// metaPollTimeout is the interval to poll metadata
const metaPollTimeout = 1 * time.Minute

// idleTimeout is the interval to check for an idle cg cache and unload those
const idleTimeout = 30 * time.Minute

// unloadTickerTimeout is the interval to see if we can unload the cgCache
const unloadTickerTimeout = 10 * time.Minute

// defaultMaxResults is the max number of extents to get from metadata
// as part of ReadConsumerGroupExtentsRequest
const defaultMaxResults = 500

// consGroupLoadReportingInterval is the freq at which CG metrics
// are reported to controller
const consGroupLoadReportingInterval = 2 * time.Second

// defaultNumOutstandingMsgs is the number of outstanding messages we can have for this CG
var defaultNumOutstandingMsgs int32 = 10000

// ErrCgUnloaded is returned when the cgCache is already unloaded
var ErrCgUnloaded = &cherami.InternalServiceError{Message: "ConsumerGroup already unloaded"}

// newConsumerGroupCache is used to get a new instance of the CG cache
func newConsumerGroupCache(destPath string, cgDesc shared.ConsumerGroupDescription, cgLogger bark.Logger, h *OutputHost) *consumerGroupCache {
	cgCache := &consumerGroupCache{
		destPath:                 destPath,
		cachedCGDesc:             cgDesc,
		cachedTime:               common.Now(),
		extentCache:              make(map[string]*extentCache),
		msgsCh:                   make(chan *cherami.ConsumerMessage, defaultPrefetchBufferSize),
		msgsRedeliveryCh:         make(chan *cherami.ConsumerMessage, defaultPrefetchBufferSize),
		priorityMsgsRedeliveryCh: make(chan *cherami.ConsumerMessage, 1),
		notifyConsCloseCh:        make(chan int, 5),
		notifyReplicaCloseCh:     make(chan string, 5),
		connections:              make(map[int]*consConnection),
		msgCacheCh:               make(chan cacheMsg, defaultPrefetchBufferSize),
		msgCacheRedeliveredCh:    make(chan cacheMsg, defaultPrefetchBufferSize),
		ackMsgCh:                 make(chan timestampedAckID, ackChannelSize), // Have a buffer of at least 1, so that we aren't necessarily synchronous with the counterparty
		nackMsgCh:                make(chan timestampedAckID, ackChannelSize),
		closeChannel:             make(chan struct{}),
		outputHostUUID:           h.GetHostUUID(),
		tClients:                 h.GetClientFactory(),
		wsConnector:              h.GetWSConnector(),
		metaClient:               h.metaClient,
		shutdownWG:               &h.shutdownWG,
		logger:                   cgLogger.WithField(common.TagModule, `cgCache`),
		notifyUnloadCh:           h.unloadCacheCh,
		m3Client:                 h.m3Client,
		notifier:                 newNotifier(),
		creditNotifyCh:           make(chan int32, 50),
		creditRequestCh:          make(chan string, 50),
		lastDisconnectTime:       time.Now(),
		sessionID:                h.sessionID,
		ackIDGen:                 h.ackMgrIDGen,
		ackMgrLoadCh:             h.ackMgrLoadCh,
		ackMgrUnloadCh:           h.ackMgrUnloadCh,
		loadReporterFactory:      h.GetLoadReporterDaemonFactory(),
		hostMetrics:              h.hostMetrics,
		cgMetrics:                load.NewCGMetrics(),
	}

	cgCache.consumerM3Client = metrics.NewClientWithTags(h.m3Client, metrics.Outputhost, cgCache.getConsumerGroupTags())
	cgCache.loadReporter = cgCache.loadReporterFactory.CreateReporter(consGroupLoadReportingInterval, cgCache, cgLogger)
	cgCache.loadReporter.Start()
	return cgCache
}

func (cgCache *consumerGroupCache) getConsumerGroupTags() map[string]string {

	destTagValue, tagErr := common.GetTagsFromPath(cgCache.destPath)
	if tagErr != nil {
		destTagValue = metrics.UnknownDirectoryTagValue
		cgCache.logger.WithField(common.TagDstPth, cgCache.destPath).
			WithField(common.TagUnknowPth, metrics.UnknownDirectoryTagValue).
			Error("unknown destination path, return default name")
	}

	cgTagValue, tagErr := common.GetTagsFromPath(cgCache.cachedCGDesc.GetConsumerGroupName())
	if tagErr != nil {
		cgTagValue = metrics.UnknownDirectoryTagValue
		cgCache.logger.WithField(common.TagCnsPth, cgCache.cachedCGDesc.GetConsumerGroupName()).
			WithField(common.TagUnknowPth, metrics.UnknownDirectoryTagValue).
			Error("unknown consumer group path, return default name")
	}
	tags := map[string]string{
		metrics.DestinationTagName:   destTagValue,
		metrics.ConsumerGroupTagName: cgTagValue,
	}
	return tags
}

// loadExtentCache loads the extent cache, if it doesn't already exist for this consumer group
func (cgCache *consumerGroupCache) loadExtentCache(tClients common.ClientFactory,
	metaClient metadata.TChanMetadataService, wsConnector common.WSConnector,
	outputHostUUID string, destUUID string, destType shared.DestinationType,
	cgUUID string, cge *metadata.ConsumerGroupExtent, startFrom int64) (err error) {
	extUUID := cge.GetExtentUUID()
	extLogger := cgCache.logger.WithField(common.TagExt, extUUID)
	if extCache, exists := cgCache.extentCache[extUUID]; !exists {
		extCache = &extentCache{
			cgUUID:               cgUUID,
			extUUID:              extUUID,
			destUUID:             destUUID,
			destType:             destType,
			storeUUIDs:           cge.StoreUUIDs,
			startFrom:            startFrom,
			notifyReplicaCloseCh: make(chan error, 5),
			closeChannel:         make(chan struct{}),
			waitConsumedCh:       make(chan bool, 1),
			msgsCh:               cgCache.msgsCh,
			connectionsClosedCh:  cgCache.notifyReplicaCloseCh,
			shutdownWG:           cgCache.shutdownWG,
			tClients:             tClients,
			wsConnector:          wsConnector,
			logger:               extLogger.WithField(common.TagModule, `extCache`),
			creditNotifyCh:       cgCache.creditNotifyCh,
			creditRequestCh:      cgCache.creditRequestCh,
			initialCredits:       defaultNumOutstandingMsgs,
			loadMetrics:          load.NewExtentMetrics(),
		}

		cgCache.extentCache[extUUID] = extCache
		cgCache.cgMetrics.Increment(load.CGMetricNumOpenExtents)
		cgCache.hostMetrics.Increment(load.HostMetricNumOpenExtents)

		// TODO: create a newAckManagerRequestArgs struct here
		extCache.ackMgr = newAckManager(cgCache, cgCache.ackIDGen.GetNextAckID(), outputHostUUID, cgUUID, extCache.extUUID, &extCache.connectedStoreUUID, extCache.waitConsumedCh, cge, metaClient, extCache.logger)
		extCache.loadReporter = cgCache.loadReporterFactory.CreateReporter(extentLoadReportingInterval, extCache, extCache.logger)

		// make sure we prevent shutdown from racing
		extCache.shutdownWG.Add(1)

		// if load fails we will unload it the usual way
		go extCache.load(outputHostUUID, cgUUID, metaClient, cge)

		// now notify the outputhost
		cgCache.ackMgrLoadCh <- ackMgrLoadMsg{uint32(extCache.ackMgr.ackMgrID), extCache.ackMgr}
	}
	return
}

// this is the routine responsible for managing the CG cache and all the
// extents within the cache.
// There are 3 scenarios to consider here:
// 1. A client stream is closed which means we need to update the
//    cgCache.connections and make sure we close all the streams below, if needed.
// 2. A replica stream can be closed underneath while the client stream is still open.
//    In this case, we need to make sure we close the client streams if we have closed
//    all the extents
// 3. Shutdown -> which means we are anyways closing everything below in unloadAll()
func (cgCache *consumerGroupCache) manageConsumerGroupCache() {

	cgCache.logger.Info(`cgCache initialized`)

	defer cgCache.shutdownWG.Done()
	quit := false

	refreshTicker := time.NewTicker(metaPollTimeout) // start ticker to refresh metadata
	defer refreshTicker.Stop()

	unloadTicker := time.NewTicker(unloadTickerTimeout) // start ticker to refresh metadata
	defer unloadTicker.Stop()

	var ackMgrUnloadID uint32
	for !quit {
		select {
		case conn := <-cgCache.notifyConsCloseCh:
			cgCache.extMutex.Lock()
			if _, ok := cgCache.connections[conn]; ok {
				cgCache.logger.WithField(`conn`, conn).Info(`removing connection`)
				delete(cgCache.connections, conn)
				// decrease the open conn count
				cgCache.cgMetrics.Decrement(load.CGMetricNumOpenConns)
			}
			// if all consumers are disconnected, keep the cgCache loaded for
			// some idle time
			if cgCache.getNumOpenConns() <= 0 {
				cgCache.lastDisconnectTime = time.Now()
			}
			cgCache.extMutex.Unlock()
		case extUUID := <-cgCache.notifyReplicaCloseCh:
			// this means we are done with this extent. remove it from the cache
			cgCache.extMutex.Lock()
			if extCache, ok := cgCache.extentCache[extUUID]; ok {
				cgCache.logger.WithField(common.TagExt, extUUID).Info("removing extent")
				// get the ackMgrID and unload it as well
				ackMgrUnloadID = uint32(extCache.ackMgr.ackMgrID)
				delete(cgCache.extentCache, extUUID)
				cgCache.cgMetrics.Decrement(load.CGMetricNumOpenExtents)
				cgCache.hostMetrics.Decrement(load.HostMetricNumOpenExtents)
			}
			if len(cgCache.extentCache) == 0 {
				// this means all the extents are closed
				// no point in keeping the streams to the client open
				for _, conn := range cgCache.connections {
					go conn.close()
				}
			}
			cgCache.extMutex.Unlock()

			// try to unload the ackMgr as well
			if ackMgrUnloadID > 0 {
				select {
				case cgCache.ackMgrUnloadCh <- ackMgrUnloadID:
					ackMgrUnloadID = 0
				default:
				}
			}
		case <-refreshTicker.C:
			cgCache.logger.Debug("refreshing all extents")
			cgCache.extMutex.Lock()
			cgCache.refreshCgCache(nil)
			cgCache.extMutex.Unlock()
		case <-unloadTicker.C:
			cgCache.extMutex.Lock()
			// if there are no consumers and we have been idle after the last disconnect
			// for more than the idleTimeout, unload the cgCache
			if cgCache.getNumOpenConns() <= 0 {
				if time.Since(cgCache.lastDisconnectTime) > idleTimeout {
					cgCache.logger.Info("unloading empty/idle cache")
					go cgCache.unloadConsumerGroupCache()
				}
			}
			cgCache.extMutex.Unlock()
		case <-cgCache.closeChannel:
			cgCache.logger.Info("stopped")
			// stop the delivery cache as well
			cgCache.msgDeliveryCache.stop()
			quit = true
		}
	}
}

// refreshCgCache contacts metadata to get all the open extents and then
// loads them if needed
func (cgCache *consumerGroupCache) refreshCgCache(ctx thrift.Context) error {

	// First check any status changes to the destination or consumer group
	// If one of them is being deleted, then just unload all extents and
	// move on. If not, check for new extents for this consumer group

	dstReq := &metadata.ReadDestinationRequest{DestinationUUID: common.StringPtr(cgCache.cachedCGDesc.GetDestinationUUID())}
	dstDesc, errRD := cgCache.metaClient.ReadDestination(ctx, dstReq)
	if errRD != nil || dstDesc == nil {
		cgCache.logger.WithField(common.TagErr, errRD).Error(`ReadDestination failed on refresh`)
		return errRD
	}

	if dstDesc.GetStatus() == shared.DestinationStatus_DELETING || dstDesc.GetStatus() == shared.DestinationStatus_DELETED {
		cgCache.logger.Info("destination deleted; unloading all extents")
		go cgCache.unloadConsumerGroupCache()
		return ErrCgUnloaded
	}

	readReq := &metadata.ReadConsumerGroupRequest{
		DestinationUUID:   common.StringPtr(cgCache.cachedCGDesc.GetDestinationUUID()),
		ConsumerGroupName: common.StringPtr(cgCache.cachedCGDesc.GetConsumerGroupName()),
	}

	cgDesc, errRCG := cgCache.metaClient.ReadConsumerGroup(ctx, readReq)
	if errRCG != nil {
		cgCache.logger.WithField(common.TagErr, errRCG).Error(`ReadConsumerGroup failed on refresh`)
		return errRCG
	}

	if cgDesc.GetStatus() == shared.ConsumerGroupStatus_DELETED {
		// If the consumer group is deleted, drain all connections
		// and unload all the extent from cache. This is the only
		// way, async clean happens in response to a DELETE
		cgCache.logger.Info("consumer group deleted; unloading all extents")
		go cgCache.unloadConsumerGroupCache()
		return ErrCgUnloaded
	}

	cgCache.cachedCGDesc.Status = cgDesc.Status
	cgCache.cachedCGDesc.MaxDeliveryCount = cgDesc.MaxDeliveryCount

	// contact the metadata to get the extent info
	cgReq := &metadata.ReadConsumerGroupExtentsRequest{
		DestinationUUID:   common.StringPtr(cgCache.cachedCGDesc.GetDestinationUUID()),
		ConsumerGroupUUID: common.StringPtr(cgCache.cachedCGDesc.GetConsumerGroupUUID()),
		OutputHostUUID:    common.StringPtr(cgCache.outputHostUUID),
		Status:            common.MetadataConsumerGroupExtentStatusPtr(metadata.ConsumerGroupExtentStatus_OPEN),
		MaxResults:        common.Int32Ptr(defaultMaxResults),
	}

	var nExtents = 0

	for {
		cgRes, errRCGES := cgCache.metaClient.ReadConsumerGroupExtents(ctx, cgReq)

		if cgRes == nil || errRCGES != nil {
			cgCache.logger.WithField(common.TagErr, errRCGES).Error("ReadConsumerGroupExtents failed on refresh")
			return errRCGES
		}

		// Now we have the cgCache. check and load extent for all extents
		for _, cge := range cgRes.GetExtents() {
			nExtents++
			errR := cgCache.loadExtentCache(cgCache.tClients, cgCache.metaClient, cgCache.wsConnector,
				cgCache.outputHostUUID, cgCache.cachedCGDesc.GetDestinationUUID(), dstDesc.GetType(),
				cgCache.cachedCGDesc.GetConsumerGroupUUID(), cge, cgCache.cachedCGDesc.GetStartFrom())
			if errR != nil {
				return errR
			}
		}

		if len(cgRes.GetNextPageToken()) == 0 {
			break
		} else {
			cgReq.PageToken = cgRes.GetNextPageToken()
		}
	}

	return nil
}

// loadConsumerGroupCache loads everything on this cache including the extents and within the cache
func (cgCache *consumerGroupCache) loadConsumerGroupCache(ctx thrift.Context, exists bool) error {
	cgCache.extMutex.Lock()
	defer cgCache.extMutex.Unlock()

	// Make sure we don't race with an unload
	if cgCache.isClosed() {
		return ErrCgUnloaded
	}

	// If we are loading this cache for the first time, make sure
	// we have the message delivery cache and also
	// spawn the management routines as well.
	if !exists {

		// Try to load the DLQ for this consumer group
		dlq, err := newDeadLetterQueue(ctx, cgCache.logger, cgCache.cachedCGDesc, cgCache.metaClient, cgCache.m3Client, cgCache.consumerM3Client)
		if err != nil {
			return err
		}

		cgCache.msgDeliveryCache = newMessageDeliveryCache(cgCache.msgsRedeliveryCh, cgCache.priorityMsgsRedeliveryCh, cgCache.msgCacheCh,
			cgCache.msgCacheRedeliveredCh, cgCache.ackMsgCh,
			cgCache.nackMsgCh, cgCache.cachedCGDesc, dlq, cgCache.logger, cgCache.m3Client, cgCache.consumerM3Client, cgCache.notifier, cgCache.creditNotifyCh, cgCache.creditRequestCh, defaultNumOutstandingMsgs, cgCache)
		cgCache.shutdownWG.Add(1)
		go cgCache.manageConsumerGroupCache() // Has cgCache.shutdownWG.Done()
		go cgCache.msgDeliveryCache.start()   // Uses the dlq, so must be after it
	}

	return cgCache.refreshCgCache(ctx)
}

func (cgCache *consumerGroupCache) reconfigureClients(updateUUID string) {

	var notified, dropped int

	// notify all connections
	cgCache.extMutex.RLock()
	for _, conn := range cgCache.connections {
		select {
		case conn.reconfigureClientCh <- updateUUID:
			notified++
		default:
			dropped++
		}
	}
	cgCache.extMutex.RUnlock()

	cgCache.logger.WithFields(bark.Fields{
		common.TagUpdateUUID: updateUUID,
		`notified`:           notified,
		`dropped`:            dropped,
	}).Info(`reconfigureClients: notified clients`)
}

func (cgCache *consumerGroupCache) isClosed() bool {
	select {
	case <-cgCache.closeChannel:
		return true
	default:
		return false
	}
}

// Report is used for reporting ConsumerGroup specific load to controller
func (cgCache *consumerGroupCache) Report(reporter common.LoadReporter) {

	msgsOut := cgCache.cgMetrics.GetAndReset(load.CGMetricMsgsOut)
	bytesOut := cgCache.cgMetrics.GetAndReset(load.CGMetricBytesOut)
	numConns := cgCache.cgMetrics.Get(load.CGMetricNumOpenConns)
	numExtents := cgCache.cgMetrics.Get(load.CGMetricNumOpenExtents)
	smartRetryOn := cgCache.cgMetrics.Get(load.CGMetricSmartRetryOn)

	cgMetrics := controller.ConsumerGroupMetrics{
		NumberOfActiveExtents:   common.Int64Ptr(numExtents),
		NumberOfConnections:     common.Int64Ptr(numConns),
		OutgoingMessagesCounter: common.Int64Ptr(msgsOut),
		OutgoingBytesCounter:    common.Int64Ptr(bytesOut),
		SmartRetryOnCounter:     common.Int64Ptr(smartRetryOn),
	}

	dstID := cgCache.cachedCGDesc.GetDestinationUUID()
	cgID := cgCache.cachedCGDesc.GetConsumerGroupUUID()

	reporter.ReportConsumerGroupMetric(dstID, cgID, cgMetrics)
	// report the number of connections gauge metric as well
	cgCache.consumerM3Client.UpdateGauge(metrics.ConsConnectionScope, metrics.OutputhostCGConsConnection, numConns)
	// report the number of extents as well
	cgCache.consumerM3Client.UpdateGauge(metrics.ConsConnectionScope, metrics.OutputhostCGNumExtents, numExtents)
}

// getNumConnections returns the number of connections on this CG
func (cgCache *consumerGroupCache) getNumOpenConns() int32 {
	return int32(cgCache.cgMetrics.Get(load.CGMetricNumOpenConns))
}

// incNumConnections increments the no: of connections on this CG by the given delta
func (cgCache *consumerGroupCache) incNumOpenConns(delta int32) int32 {
	return int32(cgCache.cgMetrics.Add(load.CGMetricNumOpenConns, int64(delta)))
}

// utilSendCreditsToExtent sends the credits to the respective extent.
// return true on success and false on failure
func (cgCache *consumerGroupCache) utilSendCreditsToExtent(extUUID string, credits int32) bool {
	cgCache.extMutex.RLock()
	defer cgCache.extMutex.RUnlock()

	ret := false
	if extCache, ok := cgCache.extentCache[extUUID]; ok {
		ret = extCache.grantCredits(credits)
	}

	return ret
}

func (cgCache *consumerGroupCache) updateLastDisconnectTime() {
	cgCache.extMutex.Lock()
	defer cgCache.extMutex.Unlock()
	cgCache.lastDisconnectTime = time.Now()
}

// unloadCGCache unloads everything on this cache
// 1. close all client connections
// 2. close all extent connections
func (cgCache *consumerGroupCache) unloadConsumerGroupCache() {
	cgCache.extMutex.Lock()

	if cgCache.isClosed() {
		cgCache.extMutex.Unlock()
		return
	}

	close(cgCache.closeChannel)
	for _, conn := range cgCache.connections {
		go conn.close()
	}
	// close all replica streams for each extent
	for _, extCache := range cgCache.extentCache {
		go extCache.unload()
	}

	cgCache.extMutex.Unlock()

	// notify the outputhost to remove this from the map
	// We do this in a blocking way to make sure the cgCache is deleted from the
	// map so that new connections can be opened on a new CG cache instance.
	// Even during shutdown there is a guarantee that we will unload everything first
	// before stopping the manageCgCache routine.
	cgCache.notifyUnloadCh <- cgCache.cachedCGDesc.GetConsumerGroupUUID()

	cgCache.loadReporter.Stop()

	// since the load reporter is stopped above make sure to mark the number of connections
	// as 0 explicitly, since we close the connections in an asynchronous fashion
	cgCache.consumerM3Client.UpdateGauge(metrics.ConsConnectionScope, metrics.OutputhostCGConsConnection, 0)
	cgCache.consumerM3Client.UpdateGauge(metrics.ConsConnectionScope, metrics.OutputhostCGNumExtents, 0)
}

// this can be used to set the max outstanding messages for testing
// XXX: this is not thread safe
func setDefaultMaxOutstandingMessages(limit int32) int32 {
	oldLimit := defaultNumOutstandingMsgs
	defaultNumOutstandingMsgs = limit
	return oldLimit
}
