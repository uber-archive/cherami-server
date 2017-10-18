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
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/dconfig"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/outputhost/load"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

const (
	kafkaConnectedStoreUUID = `cafca000-0000-0caf-ca00-0000000cafca` // Placeholder connected store for logs
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

		// dlqMerging indicates whether any DLQ extent is presently being merged. ATOMIC OPERATIONS ONLY
		dlqMerging int32

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

		// unloadInProgress is used to indicate if an unload of this CG is in progress to prevent racing loads from happening
		unloadInProgress bool

		// connsWG is used to wait for all the connections (including ext) to go away before stopping the manage routine.
		connsWG sync.WaitGroup

		// manageMsgCacheWG is used to wait for the manage delivery cache routine to go away. This is needed to make sure we cleanup all the buffered channels appropriately.
		manageMsgCacheWG sync.WaitGroup

		// cfgMgr is the reference to the cassandra backed cfgMgr
		cfgMgr dconfig.ConfigManager

		// kafkaCluster is the Kafka cluster for this consumer group, if applicable
		kafkaCluster string

		// kafkaTopics is the list of kafka topics consumed by this consumer group, if applicable
		kafkaTopics []string

		// kafkaMessageConverterFactory is a factory for kafka message converter
		kafkaMessageConverterFactory KafkaMessageConverterFactory
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
const defaultNumOutstandingMsgs int32 = 10000

// ErrCgUnloaded is returned when the cgCache is already unloaded
var ErrCgUnloaded = &cherami.InternalServiceError{Message: "ConsumerGroup already unloaded"}

// ErrConfigCast is returned when we are unable to cast to the CgConfig type
var ErrConfigCast = &cherami.InternalServiceError{Message: "Unable to cast to OutputCgConfig"}

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
		cfgMgr:                   h.cfgMgr,
		kafkaMessageConverterFactory: h.kafkaStreamFactory,
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
func (cgCache *consumerGroupCache) loadExtentCache(ctx thrift.Context, destType shared.DestinationType, cge *shared.ConsumerGroupExtent) {
	var committer Committer
	extUUID := cge.GetExtentUUID()
	if extCache, exists := cgCache.extentCache[extUUID]; !exists {
		extCache = &extentCache{
			cgUUID:                       cgCache.cachedCGDesc.GetConsumerGroupUUID(),
			extUUID:                      extUUID,
			destUUID:                     cgCache.cachedCGDesc.GetDestinationUUID(),
			destType:                     destType,
			storeUUIDs:                   cge.StoreUUIDs,
			startFrom:                    time.Unix(0, cgCache.cachedCGDesc.GetStartFrom()),
			skipOlder:                    time.Duration(int64(cgCache.cachedCGDesc.GetSkipOlderMessagesSeconds()) * int64(time.Second)),
			delay:                        time.Duration(int64(cgCache.cachedCGDesc.GetDelaySeconds()) * int64(time.Second)),
			notifyReplicaCloseCh:         make(chan error, 5),
			closeChannel:                 make(chan struct{}),
			waitConsumedCh:               make(chan bool, 1),
			msgsCh:                       cgCache.msgsCh,
			connectionsClosedCh:          cgCache.notifyReplicaCloseCh,
			shutdownWG:                   &cgCache.connsWG,
			tClients:                     cgCache.tClients,
			wsConnector:                  cgCache.wsConnector,
			logger:                       cgCache.logger.WithFields(bark.Fields{common.TagExt: extUUID, common.TagModule: `extCache`}),
			creditNotifyCh:               cgCache.creditNotifyCh,
			creditRequestCh:              cgCache.creditRequestCh,
			initialCredits:               defaultNumOutstandingMsgs,
			loadMetrics:                  load.NewExtentMetrics(),
			consumerM3Client:             cgCache.consumerM3Client,
			kafkaMessageConverterFactory: cgCache.kafkaMessageConverterFactory,
		}

		cgCache.extentCache[extUUID] = extCache
		cgCache.cgMetrics.Increment(load.CGMetricNumOpenExtents)
		cgCache.hostMetrics.Increment(load.HostMetricNumOpenExtents)

		// DEVNOTE: The term 'merging' here is a reference to the DLQ Merge feature/operation. These extents are
		// 'served' just like any other, but they are visible to this one consumer group, hence 'SingleCG'
		extCache.singleCGVisible = cgCache.checkSingleCGVisible(ctx, cge)
		if extCache.singleCGVisible {
			nDLQMergingExtents := atomic.AddInt32(&cgCache.dlqMerging, 1)
			extCache.logger.WithField(`nDLQMergingExtents`, nDLQMergingExtents).Info("Merging DLQ Extent(s)")
		}

		// get the initial credits based on the message cache size
		cfg, err := cgCache.getDynamicCgConfig()
		if err == nil {
			extCache.initialCredits = cgCache.getMessageCacheSize(cfg, defaultNumOutstandingMsgs)
		}

		if common.IsKafkaConsumerGroupExtent(cge) {
			committer = NewKafkaCommitter(
				cgCache.outputHostUUID,
				cgCache.cachedCGDesc.GetConsumerGroupUUID(),
				extCache.logger,
				&extCache.kafkaClient,
			)
		} else {
			committer = newCheramiCommitter(
				cgCache.metaClient,
				cgCache.outputHostUUID,
				cgCache.cachedCGDesc.GetConsumerGroupUUID(),
				extCache.extUUID,
				&extCache.connectedStoreUUID,
				cgCache.cachedCGDesc.GetIsMultiZone(),
				cgCache.tClients,
			)
		}

		extCache.ackMgr = newAckManager(
			cgCache,
			cgCache.ackIDGen.GetNextAckID(),
			cgCache.outputHostUUID,
			cgCache.cachedCGDesc.GetConsumerGroupUUID(),
			extCache.extUUID,
			&extCache.connectedStoreUUID,
			extCache.waitConsumedCh,
			cge,
			committer,
			extCache.logger,
		)
		extCache.loadReporter = cgCache.loadReporterFactory.CreateReporter(extentLoadReportingInterval, extCache, extCache.logger)

		// make sure we prevent shutdown from racing
		extCache.shutdownWG.Add(1)

		// if load fails we will unload it the usual way
		go extCache.load(
			cgCache.outputHostUUID,
			cgCache.cachedCGDesc.GetConsumerGroupUUID(),
			cgCache.cachedCGDesc.GetConsumerGroupName(),
			cgCache.kafkaCluster,
			cgCache.kafkaTopics,
			cgCache.metaClient,
			cge,
			cgCache.consumerM3Client,
		)

		// now notify the outputhost
		cgCache.ackMgrLoadCh <- ackMgrLoadMsg{uint32(extCache.ackMgr.ackMgrID), extCache.ackMgr}
	}
	return
}

// unloadExtentCache unloads an extent from the extentCache
func (cgCache *consumerGroupCache) unloadExtentCache(extUUID string) (ackMgrUnloadID uint32) {
	if extCache, ok := cgCache.extentCache[extUUID]; ok {
		extCache.logger.Info("removing extent")
		ackMgrUnloadID = uint32(extCache.ackMgr.ackMgrID)
		cgCache.cgMetrics.Decrement(load.CGMetricNumOpenExtents)
		cgCache.hostMetrics.Decrement(load.HostMetricNumOpenExtents)
		if extCache.singleCGVisible {
			dlqMerging := atomic.AddInt32(&cgCache.dlqMerging, -1)
			if dlqMerging <= 0 { // Note: this should never be less than 0; writing it like this for diagnosis
				cgCache.logger.Info("done merging DLQ extent(s)")
			}
		}
		delete(cgCache.extentCache, extUUID)
	} else {
		cgCache.logger.WithField(common.TagExt, extUUID).Warn("tried to unload not-loaded extent")
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
			ackMgrUnloadID = cgCache.unloadExtentCache(extUUID)
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
			// stop the delivery cache as well
			cgCache.msgDeliveryCache.stop()
			// wait for the manage routine to go away
			cgCache.manageMsgCacheWG.Wait()
			// at this point, the cg is completely unloaded, close all message channels
			// to cleanup
			cgCache.cleanupChannels()
			cgCache.logger.Info("cg is stopped and all channels are cleanedup")
			quit = true
		}
	}
}

// refreshCgCacheNoLock is a routine which is called without the extMutex held.
// It takes the mutex and in turn refreshes the cg by contacting metadata to
// get all the open extents and then loads them if needed
func (cgCache *consumerGroupCache) refreshCgCacheNoLock(ctx thrift.Context) error {
	cgCache.extMutex.Lock()
	defer cgCache.extMutex.Unlock()

	// Make sure we don't race with an unload
	if cgCache.isClosed() || cgCache.unloadInProgress {
		return ErrCgUnloaded
	}

	return cgCache.refreshCgCache(ctx)
}

// refreshCgCache contacts metadata to get all the open extents and then
// loads them if needed
// Note that this function is and must be run under the cgCache.extMutex lock
func (cgCache *consumerGroupCache) refreshCgCache(ctx thrift.Context) error {

	// First check any status changes to the destination or consumer group
	// If one of them is being deleted, then just unload all extents and
	// move on. If not, check for new extents for this consumer group

	dstReq := &shared.ReadDestinationRequest{DestinationUUID: common.StringPtr(cgCache.cachedCGDesc.GetDestinationUUID())}
	dstDesc, errRD := cgCache.metaClient.ReadDestination(ctx, dstReq)
	if errRD != nil || dstDesc == nil {
		cgCache.logger.WithField(common.TagErr, errRD).Error(`ReadDestination failed on refresh`)
		return errRD
	}

	if dstDesc.GetStatus() == shared.DestinationStatus_DELETING ||
		dstDesc.GetStatus() == shared.DestinationStatus_DELETED {
		cgCache.logger.Info("destination deleted; unloading all extents")
		go cgCache.unloadConsumerGroupCache()
		return ErrCgUnloaded
	}

	if dstDesc.GetType() == shared.DestinationType_KAFKA {
		cgCache.kafkaCluster = dstDesc.GetKafkaCluster()
		cgCache.kafkaTopics = dstDesc.GetKafkaTopics()
	}

	readReq := &shared.ReadConsumerGroupRequest{
		DestinationUUID:   common.StringPtr(cgCache.cachedCGDesc.GetDestinationUUID()),
		ConsumerGroupName: common.StringPtr(cgCache.cachedCGDesc.GetConsumerGroupName()),
	}

	cgDesc, errRCG := cgCache.metaClient.ReadConsumerGroup(ctx, readReq)
	if errRCG != nil {
		cgCache.logger.WithField(common.TagErr, errRCG).Error(`ReadConsumerGroup failed on refresh`)
		return errRCG
	}

	if cgDesc.GetStatus() == shared.ConsumerGroupStatus_DELETING ||
		cgDesc.GetStatus() == shared.ConsumerGroupStatus_DELETED {
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
	cgReq := &shared.ReadConsumerGroupExtentsRequest{
		DestinationUUID:   common.StringPtr(cgCache.cachedCGDesc.GetDestinationUUID()),
		ConsumerGroupUUID: common.StringPtr(cgCache.cachedCGDesc.GetConsumerGroupUUID()),
		OutputHostUUID:    common.StringPtr(cgCache.outputHostUUID),
		Status:            common.MetadataConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_OPEN),
		MaxResults:        common.Int32Ptr(defaultMaxResults),
	}

	for {
		cgRes, errRCGES := cgCache.metaClient.ReadConsumerGroupExtents(ctx, cgReq)

		if cgRes == nil || errRCGES != nil {
			cgCache.logger.WithField(common.TagErr, errRCGES).Error("ReadConsumerGroupExtents failed on refresh")
			return errRCGES
		}

		// Now we have the cgCache. check and load extent for all extents
		for _, cge := range cgRes.GetExtents() {
			cgCache.loadExtentCache(ctx, dstDesc.GetType(), cge)
		}

		if len(cgRes.GetNextPageToken()) == 0 {
			break
		} else {
			cgReq.PageToken = cgRes.GetNextPageToken()
		}
	}
	return nil
}

// checkSingleCGVisible determines if an extent under a certain destination is visible only to one consumer group. It is a cached call, so the
// metadata client will only be invoked once per destination+extent
func (cgCache *consumerGroupCache) checkSingleCGVisible(ctx thrift.Context, cge *shared.ConsumerGroupExtent) (singleCgVisible bool) {
	// Note that this same extent can be loaded by either a consumer group of the DLQ destination (i.e. for inspection,
	// with consumer group visibility = nil), or as an extent being merged into the original consumer group (i.e. for DLQ
	// merge, with consumer group visibility = this CG).
	req := &metadata.ReadExtentStatsRequest{
		DestinationUUID: common.StringPtr(cgCache.cachedCGDesc.GetDestinationUUID()),
		ExtentUUID:      common.StringPtr(cge.GetExtentUUID()),
	}
	ext, err := cgCache.metaClient.ReadExtentStats(ctx, req)
	if err == nil {
		if ext != nil && ext.GetExtentStats() != nil {
			singleCgVisible = ext.GetExtentStats().GetConsumerGroupVisibility() != ``
			if singleCgVisible {
				cgCache.logger.WithFields(bark.Fields{common.TagExt: common.FmtExt(cge.GetExtentUUID())}).Info("Consuming single CG visible extent")
			}
		}
	} else {
		// DEVNOTE: Unhandled error case; we would ideally retry; we may consider returning true for this case,
		// but DLQ merge is quite rare compared with the normal Smart Retry scenarios
		cgCache.logger.WithFields(bark.Fields{common.TagErr: err, common.TagExt: common.FmtExt(cge.GetExtentUUID())}).Error("ReadExtentStats failed during extent load")
	}

	return
}

// getDynamicCgConfig gets the configuration object for this host
func (cgCache *consumerGroupCache) getDynamicCgConfig() (OutputCgConfig, error) {
	dCfgIface, err := cgCache.cfgMgr.Get(common.OutputServiceName, `*`, `*`, `*`)
	if err != nil {
		cgCache.logger.WithFields(bark.Fields{common.TagErr: err}).Error(`Couldn't get the configuration object`)
		return OutputCgConfig{}, err
	}
	cfg, ok := dCfgIface.(OutputCgConfig)
	if !ok {
		cgCache.logger.Error(`Couldn't cast cfg to OutputCgConfig`)
		return OutputCgConfig{}, ErrConfigCast
	}
	return cfg, nil
}

// getMessageCacheSize gets the configured value for the message cache for this CG
func (cgCache *consumerGroupCache) getMessageCacheSize(cfg OutputCgConfig, oldSize int32) (cacheSize int32) {
	logFn := func() bark.Logger {
		return cgCache.logger
	}
	ruleKey := cgCache.destPath + `/` + cgCache.cachedCGDesc.GetConsumerGroupName()
	cacheSize = int32(common.OverrideValueByPrefix(logFn, ruleKey, cfg.MessageCacheSize, int64(oldSize), `messagecachesize`))

	return cacheSize
}

// loadConsumerGroupCache loads everything on this cache including the extents and within the cache
func (cgCache *consumerGroupCache) loadConsumerGroupCache(ctx thrift.Context, exists bool) error {
	cgCache.extMutex.Lock()
	defer cgCache.extMutex.Unlock()

	// Make sure we don't race with an unload
	if cgCache.isClosed() || cgCache.unloadInProgress {
		return ErrCgUnloaded
	}

	// If we are loading this cache for the first time, make sure
	// we have the message delivery cache and also
	// spawn the management routines as well.
	if !exists {

		// Try to load the DLQ for this consumer group
		dlq, err := newDeadLetterQueue(
			ctx,
			cgCache.logger,
			cgCache.cachedCGDesc,
			cgCache.metaClient,
			cgCache.m3Client,
			cgCache.consumerM3Client,
		)
		if err != nil {
			return err
		}

		cgCache.msgDeliveryCache = newMessageDeliveryCache(dlq, defaultNumOutstandingMsgs, cgCache)
		cgCache.shutdownWG.Add(1)
		go cgCache.manageConsumerGroupCache() // Has cgCache.shutdownWG.Done()
		cgCache.manageMsgCacheWG.Add(1)       // wait group for msgCache
		go cgCache.msgDeliveryCache.start()   // Uses the dlq, so must be after it

		// trigger a refresh the first time. If we already have the CG loaded,
		// then the refresh loop will load and refresh extents as part of the
		// event loop.
		return cgCache.refreshCgCache(ctx)
	}

	return nil
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

	if cgCache.isClosed() || cgCache.unloadInProgress {
		cgCache.extMutex.Unlock()
		return
	}

	cgCache.unloadInProgress = true
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

	// wait for all the above connections to go away
	cgCache.connsWG.Wait()

	// now close the closeChannel which will stop the manage routine
	close(cgCache.closeChannel)

	cgCache.loadReporter.Stop()

	// since the load reporter is stopped above make sure to mark the number of connections
	// as 0 explicitly, since we close the connections in an asynchronous fashion
	cgCache.consumerM3Client.UpdateGauge(metrics.ConsConnectionScope, metrics.OutputhostCGConsConnection, 0)
	cgCache.consumerM3Client.UpdateGauge(metrics.ConsConnectionScope, metrics.OutputhostCGNumExtents, 0)
}

func (cgCache *consumerGroupCache) getCgState(cgUUID string) *admin.ConsumerGroupState {
	cgCache.extMutex.RLock()
	defer cgCache.extMutex.RUnlock()
	cgState := admin.NewConsumerGroupState()
	cgState.CgUUID = common.StringPtr(cgUUID)
	cgState.NumOutstandingMsgs = common.Int32Ptr(cgCache.msgDeliveryCache.getOutstandingMsgs())
	cgState.MsgChSize = common.Int64Ptr(int64(len(cgCache.msgsCh)))
	cgState.NumConnections = common.Int64Ptr(int64(cgCache.getNumOpenConns()))

	cgState.CgExtents = make([]*admin.OutputCgExtent, 0)
	// get all extent state now
	for _, extCache := range cgCache.extentCache {
		extState := extCache.getState()
		cgState.CgExtents = append(cgState.CgExtents, extState)
	}

	return cgState
}

// this is a utility routine which closes all message channels
// to make sure we free up memory
// this can be done outside of a mutex since this will happen only after we
// closed everything.
func (cgCache *consumerGroupCache) cleanupChannels() {
	// close msgsCh
	close(cgCache.msgsCh)
	// close cacheCh
	close(cgCache.msgCacheCh)
	// redelivery channel
	close(cgCache.msgsRedeliveryCh)
	// priority redelivery channel
	close(cgCache.priorityMsgsRedeliveryCh)
	// redeliverd channel
	close(cgCache.msgCacheRedeliveredCh)
	// ackMsgsCh
	close(cgCache.ackMsgCh)
	// nackMsgsCh
	close(cgCache.nackMsgCh)
}
