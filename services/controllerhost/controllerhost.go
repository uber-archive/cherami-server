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

// Package controllerhost is a Cherami Controller implemented as a TChannel
// Thrift Service. MCP is responsible for the following high level load
// balancing functions in Cherami :
//
//    * Creation and Deletion of Extents
//    * Assignment of In/Store hosts to Extents
//    * Assignment of Producers to InputHosts
//    * Assignment of Consumers to OutputHosts
//    * Cluster re-balancing when hosts join / leave the cluster
//
package controllerhost

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	"github.com/uber/cherami-server/common/dconfig"
	"github.com/uber/cherami-server/common/metadata"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/controllerhost/load"
	a "github.com/uber/cherami-thrift/.generated/go/admin"
	c "github.com/uber/cherami-thrift/.generated/go/controller"
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	tchannel "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

var (
	// ErrMalformedUUID is returned when the UUID in the request is malformed
	ErrMalformedUUID = &shared.BadRequestError{Message: "Malformed UUID in request"}
	// ErrNoHealthyExtent is returned where there are no healthy extents in the system
	ErrNoHealthyExtent = &shared.InternalServiceError{Message: "No healthy extent found for destination"}
	// ErrEventQueueFull is returned when the internal event queue is full and
	// as a result, some action cannot be taken by the controller
	ErrEventQueueFull = &shared.InternalServiceError{Message: "EventQueue full"}
	// ErrTryLock is a temporary error that is thrown by the API
	// when it loses the race to do the computation needed to refreh the
	// API result cache. This should only happen during initial
	// bootstrap of Controller.
	ErrTryLock = &shared.InternalServiceError{Message: "Failed to acquire lock, backoff and retry"}
	// ErrUnavailable indicates any kind of intermittent
	// service error, most likely, metadata read/write
	// errors.
	ErrUnavailable = &shared.InternalServiceError{Message: "Service unavailable, backoff and retry"}

	// ErrDestinationNotExists is returned for a missing destination
	ErrDestinationNotExists = &shared.EntityNotExistsError{Message: "Destination does not exist"}
	// ErrConsumerGroupNotExists is returned for a missing consumer group
	ErrConsumerGroupNotExists = &shared.EntityNotExistsError{Message: "ConsumerGroup does not exist"}
	// ErrDestinationDisabled is returned after a destination is deleted
	ErrDestinationDisabled = &shared.EntityDisabledError{Message: "Destination is not enabled"}
	// ErrConsumerGroupDisabled is returned after a consumer group is deleted
	ErrConsumerGroupDisabled = &shared.EntityDisabledError{Message: "Consumer group is not enabled"}
)

const (
	nEventPipelineWorkers      = 2048 // most workers expected to be blocked on I/O
	hashLockTableSize          = 1024
	maxFailedExtentSealSetSize = 8192 // max # of failed extent seals we can keep track of
	maxExtentSealsPerSecond    = 200
)

type (
	// Mcp implements the ExtentController interface
	Mcp struct {
		*common.Service
		hostIDHeartbeater common.HostIDHeartbeater
		mClient           m.TChanMetadataService
		context           *Context
		started           int32
	}
	// Context holds the run-time context for controller
	Context struct {
		hostID              string
		localZone           string
		mm                  MetadataMgr
		rpm                 common.RingpopMonitor
		zoneFailoverManager common.ZoneFailoverManager
		failureDetector     Dfdd
		log                 bark.Logger
		dstLock             LockMgr
		eventPipeline       EventPipeline
		resultCache         *resultCache
		extentMonitor       *extentStateMonitor
		timeSource          common.TimeSource
		channel             *tchannel.Channel
		clientFactory       common.ClientFactory
		retMgr              *retMgrRunner
		appConfig           configure.CommonAppConfig
		m3Client            metrics.Client
		cfgMgr              dconfig.ConfigManager
		loadMetrics         load.MetricsAggregator
		placement           Placement
		extentSeals         struct {
			// set of extents for which seal is in progress
			// if an extent exist in this set, some worker
			// is guaranteed to be working on this extent
			inProgress common.ConcurrentMap
			// set of extents that have exceeded the max
			// retries for sealing. These purpose of this
			// set is to filter out these extents from the
			// GetInputHosts result. Sealing of these
			// extents will ultimately resume on the next
			// external trigger.
			failed common.ConcurrentMap
			// Rate limiter for throttling seals issued
			// per second from a single controller instance
			tokenBucket common.TokenBucket
		}
	}

	// Boolean is an alias for bool
	// that can be used as a generic
	// value type i.e. interface{}
	Boolean bool
)

// interface implementation check
var _ c.TChanController = (*Mcp)(nil)

// NewController creates and returns a new instance of Mcp controller
func NewController(cfg configure.CommonAppConfig, sVice *common.Service, metadataClient m.TChanMetadataService, zoneFailoverManager common.ZoneFailoverManager) (*Mcp, []thrift.TChanServer) {
	hostID := uuid.New()

	instance := new(Mcp)
	instance.Service = sVice

	// Get the deployment name for logger field
	deploymentName := sVice.GetConfig().GetDeploymentName()
	logger := (sVice.GetConfig().GetLogger()).WithFields(bark.Fields{common.TagCtrl: common.FmtCtrl(hostID), common.TagDplName: common.FmtDplName(deploymentName)})

	lockMgr, err := NewLockMgr(hashLockTableSize, common.UUIDHashCode, logger)
	if err != nil {
		logger.WithField(common.TagErr, err).Fatal(`Failed to create hash lock`)
	}

	context := &Context{
		appConfig:  cfg,
		timeSource: common.NewRealTimeSource(),
		log:        logger,
	}

	context.localZone, _ = common.GetLocalClusterInfo(strings.ToLower(deploymentName))
	context.zoneFailoverManager = zoneFailoverManager

	context.dstLock = lockMgr
	context.m3Client = metrics.NewClient(instance.Service.GetMetricsReporter(), metrics.Controller)
	instance.mClient = metadata.NewMetadataMetricsMgr(metadataClient, context.m3Client, context.log)
	context.mm = NewMetadataMgr(instance.mClient, context.m3Client, context.log)
	context.extentSeals.inProgress = common.NewShardedConcurrentMap(1024, common.UUIDHashCode)
	context.extentSeals.failed = common.NewShardedConcurrentMap(1024, common.UUIDHashCode)
	context.extentSeals.tokenBucket = common.NewTokenBucket(maxExtentSealsPerSecond, common.NewRealTimeSource())

	context.resultCache = newResultCache(context)
	context.cfgMgr = newConfigManager(metadataClient, context.log)
	context.loadMetrics = load.NewTimeSlotAggregator(common.NewRealTimeSource(), context.log)

	if context.placement, err = NewDistancePlacement(context); err != nil {
		context.log.WithField(common.TagErr, err).Error("Cannot initialize topology for placement")
	}
	instance.context = context

	return instance, []thrift.TChanServer{c.NewTChanControllerServer(instance),
		a.NewTChanControllerHostAdminServer(instance)}
}

// Start starts the controller service
func (mcp *Mcp) Start(thriftService []thrift.TChanServer) {

	mcp.Service.Start(thriftService)

	context := mcp.context

	context.hostID = mcp.GetHostUUID()
	context.clientFactory = mcp.GetClientFactory()
	context.channel = mcp.GetTChannel()
	context.rpm = mcp.GetRingpopMonitor()

	context.zoneFailoverManager.Start()

	context.eventPipeline = NewEventPipeline(context, nEventPipelineWorkers)
	context.eventPipeline.Start()

	context.loadMetrics.Start()
	context.cfgMgr.Start()

	context.failureDetector = NewDfdd(context, common.NewRealTimeSource())
	context.failureDetector.Start()

	context.retMgr = newRetMgrRunner(&retMgrRunnerContext{
		hostID:         mcp.GetHostUUID(),
		ringpop:        context.rpm,
		metadataClient: mcp.mClient,
		clientFactory:  context.clientFactory,
		log:            context.log,
		m3Client:       context.m3Client,
		localZone:      context.localZone,
	})
	context.retMgr.Start()

	mcp.hostIDHeartbeater = common.NewHostIDHeartbeater(mcp.mClient, mcp.GetHostUUID(), mcp.GetHostPort(), mcp.GetHostName(), mcp.context.log)
	mcp.hostIDHeartbeater.Start()

	context.extentMonitor = newExtentStateMonitor(context)
	context.extentMonitor.Start()

	atomic.StoreInt32(&mcp.started, 1)
}

// Stop stops the controller service
func (mcp *Mcp) Stop() {
	mcp.hostIDHeartbeater.Stop()
	mcp.context.zoneFailoverManager.Stop()
	mcp.context.extentMonitor.Stop()
	mcp.context.retMgr.Stop()
	mcp.context.failureDetector.Stop()
	mcp.context.eventPipeline.Stop()
	mcp.context.loadMetrics.Stop()
	mcp.context.cfgMgr.Stop()
	mcp.Service.Stop()
}

func (mcp *Mcp) isStarted() bool {
	return (atomic.LoadInt32(&mcp.started) == 1)
}

// GetInputHosts finds and returns the in hosts serving the given destination
// The algorithm is as follows:
//
// 1. Lookup result cache for precomputed result
//     If
//     	a. Cache entry is expired or
//     	b. Any of the cached hosts has failed or
//	   	c. nHealthy extents is below threshold then
//     Then
//		 recompute the result and update cache
//	   Else
//		 return result from cache
//
// Recompute Logic:
// 1. List all OPEN extents for this destination
// 2. If the number of open but healthy extents < threshold, try creating a new extent
// 3. Return all inputhosts serving the healthy open extents
// 4. Record result in the result cache
func (mcp *Mcp) GetInputHosts(ctx thrift.Context, inReq *c.GetInputHostsRequest) (*c.GetInputHostsResult_, error) {

	if !mcp.isStarted() {
		// this can happen because we listen on the tchannel
		// endpoint before the context gets completely built
		return nil, &shared.InternalServiceError{Message: "Controller not started"}
	}

	context := mcp.context
	context.m3Client.IncCounter(metrics.GetInputHostsScope, metrics.ControllerRequests)
	sw := context.m3Client.StartTimer(metrics.GetInputHostsScope, metrics.ControllerLatencyTimer)
	defer sw.Stop()

	var result *resultCacheReadResult
	var dstUUID = inReq.GetDestinationUUID()

	if !isUUIDLengthValid(dstUUID) {
		context.m3Client.IncCounter(metrics.GetInputHostsScope, metrics.ControllerErrBadRequestCounter)
		return nil, ErrMalformedUUID
	}

	response := func(err error) (*c.GetInputHostsResult_, error) {
		if len(result.cachedResult) < 1 {
			// only count as failure if our answer contains no endpoints at all
			context.m3Client.IncCounter(metrics.GetInputHostsScope, metrics.ControllerFailures)
			return nil, err
		}
		return &c.GetInputHostsResult_{InputHostIds: result.cachedResult}, nil
	}

	var now = context.timeSource.Now().UnixNano()

	result = context.resultCache.readInputHosts(dstUUID, now)
	if result.cacheHit && !result.refreshCache {
		// return result from cache, if there is
		// one else return serviceUnavailable
		return response(ErrUnavailable)
	}

	// Now that we need to refresh the result, lets just
	// have one thread of execution do the recomputation
	// while others continue to serve from the cache
	if !context.dstLock.TryLock(dstUUID, getLockTimeout(result)) {
		context.m3Client.IncCounter(metrics.GetInputHostsScope, metrics.ControllerErrTryLockCounter)
		return response(ErrTryLock)
	}

	// With the lock being held, make sure someone else did not already
	// refresh the cache in the mean time
	result = context.resultCache.readInputHosts(dstUUID, now)
	if result.cacheHit && !result.refreshCache {
		context.dstLock.Unlock(dstUUID)
		return response(ErrUnavailable)
	}

	hostIDs, err := refreshInputHostsForDst(context, dstUUID, now)
	context.dstLock.Unlock(dstUUID)
	if err != nil {

		switch {
		case isEntityError(err):
			context.m3Client.IncCounter(metrics.GetInputHostsScope, metrics.ControllerErrBadEntityCounter)
			return nil, err
		case isBadRequestError(err):
			context.m3Client.IncCounter(metrics.GetInputHostsScope, metrics.ControllerErrBadRequestCounter)
			return nil, err
		default:
			return response(&shared.InternalServiceError{Message: err.Error()})
		}
	}

	return &c.GetInputHostsResult_{InputHostIds: hostIDs}, nil
}

// GetOutputHosts finds and returns the out hosts serving the given destination
// Algorithm is as follows:
// 1. Lookup result cache for precomputed result
//     If
//     	a. Cache entry is expired or
//     	b. Any of the cached hosts has failed or
//	   	c. nHealthy extents is below threshold then
//     Then
//		 recompute the result and update cache
//	   Else
//		 return result from cache
//
// recompute logic:
// 1. List all the consumable extents for this consumer group (from the consumer_group_extents table)
// 2. Attempt to add new unadded open extents (from destination_extents) to consumer_group
// 3. If there unhealthy extents (due to out host failure), repair upto a threshold
// 4. Record result in resultsCache and return all consumable output hosts
func (mcp *Mcp) GetOutputHosts(ctx thrift.Context, inReq *c.GetOutputHostsRequest) (*c.GetOutputHostsResult_, error) {

	if !mcp.isStarted() {
		// this can happen because we listen on the tchannel
		// endpoint before the context gets completely built
		return nil, &shared.InternalServiceError{Message: "Controller not started"}
	}

	context := mcp.context
	context.m3Client.IncCounter(metrics.GetOutputHostsScope, metrics.ControllerRequests)
	sw := context.m3Client.StartTimer(metrics.GetOutputHostsScope, metrics.ControllerLatencyTimer)
	defer sw.Stop()

	var result *resultCacheReadResult
	var cgUUID = inReq.GetConsumerGroupUUID()
	var dstUUID = inReq.GetDestinationUUID()

	if !isUUIDLengthValid(dstUUID) || !isUUIDLengthValid(cgUUID) {
		context.m3Client.IncCounter(metrics.GetOutputHostsScope, metrics.ControllerErrBadRequestCounter)
		return nil, ErrMalformedUUID
	}

	response := func() (*c.GetOutputHostsResult_, error) {
		if len(result.cachedResult) < 1 && !result.consumeDisabled {
			// only count as failure if our answer contains no endpoints at all and consuming is not disabled
			context.m3Client.IncCounter(metrics.GetOutputHostsScope, metrics.ControllerFailures)
			return nil, ErrUnavailable
		}
		return &c.GetOutputHostsResult_{OutputHostIds: result.cachedResult}, nil
	}

	var now = context.timeSource.Now().UnixNano()

	result = context.resultCache.readOutputHosts(cgUUID, now)
	if result.cacheHit && !result.refreshCache {
		return response()
	}

	if !context.dstLock.TryLock(dstUUID, getLockTimeout(result)) {
		context.m3Client.IncCounter(metrics.GetOutputHostsScope, metrics.ControllerErrTryLockCounter)
		return response()
	}

	// With the lock being held, make sure someone else did not already
	// refresh the cache in the mean time
	result = context.resultCache.readOutputHosts(cgUUID, now)
	if result.cacheHit && !result.refreshCache {
		context.dstLock.Unlock(dstUUID)
		return response()
	}

	hostIDs, err := refreshOutputHostsForConsGroup(context, dstUUID, cgUUID, *result, now)
	context.dstLock.Unlock(dstUUID)
	if err != nil {
		if isEntityError(err) {
			context.m3Client.IncCounter(metrics.GetOutputHostsScope, metrics.ControllerErrBadEntityCounter)
			return nil, err
		}
		return response()
	}

	return &c.GetOutputHostsResult_{OutputHostIds: hostIDs}, nil
}

// GetQueueDepthInfo to return queue depth backlog infor for consumer group
func (mcp *Mcp) GetQueueDepthInfo(ctx thrift.Context, inReq *c.GetQueueDepthInfoRequest) (*c.GetQueueDepthInfoResult_, error) {
	if inReq == nil {
		return nil, &shared.BadRequestError{Message: "request is nil"}
	}
	cgUUID := inReq.GetKey()
	// valid the cgUUID
	if !isUUIDLengthValid(cgUUID) {
		mcp.context.log.WithFields(bark.Fields{common.TagCnsm: common.FmtCnsm(cgUUID)}).Error(`GetQueueDepthInfo: Malformed UUID ( cgUUID: )`)
		return nil, ErrMalformedUUID
	}

	queueInfo, err := mcp.context.extentMonitor.queueDepth.GetQueueDepthResult(cgUUID)
	if err == nil {
		output := &QueueDepthCacheJSONFields{
			CacheTime:        queueInfo.Time,
			BacklogAvailable: queueInfo.BacklogAvailable,
			BacklogInflight:  queueInfo.BacklogInflight,
			BacklogDLQ:       queueInfo.BacklogDLQ,
		}
		queueInfo, _ := json.Marshal(output)
		queueInfoStr := string(queueInfo)
		return &c.GetQueueDepthInfoResult_{Value: &queueInfoStr}, nil
	}
	return nil, err
}

// ExtentsUnreachable is a way for other services to notify the Controller about an unreachable extent.
// When this notification is received, controller will enqueue an ExtentDownEvent to seal the extent
// asynchronously. A successful return code from this API does not guarantee that the extent is sealed,
// it acknowledges that the notification was successfully processed.
func (mcp *Mcp) ExtentsUnreachable(ctx thrift.Context, extentsUnreachableRequest *a.ExtentsUnreachableRequest) error {

	if !mcp.isStarted() {
		// this can happen because we listen on the tchannel
		// endpoint before the context gets completely built
		return &shared.InternalServiceError{Message: "Controller not started"}
	}

	context := mcp.context
	context.m3Client.IncCounter(metrics.ExtentsUnreachableScope, metrics.ControllerRequests)
	sw := context.m3Client.StartTimer(metrics.ExtentsUnreachableScope, metrics.ControllerLatencyTimer)
	defer sw.Stop()

	dstIDs := make(map[string]struct{})

	for _, update := range extentsUnreachableRequest.GetUpdates() {
		dstID := update.GetDestinationUUID()
		extID := update.GetExtentUUID()
		// only trigger refresh and add event if
		// we aren't already in the process of
		// sealing this extent
		if !isExtentBeingSealed(context, extID) {
			dstIDs[dstID] = struct{}{}
			addExtentDownEvent(context, update.GetSealSequenceNumber(), dstID, extID)
		}
		context.log.WithFields(bark.Fields{
			common.TagDst: common.FmtDst(dstID),
			common.TagExt: common.FmtExt(string(extID)),
		}).Info("ExtentsUnreachable notificiation from inputhost")
	}

	// for each of the destinations thats about to lose
	// an extent (for write), compensate by triggering
	// creation of a new extent. This should automatically
	// happen when the clients disconnect and call GetInputHosts().
	// But because we cache the results of GetInputHosts(),
	// do a forceful cache refresh here.
	for id := range dstIDs {
		if !context.dstLock.TryLock(id, time.Second) {
			// we have waited for a second, that's
			// enough time for all the caches to expire
			// bail out and let the next GetInputHosts()
			// fix things
			break
		}
		// this should refresh / create extent / update cache
		refreshInputHostsForDst(context, id, context.timeSource.Now().UnixNano())
		context.dstLock.Unlock(id)
	}

	return nil
}

// ReportNodeMetric records the incoming set of metrics from a chearmi node
func (mcp *Mcp) ReportNodeMetric(ctx thrift.Context, request *c.ReportNodeMetricRequest) error {

	if !mcp.isStarted() {
		// this can happen because we listen on the tchannel
		// endpoint before the context gets completely built
		return &shared.InternalServiceError{Message: "Controller not started"}
	}

	context := mcp.context
	context.m3Client.IncCounter(metrics.ReportNodeMetricScope, metrics.ControllerRequests)
	sw := context.m3Client.StartTimer(metrics.ReportNodeMetricScope, metrics.ControllerLatencyTimer)
	defer sw.Stop()

	hostID := request.GetHostId()

	if len(hostID) < 1 {
		return &shared.BadRequestError{Message: "HostID cannot be empty"}
	}

	metrics := request.GetMetrics()
	timestamp := request.GetTimestamp()
	loadMetrics := mcp.context.loadMetrics

	if metrics.IsSetRemainingDiskSpace() {
		loadMetrics.Put(hostID, load.EmptyTag, load.RemDiskSpaceBytes, metrics.GetRemainingDiskSpace(), timestamp)
	}
	if metrics.IsSetNumberOfActiveExtents() {
		loadMetrics.Put(hostID, load.EmptyTag, load.NumExtentsActive, metrics.GetNumberOfActiveExtents(), timestamp)
	}
	if metrics.IsSetNumberOfConnections() {
		loadMetrics.Put(hostID, load.EmptyTag, load.NumConns, metrics.GetNumberOfConnections(), timestamp)
	}
	if metrics.IsSetIncomingMessagesCounter() {
		loadMetrics.Put(hostID, load.EmptyTag, load.MsgsInPerSec, metrics.GetIncomingMessagesCounter(), timestamp)
	}
	if metrics.IsSetIncomingBytesCounter() {
		loadMetrics.Put(hostID, load.EmptyTag, load.BytesInPerSec, metrics.GetIncomingBytesCounter(), timestamp)
	}
	if metrics.IsSetOutgoingMessagesCounter() {
		loadMetrics.Put(hostID, load.EmptyTag, load.MsgsOutPerSec, metrics.GetOutgoingMessagesCounter(), timestamp)
	}
	if metrics.IsSetOutgoingBytesCounter() {
		loadMetrics.Put(hostID, load.EmptyTag, load.BytesOutPerSec, metrics.GetOutgoingBytesCounter(), timestamp)
	}
	if metrics.IsSetNodeStatus() && request.IsSetRole() && metrics.GetNodeStatus() == c.NodeStatus_GOING_DOWN {
		switch request.GetRole() {
		case c.Role_IN:
			context.failureDetector.ReportHostGoingDown(common.InputServiceName, hostID)
		case c.Role_STORE:
			context.failureDetector.ReportHostGoingDown(common.StoreServiceName, hostID)
		}
	}

	return nil
}

// ReportDestinationMetric records the incoming set of extent metrics from inputhost
func (mcp *Mcp) ReportDestinationMetric(ctx thrift.Context, request *c.ReportDestinationMetricRequest) error {

	if !mcp.isStarted() {
		// this can happen because we listen on the tchannel
		// endpoint before the context gets completely built
		return &shared.InternalServiceError{Message: "Controller not started"}
	}

	context := mcp.context
	context.m3Client.IncCounter(metrics.ReportDestinationMetricScope, metrics.ControllerRequests)
	sw := context.m3Client.StartTimer(metrics.ReportDestinationMetricScope, metrics.ControllerLatencyTimer)
	defer sw.Stop()

	hostID := request.GetHostId()
	dstID := request.GetDestinationUUID()

	if len(hostID) < 1 || len(dstID) < 1 {
		return &shared.BadRequestError{Message: "HostID or DestinationUUID cannot be empty"}
	}

	metrics := request.GetMetrics()
	timestamp := request.GetTimestamp()
	loadMetrics := mcp.context.loadMetrics

	if metrics.IsSetNumberOfConnections() {
		loadMetrics.Put(hostID, dstID, load.NumConns, metrics.GetNumberOfConnections(), timestamp)
	}
	if metrics.IsSetNumberOfActiveExtents() {
		loadMetrics.Put(hostID, dstID, load.NumExtentsActive, metrics.GetNumberOfActiveExtents(), timestamp)
	}
	if metrics.IsSetIncomingMessagesCounter() {
		loadMetrics.Put(hostID, dstID, load.MsgsInPerSec, metrics.GetIncomingMessagesCounter(), timestamp)
	}
	if metrics.IsSetIncomingBytesCounter() {
		loadMetrics.Put(hostID, dstID, load.BytesInPerSec, metrics.GetIncomingBytesCounter(), timestamp)
	}

	return nil
}

// ReportDestinationExtentMetric records the incoming set of extent metrics from inputhost
func (mcp *Mcp) ReportDestinationExtentMetric(ctx thrift.Context, request *c.ReportDestinationExtentMetricRequest) error {

	if !mcp.isStarted() {
		// this can happen because we listen on the tchannel
		// endpoint before the context gets completely built
		return &shared.InternalServiceError{Message: "Controller not started"}
	}

	context := mcp.context
	context.m3Client.IncCounter(metrics.ReportDestinationExtentMetricScope, metrics.ControllerRequests)
	sw := context.m3Client.StartTimer(metrics.ReportDestinationExtentMetricScope, metrics.ControllerLatencyTimer)
	defer sw.Stop()

	hostID := request.GetHostId()
	extID := request.GetExtentUUID()

	if len(hostID) < 1 || len(extID) < 1 {
		return &shared.BadRequestError{Message: "HostID or ExtentUUID cannot be empty"}
	}

	metrics := request.GetMetrics()
	timestamp := request.GetTimestamp()
	loadMetrics := mcp.context.loadMetrics

	if metrics.IsSetIncomingMessagesCounter() {
		loadMetrics.Put(hostID, extID, load.MsgsInPerSec, metrics.GetIncomingMessagesCounter(), timestamp)
	}
	if metrics.IsSetIncomingBytesCounter() {
		loadMetrics.Put(hostID, extID, load.BytesInPerSec, metrics.GetIncomingBytesCounter(), timestamp)
	}

	return nil
}

// ReportConsumerGroupMetric records the incoming set of extent metrics from an out host
func (mcp *Mcp) ReportConsumerGroupMetric(ctx thrift.Context, request *c.ReportConsumerGroupMetricRequest) error {

	if !mcp.isStarted() {
		// this can happen because we listen on the tchannel
		// endpoint before the context gets completely built
		return &shared.InternalServiceError{Message: "Controller not started"}
	}

	context := mcp.context
	context.m3Client.IncCounter(metrics.ReportConsumerGroupMetricScope, metrics.ControllerRequests)
	sw := context.m3Client.StartTimer(metrics.ReportConsumerGroupMetricScope, metrics.ControllerLatencyTimer)
	defer sw.Stop()

	hostID := request.GetHostId()
	cgID := request.GetConsumerGroupUUID()

	if len(hostID) < 1 || len(cgID) < 1 {
		return &shared.BadRequestError{Message: "HostID or ConsumerGroupUUID cannot be empty"}
	}

	metrics := request.GetMetrics()
	timestamp := request.GetTimestamp()
	loadMetrics := mcp.context.loadMetrics

	if metrics.IsSetNumberOfConnections() {
		loadMetrics.Put(hostID, cgID, load.NumConns, metrics.GetNumberOfConnections(), timestamp)
	}
	if metrics.IsSetNumberOfActiveExtents() {
		loadMetrics.Put(hostID, cgID, load.NumExtentsActive, metrics.GetNumberOfActiveExtents(), timestamp)
	}
	if metrics.IsSetOutgoingMessagesCounter() {
		loadMetrics.Put(hostID, cgID, load.MsgsOutPerSec, metrics.GetOutgoingMessagesCounter(), timestamp)
	}
	if metrics.IsSetOutgoingBytesCounter() {
		loadMetrics.Put(hostID, cgID, load.BytesOutPerSec, metrics.GetOutgoingBytesCounter(), timestamp)
	}
	if metrics.IsSetSmartRetryOnCounter() {
		loadMetrics.Put(hostID, cgID, load.SmartRetryOn, metrics.GetSmartRetryOnCounter(), timestamp)
	}

	return nil
}

// ReportConsumerGroupExtentMetric records the incoming set of extent metrics from an out host
func (mcp *Mcp) ReportConsumerGroupExtentMetric(ctx thrift.Context, request *c.ReportConsumerGroupExtentMetricRequest) error {

	if !mcp.isStarted() {
		// this can happen because we listen on the tchannel
		// endpoint before the context gets completely built
		return &shared.InternalServiceError{Message: "Controller not started"}
	}

	context := mcp.context
	context.m3Client.IncCounter(metrics.ReportConsumerGroupExtentMetricScope, metrics.ControllerRequests)
	sw := context.m3Client.StartTimer(metrics.ReportConsumerGroupExtentMetricScope, metrics.ControllerLatencyTimer)
	defer sw.Stop()

	hostID := request.GetHostId()
	extID := request.GetExtentUUID()

	if len(hostID) < 1 || len(extID) < 1 {
		return &shared.BadRequestError{Message: "HostID or ExtentUUID cannot be empty"}
	}

	metrics := request.GetMetrics()
	timestamp := request.GetTimestamp()
	loadMetrics := mcp.context.loadMetrics

	if metrics.IsSetOutgoingMessagesCounter() {
		loadMetrics.Put(hostID, extID, load.MsgsOutPerSec, metrics.GetOutgoingMessagesCounter(), timestamp)
	}
	if metrics.IsSetOutgoingBytesCounter() {
		loadMetrics.Put(hostID, extID, load.BytesOutPerSec, metrics.GetOutgoingBytesCounter(), timestamp)
	}

	return nil
}

// ReportStoreExtentMetric records the incoking set of extent metrics from a store host
func (mcp *Mcp) ReportStoreExtentMetric(ctx thrift.Context, request *c.ReportStoreExtentMetricRequest) error {

	if !mcp.isStarted() {
		// this can happen because we listen on the tchannel
		// endpoint before the context gets completely built
		return &shared.InternalServiceError{Message: "Controller not started"}
	}

	context := mcp.context
	context.m3Client.IncCounter(metrics.ReportStoreExtentMetricScope, metrics.ControllerRequests)
	sw := context.m3Client.StartTimer(metrics.ReportStoreExtentMetricScope, metrics.ControllerLatencyTimer)
	defer sw.Stop()

	hostID := request.GetStoreId()
	extID := request.GetExtentUUID()

	if len(hostID) < 1 || len(extID) < 1 {
		return &shared.BadRequestError{Message: "HostID or ExtentUUID cannot be empty"}
	}

	metrics := request.GetMetrics()
	timestamp := request.GetTimestamp()
	loadMetrics := mcp.context.loadMetrics

	if metrics.IsSetNumberOfConnections() {
		loadMetrics.Put(hostID, extID, load.NumConns, metrics.GetNumberOfConnections(), timestamp)
	}
	if metrics.IsSetIncomingMessagesCounter() {
		loadMetrics.Put(hostID, extID, load.MsgsInPerSec, metrics.GetIncomingMessagesCounter(), timestamp)
	}
	if metrics.IsSetIncomingBytesCounter() {
		loadMetrics.Put(hostID, extID, load.BytesInPerSec, metrics.GetIncomingBytesCounter(), timestamp)
	}
	if metrics.IsSetOutgoingMessagesCounter() {
		loadMetrics.Put(hostID, extID, load.MsgsOutPerSec, metrics.GetOutgoingMessagesCounter(), timestamp)
	}
	if metrics.IsSetOutgoingBytesCounter() {
		loadMetrics.Put(hostID, extID, load.BytesOutPerSec, metrics.GetOutgoingBytesCounter(), timestamp)
	}
	if metrics.IsSetExtentStatus() {
		status := shared.ExtentStatus(metrics.GetExtentStatus())
		context.extentMonitor.RecvStoreExtentHeartbeat(hostID, extID, status)
	}

	return nil
}

// GetCapacities todo
func (mcp *Mcp) GetCapacities(ctx thrift.Context, getCapacitiesRequest *c.GetCapacitiesRequest) (*c.GetCapacitiesResult_, error) {
	return nil, fmt.Errorf("Not implemented")
}

// UpsertInputHostCapacities todo
func (mcp *Mcp) UpsertInputHostCapacities(ctx thrift.Context, upsertCapacitiesRequest *c.UpsertInputHostCapacitiesRequest) error {
	return fmt.Errorf("Not implemented")
}

// UpsertOutputHostCapacities todo
func (mcp *Mcp) UpsertOutputHostCapacities(ctx thrift.Context, upsertCapacitiesRequest *c.UpsertOutputHostCapacitiesRequest) error {
	return fmt.Errorf("Not implemented")
}

// UpsertStoreCapacities todo
func (mcp *Mcp) UpsertStoreCapacities(ctx thrift.Context, upsertCapacitiesRequest *c.UpsertStoreCapacitiesRequest) error {
	return fmt.Errorf("Not implemented")
}

// RemoveCapacities todo
func (mcp *Mcp) RemoveCapacities(ctx thrift.Context, removeCapacitiesRequest *c.RemoveCapacitiesRequest) error {
	return fmt.Errorf("Not implemented")
}

// CreateDestination creates local and remote destination
func (mcp *Mcp) CreateDestination(ctx thrift.Context, createRequest *shared.CreateDestinationRequest) (*shared.DestinationDescription, error) {
	if !mcp.isStarted() {
		// this can happen because we listen on the tchannel
		// endpoint before the context gets completely built
		return nil, &shared.InternalServiceError{Message: "Controller not started"}
	}

	context := mcp.context
	context.m3Client.IncCounter(metrics.ControllerCreateDestinationScope, metrics.ControllerRequests)
	sw := context.m3Client.StartTimer(metrics.ControllerCreateDestinationScope, metrics.ControllerLatencyTimer)
	defer sw.Stop()

	lclLg := context.log.WithField(common.TagDstPth, common.FmtDstPth(createRequest.GetPath()))

	// create local destination
	destDesc, err := mcp.mClient.CreateDestination(ctx, createRequest)
	if err != nil {
		lclLg.WithField(common.TagErr, err).Error("CreateDestination: local CreateDestination failed")
		context.m3Client.IncCounter(metrics.ControllerCreateDestinationScope, metrics.ControllerFailures)
		return nil, err
	}

	if createRequest.IsSetIsMultiZone() && createRequest.GetIsMultiZone() {
		// get dest uuid from local destination, re-use for remote destination
		destUUID := destDesc.GetDestinationUUID()
		createDestUUIDRequest := shared.NewCreateDestinationUUIDRequest()
		createDestUUIDRequest.Request = createRequest
		createDestUUIDRequest.DestinationUUID = common.StringPtr(destUUID)

		// send to local replicator to fan out
		localReplicator, replicatorErr := mcp.GetClientFactory().GetReplicatorClient()
		lclLg = lclLg.WithField(common.TagDst, common.FmtDst(destUUID))
		if replicatorErr != nil {
			lclLg.WithField(common.TagErr, replicatorErr).Error("CreateDestination: GetReplicatorClient failed")
			context.m3Client.IncCounter(metrics.ControllerCreateDestinationScope, metrics.ControllerErrCallReplicatorCounter)

			// errors in calling replicator doesn't fail this call
			// a reconciliation process between replicators(slow path) will fix the inconsistency eventually
			return destDesc, nil
		}

		replicatorErr = localReplicator.CreateRemoteDestinationUUID(ctx, createDestUUIDRequest)
		if replicatorErr != nil {
			lclLg.WithField(common.TagErr, replicatorErr).Error("CreateDestination: CreateRemoteDestinationUUID failed")
			context.m3Client.IncCounter(metrics.ControllerCreateDestinationScope, metrics.ControllerErrCallReplicatorCounter)

			// errors in calling replicator doesn't fail this call
			// a reconciliation process between replicators(slow path) will fix the inconsistency eventually
			return destDesc, nil
		}
	}

	return destDesc, nil
}

// UpdateDestination updates local and remote destination
func (mcp *Mcp) UpdateDestination(ctx thrift.Context, updateRequest *shared.UpdateDestinationRequest) (*shared.DestinationDescription, error) {
	if !mcp.isStarted() {
		// this can happen because we listen on the tchannel
		// endpoint before the context gets completely built
		return nil, &shared.InternalServiceError{Message: "Controller not started"}
	}

	context := mcp.context
	context.m3Client.IncCounter(metrics.ControllerUpdateDestinationScope, metrics.ControllerRequests)
	sw := context.m3Client.StartTimer(metrics.ControllerUpdateDestinationScope, metrics.ControllerLatencyTimer)
	defer sw.Stop()

	lclLg := context.log.WithField(common.TagDst, common.FmtDst(updateRequest.GetDestinationUUID()))

	if updateRequest.IsSetZoneConfigs() {
		valid, err := mcp.ValidateDestZoneConfig(ctx, lclLg, updateRequest)
		if err != nil {
			context.m3Client.IncCounter(metrics.ControllerUpdateDestinationScope, metrics.ControllerFailures)
			lclLg.WithField(common.TagErr, err).Error("UpdateDestination: ValidateDestZoneConfig returned error")
			return nil, err
		}
		if !valid {
			context.m3Client.IncCounter(metrics.ControllerUpdateDestinationScope, metrics.ControllerFailures)
			lclLg.Error("UpdateDestination: zone config validation failed")
			return nil, &shared.BadRequestError{Message: "zone config validation failed"}
		}
	}

	// update local destination
	destDesc, err := mcp.mClient.UpdateDestination(ctx, updateRequest)
	if err != nil {
		lclLg.WithField(common.TagErr, err).Error("UpdateDestination: local UpdateDestination failed")
		context.m3Client.IncCounter(metrics.ControllerUpdateDestinationScope, metrics.ControllerFailures)
		return nil, err
	}

	if destDesc.GetIsMultiZone() {
		// send to local replicator to fan out
		localReplicator, replicatorErr := mcp.GetClientFactory().GetReplicatorClient()
		lclLg = lclLg.WithField(common.TagDst, common.FmtDst(destDesc.GetDestinationUUID()))
		if replicatorErr != nil {
			lclLg.WithField(common.TagErr, replicatorErr).Error("UpdateDestination: GetReplicatorClient failed")
			context.m3Client.IncCounter(metrics.ControllerUpdateDestinationScope, metrics.ControllerErrCallReplicatorCounter)

			// errors in calling replicator doesn't fail this call
			// a reconciliation process between replicators(slow path) will fix the inconsistency eventually
			return destDesc, nil
		}

		replicatorErr = localReplicator.UpdateRemoteDestination(ctx, updateRequest)
		if replicatorErr != nil {
			lclLg.WithField(common.TagErr, replicatorErr).Error("UpdateDestination: UpdateRemoteDestination failed")
			context.m3Client.IncCounter(metrics.ControllerUpdateDestinationScope, metrics.ControllerErrCallReplicatorCounter)

			// errors in calling replicator doesn't fail this call
			// a reconciliation process between replicators(slow path) will fix the inconsistency eventually
			return destDesc, nil
		}
	}

	return destDesc, nil
}

// DeleteDestination deletes local and remote destination
func (mcp *Mcp) DeleteDestination(ctx thrift.Context, deleteRequest *shared.DeleteDestinationRequest) error {
	if !mcp.isStarted() {
		// this can happen because we listen on the tchannel
		// endpoint before the context gets completely built
		return &shared.InternalServiceError{Message: "Controller not started"}
	}

	context := mcp.context
	context.m3Client.IncCounter(metrics.ControllerDeleteDestinationScope, metrics.ControllerRequests)
	sw := context.m3Client.StartTimer(metrics.ControllerDeleteDestinationScope, metrics.ControllerLatencyTimer)
	defer sw.Stop()

	lclLg := context.log.WithField(common.TagDstPth, common.FmtDstPth(deleteRequest.GetPath()))

	// first read the destination
	readDestinationRequest := &shared.ReadDestinationRequest{
		Path: common.StringPtr(deleteRequest.GetPath()),
	}
	destDesc, err := mcp.mClient.ReadDestination(ctx, readDestinationRequest)
	if err != nil {
		lclLg.WithField(common.TagErr, err).Error("DeleteDestination: ReadDestination failed")
		context.m3Client.IncCounter(metrics.ControllerDeleteDestinationScope, metrics.ControllerFailures)
		return err
	}

	lclLg = lclLg.WithField(common.TagDst, common.FmtDst(destDesc.GetDestinationUUID()))

	// delete local destination
	err = mcp.mClient.DeleteDestination(ctx, deleteRequest)
	if err != nil {
		lclLg.WithField(common.TagErr, err).Error("DeleteDestination: local DeleteDestination failed")
		context.m3Client.IncCounter(metrics.ControllerDeleteDestinationScope, metrics.ControllerFailures)
		return err
	}

	if destDesc.GetIsMultiZone() {
		// send to local replicator to fan out
		localReplicator, replicatorErr := mcp.GetClientFactory().GetReplicatorClient()
		if replicatorErr != nil {
			lclLg.WithField(common.TagErr, replicatorErr).Error("DeleteDestination: GetReplicatorClient failed")
			context.m3Client.IncCounter(metrics.ControllerDeleteDestinationScope, metrics.ControllerErrCallReplicatorCounter)

			// errors in calling replicator doesn't fail this call
			// a reconciliation process between replicators(slow path) will fix the inconsistency eventually
			return nil
		}

		replicatorErr = localReplicator.DeleteRemoteDestination(ctx, deleteRequest)
		if replicatorErr != nil {
			lclLg.WithField(common.TagErr, replicatorErr).Error("DeleteDestination: DeleteRemoteDestination failed")
			context.m3Client.IncCounter(metrics.ControllerDeleteDestinationScope, metrics.ControllerErrCallReplicatorCounter)

			// errors in calling replicator doesn't fail this call
			// a reconciliation process between replicators(slow path) will fix the inconsistency eventually
			return nil
		}
	}

	return nil
}

// CreateConsumerGroup creates local and remote consumer group
func (mcp *Mcp) CreateConsumerGroup(ctx thrift.Context, createRequest *shared.CreateConsumerGroupRequest) (*shared.ConsumerGroupDescription, error) {
	if !mcp.isStarted() {
		// this can happen because we listen on the tchannel
		// endpoint before the context gets completely built
		return nil, &shared.InternalServiceError{Message: "Controller not started"}
	}

	context := mcp.context
	context.m3Client.IncCounter(metrics.ControllerCreateConsumerGroupScope, metrics.ControllerRequests)
	sw := context.m3Client.StartTimer(metrics.ControllerCreateConsumerGroupScope, metrics.ControllerLatencyTimer)
	defer sw.Stop()

	lclLg := context.log.WithFields(bark.Fields{
		common.TagDstPth: common.FmtDstPth(createRequest.GetDestinationPath()),
		common.TagCnsPth: common.FmtCnsPth(createRequest.GetConsumerGroupName()),
	})

	// create local consumer group
	cgDesc, err := mcp.mClient.CreateConsumerGroup(ctx, createRequest)
	if err != nil {
		lclLg.WithField(common.TagErr, err).Error("CreateConsumerGroup: local CreateConsumerGroup failed")
		context.m3Client.IncCounter(metrics.ControllerCreateConsumerGroupScope, metrics.ControllerFailures)
		return nil, err
	}

	if createRequest.GetIsMultiZone() {
		// get dest uuid from local consumer group, re-use for remote consumer group
		cgUUID := cgDesc.GetConsumerGroupUUID()
		createCGUUIDRequest := shared.NewCreateConsumerGroupUUIDRequest()
		createCGUUIDRequest.Request = createRequest
		createCGUUIDRequest.ConsumerGroupUUID = common.StringPtr(cgUUID)

		// send to local replicator to fan out
		localReplicator, replicatorErr := mcp.GetClientFactory().GetReplicatorClient()
		lclLg = lclLg.WithField(common.TagCnsm, common.FmtCnsm(cgUUID))
		if replicatorErr != nil {
			lclLg.WithField(common.TagErr, replicatorErr).Error("CreateConsumerGroup: GetReplicatorClient failed")
			context.m3Client.IncCounter(metrics.ControllerCreateConsumerGroupScope, metrics.ControllerErrCallReplicatorCounter)

			// errors in calling replicator doesn't fail this call
			// a reconciliation process between replicators(slow path) will fix the inconsistency eventually
			return cgDesc, nil
		}

		replicatorErr = localReplicator.CreateRemoteConsumerGroupUUID(ctx, createCGUUIDRequest)
		if replicatorErr != nil {
			lclLg.WithField(common.TagErr, replicatorErr).Error("CreateConsumerGroup: CreateRemoteConsumerGroupUUID error")
			context.m3Client.IncCounter(metrics.ControllerCreateConsumerGroupScope, metrics.ControllerErrCallReplicatorCounter)

			// errors in calling replicator doesn't fail this call
			// a reconciliation process between replicators(slow path) will fix the inconsistency eventually
			return cgDesc, nil
		}
	}

	return cgDesc, nil
}

// UpdateConsumerGroup updates local and remote consumer group
func (mcp *Mcp) UpdateConsumerGroup(ctx thrift.Context, updateRequest *shared.UpdateConsumerGroupRequest) (*shared.ConsumerGroupDescription, error) {
	if !mcp.isStarted() {
		// this can happen because we listen on the tchannel
		// endpoint before the context gets completely built
		return nil, &shared.InternalServiceError{Message: "Controller not started"}
	}

	context := mcp.context
	context.m3Client.IncCounter(metrics.ControllerUpdateConsumerGroupScope, metrics.ControllerRequests)
	sw := context.m3Client.StartTimer(metrics.ControllerUpdateConsumerGroupScope, metrics.ControllerLatencyTimer)
	defer sw.Stop()

	lclLg := context.log.WithFields(bark.Fields{
		common.TagDstPth: common.FmtDstPth(updateRequest.GetDestinationPath()),
		common.TagCnsPth: common.FmtCnsPth(updateRequest.GetConsumerGroupName()),
	})

	if updateRequest.IsSetZoneConfigs() {
		valid, err := mcp.ValidateCgZoneConfig(ctx, lclLg, updateRequest)
		if err != nil {
			context.m3Client.IncCounter(metrics.ControllerUpdateConsumerGroupScope, metrics.ControllerFailures)
			lclLg.WithField(common.TagErr, err).Error("UpdateConsumerGroup: ValidateCgZoneConfig returned error")
			return nil, err
		}
		if !valid {
			context.m3Client.IncCounter(metrics.ControllerUpdateConsumerGroupScope, metrics.ControllerFailures)
			lclLg.Error("UpdateConsumerGroup: zone config validation failed")
			return nil, &shared.BadRequestError{Message: "zone config validation failed"}
		}
	}

	// update local consumer group
	cgDesc, err := mcp.mClient.UpdateConsumerGroup(ctx, updateRequest)
	if err != nil {
		lclLg.WithField(common.TagErr, err).Error("UpdateConsumerGroup: local UpdateConsumerGroup failed")
		context.m3Client.IncCounter(metrics.ControllerUpdateConsumerGroupScope, metrics.ControllerFailures)
		return nil, err
	}

	if cgDesc.GetIsMultiZone() {
		// send to local replicator to fan out
		localReplicator, replicatorErr := mcp.GetClientFactory().GetReplicatorClient()
		lclLg = lclLg.WithField(common.TagCnsm, common.FmtCnsm(cgDesc.GetConsumerGroupUUID()))
		if replicatorErr != nil {
			lclLg.WithField(common.TagErr, err).Error("UpdateConsumerGroup: GetReplicatorClient failed")
			context.m3Client.IncCounter(metrics.ControllerUpdateConsumerGroupScope, metrics.ControllerErrCallReplicatorCounter)

			// errors in calling replicator doesn't fail this call
			// a reconciliation process between replicators(slow path) will fix the inconsistency eventually
			return cgDesc, nil
		}

		replicatorErr = localReplicator.UpdateRemoteConsumerGroup(ctx, updateRequest)
		if replicatorErr != nil {
			lclLg.WithField(common.TagErr, err).Error("UpdateConsumerGroup: UpdateRemoteConsumerGroup failed")
			context.m3Client.IncCounter(metrics.ControllerUpdateConsumerGroupScope, metrics.ControllerErrCallReplicatorCounter)

			// errors in calling replicator doesn't fail this call
			// a reconciliation process between replicators(slow path) will fix the inconsistency eventually
			return cgDesc, nil
		}
	}

	return cgDesc, nil
}

// DeleteConsumerGroup deletes local and remote consumer group
func (mcp *Mcp) DeleteConsumerGroup(ctx thrift.Context, deleteRequest *shared.DeleteConsumerGroupRequest) error {
	if !mcp.isStarted() {
		// this can happen because we listen on the tchannel
		// endpoint before the context gets completely built
		return &shared.InternalServiceError{Message: "Controller not started"}
	}

	context := mcp.context
	context.m3Client.IncCounter(metrics.ControllerDeleteConsumerGroupScope, metrics.ControllerRequests)
	sw := context.m3Client.StartTimer(metrics.ControllerDeleteConsumerGroupScope, metrics.ControllerLatencyTimer)
	defer sw.Stop()

	lclLg := context.log.WithFields(bark.Fields{
		common.TagDstPth: common.FmtDstPth(deleteRequest.GetDestinationPath()),
		common.TagCnsPth: common.FmtCnsPth(deleteRequest.GetConsumerGroupName()),
	})

	readCGReq := &shared.ReadConsumerGroupRequest{
		DestinationPath:   deleteRequest.DestinationPath,
		DestinationUUID:   deleteRequest.DestinationUUID,
		ConsumerGroupName: deleteRequest.ConsumerGroupName,
	}
	cgDesc, err := mcp.mClient.ReadConsumerGroup(nil, readCGReq)
	if err != nil {
		lclLg.Error(err.Error())
		context.m3Client.IncCounter(metrics.ControllerDeleteConsumerGroupScope, metrics.ControllerFailures)
		return err
	}

	// delete local consumer group
	err = mcp.mClient.DeleteConsumerGroup(ctx, deleteRequest)
	if err != nil {
		lclLg.WithField(common.TagErr, err).Error("DeleteConsumerGroup: local DeleteConsumerGroup failed")
		context.m3Client.IncCounter(metrics.ControllerDeleteConsumerGroupScope, metrics.ControllerFailures)
		return err
	}

	if cgDesc.GetIsMultiZone() {
		// send to local replicator to fan out
		localReplicator, replicatorErr := mcp.GetClientFactory().GetReplicatorClient()
		lclLg = lclLg.WithField(common.TagCnsm, common.FmtCnsm(cgDesc.GetConsumerGroupUUID()))
		if replicatorErr != nil {
			lclLg.WithField(common.TagErr, err).Error("DeleteConsumerGroup: GetReplicatorClient failed")
			context.m3Client.IncCounter(metrics.ControllerDeleteConsumerGroupScope, metrics.ControllerErrCallReplicatorCounter)

			// errors in calling replicator doesn't fail this call
			// a reconciliation process between replicators(slow path) will fix the inconsistency eventually
			return nil
		}

		replicatorErr = localReplicator.DeleteRemoteConsumerGroup(ctx, deleteRequest)
		if replicatorErr != nil {
			lclLg.WithField(common.TagErr, err).Error("DeleteConsumerGroup: DeleteRemoteConsumerGroup failed")
			context.m3Client.IncCounter(metrics.ControllerDeleteConsumerGroupScope, metrics.ControllerErrCallReplicatorCounter)

			// errors in calling replicator doesn't fail this call
			// a reconciliation process between replicators(slow path) will fix the inconsistency eventually
			return nil
		}
	}

	return nil
}

// CreateRemoteZoneExtent creates an extent that originates from another zone
func (mcp *Mcp) CreateRemoteZoneExtent(ctx thrift.Context, createRequest *shared.CreateExtentRequest) (*shared.CreateExtentResult_, error) {
	if !mcp.isStarted() {
		// this can happen because we listen on the tchannel
		// endpoint before the context gets completely built
		return nil, &shared.InternalServiceError{Message: "Controller not started"}
	}

	context := mcp.context
	context.m3Client.IncCounter(metrics.ControllerCreateRemoteZoneExtentScope, metrics.ControllerRequests)
	sw := context.m3Client.StartTimer(metrics.ControllerCreateRemoteZoneExtentScope, metrics.ControllerLatencyTimer)
	defer sw.Stop()

	lclLg := context.log.WithFields(bark.Fields{
		common.TagDstPth:   common.FmtDstPth(createRequest.GetExtent().GetDestinationUUID()),
		common.TagExt:      common.FmtExt(createRequest.GetExtent().GetExtentUUID()),
		common.TagZoneName: common.FmtZoneName(createRequest.GetExtent().GetOriginZone()),
	})

	// TODO: the replica factor should be read from the zone configs
	var nReplicasPerExtent = int(context.appConfig.GetDestinationConfig().GetReplicas())

	storehosts, err := context.placement.PickStoreHosts(nReplicasPerExtent)
	if err != nil {
		lclLg.WithField(common.TagErr, err).Error("CreateRemoteZoneExtent: PickStoreHosts failed")
		context.m3Client.IncCounter(metrics.ControllerCreateRemoteZoneExtentScope, metrics.ControllerErrPickStoreHostCounter)
		return nil, &shared.InternalServiceError{Message: err.Error()}
	}

	storeids := make([]string, nReplicasPerExtent)
	for i := 0; i < nReplicasPerExtent; i++ {
		storeids[i] = storehosts[i].UUID
	}
	remoteExtentPrimaryStore := storeids[rand.Intn(len(storeids))]

	// Since this is an extent from another zone, we don't need to assign input host. i.e. the extent is read-only
	// We use a special input host uuid (instead of an empty one) for this because there're lots of places where an valid uuid is required
	inputHost := common.InputHostForRemoteExtent

	res, err := context.mm.CreateRemoteZoneExtent(createRequest.GetExtent().GetDestinationUUID(),
		createRequest.GetExtent().GetExtentUUID(), inputHost, storeids, createRequest.GetExtent().GetOriginZone(),
		remoteExtentPrimaryStore, createRequest.GetConsumerGroupVisibility())
	if err != nil {
		lclLg.WithField(common.TagErr, err).Error("CreateRemoteZoneExtent: metadata CreateRemoteZoneExtent failed")
		context.m3Client.IncCounter(metrics.ControllerCreateRemoteZoneExtentScope, metrics.ControllerErrMetadataUpdateCounter)
		return nil, err
	}

	// trigger store to start replication
	event := NewStartReplicationForRemoteZoneExtent(createRequest.GetExtent().GetDestinationUUID(), createRequest.GetExtent().GetExtentUUID(), storeids, remoteExtentPrimaryStore)
	mcp.context.eventPipeline.Add(event)

	lclLg.Info("Remote Zone Extent Created")
	return res, nil
}

// CreateRemoteZoneConsumerGroupExtent creates an cg extent that originates from another zone
func (mcp *Mcp) CreateRemoteZoneConsumerGroupExtent(ctx thrift.Context, createRequest *shared.CreateConsumerGroupExtentRequest) error {
	if !mcp.isStarted() {
		// this can happen because we listen on the tchannel
		// endpoint before the context gets completely built
		return &shared.InternalServiceError{Message: "Controller not started"}
	}

	context := mcp.context
	context.m3Client.IncCounter(metrics.ControllerCreateRemoteZoneCgExtentScope, metrics.ControllerRequests)
	sw := context.m3Client.StartTimer(metrics.ControllerCreateRemoteZoneCgExtentScope, metrics.ControllerLatencyTimer)
	defer sw.Stop()

	lclLg := context.log.WithFields(bark.Fields{
		common.TagDst:  common.FmtDst(createRequest.GetDestinationUUID()),
		common.TagCnsm: common.FmtCnsm(createRequest.GetConsumerGroupUUID()),
		common.TagExt:  common.FmtExt(createRequest.GetExtentUUID()),
	})

	destExt, err := context.mm.ReadExtentStats(createRequest.GetDestinationUUID(), createRequest.GetExtentUUID())
	if err != nil {
		lclLg.WithField(common.TagErr, err).Error("CreateRemoteZoneConsumerGroupExtent: metadata ReadExtentStats failed")
		context.m3Client.IncCounter(metrics.ControllerCreateRemoteZoneCgExtentScope, metrics.ControllerErrMetadataReadCounter)
		return err
	}
	if !(destExt.GetStatus() == shared.ExtentStatus_OPEN) && !(destExt.GetStatus() == shared.ExtentStatus_SEALED) {
		lclLg.WithField(common.TagErr, err).Error("CreateRemoteZoneConsumerGroupExtent: dest extent is neither open nor sealed")
		context.m3Client.IncCounter(metrics.ControllerCreateRemoteZoneCgExtentScope, metrics.ControllerErrMetadataReadCounter)
		return err
	}

	outhost, err := pickOutputHostForStoreHosts(context, destExt.GetExtent().GetStoreUUIDs())
	if err != nil {
		lclLg.WithField(common.TagErr, err).Error("CreateRemoteZoneConsumerGroupExtent: Failed to pick outhost for extent")
		context.m3Client.IncCounter(metrics.ControllerCreateRemoteZoneCgExtentScope, metrics.ControllerErrPickOutHostCounter)
		return err
	}

	err = context.mm.AddExtentToConsumerGroup(createRequest.GetDestinationUUID(), createRequest.GetConsumerGroupUUID(),
		createRequest.GetExtentUUID(), outhost.UUID, destExt.GetExtent().GetStoreUUIDs())
	if err != nil {
		lclLg.WithField(common.TagErr, err).Warn("Failed to add cg extent to consumer group")
		context.m3Client.IncCounter(metrics.ControllerCreateRemoteZoneCgExtentScope, metrics.ControllerErrMetadataUpdateCounter)
		return err
	}

	// Schedule an async notification to outhost to
	// load the newly created extent
	event := NewConsGroupUpdatedEvent(createRequest.GetDestinationUUID(), createRequest.GetConsumerGroupUUID(),
		createRequest.GetExtentUUID(), outhost.UUID)
	context.eventPipeline.Add(event)

	lclLg.Info("CreateRemoteZoneConsumerGroupExtent: Extent added to consumer group")
	return nil
}

// ValidateDestZoneConfig validates the zone configs for a UpdateDestinationRequest
func (mcp *Mcp) ValidateDestZoneConfig(ctx thrift.Context, logger bark.Logger, updateRequest *shared.UpdateDestinationRequest) (bool, error) {
	// first read the destination
	destDesc, err := mcp.mClient.ReadDestination(ctx, &shared.ReadDestinationRequest{
		DestinationUUID: common.StringPtr(updateRequest.GetDestinationUUID()),
	})
	if err != nil {
		logger.WithField(common.TagErr, err).Error("ValidateDestZoneConfig: ReadDestination in local failed")
		return false, err
	}

	destPath := destDesc.GetPath()

	localReplicator, err := mcp.GetClientFactory().GetReplicatorClient()
	if err != nil {
		logger.WithField(common.TagErr, err).Error("ValidateDestZoneConfig: GetReplicatorClient failed")
		return false, err
	}

	for _, zoneConfig := range updateRequest.GetZoneConfigs() {
		if strings.EqualFold(zoneConfig.GetZone(), mcp.context.localZone) {
			continue
		}

		remoteDest, err := localReplicator.ReadDestinationInRemoteZone(ctx, &shared.ReadDestinationInRemoteZoneRequest{
			Zone: common.StringPtr(zoneConfig.GetZone()),
			Request: &shared.ReadDestinationRequest{
				Path: common.StringPtr(destPath),
			},
		})
		if err == nil {
			// path exists in remote. If uuid is different, then fail the validation (unsupported scenario)
			// We could potentially support this scenario by updating the UUID of the remote destination and moving all
			// extents to the new UUID.
			if remoteDest.GetDestinationUUID() != updateRequest.GetDestinationUUID() {
				logger.WithField(common.TagZoneName, common.FmtZoneName(zoneConfig.GetZone())).Error(`destination exists in remote but UUID is different`)
				return false, nil
			}
		} else if _, ok := err.(*shared.EntityNotExistsError); ok {
			// path doesn't exist in remote, validation succeeds
			continue
		} else {
			// some other error
			return false, err
		}
	}
	return true, nil
}

// ValidateCgZoneConfig validates the zone configs for a UpdateConsumerGroupRequest
func (mcp *Mcp) ValidateCgZoneConfig(ctx thrift.Context, logger bark.Logger, updateRequest *shared.UpdateConsumerGroupRequest) (bool, error) {
	// first read the cg
	cgDesc, err := mcp.mClient.ReadConsumerGroup(ctx, &shared.ReadConsumerGroupRequest{
		DestinationPath:   common.StringPtr(updateRequest.GetDestinationPath()),
		ConsumerGroupName: common.StringPtr(updateRequest.GetConsumerGroupName()),
	})
	if err != nil {
		logger.WithField(common.TagErr, err).Error("ValidateCgZoneConfig: ReadConsumerGroup in local failed")
		return false, err
	}

	localReplicator, err := mcp.GetClientFactory().GetReplicatorClient()
	if err != nil {
		logger.WithField(common.TagErr, err).Error("ValidateCgZoneConfig: GetReplicatorClient failed")
		return false, err
	}

	for _, zoneConfig := range updateRequest.GetZoneConfigs() {
		if strings.EqualFold(zoneConfig.GetZone(), mcp.context.localZone) {
			continue
		}

		remoteCg, err := localReplicator.ReadConsumerGroupInRemoteZone(ctx, &shared.ReadConsumerGroupInRemoteRequest{
			Zone: common.StringPtr(zoneConfig.GetZone()),
			Request: &shared.ReadConsumerGroupRequest{
				DestinationPath:   common.StringPtr(updateRequest.GetDestinationPath()),
				ConsumerGroupName: common.StringPtr(updateRequest.GetConsumerGroupName()),
			},
		})
		if err == nil {
			// cg exists in remote. If uuid is different, then fail the validation
			if remoteCg.GetDestinationUUID() != cgDesc.GetDestinationUUID() || remoteCg.GetConsumerGroupUUID() != cgDesc.GetConsumerGroupUUID() {
				logger.WithField(common.TagZoneName, common.FmtZoneName(zoneConfig.GetZone())).Error(`cg exists in remote but UUID is different`)
				return false, nil
			}
		} else if _, ok := err.(*shared.EntityNotExistsError); ok {
			// cg doesn't exist in remote, validation succeeds
			continue
		} else {
			// some other error
			return false, err
		}
	}
	return true, nil
}
