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

package inputhost

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"github.com/Sirupsen/logrus"
	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"

	ccommon "github.com/uber/cherami-client-go/common"
	"github.com/uber/cherami-server/common"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
	mm "github.com/uber/cherami-server/common/metadata"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/inputhost/load"
	"github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

const (
	// defaultIdleTimeout is the time to wait before we close all streams
	defaultIdleTimeout = 10 * time.Minute

	// TODO: need to figure out the optimum number for the bufferSize to acheieve the right balance between throughput vs latency
	defaultBufferSize = 1000

	// hostLoadReportingInterval is the interval input host load is reported to controller
	hostLoadReportingInterval = 2 * time.Second

	// minimumAllowedMessageDelaySeconds is the minimum allowed delay on a message; we reject
	// any messages whose delay is lower than this.
	minimumAllowedMessageDelaySeconds = 60

	// minimumAllowedMessageDelaySecondsTest is the min allowed delay for '/test' destinations
	minimumAllowedMessageDelaySecondsTest = 3

	// extentRolloverSeqnum{Min,Max} is the range within which extents would be sealed
	// triggering a roll-over to a new extent. the code picks a random number in this
	// range (currently, betweetn 10 million and 20 million) and will seal at this
	// sequence number proactively so that we don't have a very large extent.
	extentRolloverSeqnumMin, extentRolloverSeqnumMax = 10000000, 20000000

	// the default drain timeout
	defaultDrainTimeout = 1 * time.Minute

	// connWGTimeout is the timeout to wait after we send the drain command and
	// before we start the drain
	connWGTimeout = 5 * time.Second

	// drainAllUUID is the UUID used during drainAll
	drainAllUUID = "D3A9C5DC-AE62-4465-9898-7FE71BD1FCA"
)

var (
	batchMsgAckTimeout = 1 * time.Minute // msg ack timeout for batch messages
)

type (
	// InputHost is the main server class for InputHosts
	InputHost struct {
		logger                 bark.Logger
		loadShutdownRef        int32
		pathMutex              sync.RWMutex
		pathCache              map[string]*inPathCache
		pathCacheByDestPath    map[string]string
		shutdownWG             sync.WaitGroup
		shutdown               chan struct{}
		cacheTimeout           time.Duration
		mClient                metadata.TChanMetadataService
		hostIDHeartbeater      common.HostIDHeartbeater
		loadReporter           common.LoadReporterDaemon
		m3Client               metrics.Client
		dConfigClient          dconfig.Client
		tokenBucketValue       atomic.Value // value to controll acceess for tokenBucket
		hostConnLimit          int32
		hostConnLimitPerSecond int32
		maxConnLimit           int32
		extMsgsLimitPerSecond  int32
		connMsgsLimitPerSecond int32
		hostMetrics            *load.HostMetrics
		lastLoadReportedTime   int64        // unix nanos when the last load report was sent
		nodeStatus             atomic.Value // status of the node
		testShortExtentsByPath string       // Override to make some paths randomly have extremely short or zero-length extents
		common.SCommon
	}

	// InOptions is the options used during instantiating a new host
	InOptions struct {
		//CacheIdleTimeout
		CacheIdleTimeout time.Duration
	}

	// extentInfo contains information about location of an extent
	extentInfo struct {
		uuid     string
		replicas []string
	}

	// inReconfigInfo contains information about the reconfigure command from extent controller
	inReconfigInfo struct {
		req        *admin.DestinationUpdatedNotification
		updateUUID string
	}
)

// interface implementation check
var _ cherami.TChanBIn = &InputHost{}
var _ admin.TChanInputHostAdmin = &InputHost{}
var _ common.WSService = &InputHost{}

// ErrHostShutdown is returned when the host is already shutdown
var ErrHostShutdown = &cherami.InternalServiceError{Message: "InputHost already shutdown"}

// ErrThrottled is returned when the host is already shutdown
var ErrThrottled = &cherami.InternalServiceError{Message: "InputHost throttling publisher cconnection"}

// ErrDstNotLoaded is returned when this input host doesn't own any extents for the destination
var ErrDstNotLoaded = &cherami.InternalServiceError{Message: "Destination no longer served by this input host"}

// ErrDrainTimedout is returned when the draining of extents times out
var ErrDrainTimedout = &cherami.InternalServiceError{Message: "Draining of Extents timedout"}

func (h *InputHost) isDestinationWritable(destDesc *shared.DestinationDescription) bool {
	status := destDesc.GetStatus()
	if status != shared.DestinationStatus_ENABLED && status != shared.DestinationStatus_SENDONLY {
		return false
	}
	return true
}

// listExtentsByStatus gets all extents belonging to
// (a) this inputhost for
// (b) the given destination
// (c) with the given status
func (h *InputHost) listExtentsByStatus(ctx thrift.Context, destUUID string, status *shared.ExtentStatus) (*metadata.ListInputHostExtentsStatsResult_, error) {

	listReq := metadata.ListInputHostExtentsStatsRequest{
		InputHostUUID:   common.StringPtr(h.GetHostUUID()),
		DestinationUUID: common.StringPtr(destUUID),
		Status:          status,
	}

	listResp, err := h.mClient.ListInputHostExtentsStats(ctx, &listReq)
	if err != nil {
		return nil, fmt.Errorf("ListInputHostExtentStats() returned error dst=%v, err=%v",
			destUUID, err)
	}

	if listResp == nil || len(listResp.GetExtentStatsList()) == 0 {
		return nil, fmt.Errorf("getExtentsInfoForDestination - Found zero extents for destination, dst=%v",
			destUUID)
	}

	return listResp, nil
}

func (h *InputHost) createExtentInfo(extentUUID string, storeIDs []string) *extentInfo {
	if extentUUID == "" || storeIDs == nil || len(storeIDs) == 0 {
		return nil
	}

	var rpm = h.GetRingpopMonitor()

	var info = &extentInfo{
		uuid:     extentUUID,
		replicas: make([]string, 0, 3),
	}

	for _, storeID := range storeIDs {
		addr, err := rpm.ResolveUUID(common.StoreServiceName, storeID)
		if err != nil {
			h.logger.WithField(`storeID`, storeID).
				WithField(common.TagErr, err).
				Error("Cannot resolve UUID for store")

			return nil
		}
		info.replicas = append(info.replicas, addr)
	}

	return info
}

// getExtentsInfoForDestination returns all the active writable extents
// on this inputhost for the given destination.
func (h *InputHost) getExtentsInfoForDestination(ctx thrift.Context, destUUID string) ([]*extentInfo, error) {

	// get all OPEN extents
	listResp, err := h.listExtentsByStatus(ctx, destUUID, common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN))
	if err != nil {
		return nil, err
	}

	var extents = make([]*extentInfo, 0, 2)

	for _, item := range listResp.ExtentStatsList {
		extent := item.Extent

		info := h.createExtentInfo(extent.GetExtentUUID(), extent.StoreUUIDs)
		if info == nil {
			continue
		}
		extents = append(extents, info)
	}

	if len(extents) < 1 {
		h.logger.WithField(common.TagDst, common.FmtDst(destUUID)).
			WithField("nExtents", len(listResp.GetExtentStatsList())).
			Error("Can't find extents for destination, no healthy extent found")
		return nil, fmt.Errorf("No healthy extent found")
	}

	return extents, nil
}

// getExtentsAndLoadPathCache first tries to get all extents for the given destUUID and
// then loads the pathCache
func (h *InputHost) getExtentsAndLoadPathCache(ctx thrift.Context, destPath, destUUID string, destType shared.DestinationType) (*inPathCache, error) {
	extents, err := h.getExtentsInfoForDestination(ctx, destUUID)
	if err != nil {
		// XXX: Disable due to log spam
		// h.logger.WithField(common.TagDst, common.FmtDst(destUUID)).
		//	WithField(common.TagErr, err).Error(`Can't find extents for destination`)
		return nil, err
	}

	pathCache := h.loadPath(extents, destPath, destUUID, destType, h.m3Client)
	return pathCache, nil
}

// checkDestination reads destination from metadata store and make sure it's writable
func (h *InputHost) checkDestination(ctx thrift.Context, path string) (string, shared.DestinationType, metrics.ErrorClass, error) {
	// talk to metadata
	mGetRequest := shared.ReadDestinationRequest{Path: common.StringPtr(path)}
	destDesc, err := h.mClient.ReadDestination(ctx, &mGetRequest)
	if err != nil {
		errC, newErr := common.ConvertDownstreamErrors(h.logger, err)
		return "", shared.DestinationType_UNKNOWN, errC, newErr
	}

	// Make sure destDesc cannot be nil
	if destDesc == nil {
		errMsg := fmt.Sprintf("unable to get destination description from metadata for dst=%v", path)
		errC, newErr := common.ConvertDownstreamErrors(h.logger, &cherami.BadRequestError{Message: errMsg})
		return "", shared.DestinationType_UNKNOWN, errC, newErr
	}

	// Now make sure the destination is writable
	if !h.isDestinationWritable(destDesc) {
		errMsg := fmt.Sprintf("Destination is not writable, dst=%v, status=%v", path, destDesc.GetStatus())
		errC, newErr := common.ConvertDownstreamErrors(h.logger, &cherami.BadRequestError{Message: errMsg})
		return "", shared.DestinationType_UNKNOWN, errC, newErr
	}

	return destDesc.GetDestinationUUID(), destDesc.GetType(), metrics.NoError, nil
}

// OpenPublisherStreamHandler is websocket handler for opening publisher stream
func (h *InputHost) OpenPublisherStreamHandler(w http.ResponseWriter, r *http.Request) {

	// get parameters from header
	path := r.Header.Get("path")
	if len(path) == 0 {
		err := &cherami.BadRequestError{}
		h.logger.Warn("please set the path as part of the header. BadRequestError")
		h.m3Client.IncCounter(metrics.OpenPublisherStreamScope, metrics.InputhostUserFailures)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// setup websocket
	wsStream, err := h.GetWSConnector().AcceptPublisherStream(w, r)
	if err != nil {
		h.logger.WithField(common.TagDstPth, common.FmtDstPth(path)).
			WithField(common.TagErr, err).Error("unable to upgrade websocket connection")
		h.m3Client.IncCounter(metrics.OpenPublisherStreamScope, metrics.InputhostInternalFailures)
		return
	}

	// create fake thrift context with header
	ctx, cancel := thrift.NewContext(common.MaxDuration)
	defer cancel()
	ctx = thrift.WithHeaders(ctx, map[string]string{
		"path": path,
	})

	// create thrift stream call wrapper and deligate to streaming call
	if err = h.OpenPublisherStream(ctx, wsStream); err != nil {
		h.logger.WithField(common.TagDstPth, common.FmtDstPth(path)).
			WithField(common.TagErr, err).Error("unable to open publish stream")
		return
	}
}

// OpenPublisherStream is the implementation of the thrift handler for the In service
func (h *InputHost) OpenPublisherStream(ctx thrift.Context, call stream.BInOpenPublisherStreamInCall) error {

	h.m3Client.IncCounter(metrics.OpenPublisherStreamScope, metrics.InputhostRequests)

	path, ok := (ctx.Headers()["path"])
	if !ok {
		err := &cherami.BadRequestError{}
		h.logger.Warn("please set the path as part of the header. BadRequestError")
		// stop the stream before returning
		call.Done()
		h.m3Client.IncCounter(metrics.OpenPublisherStreamScope, metrics.InputhostUserFailures)
		return err
	}
	h.logger.WithField(common.TagDstPth, common.FmtDstPth(path)).Debug("inputhost: OpenPublisherStream called with path")

	// make sure the rate is satisfied. If not reject the request outright
	if h.IsLimitsEnabled() {
		if ok, _ = h.GetTokenBucketValue().TryConsume(1); !ok {
			h.logger.WithField(common.TagHostConnLimit,
				common.FmtHostConnLimit(h.GetHostConnLimitPerSecond())).
				Warn("Too many open publisher streams on this inputhost within a second")
			call.Done()
			h.m3Client.IncCounter(metrics.OpenPublisherStreamScope, metrics.InputhostUserFailures)
			return ErrThrottled
		}
	}

	// if we have reached the overall host connection limit, it is bad as well and we need to stop
	if h.IsLimitsEnabled() && (h.GetNumConnections() > h.GetHostConnLimitOverall()) {
		h.logger.WithField(common.TagHostConnLimit,
			common.FmtHostConnLimit(h.GetNumConnections())).
			Warn("Too many open connections on this inputhost. Rejecting this open")
		call.Done()
		h.m3Client.IncCounter(metrics.OpenPublisherStreamScope, metrics.InputhostUserFailures)
		return ErrThrottled
	}

	// If we are already shutting down, no need to do anything here.
	// the shutdown ref needs to be taken here during the path load to
	// make sure we don't race with a shutdown.
	if atomic.AddInt32(&h.loadShutdownRef, 1) <= 0 {
		// put back the loadShutdownRef
		atomic.AddInt32(&h.loadShutdownRef, -1)
		_, newErr := common.ConvertDownstreamErrors(h.logger, ErrHostShutdown)
		call.Done()
		return newErr
	}
	// from here on, we should put back the shutdown ref whenever we error out.
	// if we already have the path, no need to contact metadata
	pathCache, ok := h.getPathCacheByDestPath(path)
	if !ok || pathCache == nil {
		// Check to make sure a valid destination
		destinationUUID, destType, errC, err := h.checkDestination(ctx, path)
		if err != nil {
			h.logger.WithField(common.TagDstPth, common.FmtDstPth(path)).
				WithField(common.TagErr, err).Error("failed on destination check")
			// put back the loadShutdownRef
			atomic.AddInt32(&h.loadShutdownRef, -1)
			// stop the stream before returning
			call.Done()
			h.incFailureCounter(metrics.OpenPublisherStreamScope, errC)
			return err

		}

		pathCache, err = h.getExtentsAndLoadPathCache(ctx, path, destinationUUID, destType)

		if err != nil || pathCache == nil {
			h.logger.WithField(common.TagDstPth, common.FmtDstPth(path)).
				WithField(common.TagErr, err).Error("unable to load path cache for destination")
			// put back the loadShutdownRef
			atomic.AddInt32(&h.loadShutdownRef, -1)
			// stop the stream before returning
			call.Done()
			h.m3Client.IncCounter(metrics.OpenPublisherStreamScope, metrics.InputhostInternalFailures)
			return &ReplicaNotExistsError{}
		}
	}

	doneCh := make(chan bool, 5)

	pathCache.Lock()

	errCleanup := func() {
		pathCache.Unlock()
		// put back the loadShutdownRef
		atomic.AddInt32(&h.loadShutdownRef, -1)
		// stop the stream before returning
		call.Done()
	}

	if !pathCache.isActive() {
		// path cache is being unloaded, can't add new conns
		errCleanup()
		h.m3Client.IncCounter(metrics.OpenPublisherStreamScope, metrics.InputhostInternalFailures)
		return ErrDstNotLoaded
	}

	// if the number of connections has breached then we can reject the connection
	hostMaxConnPerDestination := h.GetMaxConnPerDest()
	if h.IsLimitsEnabled() && pathCache.dstMetrics.Get(load.DstMetricNumOpenConns) > int64(hostMaxConnPerDestination) {
		pathCache.logger.WithField(common.TagHostConnLimit,
			common.FmtHostConnLimit(hostMaxConnPerDestination)).
			Warn("Too many open connections on this path. Rejecting this open")
		errCleanup()
		h.m3Client.IncCounter(metrics.OpenPublisherStreamScope, metrics.InputhostUserFailures)
		return ErrThrottled
	}
	conn := newPubConnection(path, call, pathCache, h.m3Client, h.IsLimitsEnabled(), h.cacheTimeout, doneCh)
	pathCache.connections[pathCache.currID] = conn
	conn.open()
	pathCache.currID++
	// increase the active connection count
	pathCache.dstMetrics.Increment(load.DstMetricNumOpenConns)
	pathCache.Unlock()

	// increase the num open conns for the host
	h.hostMetrics.Increment(load.HostMetricNumOpenConns)
	// put back the loadShutdownRef, which we took for the pathcache load
	// now we can just wait for the connections to go down in case, we
	// happen to shutdown.
	atomic.AddInt32(&h.loadShutdownRef, -1)
	// wait till the conn is closed. we cannot return immediately.
	// If we do so, we will get data races reading/writing from/to the stream
	<-conn.doneCh

	// decrement the active connection count
	pathCache.dstMetrics.Decrement(load.DstMetricNumOpenConns)
	h.hostMetrics.Decrement(load.HostMetricNumOpenConns)
	return nil
}

// PutMessageBatch is a thrift handler. It publishes a batch of messages to the extents of this input host for the destination.
func (h *InputHost) PutMessageBatch(ctx thrift.Context, request *cherami.PutMessageBatchRequest) (*cherami.PutMessageBatchResult_, error) {

	sw := h.m3Client.StartTimer(metrics.PutMessageBatchInputHostScope, metrics.InputhostLatencyTimer)
	defer sw.Stop()
	h.m3Client.IncCounter(metrics.PutMessageBatchInputHostScope, metrics.InputhostRequests)
	path := request.GetDestinationPath()
	messages := request.GetMessages()
	lclLg := h.logger.WithField(common.TagDstPth, common.FmtDstPth(path))
	var msgLength int64

	// If we are already shutting down, no need to do anything here
	if atomic.AddInt32(&h.loadShutdownRef, 1) <= 0 {
		// put back the loadShutdownRef
		atomic.AddInt32(&h.loadShutdownRef, -1)
		_, newErr := common.ConvertDownstreamErrors(h.logger, ErrHostShutdown)
		return nil, newErr
	}
	pathCache, ok := h.getPathCacheByDestPath(path)

	if !ok || pathCache == nil {
		// Check to make sure a valid destination
		destinationUUID, destType, errC, err := h.checkDestination(ctx, path)
		if err != nil {
			lclLg.WithField(common.TagErr, err).Error("failed on destination check")
			h.incFailureCounter(metrics.PutMessageBatchInputHostScope, errC)
			return nil, err
		}

		// get extents and load path
		pathCache, err = h.getExtentsAndLoadPathCache(ctx, path, destinationUUID, destType)
		if err != nil || pathCache == nil {
			lclLg.WithField(common.TagErr, err).Error("unable to load path cache for destination")
			// put back the loadShutdownRef
			atomic.AddInt32(&h.loadShutdownRef, -1)
			h.m3Client.IncCounter(metrics.PutMessageBatchInputHostScope, metrics.InputhostInternalFailures)
			return nil, &ReplicaNotExistsError{}
		}
	}

	result := cherami.NewPutMessageBatchResult_()
	ackChannel := make(chan *cherami.PutMessageAck, defaultBufferSize)
	inflightRequestCnt := 0
	inflightMsgMap := make(map[string]struct{})

	for _, msg := range messages {
		msgLength = int64(len(msg.Data))
		// make sure we increment the bytes in counter for this path and this host
		pathCache.dstMetrics.Add(load.DstMetricBytesIn, msgLength)
		pathCache.hostMetrics.Add(load.HostMetricBytesIn, msgLength)

		inMsg := &inPutMessage{
			putMsg:         msg,
			putMsgAckCh:    ackChannel,
			putMsgRecvTime: time.Now(),
		}

		select {
		case pathCache.putMsgCh <- inMsg:
			// remember how many ack is needed
			inflightRequestCnt++
			inflightMsgMap[msg.GetID()] = struct{}{}
		default:
			// just send a THROTTLED status back if sending to message channel is blocked
			result.FailedMessages = append(result.FailedMessages, &cherami.PutMessageAck{
				ID:      common.StringPtr(msg.GetID()),
				Status:  common.CheramiStatusPtr(cherami.Status_THROTTLED),
				Message: common.StringPtr("throttling; inputhost is busy"),
			})
		}
	}

	internalErrs, userErrs := int64(0), int64(0)
	var respStatus cherami.Status
	var respMsg string

	ackReceived := func(ack *cherami.PutMessageAck) {
		if ack.GetStatus() != cherami.Status_OK {
			if ack.GetStatus() != cherami.Status_THROTTLED {
				internalErrs++
			} else {
				userErrs++
			}
			result.FailedMessages = append(result.FailedMessages, ack)
		} else {
			result.SuccessMessages = append(result.SuccessMessages, ack)
		}
		delete(inflightMsgMap, ack.GetID())
	}
	// Setup the msgTimer
	msgTimer := common.NewTimer(batchMsgAckTimeout)
	defer msgTimer.Stop()

	// Try to get as many acks as possible.
	// We should break out if either of the following happens:
	// 1. pathCache is unloaded
	// 2. we hit the message timeout
ACKDRAIN:
	for i := 0; i < inflightRequestCnt; i++ {
		select {
		case ack := <-ackChannel:
			ackReceived(ack)
		default:
			// Now look for either the pathCache unloading,
			// or the msgTimer timing out along with the
			// ackChannel as well.
			// We do this in the default case to make sure
			// we can drain all the acks in the channel above
			// before bailing out
			select {
			case ack := <-ackChannel:
				ackReceived(ack)
			case <-pathCache.closeCh:
				respStatus = cherami.Status_FAILED
				respMsg = "pathCache unloaded"
				break ACKDRAIN
			case <-msgTimer.C:
				respStatus = cherami.Status_TIMEDOUT
				respMsg = "message timedout"
				break ACKDRAIN
			}
		}
	}

	// all remaining messages in the inflight map failed
	if len(inflightMsgMap) > 0 {
		pathCache.logger.WithFields(bark.Fields{
			`numFailedMessages`: len(inflightMsgMap),
			`respMsg`:           respMsg,
		}).Info("failing putMessageBatch")
		for id := range inflightMsgMap {
			result.FailedMessages = append(result.FailedMessages, &cherami.PutMessageAck{
				ID:      common.StringPtr(id),
				Status:  common.CheramiStatusPtr(respStatus),
				Message: common.StringPtr(respMsg),
			})
			internalErrs++
		}
	}
	// update the last disconnect time now
	pathCache.updateLastDisconnectTime()

	// put back the loadShutdownRef
	atomic.AddInt32(&h.loadShutdownRef, -1)

	// Emit M3 metrics for per host and per destination

	h.m3Client.AddCounter(metrics.PutMessageBatchInputHostScope, metrics.InputhostMessageReceived, int64(len(result.SuccessMessages)))
	pathCache.destM3Client.AddCounter(metrics.PutMessageBatchInputHostDestScope, metrics.InputhostDestMessageReceived, int64(len(result.SuccessMessages)))
	h.m3Client.AddCounter(metrics.PutMessageBatchInputHostScope, metrics.InputhostMessageUserFailures, userErrs)
	pathCache.destM3Client.AddCounter(metrics.PutMessageBatchInputHostDestScope, metrics.InputhostDestMessageUserFailures, userErrs)
	h.m3Client.AddCounter(metrics.PutMessageBatchInputHostScope, metrics.InputhostMessageInternalFailures, internalErrs)
	pathCache.destM3Client.AddCounter(metrics.PutMessageBatchInputHostDestScope, metrics.InputhostDestMessageInternalFailures, internalErrs)

	// Increment the overall incoming messages, failed messages and acks
	pathCache.dstMetrics.Add(load.DstMetricOverallNumMsgs, int64(len(messages)))
	pathCache.dstMetrics.Add(load.DstMetricNumFailed, (internalErrs + userErrs))
	pathCache.dstMetrics.Add(load.DstMetricNumAcks, int64(len(result.SuccessMessages)))

	return result, nil
}

// DestinationsUpdated is the API exposed to Extent Controller to communicate any changes to existing view of extents
func (h *InputHost) DestinationsUpdated(ctx thrift.Context, request *admin.DestinationsUpdatedRequest) (err error) {
	defer atomic.AddInt32(&h.loadShutdownRef, -1)
	sw := h.m3Client.StartTimer(metrics.DestinationsUpdatedScope, metrics.InputhostLatencyTimer)
	defer sw.Stop()
	h.m3Client.IncCounter(metrics.DestinationsUpdatedScope, metrics.InputhostRequests)
	// If we are already shutting down, no need to do anything here
	if atomic.AddInt32(&h.loadShutdownRef, 1) <= 0 {
		h.logger.WithField(common.TagReconfigureID, common.FmtReconfigureID(request.GetUpdateUUID())).Error("inputhost: DestinationsUpdated: dropping reconfiguration due to shutdown")
		h.m3Client.IncCounter(metrics.DestinationsUpdatedScope, metrics.InputhostFailures)
		return ErrHostShutdown
	}

	var intErr error
	updateUUID := request.GetUpdateUUID()
	h.logger.WithField(common.TagReconfigureID, common.FmtReconfigureID(updateUUID)).
		Debug("inputhost: DestinationsUpdated: processing reconfiguration")
	// Find all the updates we have and do the right thing
	for _, req := range request.Updates {
		// get the destUUID and see if it is in the inputhost cache
		destUUID := req.GetDestinationUUID()
		pathCache, ok := h.getPathCacheByDestUUID(destUUID)
		if ok {
			// We have a path cache loaded
			// check if it is active or not
			if pathCache.isActiveNoLock() {
				// reconfigure the cache by letting the path cache know about this request
				pathCache.reconfigureCh <- inReconfigInfo{req: req, updateUUID: updateUUID}
			} else {
				intErr = errPathCacheUnloading
			}
		} else {
			intErr = &cherami.EntityNotExistsError{}
		}

		// just save the error and proceed to the next update
		if intErr != nil {
			err = intErr
			h.m3Client.IncCounter(metrics.DestinationsUpdatedScope, metrics.InputhostFailures)
			h.logger.WithFields(bark.Fields{
				common.TagDst:           common.FmtDst(destUUID),
				common.TagReconfigureID: common.FmtReconfigureID(updateUUID),
				common.TagErr:           intErr,
			}).Error("inputhost: DestinationsUpdated: dropping reconfiguration")
		}
	}

	h.logger.WithField(common.TagReconfigureID, common.FmtReconfigureID(updateUUID)).
		Debug("inputhost: DestinationsUpdated: finished reconfiguration")

	return
}

// UnloadDestinations is the API used to unload destination to clear the cache
func (h *InputHost) UnloadDestinations(ctx thrift.Context, request *admin.UnloadDestinationsRequest) (err error) {
	defer atomic.AddInt32(&h.loadShutdownRef, -1)
	sw := h.m3Client.StartTimer(metrics.UnloadDestinationsScope, metrics.InputhostLatencyTimer)
	defer sw.Stop()
	h.m3Client.IncCounter(metrics.UnloadDestinationsScope, metrics.InputhostRequests)
	// If we are already shutting down, no need to do anything here
	if atomic.AddInt32(&h.loadShutdownRef, 1) <= 0 {
		h.logger.Error("not unloading the path cache; inputHost already shutdown")
		h.m3Client.IncCounter(metrics.UnloadDestinationsScope, metrics.InputhostFailures)
		return ErrHostShutdown
	}

	for _, destUUID := range request.DestUUIDs {
		h.pathMutex.RLock()
		pathCache, ok := h.pathCache[destUUID]
		if ok {
			pathCache.Lock()
			pathCache.prepareForUnload()
			go pathCache.unload()
			pathCache.Unlock()
		} else {
			h.logger.WithField(common.TagDst, common.FmtDst(destUUID)).
				Error("destination is not cached at all")
			err = ErrDstNotLoaded
			h.m3Client.IncCounter(metrics.UnloadDestinationsScope, metrics.InputhostFailures)
		}
		h.pathMutex.RUnlock()
	}

	return err
}

// ListLoadedDestinations is the API used to list all the loaded destinations in memory
func (h *InputHost) ListLoadedDestinations(ctx thrift.Context) (result *admin.ListLoadedDestinationsResult_, err error) {
	defer atomic.AddInt32(&h.loadShutdownRef, -1)
	sw := h.m3Client.StartTimer(metrics.ListLoadedDestinationsScope, metrics.InputhostLatencyTimer)
	defer sw.Stop()
	h.m3Client.IncCounter(metrics.ListLoadedDestinationsScope, metrics.InputhostRequests)
	// If we are already shutting down, no need to do anything here
	if atomic.AddInt32(&h.loadShutdownRef, 1) <= 0 {
		h.logger.Error("inputHost already shutdown")
		h.m3Client.IncCounter(metrics.ListLoadedDestinationsScope, metrics.InputhostFailures)
		return nil, ErrHostShutdown
	}

	result = admin.NewListLoadedDestinationsResult_()
	h.pathMutex.RLock()
	result.Dests = make([]*admin.Destinations, len(h.pathCache))
	count := 0
	for destUUID, pathCache := range h.pathCache {
		destRes := admin.NewDestinations()
		destRes.DestUUID = common.StringPtr(destUUID)
		destRes.DestPath = common.StringPtr(pathCache.destinationPath)

		result.Dests[count] = destRes
		count++
	}
	h.pathMutex.RUnlock()

	return result, err
}

// ReadDestState is the API used to read the state of the destination which is loaded on this inputhost
func (h *InputHost) ReadDestState(ctx thrift.Context, request *admin.ReadDestinationStateRequest) (result *admin.ReadDestinationStateResult_, err error) {
	defer atomic.AddInt32(&h.loadShutdownRef, -1)
	sw := h.m3Client.StartTimer(metrics.ReadDestStateScope, metrics.InputhostLatencyTimer)
	defer sw.Stop()
	h.m3Client.IncCounter(metrics.ReadDestStateScope, metrics.InputhostRequests)
	// If we are already shutting down, no need to do anything here
	if atomic.AddInt32(&h.loadShutdownRef, 1) <= 0 {
		h.logger.Error("inputHost already shutdown")
		h.m3Client.IncCounter(metrics.ReadDestStateScope, metrics.InputhostFailures)
		return nil, ErrHostShutdown
	}

	result = admin.NewReadDestinationStateResult_()

	result.InputHostUUID = common.StringPtr(h.GetHostUUID())
	result.DestState = make([]*admin.DestinationState, 0)

	// Now populate all destination state
	h.pathMutex.RLock()
	for _, destUUID := range request.DestUUIDs {
		pathCache, ok := h.pathCache[destUUID]
		if ok {
			destState := pathCache.getState()
			result.DestState = append(result.DestState, destState)
		} else {
			h.logger.WithField(common.TagDst, common.FmtDst(destUUID)).
				Warn("destination is not cached at all")
			err = ErrDstNotLoaded
		}
	}
	h.pathMutex.RUnlock()

	if err != nil {
		// update the failure metric
		h.m3Client.IncCounter(metrics.ReadDestStateScope, metrics.InputhostFailures)
	}
	return result, err

}

// determine how long we need to wait for the wait group
// if the thrift context timeout is set and is smaller than the default timeout, we
// should use that.
func getDrainTimeout(ctx thrift.Context) time.Duration {
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			return deadline.Sub(time.Now())
		}
	}
	return defaultDrainTimeout
}

// DrainExtent is the implementation of the thrift handler for the inputhost
func (h *InputHost) DrainExtent(ctx thrift.Context, request *admin.DrainExtentsRequest) (err error) {
	defer atomic.AddInt32(&h.loadShutdownRef, -1)
	sw := h.m3Client.StartTimer(metrics.DrainExtentsScope, metrics.InputhostLatencyTimer)
	defer sw.Stop()
	h.m3Client.IncCounter(metrics.DrainExtentsScope, metrics.InputhostRequests)
	// If we are already shutting down, no need to do anything here
	if atomic.AddInt32(&h.loadShutdownRef, 1) <= 0 {
		h.logger.WithField(common.TagReconfigureID, common.FmtReconfigureID(request.GetUpdateUUID())).Error("inputhost: DrainExtent: dropping due to shutdown")
		h.m3Client.IncCounter(metrics.DrainExtentsScope, metrics.InputhostFailures)
		return ErrHostShutdown
	}

	var intErr error
	updateUUID := request.GetUpdateUUID()
	h.logger.WithField(common.TagReconfigureID, common.FmtReconfigureID(updateUUID)).
		Debug("inputhost: DrainExtent: processing drain")
	timeoutTime := getDrainTimeout(ctx)
	var drainWG sync.WaitGroup
	var extentUUID string
	// Find all the extents we have and do the right thing
	for _, req := range request.Extents {
		// get the destUUID and see if it is in the inputhost cache
		destUUID := req.GetDestinationUUID()
		pathCache, ok := h.getPathCacheByDestUUID(destUUID)
		if ok {
			// We have a path cache loaded
			// check if it is active or not
			extentUUID = req.GetExtentUUID()
			if pathCache.isActiveNoLock() {
				drainWG.Add(1)
				go pathCache.drainExtent(extentUUID, updateUUID, &drainWG, timeoutTime)
			} else {
				intErr = errPathCacheUnloading
			}
		} else {
			intErr = &cherami.EntityNotExistsError{}
		}

		// just save the error and proceed to the next update
		if intErr != nil {
			err = intErr
			h.m3Client.IncCounter(metrics.DrainExtentsScope, metrics.InputhostFailures)
			h.logger.WithFields(bark.Fields{
				common.TagDst:           common.FmtDst(destUUID),
				common.TagExt:           common.FmtDst(extentUUID),
				common.TagReconfigureID: common.FmtReconfigureID(updateUUID),
				common.TagErr:           intErr,
			}).Error("inputhost: DrainExtent: dropping reconfiguration")
		}
	}

	h.logger.WithField(common.TagReconfigureID, common.FmtReconfigureID(updateUUID)).
		Debug("inputhost: DrainExtent: finished reconfiguration")
	if ok := common.AwaitWaitGroup(&drainWG, timeoutTime); !ok {
		err = ErrDrainTimedout
		h.m3Client.IncCounter(metrics.DrainExtentsScope, metrics.InputhostFailures)
		h.logger.WithFields(bark.Fields{
			common.TagReconfigureID: common.FmtReconfigureID(updateUUID),
			common.TagErr:           err,
		}).Error("inputhost: DrainExtent: timed out")
	}

	return
}

func (h *InputHost) reportHostMetric(reporter common.LoadReporter, intervalSecs int64) int64 {
	msgsInPerSec := h.hostMetrics.GetAndReset(load.HostMetricMsgsIn) / intervalSecs
	// We just report the delta for the bytes in counter. so get the value and
	// reset it.
	bytesInSinceLastReport := h.hostMetrics.GetAndReset(load.HostMetricBytesIn)

	hostMetrics := controller.NodeMetrics{
		NumberOfActiveExtents:   common.Int64Ptr(h.hostMetrics.Get(load.HostMetricNumOpenExtents)),
		NumberOfConnections:     common.Int64Ptr(h.hostMetrics.Get(load.HostMetricNumOpenConns)),
		IncomingMessagesCounter: common.Int64Ptr(msgsInPerSec),
		IncomingBytesCounter:    common.Int64Ptr(bytesInSinceLastReport),
		NodeStatus:              common.NodeStatusPtr(h.GetNodeStatus()),
	}

	reporter.ReportHostMetric(hostMetrics)
	return *(hostMetrics.NumberOfConnections)
}

// Report is the implementation for reporting host specific load to controller
func (h *InputHost) Report(reporter common.LoadReporter) {

	now := time.Now().UnixNano()
	intervalSecs := (now - h.lastLoadReportedTime) / int64(time.Second)
	if intervalSecs < 1 {
		return
	}

	numConns := h.reportHostMetric(reporter, intervalSecs)
	h.lastLoadReportedTime = now

	// Also update the metrics reporter to make sure the connection gauge is incremented
	h.m3Client.UpdateGauge(metrics.PubConnectionStreamScope, metrics.InputhostPubConnection, numConns)
}

// GetHostConnLimitPerSecond gets the host connection limit for inputhost
func (h *InputHost) GetHostConnLimitPerSecond() int {
	return int(atomic.LoadInt32(&h.hostConnLimitPerSecond))
}

// SetHostConnLimitPerSecond sets the rate at which this host can accept conns
func (h *InputHost) SetHostConnLimitPerSecond(connLimit int32) {
	h.logger.WithField(`val`, connLimit).Info(`SetHostConnLimitPerSecond`)
	atomic.StoreInt32(&h.hostConnLimitPerSecond, connLimit)
	h.SetTokenBucketValue(int32(connLimit))
}

// GetExtMsgsLimitPerSecond gets the rate limit for per extent per second
func (h *InputHost) GetExtMsgsLimitPerSecond() int {
	return int(atomic.LoadInt32(&h.extMsgsLimitPerSecond))
}

// SetExtMsgsLimitPerSecond sets the rate limit for per extent per second
func (h *InputHost) SetExtMsgsLimitPerSecond(connLimit int32) {
	h.logger.WithField(`val`, connLimit).Info(`SetExtMsgsLimitPerSecond`)
	atomic.StoreInt32(&h.extMsgsLimitPerSecond, connLimit)
	h.updateExtTokenBucket(int32(connLimit))
}

// GetConnMsgsLimitPerSecond gets the rate limit for per connection per second
func (h *InputHost) GetConnMsgsLimitPerSecond() int {
	return int(atomic.LoadInt32(&h.connMsgsLimitPerSecond))
}

// SetConnMsgsLimitPerSecond sets the rate limit for per connection per second
func (h *InputHost) SetConnMsgsLimitPerSecond(connLimit int32) {
	h.logger.WithField(`val`, connLimit).Info(`SetConnMsgsLimitPerSecond`)
	atomic.StoreInt32(&h.connMsgsLimitPerSecond, connLimit)
	h.updateConnTokenBucket(int32(connLimit))
}

// GetTokenBucketValue gets token bucket for hostConnLimitPerSecond
func (h *InputHost) GetTokenBucketValue() common.TokenBucket {
	return h.tokenBucketValue.Load().(common.TokenBucket)
}

// SetTokenBucketValue sets token bucket for hostConnLimitPerSecond
func (h *InputHost) SetTokenBucketValue(connLimit int32) {
	h.logger.WithField(`val`, connLimit).Info(`SetTokenBucketValue`)
	tokenBucket := common.NewTokenBucket(int(connLimit), common.NewRealTimeSource())
	h.tokenBucketValue.Store(tokenBucket)
}

// SetHostConnLimit sets the conn limit for this host
func (h *InputHost) SetHostConnLimit(connLimit int32) {
	h.logger.WithField(`val`, connLimit).Info(`SetHostConnLimit`)
	atomic.StoreInt32(&h.hostConnLimit, connLimit)
}

// GetHostConnLimitOverall gets the host connection limit for inputhost
func (h *InputHost) GetHostConnLimitOverall() int {
	return int(atomic.LoadInt32(&h.hostConnLimit))
}

// SetMaxConnPerDest sets the max connection limit per destination
func (h *InputHost) SetMaxConnPerDest(connLimit int32) {
	h.logger.WithField(`val`, connLimit).Info(`SetMaxConnPerDest`)
	atomic.StoreInt32(&h.maxConnLimit, connLimit)
}

// GetMaxConnPerDest gets the max connection limit per destination
func (h *InputHost) GetMaxConnPerDest() int {
	return int(atomic.LoadInt32(&h.maxConnLimit))
}

// GetNumConnections is the number of connections on this host
func (h *InputHost) GetNumConnections() int {
	return int(h.hostMetrics.Get(load.HostMetricNumOpenConns))
}

// GetNodeStatus is the current status of this host
func (h *InputHost) GetNodeStatus() controller.NodeStatus {
	return h.nodeStatus.Load().(controller.NodeStatus)
}

// SetNodeStatus sets the status of this host
func (h *InputHost) SetNodeStatus(status controller.NodeStatus) {
	h.logger.WithField(`val`, status).Info(`SetNodeStatus`)
	h.nodeStatus.Store(status)
}

// SetTestShortExtentsByPath sets path override that enables testing short extents
func (h *InputHost) SetTestShortExtentsByPath(override string) {
	h.logger.WithField(`val`, override).Info(`SetTestShortExtentsByPath`)
	h.testShortExtentsByPath = override
}

// GetTestShortExtentsByPath gets path override that enables testing short extents
func (h *InputHost) GetTestShortExtentsByPath() (override string) {
	return h.testShortExtentsByPath
}

// Shutdown shutsdown all the InputHost cleanly
func (h *InputHost) Shutdown() {
	// make sure we have atleast loaded everything
	atomic.AddInt32(&h.loadShutdownRef, -0x80000000)
	for atomic.LoadInt32(&h.loadShutdownRef) > -0x80000000 {
		time.Sleep(time.Second)
	}
	// close all open streams
	h.unloadAll()
	close(h.shutdown)
	h.shutdownWG.Wait()
}

// Stop stops the service
func (h *InputHost) Stop() {
	h.loadReporter.Stop()
	h.hostIDHeartbeater.Stop()
	h.SCommon.Stop()
}

// Start starts the inputhost service
func (h *InputHost) Start(thriftService []thrift.TChanServer) {
	h.SCommon.Start(thriftService)
	h.hostIDHeartbeater = common.NewHostIDHeartbeater(h.mClient, h.GetHostUUID(), h.GetHostPort(), h.GetHostName(), h.logger)
	h.hostIDHeartbeater.Start()
	h.loadReporter = h.GetLoadReporterDaemonFactory().CreateReporter(hostLoadReportingInterval, h, h.logger)
	h.loadReporter.Start()
	// Add the IP tag as well
	h.logger = h.logger.WithField(common.TagHostIP, common.FmtHostIP(h.SCommon.GetHostPort()))
}

// RegisterWSHandler is the implementation of WSService interface
func (h *InputHost) RegisterWSHandler() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc(fmt.Sprintf(ccommon.HTTPHandlerPattern, ccommon.EndpointOpenPublisherStream), h.OpenPublisherStreamHandler)
	return mux
}

// UpgradeHandler implements the upgrade end point
func (h *InputHost) UpgradeHandler(w http.ResponseWriter, r *http.Request) {
	h.logger.Info("Upgrade endpoint called on inputhost")
	h.SetNodeStatus(controller.NodeStatus_GOING_DOWN)
	reporter := h.loadReporter.GetReporter()
	// report as down
	go h.reportHostMetric(reporter, 1)
	// start draining everything
	h.drainAll()
	// at this point, we have marked ourself as down and drained everything. Exit since we are no longer useful
	os.Exit(0)
}

// NewInputHost is the constructor for BIn
func NewInputHost(serviceName string, sVice common.SCommon, mClient metadata.TChanMetadataService, opts *InOptions) (*InputHost, []thrift.TChanServer) {

	// Get the deployment name for logger field
	deploymentName := sVice.GetConfig().GetDeploymentName()
	bs := InputHost{
		logger:               (sVice.GetConfig().GetLogger()).WithFields(bark.Fields{common.TagIn: common.FmtIn(sVice.GetHostUUID()), common.TagDplName: common.FmtDplName(deploymentName)}),
		SCommon:              sVice,
		pathCache:            make(map[string]*inPathCache),
		pathCacheByDestPath:  make(map[string]string), // simple map which just resolves the path to uuid
		cacheTimeout:         defaultIdleTimeout,
		shutdown:             make(chan struct{}),
		hostMetrics:          load.NewHostMetrics(),
		lastLoadReportedTime: time.Now().UnixNano(),
	}

	// Set the host limits from the common package
	// TODO: once limits are moved behind an interface we can change this
	// this is mainly exposed via getters and setters for testing
	bs.SetHostConnLimit(int32(common.HostOverallConnLimit))
	bs.SetHostConnLimitPerSecond(int32(common.HostPerSecondConnLimit))
	bs.SetMaxConnPerDest(int32(common.HostMaxConnPerDestination))

	// create the token bucket for this host
	bs.SetTokenBucketValue(int32(bs.GetHostConnLimitPerSecond()))

	bs.m3Client = metrics.NewClient(sVice.GetMetricsReporter(), metrics.Inputhost)
	if opts != nil {
		bs.cacheTimeout = opts.CacheIdleTimeout
	}

	bs.mClient = mm.NewMetadataMetricsMgr(mClient, bs.m3Client, bs.logger)

	// manage uconfig, regiester handerFunc and verifyFunc for uConfig values
	bs.dConfigClient = sVice.GetDConfigClient()
	bs.dynamicConfigManage()
	bs.SetNodeStatus(controller.NodeStatus_UP)
	return &bs, []thrift.TChanServer{cherami.NewTChanBInServer(&bs), admin.NewTChanInputHostAdminServer(&bs)}
}

func (h *InputHost) incFailureCounter(metricsScope int, c metrics.ErrorClass) {
	switch c {
	case metrics.UserError:
		h.m3Client.IncCounter(metricsScope, metrics.InputhostUserFailures)
	default:
		h.m3Client.IncCounter(metricsScope, metrics.InputhostInternalFailures)
	}
}
