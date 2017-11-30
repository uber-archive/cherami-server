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
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"github.com/Sirupsen/logrus"
	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"

	"github.com/Shopify/sarama"
	ccommon "github.com/uber/cherami-client-go/common"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	cassDconfig "github.com/uber/cherami-server/common/dconfig"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
	mm "github.com/uber/cherami-server/common/metadata"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/outputhost/load"
	"github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	ccherami "github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

const (
	// defaultIdleTimeout is the time to wait before we close all streams
	defaultIdleTimeout        = 10 * time.Minute
	defaultPrefetchBufferSize = 1000 // XXX: find the optimal prefetch buffer size
	defaultUnloadChSize       = 50
	defaultAckMgrMapChSize    = 500
	defaultAckMgrIDStartFrom  = 0                // the default ack mgr id for this host to start from
	defaultMaxConnLimitPerCg  = 1000             // default max connections per CG per host
	hostLoadReportingInterval = 2 * time.Second  // interval at which output host load is reported to controller
	msgCacheWriteTimeout      = 10 * time.Minute // timeout to write to message cache. Intentionally set to a large number
	// because if we don't write to cache, client won't be able to ack the message
)

var thisOutputHost *OutputHost

type (
	// OutputHost is the main server class for OutputHosts
	OutputHost struct {
		logger             bark.Logger
		loadShutdownRef    int32
		cgMutex            sync.RWMutex
		cgCache            map[string]*consumerGroupCache
		shutdownWG         sync.WaitGroup
		cacheTimeout       time.Duration
		metaClient         metadata.TChanMetadataService
		frontendClient     ccherami.TChanBFrontend
		hostIDHeartbeater  common.HostIDHeartbeater
		loadReporter       common.LoadReporterDaemon
		shutdownCh         chan struct{}
		unloadCacheCh      chan string
		m3Client           metrics.Client
		dClient            dconfig.Client
		numConsConn        int32                  // number of active pubConnection
		ackMgrMap          map[uint32]*ackManager // map of all the ack managers on this output
		ackMgrMutex        sync.RWMutex           // mutex protecting the above map
		sessionID          uint16
		ackMgrIDGen        common.HostAckIDGenerator // this is the interface used to generate ackIDs for this host
		ackMgrLoadCh       chan ackMgrLoadMsg
		ackMgrUnloadCh     chan uint32
		hostMetrics        *load.HostMetrics
		cfgMgr             cassDconfig.ConfigManager
		kafkaCfg           configure.CommonKafkaConfig
		kafkaStreamFactory KafkaMessageConverterFactory
		common.SCommon
	}

	// OutOptions is the options used during instantiating a new host
	OutOptions struct {
		//CacheIdleTimeout
		CacheIdleTimeout time.Duration
		//KStreamFactory
		KStreamFactory KafkaMessageConverterFactory
	}

	ackMgrLoadMsg struct {
		ackMgrID uint32
		ackMgr   *ackManager
	}
)

// interface implementation check
var _ cherami.TChanBOut = &OutputHost{}
var _ admin.TChanOutputHostAdmin = &OutputHost{}
var _ common.WSService = &OutputHost{}

// ErrHostShutdown is returned when the host is already shutdown
var ErrHostShutdown = &cherami.InternalServiceError{Message: "OutputHost already shutdown"}

// ErrCgAlreadyUnloaded is returned when the cg is already unloaded
var ErrCgAlreadyUnloaded = &cherami.InternalServiceError{Message: "Consumer Group already unloaded"}

// ErrLimit is returned when the overall limit for the CG is violated
var ErrLimit = &cherami.InternalServiceError{Message: "Outputhost cons connection limit exceeded"}

func (h *OutputHost) getConsumerGroup(ctx thrift.Context, path string, cgName string, rejectDisabled bool) (*shared.ConsumerGroupDescription, metrics.ErrorClass, error) {
	mGetRequest := shared.ReadConsumerGroupRequest{
		DestinationPath:   common.StringPtr(path),
		ConsumerGroupName: common.StringPtr(cgName),
	}
	cgDesc, err := h.metaClient.ReadConsumerGroup(ctx, &mGetRequest)

	if cgDesc != nil && err == nil {
		if rejectDisabled {
			if cgDesc.GetStatus() != shared.ConsumerGroupStatus_ENABLED {
				return nil, metrics.UserError, cherami.NewEntityDisabledError()
			}
		}

		return cgDesc, metrics.NoError, nil
	}

	return nil, common.ClassifyErrorByType(err), err
}

func (h *OutputHost) createConsumerGroupCache(cgDesc shared.ConsumerGroupDescription, destPath string,
	cgLogger bark.Logger) (cgCache *consumerGroupCache, exists bool) {
	h.cgMutex.Lock()
	if cgCache, exists = h.cgCache[cgDesc.GetConsumerGroupUUID()]; !exists {
		cgCache = newConsumerGroupCache(destPath, cgDesc, cgLogger, h)
		h.cgCache[cgDesc.GetConsumerGroupUUID()] = cgCache
	}
	h.cgMutex.Unlock()
	return
}

// loadPath loads the extent stuff into the respective caches and opens up the replica stream
func (h *OutputHost) createAndLoadCGCache(ctx thrift.Context, cgDesc shared.ConsumerGroupDescription, destPath string,
	cgLogger bark.Logger) (*consumerGroupCache, error) {
	// before loading the CGCache check if we are already in the process of shuttingdown
	if atomic.AddInt32(&h.loadShutdownRef, 1) <= 0 {
		cgLogger.Warn("not loading the CG cache outputHost already shutdown")
		return nil, ErrHostShutdown
	}

	// First make sure we have the path cached.
	cgCache, exists := h.createConsumerGroupCache(cgDesc, destPath, cgLogger)

	return h.loadCGCache(ctx, exists, cgCache)
}

// loadCGCache is the routine which loads the Consumer Group cache and also all the extents
// belonging to this Consumer Group by contacting the metadata
// The "exists" bool flag is used to spawn the cache management routines just once
func (h *OutputHost) loadCGCache(ctx thrift.Context, exists bool, cgCache *consumerGroupCache) (*consumerGroupCache, error) {
	// load the cache
	err := cgCache.loadConsumerGroupCache(ctx, exists)
	if err != nil {

		cgCache.logger.Error("outhost: error loading the CG cache")

		// remove this from the outputhost cache, if this was created new as well
		if !exists && err != ErrCgUnloaded {
			cgCache.logger.Warn("outputhost: unloading consumer group cache.")
			go cgCache.unloadConsumerGroupCache()
		}
		return nil, err
	}
	return cgCache, nil
}

func (h *OutputHost) getAckMgr(ackMgrID uint16) (ackmgr *ackManager) {
	// get the ackMgr from the map
	h.ackMgrMutex.RLock()
	ackmgr, _ = h.ackMgrMap[uint32(ackMgrID)]
	h.ackMgrMutex.RUnlock()

	return
}

// OpenConsumerStreamHandler is websocket handler for opening consumer stream
func (h *OutputHost) OpenConsumerStreamHandler(w http.ResponseWriter, r *http.Request) {

	// get parameters from header
	path := r.Header.Get("path")
	if len(path) == 0 {
		err := &cherami.BadRequestError{Message: `please set path as part of the header`}
		h.logger.WithField(common.TagErr, err).Error("OpenConsumerStreamHandler failed")
		h.incFailureCounter(metrics.OpenConsumerStreamScope, metrics.UserError)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cgName := r.Header.Get("consumerGroupName")
	if len(cgName) == 0 {
		err := &cherami.BadRequestError{Message: `please set consumerGroupName as part of the header`}
		h.logger.WithField(common.TagErr, err).Error("OpenConsumerStreamScope failed")
		h.incFailureCounter(metrics.OpenConsumerStreamScope, metrics.UserError)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// setup websocket
	wsStream, err := h.GetWSConnector().AcceptConsumerStream(w, r)
	if err != nil {
		h.logger.WithFields(bark.Fields{
			common.TagDstPth: common.FmtDstPth(path),
			common.TagCnsPth: common.FmtCnsPth(cgName),
			common.TagErr:    err,
		}).Error("unable to upgrade websocket connection")
		h.incFailureCounter(metrics.OpenConsumerStreamScope, metrics.InternalError)
		return
	}

	// create fake thrift context with header
	ctx, cancel := thrift.NewContext(common.MaxDuration)
	defer cancel()
	ctx = thrift.WithHeaders(ctx, map[string]string{
		"path":              path,
		"consumerGroupName": cgName,
	})

	// create thrift stream call wrapper and deligate to streaming call
	h.OpenConsumerStream(ctx, wsStream)
}

// OpenConsumerStream is the implementation of the thrift handler for the Out service
// TODO:  find remote "host" from the context as a tag (pass to newConsConnection)
func (h *OutputHost) OpenConsumerStream(ctx thrift.Context, call stream.BOutOpenConsumerStreamInCall) error {
	h.m3Client.IncCounter(metrics.OpenConsumerStreamScope, metrics.OutputhostRequests)
	// get the path from the headers
	path, ok := ctx.Headers()["path"]
	if !ok {
		err := &cherami.BadRequestError{Message: `please set path as part of the header`}
		h.logger.WithField(common.TagErr, err).Error("OpenConsumerStream failed")
		h.incFailureCounter(metrics.OpenConsumerStreamScope, metrics.UserError)
		call.Done()
		return err
	}

	cgName, ok := ctx.Headers()["consumerGroupName"]
	if !ok {
		err := &cherami.BadRequestError{Message: `please set consumerGroupName as part of the header`}
		h.logger.WithField(common.TagErr, err).Error("OpenConsumerStream failed")
		h.incFailureCounter(metrics.OpenConsumerStreamScope, metrics.UserError)
		call.Done()
		return err
	}
	// Create a logger with destinationPath and consumerGroupName tags
	cgLogger := h.logger.WithFields(bark.Fields{
		common.TagDstPth: common.FmtDstPth(path),
		common.TagCnsPth: common.FmtCnsPth(cgName),
		// TODO: add "host name" tag, extract from context
	})

	cgDesc, errC, err := h.getConsumerGroup(ctx, path, cgName, false /*don't rejectDisabled*/)

	if err != nil || cgDesc == nil || len(cgDesc.GetDestinationUUID()) == 0 || len(cgDesc.GetConsumerGroupUUID()) == 0 {
		cgLogger.WithField(common.TagErr, err).Error(`error translating dest/cg name to uuid`)
		call.Done()
		h.incFailureCounter(metrics.OpenConsumerStreamScope, errC)
		return err
	}

	// load the CG and all the extents for this CG
	cgLogger = cgLogger.WithFields(bark.Fields{
		common.TagCnsm: cgDesc.GetConsumerGroupUUID(),
		common.TagDst:  cgDesc.GetDestinationUUID(),
	})

	cgCache, err := h.createAndLoadCGCache(ctx, *cgDesc, path, cgLogger)
	if err != nil {
		cgLogger.WithField(common.TagErr, err).Error(`error loading consumer group`)
		// putback the load ref which we got in the createAndLoadCGCache
		atomic.AddInt32(&h.loadShutdownRef, -1)
		call.Done()
		h.incFailureCounter(metrics.OpenConsumerStreamScope, errC)
		return err
	}

	cgCache.extMutex.Lock()
	if h.IsLimitsEnabled() && (cgCache.getNumOpenConns() > defaultMaxConnLimitPerCg) {
		cgCache.extMutex.Unlock()
		cgLogger.WithFields(bark.Fields{
			common.TagHostConnLimit: common.FmtHostConnLimit(int(cgCache.getNumOpenConns())),
			common.TagErr:           ErrLimit,
		}).Error("per-cg limit exceeded; rejecting connection")
		// put back the loadShutdownRef
		atomic.AddInt32(&h.loadShutdownRef, -1)
		// stop the stream before returning
		call.Done()
		h.incFailureCounter(metrics.OpenConsumerStreamScope, metrics.UserError)
		return ErrLimit
	}
	conn := newConsConnection(cgCache.currID, cgCache, call, h.cacheTimeout, cgLogger)
	cgCache.connections[cgCache.currID] = conn
	connWG := conn.open()
	cgCache.currID++
	// increment the active connection count
	cgCache.incNumOpenConns(1)
	cgCache.extMutex.Unlock()
	// putback the load ref which we got in the createAndLoadCGCache
	atomic.AddInt32(&h.loadShutdownRef, -1)
	// update active consconnection gauge for m3 metrics

	h.hostMetrics.Increment(load.HostMetricNumOpenConns)

	// wait till the conn is closed. we cannot return immediately.
	// If we do so, we will get data races reading/writing from/to the stream
	connWG.Wait()
	h.hostMetrics.Decrement(load.HostMetricNumOpenConns)
	return nil
}

func (h *OutputHost) processAcks(ackIds []string, isNack bool) (invalidIDs []string) {
	for _, ackIDStr := range ackIds {
		ackID := AckID(ackIDStr)

		// parse the ackID to get everything we need
		// if we fail to even parse the ackID we definitely need to
		// return an error
		ackIDObj, err := common.AckIDFromString(string(ackID))
		if err != nil {
			h.logger.WithFields(bark.Fields{
				common.TagErr:   fmt.Sprintf("%v", err),
				common.TagAckID: ackIDStr,
			}).Error("error parsing the ackID")
			invalidIDs = append(invalidIDs, ackIDStr)
			if !isNack {
				h.m3Client.IncCounter(metrics.AckMessagesScope, metrics.OutputhostMessageAckFailures)
			} else {
				h.m3Client.IncCounter(metrics.AckMessagesScope, metrics.OutputhostMessageNackFailures)
			}
			continue
		}

		// At this point, we were able to parse the AckID but we could get
		// duplicate acks or acks after a restart of the host which means
		// we don't need to error out.
		// Deconstruct the combined id to get the individual values
		sessionID, ackMgrID, seqNum := ackIDObj.MutatedID.DeconstructCombinedID()

		if sessionID != h.sessionID {
			h.logger.WithFields(bark.Fields{
				"OutputSessionID": fmt.Sprintf("%v", h.sessionID),
				"ackSessionID":    fmt.Sprintf("%v", sessionID),
			}).Error("ackID intended for a different outputhost")
			h.m3Client.IncCounter(metrics.AckMessagesScope, metrics.OutputhostMessageDiffSession)
			continue
		}

		// find the ackMgr
		ackMgr := h.getAckMgr(ackMgrID)
		if ackMgr == nil {
			h.logger.WithFields(bark.Fields{
				common.TagAckID: ackIDStr,
				`ackMgrID`:      ackMgrID,
			}).Info("processAcks could not get ack manager (probably extent is consumed)")
			h.m3Client.IncCounter(metrics.AckMessagesScope, metrics.OutputhostMessageNoAckManager)
			continue
		}

		// let the ackMgr know; from the perspective of the ackManager, ack == nack
		if err = ackMgr.acknowledgeMessage(ackID, ackIndex(seqNum), ackIDObj.Address, isNack); err == nil {
			continue
		} else {
			h.logger.WithFields(bark.Fields{
				common.TagErr:   fmt.Sprintf("%v", err),
				common.TagAckID: ackIDStr,
			}).Warn("processAcks ack manager returned error")
			h.m3Client.IncCounter(metrics.AckMessagesScope, metrics.OutputhostMessageAckManagerError)
		}
	}
	return
}

// AckMessages is the implementation of the thrift handler for the BOut service
func (h *OutputHost) AckMessages(ctx thrift.Context, ackRequest *cherami.AckMessagesRequest) error {

	// record the ack latency metric
	sw := h.m3Client.StartTimer(metrics.AckMessagesScope, metrics.OutputhostLatencyTimer)
	defer sw.Stop()
	h.m3Client.IncCounter(metrics.AckMessagesScope, metrics.OutputhostRequests)

	invalidAckIds := h.processAcks(ackRequest.AckIds, false /*Ack*/)
	invalidNackIds := h.processAcks(ackRequest.NackIds, true /*Nack*/)

	sw.Stop()
	if len(invalidAckIds) > 0 || len(invalidNackIds) > 0 {
		h.logger.WithFields(bark.Fields{
			"inv_ackid_count":  strconv.Itoa(len(invalidAckIds)),
			"inv_nackid_count": strconv.Itoa(len(invalidNackIds)),
		}).Warn("Invalid AckId or NackId returned")

		return &cherami.InvalidAckIdError{
			Message: "some ackIds not recognized",
			AckIds:  invalidAckIds,
			NackIds: invalidNackIds,
		}
	}
	return nil
}

// ReceiveMessageBatch is a thrift handler. It consumes messages from this output host within thrift context deadline. This is long-poll able.
func (h *OutputHost) ReceiveMessageBatch(ctx thrift.Context, request *cherami.ReceiveMessageBatchRequest) (*cherami.ReceiveMessageBatchResult_, error) {

	sw := h.m3Client.StartTimer(metrics.ReceiveMessageBatchOutputHostScope, metrics.OutputhostLatencyTimer)
	defer sw.Stop()
	h.m3Client.IncCounter(metrics.ReceiveMessageBatchOutputHostScope, metrics.OutputhostRequests)
	path := request.GetDestinationPath()
	cgName := request.GetConsumerGroupName()
	count := request.GetMaxNumberOfMessages()
	timeoutTime := time.Now().Add(time.Duration(request.GetReceiveTimeout()) * time.Second)
	deadline, hasDeadline := ctx.Deadline()
	// use the smaller timeout between thrift context and user ask
	if hasDeadline && deadline.Before(timeoutTime) {
		timeoutTime = deadline
	}

	lclLg := h.logger.WithFields(bark.Fields{
		common.TagDstPth: common.FmtDstPth(path),
		common.TagCnsPth: common.FmtCnsPth(cgName),
	})

	cgDesc, errC, err := h.getConsumerGroup(ctx, path, cgName, false /*don't rejectDisabled*/)
	if err != nil || cgDesc == nil || len(cgDesc.GetDestinationUUID()) == 0 || len(cgDesc.GetConsumerGroupUUID()) == 0 {
		lclLg.WithField(common.TagErr, err).Error(`Error translating to UUID with error`)
		h.incFailureCounter(metrics.ReceiveMessageBatchOutputHostScope, errC)
		return nil, err
	}

	// load the CG and all the extents for this CG
	lclLg = lclLg.WithField(common.TagCnsm, cgDesc.GetConsumerGroupUUID())
	cgCache, err := h.createAndLoadCGCache(ctx, *cgDesc, path, lclLg)
	// putback the load ref which we got in the createAndLoadCGCache
	defer atomic.AddInt32(&h.loadShutdownRef, -1)
	if err != nil {
		lclLg.WithField(common.TagErr, err).Error(`unable to load consumer group with error`)
		h.m3Client.IncCounter(metrics.ReceiveMessageBatchOutputHostScope, metrics.OutputhostFailures)
		return nil, err
	}

	cgCache.updateLastDisconnectTime()

	// make sure we don't close all the channels here
	cgCache.connsWG.Add(1) // this WG is for the cgCache
	defer cgCache.connsWG.Done()

	res := cherami.NewReceiveMessageBatchResult_()
	msgCacheWriteTicker := common.NewTimer(msgCacheWriteTimeout)
	defer msgCacheWriteTicker.Stop()
	deliver := func(msg *cherami.ConsumerMessage, msgCacheCh chan cacheMsg) {
		// send msg back to caller
		res.Messages = append(res.Messages, msg)

		// long poll consumers don't have a connectionID and we don't have anything to throttle here
		// using -1 as a special connection ID here,
		// XXX: move this to a const

		// Note: we use a very large time out here. Because if we won't write the message to the cache, client
		// won't be able to ack the message. This means client might already timeout while we're still waiting for
		// writing to message cache but this is the best we can do.
		msgCacheWriteTicker.Reset(msgCacheWriteTimeout)
		select {
		case msgCacheCh <- cacheMsg{msg: msg, connID: -1}:
		case <-cgCache.closeChannel:
			lclLg.WithField(common.TagAckID, common.FmtAckID(msg.GetAckId())).
				Error("outputhost: Unable to write the message to the cache because cg cache closing")
		case <-msgCacheWriteTicker.C:
			lclLg.WithField(common.TagAckID, common.FmtAckID(msg.GetAckId())).
				Error("outputhost: Unable to write the message to the cache because time out")
			h.m3Client.IncCounter(metrics.ReceiveMessageBatchOutputHostScope, metrics.OutputhostReceiveMsgBatchWriteToMsgCacheTimeout)
		}
	}

	// just timeout for long pull if no message available
	firstResultTimer := common.NewTimer(timeoutTime.Sub(time.Now()))
	defer firstResultTimer.Stop()
	select {
	case msg, ok := <-cgCache.msgsRedeliveryCh:
		if !ok {
			h.m3Client.IncCounter(metrics.ReceiveMessageBatchOutputHostScope, metrics.OutputhostFailures)
			return nil, ErrCgUnloaded
		}
		deliver(msg, cgCache.msgCacheRedeliveredCh)
	case msg, ok := <-cgCache.msgsCh:
		if !ok {
			h.m3Client.IncCounter(metrics.ReceiveMessageBatchOutputHostScope, metrics.OutputhostFailures)
			return nil, ErrCgUnloaded
		}
		deliver(msg, cgCache.msgCacheCh)
	case <-firstResultTimer.C:
		exception := cherami.NewTimeoutError()
		exception.Message = "Timeout getting next message, probably no message available at the moment."
		h.m3Client.IncCounter(metrics.ReceiveMessageBatchOutputHostScope, metrics.OutputhostLongPollingTimeOut)
		return nil, exception
	}

	// once there's any message, try get back to caller with as much as possbile messages before timeout
	moreResultTimer := common.NewTimer(timeoutTime.Sub(time.Now()))
	defer moreResultTimer.Stop()
MORERESULTLOOP:
	for remaining := count - 1; remaining > 0; remaining-- {
		select {
		case <-moreResultTimer.C:
			break MORERESULTLOOP
		default:
			select {
			case msg, ok := <-cgCache.msgsRedeliveryCh:
				if !ok {
					// the cg is already unloaded
					break MORERESULTLOOP
				}
				deliver(msg, cgCache.msgCacheRedeliveredCh)
			case msg, ok := <-cgCache.msgsCh:
				if !ok {
					// the cg is already unloaded
					break MORERESULTLOOP
				}
				deliver(msg, cgCache.msgCacheCh)
			case <-moreResultTimer.C:
				break MORERESULTLOOP
			}
		}
	}

	// Emit M3 metrics for per host and per consumer group

	h.m3Client.AddCounter(metrics.ReceiveMessageBatchOutputHostScope, metrics.OutputhostMessageSent, int64(len(res.Messages)))
	cgCache.consumerM3Client.AddCounter(metrics.ReceiveMessageBatchOutputHostCGScope, metrics.OutputhostCGMessageSent, int64(len(res.Messages)))

	return res, nil
}

// ConsumerGroupsUpdated is the API exposed to ExtentController to communicate any changes to current extent view
func (h *OutputHost) ConsumerGroupsUpdated(ctx thrift.Context, request *admin.ConsumerGroupsUpdatedRequest) (err error) {
	defer atomic.AddInt32(&h.loadShutdownRef, -1)
	sw := h.m3Client.StartTimer(metrics.ConsumerGroupsUpdatedScope, metrics.OutputhostLatencyTimer)
	defer sw.Stop()
	h.m3Client.IncCounter(metrics.ConsumerGroupsUpdatedScope, metrics.OutputhostRequests)
	// If we are already shutting down, no need to do anything here
	if atomic.AddInt32(&h.loadShutdownRef, 1) <= 0 {
		h.logger.Error("not loading the CG cache outputHost already shutdown")
		h.m3Client.IncCounter(metrics.ConsumerGroupsUpdatedScope, metrics.OutputhostFailures)
		return ErrHostShutdown
	}

	var intErr error

	updateUUID := request.GetUpdateUUID()

	h.logger.WithField(common.TagUpdateUUID, updateUUID).Debug("ConsumerGroupsUpdated: processing notification")

	// Find all the updates we have and do the right thing
	for _, req := range request.Updates {
		// get the CgUUID and see if it is in the outputhost cache
		cgUUID := req.GetConsumerGroupUUID()
		h.cgMutex.RLock()
		cg, ok := h.cgCache[cgUUID]
		h.cgMutex.RUnlock()
		if ok {

			// We have a consumer group cache loaded
			// we need to reload the cache, if the notification type is either
			// HOST or ALL. CGDesc is updated in refresh CG cache

			notifyType := req.GetType()

			h.logger.WithFields(bark.Fields{
				common.TagCnsm:       common.FmtCnsm(cgUUID),
				common.TagUpdateUUID: updateUUID,
				`notifyType`:         notifyType,
			}).Info("ConsumerGroupsUpdated: processing notification for consumer-group")

			switch notifyType {
			case admin.NotificationType_CLIENT:
				// Notify the client
				cg.reconfigureClients(updateUUID)
			case admin.NotificationType_HOST:
				// just refresh the cg directly.
				// at this point cg is already loaded
				intErr = cg.refreshCgCacheNoLock(ctx)
			case admin.NotificationType_ALL:
				// just refresh the cg directly.
				// at this point cg is already loaded
				intErr = cg.refreshCgCacheNoLock(ctx)
				if intErr == nil {
					cg.reconfigureClients(updateUUID)
				}
			default:
				h.logger.WithFields(bark.Fields{
					common.TagCnsm:       common.FmtCnsm(cgUUID),
					common.TagUpdateUUID: updateUUID,
					`notifyType`:         notifyType,
				}).Error(`ConsumerGroupsUpdated: unknown notification type`)
				intErr = &cherami.BadRequestError{}
			}
		} else {
			h.logger.WithFields(bark.Fields{
				common.TagCnsm:       common.FmtCnsm(cgUUID),
				common.TagUpdateUUID: updateUUID,
				`notifyType`:         req.GetType(),
			}).Error("ConsumerGroupsUpdated: cgCache does not exist")
			intErr = &cherami.EntityNotExistsError{}
		}

		// just save the error and proceed to the next update
		if intErr != nil {
			err = intErr
			h.m3Client.IncCounter(metrics.ConsumerGroupsUpdatedScope, metrics.OutputhostFailures)
		}
	}
	return err
}

// unloadAll tries to unload everything on this OutputHost
func (h *OutputHost) unloadAll() {
	h.cgMutex.Lock()
	for cgUUID, cgCache := range h.cgCache {
		h.logger.WithField(common.TagCnsm, cgUUID).Info("outputhost: closing streams for consumer group.")
		// unload in an async manner.
		// we already have the shutdownWG on cgCache. No need to do this in a synchronous manner
		go cgCache.unloadConsumerGroupCache()
	}
	h.cgMutex.Unlock()
}

// Shutdown shutsdown all the OutputHost cleanly
func (h *OutputHost) Shutdown() {
	// Shutdown Order
	// 1. Wait for ongoing load to complete (shutdownRef)
	// 2. Close all open streams (by calling unloadAll) and wait for them to unload.
	// 3. Stop the manageCgCache routine
	// make sure we have at least loaded everything
	atomic.AddInt32(&h.loadShutdownRef, -0x80000000)
	for atomic.LoadInt32(&h.loadShutdownRef) > -0x80000000 {
		time.Sleep(time.Second)
	}
	// close all open streams
	h.unloadAll()
	h.shutdownWG.Wait()
	// stop the manageCgCache routine
	close(h.shutdownCh)
}

// Start starts the outputhost service
func (h *OutputHost) Start(thriftService []thrift.TChanServer) {
	h.SCommon.Start(thriftService)
	h.hostIDHeartbeater = common.NewHostIDHeartbeater(h.metaClient, h.GetHostUUID(), h.GetHostPort(), h.GetHostName(), h.logger)
	h.logger = h.logger.WithField(common.TagHostIP, common.FmtHostIP(h.SCommon.GetHostPort()))
	h.loadReporter = h.GetLoadReporterDaemonFactory().CreateReporter(hostLoadReportingInterval, h, h.logger)
	h.loadReporter.Start()
	h.hostIDHeartbeater.Start()
	h.cfgMgr.Start()
	go h.manageCgCache()
}

// manageCgCache is a very lightweight goroutine which either listens for
// unloadCacheCh to unload the cgUUID or
// listens for inserting the ackMgr into the Map or
// listens for shutdown
func (h *OutputHost) manageCgCache() {
	for {
		select {
		case cgUUID := <-h.unloadCacheCh:
			h.cgMutex.Lock()
			h.logger.WithField(common.TagCnsm, common.FmtCnsm(cgUUID)).
				Info("unloading consumer group")
			delete(h.cgCache, cgUUID)
			h.cgMutex.Unlock()
		case ackloadMsg := <-h.ackMgrLoadCh:
			h.ackMgrMutex.Lock()
			h.ackMgrMap[ackloadMsg.ackMgrID] = ackloadMsg.ackMgr
			h.ackMgrMutex.Unlock()
		case ackMgrID := <-h.ackMgrUnloadCh:
			h.ackMgrMutex.Lock()
			delete(h.ackMgrMap, ackMgrID)
			h.ackMgrMutex.Unlock()
		case <-h.shutdownCh:
			return
		}
	}
}

// Report is the implementation for reporting host specific load to controller
func (h *OutputHost) Report(reporter common.LoadReporter) {

	msgsOut := h.hostMetrics.GetAndReset(load.HostMetricMsgsOut)
	bytesOut := h.hostMetrics.GetAndReset(load.HostMetricBytesOut)
	numConns := h.hostMetrics.Get(load.HostMetricNumOpenConns)
	numExtents := h.hostMetrics.Get(load.HostMetricNumOpenExtents)

	hostMetrics := controller.NodeMetrics{
		NumberOfActiveExtents:   common.Int64Ptr(numExtents),
		NumberOfConnections:     common.Int64Ptr(numConns),
		OutgoingMessagesCounter: common.Int64Ptr(msgsOut),
		OutgoingBytesCounter:    common.Int64Ptr(bytesOut),
	}

	reporter.ReportHostMetric(hostMetrics)

	// Also update the metrics reporter
	h.m3Client.UpdateGauge(metrics.ConsConnectionStreamScope, metrics.OutputhostConsConnection, numConns)
}

// Stop stops the service
func (h *OutputHost) Stop() {
	h.hostIDHeartbeater.Stop()
	h.loadReporter.Stop()
	h.cfgMgr.Stop()
	h.SCommon.Stop()
}

// RegisterWSHandler is the implementation of WSService interface
func (h *OutputHost) RegisterWSHandler() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc(fmt.Sprintf(ccommon.HTTPHandlerPattern, ccommon.EndpointOpenConsumerStream), h.OpenConsumerStreamHandler)
	return mux
}

// SetFrontendClient is used to set the frontend client after we start the output
func (h *OutputHost) SetFrontendClient(frontendClient ccherami.TChanBFrontend) {
	h.frontendClient = frontendClient
}

// NewOutputHost is the constructor for BOut
func NewOutputHost(
	serviceName string,
	sVice common.SCommon,
	metadataClient metadata.TChanMetadataService,
	frontendClient ccherami.TChanBFrontend,
	opts *OutOptions,
	kafkaCfg configure.CommonKafkaConfig,
) (*OutputHost, []thrift.TChanServer) {

	// Get the deployment name for logger field
	deploymentName := sVice.GetConfig().GetDeploymentName()
	bs := OutputHost{
		logger:         (sVice.GetConfig().GetLogger()).WithFields(bark.Fields{common.TagOut: common.FmtOut(sVice.GetHostUUID()), common.TagDplName: common.FmtDplName(deploymentName)}),
		SCommon:        sVice,
		frontendClient: frontendClient,
		cgCache:        make(map[string]*consumerGroupCache),
		cacheTimeout:   defaultIdleTimeout,
		shutdownCh:     make(chan struct{}),
		unloadCacheCh:  make(chan string, defaultUnloadChSize),
		ackMgrMap:      make(map[uint32]*ackManager),
		ackMgrLoadCh:   make(chan ackMgrLoadMsg, defaultAckMgrMapChSize),
		ackMgrUnloadCh: make(chan uint32, defaultAckMgrMapChSize),
		ackMgrIDGen:    common.NewHostAckIDGenerator(defaultAckMgrIDStartFrom),
		hostMetrics:    load.NewHostMetrics(),
		kafkaCfg:       kafkaCfg,
	}

	sarama.Logger = NewSaramaLoggerFromBark(bs.logger, `sarama`)

	bs.sessionID = common.UUIDToUint16(sVice.GetHostUUID())

	bs.m3Client = metrics.NewClient(sVice.GetMetricsReporter(), metrics.Outputhost)
	if opts != nil {
		if opts.CacheIdleTimeout != 0 {
			bs.cacheTimeout = opts.CacheIdleTimeout
		}
		if opts.KStreamFactory != nil {
			bs.kafkaStreamFactory = opts.KStreamFactory
		}
	}

	bs.metaClient = mm.NewMetadataMetricsMgr(metadataClient, bs.m3Client, bs.logger)

	// manage uconfig, regiester handerFunc and verifyFunc for uConfig values
	bs.dClient = sVice.GetDConfigClient()
	bs.dynamicConfigManage()

	// cassandra config manager
	bs.cfgMgr = newConfigManager(metadataClient, bs.logger)

	common.StartEKG(bs.logger)
	thisOutputHost = &bs
	return &bs, []thrift.TChanServer{cherami.NewTChanBOutServer(&bs), admin.NewTChanOutputHostAdminServer(&bs)}
}

// UnloadConsumerGroups is the API used to unload consumer groups to clear the cache
func (h *OutputHost) UnloadConsumerGroups(ctx thrift.Context, request *admin.UnloadConsumerGroupsRequest) (err error) {
	defer atomic.AddInt32(&h.loadShutdownRef, -1)
	sw := h.m3Client.StartTimer(metrics.UnloadConsumerGroupsScope, metrics.OutputhostLatencyTimer)
	defer sw.Stop()
	h.m3Client.IncCounter(metrics.UnloadConsumerGroupsScope, metrics.OutputhostRequests)
	// If we are already shutting down, no need to do anything here
	if atomic.AddInt32(&h.loadShutdownRef, 1) <= 0 {
		h.logger.Error("not unloading the CG cache outputHost already shutdown")
		h.m3Client.IncCounter(metrics.UnloadConsumerGroupsScope, metrics.OutputhostFailures)
		return ErrHostShutdown
	}

	for _, cgUUID := range request.CgUUIDs {
		h.cgMutex.RLock()
		cg, ok := h.cgCache[cgUUID]
		if ok {
			go cg.unloadConsumerGroupCache()
		} else {
			h.logger.WithField(common.TagCnsm, common.FmtCnsm(cgUUID)).
				Error("consumer group is not cached at all")
			err = ErrCgAlreadyUnloaded
			h.m3Client.IncCounter(metrics.UnloadConsumerGroupsScope, metrics.OutputhostFailures)
		}
		h.cgMutex.RUnlock()
	}

	return err
}

// ListLoadedConsumerGroups is the API used to unload consumer groups to clear the cache
func (h *OutputHost) ListLoadedConsumerGroups(ctx thrift.Context) (result *admin.ListConsumerGroupsResult_, err error) {
	defer atomic.AddInt32(&h.loadShutdownRef, -1)
	//sw := h.m3Client.StartTimer(metrics.ListLoadedConsumerGroupsScope, metrics.OutputhostLatencyTimer)
	//defer sw.Stop()
	//h.m3Client.IncCounter(metrics.ListLoadedConsumerGroupsScope, metrics.OutputhostRequests)
	// If we are already shutting down, no need to do anything here
	if atomic.AddInt32(&h.loadShutdownRef, 1) <= 0 {
		h.logger.Error("not unloading the CG cache outputHost already shutdown")
		//h.m3Client.IncCounter(metrics.ListLoadedConsumerGroupsScope, metrics.OutputhostFailures)
		return nil, ErrHostShutdown
	}

	result = admin.NewListConsumerGroupsResult_()
	result.Cgs = make([]*admin.ConsumerGroups, 0)
	h.cgMutex.RLock()
	for cgUUID, cg := range h.cgCache {
		cgRes := admin.NewConsumerGroups()
		cgRes.CgUUID = common.StringPtr(cgUUID)
		cgRes.CgName = common.StringPtr(cg.cachedCGDesc.GetConsumerGroupName())
		cgRes.DestPath = common.StringPtr(cg.destPath)

		result.Cgs = append(result.Cgs, cgRes)
	}
	h.cgMutex.RUnlock()

	return result, err
}

//ReadCgState is the API used to get the cg state
func (h *OutputHost) ReadCgState(ctx thrift.Context, req *admin.ReadConsumerGroupStateRequest) (result *admin.ReadConsumerGroupStateResult_, err error) {
	defer atomic.AddInt32(&h.loadShutdownRef, -1)
	//sw := h.m3Client.StartTimer(metrics.ListLoadedConsumerGroupsScope, metrics.OutputhostLatencyTimer)
	//defer sw.Stop()
	//h.m3Client.IncCounter(metrics.ListLoadedConsumerGroupsScope, metrics.OutputhostRequests)
	// If we are already shutting down, no need to do anything here
	if atomic.AddInt32(&h.loadShutdownRef, 1) <= 0 {
		h.logger.Error("not unloading the CG cache outputHost already shutdown")
		//h.m3Client.IncCounter(metrics.ListLoadedConsumerGroupsScope, metrics.OutputhostFailures)
		return nil, ErrHostShutdown
	}

	result = admin.NewReadConsumerGroupStateResult_()

	result.SessionID = common.Int16Ptr(int16(h.sessionID))
	result.CgState = make([]*admin.ConsumerGroupState, 0)

	// Now populate all cg state
	h.cgMutex.RLock()
	for _, cgUUID := range req.CgUUIDs {
		cg, ok := h.cgCache[cgUUID]
		if ok {
			cgState := cg.getCgState(cgUUID)
			result.CgState = append(result.CgState, cgState)
		} else {
			h.logger.WithField(common.TagCnsm, common.FmtCnsm(cgUUID)).
				Error("consumer group is not cached at all")
			err = ErrCgAlreadyUnloaded
			//h.m3Client.IncCounter(metrics.UnloadConsumerGroupsScope, metrics.OutputhostFailures)
		}
	}
	h.cgMutex.RUnlock()

	return result, err
}

// UtilGetPickedStore is used by the integration test to figure out which store host
// we are currently connected to
// XXX: This should not be used anywhere else other than the test.
func (h *OutputHost) UtilGetPickedStore(cgName string, path string) (connStore string) {
	// First get the UUID
	mGetRequest := shared.ReadConsumerGroupRequest{
		DestinationPath:   common.StringPtr(path),
		ConsumerGroupName: common.StringPtr(cgName),
	}
	cgDesc, _ := h.metaClient.ReadConsumerGroup(nil, &mGetRequest)

	// Now try to find the store
	if cgDesc != nil {
		h.cgMutex.Lock()
		if cg, ok := h.cgCache[cgDesc.GetConsumerGroupUUID()]; ok {
			cg.extMutex.Lock()
			// pick one store to shutdown
			for _, ext := range cg.extentCache {
				ext.cacheMutex.Lock()
				connStore = ext.storeUUIDs[ext.pickedIndex]
				h.logger.WithField(common.TagStor, connStore).Info("Found store")
				ext.cacheMutex.Unlock()
				break
			}
			cg.extMutex.Unlock()
		}
		h.cgMutex.Unlock()
	}
	return
}

// OpenStreamingConsumerStream is unimplemented
func (h *OutputHost) OpenStreamingConsumerStream(ctx thrift.Context, call stream.BOutOpenStreamingConsumerStreamInCall) error {
	err := cherami.NewInternalServiceError()
	err.Message = `Unimplemented`
	return err
}

// SetConsumedMessages is unimplemented
func (h *OutputHost) SetConsumedMessages(ctx thrift.Context, request *cherami.SetConsumedMessagesRequest) error {
	err := cherami.NewInternalServiceError()
	err.Message = `Unimplemented`
	return err
}

func (h *OutputHost) incFailureCounter(metricsScope int, c metrics.ErrorClass) {
	h.m3Client.IncCounter(metricsScope, metrics.OutputhostFailures)
	switch c {
	case metrics.UserError:
		h.m3Client.IncCounter(metricsScope, metrics.OutputhostUserFailures)
	default:
		h.m3Client.IncCounter(metricsScope, metrics.OutputhostInternalFailures)
	}
}
