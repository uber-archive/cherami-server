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

package storehost

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	ccommon "github.com/uber/cherami-client-go/common"
	"github.com/uber/cherami-server/common"
	mm "github.com/uber/cherami-server/common/metadata"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/common/throttler"
	"github.com/uber/cherami-server/storage"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/store"
	// "code.uber.internal/odp/cherami/storage/rockstor"
	"github.com/uber/cherami-server/services/storehost/load"
	"github.com/uber/cherami-server/storage/manyrocks"
	storeStream "github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/controller"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"
)

// sealCheckInterval is the regular interval at which both the read and write paths check
// to see if the extent was sealed in the background. This is to handle the case where there
// are no more messages available, and asynchronously the extent was sealed.
const (
	sealCheckInterval time.Duration = 500 * time.Millisecond
)

const (
	// storehostLoadReportingInterval is the interval store host load is reported to controller
	storehostLoadReportingInterval = 2 * time.Second
)

// Buffer-sizes to use for the channels that wrap the read/write tchannel-streams
const (
	writeMsgChanBuf int = 4096 // msgC buffer to use for the write path
	writeAckChanBuf int = 1024 // ackC buffer to use for the write path
	readMsgChanBuf  int = 1024 // msgC buffer to use for the read path
)

// SealExtent throttling: throttle requests beyond 25 every 250ms
const (
	sealExtentThrottleRequests               = 25
	sealExtentThrottlePeriod   time.Duration = 250 * time.Millisecond
)

// both inConn and outConn implement this interface
type conn interface {
	// start the pumps/go-routines
	Start()

	// to initiate stoppage of the pumps (used in response to shutdown)
	Stop()

	// returns channel to wait on to detect when it is done
	Done() <-chan struct{}
}

// defaults for store, baseDir
const (
	defaultStore   Store  = ManyRocks // RockCFstor // Rockstor
	defaultBaseDir string = "./CHERAMI_STORE"
)

// Mode of operation: timer-queue or append-only mode
type Mode int

const (
	_ Mode = iota // skip the '0' value

	// AppendOnly mode (assumes all extents are in "append only" message queues)
	AppendOnly

	// TimerQueue mode (assumes all extents are in "timer queues")
	TimerQueue

	// Log mode (similar to AppendOnly which is gated on watermark)
	Log
)

func (t Mode) String() string {
	switch t {
	case AppendOnly:
		return "AppendOnly"
	case TimerQueue:
		return "TimerQueue"
	case Log:
		return "Log"
	default:
		return "UNKNOWN"
	}
}

// Store indicates the underlying storage to use
type Store int

const (
	_ Store = iota // skip the '0' value

	// Rockstor for store
	Rockstor

	// Chunky for store
	Chunky

	// ManyRocks for store
	ManyRocks

	// RockCFstor for store
	RockCFstor
)

func (t Store) String() string {
	switch t {
	case Rockstor:
		return "Rockstor"
	case Chunky:
		return "Chunky"
	case ManyRocks:
		return "ManyRocks"
	case RockCFstor:
		return "RockCFstor"
	default:
		return "UNKNOWN"
	}
}

type (
	// Options are the arguments passed to the storehost
	Options struct {
		Store   Store
		BaseDir string
	}

	// StoreHost is the main server class for StoreHosts
	StoreHost struct {
		common.SCommon
		opts              *Options
		mClient           metadata.TChanMetadataService
		hostIDHeartbeater common.HostIDHeartbeater
		loadReporter      common.LoadReporterDaemon

		shutdownWG sync.WaitGroup

		// the following is used by inConn/outConn
		xMgr          *ExtentManager      // extent manager
		replMgr       *ReplicationManager // replication manager
		shutdownC     chan struct{}
		disableWriteC chan struct{}

		numInConn, numOutConn int64 // number of active inConns/outConns respectively

		sealExtentThrottler *throttler.Throttler

		//logger
		logger bark.Logger

		// metric Client for storage
		m3Client metrics.Client

		// ReplicationJobRunner periodically starts replication jobs
		replicationJobRunner ReplicationJobRunner

		// extStatsReporter reports various stats on active extents
		extStatsReporter *ExtStatsReporter

		// Storage Monitoring
		storageMonitor StorageMonitor

		// metrics aggregated at host level and reported to controller
		hostMetrics *load.HostMetrics

		// unix nanos when the host level metrics were last reported
		// to the controller
		lastLoadReportedTime int64

		// status of the node
		nodeStatus atomic.Value

		// started?
		started int32
	}
)

// interface implementation check
var _ store.TChanBStore = &StoreHost{}
var _ common.WSService = &StoreHost{}

// NewStoreHost is the constructor for store host
func NewStoreHost(serviceName string, sCommon common.SCommon, mClient metadata.TChanMetadataService, opts *Options) (*StoreHost, []thrift.TChanServer) {

	// setup default options, if needed
	if opts == nil {
		opts = &Options{}
	}

	logger := sCommon.GetConfig().GetLogger().WithFields(bark.Fields{
		common.TagStor:    common.FmtStor(sCommon.GetHostUUID()),
		common.TagDplName: common.FmtDplName(sCommon.GetConfig().GetDeploymentName()),
	})

	m3Client := metrics.NewClient(sCommon.GetMetricsReporter(), metrics.Storage)

	t := &StoreHost{
		SCommon:       sCommon,
		opts:          opts,
		hostMetrics:   load.NewHostMetrics(),
		shutdownC:     make(chan struct{}),
		disableWriteC: make(chan struct{}),
		logger:        logger,
		m3Client:      m3Client,
		mClient:       mm.NewMetadataMetricsMgr(mClient, m3Client, logger),
	}

	return t, []thrift.TChanServer{store.NewTChanBStoreServer(t)}
}

// Start starts the storehost service
func (t *StoreHost) Start(thriftService []thrift.TChanServer) {

	t.SCommon.Start(thriftService)

	hostID := t.GetHostUUID()

	t.hostIDHeartbeater = common.NewHostIDHeartbeater(t.mClient, hostID, t.GetHostPort(), t.GetHostName(), t.logger)
	t.hostIDHeartbeater.Start()

	// setup the store manager
	var storeMgr storage.StoreManager
	var baseDir string

	switch t.opts.Store {
	default:
		fallthrough
	case ManyRocks:
		var err error

		// create base-dir for this 'host'
		baseDir = fmt.Sprintf("%s/%s", t.opts.BaseDir, hostID)

		storeMgr, err = manyrocks.New(&manyrocks.Opts{BaseDir: baseDir}, t.logger)

		if err != nil {
			t.logger.WithField("options", fmt.Sprintf("Store=%v BaseDir=%v", t.opts.Store, t.opts.BaseDir)).
				Fatal("Error initializing ManyRocks")
			return
		}

	}

	t.sealExtentThrottler = throttler.New(sealExtentThrottleRequests, sealExtentThrottlePeriod)

	t.xMgr = NewExtentManager(storeMgr, t.m3Client, t.hostMetrics, t.logger)

	t.storageMonitor = NewStorageMonitor(t, t.m3Client, t.hostMetrics, t.logger, baseDir)
	t.storageMonitor.Start()

	t.replMgr = NewReplicationManager(t.xMgr, t.m3Client, t.mClient, t.logger, hostID, t.GetWSConnector())

	t.replicationJobRunner = NewReplicationJobRunner(t.mClient, t, t.logger, t.m3Client)
	go t.replicationJobRunner.Start()

	// set the status as UP
	t.SetNodeStatus(controller.NodeStatus_UP)

	loadReporterDaemonfactory := t.GetLoadReporterDaemonFactory()
	t.xMgr.loadReporterFactory = loadReporterDaemonfactory
	t.loadReporter = loadReporterDaemonfactory.CreateReporter(storehostLoadReportingInterval, t, t.logger)
	t.lastLoadReportedTime = time.Now().UnixNano()
	t.loadReporter.Start()

	t.extStatsReporter = NewExtStatsReporter(hostID, t.xMgr, t.mClient, t.logger)
	t.extStatsReporter.Start()

	atomic.StoreInt32(&t.started, 1) // started

	t.logger.WithField("options", fmt.Sprintf("Store=%v BaseDir=%v", t.opts.Store, t.opts.BaseDir)).
		Info("StoreHost: started")
}

// Stop stops the service
func (t *StoreHost) Stop() {

	atomic.StoreInt32(&t.started, 0) // stopped

	t.loadReporter.Stop()
	t.hostIDHeartbeater.Stop()
	t.storageMonitor.Stop()
	t.replicationJobRunner.Stop()
	t.extStatsReporter.Stop()
	t.SCommon.Stop()
	t.logger.Info("StoreHost: stopped")
}

func getInConnArgs(ctx thrift.Context) (args *inConnArgs, err error) {

	req, err := common.GetOpenAppendStreamRequestStruct(ctx.Headers())

	if err != nil {
		return nil, err
	}

	args = &inConnArgs{} // alloc new inConnArgs

	// parse ExtentUUID
	if args.extentID = uuid.Parse(req.GetExtentUUID()); args.extentID == nil {
		return nil, newBadRequestError(fmt.Sprintf("BadRequestError: error parsing uuid (%s)", req.GetExtentUUID()))
	}

	// parse DestinationUUID
	if args.destID = uuid.Parse(req.GetDestinationUUID()); args.destID == nil {
		return nil, newBadRequestError(fmt.Sprintf("BadRequestError: error parsing uuid (%s)", req.GetDestinationUUID()))
	}

	// extract DestinationType
	args.destType = req.GetDestinationType()

	// find mode corresponding to extent
	args.mode = getModeForDestinationType(args.destType)

	if args.mode == 0 {
		return nil, newBadRequestError(fmt.Sprintf("BadRequestError: unknown type (%d)", args.mode))
	}

	return args, nil
}

// OpenAppendStreamHandler is websocket handler for opening write stream
func (t *StoreHost) OpenAppendStreamHandler(w http.ResponseWriter, r *http.Request) {

	req, err := common.GetOpenAppendStreamRequestHTTP(r.Header)
	if err != nil {
		t.logger.WithField(`error`, err).Error("unable to parse all needed headers")
		t.m3Client.IncCounter(metrics.OpenAppendStreamScope, metrics.StorageFailures)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// setup websocket
	wsStream, err := t.GetWSConnector().AcceptAppendStream(w, r)
	if err != nil {
		t.logger.WithField(`error`, err).Error("unable to upgrade websocket connection")
		t.m3Client.IncCounter(metrics.OpenAppendStreamScope, metrics.StorageFailures)
		return
	}

	// create fake thrift context with header
	ctx, cancel := thrift.NewContext(common.MaxDuration)
	defer cancel()
	ctx = thrift.WithHeaders(ctx, common.GetOpenAppendStreamRequestHeaders(req))

	// create thrift stream call wrapper and deligate to streaming call
	if err = t.OpenAppendStream(ctx, wsStream); err != nil {
		t.logger.WithField(`error`, err).Error("unable to open append stream")
		return
	}
}

// OpenAppendStream is the implementation of the thrift handler for the store host
func (t *StoreHost) OpenAppendStream(ctx thrift.Context, call storeStream.BStoreOpenAppendStreamInCall) error {

	t.m3Client.IncCounter(metrics.OpenAppendStreamScope, metrics.StorageRequests)

	if atomic.LoadInt32(&t.started) == 0 {
		call.Done()
		t.m3Client.IncCounter(metrics.OpenAppendStreamScope, metrics.StorageFailures)
		return newInternalServiceError("StoreHost not started")
	}

	// If the disk available space is low, we should fail any request to write extent
	if t.storageMonitor != nil && t.storageMonitor.GetStorageMode() == SMReadOnly {
		call.Done()
		t.m3Client.IncCounter(metrics.OpenAppendStreamScope, metrics.StorageFailures)
		return newInternalServiceError("StoreHost in read-only mode")
	}

	// read in args passed in via the Thrift context headers
	args, err := getInConnArgs(ctx)

	if err != nil {
		call.Done()
		t.m3Client.IncCounter(metrics.OpenAppendStreamScope, metrics.StorageFailures)
		return err
	}

	log := t.logger.WithFields(bark.Fields{
		common.TagExt: common.FmtExt(args.extentID.String()),
		common.TagDst: common.FmtDst(args.destID.String()),
	})

	log.WithField("args", fmt.Sprintf("destType=%v mode=%v", args.destType, args.mode)).
		Info("OpenAppendStream: starting inConn")

	in := newInConn(args, call, t.xMgr, t.m3Client, log)

	t.shutdownWG.Add(1)
	defer t.shutdownWG.Done()

	in.Start() // start!

	numInConn := atomic.AddInt64(&t.numInConn, 1)
	t.m3Client.UpdateGauge(metrics.OpenAppendStreamScope, metrics.StorageWriteStreams, numInConn)
	log.WithField(`numInConn`, numInConn).Info("OpenAppendStream: write stream opened")

	select {
	// wait for inConn to be done
	case err = <-in.Done():

	// .. or wait for shutdown to be triggered
	case <-t.shutdownC:
		err = in.Stop() // attempt to stop connection

	// listen to extreme situations
	case <-t.disableWriteC:
		log.Info("Stop write due to available disk space is extremely low")
		err = in.Stop()
	}

	numInConn = atomic.AddInt64(&t.numInConn, -1)
	t.m3Client.UpdateGauge(metrics.OpenAppendStreamScope, metrics.StorageWriteStreams, numInConn)
	log.WithField(`numInConn`, numInConn).Info("OpenAppendStream: write stream closed")

	log.Info("OpenAppendStream done")
	return err // FIXME: tchannel does *not* currently propagate this to the remote caller
}

func getOutConnArgs(ctx thrift.Context) (args *outConnArgs, err error) {

	req, err := common.GetOpenReadStreamRequestStruct(ctx.Headers())

	if err != nil {
		return nil, err
	}

	args = &outConnArgs{} // alloc new outConnArgs

	// parse ExtentUUID
	if args.extentID = uuid.Parse(req.GetExtentUUID()); args.extentID == nil {
		return nil, newBadRequestError(fmt.Sprintf("error parsing uuid (%s)", req.GetExtentUUID()))
	}

	// parse DestinationUUID
	if args.destID = uuid.Parse(req.GetDestinationUUID()); args.destID == nil {
		return nil, newBadRequestError(fmt.Sprintf("BadRequestError: error parsing uuid (%s)", req.GetDestinationUUID()))
	}

	// extract DestinationType
	args.destType = req.GetDestinationType()

	// find mode corresponding to extent
	args.mode = getModeForDestinationType(args.destType)

	if args.mode == 0 {
		return nil, newBadRequestError(fmt.Sprintf("BadRequestError: unknown type (%d)", args.mode))
	}

	// parse ConsumerGroupUUID
	if args.consGroupID = uuid.Parse(req.GetConsumerGroupUUID()); args.consGroupID == nil {
		return nil, newBadRequestError(fmt.Sprintf("BadRequestError: error parsing uuid (%s)", req.GetConsumerGroupUUID()))
	}

	// extract address
	args.address = req.GetAddress()

	// extract 'inclusive' flag
	args.inclusive = req.GetInclusive()

	return args, nil
}

// OpenReadStreamHandler is websocket handler for opening read stream
func (t *StoreHost) OpenReadStreamHandler(w http.ResponseWriter, r *http.Request) {
	req, err := common.GetOpenReadStreamRequestHTTP(r.Header)
	if err != nil {
		t.logger.WithField(`error`, err).Error("unable to parse all needed headers")
		t.m3Client.IncCounter(metrics.OpenReadStreamScope, metrics.StorageFailures)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// setup websocket
	wsStream, err := t.GetWSConnector().AcceptReadStream(w, r)
	if err != nil {
		t.logger.WithField(`error`, err).Error("unable to upgrade websocket connection")
		t.m3Client.IncCounter(metrics.OpenReadStreamScope, metrics.StorageFailures)
		return
	}

	// create fake thrift context with header
	ctx, cancel := thrift.NewContext(common.MaxDuration)
	defer cancel()
	ctx = thrift.WithHeaders(ctx, common.GetOpenReadStreamRequestHeaders(req))

	// create thrift stream call wrapper and deligate to streaming call
	if err = t.OpenReadStream(ctx, wsStream); err != nil {
		t.logger.WithField(`error`, err).Error("unable to open read stream")
		return
	}
}

// OpenReadStream is the implementation of the thrift handler for the store host
func (t *StoreHost) OpenReadStream(ctx thrift.Context, call storeStream.BStoreOpenReadStreamInCall) error {

	t.m3Client.IncCounter(metrics.OpenReadStreamScope, metrics.StorageRequests)

	if atomic.LoadInt32(&t.started) == 0 {
		call.Done()
		t.m3Client.IncCounter(metrics.OpenReadStreamScope, metrics.StorageFailures)
		return newInternalServiceError("StoreHost not started")
	}

	// read in args passed in via the Thrift context headers
	args, e := getOutConnArgs(ctx)

	if e != nil {

		err := newReadMessageError(e.Error())

		t.logger.WithField(common.TagErr, err).
			Error("OpenReadStream: getReadStreamArgs error")

		if e = call.Write(err); e != nil {
			t.logger.WithField(common.TagErr, err).
				Error("OpenReadStream: stream.Write error")
		}

		call.Done()
		t.m3Client.IncCounter(metrics.OpenReadStreamScope, metrics.StorageFailures)
		return nil // tchannel-stream does not convey errors to remote caller
	}

	log := t.logger.WithFields(bark.Fields{
		common.TagExt:  common.FmtExt(args.extentID.String()),
		common.TagDst:  common.FmtDst(args.destID.String()),
		common.TagCnsm: common.FmtCnsm(args.consGroupID.String()),
	})

	out := newOutConn(args, call, t.xMgr, t.m3Client, log)

	t.shutdownWG.Add(1)
	defer t.shutdownWG.Done()

	out.Start() // start!

	numOutConn := atomic.AddInt64(&t.numOutConn, 1)
	t.m3Client.UpdateGauge(metrics.OpenReadStreamScope, metrics.StorageReadStreams, numOutConn)

	log.WithFields(bark.Fields{
		`destType`:   args.destType,
		`mode`:       args.mode,
		`address`:    args.address,
		`inclusive`:  args.inclusive,
		`numOutConn`: numOutConn,
	}).Info("OpenReadStream: outConn started")

	select {
	// wait for outConn to be done
	case <-out.Done():

	// .. or wait for shutdown to be triggered
	case <-t.shutdownC:
		out.Stop() // attempt to stop connection
	}

	numOutConn = atomic.AddInt64(&t.numOutConn, -1)
	t.m3Client.UpdateGauge(metrics.OpenReadStreamScope, metrics.StorageReadStreams, numOutConn)
	log.WithField(`numOutConn`, numOutConn).Info("OpenReadStream: outConn done")

	return nil // tchannel-stream does not convey errors to remote caller
}

// GetAddressFromTimestamp is the implementation of the thrift handler for store host
func (t *StoreHost) GetAddressFromTimestamp(ctx thrift.Context, req *store.GetAddressFromTimestampRequest) (res *store.GetAddressFromTimestampResult_, err error) {

	t.m3Client.IncCounter(metrics.GetAddressFromTimestampScope, metrics.StorageRequests)

	if atomic.LoadInt32(&t.started) == 0 {
		t.m3Client.IncCounter(metrics.GetAddressFromTimestampScope, metrics.StorageFailures)
		return nil, newInternalServiceError("StoreHost not started")
	}

	sw := t.m3Client.StartTimer(metrics.GetAddressFromTimestampScope, metrics.StorageLatencyTimer)
	defer sw.Stop()

	log := t.logger.WithField(common.TagExt, common.FmtExt(req.GetExtentUUID()))
	log.WithField("args", fmt.Sprintf("timestamp=%x", req.GetTimestamp())).
		Info("GetAddressFromTimestamp")

	extentID := uuid.Parse(req.GetExtentUUID())
	timestamp := req.GetTimestamp()

	if extentID == nil {
		log.Error("GetAddressFromTimestamp failed: error parsing uuid")
		t.m3Client.IncCounter(metrics.GetAddressFromTimestampScope, metrics.StorageFailures)
		return nil, newBadRequestError(fmt.Sprintf("error parsing uuid (%s)", req.GetExtentUUID()))
	}

	// FIXME: T471157 since we do not have the mode available currently, just open in AppendOnly for now;
	// eventually, the "mode" the extent needs to be opened in should be derived from the destination
	// type for the extent.
	x, err := t.xMgr.OpenExtent(extentID, AppendOnly, OpenIntentGetAddressFromTimestamp)

	if err != nil {
		log.WithField("context", fmt.Sprintf("err=%v", err)).
			Error("GetAddressFromTimestamp failed: OpenExtent error")
		t.m3Client.IncCounter(metrics.GetAddressFromTimestampScope, metrics.StorageStoreFailures)
		t.m3Client.IncCounter(metrics.GetAddressFromTimestampScope, metrics.StorageFailures)
		return nil, newExtentNotFoundError(extentID, fmt.Sprintf("OpenExtent error: %v", err))
	}

	defer x.Close() // close extent before leaving

	// seek with a key constructed using the timestamp and a "0" seqnum
	timeKey := x.constructKey(timestamp, 0)

	// look for a message with a key less than or equal to given timeKey
	addr, addrKey, err := x.storeSeekFloor(timeKey)

	if err != nil {
		log.WithField("context", fmt.Sprintf("timeKey=%x err=%v", timeKey, err)).
			Error("GetAddressFromTimestamp failed SeekFloor()")
		t.m3Client.IncCounter(metrics.GetAddressFromTimestampScope, metrics.StorageStoreFailures)
		t.m3Client.IncCounter(metrics.GetAddressFromTimestampScope, metrics.StorageFailures)
		return nil, newInternalServiceError(fmt.Sprintf("%v store.SeekFloor(%x) failed: %v", extentID, timeKey, err))
	}

	// check to see if the extent is sealed beyond this messages
	_, nextKey, err := x.storeNext(addr)

	if err != nil {
		log.WithField("context", fmt.Sprintf("addr=%x err=%v", addr, err)).
			Error("GetAddressFromTimestamp failed Next()")
		t.m3Client.IncCounter(metrics.GetAddressFromTimestampScope, metrics.StorageStoreFailures)
		t.m3Client.IncCounter(metrics.GetAddressFromTimestampScope, metrics.StorageFailures)
		return nil, newInternalServiceError(fmt.Sprintf("%v store.Next(%x) failed: %v", extentID, addr, err))
	}

	res = store.NewGetAddressFromTimestampResult_()

	if addr == storage.MinAddr {
		res.Address = common.Int64Ptr(store.ADDR_BEGIN)
		res.SequenceNumber = common.Int64Ptr(0)
	} else {
		// FIXME: T471157 as above, the mode is not available, and the implementation doesn't properly support TIMER queues.
		// This sequence number will be incorrect for timer queues
		_, seqNum := x.deconstructKey(addrKey)
		res.Address = common.Int64Ptr(int64(addr))
		res.SequenceNumber = common.Int64Ptr(seqNum)
	}

	if nextKey != storage.InvalidKey && x.isSealExtentKey(nextKey) {
		res.Sealed = common.BoolPtr(true)
	} else {
		res.Sealed = common.BoolPtr(false)
	}

	log.WithField("return", fmt.Sprintf("timestamp=%v, address=%x, seq=%d, sealed=%v", req.GetTimestamp(), res.GetAddress(), res.GetSequenceNumber(), res.GetSealed())).
		Info("GetAddressFromTimestamp done")

	return res, nil
}

// SealExtent is the implementation of the thrift handler for the store host
func (t *StoreHost) SealExtent(ctx thrift.Context, req *store.SealExtentRequest) (err error) {

	t.m3Client.IncCounter(metrics.SealExtentScope, metrics.StorageRequests)

	if atomic.LoadInt32(&t.started) == 0 {
		t.m3Client.IncCounter(metrics.SealExtentScope, metrics.StorageFailures)
		return newInternalServiceError("StoreHost not started")
	}

	var sealSeqNum, unspecifiedSealSeqNum = seqNumUnspecifiedSeal, true

	if req.IsSetSequenceNumber() {
		sealSeqNum = req.GetSequenceNumber()
		unspecifiedSealSeqNum = false
	}

	log := t.logger.WithField(common.TagExt, common.FmtExt(req.GetExtentUUID()))
	log.WithField("args", fmt.Sprintf("sealSeqNum=%d", sealSeqNum)).
		Info("SealExtent()")

	if !t.sealExtentThrottler.Allow() {
		log.Error("SealExtent failed: request throttled")
		t.m3Client.IncCounter(metrics.SealExtentScope, metrics.StorageFailures)
		return newInternalServiceError("request throttled")
	}

	sw := t.m3Client.StartTimer(metrics.SealExtentScope, metrics.StorageLatencyTimer)
	defer sw.Stop()

	extentID := uuid.Parse(req.GetExtentUUID())

	if extentID == nil {
		log.Error("SealExtent failed: error parsing extentuuid")
		t.m3Client.IncCounter(metrics.SealExtentScope, metrics.StorageFailures)
		return newBadRequestError(fmt.Sprintf("error parsing extentuuid (%s)", req.GetExtentUUID()))
	}

	// open extent and see if it was already sealed
	// FIXME: since we do not have the mode available currently, just open in AppendOnly for now;
	// eventually, the "mode" the extent needs to be opened in should be derived from the destination
	// type for the extent.
	x, err := t.xMgr.OpenExtent(extentID, AppendOnly, OpenIntentSealExtent)

	if err != nil {
		log.WithField("context", fmt.Sprintf("err=%v", err)).
			Error("SealExtent failed: OpenExtent error")
		t.m3Client.IncCounter(metrics.SealExtentScope, metrics.StorageFailures)
		t.m3Client.IncCounter(metrics.SealExtentScope, metrics.StorageStoreFailures)
		return newInternalServiceError(fmt.Sprintf("%v OpenExtent error: %v", extentID, err))
	}

	defer x.Close() // close extent before leaving
	defer x.storeSync()

	// TODO: for now don't error on any validations, and make a 'best effort'
	// to mark the extent as "sealed".  we will revisit this when we need to
	// support consistent destinations, etc.

	if oldSealSeqNum := x.getSealSeqNum(); oldSealSeqNum != seqNumNotSealed { // extent is sealed

		switch {
		case sealSeqNum == seqNumNotSealed: // redundant request

			log.WithField("context", fmt.Sprintf("oldSealSeqNum=%d", oldSealSeqNum)).
				Error("SealExtent: extent already sealed")
			// return newExtentSealedError(extentID, oldSealSeqNum,
			// 	fmt.Sprintf("%v extent already sealed at %d", extentID, oldSealSeqNum))

		case sealSeqNum > oldSealSeqNum: // seal-seqnum is beyond old seal-seqnum
			log.WithField("context", fmt.Sprintf("oldSealSeqNum=%d sealSeqNum=%d", oldSealSeqNum, sealSeqNum)).
				Error("SealExtent: extent sealed at seqNumber less than requested")
			// return newExtentSealedError(extentID, oldSealSeqNum,
			// 	fmt.Sprintf("%v extent sealed at %d (less than requested %d)", extentID, oldSealSeqNum, sealSeqNum))

		case sealSeqNum == oldSealSeqNum: // redundant request

			log.WithField("context", fmt.Sprintf("sealSeqNum=%d", sealSeqNum)).
				Info("SealExtent: extent already sealed at requested seqnum")
			// if already sealed at requested seqNum, ignore
			return nil

		default: // the sealSeqNum is less that oldSealSeqNum; continue ..
		}

		// get the sealExtentSync as we go into seal this extent at 'sealSeqNum'

		x.extentLock()
		defer x.extentUnlock()

		oldSealSeqNum = x.getSealSeqNum()

		// double check with lock held, in case we lost the race to another SealExtent call
		if sealSeqNum >= oldSealSeqNum {

			if sealSeqNum == oldSealSeqNum { // redundant request
				log.WithField("context", fmt.Sprintf("sealSeqNum=%d", sealSeqNum)).
					Info("SealExtent: extent already sealed at requested seqnum")
				return nil // already sealed with 'seqNum'; no-op
			}

			if sealSeqNum > oldSealSeqNum { // seal-seqnum is beyond old seal-seqnum
				log.WithField("context", fmt.Sprintf("oldSealSeqNum=%d sealSeqNum=%d", oldSealSeqNum, sealSeqNum)).
					Error("SealExtent: [2] extent sealed at seqNumber less than requested")
				// return newExtentSealedError(extentID, oldSealSeqNum, fmt.Sprintf("%v extent sealed at %d (less than requested %d)", extentID, oldSealSeqNum, sealSeqNum))
			}
		}

	} else { // extent is not sealed

		// FIXME
		// ensure that the requested sealSeqNum is less than or equal to the seqNum of
		// the last message in the extent. if the lastSeqNum is not available in memory,
		// we should really load the extent from disk and query the last seqNum -- but
		// unfortunately in the current implementation of 'rockstor' there isn't a way to
		// do this efficiently (ie, without having to scan through the entire extent).
		// so for now, simply use the 'lastSeqNum' available in memory, and if it isn't
		// available, fail!

		x.extentLock()
		defer x.extentUnlock()

		// read lastSeqNum, if available
		lastSeqNum := x.getLastSeqNum()

		// that said, we are going to allow a special SealExtent request with a seqNum
		// of 'seqNumUnspecifiedSeal' to go through, to support sealing an extent as a best effort.

		switch {
		case lastSeqNum == SeqnumInvalid && unspecifiedSealSeqNum: // no sealSeqNum specified and we don't know lastSeqNum

			// continue -> just write a sealExtentKey with 'seqNumUnspecifiedSeal' seqNum (for now)

		case lastSeqNum == SeqnumInvalid && !unspecifiedSealSeqNum: // we were given a sealSeqNum, but we don't know lastSeqNum

			log.Error("SealExtent: last-seqnum unknown (timer-queue mode?)")
			// return newInternalServiceError(fmt.Sprintf("%v last-seqnum unknown", extentID))

		case lastSeqNum != SeqnumInvalid && unspecifiedSealSeqNum: // no seal-seqnum specified and we have a known last-seqnum

			sealSeqNum = lastSeqNum // seal at last-seqnum
			// FIXME: we should return the sealSeqNum to the caller, in this particular case!

		case lastSeqNum != SeqnumInvalid && !unspecifiedSealSeqNum: // we have seal-seqnum and last-seqnum available

			// ensure the seal-seqnum is less than (or equal to) last-seqnum
			if sealSeqNum > lastSeqNum {

				log.WithField("context", fmt.Sprintf("lastSeqNum=%d sealSeqNum=%d", lastSeqNum, sealSeqNum)).Error("SealExtent: last-seqnum less than seal-seqnum")
				// return newExtentFailedToSealError(extentID, sealSeqNum, lastSeqNum,
				// 	fmt.Sprintf("%v sealSeqNum=%d exceeds lastSeqNum=%d", extentID, sealSeqNum, lastSeqNum))
			}

			// log the case where we have extraneous messages
			if sealSeqNum < lastSeqNum {
				log.WithField("context", fmt.Sprintf("lastSeqNum=%d sealSeqNum=%d", lastSeqNum, sealSeqNum)).Error("sealSeqNum < lastSeqNum: potentially un-acked messages")
			}
		}
	}

	// write a new SealExtentKey using given seqNum
	sealExtentKey := x.constructSealExtentKey(sealSeqNum)

	_, err = x.storePut(sealExtentKey, []byte{})

	if err != nil {
		log.WithField("context", fmt.Sprintf("err=%v", err)).
			Error("SealExtent: error writing seal-key")
		t.m3Client.IncCounter(metrics.SealExtentScope, metrics.StorageFailures)
		return newInternalServiceError(fmt.Sprintf("%v failed writing seal-key: %v", extentID, err))
	}

	x.setSealSeqNum(sealSeqNum) // save sealSeqNum

	if unspecifiedSealSeqNum {
		log.WithField("context", fmt.Sprintf("sealSeqNum=%d", sealSeqNum)).
			Info("SealExtent with unspecified seqnum sealed")
		// return newExtentSealedError(extentID, sealSeqNum, "extent sealed")
	}

	log.WithField("context", fmt.Sprintf("sealSeqNum=%d", sealSeqNum)).
		Info("SealExtent done")

	return nil
}

// GetExtentInfo is the implementation of the thrift handler for the store host
func (t *StoreHost) GetExtentInfo(ctx thrift.Context, extReq *store.GetExtentInfoRequest) (*store.ExtentInfo, error) {

	if atomic.LoadInt32(&t.started) == 0 {
		return nil, newInternalServiceError("StoreHost not started")
	}

	sw := t.m3Client.StartTimer(metrics.GetExtentInfoScope, metrics.StorageLatencyTimer)
	defer sw.Stop()
	t.m3Client.IncCounter(metrics.GetExtentInfoScope, metrics.StorageRequests)
	log := t.logger.WithField(common.TagExt, common.FmtExt(extReq.GetExtentUUID()))

	extentID := uuid.Parse(extReq.GetExtentUUID())

	if extentID == nil {
		log.Error("GetExtentInfo failed: error parsing extentuuid")
		t.m3Client.IncCounter(metrics.GetExtentInfoScope, metrics.StorageFailures)
		return nil, newBadRequestError(fmt.Sprintf("error parsing extentuuid (%s)", extReq.GetExtentUUID()))
	}

	// open extent
	// FIXME: since we do not have the mode available currently, just open in AppendOnly for now;
	// eventually, the "mode" the extent needs to be opened in should be derived from the destination
	// type for the extent.
	x, err := t.xMgr.OpenExtent(extentID, AppendOnly, OpenIntentGetExtentInfo)

	if err != nil {
		log.WithField("context", fmt.Sprintf("err=%v", err)).
			Error("GetExtentInfo failed: OpenExtent error")
		t.m3Client.IncCounter(metrics.GetExtentInfoScope, metrics.StorageFailures)
		return nil, newInternalServiceError(fmt.Sprintf("%v OpenExtent error: %v", extentID, err))
	}

	defer x.Close() // close extent before leaving

	extentInfo := &store.ExtentInfo{
		ExtentUUID: common.StringPtr(extReq.GetExtentUUID()),
	}

	addressFirst, keyFirst, err := x.storeSeekFirst()

	if err != nil {
		log.WithField("context", fmt.Sprintf("err=%v", err)).
			Error("GetExtentInfo failed SeekFirst()")
		t.m3Client.IncCounter(metrics.GetExtentInfoScope, metrics.StorageFailures)
		return nil, newInternalServiceError(fmt.Sprintf("%v store.SeekFirst()) failed: %v", extentID, err))
	}

	if keyFirst != storage.InvalidKey {
		*extentInfo.BeginEnqueueTimeUtc, *extentInfo.BeginSequence = x.deconstructKey(keyFirst)
		*extentInfo.BeginAddress = int64(addressFirst)
	}

	addressLast, keyLast, err := x.storeSeekLast()

	if err != nil {
		log.WithField("context", fmt.Sprintf("err=%v", err)).
			Error("GetExtentInfo failed SeekLast()")
		t.m3Client.IncCounter(metrics.GetExtentInfoScope, metrics.StorageFailures)
		return nil, newInternalServiceError(fmt.Sprintf("%v store.SeekLast() failed: %v", extentID, err))
	}

	if keyLast != storage.InvalidKey {
		*extentInfo.LastEnqueueTimeUtc, *extentInfo.LastSequence = x.deconstructKey(keyLast)
		*extentInfo.LastAddress = int64(addressLast)
	}

	return extentInfo, nil
}

// PurgeMessages is the implementation of the thrift handler for the store host
func (t *StoreHost) PurgeMessages(ctx thrift.Context, req *store.PurgeMessagesRequest) (res *store.PurgeMessagesResult_, err error) {

	t.m3Client.IncCounter(metrics.PurgeMessagesScope, metrics.StorageRequests)

	if atomic.LoadInt32(&t.started) == 0 {
		t.m3Client.IncCounter(metrics.PurgeMessagesScope, metrics.StorageFailures)
		return nil, newInternalServiceError("StoreHost not started")
	}

	sw := t.m3Client.StartTimer(metrics.PurgeMessagesScope, metrics.StorageLatencyTimer)
	defer sw.Stop()

	purgeAddr := req.GetAddress()

	log := t.logger.WithField(common.TagExt, common.FmtExt(req.GetExtentUUID()))
	log.WithField("args", fmt.Sprintf("address=%x", purgeAddr)).
		Info("PurgeMessages()")

	extentID := uuid.Parse(req.GetExtentUUID())

	if extentID == nil {
		log.Error("PurgeMessages failed: error parsing extentuuid")
		return nil, newBadRequestError(fmt.Sprintf("error parsing extentuuid (%s)", req.GetExtentUUID()))
	}

	// open extent
	// FIXME: since we do not have the mode available currently, just open in AppendOnly for now;
	// eventually, the "mode" the extent needs to be opened in should be derived from the destination
	// type for the extent.
	x, err := t.xMgr.OpenExtent(extentID, AppendOnly, OpenIntentPurgeMessages)

	if err != nil {
		log.WithField("context", fmt.Sprintf("err=%v", err)).
			Error("PurgeMessages failed: OpenExtent error")
		// FIXME: check if 'file not found' error and return "success", if purgeAddr == ADDR_SEAL?
		t.m3Client.IncCounter(metrics.PurgeMessagesScope, metrics.StorageFailures)
		return nil, newExtentNotFoundError(extentID, fmt.Sprintf("%v OpenExtent error: %v", extentID, err))
	}

	defer x.Close() // close extent before leaving

	res = store.NewPurgeMessagesResult_()

	switch purgeAddr {
	case store.ADDR_SEAL: // ADDR_SEAL -> delete entire extent

		// the following is to handle the case, where we have an OpenAppendStream
		// and/or OpenReadStream calls active. in case we got a PurgeMessages with
		// purgeAddr == ADDR_SEAL, this means that this particular storehost did not
		// see the "seal" for this extent, but have been requested to delete the
		// entire extent. in order to unblock the OpenAppendStream/OpenReadStream
		// waiters, we seal the extent here.
		if x.getSealSeqNum() == seqNumNotSealed {

			x.extentLock()

			if x.getSealSeqNum() == seqNumNotSealed {

				sealSeqNum := seqNumUnspecifiedSeal

				// write a new SealExtentKey using given seqNum
				sealExtentKey := x.constructSealExtentKey(sealSeqNum)

				_, err = x.storePut(sealExtentKey, []byte{})

				if err != nil {
					log.WithField("context", fmt.Sprintf("sealExtentKey=%x err=%v", sealExtentKey, err)).
						Error("PurgeMessages: error writing seal-key")
					// x.extentUnlock()
					// return newInternalServiceError(fmt.Sprintf("%v failed writing seal-key: %v", extentID, err))
				}

				x.setSealSeqNum(sealSeqNum)

				log.Info("PurgeMessages: sealed extent")
			}

			x.extentUnlock()
		}

		// delete all messages from the extent up until, but not including, the seal-key,
		// so that all active consumers will be closed with an extent-sealed error.

		sealExtentKey := x.constructSealExtentKey(x.getSealSeqNum())

		// look for a message with a key that is immediately before sealExtentKey, if any
		if endAddr, _, err2 := x.storeSeekFloor(sealExtentKey - 1); err2 == nil {

			_, _, err = x.storePurge(storage.Address(endAddr))

			if err != nil {
				log.WithField("context", fmt.Sprintf("address=%x error=%v", endAddr, err)).
					Error("PurgeMessages: error deleting all messages from sealed extent")
				// ignore error and continue ..
			}

		} else {

			log.WithField("context", fmt.Sprintf("key=%x err=%v", sealExtentKey-1, err2)).
				Error("PurgeMessages: error storeSeekFloor of sealKey-1")
			// ignore error and continue ..
		}

		// mark extent for delete on close
		x.Delete()

		res.Address = common.Int64Ptr(store.ADDR_BEGIN)
		x.setFirstMsg(SeqnumInvalid, AddressInvalid, TimestampInvalid)
		log.Info("PurgeMessages done: marked extent for deletion")
		return res, nil

	case store.ADDR_END: // we will not accept 'ADDR_END'

		log.Error("PurgeMessages failed: invalid address (ADDR_END)")
		t.m3Client.IncCounter(metrics.PurgeMessagesScope, metrics.StorageFailures)
		return nil, newBadRequestError(fmt.Sprintf("%v invalid purgeAddr=%x (ADDR_END)", extentID, purgeAddr))
	}

	// delete messages from storage
	nextAddr, nextKey, err := x.storePurge(storage.Address(purgeAddr))

	if err != nil {
		log.WithField("context", fmt.Sprintf("address=%x error=%v", purgeAddr, err)).
			Error("PurgeMessages failed: error purging messages")
		t.m3Client.IncCounter(metrics.PurgeMessagesScope, metrics.StorageFailures)
		return nil, newInternalServiceError(fmt.Sprintf("%v Purge(addr=%x) error: %v", extentID, purgeAddr, err))
	}

	switch {
	case nextAddr == storage.EOX:
		res.Address = common.Int64Ptr(store.ADDR_END)
		x.setFirstMsg(SeqnumInvalid, AddressInvalid, TimestampInvalid)

	case x.isSealExtentKey(nextKey):
		res.Address = common.Int64Ptr(store.ADDR_SEAL)
		x.setFirstMsg(SeqnumInvalid, AddressInvalid, TimestampInvalid)

	default:
		visibilityTime, firstSeqNum := x.deconstructKey(nextKey)
		x.setFirstMsg(int64(nextKey), firstSeqNum, visibilityTime)
		res.Address = common.Int64Ptr(int64(nextKey))
	}

	log.WithField("return", fmt.Sprintf("purgeAddr=%x nextKey=%x", purgeAddr, nextKey)).
		Info("PurgeMessages done")
	return res, nil
}

// ReadMessages reads a set of messages start from the set StartAddress
func (t *StoreHost) ReadMessages(ctx thrift.Context, req *store.ReadMessagesRequest) (result *store.ReadMessagesResult_, err error) {

	if atomic.LoadInt32(&t.started) == 0 {
		t.m3Client.IncCounter(metrics.OpenReadStreamScope, metrics.StorageFailures)
		return nil, newInternalServiceError("StoreHost not started")
	}

	startAddr := req.GetStartAddress()
	numRequested := int64(req.GetNumMessages())
	extentIDStr := req.GetExtentUUID()

	if !req.IsSetNumMessages() {
		numRequested = 1
	}

	log := t.logger.WithField(common.TagExt, common.FmtExt(extentIDStr))
	log.WithField("StartAddress", startAddr).Info("ReadMessages start")

	extentID := uuid.Parse(extentIDStr)
	if extentID == nil {
		log.Error("ReadMessages failed: error parsing extentuuid")
		return nil, newBadRequestError(fmt.Sprintf("Error parsing extent %s", extentIDStr))
	}

	// open extent for read
	x, err := t.xMgr.OpenExtent(extentID, AppendOnly, OpenIntentReadStream)
	if err != nil {
		log.WithField(common.TagErr, err).Error(`ReadMessages failed on OpenExtent`)
		return nil, newExtentNotFoundError(
			extentID,
			fmt.Sprintf("ReadMessages on extent %v addr %x error: %v", extentID, startAddr, err))
	}

	defer x.Close() // cleanup/close extent

	// locate the first message
	var addr storage.Address // 'addr': address of first message requested to read
	var next storage.Address // 'next': address of next message to read
	var nextKey storage.Key  // 'nextKey': "key" of the next message

	result = store.NewReadMessagesResult_()
	result.Messages = make([]*store.ReadMessageContent, 0)

	if !req.GetStartAddressInclusive() || startAddr == store.ADDR_BEGIN {
		addr = storage.Address(startAddr)

		// do a 'SeekCeil' of addr+1 to find the address to start from
		if next, nextKey, err = x.storeNext(addr); err != nil {
			log.WithField(common.TagErr, err).Error(`ReadMessages failed`)
			result.Messages = append(
				result.Messages,
				newReadMessageContentError(fmt.Sprintf("InvalidAddressError: %v Next(%x) error: %v", extentID, addr, err)),
			)
			return
		}
	} else {
		addr = storage.Address(startAddr - 1)

		next = storage.Address(startAddr)
		nextKey, err = x.storeGetKey(next)

		if err != nil {
			log.WithField(common.TagErr, err).Error(`ReadMessages failed`)
			result.Messages = append(
				result.Messages,
				newReadMessageContentError(fmt.Sprintf("InvalidAddressError: %v GetKey(%x) error: %v", extentID, next, err)),
			)
			return
		}
	}

	var numMsgs int64

readMsgsLoop:
	for next != storage.EOX && numMsgs < numRequested {
		if x.isSealExtentKey(nextKey) { // check if we found the seal marker
			log.WithFields(bark.Fields{
				"error":   "extent sealed",
				"sealKey": strconv.FormatInt(int64(nextKey), 16),
				"extra":   "[purge race]",
				"numMsgs": strconv.FormatInt(numMsgs, 10),
			}).Info("ReadMessages done")
			result.Messages = append(
				result.Messages,
				newReadMessageContentSealed(extentID, x.deconstructSealExtentKey(nextKey)),
			)
			return
		}

		addr = next

		var val storage.Value
		_, val, next, nextKey, err = x.storeGet(addr)

		if err != nil { // read error?

			// retry, in case the error was because we were racing with a
			// 'PurgeMessages' call, and if so push the 'cursor' to the
			// first available message.

			// find next of 'addr - 1' and check if we get an address that's not 'addr'
			if next, nextKey, err = x.storeNext(addr - 1); err != nil {
				log.WithFields(bark.Fields{
					"error":        "ext.Next failed",
					"error-string": err.Error(),
					"addr":         strconv.FormatInt(int64(addr-1), 16),
					"extra":        "[purge race]",
					"numMsgs":      strconv.FormatInt(numMsgs, 10),
				}).Info("ReadMessages done")
				result.Messages = append(
					result.Messages,
					newReadMessageContentNoMoreMsgs(
						extentIDStr,
						fmt.Sprintf("NoMoreMessageError: %v error: %v", extentID, err)),
				)
				return
			}

			if next != addr { // this indicates that perhaps some messages were purged, so retry

				if x.isSealExtentKey(nextKey) {
					log.WithFields(bark.Fields{
						"error":   "extent sealed",
						"sealKey": strconv.FormatInt(int64(nextKey), 16),
						"extra":   "[purge race]",
						"numMsgs": strconv.FormatInt(numMsgs, 10),
					}).Info("ReadMessages done")
					result.Messages = append(
						result.Messages,
						newReadMessageContentSealed(extentID, x.deconstructSealExtentKey(nextKey)),
					)
					return
				}

				continue readMsgsLoop
			}

			log.WithFields(bark.Fields{
				"error":        "ext.Get failed",
				"error-string": err.Error(),
				"addr":         strconv.FormatInt(int64(addr), 16),
				"extra":        "[purge race]",
				"numMsgs":      strconv.FormatInt(numMsgs, 10),
			}).Error("ReadMessages done")
			result.Messages = append(
				result.Messages,
				newReadMessageContentError(
					fmt.Sprintf("InternalServiceError: %v store.Get(addr=%x) error: %v", extentID, addr, err)),
			)
			return
		}

		var appMsg *store.AppendMessage
		appMsg, err = deserializeMessage(val)

		if err != nil { // corrupt message?
			log.WithFields(bark.Fields{
				"error":        "deserializeMessage failed",
				"error-string": err.Error(),
				"addr":         strconv.FormatInt(int64(addr), 16),
				"len":          strconv.FormatInt(int64(len(val)), 10),
				"numMsgs":      strconv.FormatInt(numMsgs, 10),
			}).Error("ReadMessages done")
			result.Messages = append(
				result.Messages,
				newReadMessageContentError(
					fmt.Sprintf("InternalServiceError: %v error deserializing message (addr=%x, len=%d bytes): %v", extentID, addr, len(val), err)),
			)
			return
		}

		result.Messages = append(result.Messages, newReadMessageContent(addr, appMsg))

		numMsgs++ // track total messages sent
	}

	if numMsgs < numRequested {
		log.WithFields(bark.Fields{
			"error":   "End of extent",
			"extra":   "extent not sealed yet",
			"numMsgs": strconv.FormatInt(numMsgs, 10),
		}).Info("ReadMessages done")
		result.Messages = append(
			result.Messages,
			newReadMessageContentNoMoreMsgs(extentIDStr, fmt.Sprintf("NoMoreMessageError: %v", extentID)),
		)
	} else {
		log.WithFields(bark.Fields{
			"numMsgs": strconv.FormatInt(numMsgs, 10),
		}).Info("ReadMessages done")
	}

	return
}

// Shutdown storehost
func (t *StoreHost) Shutdown() {

	atomic.StoreInt32(&t.started, 0) // shutdown
	t.logger.Info("Storehost: shutting down")
	close(t.shutdownC) // 'broadcast' shutdown, by closing the shutdownC
	// wait until all connections have been closed
	if !common.AwaitWaitGroup(&t.shutdownWG, 30*time.Second) {
		t.logger.Error("Timed out waiting for store host to shutdown")
	}
}

// DisableWrite disables all the write
func (t *StoreHost) DisableWrite() {
	t.logger.Error("Write disabled")
	close(t.disableWriteC)
}

// EnableWrite enables write mode
func (t *StoreHost) EnableWrite() {
	t.disableWriteC = make(chan struct{})
}

// RegisterWSHandler is the implementation of WSService interface
func (t *StoreHost) RegisterWSHandler() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc(fmt.Sprintf(ccommon.HTTPHandlerPattern, ccommon.EndpointOpenAppendStream), t.OpenAppendStreamHandler)
	mux.HandleFunc(fmt.Sprintf(ccommon.HTTPHandlerPattern, ccommon.EndpointOpenReadStream), t.OpenReadStreamHandler)

	return mux
}

func (t *StoreHost) reportHostMetric(reporter common.LoadReporter, diffSecs int64) {
	msgsInPerSec := t.hostMetrics.GetAndReset(load.HostMetricMsgsWritten) / diffSecs
	msgsOutPerSec := t.hostMetrics.GetAndReset(load.HostMetricMsgsRead) / diffSecs
	bytesInPerSec := t.hostMetrics.GetAndReset(load.HostMetricBytesWritten) / diffSecs
	bytesOutPerSec := t.hostMetrics.GetAndReset(load.HostMetricBytesRead) / diffSecs
	numConns := t.hostMetrics.Get(load.HostMetricNumReadConns) + t.hostMetrics.Get(load.HostMetricNumWriteConns)

	hostMetrics := controller.NodeMetrics{
		IncomingMessagesCounter: common.Int64Ptr(msgsInPerSec),
		OutgoingMessagesCounter: common.Int64Ptr(msgsOutPerSec),
		IncomingBytesCounter:    common.Int64Ptr(bytesInPerSec),
		OutgoingBytesCounter:    common.Int64Ptr(bytesOutPerSec),
		NumberOfConnections:     common.Int64Ptr(numConns),
		NodeStatus:              common.NodeStatusPtr(t.GetNodeStatus()),
	}

	remDiskSpaceBytes := t.hostMetrics.Get(load.HostMetricFreeDiskSpaceBytes)
	if remDiskSpaceBytes > 0 {
		// the remaining disk space computation happens
		// as part of the storageMonitor thread and the
		// load reporter could be called before the storage
		// monitor gets a chance to do this computation.
		// Make sure we don't report zero values in the
		// meantime
		hostMetrics.RemainingDiskSpace = common.Int64Ptr(remDiskSpaceBytes)
	}

	reporter.ReportHostMetric(hostMetrics)
}

// Report is used for reporting Host specific load to controller
func (t *StoreHost) Report(reporter common.LoadReporter) {

	now := time.Now().UnixNano()
	diffSecs := (now - t.lastLoadReportedTime) / int64(time.Second)

	if diffSecs == 0 {
		return
	}

	t.reportHostMetric(reporter, diffSecs)
	t.lastLoadReportedTime = now
}

// resolveReplica finds the websocket host-port to connect to for re-replication
func getSourceReplicaHostPort(sCommon common.SCommon, replicaID uuid.UUID) (hostPort string, err error) {

	rpm := sCommon.GetRingpopMonitor()

	hostPort, err = rpm.ResolveUUID(common.StoreServiceName, replicaID.String())

	if err != nil {
		return
	}

	// HACK: for now replace the port
	host, _, _ := net.SplitHostPort(hostPort)

	port := os.Getenv("CHERAMI_STOREHOST_WS_PORT")

	if len(port) == 0 {
		port = strconv.Itoa(sCommon.GetConfig().GetWebsocketPort()) // 6191
	}

	return net.JoinHostPort(host, port), nil
}

func getReplicationArgsFromReplicateRequest(req *store.ReplicateExtentRequest) (args *replicationArgs, err error) {

	args = &replicationArgs{}

	// parse ExtentUUID
	if args.extentID = uuid.Parse(req.GetExtentUUID()); args.extentID == nil {
		return nil, newBadRequestError(fmt.Sprintf("BadRequestError: error parsing extent uuid (%s)", req.GetExtentUUID()))
	}

	// parse DestinationUUID
	if args.destID = uuid.Parse(req.GetDestinationUUID()); args.destID == nil {
		return nil, newBadRequestError(fmt.Sprintf("BadRequestError: error parsing destination uuid (%s)", req.GetDestinationUUID()))
	}

	// extract DestinationType
	args.destType = req.GetDestinationType()

	// find mode corresponding to extent
	args.mode = getModeForDestinationType(args.destType)

	if args.mode == 0 {
		return nil, newBadRequestError(fmt.Sprintf("BadRequestError: unknown type (%d)", args.mode))
	}

	// parse StoreUUID
	if args.sourceHostID = uuid.Parse(req.GetStoreUUID()); args.sourceHostID == nil {
		return nil, newBadRequestError(fmt.Sprintf("BadRequestError: error parsing store uuid (%s)", req.GetStoreUUID()))
	}

	return args, nil
}

func getReplicationArgsFromRemoteReplicateRequest(req *store.RemoteReplicateExtentRequest, sCommon common.SCommon) (args *replicationArgs, err error) {

	args = &replicationArgs{}

	// parse ExtentUUID
	if args.extentID = uuid.Parse(req.GetExtentUUID()); args.extentID == nil {
		return nil, newBadRequestError(fmt.Sprintf("BadRequestError: error parsing extent uuid (%s)", req.GetExtentUUID()))
	}

	// parse DestinationUUID
	if args.destID = uuid.Parse(req.GetDestinationUUID()); args.destID == nil {
		return nil, newBadRequestError(fmt.Sprintf("BadRequestError: error parsing destination uuid (%s)", req.GetDestinationUUID()))
	}

	// extract DestinationType
	args.destType = req.GetDestinationType()

	// find mode corresponding to extent
	args.mode = getModeForDestinationType(args.destType)
	if args.mode == 0 {
		return nil, newBadRequestError(fmt.Sprintf("BadRequestError: unknown type (%d)", args.mode))
	}

	rpm := sCommon.GetRingpopMonitor()
	hostInfo, err := rpm.FindRandomHost(common.ReplicatorServiceName)
	if err != nil {
		return nil, newInternalServiceError(fmt.Sprintf("error finding replicator host : (%v)", err))
	}

	if args.sourceHostID = uuid.Parse(hostInfo.UUID); args.sourceHostID == nil {
		return nil, newInternalServiceError(fmt.Sprintf("error parsing replicator uuid (%v)", hostInfo.UUID))
	}

	host, _, _ := net.SplitHostPort(hostInfo.Addr)
	port := os.Getenv("CHERAMI_REPLICATOR_WS_PORT")
	if len(port) == 0 {
		port = "6310"
	}

	args.sourceHostPort = net.JoinHostPort(host, port)

	return args, nil
}

// ReplicateExtent creates a replica of the extent in the local store
func (t *StoreHost) ReplicateExtent(tCtx thrift.Context, req *store.ReplicateExtentRequest) (err error) {

	if atomic.LoadInt32(&t.started) == 0 {
		return newInternalServiceError("StoreHost not started")
	}

	log := t.logger.WithFields(bark.Fields{
		common.TagDst: common.FmtExt(req.GetDestinationUUID()),
		common.TagExt: common.FmtExt(req.GetExtentUUID()),
	})

	args, err := getReplicationArgsFromReplicateRequest(req)

	if err != nil {
		log.WithFields(bark.Fields{
			`context`:     `error parsing arguments`,
			common.TagErr: err,
		}).Error("ReplicateExtent done")

		return err
	}

	log.WithFields(bark.Fields{
		"destType":        args.destType,
		common.TagDst:     common.FmtDst(args.destID.String()),
		common.TagExt:     common.FmtExt(args.extentID.String()),
		"mode":            args.mode,
		"sourceReplicaID": req.GetStoreUUID(),
	}).Info("ReplicateExtent request")

	// we currently only support replicating PLAIN/LOG destinations
	if args.destType != cherami.DestinationType_PLAIN &&
		args.destType != cherami.DestinationType_LOG {

		err = fmt.Errorf("unsupported destination type: %v", args.destType)

		log.WithFields(bark.Fields{
			"destType":        args.destType,
			common.TagDst:     common.FmtDst(args.destID.String()),
			common.TagExt:     common.FmtExt(args.extentID.String()),
			"mode":            args.mode,
			"sourceReplicaID": args.sourceHostID,
			common.TagErr:     err,
		}).Error("ReplicateExtent done")

		return newBadRequestError(err.Error())
	}

	args.sourceHostPort, err = getSourceReplicaHostPort(t.SCommon, args.sourceHostID)

	if err != nil {

		log.WithFields(bark.Fields{
			`destType`:        args.destType,
			common.TagDst:     common.FmtDst(args.destID.String()),
			common.TagExt:     common.FmtExt(args.extentID.String()),
			`mode`:            args.mode,
			`sourceReplicaID`: req.GetStoreUUID(),
			common.TagErr:     err,
		}).Error(`ReplicateExtent: error resolving storehost`)

		return newInternalServiceError(fmt.Sprintf("error resolving storehost: %v", err))
	}

	job := t.replMgr.NewJob(JobTypeReReplication, args, log)

	t.shutdownWG.Add(1)
	defer t.shutdownWG.Done()

	err = job.Start() // start the replication job(the actual message replication will be done asynchronously)
	if err != nil {
		log.WithFields(bark.Fields{
			"destType":        args.destType,
			common.TagDst:     common.FmtDst(args.destID.String()),
			common.TagExt:     common.FmtExt(args.extentID.String()),
			"mode":            args.mode,
			"sourceReplicaID": args.sourceHostID,
			common.TagErr:     err,
		}).Error("ReplicateExtent done")
		return newInternalServiceError(err.Error())
	}

	return nil
}

// RemoteReplicateExtent replicates a remote extent from replicator
func (t *StoreHost) RemoteReplicateExtent(tCtx thrift.Context, req *store.RemoteReplicateExtentRequest) (err error) {

	if atomic.LoadInt32(&t.started) == 0 {
		return newInternalServiceError("StoreHost not started")
	}

	log := t.logger.WithFields(bark.Fields{
		common.TagDst: common.FmtExt(req.GetDestinationUUID()),
		common.TagExt: common.FmtExt(req.GetExtentUUID()),
	})

	args, err := getReplicationArgsFromRemoteReplicateRequest(req, t.SCommon)

	if err != nil {

		log.WithFields(bark.Fields{
			`context`:     `error parsing arguments`,
			common.TagErr: err,
		}).Error("RemoteReplicateExtent done")

		return err
	}

	log.WithFields(bark.Fields{
		"destType":    args.destType,
		common.TagDst: common.FmtDst(args.destID.String()),
		common.TagExt: common.FmtExt(args.extentID.String()),
		"mode":        args.mode,
	}).Info("RemoteReplicateExtent request")

	// we currently only support replicating PLAIN destinations
	if args.destType != cherami.DestinationType_PLAIN {

		err = fmt.Errorf("unsupported destination type: %v", args.destType)

		log.WithFields(bark.Fields{
			"destType":    args.destType,
			common.TagDst: common.FmtDst(args.destID.String()),
			common.TagExt: common.FmtExt(args.extentID.String()),
			"mode":        args.mode,
			common.TagErr: err,
		}).Error("RemoteReplicateExtent done")

		return newBadRequestError(err.Error())
	}

	job := t.replMgr.NewJob(JobTypeRemoteReplication, args, log)

	t.shutdownWG.Add(1)
	defer t.shutdownWG.Done()

	err = job.Start() // start the replication job(the actual message replication will be done asynchronously)
	if err != nil {
		log.WithFields(bark.Fields{
			"destType":    args.destType,
			common.TagDst: common.FmtDst(args.destID.String()),
			common.TagExt: common.FmtExt(args.extentID.String()),
			"mode":        args.mode,
			common.TagErr: err,
		}).Error("RemoteReplicateExtent done")
		return newInternalServiceError(err.Error())
	}

	return nil
}

// ListExtents lists extents available on this storehost
func (t *StoreHost) ListExtents(tCtx thrift.Context) (res *store.ListExtentsResult_, err error) {

	t.m3Client.IncCounter(metrics.ListExtentsScope, metrics.StorageRequests)

	if atomic.LoadInt32(&t.started) == 0 {
		t.m3Client.IncCounter(metrics.ListExtentsScope, metrics.StorageFailures)
		return nil, newInternalServiceError("StoreHost not started")
	}

	sw := t.m3Client.StartTimer(metrics.ListExtentsScope, metrics.StorageLatencyTimer)
	defer sw.Stop()

	log := t.logger

	extents, lsxErr := t.xMgr.ListExtents()

	if lsxErr != nil {

		log.WithField(common.TagErr, lsxErr).Error(`ListExtents failed`)

		svcErr := store.NewStoreServiceError()
		svcErr.Message = lsxErr.Error()

		return nil, svcErr
	}

	res = store.NewListExtentsResult_()

	res.Extents = make([]*store.ListExtentsElem, len(extents))

	for i, x := range extents {

		res.Extents[i] = store.NewListExtentsElem()
		res.Extents[i].ExtentUUID = common.StringPtr(x)
		// res.Extents[i].DestinationUUID  = common.StringPtr(...) // TODO: currently unavailable

		if info, gxiErr := t.xMgr.GetExtentInfo(x); gxiErr == nil {

			res.Extents[i].Size = common.Int64Ptr(info.Size)
			res.Extents[i].ModifiedTime = common.Int64Ptr(info.Modified)
		}
	}

	return res, nil
}

// UpgradeHandler implements the upgrade end point
func (t *StoreHost) UpgradeHandler(w http.ResponseWriter, r *http.Request) {
	t.logger.Info("Upgrade endpoint called on storehost")
	// just report to controller as GOING_DOWN
	t.SetNodeStatus(controller.NodeStatus_GOING_DOWN)
	reporter := t.loadReporter.GetReporter()
	// report as down
	t.reportHostMetric(reporter, 1)

	// wait for upgrade timeout
	time.Sleep(common.DefaultUpgradeTimeout)

	// at this point, we have marked ourself as down and waited for the timeout period. Exit since we are no longer useful
	os.Exit(0)
}

// GetNodeStatus is the current status of this host
func (t *StoreHost) GetNodeStatus() controller.NodeStatus {
	return t.nodeStatus.Load().(controller.NodeStatus)
}

// SetNodeStatus sets the status of this host
func (t *StoreHost) SetNodeStatus(status controller.NodeStatus) {
	t.nodeStatus.Store(status)
}
