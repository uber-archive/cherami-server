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
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	metadataClient "github.com/uber/cherami-server/clients/metadata"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
	"github.com/uber/cherami-server/test"
	mockmeta "github.com/uber/cherami-server/test/mocks/metadata"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/store"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/tchannel-go/thrift"
)

// var runShort = flag.Bool("short", false, "do a short run") // "-short" to do a short run (already defined by package 'suite')
var runLong = flag.Bool("long", false, "do a long run")                             // "-long" to do a long run
var logLevel = flag.String("log", "", "enabling stdout logging at specified level") // "-log=<level>" to enable log level
var enablePerMessageLogs = flag.Bool("msg", false, "enable per message logs")       // "-msg" enable per message logs (requires '-vv')

var testStore = ManyRocks
var testDestinationUUID = "00000000-0000-0000-0000-000000000000"
var testConsumerGroupUUID = "11111111-1111-1111-1111-111111111111"

const randBufSize = 64 * 1048576 // 64 MiB
var randBuf = make([]byte, randBufSize)

type (
	StoreHostSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions;
		// this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
		cfg configure.CommonAppConfig
		metadataClient.TestCluster

		testBase   *testBase
		storehost0 *testStoreHost
		storehost1 *testStoreHost
		baseDir    string
	}

	testStoreHost struct {
		hostID    uuid.UUID
		storehost *StoreHost
		hostPort  string
		port      int
		wsPort    int
	}

	testBase struct {
		hwInfoReader common.HostHardwareInfoReader
		uuidResolver *mockUUIDResolver
		mClient      *mockmeta.TChanMetadataService

		baseDir     string
		ringhosts   string
		listenHosts map[string]string
		storehosts  []*testStoreHost
	}
)

func TestStoreHostSuite(t *testing.T) {
	suite.Run(t, new(StoreHostSuite))
}

func (s *StoreHostSuite) RunShort() bool {
	return testing.Short()
}
func (s *StoreHostSuite) RunLong() bool {
	return *runLong
}

// SetupSuite will run once before any of the tests in the suite are run
func (s *StoreHostSuite) SetupSuite() {

	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil

	s.TestCluster.SetupTestCluster()

	s.cfg = common.SetupServerConfig(configure.NewCommonConfigure())

	if *logLevel != "" {

		log.SetOutput(os.Stdout) // test output to stdout

		// enable logs at specified level
		switch *logLevel {
		case "debug":
			log.SetLevel(log.DebugLevel)
		case "info":
			log.SetLevel(log.InfoLevel)
		case "error":
			log.SetLevel(log.ErrorLevel)
		}
	}

	testBaseDir, _ := ioutil.TempDir("", "storehost-test")
	s.testBase = newTestBase(testBaseDir)

	// setup storehosts to use for tests
	s.storehost0 = s.testBase.newTestStoreHost(testStore)
	s.storehost1 = s.testBase.newTestStoreHost(testStore)

	// sleep until the ringpop on the second store refreshes and finds the
	// first one; we do this because we were seeing 'error resolving uuid'
	// when ReplicateExtent tries to resolve/connect to the source replica
	time.Sleep(1250 * time.Millisecond)

	s.baseDir = testBaseDir

	rand.Read(randBuf) // fill randBuf with random bytes
}

func newTestBase(baseDir string) *testBase {

	t := &testBase{
		baseDir:      baseDir,
		uuidResolver: newMockUUIDResolver(),
		hwInfoReader: newMockHostInfoReader(),
		mClient:      new(mockmeta.TChanMetadataService),
	}

	t.mClient.On("RegisterHostUUID", mock.Anything, mock.Anything).Return(nil)
	t.mClient.On("UpdateStoreExtentReplicaStats", mock.Anything, mock.Anything).Return(nil)
	t.mClient.On("ListStoreExtentsStats", mock.Anything, mock.Anything).Return(metadata.NewListStoreExtentsStatsResult_(), nil)

	return t
}

// not re-entrant (ie, cannot be called multiple times concurrently)
func (t *testBase) newTestStoreHost(store Store) *testStoreHost {

	hostID := uuid.NewRandom()

	hostPort, listenIP, port, _ := test.FindEphemeralPort()

	// if listenHosts is 'nil', this is the first storehost,
	// so use its hostPort that for listenHosts and ringhosts
	if t.listenHosts == nil {
		t.listenHosts = map[string]string{common.StoreServiceName: hostPort}
		t.ringhosts = hostPort
	}

	t.uuidResolver.Set(hostID.String(), hostPort)

	cfg := &configure.ServiceConfig{
		Port:          port,
		RingHosts:     t.ringhosts,
		LimitsEnabled: false,
		ListenAddress: listenIP,
	}

	reporter := common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig())
	dClient := dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.StoreServiceName)

	sCommon := common.NewService(common.StoreServiceName, hostID.String(), cfg, t.uuidResolver, t.hwInfoReader, reporter, dClient, common.NewBypassAuthManager())

	storehost, tc := NewStoreHost(common.StoreServiceName, sCommon, t.mClient, &Options{Store: store, BaseDir: t.baseDir})

	storehost.Start(tc)

	_, _, wsPort, _ := test.FindEphemeralPort()
	common.WSStart(``, wsPort, storehost) // start websocket server, listen on all addresses

	sh := &testStoreHost{
		hostID:    hostID,
		hostPort:  hostPort,
		port:      port,
		storehost: storehost,
		wsPort:    wsPort,
	}

	t.storehosts = append(t.storehosts, sh)

	return sh
}

func (t *testBase) cleanup() {

	for _, sh := range t.storehosts {
		sh.storehost.Shutdown()
		sh.storehost.Stop()
	}
}

// TeardownSuite is called after all the test in the suite are run
func (s *StoreHostSuite) TeardownSuite() {

	s.testBase.cleanup()

	os.RemoveAll(s.baseDir)

	log.SetLevel(log.GetLevel())
}

// SetupTest will run before each test
func (s *StoreHostSuite) SetupTest() {
}

// TearDownTest is called after every test finishes
func (s *StoreHostSuite) TearDownTest() {
}

// wait for given 'timeout' (in milliseconds) for condition 'cond' to satisfy
func waitFor(timeout int, cond func() bool) bool {
	for i := 0; i < timeout/10; i++ {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func isDone(errC <-chan error) bool {
	select {
	case <-errC:
		return true
	default:
		return false
	}
}

func tCtxAppend(extentID uuid.UUID, mode Mode) (ctx thrift.Context) {

	var destType cherami.DestinationType

	switch mode {
	case AppendOnly:
		destType = cherami.DestinationType_PLAIN
	case TimerQueue:
		destType = cherami.DestinationType_TIMER
	case Log:
		destType = cherami.DestinationType_LOG
	default:
		log.Errorf("unknown mode (%d)", mode)
		return nil
	}

	req := &store.OpenAppendStreamRequest{
		DestinationUUID: common.StringPtr(testDestinationUUID),
		DestinationType: cherami.DestinationTypePtr(destType),
		ExtentUUID:      common.StringPtr(extentID.String()),
	}

	ctx, _ = thrift.NewContext(0)
	return thrift.WithHeaders(ctx, common.GetOpenAppendStreamRequestHeaders(req))
}

func tCtxRead(extentID uuid.UUID, mode Mode) (ctx thrift.Context) {

	var destType cherami.DestinationType

	switch mode {
	case AppendOnly:
		destType = cherami.DestinationType_PLAIN
	case TimerQueue:
		destType = cherami.DestinationType_TIMER
	case Log:
		destType = cherami.DestinationType_LOG
	default:
		log.Errorf("unknown mode (%d)", mode)
		return nil
	}

	req := &store.OpenReadStreamRequest{
		DestinationUUID:   common.StringPtr(testDestinationUUID),
		DestinationType:   cherami.DestinationTypePtr(destType),
		ExtentUUID:        common.StringPtr(extentID.String()),
		ConsumerGroupUUID: common.StringPtr(testConsumerGroupUUID),
		Address:           common.Int64Ptr(store.ADDR_BEGIN), // start reading from the first address the beginning
		Inclusive:         common.BoolPtr(false),             // non-inclusive
	}

	ctx, _ = thrift.NewContext(0)
	return thrift.WithHeaders(ctx, common.GetOpenReadStreamRequestHeaders(req))
}

type inhostStreamMock struct {
	sendC chan *store.AppendMessage
	recvC chan *store.AppendMessageAck
	doneC chan struct{}

	sentMsgs int32
	recvAcks int32
	recvNaks int32

	lastAckPtr unsafe.Pointer
}

func newInhostStreamMock(sendCBuf, recvCBuf int) *inhostStreamMock {

	lastAck := store.NewAppendMessageAck()

	lastAck.SequenceNumber = common.Int64Ptr(0)
	lastAck.Address = common.Int64Ptr(0)
	lastAck.Status = cherami.StatusPtr(cherami.Status_FAILED)
	lastAck.Message = common.StringPtr("")

	return &inhostStreamMock{
		sendC:      make(chan *store.AppendMessage, sendCBuf),
		recvC:      make(chan *store.AppendMessageAck, recvCBuf),
		doneC:      make(chan struct{}),
		lastAckPtr: unsafe.Pointer(lastAck),
	}
}

func (t *inhostStreamMock) isDone() bool {
	select {
	case <-t.doneC:
		return true
	default:
		return false
	}
}

func (t *inhostStreamMock) acksRecv() int {
	return int(atomic.LoadInt32(&t.recvAcks))
}

func (t *inhostStreamMock) msgsSent() int {
	return int(atomic.LoadInt32(&t.sentMsgs))
}

func (t *inhostStreamMock) lastAck() *store.AppendMessageAck {
	return (*store.AppendMessageAck)(atomic.LoadPointer(&t.lastAckPtr))
}

func (t *inhostStreamMock) SetResponseHeaders(headers map[string]string) error {
	return nil
}

func (t *inhostStreamMock) Read() (*store.AppendMessage, error) {
	select {
	case msg, ok := <-t.sendC:
		if !ok {
			return nil, io.EOF
		}
		atomic.AddInt32(&t.sentMsgs, 1)

		if *enablePerMessageLogs {
			log.Debugf("inhostStreamMock: MSG seq=%d enq=%x", msg.GetSequenceNumber(), msg.GetEnqueueTimeUtc())
		}

		return msg, nil
	case <-t.doneC:
		return nil, io.EOF
	}
}

func (t *inhostStreamMock) Write(ack *store.AppendMessageAck) error {

	if ack.GetStatus() == cherami.Status_OK {
		atomic.AddInt32(&t.recvAcks, 1) // count acks received

		if *enablePerMessageLogs {
			log.Debugf("inhostStreamMock: ACK seq=%d address=%x", ack.GetSequenceNumber(), ack.GetAddress())
		}

	} else {
		atomic.AddInt32(&t.recvNaks, 1) // count n-acks received

		log.Debugf("inhostStreamMock: NACK seq=%d address=%x", ack.GetSequenceNumber(), ack.GetAddress())
	}

	atomic.StorePointer(&t.lastAckPtr, unsafe.Pointer(ack))

	select {
	case t.recvC <- ack:
		return nil
	case <-t.doneC:
		close(t.recvC)
		return io.EOF
	default:
		// if recvC is blocked, move on .. since we already counted acks
		return nil
	}
}

func (t *inhostStreamMock) Flush() error {
	// TODO: simulate tchannel "Flush" behaviour, by buffering up
	// 'n' writes and sending them out on flush, etc.
	return nil
}

func (t *inhostStreamMock) Done() error {
	close(t.doneC)
	close(t.recvC)
	return nil
}

type outhostStreamMock struct {
	sendC chan *cherami.ControlFlow
	recvC chan *store.ReadMessageContent
	doneC chan struct{}

	sentCred int32
	recvMsgs int32

	lastMsgPtr unsafe.Pointer
}

func newOuthostStreamMock(sendCBuf, recvCBuf int) *outhostStreamMock {

	lastMsg := store.NewReadMessageContent()

	lastMsg.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_ERROR)
	lastMsg.Error = store.NewStoreServiceError()

	return &outhostStreamMock{
		sendC:      make(chan *cherami.ControlFlow, sendCBuf),
		recvC:      make(chan *store.ReadMessageContent, recvCBuf),
		doneC:      make(chan struct{}),
		lastMsgPtr: unsafe.Pointer(lastMsg),
	}
}

func (t *outhostStreamMock) isDone() bool {
	select {
	case <-t.doneC:
		return true
	default:
		return false
	}
}

func (t *outhostStreamMock) msgsRecv() int {
	return int(atomic.LoadInt32(&t.recvMsgs))
}

func (t *outhostStreamMock) credsSent() int {
	return int(atomic.LoadInt32(&t.sentCred))
}

func (t *outhostStreamMock) lastMsg() *store.ReadMessageContent {
	return (*store.ReadMessageContent)(atomic.LoadPointer(&t.lastMsgPtr))
}

func (t *outhostStreamMock) SetResponseHeaders(headers map[string]string) error {
	return nil
}

func (t *outhostStreamMock) Read() (*cherami.ControlFlow, error) {
	select {
	case cf, ok := <-t.sendC:

		if !ok {
			close(t.doneC)
			return nil, io.EOF
		}

		atomic.AddInt32(&t.sentCred, cf.GetCredits())

		if *enablePerMessageLogs {
			log.Debugf("outhostStreamMock: CRED credits=%d", cf.GetCredits())
		}

		return cf, nil
	case <-t.doneC:
		return nil, io.EOF
	}
}

func (t *outhostStreamMock) Write(msg *store.ReadMessageContent) error {

	// remember last message that we got
	atomic.StorePointer(&t.lastMsgPtr, unsafe.Pointer(msg))

	if *enablePerMessageLogs {

		switch msg.GetType() {
		case store.ReadMessageContentType_MESSAGE:
			log.Debugf("outhostStreamMock: msg-msg seq=%d addr=%x", msg.GetMessage().GetMessage().GetSequenceNumber(), msg.GetMessage().GetAddress())

		case store.ReadMessageContentType_SEALED:
			log.Debugf("outhostStreamMock: msg-seal seq=%d", msg.GetSealed().GetSequenceNumber())

		case store.ReadMessageContentType_ERROR:
			log.Debugf("outhostStreamMock: msg-err %v", msg.GetError().GetMessage())
		}
	}

	switch msg.GetType() {
	case store.ReadMessageContentType_MESSAGE:

		atomic.AddInt32(&t.recvMsgs, 1) // count messages received

	case store.ReadMessageContentType_SEALED:
	case store.ReadMessageContentType_ERROR:
	}

	select {
	case t.recvC <- msg:
		return nil
	case <-t.doneC:
		close(t.recvC)
		return io.EOF
	default:
		// if recvC is blocked, move on .. since we already counted msgs
		return nil
	}
}

func (t *outhostStreamMock) Flush() error {
	// TODO: simulate tchannel "Flush" behaviour, by buffering up
	// 'n' writes and sending them out on flush, etc.
	return nil
}

func (t *outhostStreamMock) Done() error {
	close(t.doneC)
	close(t.recvC)
	return nil
}

func newAppendMessage(seqNum, delay, dataSize int, r *rand.Rand) *store.AppendMessage {

	var randBufOffs int

	if r != nil {
		randBufOffs = r.Intn(randBufSize - dataSize)
	} else {
		randBufOffs = rand.Intn(randBufSize - dataSize)
	}

	return &store.AppendMessage{
		SequenceNumber: common.Int64Ptr(int64(seqNum)),
		EnqueueTimeUtc: common.Int64Ptr(time.Now().UnixNano()),
		Payload: &cherami.PutMessage{
			ID: common.StringPtr(fmt.Sprintf("ID%08X", seqNum)),
			DelayMessageInSeconds: common.Int32Ptr(int32(delay)),
			Data: randBuf[randBufOffs : randBufOffs+dataSize],
		},
	}
}

func newWatermarkMessage(seqNum, watermark int) *store.AppendMessage {

	return &store.AppendMessage{
		SequenceNumber:           common.Int64Ptr(int64(seqNum)),
		EnqueueTimeUtc:           common.Int64Ptr(time.Now().UnixNano()),
		FullyReplicatedWatermark: common.Int64Ptr(int64(watermark)),
	}
}

func validateAppendMessage(msg *store.AppendMessage, seqNum, delay, dataSize int, r *rand.Rand) bool {

	var randBufOffs int

	if r != nil {
		randBufOffs = r.Intn(randBufSize - dataSize)
	} else {
		randBufOffs = rand.Intn(randBufSize - dataSize)
	}

	if msg.GetSequenceNumber() != int64(seqNum) {
		return false
	}

	if msg.GetPayload().GetDelayMessageInSeconds() != int32(delay) {
		return false
	}

	data := msg.GetPayload().GetData()

	if len(data) != dataSize {
		return false
	}

	for i := range data {
		if data[i] != randBuf[randBufOffs+i] {
			return false
		}
	}

	return true
}

func newControlFlow(credits int) *cherami.ControlFlow {
	return &cherami.ControlFlow{Credits: common.Int32Ptr(int32(credits))}
}

func (t *testStoreHost) OpenAppendStream(extent uuid.UUID, mode Mode) (*inhostStreamMock, <-chan error) {

	inStream := newInhostStreamMock(1024, 32)
	errC := make(chan error, 1)

	go func() {
		thriftCtx := tCtxAppend(extent, mode)
		errC <- t.storehost.OpenAppendStream(thriftCtx, inStream)
		close(errC)
	}()

	return inStream, errC
}

func (t *testStoreHost) OpenReadStream(extent uuid.UUID, mode Mode) (*outhostStreamMock, <-chan error) {

	outStream := newOuthostStreamMock(1024, 32)
	errC := make(chan error, 1)

	go func() {
		thriftCtx := tCtxRead(extent, mode)
		errC <- t.storehost.OpenReadStream(thriftCtx, outStream)
		close(errC)
	}()

	return outStream, errC
}

func (t *testStoreHost) OpenReadStreamBuffered(extent uuid.UUID, mode Mode, sendCBuf, recvCBuf int) (*outhostStreamMock, <-chan error) {

	outStream := newOuthostStreamMock(sendCBuf, recvCBuf)
	errC := make(chan error, 1)

	go func() {
		thriftCtx := tCtxRead(extent, mode)
		errC <- t.storehost.OpenReadStream(thriftCtx, outStream)
		close(errC)
	}()

	return outStream, errC
}

func (t *testStoreHost) SealExtent(extent uuid.UUID, sealSeqNum int64) error {

	req := store.NewSealExtentRequest()
	req.ExtentUUID = common.StringPtr(extent.String())

	if sealSeqNum != math.MaxInt64-1 {
		req.SequenceNumber = common.Int64Ptr(sealSeqNum)
	}

	return t.storehost.SealExtent(nil, req)
}

func (t *testStoreHost) GetAddressFromTimestamp(extent uuid.UUID, timestamp int64) (addr int64, err error) {

	req := store.NewGetAddressFromTimestampRequest()
	req.ExtentUUID = common.StringPtr(extent.String())
	req.Timestamp = common.Int64Ptr(timestamp)

	res, err := t.storehost.GetAddressFromTimestamp(nil, req)

	if err == nil {
		addr = res.GetAddress()
	}

	return
}

func (t *testStoreHost) PurgeMessages(extent uuid.UUID, addr int64) (nextAddr int64, err error) {

	req := store.NewPurgeMessagesRequest()
	req.ExtentUUID = common.StringPtr(extent.String())
	req.Address = common.Int64Ptr(addr)

	res, err := t.storehost.PurgeMessages(nil, req)

	if err == nil {
		nextAddr = res.GetAddress()
	}

	return
}

func (t *testStoreHost) ReadMessages(
	extent uuid.UUID, startAddr int64, numMessages int32, startAddressInclusive bool) (msgs []*store.ReadMessageContent, err error) {

	req := store.NewReadMessagesRequest()
	req.ExtentUUID = common.StringPtr(extent.String())
	req.StartAddress = common.Int64Ptr(startAddr)
	req.NumMessages = common.Int32Ptr(numMessages)
	req.StartAddressInclusive = common.BoolPtr(startAddressInclusive)

	res, err := t.storehost.ReadMessages(nil, req)

	if res != nil {
		msgs = res.GetMessages()
	}

	return
}

func (t *testStoreHost) ReplicateExtent(extentID uuid.UUID, mode Mode, sourceReplicaID uuid.UUID) (err error) {

	var destType cherami.DestinationType

	switch mode {
	case AppendOnly:
		destType = cherami.DestinationType_PLAIN
	case TimerQueue:
		destType = cherami.DestinationType_TIMER
	case Log:
		destType = cherami.DestinationType_LOG
	default:
		log.Errorf("unknown mode (%d)", mode)
		return nil
	}
	req := &store.ReplicateExtentRequest{
		DestinationUUID: common.StringPtr(testDestinationUUID),
		DestinationType: cherami.DestinationTypePtr(destType),
		ExtentUUID:      common.StringPtr(extentID.String()),
		StoreUUID:       common.StringPtr(sourceReplicaID.String()),
	}

	err = t.storehost.ReplicateExtent(nil, req)

	return
}
