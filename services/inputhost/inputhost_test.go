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
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/inputhost/load"
	mockcommon "github.com/uber/cherami-server/test/mocks/common"
	mockin "github.com/uber/cherami-server/test/mocks/inputhost"
	mockmeta "github.com/uber/cherami-server/test/mocks/metadata"
	mockstore "github.com/uber/cherami-server/test/mocks/storehost"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
	"golang.org/x/net/context"
)

type InputHostSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
	cfg                   configure.CommonAppConfig
	mockPub               *mockin.MockBInOpenPublisherStreamInCall
	mockStore             *mockstore.MockStoreHost
	mockAppend            *mockstore.MockBStoreOpenAppendStreamOutCall
	mockService           *common.MockService
	mockMeta              *mockmeta.TChanMetadataService
	mockClientFactory     *mockcommon.MockClientFactory
	mockWSConnector       *mockcommon.MockWSConnector
	mockRpm               *common.MockRingpopMonitor
	desinationDescription *shared.DestinationDescription
}

// TODO: move this to mocks
type mockTimeSource struct {
	lk       sync.RWMutex
	currTime time.Time
}

func (ts *mockTimeSource) Now() time.Time {
	ts.lk.RLock()
	defer ts.lk.RUnlock()
	return ts.currTime
}

func (ts *mockTimeSource) advance(d time.Duration) {
	ts.lk.Lock()
	defer ts.lk.Unlock()
	ts.currTime = ts.currTime.Add(d)
}

func TestInputHostSuite(t *testing.T) {
	// Setup all the common stuff for this suite right here
	s := new(InputHostSuite)
	s.cfg = common.SetupServerConfig(configure.NewCommonConfigure())

	suite.Run(t, s)
}

// TODO: refactor the common setup once we start adding more tests
// as it makes sense
func (s *InputHostSuite) SetupCommonMock() {
	s.mockPub = new(mockin.MockBInOpenPublisherStreamInCall)
	s.mockStore = new(mockstore.MockStoreHost)
	s.mockAppend = new(mockstore.MockBStoreOpenAppendStreamOutCall)
	s.mockService = new(common.MockService)
	s.mockClientFactory = new(mockcommon.MockClientFactory)
	s.mockMeta = new(mockmeta.TChanMetadataService)
	s.mockWSConnector = new(mockcommon.MockWSConnector)
	ch, _ := tchannel.NewChannel("cherami-test-in", nil)

	s.mockAppend.On("Done").Return(nil)
	s.mockAppend.On("Flush").Return(nil)
	s.mockPub.On("Done").Return(nil)
	s.mockPub.On("Flush").Return(nil)
	s.mockService.On("GetConfig").Return(s.cfg.GetServiceConfig(common.InputServiceName))

	/*
		scfg := s.cfg.ServiceConfig[common.InputServiceName]
		uConfig, uerr := uconfig.AdvancedUConfig(scfg.ClayRuntimeConfig, common.InputServiceName,
			uconfig.OptionalConfigs{
				TimeBetweenSyncSeconds: scfg.TimeBetweenSyncSeconds,
			})

		log.WithField("TimeBetweenSyncSeconds", scfg.TimeBetweenSyncSeconds).
			Infof("Passing the uconfig ConfigDefinaitons for mock servic: %s is %v \n, to the Uconfig client.\n", common.InputServiceName, scfg.ClayRuntimeConfig)

		if uerr != nil {
			log.Fatalf("Error initializing uconfig configuration in test: %s", uerr)
		}
		s.mockService.On("GetUConfig").Return(uConfig)
	*/
	s.mockService.On("GetTChannel").Return(ch)
	s.mockService.On("GetHostPort").Return("inputhost:port")
	s.mockService.On("IsLimitsEnabled").Return(true)
	s.mockService.On("GetHostUUID").Return("99999999-9999-9999-9999-999999999999")
	s.mockService.On("GetMetricsReporter").Return(common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig()))
	s.mockService.On("GetDConfigClient").Return(dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.InputServiceName))

	s.mockClientFactory.On("GetThriftStoreClient", mock.Anything, mock.Anything).Return(store.TChanBStore(s.mockStore), nil)
	s.mockService.On("GetWSConnector").Return(s.mockWSConnector, nil)
	s.mockClientFactory.On("ReleaseThriftStoreClient", mock.Anything).Return(nil)
	s.mockClientFactory.On("GetAdminControllerClient").Return(nil, fmt.Errorf("no real controller"))

	s.mockWSConnector.On("OpenAppendStream", mock.Anything, mock.Anything).Return(s.mockAppend, nil)
	s.desinationDescription = shared.NewDestinationDescription()
	s.desinationDescription.DestinationUUID = common.StringPtr(uuid.New())

	mExt := shared.NewExtent()
	mExt.ExtentUUID = common.StringPtr(uuid.New())
	mExt.InputHostUUID = common.StringPtr(uuid.New())
	mExt.StoreUUIDs = make([]string, 0, 4)

	s.mockStore.On("OpenAppendStream", mock.Anything).Return(s.mockAppend, nil).Once()

	mExtStats := shared.NewExtentStats()
	mExtStats.Extent = mExt
	mExtStats.Status = common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN)

	rpm := common.NewMockRingpopMonitor()
	rpm.Add(common.InputServiceName, mExt.GetInputHostUUID(), "127.0.0.1")

	for i := 1; i < 2; i++ {
		uuid := uuid.New()
		rpm.Add(common.StoreServiceName, uuid, "127.0.2."+strconv.Itoa(i))
		mExt.StoreUUIDs = append(mExt.StoreUUIDs, uuid)
	}

	s.mockRpm = rpm
	mListExtStats := metadata.NewListInputHostExtentsStatsResult_()
	mListExtStats.ExtentStatsList = []*shared.ExtentStats{mExtStats}
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(s.desinationDescription, nil)
	s.mockMeta.On("ListInputHostExtentsStats", mock.Anything, mock.Anything).Return(mListExtStats, nil)
	s.mockService.On("GetRingpopMonitor").Return(s.mockRpm)
	s.mockService.On("GetClientFactory").Return(s.mockClientFactory)

	mockLoadReporterDaemonFactory := setupMockLoadReporterDaemonFactory()
	s.mockService.On("GetLoadReporterDaemonFactory").Return(mockLoadReporterDaemonFactory)
}

func (s *InputHostSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.SetupCommonMock()
}

func (s *InputHostSuite) TearDownTest() {
}

// utility routine to get thrift context
func utilGetThriftContext() (thrift.Context, context.CancelFunc) {
	return thrift.NewContext(20 * time.Second)
}

// utility routine to get thrift context with path
func utilGetThriftContextWithPath(path string) (thrift.Context, context.CancelFunc) {
	ctx, cancel := utilGetThriftContext()
	headers := make(map[string]string)
	headers["path"] = "foo"
	ctx = thrift.WithHeaders(ctx, headers)
	return ctx, cancel
}

// TestInputHostRejectNoPath tests to make sure we
// fail, if there is no path given to publish
func (s *InputHostSuite) TestInputHostRejectNoPath() {
	sName := "chermai-test-in"
	inputHost, _ := NewInputHost(sName, s.mockService, nil, nil)
	ctx, _ := utilGetThriftContext()
	err := inputHost.OpenPublisherStream(ctx, s.mockPub)
	s.Error(err)
	assert.IsType(s.T(), &cherami.BadRequestError{}, err)
}

// TestInputHostNoReplicas tests to make sure we cannot
// publish, if the replica set is empty
func (s *InputHostSuite) TestInputHostNoReplicas() {

	sName := "chermai-test-in"

	mExt := shared.Extent{
		ExtentUUID:    common.StringPtr(uuid.New()),
		InputHostUUID: common.StringPtr(uuid.New()),
		StoreUUIDs:    make([]string, 0, 3),
	}
	mExtStats := shared.ExtentStats{
		Extent: &mExt,
		Status: common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN),
	}

	mListExtStats := metadata.NewListInputHostExtentsStatsResult_()
	mListExtStats.ExtentStatsList = []*shared.ExtentStats{&mExtStats}
	mockMeta := new(mockmeta.TChanMetadataService)
	mockMeta.On("ListInputHostExtentsStats", mock.Anything, mock.Anything).Return(mListExtStats, nil)

	destDesc := shared.DestinationDescription{
		DestinationUUID: common.StringPtr(uuid.New()),
	}
	mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(&destDesc, nil)

	inputHost, _ := NewInputHost(sName, s.mockService, mockMeta, nil)
	ctx, _ := utilGetThriftContextWithPath("foo")
	err := inputHost.OpenPublisherStream(ctx, s.mockPub)
	s.Error(err)
	assert.IsType(s.T(), &ReplicaNotExistsError{}, err)
}

// TestInputHostPublishMessage just publishes a bunch of messages and make sure
// we get the ack back
func (s *InputHostSuite) TestInputHostPublishMessage() {
	count := 10

	inputHost, _ := NewInputHost("inputhost-test", s.mockService, s.mockMeta, nil)
	ctx, cancel := utilGetThriftContextWithPath("foo")
	defer cancel()

	s.mockAppend.On("Write", mock.Anything).Return(nil)

	ackCh := make(chan *cherami.InputHostCommand, count)
	s.mockPub.On("Write", mock.Anything).Return(
		func(ack *cherami.InputHostCommand) error {
			ackCh <- ack
			return nil
		})

	// setup the mock so that we can send 10 messages
	for i := 0; i < count; i++ {
		aMsg := store.NewAppendMessageAck()
		aMsg.SequenceNumber = common.Int64Ptr(int64(i + 1))
		aMsg.Status = common.CheramiStatusPtr(cherami.Status_OK)
		aMsg.Address = common.Int64Ptr(int64(i + 100))
		msg := cherami.NewPutMessage()
		msg.ID = common.StringPtr(strconv.Itoa(i))
		msg.Data = []byte(fmt.Sprintf("hello-%d", i))
		msg.UserContext = map[string]string{"UserMsgId": fmt.Sprintf("user-msg-%d", i)}
		s.mockAppend.On("Read").Return(aMsg, nil).Once()
		s.mockPub.On("Read").Return(msg, nil).Once()
	}

	// close the read stream
	s.mockAppend.On("Read").Return(nil, io.EOF)
	s.mockPub.On("Read").Return(nil, io.EOF)

	err := inputHost.OpenPublisherStream(ctx, s.mockPub)
	s.NoError(err)

	extStats, _ := s.mockMeta.ListInputHostExtentsStats(nil, nil)
	s.Len(extStats.GetExtentStatsList(), 1)
	extUUID := extStats.GetExtentStatsList()[0].Extent.ExtentUUID
	s.NotNil(extUUID)

	for i := 0; i < count; i++ {
		select {
		case ack := <-ackCh:
			s.Equal(cherami.InputHostCommandType_ACK, ack.GetType())
			s.Equal(strconv.Itoa(i), ack.GetAck().GetID())
			s.Equal(fmt.Sprintf("user-msg-%d", i), ack.GetAck().GetUserContext()["UserMsgId"])
			receipt := ack.GetAck().GetReceipt()
			parts := strings.Split(receipt, ":")
			s.Equal(*extUUID, parts[0])
			s.Equal(strconv.Itoa(i+1), parts[1])
			s.Equal(fmt.Sprintf("%8x", i+100), parts[2])
		default:
		}
	}

	inputHost.Shutdown()
}

// TestInputHostPublishMessage just publishes a bunch of messages and make sure
// that watermarks are sent and we get the acks back for messages only.
func (s *InputHostSuite) _TestInputHostPublishMessageLogDestination() {
	*s.desinationDescription = *shared.NewDestinationDescription()
	s.desinationDescription.DestinationUUID = common.StringPtr(uuid.New())
	logType := shared.DestinationType_LOG
	s.desinationDescription.Type = &logType

	count := 10

	inputHost, _ := NewInputHost("inputhost-test", s.mockService, s.mockMeta, nil)
	ctx, cancel := utilGetThriftContextWithPath("foo")
	defer cancel()

	s.mockAppend.On("Write", mock.Anything).Return(nil)

	ackCh := make(chan *cherami.InputHostCommand, count)
	s.mockPub.On("Write", mock.Anything).Return(
		func(ack *cherami.InputHostCommand) error {
			ackCh <- ack
			return nil
		})

	// setup the mock so that we can send 10 messages
	for i := 0; i < count*2; i += 2 {
		aMsg := store.NewAppendMessageAck()
		aMsg.SequenceNumber = common.Int64Ptr(int64(i + 1))
		aMsg.Status = common.CheramiStatusPtr(cherami.Status_OK)
		msg := cherami.NewPutMessage()
		msg.ID = common.StringPtr(strconv.Itoa(i))
		msg.Data = []byte(fmt.Sprintf("hello-%d", i))
		msg.UserContext = map[string]string{"UserMsgId": fmt.Sprintf("user-msg-%d", i)}
		s.mockAppend.On("Read").Return(aMsg, nil).Once()
		// Watermark only
		aMsg2 := store.NewAppendMessageAck()
		aMsg2.SequenceNumber = common.Int64Ptr(int64(i + 2))
		aMsg2.Status = common.CheramiStatusPtr(cherami.Status_OK)
		s.mockAppend.On("Read").Return(aMsg2, nil).Once()

		s.mockPub.On("Read").Return(msg, nil).Once()
	}

	// close the read stream
	s.mockAppend.On("Read").Return(nil, io.EOF)
	s.mockPub.On("Read").Return(nil, io.EOF)

	err := inputHost.OpenPublisherStream(ctx, s.mockPub)
	s.NoError(err)

	extStats, _ := s.mockMeta.ListInputHostExtentsStats(nil, nil)
	s.Len(extStats.GetExtentStatsList(), 1)
	extUUID := extStats.GetExtentStatsList()[0].Extent.ExtentUUID
	s.NotNil(extUUID)

	// No acks for watermark only messages
	for i := 0; i < count*2; i += 2 {
		select {
		case ack := <-ackCh:
			s.Equal(cherami.InputHostCommandType_ACK, ack.GetType())
			s.Equal(strconv.Itoa(i), ack.GetAck().GetID())
			s.Equal(fmt.Sprintf("user-msg-%d", i), ack.GetAck().GetUserContext()["UserMsgId"])
			receipt := ack.GetAck().GetReceipt()
			parts := strings.Split(receipt, ":")
			s.Equal(*extUUID, parts[0])
			s.Equal(strconv.Itoa(i/2+1), parts[1])
		}
	}

	inputHost.Shutdown()
}

// TestIntegrationMultipleClients opens up multiple streams to
// inputhost and makes sure we don't open multiple streams to the
// replicas
func (s *InputHostSuite) TestInputHostMultipleClients() {
	numClients := 3
	numStoreStreams := 1
	numExtents := 1
	for i := 0; i < numClients; i++ {
		// Published message
		s.mockAppend.On("Write", mock.Anything).Return(nil).Once()
		// Fully replicated watermark message.
		// Sequence numbers start from 1
		expectedWatermark := int64(i + 1) //  captures value of i at each loop iteration
		s.mockAppend.On("Write", mock.MatchedBy(func(m *store.AppendMessage) bool {
			return m.GetFullyReplicatedWatermark() == expectedWatermark
		})).Return(nil).Once()
	}
	inputHost, _ := NewInputHost("inputhost-test", s.mockService, s.mockMeta, nil)

	mockPub := make([]*mockin.MockBInOpenPublisherStreamInCall, numClients)
	// create 3 instances of mockpub
	// call OpenPublisherStream multiple times
	for i := 0; i < numClients; i++ {
		mockPub[i] = new(mockin.MockBInOpenPublisherStreamInCall)
		mockPub[i].On("Write", mock.Anything).Return(nil)
		mockPub[i].On("Done").Return(nil)
		mockPub[i].On("Flush").Return(nil)
		aMsg := store.NewAppendMessageAck()
		aMsg.SequenceNumber = common.Int64Ptr(int64(i + 1))
		aMsg.Status = common.CheramiStatusPtr(cherami.Status_OK)
		msg := cherami.NewPutMessage()
		msg.ID = common.StringPtr(strconv.Itoa(i))
		msg.Data = []byte(fmt.Sprintf("hello-%d", i))
		s.mockAppend.On("Read").Return(aMsg, nil).Once()
		mockPub[i].On("Read").Return(msg, nil).Once()
		mockPub[i].On("Read").Return(nil, io.EOF).On("Write", mock.Anything).Return(nil)
		ctx, cancel := utilGetThriftContextWithPath("foo")
		defer cancel()
		err := inputHost.OpenPublisherStream(ctx, mockPub[i])
		s.NoError(err)
	}

	inputHost.pathMutex.RLock()
	s.Equal(numExtents, len(inputHost.pathCache))
	// Make sure we just have one stream to replica even though we have numClients clients
	for _, pathCache := range inputHost.pathCache {
		pathCache.RLock()
		for _, extInfo := range pathCache.extentCache {
			s.Equal(numStoreStreams, len(extInfo.connection.streams))
		}
		pathCache.RUnlock()
	}
	inputHost.pathMutex.RUnlock()

	for i := 0; i < numClients; i++ {
		mockPub[i].On("Read").Return(nil, io.EOF).Once()
		mockPub[i].On("Write").Return(nil, io.EOF).Once()
	}
	s.mockAppend.On("Write", mock.Anything).Return(io.EOF)
	s.mockAppend.On("Read").Return(nil, io.EOF)

	inputHost.Shutdown()
}

func (s *InputHostSuite) TestInputHostCacheTime() {
	numExtents := 1

	inputHost, _ := NewInputHost("inputhost-test", s.mockService, s.mockMeta, &InOptions{
		CacheIdleTimeout: 2 * time.Second,
	})
	ctx, cancel := utilGetThriftContextWithPath("foo")
	defer cancel()

	s.mockAppend.On("Write", mock.Anything).Return(nil).Once()
	s.mockAppend.On("Read").Return(&store.AppendMessageAck{}, nil).Once()
	s.mockPub.On("Write", mock.Anything).Return(nil).Once()
	s.mockPub.On("Read").Return(&cherami.PutMessage{}, io.EOF).WaitUntil(time.After(3 * time.Second))

	err := inputHost.OpenPublisherStream(ctx, s.mockPub)
	s.NoError(err)

	time.Sleep(3 * time.Second)

	inputHost.pathMutex.RLock()
	s.Equal(numExtents, len(inputHost.pathCache))
	for _, pathCache := range inputHost.pathCache {
		pathCache.RLock()
		for _, conn := range pathCache.connections {
			conn.lk.Lock()
			s.Equal(false, conn.closed, "connection must be closed")
			conn.lk.Unlock()
		}
		pathCache.RUnlock()
	}
	inputHost.pathMutex.RUnlock()

	s.mockAppend.On("Write", mock.Anything).Return(io.EOF)
	s.mockAppend.On("Read").Return(nil, io.EOF)

	inputHost.Shutdown()
}

func (s *InputHostSuite) TestInputHostLoadUnloadRace() {
	numAttempts := 10

	inputHost, _ := NewInputHost("inputhost-test", s.mockService, s.mockMeta, nil)
	ctx, cancel := utilGetThriftContextWithPath("foo")
	defer cancel()

	s.mockAppend.On("Write", mock.Anything).Return(io.EOF).Times(numAttempts)
	s.mockAppend.On("Read").Return(&store.AppendMessageAck{}, io.EOF).Times(numAttempts)
	s.mockPub.On("Write", mock.Anything).Return(io.EOF).Times(numAttempts)
	s.mockPub.On("Read").Return(&cherami.PutMessage{}, io.EOF).Times(numAttempts)

	var wg sync.WaitGroup

	// call open publisher stream 10 times - racing load
	for i := 0; i < numAttempts; i++ {
		wg.Add(1)
		go func(waitG *sync.WaitGroup) {
			inputHost.OpenPublisherStream(ctx, s.mockPub)
			waitG.Done()
		}(&wg)
	}

	// sleep a bit so that we create the path
	time.Sleep(1 * time.Second)

	// first get the pathCache now
	pathCache, _ := inputHost.getPathCacheByDestPath("foo")
	s.NotNil(pathCache)

	// unload everything
	inputHost.unloadAll()

	// make sure the pathCache is inactive
	s.Equal(false, pathCache.isActiveNoLock(), "pathCache should not be active")

	// make sure everything is stopped
	wg.Wait()
	inputHost.Shutdown()
}

func (s *InputHostSuite) TestInputHostReconfigure() {
	destUUID := uuid.New()
	extUUID1 := uuid.New()
	extUUID2 := uuid.New()
	hostUUID := uuid.New()

	// 0. Use a new mock service object for this test, rather than the common one since it needs a different RPM
	ch, _ := tchannel.NewChannel("cherami-test-in", nil)
	mockService := new(common.MockService)
	mockService.On("GetConfig").Return(s.cfg.GetServiceConfig(common.InputServiceName))
	/*
			scfg := s.cfg.ServiceConfig[common.InputServiceName]
			uConfig, uerr := uconfig.AdvancedUConfig(scfg.ClayRuntimeConfig, common.InputServiceName,
				uconfig.OptionalConfigs{
					TimeBetweenSyncSeconds: scfg.TimeBetweenSyncSeconds,
				})

			log.WithField("TimeBetweenSyncSeconds", scfg.TimeBetweenSyncSeconds).
				Infof("Passing the uconfig ConfigDefinaitons for mock servic: %s is %v \n, to the Uconfig client.\n", common.InputServiceName, scfg.ClayRuntimeConfig)

			if uerr != nil {
				log.Fatalf("Error initializing uconfig configuration in test: %s", uerr)
			}
		mockService.On("GetUConfig").Return(uConfig)
	*/
	mockService.On("GetMetricsReporter").Return(common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig()))
	mockService.On("GetDConfigClient").Return(dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.InputServiceName))
	mockService.On("GetTChannel").Return(ch)
	mockService.On("IsLimitsEnabled").Return(true)
	mockService.On("GetHostConnLimit").Return(100)
	mockService.On("GetHostConnLimitPerSecond").Return(100)
	mockService.On("GetWSConnector").Return(s.mockWSConnector, nil)

	// 1. Prepare the inputhost for all the metadata stuff
	mockMeta := new(mockmeta.TChanMetadataService)

	s.mockAppend.On("Write", mock.Anything).Return(nil)
	s.mockPub.On("Write", mock.Anything).Return(nil)

	aMsg := store.NewAppendMessageAck()
	msg := cherami.NewPutMessage()

	appendTicker := time.NewTicker(5 * time.Second)
	defer appendTicker.Stop()

	pubTicker := time.NewTicker(5 * time.Second)
	defer pubTicker.Stop()

	s.mockAppend.On("Read").Return(aMsg, io.EOF).WaitUntil(appendTicker.C)
	s.mockPub.On("Read").Return(msg, io.EOF).WaitUntil(pubTicker.C)

	s.mockStore.On("OpenAppendStream", mock.Anything).Return(s.mockAppend, nil)

	destDesc := shared.NewDestinationDescription()
	destDesc.DestinationUUID = common.StringPtr(destUUID)

	mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil)

	mExt := shared.NewExtent()
	mExt.ExtentUUID = common.StringPtr(extUUID1)
	mExt.InputHostUUID = common.StringPtr(hostUUID)
	mExt.StoreUUIDs = make([]string, 0, 4)

	rpm := common.NewMockRingpopMonitor()
	rpm.Add(common.InputServiceName, mExt.GetInputHostUUID(), "127.0.0.1")

	for i := 1; i < 2; i++ {
		uuid := uuid.New()
		rpm.Add(common.StoreServiceName, uuid, fmt.Sprintf("127.0.2.%v", i))
		mExt.StoreUUIDs = append(mExt.StoreUUIDs, uuid)
	}

	mockLoadReporterDaemonFactory := setupMockLoadReporterDaemonFactory()
	mockService.On("GetLoadReporterDaemonFactory").Return(mockLoadReporterDaemonFactory)
	mockService.On("GetHostUUID").Return("99999999-9999-9999-9999-999999999999")
	mockService.On("GetRingpopMonitor").Return(rpm)
	mockService.On("GetClientFactory").Return(s.mockClientFactory)
	mExtStats := shared.NewExtentStats()
	mExtStats.Extent = mExt
	mExtStats.Status = common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN)

	mListExtStats := metadata.NewListInputHostExtentsStatsResult_()
	mListExtStats.ExtentStatsList = append(mListExtStats.ExtentStatsList, mExtStats)
	mockMeta.On("ListInputHostExtentsStats", mock.Anything, mock.Anything).Return(mListExtStats, nil)

	inputHost, _ := NewInputHost("inputhost-test", mockService, mockMeta, nil)
	ctx, cancel := utilGetThriftContextWithPath("foo")
	defer cancel()

	// 2. Call OpenPublisherStream; we do it in a go routine because it is blocking
	go func() {
		inputHost.OpenPublisherStream(ctx, s.mockPub)
	}()

	time.Sleep(1 * time.Second)

	// 3. Make sure we just have one extent
	pathCache, ok := inputHost.getPathCacheByDestPath("foo")
	s.True(ok)
	pathCache.Lock()
	s.Equal(1, len(pathCache.extentCache))
	pathCache.Unlock()

	// 4. Now add another extent
	mExt1 := shared.NewExtent()
	mExt1.ExtentUUID = common.StringPtr(extUUID2)
	mExt1.InputHostUUID = common.StringPtr(hostUUID)
	mExt1.StoreUUIDs = make([]string, 0, 4)

	for i := 1; i < 2; i++ {
		uuid := uuid.New()
		rpm.Add(common.StoreServiceName, uuid, fmt.Sprintf("127.0.2.%v", i))
		mExt1.StoreUUIDs = append(mExt1.StoreUUIDs, uuid)
	}

	mExtStats1 := shared.NewExtentStats()
	mExtStats1.Extent = mExt1
	mExtStats1.Status = common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN)

	mListExtStats.ExtentStatsList = append(mListExtStats.ExtentStatsList, mExtStats1)
	mockMeta.On("ListInputHostExtentsStats", mock.Anything, mock.Anything).Return(mListExtStats, nil)

	// 5. Reconfigure the inputhost
	reconfigType := admin.NotificationType_ALL
	req := admin.NewDestinationsUpdatedRequest()
	req.UpdateUUID = common.StringPtr(uuid.New())

	destUpdate := admin.NewDestinationUpdatedNotification()
	destUpdate.DestinationUUID = common.StringPtr(destUUID)
	destUpdate.Type = &reconfigType
	req.Updates = append(req.Updates, destUpdate)

	err := inputHost.DestinationsUpdated(ctx, req)
	s.NoError(err)

	// wait for sometime so that the update goes through
	time.Sleep(1 * time.Second)

	// 6. Now make sure we have 2 extents
	pathCache, ok = inputHost.getPathCacheByDestPath("foo")
	if ok {
		pathCache.Lock()
		s.Equal(2, len(pathCache.extentCache))
		pathCache.Unlock()
	}

	inputHost.Shutdown()
}

// TestFailedOpenAppendStream makes sure we error out if we see an openreplica stream failure
func (s *InputHostSuite) TestFailedOpenAppendStream() {
	ch, _ := tchannel.NewChannel("cherami-test-in", nil)

	mockService := new(common.MockService)
	mockService.On("GetConfig").Return(s.cfg.GetServiceConfig(common.InputServiceName))
	/*
		scfg := s.cfg.ServiceConfig[common.InputServiceName]
		uConfig, uerr := uconfig.AdvancedUConfig(scfg.ClayRuntimeConfig, common.InputServiceName,
			uconfig.OptionalConfigs{
				TimeBetweenSyncSeconds: scfg.TimeBetweenSyncSeconds,
			})

		log.WithField("TimeBetweenSyncSeconds", scfg.TimeBetweenSyncSeconds).
			Infof("Passing the uconfig ConfigDefinaitons for mock servic: %s is %v \n, to the Uconfig client.\n", common.InputServiceName, scfg.ClayRuntimeConfig)

		if uerr != nil {
			log.Fatalf("Error initializing uconfig configuration in test: %s", uerr)
		}
		mockService.On("GetUConfig").Return(uConfig)
	*/
	mockService.On("GetMetricsReporter").Return(common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig()))
	mockService.On("GetDConfigClient").Return(dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.InputServiceName))
	mockService.On("GetTChannel").Return(ch)
	mockService.On("IsLimitsEnabled").Return(true)
	mockService.On("GetHostConnLimit").Return(100)
	mockService.On("GetHostConnLimitPerSecond").Return(100)
	mockService.On("GetHostUUID").Return("99999999-9999-9999-9999-999999999998")

	mockWSConnector := new(mockcommon.MockWSConnector)
	mockService.On("GetWSConnector").Return(mockWSConnector, nil)
	mockWSConnector.On("OpenAppendStream", mock.Anything, mock.Anything).Return(s.mockAppend, fmt.Errorf("no store found"))

	mockClientFactory := new(mockcommon.MockClientFactory)
	mockService.On("GetClientFactory").Return(mockClientFactory)

	mockMeta := new(mockmeta.TChanMetadataService)

	destDesc := shared.NewDestinationDescription()
	destDesc.DestinationUUID = common.StringPtr(uuid.New())

	mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil)

	mExt := shared.NewExtent()
	mExt.ExtentUUID = common.StringPtr(uuid.New())
	mExt.InputHostUUID = common.StringPtr("99999999-9999-9999-9999-999999999998")
	mExt.StoreUUIDs = make([]string, 0, 4)

	rpm := common.NewMockRingpopMonitor()
	rpm.Add(common.InputServiceName, mExt.GetInputHostUUID(), "127.0.0.1")

	for i := 1; i < 2; i++ {
		uuid := uuid.New()
		rpm.Add(common.StoreServiceName, uuid, fmt.Sprintf("127.0.2.%v", i))
		mExt.StoreUUIDs = append(mExt.StoreUUIDs, uuid)
	}

	mExtStats := shared.NewExtentStats()
	mExtStats.Extent = mExt
	mExtStats.Status = common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN)

	mListExtStats := metadata.NewListInputHostExtentsStatsResult_()
	mListExtStats.ExtentStatsList = append(mListExtStats.ExtentStatsList, mExtStats)
	mockMeta.On("ListInputHostExtentsStats", mock.Anything, mock.Anything).Return(mListExtStats, nil)

	mockLoadReporterDaemonFactory := setupMockLoadReporterDaemonFactory()
	mockService.On("GetLoadReporterDaemonFactory").Return(mockLoadReporterDaemonFactory)
	mockService.On("GetRingpopMonitor").Return(rpm)
	mockService.On("GetClientFactory").Return(mockClientFactory)

	inputHost, _ := NewInputHost("inputhost-test", mockService, mockMeta, nil)
	ctx, cancel := utilGetThriftContextWithPath("foo")
	defer cancel()

	s.mockPub.On("Read").Return(nil, io.EOF)

	err := inputHost.OpenPublisherStream(ctx, s.mockPub)
	s.Error(err)
	inputHost.Shutdown()
}

// TestInputHostPutMessageBatch publishes a batch of messages and make sure
// we get the ack back
func (s *InputHostSuite) _TestInputHostPutMessageBatch() {
	succMsgCount := 5
	failMsgCount := 5
	totalMsgCount := succMsgCount + failMsgCount
	extUUID := uuid.New()

	destinationPath := "foo"
	ch, _ := tchannel.NewChannel("cherami-test-in", nil)

	mockService := new(common.MockService)
	mockService.On("GetConfig").Return(s.cfg.GetServiceConfig(common.InputServiceName))

	mockService.On("GetDConfigClient").Return(dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.InputServiceName))
	mockService.On("GetMetricsReporter").Return(common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig()))
	mockService.On("GetTChannel").Return(ch)
	mockService.On("IsLimitsEnabled").Return(false)
	mockService.On("GetHostConnLimit").Return(100)
	mockService.On("GetHostConnLimitPerSecond").Return(100)
	mockService.On("GetHostUUID").Return("99999999-9999-9999-9999-999999999997")

	mockClientFactory := new(mockcommon.MockClientFactory)
	mockService.On("GetClientFactory").Return(mockClientFactory)

	mockLoadReporterDaemonFactory := setupMockLoadReporterDaemonFactory()
	mockService.On("GetLoadReporterDaemonFactory").Return(mockLoadReporterDaemonFactory)

	mockMeta := new(mockmeta.TChanMetadataService)

	destDesc := shared.NewDestinationDescription()
	destDesc.DestinationUUID = common.StringPtr(uuid.New())

	mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil)

	mExt := shared.NewExtent()
	mExt.ExtentUUID = common.StringPtr(extUUID)
	mExt.InputHostUUID = common.StringPtr("99999999-9999-9999-9999-999999999997")
	mExt.StoreUUIDs = make([]string, 0, 4)

	rpm := common.NewMockRingpopMonitor()
	rpm.Add(common.InputServiceName, mExt.GetInputHostUUID(), "127.0.0.1")

	for i := 1; i < 2; i++ {
		uuid := uuid.New()
		rpm.Add(common.StoreServiceName, uuid, fmt.Sprintf("127.0.2.%v", i))
		mExt.StoreUUIDs = append(mExt.StoreUUIDs, uuid)
	}

	mExtStats := shared.NewExtentStats()
	mExtStats.Extent = mExt
	mExtStats.Status = common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN)

	mListExtStats := metadata.NewListInputHostExtentsStatsResult_()
	mListExtStats.ExtentStatsList = append(mListExtStats.ExtentStatsList, mExtStats)
	mockMeta.On("ListInputHostExtentsStats", mock.Anything, mock.Anything).Return(mListExtStats, nil)

	mockService.On("GetRingpopMonitor").Return(rpm)
	mockService.On("GetClientFactory").Return(mockClientFactory)
	mockClientFactory.On("GetThriftStoreClient", mock.Anything, mock.Anything).Return(store.TChanBStore(s.mockStore), nil)
	mockClientFactory.On("ReleaseThriftStoreClient", mock.Anything).Return(nil)
	mockClientFactory.On("GetAdminControllerClient").Return(nil, fmt.Errorf("no real controller"))

	s.mockStore.On("OpenAppendStream", mock.Anything).Return(s.mockAppend, nil).Once()
	s.mockAppend.On("Write", mock.Anything).Return(nil)

	inputHost, _ := NewInputHost("inputhost-test", mockService, mockMeta, nil)
	ctx, cancel := utilGetThriftContextWithPath(destinationPath)
	defer cancel()

	var wg sync.WaitGroup
	var putMessageAcks *cherami.PutMessageBatchResult_

	// setup the mock so that we can send and ack messages
	createMessage := func(seq int, sts cherami.Status) (*store.AppendMessageAck, *cherami.PutMessage) {
		aMsg := store.NewAppendMessageAck()
		aMsg.SequenceNumber = common.Int64Ptr(int64(seq + 1))
		aMsg.Status = common.CheramiStatusPtr(sts)
		msg := cherami.NewPutMessage()
		msg.ID = common.StringPtr(strconv.Itoa(seq))
		msg.Data = []byte(fmt.Sprintf("hello-%d", seq))
		return aMsg, msg
	}

	putMessageRequest := &cherami.PutMessageBatchRequest{DestinationPath: &destinationPath}
	for i := 0; i < succMsgCount; i++ {
		aMsg, msg := createMessage(i, cherami.Status_OK)
		s.mockAppend.On("Read").Return(aMsg, nil).Once()
		putMessageRequest.Messages = append(putMessageRequest.Messages, msg)
	}
	for i := succMsgCount; i < totalMsgCount; i++ {
		aMsg, msg := createMessage(i, cherami.Status_FAILED)
		s.mockAppend.On("Read").Return(aMsg, nil).Once()
		putMessageRequest.Messages = append(putMessageRequest.Messages, msg)
	}

	wg.Add(1)
	go func() {
		var err error
		putMessageAcks, err = inputHost.PutMessageBatch(ctx, putMessageRequest)
		s.NoError(err)
		wg.Done()
	}()

	wg.Wait()
	// close the read stream
	s.mockAppend.On("Read").Return(nil, io.EOF)
	s.Len(putMessageAcks.GetSuccessMessages(), succMsgCount)
	s.Len(putMessageAcks.GetFailedMessages(), failMsgCount)

	// make sure the cache is populated
	pathCache, ok := inputHost.getPathCacheByDestPath(destinationPath)
	s.Equal(true, ok, "destination should be in the resolver cache now")
	s.NotNil(pathCache)
	inputHost.pathMutex.Lock()
	pathCache.Lock()
	pathCache.prepareForUnload()
	pathCache.Unlock()
	inputHost.pathMutex.Unlock()
	pathCache.unload()

	// Now check the both the caches
	_, ok = inputHost.getPathCacheByDestUUID(destDesc.GetDestinationUUID())
	s.Equal(false, ok, "destination should now be gone from the resolver cache now")
	_, ok = inputHost.getPathCacheByDestPath(destinationPath)
	s.Equal(false, ok, "destination should now be gone from the resolver cache now")

	inputHost.Shutdown()
}

// TestInputHostPutMessageBatchTimeout publishes a batch of messages and make sure
// we timeout appropriately
func (s *InputHostSuite) TestInputHostPutMessageBatchTimeout() {
	destinationPath := "foo"
	ctx, cancel := utilGetThriftContextWithPath(destinationPath)
	defer cancel()

	// set the batchMsgAckTimeout to a very low value before
	// publishing the message through the batch API.
	// We should see a failed message in the ack we get back and
	// it's status should be timeout as well
	batchMsgAckTimeout = 1 * time.Millisecond

	inputHost, _ := NewInputHost("inputhost-test", s.mockService, s.mockMeta, nil)
	aMsg := store.NewAppendMessageAck()
	msg := cherami.NewPutMessage()

	appendTicker := time.NewTicker(5 * time.Second)
	defer appendTicker.Stop()

	pubTicker := time.NewTicker(5 * time.Second)
	defer pubTicker.Stop()

	// make sure we don't respond immediately, so that we can timeout
	s.mockAppend.On("Write", mock.Anything).Return(nil).WaitUntil(appendTicker.C)
	s.mockPub.On("Write", mock.Anything).Return(nil).WaitUntil(pubTicker.C)

	s.mockAppend.On("Read").Return(aMsg, io.EOF).WaitUntil(appendTicker.C)
	s.mockPub.On("Read").Return(msg, io.EOF).WaitUntil(pubTicker.C)

	s.mockStore.On("OpenAppendStream", mock.Anything).Return(s.mockAppend, nil)

	// setup a putMessageRequest, which is about to be timed out
	putMessageRequest := &cherami.PutMessageBatchRequest{DestinationPath: &destinationPath}
	msg.ID = common.StringPtr(strconv.Itoa(1))
	msg.Data = []byte(fmt.Sprintf("hello-%d", 1))

	putMessageRequest.Messages = append(putMessageRequest.Messages, msg)
	putMessageAcks, err := inputHost.PutMessageBatch(ctx, putMessageRequest)
	s.NoError(err)
	s.NotNil(putMessageAcks)
	s.Len(putMessageAcks.GetSuccessMessages(), 0)
	s.Len(putMessageAcks.GetFailedMessages(), 1)
	// make sure the status is Status_TIMEDOUT
	s.Equal(cherami.Status_TIMEDOUT, putMessageAcks.GetFailedMessages()[0].GetStatus())

	inputHost.Shutdown()
}

func (s *InputHostSuite) TestInputHostConnLimit() {
	destinationPath := "foo"
	inputHost, _ := NewInputHost("inputhost-test", s.mockService, s.mockMeta, nil)
	ctx, cancel := utilGetThriftContextWithPath(destinationPath)
	defer cancel()

	// set the limits for this inputhost
	inputHost.SetHostConnLimit(int32(100))
	inputHost.SetHostConnLimitPerSecond(int32(10000))

	// use a mock time source
	ts := &mockTimeSource{currTime: time.Now()}
	connLimit := inputHost.GetHostConnLimitPerSecond()
	inputHost.SetTokenBucketValue(int32(connLimit))

	aMsg := store.NewAppendMessageAck()
	aMsg.SequenceNumber = common.Int64Ptr(int64(1))
	aMsg.Status = common.CheramiStatusPtr(cherami.Status_OK)

	wCh := make(chan time.Time)
	msg := cherami.NewPutMessage()
	msg.ID = common.StringPtr(strconv.Itoa(1))
	msg.Data = []byte(fmt.Sprintf("hello-%d", 1))

	s.mockAppend.On("Read").Return(nil, io.EOF).WaitUntil(wCh)
	s.mockAppend.On("Write", mock.Anything).Return(io.EOF).WaitUntil(wCh)
	s.mockPub.On("Read").Return(nil, io.EOF).WaitUntil(wCh)
	s.mockPub.On("Write", mock.Anything).Return(io.EOF).WaitUntil(wCh)

	for i := 0; i <= 101; i++ {
		go func() {
			inputHost.OpenPublisherStream(ctx, s.mockPub)
		}()
		// make sure we advance the time so that we don't hit the rate limit
		if i%9 == 0 {
			// wait for open pub stream to settle
			ts.advance(time.Millisecond * 101)
		}
	}

	ts.advance(time.Millisecond * 101)
	time.Sleep(2 * time.Second)
	// we should now see an error as we have opened upto the limit
	err := inputHost.OpenPublisherStream(ctx, s.mockPub)
	s.Error(err)

	close(wCh)

	inputHost.Shutdown()
}

func (s *InputHostSuite) TestInputExtHostShutdown() {
	destinationPath := "foo_limit"
	inputHost, _ := NewInputHost("inputhost-test-block", s.mockService, s.mockMeta, nil)
	ctx, cancel := utilGetThriftContextWithPath(destinationPath)
	defer cancel()

	// set the limits for this inputhost
	inputHost.SetHostConnLimit(int32(100))
	inputHost.SetHostConnLimitPerSecond(int32(10000))
	inputHost.SetMaxConnPerDest(int32(1000))
	inputHost.SetExtMsgsLimitPerSecond(int32(20000))
	inputHost.SetConnMsgsLimitPerSecond(int32(80000))

	aMsg := store.NewAppendMessageAck()
	aMsg.SequenceNumber = common.Int64Ptr(int64(1))
	aMsg.Status = common.CheramiStatusPtr(cherami.Status_OK)

	wCh := make(chan time.Time)
	msg := cherami.NewPutMessage()
	msg.ID = common.StringPtr(strconv.Itoa(1))
	msg.Data = []byte(fmt.Sprintf("hello-%d", 1))

	s.mockAppend.On("Read").Return(nil, io.EOF).WaitUntil(wCh)
	s.mockAppend.On("Write", mock.Anything).Return(io.EOF).WaitUntil(wCh)
	s.mockPub.On("Read").Return(nil, io.EOF).WaitUntil(wCh)
	s.mockPub.On("Write", mock.Anything).Return(io.EOF).WaitUntil(wCh)

	destUUID, destType, _, _ := inputHost.checkDestination(ctx, destinationPath)

	extents, e := inputHost.getExtentsInfoForDestination(ctx, destUUID)
	s.Nil(e)

	putMsgCh := make(chan *inPutMessage, 1)
	ackChannel := make(chan *cherami.PutMessageAck) // intentionally create a non-buffered channel so that we make sure we can shutdown

	mockLoadReporterDaemonFactory := setupMockLoadReporterDaemonFactory()

	reporter := metrics.NewSimpleReporter(nil)
	logger := common.GetDefaultLogger().WithFields(bark.Fields{"test": "ExtHostRateLimit"})

	pathCache := &inPathCache{
		destinationPath:       destinationPath,
		destUUID:              destUUID,
		destType:              destType,
		extentCache:           make(map[extentUUID]*inExtentCache),
		loadReporterFactory:   mockLoadReporterDaemonFactory,
		reconfigureCh:         make(chan inReconfigInfo, defaultBufferSize),
		putMsgCh:              putMsgCh,
		connections:           make(map[connectionID]*pubConnection),
		closeCh:               make(chan struct{}),
		notifyExtHostCloseCh:  make(chan string, defaultExtCloseNotifyChSize),
		notifyExtHostUnloadCh: make(chan string, defaultExtCloseNotifyChSize),
		notifyConnsCloseCh:    make(chan connectionID, defaultConnsCloseChSize),
		logger:                logger,
		m3Client:              metrics.NewClient(reporter, metrics.Inputhost),
		lastDisconnectTime:    time.Now(),
		dstMetrics:            load.NewDstMetrics(),
		hostMetrics:           load.NewHostMetrics(),
		inputHost:             inputHost,
	}

	pathCache.loadReporter = inputHost.GetLoadReporterDaemonFactory().CreateReporter(time.Minute, pathCache, logger)

	pathCache.destM3Client = metrics.NewClientWithTags(pathCache.m3Client, metrics.Inputhost, common.GetDestinationTags(destinationPath, logger))

	var connection *extHost
	for _, extent := range extents {
		connection = newExtConnection(
			destUUID,
			pathCache,
			string(extent.uuid),
			len(extent.replicas),
			mockLoadReporterDaemonFactory,
			inputHost.logger,
			inputHost.GetClientFactory(),
			&pathCache.connsWG,
			true)
		err := pathCache.checkAndLoadReplicaStreams(connection, extentUUID(extent.uuid), extent.replicas)
		s.Nil(err)

		// overwrite the token bucket so that we always throttle
		connection.SetExtTokenBucketValue(0)

		pathCache.connsWG.Add(1)
		connection.open()
		break
	}

	inMsg := &inPutMessage{
		putMsg:      msg,
		putMsgAckCh: ackChannel,
	}

	// send a message and then immediately shutdown without
	// reading the putMsg ack channel.
	// The shutdown should go through properly.
	putMsgCh <- inMsg
	close(wCh)
	close(connection.forceUnloadCh)
	connection.close()
	inputHost.Shutdown()
}

func (s *InputHostSuite) TestInputExtHostRateLimit() {
	destinationPath := "foo"
	inputHost, _ := NewInputHost("inputhost-test", s.mockService, s.mockMeta, nil)
	ctx, cancel := utilGetThriftContextWithPath(destinationPath)
	defer cancel()

	// set the limits for this inputhost
	inputHost.SetHostConnLimit(int32(100))
	inputHost.SetHostConnLimitPerSecond(int32(10000))
	inputHost.SetMaxConnPerDest(int32(1000))
	inputHost.SetExtMsgsLimitPerSecond(int32(20000))
	inputHost.SetConnMsgsLimitPerSecond(int32(80000))

	// use a mock time source
	ts := &mockTimeSource{currTime: time.Now()}

	aMsg := store.NewAppendMessageAck()
	aMsg.SequenceNumber = common.Int64Ptr(int64(1))
	aMsg.Status = common.CheramiStatusPtr(cherami.Status_OK)

	wCh := make(chan time.Time)
	msg := cherami.NewPutMessage()
	msg.ID = common.StringPtr(strconv.Itoa(1))
	msg.Data = []byte(fmt.Sprintf("hello-%d", 1))

	s.mockAppend.On("Read").Return(nil, io.EOF).WaitUntil(wCh)
	s.mockAppend.On("Write", mock.Anything).Return(io.EOF).WaitUntil(wCh)
	s.mockPub.On("Read").Return(nil, io.EOF).WaitUntil(wCh)
	s.mockPub.On("Write", mock.Anything).Return(io.EOF).WaitUntil(wCh)

	destUUID, destType, _, _ := inputHost.checkDestination(ctx, destinationPath)

	extents, e := inputHost.getExtentsInfoForDestination(ctx, destUUID)
	s.Nil(e)

	putMsgCh := make(chan *inPutMessage, 90)
	ackChannel := make(chan *cherami.PutMessageAck, 90)

	mockLoadReporterDaemonFactory := setupMockLoadReporterDaemonFactory()

	reporter := metrics.NewSimpleReporter(nil)
	logger := common.GetDefaultLogger().WithFields(bark.Fields{"test": "ExtHostRateLimit"})

	pathCache := &inPathCache{
		destinationPath:       destinationPath,
		destUUID:              destUUID,
		destType:              destType,
		extentCache:           make(map[extentUUID]*inExtentCache),
		loadReporterFactory:   mockLoadReporterDaemonFactory,
		reconfigureCh:         make(chan inReconfigInfo, defaultBufferSize),
		putMsgCh:              putMsgCh,
		connections:           make(map[connectionID]*pubConnection),
		closeCh:               make(chan struct{}),
		notifyExtHostCloseCh:  make(chan string, defaultExtCloseNotifyChSize),
		notifyExtHostUnloadCh: make(chan string, defaultExtCloseNotifyChSize),
		notifyConnsCloseCh:    make(chan connectionID, defaultConnsCloseChSize),
		logger:                logger,
		m3Client:              metrics.NewClient(reporter, metrics.Inputhost),
		lastDisconnectTime:    time.Now(),
		dstMetrics:            load.NewDstMetrics(),
		hostMetrics:           load.NewHostMetrics(),
		inputHost:             inputHost,
	}

	pathCache.loadReporter = inputHost.GetLoadReporterDaemonFactory().CreateReporter(time.Minute, pathCache, logger)
	pathCache.destM3Client = metrics.NewClientWithTags(pathCache.m3Client, metrics.Inputhost, common.GetDestinationTags(destinationPath, logger))

	var connection *extHost
	for _, extent := range extents {
		connection = newExtConnection(
			destUUID,
			pathCache,
			string(extent.uuid),
			len(extent.replicas),
			mockLoadReporterDaemonFactory,
			inputHost.logger,
			inputHost.GetClientFactory(),
			&pathCache.connsWG,
			true)
		err := pathCache.checkAndLoadReplicaStreams(connection, extentUUID(extent.uuid), extent.replicas)
		s.Nil(err)

		// overwrite the token bucket

		connection.SetExtTokenBucketValue(90)

		pathCache.connsWG.Add(1)
		connection.open()
		break
	}

	inMsg := &inPutMessage{
		putMsg:      msg,
		putMsgAckCh: ackChannel,
	}

	// try to push upto the limit on putMsgsCh
	total := 0
	attempts := 1
	for ; attempts < 11; attempts++ {
		for c := 0; c < 2; c++ {
			for i := 0; i < 10; i++ {
				select {
				case putMsgCh <- inMsg:
					total++
				default:
				}
			}
		}

		if total >= 90 {
			break
		}
		ts.advance(time.Millisecond * 101)
	}
	// XXX: Disabling this one for now. Relying on the tb_test on common
	// s.Equal(90, total, "Token bucket failed to enforce limit")
	ts.advance(time.Millisecond * 101)

	//send 9 more msgs
	for i := 0; i < 9; i++ {
		go func() {
			select {
			case putMsgCh <- inMsg:
			default:
			}
		}()
	}

	// now we should be rate limited
	select {
	case putMsgCh <- inMsg:
	// we shouldn't come here
	// XXX: Disabling this for now, until the flaky test is fixed.
	// s.False(true, "Token bucket failed to enforce rate limit")
	default:
		// rate limited
	}
	close(wCh)
	close(connection.forceUnloadCh)
	connection.close()
	inputHost.Shutdown()
}

func (s *InputHostSuite) TestInputExtHostSizeLimit() {
	destinationPath := "foo"

	// setup mockService here to use the mock admin controller client
	ch, _ := tchannel.NewChannel("cherami-test-size-in", nil)
	mockService := new(common.MockService)
	mockService.On("GetConfig").Return(s.cfg.GetServiceConfig(common.InputServiceName))
	mockService.On("GetMetricsReporter").Return(common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig()))
	mockService.On("GetDConfigClient").Return(dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.InputServiceName))
	mockService.On("GetTChannel").Return(ch)
	mockService.On("IsLimitsEnabled").Return(true)
	mockService.On("GetHostConnLimit").Return(100)
	mockService.On("GetHostConnLimitPerSecond").Return(100)
	mockService.On("GetWSConnector").Return(s.mockWSConnector, nil)

	mockAdminC := &mockAdminController{}
	mockClientFactory := new(mockcommon.MockClientFactory)
	mockClientFactory.On("GetThriftStoreClient", mock.Anything, mock.Anything).Return(store.TChanBStore(s.mockStore), nil)
	mockClientFactory.On("ReleaseThriftStoreClient", mock.Anything).Return(nil)
	mockClientFactory.On("GetAdminControllerClient").Return(mockAdminC, nil)
	mockService.On("GetClientFactory").Return(mockClientFactory)

	mockLoadReporterDaemonFactory := setupMockLoadReporterDaemonFactory()
	mockService.On("GetLoadReporterDaemonFactory").Return(mockLoadReporterDaemonFactory)
	mockService.On("GetHostUUID").Return("99999999-9999-9999-9999-999999999999")
	mockService.On("GetRingpopMonitor").Return(s.mockRpm)

	inputHost, _ := NewInputHost("inputhost-test", mockService, s.mockMeta, nil)
	ctx, cancel := utilGetThriftContextWithPath(destinationPath)
	defer cancel()

	// create the message of size 2k
	var dataBuf = make([]byte, 2048)

	msg := cherami.NewPutMessage()
	msg.ID = common.StringPtr(strconv.Itoa(1))
	msg.Data = dataBuf

	wCh := make(chan time.Time)

	s.mockAppend.On("Read").Return(nil, io.EOF).WaitUntil(wCh)
	s.mockAppend.On("Write", mock.Anything).Return(io.EOF).WaitUntil(wCh)
	s.mockPub.On("Read").Return(nil, io.EOF).WaitUntil(wCh)
	s.mockPub.On("Write", mock.Anything).Return(io.EOF).WaitUntil(wCh)

	aMsg := store.NewAppendMessageAck()
	aMsg.Status = common.CheramiStatusPtr(cherami.Status_OK)
	aMsg.Address = common.Int64Ptr(int64(100))

	destUUID, destType, _, _ := inputHost.checkDestination(ctx, destinationPath)
	extents, e := inputHost.getExtentsInfoForDestination(ctx, destUUID)
	s.Nil(e)

	putMsgCh := make(chan *inPutMessage)
	ackChannel := make(chan *cherami.PutMessageAck)

	reporter := metrics.NewSimpleReporter(nil)
	logger := common.GetDefaultLogger().WithFields(bark.Fields{"test": "ExtHostRateLimit"})

	pathCache := &inPathCache{
		destinationPath:       destinationPath,
		destUUID:              destUUID,
		destType:              destType,
		extentCache:           make(map[extentUUID]*inExtentCache),
		loadReporterFactory:   mockLoadReporterDaemonFactory,
		reconfigureCh:         make(chan inReconfigInfo, defaultBufferSize),
		putMsgCh:              putMsgCh,
		connections:           make(map[connectionID]*pubConnection),
		closeCh:               make(chan struct{}),
		notifyExtHostCloseCh:  make(chan string, defaultExtCloseNotifyChSize),
		notifyExtHostUnloadCh: make(chan string, defaultExtCloseNotifyChSize),
		notifyConnsCloseCh:    make(chan connectionID, defaultConnsCloseChSize),
		logger:                logger,
		m3Client:              metrics.NewClient(reporter, metrics.Inputhost),
		lastDisconnectTime:    time.Now(),
		dstMetrics:            load.NewDstMetrics(),
		hostMetrics:           load.NewHostMetrics(),
		inputHost:             inputHost,
	}

	pathCache.loadReporter = inputHost.GetLoadReporterDaemonFactory().CreateReporter(time.Minute, pathCache, logger)
	pathCache.destM3Client = metrics.NewClientWithTags(pathCache.m3Client, metrics.Inputhost, common.GetDestinationTags(destinationPath, logger))
	var connection *extHost
	for _, extent := range extents {
		connection = newExtConnection(
			destUUID,
			pathCache,
			string(extent.uuid),
			len(extent.replicas),
			mockLoadReporterDaemonFactory,
			inputHost.logger,
			inputHost.GetClientFactory(),
			&pathCache.connsWG,
			true)
		// overwrite the size limit
		connection.maxSizeBytes = 1024 // 1k extent size
		err := pathCache.checkAndLoadReplicaStreams(connection, extentUUID(extent.uuid), extent.replicas)
		s.Nil(err)

		pathCache.connsWG.Add(1)
		connection.open()
		break
	}

	inMsg := &inPutMessage{
		putMsg:      msg,
		putMsgAckCh: ackChannel,
	}
	// try to push upto the limit on putMsgsCh
	putMsgCh <- inMsg
	<-ackChannel

	// sleep a bit to make sure seal goes through
	time.Sleep(100 * time.Millisecond)

	// make sure the extent is sealed now
	connection.sealLk.Lock()
	extSeal := atomic.LoadUint32(&connection.sealed)
	connection.sealLk.Unlock()

	s.Equal(extSeal, sealed)

	close(wCh)
	close(connection.forceUnloadCh)
	connection.close()
	inputHost.Shutdown()
}

func (s *InputHostSuite) TestInputExtHostDrain() {
	destinationPath := "foo"

	// setup mockService here to use the mock admin controller client
	ch, _ := tchannel.NewChannel("cherami-test-drain-in", nil)
	mockService := new(common.MockService)
	mockService.On("GetConfig").Return(s.cfg.GetServiceConfig(common.InputServiceName))
	mockService.On("GetMetricsReporter").Return(common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig()))
	mockService.On("GetDConfigClient").Return(dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.InputServiceName))
	mockService.On("GetTChannel").Return(ch)
	mockService.On("IsLimitsEnabled").Return(true)
	mockService.On("GetHostConnLimit").Return(100)
	mockService.On("GetHostConnLimitPerSecond").Return(100)
	mockService.On("GetWSConnector").Return(s.mockWSConnector, nil)

	mockAdminC := &mockAdminController{}
	mockClientFactory := new(mockcommon.MockClientFactory)
	mockClientFactory.On("GetThriftStoreClient", mock.Anything, mock.Anything).Return(store.TChanBStore(s.mockStore), nil)
	mockClientFactory.On("ReleaseThriftStoreClient", mock.Anything).Return(nil)
	mockClientFactory.On("GetAdminControllerClient").Return(mockAdminC, nil)
	mockService.On("GetClientFactory").Return(mockClientFactory)

	mockLoadReporterDaemonFactory := setupMockLoadReporterDaemonFactory()
	mockService.On("GetLoadReporterDaemonFactory").Return(mockLoadReporterDaemonFactory)
	mockService.On("GetHostUUID").Return("99999999-9999-9999-9999-999999999999")
	mockService.On("GetRingpopMonitor").Return(s.mockRpm)

	inputHost, _ := NewInputHost("inputhost-test", mockService, s.mockMeta, nil)
	ctx, cancel := utilGetThriftContextWithPath(destinationPath)
	defer cancel()

	msg := cherami.NewPutMessage()
	msg.ID = common.StringPtr(strconv.Itoa(1))
	msg.Data = []byte("hello")

	wCh := make(chan time.Time)

	s.mockAppend.On("Read").Return(nil, io.EOF).WaitUntil(wCh)
	s.mockAppend.On("Write", mock.Anything).Return(io.EOF).WaitUntil(wCh)
	s.mockPub.On("Read").Return(nil, io.EOF).WaitUntil(wCh)
	s.mockPub.On("Write", mock.Anything).Return(io.EOF).WaitUntil(wCh)

	aMsg := store.NewAppendMessageAck()
	aMsg.Status = common.CheramiStatusPtr(cherami.Status_OK)
	aMsg.Address = common.Int64Ptr(int64(100))

	putMsgCh := make(chan *inPutMessage, 1)
	ackChannel := make(chan *cherami.PutMessageAck, 1)

	go func() {
		inputHost.OpenPublisherStream(ctx, s.mockPub)
	}()

	// wait for the extent to be loaded
	common.SpinWaitOnCondition(func() bool {
		return inputHost.hostMetrics.Get(load.HostMetricNumOpenExtents) == 1
	}, time.Second)

	// 3. Make sure we just have one extent
	var connection *extHost
	pathCache, ok := inputHost.getPathCacheByDestPath("foo")
	s.True(ok)
	pathCache.Lock()
	s.Equal(1, len(pathCache.extentCache))
	for _, extCache := range pathCache.extentCache {
		connection = extCache.connection
	}
	pathCache.Unlock()

	inMsg := &inPutMessage{
		putMsg:      msg,
		putMsgAckCh: ackChannel,
	}
	putMsgCh <- inMsg

	// send a drain request
	req := admin.NewDrainExtentsRequest()
	req.UpdateUUID = common.StringPtr(uuid.New())

	drainReq := admin.NewDrainExtents()
	drainReq.DestinationUUID = common.StringPtr(pathCache.destUUID)
	drainReq.ExtentUUID = common.StringPtr(connection.extUUID)
	req.Extents = append(req.Extents, drainReq)

	dCtx, dCancel := thrift.NewContext(2 * time.Second)
	defer dCancel()
	err := inputHost.DrainExtent(dCtx, req)
	s.Error(err)
	// Note: we will timeout here since the store connections are mocked to block above
	assert.IsType(s.T(), ErrDrainTimedout, err)

	// we should have definitely stopped the write pump at this point
	drained := false
	select {
	case <-connection.closeChannel:
		drained = true
	case <-time.After(5 * time.Second):
	}

	s.Equal(true, drained)
	close(wCh)
	connection.close()
	inputHost.Shutdown()
}

func (s *InputHostSuite) TestInputExtHostDrainDuplicate() {
	destinationPath := "foo"

	// setup mockService here to use the mock admin controller client
	ch, _ := tchannel.NewChannel("cherami-test-drain-in", nil)
	mockService := new(common.MockService)
	mockService.On("GetConfig").Return(s.cfg.GetServiceConfig(common.InputServiceName))
	mockService.On("GetMetricsReporter").Return(common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig()))
	mockService.On("GetDConfigClient").Return(dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.InputServiceName))
	mockService.On("GetTChannel").Return(ch)
	mockService.On("IsLimitsEnabled").Return(true)
	mockService.On("GetHostConnLimit").Return(100)
	mockService.On("GetHostConnLimitPerSecond").Return(100)
	mockService.On("GetWSConnector").Return(s.mockWSConnector, nil)

	mockAdminC := &mockAdminController{}
	mockClientFactory := new(mockcommon.MockClientFactory)
	mockClientFactory.On("GetThriftStoreClient", mock.Anything, mock.Anything).Return(store.TChanBStore(s.mockStore), nil)
	mockClientFactory.On("ReleaseThriftStoreClient", mock.Anything).Return(nil)
	mockClientFactory.On("GetAdminControllerClient").Return(mockAdminC, nil)
	mockService.On("GetClientFactory").Return(mockClientFactory)

	mockLoadReporterDaemonFactory := setupMockLoadReporterDaemonFactory()
	mockService.On("GetLoadReporterDaemonFactory").Return(mockLoadReporterDaemonFactory)
	mockService.On("GetHostUUID").Return("99999999-9999-9999-9999-999999999999")
	mockService.On("GetRingpopMonitor").Return(s.mockRpm)

	inputHost, _ := NewInputHost("inputhost-test", mockService, s.mockMeta, nil)
	ctx, cancel := utilGetThriftContextWithPath(destinationPath)
	defer cancel()

	msg := cherami.NewPutMessage()
	msg.ID = common.StringPtr(strconv.Itoa(1))
	msg.Data = []byte("hello")

	wCh := make(chan time.Time)

	s.mockAppend.On("Read").Return(nil, io.EOF).WaitUntil(wCh)
	s.mockAppend.On("Write", mock.Anything).Return(io.EOF).WaitUntil(wCh)
	s.mockPub.On("Read").Return(nil, io.EOF).WaitUntil(wCh)
	s.mockPub.On("Write", mock.Anything).Return(io.EOF).WaitUntil(wCh)

	aMsg := store.NewAppendMessageAck()
	aMsg.Status = common.CheramiStatusPtr(cherami.Status_OK)
	aMsg.Address = common.Int64Ptr(int64(100))

	putMsgCh := make(chan *inPutMessage, 1)
	ackChannel := make(chan *cherami.PutMessageAck, 1)

	go func() {
		inputHost.OpenPublisherStream(ctx, s.mockPub)
	}()

	// wait for the extent to be loaded
	common.SpinWaitOnCondition(func() bool {
		return inputHost.hostMetrics.Get(load.HostMetricNumOpenExtents) == 1
	}, time.Second)

	// 3. Make sure we just have one extent
	var connection *extHost
	pathCache, ok := inputHost.getPathCacheByDestPath("foo")
	s.True(ok)
	pathCache.Lock()
	s.Equal(1, len(pathCache.extentCache))
	for _, extCache := range pathCache.extentCache {
		connection = extCache.connection
	}
	pathCache.Unlock()

	inMsg := &inPutMessage{
		putMsg:      msg,
		putMsgAckCh: ackChannel,
	}
	putMsgCh <- inMsg

	// send a drain request
	req := admin.NewDrainExtentsRequest()
	req.UpdateUUID = common.StringPtr(uuid.New())

	drainReq := admin.NewDrainExtents()
	drainReq.DestinationUUID = common.StringPtr(pathCache.destUUID)
	drainReq.ExtentUUID = common.StringPtr(connection.extUUID)
	req.Extents = append(req.Extents, drainReq)

	dCtx, dCancel := thrift.NewContext(2 * time.Second)
	defer dCancel()
	err := inputHost.DrainExtent(dCtx, req)
	s.Error(err)
	// Note: we will timeout here since the store connections are mocked to block above
	assert.IsType(s.T(), ErrDrainTimedout, err)
	// we should have definitely stopped the write pump at this point
	drained := false
	select {
	case <-connection.closeChannel:
		drained = true
	case <-time.After(5 * time.Second):
	}

	s.Equal(true, drained)

	dNewCtx, dNewCancel := thrift.NewContext(2 * time.Second)
	defer dNewCancel()
	// send another drain request
	// this should just bail out, since we have already started drain above
	err = inputHost.DrainExtent(dNewCtx, req)
	s.NoError(err)

	// try to get the pathCacheLock, it should succeed
	common.SpinWaitOnCondition(func() bool {
		pathCache.Lock()
		// got the lock.. now unlock
		pathCache.Unlock()
		return true
	}, time.Second)

	// close everything
	close(wCh)
	connection.close()
	inputHost.Shutdown()
}

func setupMockLoadReporterDaemonFactory() *common.MockLoadReporterDaemonFactory {
	mockLoadReporterDaemonFactory := new(common.MockLoadReporterDaemonFactory)
	mockLoadReporterDaemon := new(common.MockLoadReporterDaemon)
	mockLoadReporterDaemonFactory.On("CreateReporter", mock.Anything, mock.Anything, mock.Anything).Return(mockLoadReporterDaemon)
	mockLoadReporterDaemon.On("Start").Return()
	mockLoadReporterDaemon.On("Stop").Return()

	return mockLoadReporterDaemonFactory
}

// mockAdminController is the mock controller used for
// seal notification
type mockAdminController struct {
}

func (ma *mockAdminController) ExtentsUnreachable(ctx thrift.Context, extentsUnreachableRequest *admin.ExtentsUnreachableRequest) error {
	return nil
}
