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
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
	mockcommon "github.com/uber/cherami-server/test/mocks/common"
	mockcontroller "github.com/uber/cherami-server/test/mocks/controllerhost"
	mockmeta "github.com/uber/cherami-server/test/mocks/metadata"
	mockin "github.com/uber/cherami-server/test/mocks/outputhost"
	mockstore "github.com/uber/cherami-server/test/mocks/storehost"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

type OutputHostSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
	cfg                  configure.CommonAppConfig
	mockCons             *mockin.MockBOutOpenConsumerStreamInCall
	mockMeta             *mockmeta.TChanMetadataService
	mockStore            *mockstore.MockStoreHost
	mockRead             *mockstore.MockBStoreOpenReadStreamOutCall
	mockService          *common.MockService
	mockClientFactory    *mockcommon.MockClientFactory
	mockControllerClient *mockcontroller.MockControllerHost
	mockWSConnector      *mockcommon.MockWSConnector
	mockHTTPResponse     *mockcommon.MockHTTPResponseWriter
}

func TestOutputHostSuite(t *testing.T) {
	suite.Run(t, new(OutputHostSuite))
}

// TODO: refactor the common setup once we start adding more tests
// as it makes sense
func (s *OutputHostSuite) SetupCommonMock() {
	s.mockCons = new(mockin.MockBOutOpenConsumerStreamInCall)
	s.mockStore = new(mockstore.MockStoreHost)
	s.mockRead = new(mockstore.MockBStoreOpenReadStreamOutCall)
	s.mockMeta = new(mockmeta.TChanMetadataService)
	s.mockService = new(common.MockService)
	s.mockControllerClient = new(mockcontroller.MockControllerHost)
	s.mockClientFactory = new(mockcommon.MockClientFactory)
	s.mockWSConnector = new(mockcommon.MockWSConnector)
	s.mockHTTPResponse = new(mockcommon.MockHTTPResponseWriter)

	s.mockStore.On("OpenReadStream", mock.Anything).Return(s.mockRead, nil)
	s.mockRead.On("Done").Return(nil)
	s.mockRead.On("Flush").Return(nil)
	s.mockCons.On("Done").Return(nil)
	s.mockCons.On("Flush").Return(nil)
	s.mockService.On("GetConfig").Return(s.cfg.GetServiceConfig(common.OutputServiceName))
	s.mockService.On("GetTChannel").Return(&tchannel.Channel{})
	s.mockService.On("GetHostPort").Return("outputhost:port")
	s.mockService.On("GetHostUUID").Return("99999999-9999-9999-9999-999999999999")
	s.mockService.On("IsLimitsEnabled").Return(true)

	rpm := common.NewMockRingpopMonitor()
	rpm.Add(common.StoreServiceName, "mock", "127.0.2.1")
	rpm.Add(common.StoreServiceName, "mock2", "127.0.2.2")
	s.mockService.On("GetRingpopMonitor").Return(rpm)

	s.mockClientFactory.On("GetThriftStoreClient", mock.Anything, mock.Anything).Return(store.TChanBStore(s.mockStore), nil)
	s.mockClientFactory.On("GetThriftStoreClientUUID", mock.Anything, mock.Anything).Return(store.TChanBStore(s.mockStore), "", nil)
	s.mockClientFactory.On("ReleaseThriftStoreClient", mock.Anything).Return(nil)
	s.mockClientFactory.On("GetAdminControllerClient").Return(nil, fmt.Errorf("no real controller"))
	s.mockService.On("GetClientFactory").Return(s.mockClientFactory)
	s.mockClientFactory.On("GetControllerClient").Return(s.mockControllerClient, nil)
	s.mockControllerClient.On("ReportConsumerGroupExtentMetric", mock.Anything, mock.Anything).Return(nil)

	s.mockWSConnector.On("AcceptConsumerStream", mock.Anything, mock.Anything).Return(s.mockCons, nil)
	s.mockWSConnector.On("OpenReadStream", mock.Anything, mock.Anything).Return(s.mockRead, nil)
	s.mockService.On("GetWSConnector").Return(s.mockWSConnector)

	s.mockHTTPResponse.On("Header").Return(http.Header{})
	s.mockHTTPResponse.On("WriteHeader", mock.Anything)
	s.mockHTTPResponse.On("Write", mock.Anything).Return(0, nil)

	mockLoadReporterDaemonFactory := common.NewLoadReporterDaemonFactory(s.mockService.GetHostUUID(), controller.SKU_Machine1, controller.Role_OUT, s.mockService.GetClientFactory(),
		common.NewRealTimeTickerFactory(), common.NewTokenBucketFactory(), common.GetDefaultLogger())
	s.mockService.On("GetLoadReporterDaemonFactory").Return(mockLoadReporterDaemonFactory)
	s.mockService.On("GetMetricsReporter").Return(common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig()))
	s.mockService.On("GetDConfigClient").Return(dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.InputServiceName))
}

func (s *OutputHostSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.cfg = common.SetupServerConfig(configure.NewCommonConfigure())
	s.SetupCommonMock()
}

func (s *OutputHostSuite) TearDownTest() {
}

// utility routine to get thrift context
func utilGetThriftContext() (thrift.Context, context.CancelFunc) {
	return thrift.NewContext(10 * time.Minute)
}

// utility routine to get http request
func utilGetHTTPRequest() *http.Request {
	return &http.Request{}
}

// utility routine to get http request with path
func utilGetHTTPRequestWithPath(path string) *http.Request {
	// those are http header keys from textproto.CanonicalMIMEHeaderKey()
	header := map[string][]string{"Path": {path}, "Consumergroupname": {"testcons"}}
	return &http.Request{Header: http.Header(header)}
}

// TestOutputHostRejectNoPath tests to make sure we
// fail, if there is no path given to publish
func (s *OutputHostSuite) TestOutputHostRejectNoPath() {
	sName := "cherami-test-out"
	outputHost, _ := NewOutputHost(sName, s.mockService, nil, nil, nil)
	httpRequest := utilGetHTTPRequest()
	outputHost.OpenConsumerStreamHandler(s.mockHTTPResponse, httpRequest)
	s.mockHTTPResponse.AssertCalled(s.T(), "WriteHeader", http.StatusBadRequest)
}

// TestOutputHostReadMessage just reads a bunch of messages and make sure
// we get the msg back
func (s *OutputHostSuite) TestOutputHostReadMessage() {
	var count int32
	count = 10

	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil)
	httpRequest := utilGetHTTPRequestWithPath("foo")

	destUUID := uuid.New()
	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr("/foo/bar")
	destDesc.DestinationUUID = common.StringPtr(destUUID)
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil).Once()

	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(uuid.New())
	cgDesc.DestinationUUID = common.StringPtr(destUUID)
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Twice()

	cgExt := metadata.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(uuid.New())
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &metadata.ReadConsumerGroupExtentsResult_{}
	cgRes.Extents = append(cgRes.Extents, cgExt)
	s.mockMeta.On("ReadConsumerGroupExtents", mock.Anything, mock.Anything).Return(cgRes, nil).Once()
	s.mockRead.On("Write", mock.Anything).Return(nil)
	s.mockCons.On("Write", mock.Anything).Return(nil)

	cFlow := cherami.NewControlFlow()
	cFlow.Credits = common.Int32Ptr(count)

	s.mockCons.On("Read").Return(cFlow, nil).Once()

	// setup the mock so that we can read 10 messages
	for i := 0; i < int(count); i++ {
		aMsg := store.NewAppendMessage()
		aMsg.SequenceNumber = common.Int64Ptr(int64(i))
		pMsg := cherami.NewPutMessage()
		pMsg.ID = common.StringPtr(strconv.Itoa(i))
		pMsg.Data = []byte(fmt.Sprintf("hello-%d", i))

		aMsg.Payload = pMsg
		rMsg := store.NewReadMessage()
		rMsg.Message = aMsg

		rmc := store.NewReadMessageContent()
		rmc.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_MESSAGE)
		rmc.Message = rMsg

		s.mockRead.On("Read").Return(rmc, nil).Once()
	}

	// close the read stream
	s.mockRead.On("Read").Return(nil, io.EOF)
	s.mockCons.On("Read").Return(nil, io.EOF)

	outputHost.OpenConsumerStreamHandler(s.mockHTTPResponse, httpRequest)
	s.mockHTTPResponse.AssertNotCalled(s.T(), "WriteHeader", mock.Anything)
	outputHost.Shutdown()
}

func (s *OutputHostSuite) TestOutputHostAckMessage() {
	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil)
	httpRequest := utilGetHTTPRequestWithPath("foo")
	ctx, _ := utilGetThriftContext()

	go outputHost.manageCgCache()

	destUUID := uuid.New()
	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr("/foo/bar")
	destDesc.DestinationUUID = common.StringPtr(destUUID)
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil).Once()

	cgUUID := uuid.New()
	extUUID := uuid.New()
	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(cgUUID)
	cgDesc.DestinationUUID = common.StringPtr(destUUID)
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Twice()
	s.mockMeta.On("SetAckOffset", mock.Anything, mock.Anything).Return(nil)

	cgExt := metadata.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(extUUID)
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &metadata.ReadConsumerGroupExtentsResult_{}
	cgRes.Extents = append(cgRes.Extents, cgExt)
	s.mockMeta.On("ReadConsumerGroupExtents", mock.Anything, mock.Anything).Return(cgRes, nil).Once()
	s.mockRead.On("Write", mock.Anything).Return(nil)
	s.mockCons.On("Write", mock.Anything).Return(nil)

	cFlow := cherami.NewControlFlow()
	cFlow.Credits = common.Int32Ptr(10)

	s.mockCons.On("Read").Return(cFlow, nil)

	aMsg := store.NewAppendMessage()
	aMsg.SequenceNumber = common.Int64Ptr(1)
	pMsg := cherami.NewPutMessage()
	pMsg.ID = common.StringPtr("1")
	pMsg.Data = []byte(fmt.Sprintf("hello-%d", 1))

	aMsg.Payload = pMsg
	rMsg := store.NewReadMessage()
	rMsg.Message = aMsg

	rmc := store.NewReadMessageContent()
	rmc.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_MESSAGE)
	rmc.Message = rMsg

	s.mockRead.On("Read").Return(rmc, nil)

	// since open consumer stream is blocking make it in a go routine
	go func() {
		outputHost.OpenConsumerStreamHandler(s.mockHTTPResponse, httpRequest)
	}()

	time.Sleep(1 * time.Second)
	s.mockCons.On("Read").Return(nil, io.EOF)
	s.mockRead.On("Read").Return(nil, io.EOF)

	// ack a couple of messages
	for i := 0; i < 2; i++ {
		stAckID := common.AckID{Address: 0}
		stAckID.MutatedID = stAckID.ConstructCombinedID(uint64(common.UUIDToUint16(outputHost.GetHostUUID())), uint64(1), uint64(i))

		ackID := stAckID.ToString()

		// ack the message
		ackMsgReq := cherami.NewAckMessagesRequest()
		ackMsgReq.AckIds = []string{ackID}
		ackMsgReq.NackIds = []string{}
		err := outputHost.AckMessages(ctx, ackMsgReq)
		s.NoError(err)
	}

	// TODO: check if we have acked the message
	outputHost.Shutdown()
}

func (s *OutputHostSuite) TestOutputHostInvalidAcks() {
	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil)
	ctx, _ := utilGetThriftContext()

	// ack a couple of invalid messages
	// we should get an error here.
	for i := 0; i < 2; i++ {
		ackID := "invalidAck"
		ackMsgReq := cherami.NewAckMessagesRequest()
		ackMsgReq.AckIds = []string{ackID}
		ackMsgReq.NackIds = []string{ackID}
		err := outputHost.AckMessages(ctx, ackMsgReq)
		s.Error(err)
		assert.IsType(s.T(), &cherami.InvalidAckIdError{}, err)
	}

	// ack a couple of messages for which there is no ack manager
	// we should not get any error now.
	for i := 0; i < 2; i++ {
		ackID := common.AckID{
			MutatedID: common.CombinedID(i),
			Address:   0,
		}.ToString()
		// ack the message
		ackMsgReq := cherami.NewAckMessagesRequest()
		ackMsgReq.AckIds = []string{ackID}
		ackMsgReq.NackIds = []string{ackID}
		err := outputHost.AckMessages(ctx, ackMsgReq)
		s.NoError(err)
	}
	outputHost.Shutdown()
}

func (s *OutputHostSuite) TestOutputHostReconfigure() {
	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil)
	httpRequest := utilGetHTTPRequestWithPath("foo")
	ctx, _ := utilGetThriftContext()

	destUUID := uuid.New()
	cgUUID := uuid.New()
	extUUID1 := uuid.New()
	extUUID2 := uuid.New()

	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr("/foo/bar")
	destDesc.DestinationUUID = common.StringPtr(destUUID)
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil).Twice()

	// 1. Make sure we setup the ReadConsumerGroup metadata
	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(cgUUID)
	cgDesc.DestinationUUID = common.StringPtr(destUUID)
	// Twice during initial load
	// Once during reconfigure
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Times(3)

	// 2. Setup the mock replica read, write and cons read and write calls
	aMsg := store.NewAppendMessage()
	aMsg.SequenceNumber = common.Int64Ptr(1)
	pMsg := cherami.NewPutMessage()
	pMsg.ID = common.StringPtr("1")
	pMsg.Data = []byte(fmt.Sprintf("hello-%d", 1))

	aMsg.Payload = pMsg
	rMsg := store.NewReadMessage()
	rMsg.Message = aMsg

	rmc := store.NewReadMessageContent()
	rmc.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_MESSAGE)
	rmc.Message = rMsg

	s.mockRead.On("Read").Return(rmc, nil)
	s.mockRead.On("Write", mock.Anything).Return(nil)
	s.mockStore.On("OpenReadStream", mock.Anything).Return(s.mockRead, nil)

	cFlow := cherami.NewControlFlow()
	cFlow.Credits = common.Int32Ptr(10)

	s.mockCons.On("Read").Return(cFlow, nil)
	s.mockCons.On("Write", mock.Anything).Return(nil)

	// 3. Setup the ReadConsumerGroupExtents mock.
	// At this time we will just have one extent "extUUID1"
	cgExt := metadata.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(extUUID1)
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &metadata.ReadConsumerGroupExtentsResult_{}
	cgRes.Extents = append(cgRes.Extents, cgExt)
	s.mockMeta.On("ReadConsumerGroupExtents", mock.Anything, mock.Anything).Return(cgRes, nil)

	// 4. OpenConsumerStreamHandler; We do this as a go routine because it is blocking
	go func() {
		outputHost.OpenConsumerStreamHandler(s.mockHTTPResponse, httpRequest)
	}()

	// 5. Wait for OpenConsumerStreamHandler to load
	time.Sleep(1 * time.Second)

	// 6. Make sure we just have one extent in the CGCache
	outputHost.cgMutex.Lock()
	if cg, ok := outputHost.cgCache[cgUUID]; ok {
		cg.extMutex.Lock()
		s.Equal(1, len(cg.extentCache))
		cg.extMutex.Unlock()
	}
	outputHost.cgMutex.Unlock()

	// 7. Now add another extent to the "ReadConsumerGroupExtents" call (extUUID2)
	cgExt2 := metadata.NewConsumerGroupExtent()
	cgExt2.ExtentUUID = common.StringPtr(extUUID2)
	cgExt2.StoreUUIDs = []string{"mock2"}
	cgRes.Extents = append(cgRes.Extents, cgExt2)
	s.mockMeta.On("ReadConsumerGroupExtents", mock.Anything, mock.Anything).Return(cgRes, nil)

	// 8. Call Reconfigure on the outputhost to make sure we load the new extent
	reconfigType := admin.NotificationType_ALL
	req := admin.NewConsumerGroupsUpdatedRequest()
	req.UpdateUUID = common.StringPtr(uuid.New())
	cgUpdate := admin.NewConsumerGroupUpdatedNotification()
	cgUpdate.ConsumerGroupUUID = common.StringPtr(cgUUID)
	cgUpdate.Type = &reconfigType
	req.Updates = append(req.Updates, cgUpdate)
	outputHost.ConsumerGroupsUpdated(ctx, req)

	// 9. Make sure we now have 2 and exactly 2 extents
	var ok bool
	var cgCache *consumerGroupCache
	outputHost.cgMutex.Lock()
	if cgCache, ok = outputHost.cgCache[cgUUID]; ok {
		cgCache.extMutex.Lock()
		s.Equal(2, len(cgCache.extentCache))
		cgCache.extMutex.Unlock()
	}
	outputHost.cgMutex.Unlock()

	// 10. Mark the consumer group as deleted and make sure its unloaded
	cgDesc.Status = common.InternalConsumerGroupStatusPtr(shared.ConsumerGroupStatus_DELETED)
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Once()
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil).Once()

	// 11. Call Reconfigure on the outputhost to make sure we unload the new extent
	reconfigType = admin.NotificationType_ALL
	req = admin.NewConsumerGroupsUpdatedRequest()
	req.UpdateUUID = common.StringPtr(uuid.New())
	cgUpdate = admin.NewConsumerGroupUpdatedNotification()
	cgUpdate.ConsumerGroupUUID = common.StringPtr(cgUUID)
	cgUpdate.Type = &reconfigType
	req.Updates = append(req.Updates, cgUpdate)
	outputHost.ConsumerGroupsUpdated(ctx, req)

	var succ = false
	select {
	case <-cgCache.closeChannel:
		succ = true
	case <-time.After(time.Second):
		succ = false
	}

	s.True(succ, "refreshCGCache failed to unload extents for DELETED consumer group")

	// 12. Shutdown
	outputHost.Shutdown()
}

// TestOutputHostReceiveMessageBatch receives a batch of messages and make sure
// we get the msg back
func (s *OutputHostSuite) TestOutputHostReceiveMessageBatch() {
	var count int32
	count = 10

	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil)
	ctx, _ := utilGetThriftContext()

	destUUID := uuid.New()
	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr("/foo/bar")
	destDesc.DestinationUUID = common.StringPtr(destUUID)
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil).Once()

	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(uuid.New())
	cgDesc.DestinationUUID = common.StringPtr(destUUID)
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Twice()

	cgExt := metadata.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(uuid.New())
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &metadata.ReadConsumerGroupExtentsResult_{}
	cgRes.Extents = append(cgRes.Extents, cgExt)
	s.mockMeta.On("ReadConsumerGroupExtents", mock.Anything, mock.Anything).Return(cgRes, nil).Once()
	s.mockRead.On("Write", mock.Anything).Return(nil)

	// setup the mock so that we can read 10 messages
	for i := 0; i < int(count); i++ {
		aMsg := store.NewAppendMessage()
		aMsg.SequenceNumber = common.Int64Ptr(int64(i))
		pMsg := cherami.NewPutMessage()
		pMsg.ID = common.StringPtr(strconv.Itoa(i))
		pMsg.Data = []byte(fmt.Sprintf("hello-%d", i))

		aMsg.Payload = pMsg
		rMsg := store.NewReadMessage()
		rMsg.Message = aMsg

		rmc := store.NewReadMessageContent()
		rmc.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_MESSAGE)
		rmc.Message = rMsg

		s.mockRead.On("Read").Return(rmc, nil).Once()
	}

	// close the read stream
	s.mockRead.On("Read").Return(nil, io.EOF)

	receiveMessageRequest := &cherami.ReceiveMessageBatchRequest{
		DestinationPath:     common.StringPtr("foo"),
		ConsumerGroupName:   common.StringPtr("testcons"),
		MaxNumberOfMessages: common.Int32Ptr(count),
		ReceiveTimeout:      common.Int32Ptr(30),
	}

	receivedMessages, err := outputHost.ReceiveMessageBatch(ctx, receiveMessageRequest)
	s.NoError(err)
	s.Len(receivedMessages.GetMessages(), int(count))
	for i := 0; i < int(count); i++ {
		s.Equal(strconv.Itoa(i), receivedMessages.GetMessages()[i].GetPayload().GetID())
	}

	outputHost.Shutdown()
}

// TestOutputHostReceiveMessageBatch_NoMsg tests the no message available scenario
func (s *OutputHostSuite) TestOutputHostReceiveMessageBatch_NoMsg() {
	var count int32
	count = 10

	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil)
	ctx, _ := utilGetThriftContext()

	destUUID := uuid.New()
	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr("/foo/bar")
	destDesc.DestinationUUID = common.StringPtr(destUUID)
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil)

	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(uuid.New())
	cgDesc.DestinationUUID = common.StringPtr(destUUID)
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Twice()

	cgExt := metadata.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(uuid.New())
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &metadata.ReadConsumerGroupExtentsResult_{}
	cgRes.Extents = append(cgRes.Extents, cgExt)
	s.mockMeta.On("ReadConsumerGroupExtents", mock.Anything, mock.Anything).Return(cgRes, nil).Once()
	s.mockRead.On("Write", mock.Anything).Return(nil)

	// read will delay for 2 seconds
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	s.mockRead.On("Read").Return(nil, io.EOF).WaitUntil(ticker.C)

	receiveMessageRequest := &cherami.ReceiveMessageBatchRequest{
		DestinationPath:     common.StringPtr("foo"),
		ConsumerGroupName:   common.StringPtr("testcons"),
		MaxNumberOfMessages: common.Int32Ptr(count),
		ReceiveTimeout:      common.Int32Ptr(1),
	}

	receivedMessages, err := outputHost.ReceiveMessageBatch(ctx, receiveMessageRequest)
	s.Error(err)
	assert.IsType(s.T(), &cherami.TimeoutError{}, err)
	s.Nil(receivedMessages)

	outputHost.Shutdown()
}

// TestOutputHostReceiveMessageBatch_NoMsg tests the some message available scenario
func (s *OutputHostSuite) TestOutputHostReceiveMessageBatch_SomeMsgAvailable() {
	var count int32
	count = 10

	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil)
	ctx, _ := utilGetThriftContext()

	destUUID := uuid.New()
	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr("/foo/bar")
	destDesc.DestinationUUID = common.StringPtr(destUUID)
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil)

	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(uuid.New())
	cgDesc.DestinationUUID = common.StringPtr(destUUID)
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Twice()

	cgExt := metadata.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(uuid.New())
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &metadata.ReadConsumerGroupExtentsResult_{}
	cgRes.Extents = append(cgRes.Extents, cgExt)
	s.mockMeta.On("ReadConsumerGroupExtents", mock.Anything, mock.Anything).Return(cgRes, nil).Once()
	s.mockRead.On("Write", mock.Anything).Return(nil)

	// setup the mock so that we can read 10 messages
	for i := 0; i < int(count); i++ {
		aMsg := store.NewAppendMessage()
		aMsg.SequenceNumber = common.Int64Ptr(int64(i))
		pMsg := cherami.NewPutMessage()
		pMsg.ID = common.StringPtr(strconv.Itoa(i))
		pMsg.Data = []byte(fmt.Sprintf("hello-%d", i))

		aMsg.Payload = pMsg
		rMsg := store.NewReadMessage()
		rMsg.Message = aMsg

		rmc := store.NewReadMessageContent()
		rmc.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_MESSAGE)
		rmc.Message = rMsg

		s.mockRead.On("Read").Return(rmc, nil).Once()
	}

	// next read will delay for 2 seconds
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	s.mockRead.On("Read").Return(nil, io.EOF).WaitUntil(ticker.C)

	receiveMessageRequest := &cherami.ReceiveMessageBatchRequest{
		DestinationPath:     common.StringPtr("foo"),
		ConsumerGroupName:   common.StringPtr("testcons"),
		MaxNumberOfMessages: common.Int32Ptr(count + 1),
		ReceiveTimeout:      common.Int32Ptr(1),
	}

	receivedMessages, err := outputHost.ReceiveMessageBatch(ctx, receiveMessageRequest)
	s.NoError(err)
	s.Len(receivedMessages.GetMessages(), int(count))
	for i := 0; i < int(count); i++ {
		s.Equal(strconv.Itoa(i), receivedMessages.GetMessages()[i].GetPayload().GetID())
	}

	outputHost.Shutdown()
}

func (s *OutputHostSuite) TestOutputCgUnload() {
	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil)
	httpRequest := utilGetHTTPRequestWithPath("foo")
	ctx, _ := utilGetThriftContext()

	go outputHost.manageCgCache()

	destUUID := uuid.New()
	cgUUID := uuid.New()
	extUUID1 := uuid.New()

	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr("/foo/bar")
	destDesc.DestinationUUID = common.StringPtr(destUUID)
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil).Twice()

	// 1. Make sure we setup the ReadConsumerGroup metadata
	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(cgUUID)
	cgDesc.DestinationUUID = common.StringPtr(destUUID)
	// Twice during initial load
	// Once during reconfigure
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Times(3)

	// 2. Setup the mock replica read, write and cons read and write calls
	aMsg := store.NewAppendMessage()
	aMsg.SequenceNumber = common.Int64Ptr(1)
	pMsg := cherami.NewPutMessage()
	pMsg.ID = common.StringPtr("1")
	pMsg.Data = []byte(fmt.Sprintf("hello-%d", 1))

	aMsg.Payload = pMsg
	rMsg := store.NewReadMessage()
	rMsg.Message = aMsg

	rmc := store.NewReadMessageContent()
	rmc.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_MESSAGE)
	rmc.Message = rMsg

	s.mockRead.On("Read").Return(rmc, nil)
	s.mockRead.On("Write", mock.Anything).Return(nil)
	s.mockStore.On("OpenReadStream", mock.Anything).Return(s.mockRead, nil)

	cFlow := cherami.NewControlFlow()
	cFlow.Credits = common.Int32Ptr(10)

	s.mockCons.On("Read").Return(cFlow, nil)
	s.mockCons.On("Write", mock.Anything).Return(nil)

	// 3. Setup the ReadConsumerGroupExtents mock.
	// At this time we will just have one extent "extUUID1"
	cgExt := metadata.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(extUUID1)
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &metadata.ReadConsumerGroupExtentsResult_{}
	cgRes.Extents = append(cgRes.Extents, cgExt)
	s.mockMeta.On("ReadConsumerGroupExtents", mock.Anything, mock.Anything).Return(cgRes, nil)

	// 4. OpenConsumerStreamHandler; We do this as a go routine because it is blocking
	go func() {
		outputHost.OpenConsumerStreamHandler(s.mockHTTPResponse, httpRequest)
	}()

	// 5. Wait for OpenConsumerStreamHandler to load
	time.Sleep(1 * time.Second)

	// 6. Make sure we just have one extent in the CGCache
	outputHost.cgMutex.Lock()
	if cg, ok := outputHost.cgCache[cgUUID]; ok {
		cg.extMutex.Lock()
		s.Equal(1, len(cg.extentCache))
		cg.extMutex.Unlock()
	}
	outputHost.cgMutex.Unlock()

	// 7. Unload the cg
	cgUnloadReq := admin.NewUnloadConsumerGroupsRequest()
	cgUnloadReq.CgUUIDs = []string{cgUUID}
	err := outputHost.UnloadConsumerGroups(ctx, cgUnloadReq)
	s.NoError(err)

	time.Sleep(1 * time.Second)
	// 8. Make sure it is unloaded
	outputHost.cgMutex.Lock()
	_, ok := outputHost.cgCache[cgUUID]
	outputHost.cgMutex.Unlock()
	s.Equal(ok, false)

	// 9. try to unload again. should get an error
	err = outputHost.UnloadConsumerGroups(ctx, cgUnloadReq)
	s.Error(err)

	outputHost.Shutdown()
}

func (s *OutputHostSuite) TestOutputAckMgrReset() {
	outputHost, _ := NewOutputHost("outputhost-test-reset", s.mockService, s.mockMeta, nil, nil)
	httpRequest := utilGetHTTPRequestWithPath("foo")

	go outputHost.manageCgCache()

	destUUID := uuid.New()
	cgUUID := uuid.New()
	extUUID1 := uuid.New()

	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr("/foo/reset")
	destDesc.DestinationUUID = common.StringPtr(destUUID)
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil).Twice()

	// 1. Make sure we setup the ReadConsumerGroup metadata
	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(cgUUID)
	cgDesc.DestinationUUID = common.StringPtr(destUUID)
	// Twice during initial load
	// Once during reconfigure
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Times(3)

	// 2. Setup the mock replica read, write and cons read and write calls
	aMsg := store.NewAppendMessage()
	aMsg.SequenceNumber = common.Int64Ptr(1)
	pMsg := cherami.NewPutMessage()
	pMsg.ID = common.StringPtr("1")
	pMsg.Data = []byte(fmt.Sprintf("hello-%d", 1))

	aMsg.Payload = pMsg
	rMsg := store.NewReadMessage()
	rMsg.Message = aMsg

	rmc := store.NewReadMessageContent()
	rmc.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_MESSAGE)
	rmc.Message = rMsg

	s.mockRead.On("Read").Return(rmc, nil)
	s.mockRead.On("Write", mock.Anything).Return(nil)
	s.mockStore.On("OpenReadStream", mock.Anything).Return(s.mockRead, nil)

	cFlow := cherami.NewControlFlow()
	cFlow.Credits = common.Int32Ptr(10)

	s.mockCons.On("Read").Return(cFlow, nil)
	s.mockCons.On("Write", mock.Anything).Return(nil)

	// 3. Setup the ReadConsumerGroupExtents mock.
	// At this time we will just have one extent "extUUID1"
	cgExt := metadata.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(extUUID1)
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &metadata.ReadConsumerGroupExtentsResult_{}
	cgRes.Extents = append(cgRes.Extents, cgExt)
	s.mockMeta.On("ReadConsumerGroupExtents", mock.Anything, mock.Anything).Return(cgRes, nil)

	// 4. OpenConsumerStreamHandler; We do this as a go routine because it is blocking
	go func() {
		outputHost.OpenConsumerStreamHandler(s.mockHTTPResponse, httpRequest)
	}()

	// 5. Wait for OpenConsumerStreamHandler to load
	time.Sleep(1 * time.Second)

	// 6. Make sure we just have one extent in the CGCache
	outputHost.cgMutex.Lock()
	if cg, ok := outputHost.cgCache[cgUUID]; ok {
		cg.extMutex.Lock()
		s.Equal(1, len(cg.extentCache))
		cg.extMutex.Unlock()
	}
	outputHost.cgMutex.Unlock()

	// 7. break the cons connection so that no one is reading on the msgCache Ch
	s.mockCons.On("Write", mock.Anything).Return(fmt.Errorf("breaking write pipe"))

	// 8. get the ackMgr
	var readLevel common.SequenceNumber
	var ackMgr *ackManager
	outputHost.cgMutex.RLock()
	if cg, ok := outputHost.cgCache[cgUUID]; ok {
		cg.extMutex.RLock()
		if extCache, found := cg.extentCache[extUUID1]; found {
			extCache.cacheMutex.RLock()
			ackMgr = extCache.ackMgr
			extCache.cacheMutex.RUnlock()
		}
		cg.extMutex.RUnlock()
	}
	outputHost.cgMutex.RUnlock()
	s.NotNil(ackMgr)

	// 8. get the readLevel and sleep a bit for traffic to flow through
	ackMgr.lk.RLock()
	readLevel = ackMgr.readLevel
	ackMgr.lk.RUnlock()
	time.Sleep(500 * time.Millisecond)

	// 8. break the replica connection and shutdown so that we unload replica connection
	s.mockRead.On("Write", mock.Anything).Return(fmt.Errorf("breaking write pipe"))
	outputHost.Shutdown()

	// 9. Make sure we reset the readLevel
	var newReadLevel common.SequenceNumber
	ackMgr.lk.RLock()
	newReadLevel = ackMgr.readLevel
	ackMgr.lk.RUnlock()

	// 10. make sure the readlevels are not the same
	assert.NotEqual(s.T(), readLevel, newReadLevel, "read levels should not be the same")
}
