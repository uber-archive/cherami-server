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
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
	"github.com/uber/cherami-server/common/set"
	mockcommon "github.com/uber/cherami-server/test/mocks/common"
	mockcontroller "github.com/uber/cherami-server/test/mocks/controllerhost"
	mockmeta "github.com/uber/cherami-server/test/mocks/metadata"
	mockin "github.com/uber/cherami-server/test/mocks/outputhost"
	mockstore "github.com/uber/cherami-server/test/mocks/storehost"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"

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
	s.mockControllerClient.On("ReportConsumerGroupMetric", mock.Anything, mock.Anything).Return(nil)

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
	outputHost, _ := NewOutputHost(sName, s.mockService, nil, nil, nil, nil)
	httpRequest := utilGetHTTPRequest()
	outputHost.OpenConsumerStreamHandler(s.mockHTTPResponse, httpRequest)
	s.mockHTTPResponse.AssertCalled(s.T(), "WriteHeader", http.StatusBadRequest)
}

// TestOutputHostReadMessage just reads a bunch of messages and make sure
// we get the msg back
func (s *OutputHostSuite) TestOutputHostReadMessage() {

	count := 10

	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil, nil)
	httpRequest := utilGetHTTPRequestWithPath("foo")

	destUUID := uuid.New()
	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr("/foo/bar")
	destDesc.DestinationUUID = common.StringPtr(destUUID)
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil).Once()
	s.mockMeta.On("ReadExtentStats", mock.Anything, mock.Anything).Return(nil, fmt.Errorf(`foo`))

	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(uuid.New())
	cgDesc.DestinationUUID = common.StringPtr(destUUID)
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Twice()

	cgExt := shared.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(uuid.New())
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &shared.ReadConsumerGroupExtentsResult_{}
	cgRes.Extents = append(cgRes.Extents, cgExt)
	s.mockMeta.On("ReadConsumerGroupExtents", mock.Anything, mock.Anything).Return(cgRes, nil).Once()
	s.mockRead.On("Write", mock.Anything).Return(nil)

	nWritten := 0
	writeDoneCh := make(chan struct{})

	s.mockCons.On("Write", mock.Anything).Return(nil).Times(count).Run(func(args mock.Arguments) {
		nWritten++
		if nWritten == count {
			close(writeDoneCh)
		}
	})

	cFlow := cherami.NewControlFlow()
	cFlow.Credits = common.Int32Ptr(int32(count))

	connOpenedCh := make(chan struct{})
	s.mockCons.On("Read").Return(cFlow, nil).Once().Run(func(args mock.Arguments) { close(connOpenedCh) })

	// setup the mock so that we can read 10 messages
	writeSeq := int64(0)

	rmc := store.NewReadMessageContent()
	rmc.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_MESSAGE)

	s.mockRead.On("Read").Return(rmc, nil).Times(10).Run(func(args mock.Arguments) {
		aMsg := store.NewAppendMessage()
		aMsg.SequenceNumber = common.Int64Ptr(writeSeq)
		pMsg := cherami.NewPutMessage()
		pMsg.ID = common.StringPtr(strconv.Itoa(int(writeSeq)))
		pMsg.Data = []byte(fmt.Sprintf("hello-%d", writeSeq))
		aMsg.Payload = pMsg
		rMsg := store.NewReadMessage()
		rMsg.Message = aMsg
		rmc.Message = rMsg
		writeSeq++
	}).Times(count)

	streamDoneCh := make(chan struct{})
	go func() {
		outputHost.OpenConsumerStreamHandler(s.mockHTTPResponse, httpRequest)
		close(streamDoneCh)
	}()

	// close the read stream
	creditUnblockCh := make(chan struct{})
	s.mockRead.On("Read").Return(nil, io.EOF)
	s.mockCons.On("Read").Return(nil, io.EOF).Run(func(args mock.Arguments) { <-writeDoneCh; <-creditUnblockCh })

	<-connOpenedCh // wait for the consConnection to open

	// look up cgcache and the underlying client connnection
	outputHost.cgMutex.RLock()
	cgCache, ok := outputHost.cgCache[cgDesc.GetConsumerGroupUUID()]
	outputHost.cgMutex.RUnlock()
	s.True(ok, "cannot find cgcache entry")

	var nConns = 0
	var conn *consConnection
	cgCache.extMutex.RLock()
	for _, conn = range cgCache.connections {
		break
	}
	nConns = len(cgCache.connections)
	cgCache.extMutex.RUnlock()
	s.Equal(1, nConns, "wrong number of consumer connections")
	s.NotNil(conn, "failed to find consConnection within cgcache")

	creditUnblockCh <- struct{}{} // now unblock the readCreditsPump on the consconnection
	<-streamDoneCh

	s.mockHTTPResponse.AssertNotCalled(s.T(), "WriteHeader", mock.Anything)
	s.Equal(int64(count), conn.sentMsgs, "wrong sentMsgs count")
	s.Equal(int64(0), conn.reSentMsgs, "wrong reSentMsgs count")
	s.Equal(conn.sentMsgs, conn.sentToMsgCache, "sentMsgs != sentToMsgCache")
	outputHost.Shutdown()
}

// TestOutputHostPutsMsgIntoMsgCacheOnWriteError verifies that
// consConnection always puts a dequeued into message cache regardless
// of whether write() succeeds or fails.
func (s *OutputHostSuite) TestOutputHostPutsMsgIntoMsgCacheOnWriteError() {

	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil, nil)
	httpRequest := utilGetHTTPRequestWithPath("foo")

	destUUID := uuid.New()
	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr("/foo/bar")
	destDesc.DestinationUUID = common.StringPtr(destUUID)
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil).Once()
	s.mockMeta.On("ReadExtentStats", mock.Anything, mock.Anything).Return(nil, fmt.Errorf(`foo`))

	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(uuid.New())
	cgDesc.DestinationUUID = common.StringPtr(destUUID)
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Twice()

	cgExt := shared.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(uuid.New())
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &shared.ReadConsumerGroupExtentsResult_{}
	cgRes.Extents = append(cgRes.Extents, cgExt)
	s.mockMeta.On("ReadConsumerGroupExtents", mock.Anything, mock.Anything).Return(cgRes, nil).Once()
	s.mockRead.On("Write", mock.Anything).Return(nil)

	writeDoneCh := make(chan struct{}) // signal after write() is called
	s.mockCons.On("Write", mock.Anything).Return(errors.New("write error")).Run(func(args mock.Arguments) { close(writeDoneCh) })

	cFlow := cherami.NewControlFlow()
	cFlow.Credits = common.Int32Ptr(10)

	s.mockCons.On("Read").Return(cFlow, nil).Once()

	// setup to send a mock message from store
	aMsg := store.NewAppendMessage()
	aMsg.SequenceNumber = common.Int64Ptr(int64(1))
	pMsg := cherami.NewPutMessage()
	pMsg.ID = common.StringPtr(strconv.Itoa(1))
	pMsg.Data = []byte("hello")

	aMsg.Payload = pMsg
	rMsg := store.NewReadMessage()
	rMsg.Message = aMsg

	rmc := store.NewReadMessageContent()
	rmc.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_MESSAGE)
	rmc.Message = rMsg
	// message from store to output
	s.mockRead.On("Read").Return(rmc, nil).Once()

	creditUnblockCh := make(chan struct{}) // channel to keep the consConnection open until we validate results
	// close the read stream
	s.mockRead.On("Read").Return(nil, io.EOF)
	// make the credit channel wait until we verify the output, this prevents the cgcache from unloading
	s.mockCons.On("Read").Return(nil, io.EOF).Run(func(args mock.Arguments) { <-creditUnblockCh })

	// initiate a consumer connection
	streamDoneCh := make(chan struct{})
	go func() {
		outputHost.OpenConsumerStreamHandler(s.mockHTTPResponse, httpRequest)
		close(streamDoneCh)
	}()

	<-writeDoneCh // wait for the message to be written

	// look up cgcache and the underlying client connnection
	outputHost.cgMutex.RLock()
	cgCache, ok := outputHost.cgCache[cgDesc.GetConsumerGroupUUID()]
	outputHost.cgMutex.RUnlock()
	s.True(ok, "cannot find cgcache entry")

	var nConns = 0
	var conn *consConnection
	cgCache.extMutex.RLock()
	for _, conn = range cgCache.connections {
		break
	}
	nConns = len(cgCache.connections)
	cgCache.extMutex.RUnlock()
	s.Equal(1, nConns, "wrong number of consumer connections")
	s.NotNil(conn, "failed to find consConnection within cgcache")

	creditUnblockCh <- struct{}{} // now unblock the readCreditsPump on the consconnection
	outputHost.Shutdown()
	<-streamDoneCh
	// now that the whole CG is unloaded, assert that we actually put msgs into message cache
	s.Equal(int64(1), conn.sentToMsgCache, "consConnection failed to put message into message cache")
}

func (s *OutputHostSuite) TestOutputHostAckMessage() {
	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil, nil)
	httpRequest := utilGetHTTPRequestWithPath("foo")
	ctx, _ := utilGetThriftContext()

	go outputHost.manageCgCache()

	destUUID := uuid.New()
	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr("/foo/bar")
	destDesc.DestinationUUID = common.StringPtr(destUUID)
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil).Once()
	s.mockMeta.On("ReadExtentStats", mock.Anything, mock.Anything).Return(nil, fmt.Errorf(`foo`))

	cgUUID := uuid.New()
	extUUID := uuid.New()
	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(cgUUID)
	cgDesc.DestinationUUID = common.StringPtr(destUUID)
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Twice()
	s.mockMeta.On("SetAckOffset", mock.Anything, mock.Anything).Return(nil)

	cgExt := shared.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(extUUID)
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &shared.ReadConsumerGroupExtentsResult_{}
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
	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil, nil)
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
	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil, nil)
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
	s.mockMeta.On("ReadExtentStats", mock.Anything, mock.Anything).Return(nil, fmt.Errorf(`foo`))

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
	cgExt := shared.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(extUUID1)
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &shared.ReadConsumerGroupExtentsResult_{}
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
	cg, ok := outputHost.cgCache[cgUUID]
	s.True(ok, "CG not found in the outputhost")
	s.NotNil(cg, "CG should be loaded but it is not")
	cg.extMutex.Lock()
	s.Equal(1, len(cg.extentCache))
	cg.extMutex.Unlock()
	outputHost.cgMutex.Unlock()

	// 7. Now add another extent to the "ReadConsumerGroupExtents" call (extUUID2)
	cgExt2 := shared.NewConsumerGroupExtent()
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
	cg.extMutex.Lock()
	s.Equal(2, len(cg.extentCache))
	cg.extMutex.Unlock()

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

	time.Sleep(1 * time.Second)
	cg.extMutex.Lock()
	s.True(cg.unloadInProgress, "refreshCGCache failed to unload extents for DELETED consumer group")
	cg.extMutex.Unlock()

	// 12. Shutdown
	outputHost.Shutdown()
}

// TestOutputHostReceiveMessageBatch receives a batch of messages and make sure
// we get the msg back
func (s *OutputHostSuite) TestOutputHostReceiveMessageBatch() {
	var count int32
	count = 10

	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil, nil)
	ctx, _ := utilGetThriftContext()

	destUUID := uuid.New()
	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr("/foo/bar")
	destDesc.DestinationUUID = common.StringPtr(destUUID)
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil).Once()
	s.mockMeta.On("ReadExtentStats", mock.Anything, mock.Anything).Return(nil, fmt.Errorf(`foo`))

	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(uuid.New())
	cgDesc.DestinationUUID = common.StringPtr(destUUID)
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Twice()

	cgExt := shared.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(uuid.New())
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &shared.ReadConsumerGroupExtentsResult_{}
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

	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil, nil)
	ctx, _ := utilGetThriftContext()

	destUUID := uuid.New()
	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr("/foo/bar")
	destDesc.DestinationUUID = common.StringPtr(destUUID)
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil)
	s.mockMeta.On("ReadExtentStats", mock.Anything, mock.Anything).Return(nil, fmt.Errorf(`foo`))

	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(uuid.New())
	cgDesc.DestinationUUID = common.StringPtr(destUUID)
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Twice()

	cgExt := shared.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(uuid.New())
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &shared.ReadConsumerGroupExtentsResult_{}
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

	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil, nil)
	ctx, _ := utilGetThriftContext()

	destUUID := uuid.New()
	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr("/foo/bar")
	destDesc.DestinationUUID = common.StringPtr(destUUID)
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil)
	s.mockMeta.On("ReadExtentStats", mock.Anything, mock.Anything).Return(nil, fmt.Errorf(`foo`))

	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(uuid.New())
	cgDesc.DestinationUUID = common.StringPtr(destUUID)
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Twice()

	cgExt := shared.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(uuid.New())
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &shared.ReadConsumerGroupExtentsResult_{}
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
	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil, nil)
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
	s.mockMeta.On("ReadExtentStats", mock.Anything, mock.Anything).Return(nil, fmt.Errorf(`foo`))

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
	cgExt := shared.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(extUUID1)
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &shared.ReadConsumerGroupExtentsResult_{}
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
	cg, ok := outputHost.cgCache[cgUUID]
	if ok {
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
	cg.extMutex.Lock()
	s.Equal(cg.unloadInProgress, true)
	cg.extMutex.Unlock()

	outputHost.Shutdown()
}

func (s *OutputHostSuite) TestOutputAckMgrReset() {
	outputHost, _ := NewOutputHost("outputhost-test-reset", s.mockService, s.mockMeta, nil, nil, nil)
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
	s.mockMeta.On("ReadExtentStats", mock.Anything, mock.Anything).Return(nil, fmt.Errorf(`foo`))

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
	cgExt := shared.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(extUUID1)
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &shared.ReadConsumerGroupExtentsResult_{}
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
	var readLevel ackIndex
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
	var newReadLevel ackIndex
	ackMgr.lk.RLock()
	newReadLevel = ackMgr.readLevel
	ackMgr.lk.RUnlock()

	// 10. make sure the readlevels are not the same
	assert.NotEqual(s.T(), readLevel, newReadLevel, "read levels should not be the same")
}

func (s *OutputHostSuite) TestOutputHostReplicaRollover() {

	count := 50

	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil, nil)
	httpRequest := utilGetHTTPRequestWithPath("foo")

	destUUID := uuid.New()
	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr("/foo/bar")
	destDesc.DestinationUUID = common.StringPtr(destUUID)
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil).Once()
	s.mockMeta.On("ReadExtentStats", mock.Anything, mock.Anything).Return(nil, fmt.Errorf(`foo`))

	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(uuid.New())
	cgDesc.DestinationUUID = common.StringPtr(destUUID)
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Twice()

	cgExt := shared.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(uuid.New())
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &shared.ReadConsumerGroupExtentsResult_{}
	cgRes.Extents = append(cgRes.Extents, cgExt)
	s.mockMeta.On("ReadConsumerGroupExtents", mock.Anything, mock.Anything).Return(cgRes, nil).Once()
	s.mockRead.On("Write", mock.Anything).Return(nil)

	writeDoneCh := make(chan struct{})
	var msgsRecv = set.New(count)

	s.mockCons.On("Write", mock.Anything).Return(nil).Run(func(args mock.Arguments) {

		ohc := args.Get(0).(*cherami.OutputHostCommand)

		if ohc.GetType() == cherami.OutputHostCommandType_MESSAGE {

			msg := ohc.GetMessage()
			id := msg.GetPayload().GetID()

			// ensure we don't see duplicates
			s.False(msgsRecv.Contains(id))

			if msgsRecv.Insert(id); msgsRecv.Count() == count {
				close(writeDoneCh)
			}
		}

	}).Times(count)

	cFlow := cherami.NewControlFlow()
	cFlow.Credits = common.Int32Ptr(int32(count))

	connOpenedCh := make(chan struct{})
	s.mockCons.On("Read").Return(cFlow, nil).Once().Run(func(args mock.Arguments) { close(connOpenedCh) })

	rmc := store.NewReadMessageContent()
	rmc.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_MESSAGE)

	var seqnum int64

	s.mockRead.On("Read").Return(rmc, nil).Run(func(args mock.Arguments) {
		seqnum++
		aMsg := store.NewAppendMessage()
		aMsg.SequenceNumber = common.Int64Ptr(seqnum)
		pMsg := cherami.NewPutMessage()
		pMsg.ID = common.StringPtr(strconv.Itoa(int(seqnum)))
		pMsg.Data = []byte(fmt.Sprintf("seqnum=%d", seqnum))
		aMsg.Payload = pMsg
		rMsg := store.NewReadMessage()
		rMsg.Message = aMsg
		rMsg.Address = common.Int64Ptr(1234000000 + seqnum)
		rmc.Message = rMsg
	}).Times(20)

	// simulate error from a replica, that should cause outputhost to resume
	// by re-connecting to the replica
	s.mockRead.On("Read").Return(0, errors.New("store error")).Once()

	s.mockRead.On("Read").Return(rmc, nil).Run(func(args mock.Arguments) {
		seqnum++
		aMsg := store.NewAppendMessage()
		aMsg.SequenceNumber = common.Int64Ptr(seqnum)
		pMsg := cherami.NewPutMessage()
		pMsg.ID = common.StringPtr(strconv.Itoa(int(seqnum)))
		pMsg.Data = []byte(fmt.Sprintf("seqnum=%d", seqnum))
		aMsg.Payload = pMsg
		rMsg := store.NewReadMessage()
		rMsg.Message = aMsg
		rMsg.Address = common.Int64Ptr(1234000000 + seqnum)
		rmc.Message = rMsg
	}).Times(count - 20)

	rmcSeal := store.NewReadMessageContent()
	rmcSeal.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_SEALED)
	rmcSeal.Sealed = store.NewExtentSealedError()
	s.mockRead.On("Read").Return(rmcSeal, nil).Once()

	streamDoneCh := make(chan struct{})
	go func() {
		outputHost.OpenConsumerStreamHandler(s.mockHTTPResponse, httpRequest)
		close(streamDoneCh)
	}()

	// close the read stream
	creditUnblockCh := make(chan struct{})
	s.mockRead.On("Read").Return(nil, io.EOF)
	s.mockCons.On("Read").Return(nil, io.EOF).Run(func(args mock.Arguments) { <-writeDoneCh; <-creditUnblockCh })

	<-connOpenedCh // wait for the consConnection to open

	// look up cgcache and the underlying client connnection
	outputHost.cgMutex.RLock()
	cgCache, ok := outputHost.cgCache[cgDesc.GetConsumerGroupUUID()]
	outputHost.cgMutex.RUnlock()
	s.True(ok, "cannot find cgcache entry")

	var nConns = 0
	var conn *consConnection
	cgCache.extMutex.RLock()
	for _, conn = range cgCache.connections {
		break
	}
	nConns = len(cgCache.connections)
	cgCache.extMutex.RUnlock()
	s.Equal(1, nConns, "wrong number of consumer connections")
	s.NotNil(conn, "failed to find consConnection within cgcache")

	creditUnblockCh <- struct{}{} // now unblock the readCreditsPump on the consconnection
	<-streamDoneCh

	s.mockHTTPResponse.AssertNotCalled(s.T(), "WriteHeader", mock.Anything)
	s.Equal(int64(count), conn.sentMsgs, "wrong sentMsgs count")
	s.Equal(int64(0), conn.reSentMsgs, "wrong reSentMsgs count")
	s.Equal(conn.sentMsgs, conn.sentToMsgCache, "sentMsgs != sentToMsgCache")
	outputHost.Shutdown()
}

func (s *OutputHostSuite) TestOutputHostSkipOlder() {

	count := 60
	skipCount := 20
	nonSkipCount := count - skipCount
	skipOlder := 60 * time.Second

	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil, nil)
	httpRequest := utilGetHTTPRequestWithPath("foo")

	destUUID := uuid.New()
	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr("/foo/bar")
	destDesc.DestinationUUID = common.StringPtr(destUUID)
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil).Once()
	s.mockMeta.On("ReadExtentStats", mock.Anything, mock.Anything).Return(nil, fmt.Errorf(`foo`))

	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(uuid.New())
	cgDesc.DestinationUUID = common.StringPtr(destUUID)
	cgDesc.SkipOlderMessagesSeconds = common.Int32Ptr(int32(skipOlder.Seconds()))
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Twice()

	cgExt := shared.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(uuid.New())
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &shared.ReadConsumerGroupExtentsResult_{}
	cgRes.Extents = append(cgRes.Extents, cgExt)
	s.mockMeta.On("ReadConsumerGroupExtents", mock.Anything, mock.Anything).Return(cgRes, nil).Once()
	s.mockRead.On("Write", mock.Anything).Return(nil)

	s.mockStore.On("GetAddressFromTimestamp", mock.Anything, mock.Anything).Once().Return(&store.GetAddressFromTimestampResult_{
		Address:        common.Int64Ptr(1234000000),
		SequenceNumber: common.Int64Ptr(1),
		Sealed:         common.BoolPtr(false),
	}, nil).Run(func(args mock.Arguments) {

		req := args.Get(1).(*store.GetAddressFromTimestampRequest)
		s.True(req.GetTimestamp() <= time.Now().Add(-skipOlder).UnixNano())
	})

	// set enqueue times older that 'now - skip-older'
	tSkipOlder := time.Now().UnixNano() - int64(skipOlder)

	writeDoneCh := make(chan struct{})

	var recvMsgs int
	s.mockCons.On("Write", mock.Anything).Return(nil).Run(func(args mock.Arguments) {

		ohc := args.Get(0).(*cherami.OutputHostCommand)

		if ohc.GetType() == cherami.OutputHostCommandType_MESSAGE {

			recvMsgs++
			msg := ohc.GetMessage()
			tEnq := msg.GetEnqueueTimeUtc()
			s.True(tEnq == 0 || tEnq > tSkipOlder) // ensure all messages are strictly over the skip-older threshold

			if recvMsgs == nonSkipCount {
				close(writeDoneCh)
			}
		}

	}).Times(count)

	cFlow := cherami.NewControlFlow()
	cFlow.Credits = common.Int32Ptr(int32(count))

	connOpenedCh := make(chan struct{})
	s.mockCons.On("Read").Return(cFlow, nil).Once().Run(func(args mock.Arguments) { close(connOpenedCh) })

	rmc := store.NewReadMessageContent()
	rmc.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_MESSAGE)

	var seqnum int64

	// send 'skipCount' messages older than tSkipOlder
	s.mockRead.On("Read").Return(rmc, nil).Run(func(args mock.Arguments) {

		seqnum++
		aMsg := store.NewAppendMessage()
		aMsg.SequenceNumber = common.Int64Ptr(seqnum)
		aMsg.EnqueueTimeUtc = common.Int64Ptr(tSkipOlder - (int64(skipCount)-seqnum)*int64(time.Second))
		pMsg := cherami.NewPutMessage()
		pMsg.ID = common.StringPtr(strconv.Itoa(int(seqnum)))
		pMsg.Data = []byte(fmt.Sprintf("seqnum=%d", seqnum))
		aMsg.Payload = pMsg
		rMsg := store.NewReadMessage()
		rMsg.Message = aMsg
		rMsg.Address = common.Int64Ptr(1234000000 + seqnum)
		rmc.Message = rMsg

	}).Times(skipCount)

	// send 'nonSkipCount - 1' messages that are current
	s.mockRead.On("Read").Return(rmc, nil).Run(func(args mock.Arguments) {

		seqnum++
		aMsg := store.NewAppendMessage()
		aMsg.SequenceNumber = common.Int64Ptr(seqnum)
		aMsg.EnqueueTimeUtc = common.Int64Ptr(time.Now().UnixNano())
		pMsg := cherami.NewPutMessage()
		pMsg.ID = common.StringPtr(strconv.Itoa(int(seqnum)))
		pMsg.Data = []byte(fmt.Sprintf("seqnum=%d", seqnum))
		aMsg.Payload = pMsg
		rMsg := store.NewReadMessage()
		rMsg.Message = aMsg
		rMsg.Address = common.Int64Ptr(1234000000 + seqnum)
		rmc.Message = rMsg

	}).Times(nonSkipCount - 1)

	// send one message with no enqueue-time (that we should receive)
	s.mockRead.On("Read").Return(rmc, nil).Run(func(args mock.Arguments) {

		seqnum++
		aMsg := store.NewAppendMessage()
		aMsg.SequenceNumber = common.Int64Ptr(seqnum)
		// aMsg.EnqueueTimeUtc = common.Int64Ptr(tSkipOlder - (int64(skipCount)-seqnum)*int64(time.Second))
		pMsg := cherami.NewPutMessage()
		pMsg.ID = common.StringPtr(strconv.Itoa(int(seqnum)))
		pMsg.Data = []byte(fmt.Sprintf("seqnum=%d", seqnum))
		aMsg.Payload = pMsg
		rMsg := store.NewReadMessage()
		rMsg.Message = aMsg
		rMsg.Address = common.Int64Ptr(1234000000 + seqnum)
		rmc.Message = rMsg

	}).Times(1)

	s.mockRead.On("Read").Return(&store.ReadMessageContent{
		Type:   store.ReadMessageContentTypePtr(store.ReadMessageContentType_SEALED),
		Sealed: store.NewExtentSealedError(),
	}, nil).Once()

	streamDoneCh := make(chan struct{})
	go func() {
		outputHost.OpenConsumerStreamHandler(s.mockHTTPResponse, httpRequest)
		close(streamDoneCh)
	}()

	// close the read stream
	creditUnblockCh := make(chan struct{})
	s.mockRead.On("Read").Return(nil, io.EOF)
	s.mockCons.On("Read").Return(nil, io.EOF).Run(func(args mock.Arguments) {
		<-writeDoneCh
		<-creditUnblockCh
	})

	<-connOpenedCh // wait for the consConnection to open

	// look up cgcache and the underlying client connnection
	outputHost.cgMutex.RLock()
	cgCache, ok := outputHost.cgCache[cgDesc.GetConsumerGroupUUID()]
	outputHost.cgMutex.RUnlock()
	s.True(ok, "cannot find cgcache entry")

	var nConns = 0
	var conn *consConnection
	cgCache.extMutex.RLock()
	for _, conn = range cgCache.connections {
		break
	}
	nConns = len(cgCache.connections)
	cgCache.extMutex.RUnlock()
	s.Equal(1, nConns, "wrong number of consumer connections")
	s.NotNil(conn, "failed to find consConnection within cgcache")

	creditUnblockCh <- struct{}{} // now unblock the readCreditsPump on the consconnection
	<-streamDoneCh

	s.mockHTTPResponse.AssertNotCalled(s.T(), "WriteHeader", mock.Anything)
	s.Equal(int64(nonSkipCount), conn.sentMsgs, "wrong sentMsgs count")
	s.Equal(conn.sentMsgs, conn.sentToMsgCache, "sentMsgs != sentToMsgCache")
	outputHost.Shutdown()
}

func (s *OutputHostSuite) TestOutputHostDelay() {

	count := 60
	delay := 50 * time.Second
	skipOlder := 20 * time.Second

	skipCount := 20
	nonSkipCount := count - skipCount

	startFrom := time.Now()
	startFromExpected := startFrom.Add(-delay)

	outputHost, _ := NewOutputHost("outputhost-test", s.mockService, s.mockMeta, nil, nil, nil)
	httpRequest := utilGetHTTPRequestWithPath("foo")

	destUUID := uuid.New()
	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr("/foo/bar")
	destDesc.DestinationUUID = common.StringPtr(destUUID)
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil).Once()
	s.mockMeta.On("ReadExtentStats", mock.Anything, mock.Anything).Return(nil, fmt.Errorf(`foo`))

	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(uuid.New())
	cgDesc.DestinationUUID = common.StringPtr(destUUID)
	cgDesc.DelaySeconds = common.Int32Ptr(int32(delay.Seconds()))
	cgDesc.SkipOlderMessagesSeconds = common.Int32Ptr(int32(skipOlder.Seconds()))
	cgDesc.StartFrom = common.Int64Ptr(startFrom.UnixNano())
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Twice()

	cgExt := shared.NewConsumerGroupExtent()
	cgExt.ExtentUUID = common.StringPtr(uuid.New())
	cgExt.StoreUUIDs = []string{"mock"}

	cgRes := &shared.ReadConsumerGroupExtentsResult_{}
	cgRes.Extents = append(cgRes.Extents, cgExt)
	s.mockMeta.On("ReadConsumerGroupExtents", mock.Anything, mock.Anything).Return(cgRes, nil).Once()
	s.mockRead.On("Write", mock.Anything).Return(nil)

	s.mockStore.On("GetAddressFromTimestamp", mock.Anything, mock.Anything).Once().Return(&store.GetAddressFromTimestampResult_{
		Address:        common.Int64Ptr(1234000000),
		SequenceNumber: common.Int64Ptr(1),
		Sealed:         common.BoolPtr(false),
	}, nil).Run(func(args mock.Arguments) {

		req := args.Get(1).(*store.GetAddressFromTimestampRequest)
		s.Equal(req.GetTimestamp(), startFromExpected.UnixNano())
	})

	tSkipOlder := time.Now().Add(-skipOlder)
	tVisible := time.Now().Add(-skipOlder).Add(-delay)

	writeDoneCh := make(chan struct{})

	var recvMsgs int
	s.mockCons.On("Write", mock.Anything).Return(nil).Run(func(args mock.Arguments) {

		ohc := args.Get(0).(*cherami.OutputHostCommand)

		if ohc.GetType() == cherami.OutputHostCommandType_MESSAGE {

			recvMsgs++
			msg := ohc.GetMessage()

			// compute visible time
			msgVisibilityTime := time.Unix(0, msg.GetEnqueueTimeUtc()).Add(delay)

			s.True(tSkipOlder.Before(msgVisibilityTime)) // message should not have been skipped
			s.True(time.Now().After(msgVisibilityTime))  // message is expected to be 'visible'

			if recvMsgs == nonSkipCount {
				close(writeDoneCh)
			}
		}

	}).Times(count)

	cFlow := cherami.NewControlFlow()
	cFlow.Credits = common.Int32Ptr(int32(count))

	connOpenedCh := make(chan struct{})
	s.mockCons.On("Read").Return(cFlow, nil).Once().Run(func(args mock.Arguments) { close(connOpenedCh) })

	rmc := store.NewReadMessageContent()
	rmc.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_MESSAGE)

	var seqnum int64

	s.mockRead.On("Read").Return(rmc, nil).Run(func(args mock.Arguments) {

		seqnum++
		aMsg := store.NewAppendMessage()
		aMsg.SequenceNumber = common.Int64Ptr(seqnum)
		// set the enqueue-time such that 'visibleCount' messages are readily visible (ie, past delay)
		// while the rest get visible one every 100ms
		aMsg.EnqueueTimeUtc = common.Int64Ptr(tVisible.UnixNano() - (int64(skipCount)-seqnum)*int64(100*time.Millisecond))

		pMsg := cherami.NewPutMessage()
		pMsg.ID = common.StringPtr(strconv.Itoa(int(seqnum)))
		pMsg.Data = []byte(fmt.Sprintf("seqnum=%d", seqnum))
		aMsg.Payload = pMsg
		rMsg := store.NewReadMessage()
		rMsg.Message = aMsg
		rMsg.Address = common.Int64Ptr(1234000000 + seqnum)
		rmc.Message = rMsg

	}).Times(count)

	s.mockRead.On("Read").Return(&store.ReadMessageContent{
		Type:   store.ReadMessageContentTypePtr(store.ReadMessageContentType_SEALED),
		Sealed: store.NewExtentSealedError(),
	}, nil).Once()

	streamDoneCh := make(chan struct{})
	go func() {
		outputHost.OpenConsumerStreamHandler(s.mockHTTPResponse, httpRequest)
		close(streamDoneCh)
	}()

	// close the read stream
	creditUnblockCh := make(chan struct{})
	s.mockRead.On("Read").Return(nil, io.EOF)
	s.mockCons.On("Read").Return(nil, io.EOF).Run(func(args mock.Arguments) {
		<-writeDoneCh
		<-creditUnblockCh
	})

	<-connOpenedCh // wait for the consConnection to open

	// look up cgcache and the underlying client connnection
	outputHost.cgMutex.RLock()
	cgCache, ok := outputHost.cgCache[cgDesc.GetConsumerGroupUUID()]
	outputHost.cgMutex.RUnlock()
	s.True(ok, "cannot find cgcache entry")

	var nConns = 0
	var conn *consConnection
	cgCache.extMutex.RLock()
	for _, conn = range cgCache.connections {
		break
	}
	nConns = len(cgCache.connections)
	cgCache.extMutex.RUnlock()
	s.Equal(1, nConns, "wrong number of consumer connections")
	s.NotNil(conn, "failed to find consConnection within cgcache")

	creditUnblockCh <- struct{}{} // now unblock the readCreditsPump on the consconnection
	<-streamDoneCh

	s.mockHTTPResponse.AssertNotCalled(s.T(), "WriteHeader", mock.Anything)
	s.Equal(int64(nonSkipCount), conn.sentMsgs, "wrong sentMsgs count")
	s.Equal(conn.sentMsgs, conn.sentToMsgCache, "sentMsgs != sentToMsgCache")
	outputHost.Shutdown()
}
