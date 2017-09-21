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

package replicator

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
	mockcommon "github.com/uber/cherami-server/test/mocks/common"
	mockcontroller "github.com/uber/cherami-server/test/mocks/controllerhost"
	mockmeta "github.com/uber/cherami-server/test/mocks/metadata"
	mockreplicator "github.com/uber/cherami-server/test/mocks/replicator"
	mockStore "github.com/uber/cherami-server/test/mocks/storehost"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/tchannel-go"
)

type ReplicatorSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
	cfg                         configure.CommonAppConfig
	mockInStream                *mockreplicator.MockBStoreOpenReadStreamInCallForReplicator
	mockMeta                    *mockmeta.TChanMetadataService
	mockOutStream               *mockreplicator.MockBStoreOpenReadStreamOutCallForReplicator
	mockService                 *common.MockService
	mockReplicatorClientFactory *mockreplicator.MockReplicatorClientFactory
	mockControllerClient        *mockcontroller.MockControllerHost
	mockStoreClient             *mockStore.MockStoreHost
	mockClientFactory           *mockcommon.MockClientFactory
	mockWSConnector             *mockcommon.MockWSConnector
	mockHTTPResponse            *mockcommon.MockHTTPResponseWriter
}

func TestReplicatorSuite(t *testing.T) {
	suite.Run(t, new(ReplicatorSuite))
}

func (s *ReplicatorSuite) SetupCommonMock() {
	s.mockInStream = new(mockreplicator.MockBStoreOpenReadStreamInCallForReplicator)
	s.mockOutStream = new(mockreplicator.MockBStoreOpenReadStreamOutCallForReplicator)
	s.mockReplicatorClientFactory = new(mockreplicator.MockReplicatorClientFactory)
	s.mockReplicatorClientFactory.On("GetHostsForAllDeployment").Return(nil)
	s.mockStoreClient = new(mockStore.MockStoreHost)
	s.mockControllerClient = new(mockcontroller.MockControllerHost)
	s.mockClientFactory = new(mockcommon.MockClientFactory)
	s.mockClientFactory.On("GetControllerClient").Return(s.mockControllerClient, nil)
	s.mockClientFactory.On("GetThriftStoreClientUUID", mock.Anything, mock.Anything).Return(s.mockStoreClient, "hostport", nil)

	s.mockWSConnector = new(mockcommon.MockWSConnector)
	s.mockWSConnector.On("AcceptReplicationReadStream", mock.Anything, mock.Anything).Return(s.mockInStream, nil)
	s.mockWSConnector.On("OpenReadStream", mock.Anything, mock.Anything).Return(s.mockOutStream, nil)

	rpm := common.NewMockRingpopMonitor()
	rpm.Add(common.StoreServiceName, "mock_uuid", "127.0.0.1")

	s.mockMeta = new(mockmeta.TChanMetadataService)

	s.mockService = new(common.MockService)
	s.mockService.On("GetConfig").Return(s.cfg.GetServiceConfig(common.ReplicatorServiceName))
	s.mockService.On("GetTChannel").Return(&tchannel.Channel{})
	s.mockService.On("GetHostPort").Return("replicator:port")
	s.mockService.On("GetHostUUID").Return("99999999-9999-9999-9999-999999999999")
	s.mockService.On("GetRingpopMonitor").Return(rpm)
	s.mockService.On("GetWSConnector").Return(s.mockWSConnector)
	s.mockService.On("GetMetricsReporter").Return(common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig()))
	s.mockService.On("GetDConfigClient").Return(dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.ReplicatorServiceName))
	s.mockService.On("GetClientFactory").Return(s.mockClientFactory)
	s.mockService.On("Stop").Return()

	s.mockHTTPResponse = new(mockcommon.MockHTTPResponseWriter)
	s.mockHTTPResponse.On("Header").Return(http.Header{})
	s.mockHTTPResponse.On("WriteHeader", mock.Anything)
	s.mockHTTPResponse.On("Write", mock.Anything).Return(0, nil)
}

func (s *ReplicatorSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.cfg = common.SetupServerConfig(configure.NewCommonConfigure())
	s.SetupCommonMock()
}

func (s *ReplicatorSuite) TearDownTest() {
}

func utilGetHTTPRequestWithPath() *http.Request {
	req, _ := http.NewRequest("GET", "mock_url", nil)
	req.Header.Add("DestinationUUID", "481c4d7e-7a1c-4ce0-9731-180115e390ea")
	req.Header.Add("DestinationType", "PLAIN")
	req.Header.Add("ExtentUUID", "481c4d7e-7a1c-4ce0-9731-180115e390ea")
	req.Header.Add("ConsumerGroupUUID", "481c4d7e-7a1c-4ce0-9731-180115e390ea")
	req.Header.Add("Address", "0")
	req.Header.Add("Inclusive", "false")
	return req
}

func (s *ReplicatorSuite) TestReplicatorReadMessage() {
	count := 10

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	httpRequest := utilGetHTTPRequestWithPath()

	mExtent := shared.NewExtent()
	mExtent.StoreUUIDs = []string{"mock_uuid"}
	mExtentStats := shared.NewExtentStats()
	mExtentStats.Extent = mExtent
	mReadExtentStatsRes := metadata.NewReadExtentStatsResult_()
	mReadExtentStatsRes.ExtentStats = mExtentStats
	s.mockMeta.On("ReadExtentStats", mock.Anything, mock.Anything).Return(mReadExtentStatsRes, nil).Once()

	mDestDesc := shared.NewDestinationDescription()
	mDestDesc.Path = common.StringPtr(`/a/b`)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(mDestDesc, nil).Once()

	s.mockOutStream.On("Write", mock.Anything).Return(nil)

	cFlow := cherami.NewControlFlow()
	cFlow.Credits = common.Int32Ptr(int32(count))
	s.mockInStream.On("Read").Return(cFlow, nil).Once()

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

		s.mockOutStream.On("Read").Return(rmc, nil).Once()
		s.mockInStream.On("Write", mock.Anything).Return(nil).Once()
	}

	// close the stream after 2 seconds. The wait is necessary otherwise stream may closed before
	// message is transferred from outConn to inConn
	s.mockOutStream.On("Read").Return(nil, io.EOF).After(2 * time.Second)
	s.mockInStream.On("Read").Return(nil, io.EOF).After(2 * time.Second)

	// now make the call, assert call is success and all expectations are met
	repliator.OpenReplicationReadStreamHandler(s.mockHTTPResponse, httpRequest)
	s.mockHTTPResponse.AssertNotCalled(s.T(), "WriteHeader", mock.Anything)
	s.mockInStream.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestCreateDestinationUUID() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	destUUID := uuid.New()
	req := &shared.CreateDestinationUUIDRequest{
		DestinationUUID: common.StringPtr(destUUID),
		Request:         shared.NewCreateDestinationRequest(),
	}

	s.mockMeta.On("CreateDestinationUUID", mock.Anything, mock.Anything).Return(
		&shared.DestinationDescription{
			DestinationUUID: common.StringPtr(destUUID),
		}, nil,
	).Run(func(args mock.Arguments) {
		createReq := args.Get(1).(*shared.CreateDestinationUUIDRequest)
		s.True(createReq.IsSetRequest())
		s.Equal(req.GetDestinationUUID(), createReq.GetDestinationUUID())
	})
	destDesc, err := repliator.CreateDestinationUUID(nil, req)
	s.NoError(err)
	s.NotNil(destDesc)
	s.mockMeta.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestCreateRemoteDestinationUUIDBadRequests() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)

	err := repliator.CreateRemoteDestinationUUID(nil, shared.NewCreateDestinationUUIDRequest())
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)

	err = repliator.CreateRemoteDestinationUUID(nil, &shared.CreateDestinationUUIDRequest{
		DestinationUUID: common.StringPtr(uuid.New()),
	})
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)

	err = repliator.CreateRemoteDestinationUUID(nil, &shared.CreateDestinationUUIDRequest{
		Request: shared.NewCreateDestinationRequest(),
	})
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)

	err = repliator.CreateRemoteDestinationUUID(nil, &shared.CreateDestinationUUIDRequest{
		DestinationUUID: common.StringPtr(uuid.New()),
		Request: &shared.CreateDestinationRequest{
			IsMultiZone: common.BoolPtr(false),
		},
	})
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)

	err = repliator.CreateRemoteDestinationUUID(nil, &shared.CreateDestinationUUIDRequest{
		DestinationUUID: common.StringPtr(uuid.New()),
		Request: &shared.CreateDestinationRequest{
			IsMultiZone: common.BoolPtr(true),
		},
	})
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)

	err = repliator.CreateRemoteDestinationUUID(nil, &shared.CreateDestinationUUIDRequest{
		DestinationUUID: common.StringPtr(uuid.New()),
		Request: &shared.CreateDestinationRequest{
			IsMultiZone: common.BoolPtr(true),
			ZoneConfigs: []*shared.DestinationZoneConfig{},
		},
	})
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)
}

func (s *ReplicatorSuite) TestCreateRemoteDestinationUUIDFailed() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	createReq := &shared.CreateDestinationUUIDRequest{
		DestinationUUID: common.StringPtr(uuid.New()),
		Request: &shared.CreateDestinationRequest{
			IsMultiZone: common.BoolPtr(true),
			ZoneConfigs: []*shared.DestinationZoneConfig{shared.NewDestinationZoneConfig()},
		},
	}
	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("CreateDestinationUUID", mock.Anything, mock.Anything).Return(nil, &shared.BadRequestError{Message: "test2"})
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	err := repliator.createDestinationRemoteCall(`zone1`, repliator.logger, createReq)
	s.Error(err)
	s.mockReplicatorClientFactory.AssertExpectations(s.T())
	mockReplicator.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestCreateRemoteDestinationUUIDSuccess() {
	destUUID := uuid.New()
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	createReq := &shared.CreateDestinationUUIDRequest{
		DestinationUUID: common.StringPtr(destUUID),
		Request: &shared.CreateDestinationRequest{
			IsMultiZone: common.BoolPtr(true),
			ZoneConfigs: []*shared.DestinationZoneConfig{shared.NewDestinationZoneConfig()},
		},
	}

	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("CreateDestinationUUID", mock.Anything, mock.Anything).Return(nil, nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.CreateDestinationUUIDRequest)
		s.True(req.IsSetRequest())
		s.True(req.GetRequest().GetIsMultiZone())
		s.Equal(destUUID, req.GetDestinationUUID())
	})
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	err := repliator.createDestinationRemoteCall(`zone1`, repliator.logger, createReq)
	s.NoError(err)
	s.mockReplicatorClientFactory.AssertExpectations(s.T())
	mockReplicator.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestUpdateDestination() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	destUUID := uuid.New()
	newEmail := `newowner@uber.com`
	req := &shared.UpdateDestinationRequest{
		DestinationUUID: common.StringPtr(destUUID),
		OwnerEmail:      common.StringPtr(newEmail),
	}

	s.mockMeta.On("UpdateDestination", mock.Anything, mock.Anything).Return(
		&shared.DestinationDescription{
			DestinationUUID: common.StringPtr(destUUID),
			OwnerEmail:      common.StringPtr(newEmail),
		}, nil,
	).Run(func(args mock.Arguments) {
		updateReq := args.Get(1).(*shared.UpdateDestinationRequest)
		s.Equal(req.GetDestinationUUID(), updateReq.GetDestinationUUID())
		s.Equal(req.GetOwnerEmail(), updateReq.GetOwnerEmail())
	})
	destDesc, err := repliator.UpdateDestination(nil, req)
	s.NoError(err)
	s.NotNil(destDesc)
	s.mockMeta.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestUpdateRemoteDestinationFailed() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	destUUID := uuid.New()
	newEmail := `newowner@uber.com`
	req := &shared.UpdateDestinationRequest{
		DestinationUUID: common.StringPtr(destUUID),
		OwnerEmail:      common.StringPtr(newEmail),
	}

	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("UpdateDestination", mock.Anything, mock.Anything).Return(nil, &shared.InternalServiceError{Message: "test2"})
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	err := repliator.updateDestinationRemoteCall(`zone1`, repliator.logger, req)
	s.Error(err)
	s.mockReplicatorClientFactory.AssertExpectations(s.T())
	mockReplicator.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestUpdateRemoteDestinationSuccess() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	destUUID := uuid.New()
	newEmail := `newowner@uber.com`
	req := &shared.UpdateDestinationRequest{
		DestinationUUID: common.StringPtr(destUUID),
		OwnerEmail:      common.StringPtr(newEmail),
	}

	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("UpdateDestination", mock.Anything, mock.Anything).Return(nil, nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.UpdateDestinationRequest)
		s.Equal(destUUID, req.GetDestinationUUID())
		s.Equal(newEmail, req.GetOwnerEmail())
	})
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	err := repliator.updateDestinationRemoteCall(`zone1`, repliator.logger, req)
	s.NoError(err)
	s.mockReplicatorClientFactory.AssertExpectations(s.T())
	mockReplicator.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestDeleteDestination() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	path := `path`
	req := &shared.DeleteDestinationRequest{
		Path: common.StringPtr(path),
	}

	s.mockMeta.On("DeleteDestination", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		delReq := args.Get(1).(*shared.DeleteDestinationRequest)
		s.Equal(path, delReq.GetPath())
	})
	err := repliator.DeleteDestination(nil, req)
	s.NoError(err)
	s.mockMeta.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestDeleteRemoteDestinationFailed() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	path := `path`
	req := &shared.DeleteDestinationRequest{
		Path: common.StringPtr(path),
	}

	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("DeleteDestination", mock.Anything, mock.Anything).Return(&shared.InternalServiceError{Message: "test2"})
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	err := repliator.deleteDestinationRemoteCall(`zone1`, repliator.logger, req)
	s.Error(err)
	s.mockReplicatorClientFactory.AssertExpectations(s.T())
	mockReplicator.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestDeleteRemoteDestinationSuccess() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	path := `path`
	req := &shared.DeleteDestinationRequest{
		Path: common.StringPtr(path),
	}

	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("DeleteDestination", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.DeleteDestinationRequest)
		s.Equal(path, req.GetPath())
	})
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	err := repliator.deleteDestinationRemoteCall(`zone1`, repliator.logger, req)
	s.NoError(err)
	s.mockReplicatorClientFactory.AssertExpectations(s.T())
	mockReplicator.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestCreateCgUUID() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	cgUUID := uuid.New()
	req := &shared.CreateConsumerGroupUUIDRequest{
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		Request:           shared.NewCreateConsumerGroupRequest(),
	}

	s.mockMeta.On("CreateConsumerGroupUUID", mock.Anything, mock.Anything).Return(
		&shared.ConsumerGroupDescription{
			ConsumerGroupUUID: common.StringPtr(cgUUID),
		}, nil,
	).Run(func(args mock.Arguments) {
		createReq := args.Get(1).(*shared.CreateConsumerGroupUUIDRequest)
		s.True(createReq.IsSetRequest())
		s.Equal(req.GetConsumerGroupUUID(), createReq.GetConsumerGroupUUID())
	})
	cgDesc, err := repliator.CreateConsumerGroupUUID(nil, req)
	s.NoError(err)
	s.NotNil(cgDesc)
	s.mockMeta.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestCreateRemoteConsumerGroupUUIDBadRequests() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)

	err := repliator.CreateRemoteConsumerGroupUUID(nil, shared.NewCreateConsumerGroupUUIDRequest())
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)

	err = repliator.CreateRemoteConsumerGroupUUID(nil, &shared.CreateConsumerGroupUUIDRequest{
		ConsumerGroupUUID: common.StringPtr(uuid.New()),
	})
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)

	err = repliator.CreateRemoteConsumerGroupUUID(nil, &shared.CreateConsumerGroupUUIDRequest{
		Request: shared.NewCreateConsumerGroupRequest(),
	})
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)

	err = repliator.CreateRemoteConsumerGroupUUID(nil, &shared.CreateConsumerGroupUUIDRequest{
		ConsumerGroupUUID: common.StringPtr(uuid.New()),
		Request: &shared.CreateConsumerGroupRequest{
			IsMultiZone: common.BoolPtr(false),
		},
	})
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)

	err = repliator.CreateRemoteConsumerGroupUUID(nil, &shared.CreateConsumerGroupUUIDRequest{
		ConsumerGroupUUID: common.StringPtr(uuid.New()),
		Request: &shared.CreateConsumerGroupRequest{
			IsMultiZone: common.BoolPtr(true),
		},
	})
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)

	err = repliator.CreateRemoteConsumerGroupUUID(nil, &shared.CreateConsumerGroupUUIDRequest{
		ConsumerGroupUUID: common.StringPtr(uuid.New()),
		Request: &shared.CreateConsumerGroupRequest{
			IsMultiZone: common.BoolPtr(true),
			ZoneConfigs: []*shared.ConsumerGroupZoneConfig{},
		},
	})
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)
}

func (s *ReplicatorSuite) TestCreateRemoteCgUUIDFailed() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	createReq := &shared.CreateConsumerGroupUUIDRequest{
		ConsumerGroupUUID: common.StringPtr(uuid.New()),
		Request: &shared.CreateConsumerGroupRequest{
			IsMultiZone: common.BoolPtr(true),
			ZoneConfigs: []*shared.ConsumerGroupZoneConfig{shared.NewConsumerGroupZoneConfig()},
		},
	}
	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("CreateConsumerGroupUUID", mock.Anything, mock.Anything).Return(nil, &shared.BadRequestError{Message: "test2"})
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	err := repliator.createConsumerGroupRemoteCall(`zone1`, repliator.logger, createReq)
	s.Error(err)
	s.mockReplicatorClientFactory.AssertExpectations(s.T())
	mockReplicator.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestCreateRemoteCgUUIDSuccess() {
	cgUUID := uuid.New()
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	createReq := &shared.CreateConsumerGroupUUIDRequest{
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		Request: &shared.CreateConsumerGroupRequest{
			IsMultiZone: common.BoolPtr(true),
			ZoneConfigs: []*shared.ConsumerGroupZoneConfig{shared.NewConsumerGroupZoneConfig()},
		},
	}

	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("CreateConsumerGroupUUID", mock.Anything, mock.Anything).Return(nil, nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.CreateConsumerGroupUUIDRequest)
		s.True(req.IsSetRequest())
		s.True(req.GetRequest().GetIsMultiZone())
		s.Equal(cgUUID, req.GetConsumerGroupUUID())
	})
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	err := repliator.createConsumerGroupRemoteCall(`zone1`, repliator.logger, createReq)
	s.NoError(err)
	s.mockReplicatorClientFactory.AssertExpectations(s.T())
	mockReplicator.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestUpdateCg() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	cgName := `cg`
	newEmail := `newowner@uber.com`
	req := &shared.UpdateConsumerGroupRequest{
		ConsumerGroupName: common.StringPtr(cgName),
		OwnerEmail:        common.StringPtr(newEmail),
	}

	s.mockMeta.On("UpdateConsumerGroup", mock.Anything, mock.Anything).Return(
		&shared.ConsumerGroupDescription{
			ConsumerGroupName: common.StringPtr(cgName),
			OwnerEmail:        common.StringPtr(newEmail),
		}, nil,
	).Run(func(args mock.Arguments) {
		updateReq := args.Get(1).(*shared.UpdateConsumerGroupRequest)
		s.Equal(req.GetConsumerGroupName(), updateReq.GetConsumerGroupName())
		s.Equal(req.GetOwnerEmail(), updateReq.GetOwnerEmail())
	})
	cgDesc, err := repliator.UpdateConsumerGroup(nil, req)
	s.NoError(err)
	s.NotNil(cgDesc)
	s.mockMeta.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestUpdateRemoteCgFailed() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	cgName := `cg`
	newEmail := `newowner@uber.com`
	req := &shared.UpdateConsumerGroupRequest{
		ConsumerGroupName: common.StringPtr(cgName),
		OwnerEmail:        common.StringPtr(newEmail),
	}

	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("UpdateConsumerGroup", mock.Anything, mock.Anything).Return(nil, &shared.InternalServiceError{Message: "test2"})
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	err := repliator.updateCgRemoteCall(`zone1`, repliator.logger, req)
	s.Error(err)
	s.mockReplicatorClientFactory.AssertExpectations(s.T())
	mockReplicator.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestUpdateRemoteCgSuccess() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	cgName := uuid.New()
	newEmail := `newowner@uber.com`
	req := &shared.UpdateConsumerGroupRequest{
		ConsumerGroupName: common.StringPtr(cgName),
		OwnerEmail:        common.StringPtr(newEmail),
	}

	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("UpdateConsumerGroup", mock.Anything, mock.Anything).Return(nil, nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.UpdateConsumerGroupRequest)
		s.Equal(cgName, req.GetConsumerGroupName())
		s.Equal(newEmail, req.GetOwnerEmail())
	})
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	err := repliator.updateCgRemoteCall(`zone1`, repliator.logger, req)
	s.NoError(err)
	s.mockReplicatorClientFactory.AssertExpectations(s.T())
	mockReplicator.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestDeleteCg() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	path := `path`
	cg := `cg`
	req := &shared.DeleteConsumerGroupRequest{
		DestinationPath:   common.StringPtr(path),
		ConsumerGroupName: common.StringPtr(cg),
	}

	s.mockMeta.On("DeleteConsumerGroup", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		delReq := args.Get(1).(*shared.DeleteConsumerGroupRequest)
		s.Equal(path, delReq.GetDestinationPath())
		s.Equal(cg, delReq.GetConsumerGroupName())
	})
	err := repliator.DeleteConsumerGroup(nil, req)
	s.NoError(err)
	s.mockMeta.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestDeleteRemoteCgFailed() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	path := `path`
	cg := `cg`
	req := &shared.DeleteConsumerGroupRequest{
		DestinationPath:   common.StringPtr(path),
		ConsumerGroupName: common.StringPtr(cg),
	}

	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("DeleteConsumerGroup", mock.Anything, mock.Anything).Return(&shared.InternalServiceError{Message: "test2"})
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	err := repliator.deleteCgRemoteCall(`zone1`, repliator.logger, req)
	s.Error(err)
	s.mockReplicatorClientFactory.AssertExpectations(s.T())
	mockReplicator.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestDeleteRemoteCgSuccess() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	path := `path`
	cg := `cg`
	req := &shared.DeleteConsumerGroupRequest{
		DestinationPath:   common.StringPtr(path),
		ConsumerGroupName: common.StringPtr(cg),
	}

	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("DeleteConsumerGroup", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.DeleteConsumerGroupRequest)
		s.Equal(path, req.GetDestinationPath())
		s.Equal(cg, req.GetConsumerGroupName())
	})
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	err := repliator.deleteCgRemoteCall(`zone1`, repliator.logger, req)
	s.NoError(err)
	s.mockReplicatorClientFactory.AssertExpectations(s.T())
	mockReplicator.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestCreateExtentSuccess() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	destUUID := uuid.New()
	extentUUID := uuid.New()
	req := &shared.CreateExtentRequest{
		Extent: &shared.Extent{
			DestinationUUID: common.StringPtr(destUUID),
			ExtentUUID:      common.StringPtr(extentUUID),
		},
	}

	s.mockControllerClient.On("CreateRemoteZoneExtent", mock.Anything, mock.Anything).Return(
		&shared.CreateExtentResult_{}, nil,
	).Run(func(args mock.Arguments) {
		createReq := args.Get(1).(*shared.CreateExtentRequest)
		s.True(createReq.IsSetExtent())
		s.Equal(req.GetExtent().GetDestinationUUID(), createReq.GetExtent().GetDestinationUUID())
		s.Equal(req.GetExtent().GetExtentUUID(), createReq.GetExtent().GetExtentUUID())
	})
	res, err := repliator.CreateExtent(nil, req)
	s.NoError(err)
	s.NotNil(res)
	s.mockMeta.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestCreateRemoteExtentBadRequests() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	destUUID := uuid.New()
	extentUUID := uuid.New()
	originZone := `zone1`

	err := repliator.CreateRemoteExtent(nil, shared.NewCreateExtentRequest())
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)

	// No origin zone
	err = repliator.CreateRemoteExtent(nil, &shared.CreateExtentRequest{
		Extent: &shared.Extent{
			ExtentUUID:      common.StringPtr(extentUUID),
			DestinationUUID: common.StringPtr(destUUID),
		},
	})
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)

	singleZoneDest := &shared.DestinationDescription{
		IsMultiZone: common.BoolPtr(false),
	}
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(singleZoneDest, nil)
	err = repliator.CreateRemoteExtent(nil, &shared.CreateExtentRequest{
		Extent: &shared.Extent{
			ExtentUUID:      common.StringPtr(extentUUID),
			DestinationUUID: common.StringPtr(destUUID),
			OriginZone:      common.StringPtr(originZone),
		},
	})
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)

	noConfigZoneDest := &shared.DestinationDescription{
		IsMultiZone: common.BoolPtr(true),
		ZoneConfigs: []*shared.DestinationZoneConfig{},
	}
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(noConfigZoneDest, nil)
	err = repliator.CreateRemoteExtent(nil, &shared.CreateExtentRequest{
		Extent: &shared.Extent{
			ExtentUUID:      common.StringPtr(extentUUID),
			DestinationUUID: common.StringPtr(destUUID),
			OriginZone:      common.StringPtr(originZone),
		},
	})
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)
}

func (s *ReplicatorSuite) TestCreateRemoteExtentSuccess() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	destUUID := uuid.New()
	extentUUID := uuid.New()
	originZone := `zone1`
	remoteZone := `zone2`

	dest := &shared.DestinationDescription{
		IsMultiZone: common.BoolPtr(true),
		ZoneConfigs: []*shared.DestinationZoneConfig{
			{
				Zone:         common.StringPtr(remoteZone),
				AllowConsume: common.BoolPtr(true),
			},
		},
	}
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(dest, nil)

	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("CreateExtent", mock.Anything, mock.Anything).Return(nil, nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.CreateExtentRequest)
		s.True(req.IsSetExtent())
		s.Equal(destUUID, req.GetExtent().GetDestinationUUID())
		s.Equal(extentUUID, req.GetExtent().GetExtentUUID())
		s.Equal(originZone, req.GetExtent().GetOriginZone())
	})
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	err := repliator.createExtentRemoteCall(remoteZone, repliator.logger, &shared.CreateExtentRequest{
		Extent: &shared.Extent{
			ExtentUUID:      common.StringPtr(extentUUID),
			DestinationUUID: common.StringPtr(destUUID),
			OriginZone:      common.StringPtr(originZone),
		},
	})
	s.NoError(err)
	s.mockReplicatorClientFactory.AssertExpectations(s.T())
	mockReplicator.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestCreateRemoteExtentFailure() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	destUUID := uuid.New()
	extentUUID := uuid.New()
	originZone := `zone1`
	remoteZone := `zone2`

	dest := &shared.DestinationDescription{
		IsMultiZone: common.BoolPtr(true),
		ZoneConfigs: []*shared.DestinationZoneConfig{
			{
				Zone:         common.StringPtr(remoteZone),
				AllowConsume: common.BoolPtr(true),
			},
		},
	}
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(dest, nil)

	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("CreateExtent", mock.Anything, mock.Anything).Return(nil, &shared.BadRequestError{Message: "test2"})
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	err := repliator.createExtentRemoteCall(remoteZone, repliator.logger, &shared.CreateExtentRequest{
		Extent: &shared.Extent{
			ExtentUUID:      common.StringPtr(extentUUID),
			DestinationUUID: common.StringPtr(destUUID),
			OriginZone:      common.StringPtr(originZone),
		},
	})
	s.Error(err)
	s.mockReplicatorClientFactory.AssertExpectations(s.T())
	mockReplicator.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestCreateCgExtentSuccess() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	destUUID := uuid.New()
	extentUUID := uuid.New()
	cgUUID := uuid.New()
	req := &shared.CreateConsumerGroupExtentRequest{
		DestinationUUID:   common.StringPtr(destUUID),
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		ExtentUUID:        common.StringPtr(extentUUID),
	}

	s.mockControllerClient.On("CreateRemoteZoneConsumerGroupExtent", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		createReq := args.Get(1).(*shared.CreateConsumerGroupExtentRequest)
		s.Equal(req.GetDestinationUUID(), createReq.GetDestinationUUID())
		s.Equal(req.GetExtentUUID(), createReq.GetExtentUUID())
		s.Equal(req.GetConsumerGroupUUID(), createReq.GetConsumerGroupUUID())
	})
	err := repliator.CreateConsumerGroupExtent(nil, req)
	s.NoError(err)
	s.mockMeta.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestCreateRemoteConsumerGroupExtentBadRequests() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	destUUID := uuid.New()
	extentUUID := uuid.New()
	cgUUID := uuid.New()

	err := repliator.CreateRemoteConsumerGroupExtent(nil, shared.NewCreateConsumerGroupExtentRequest())
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)

	// No cg uuid
	err = repliator.CreateRemoteConsumerGroupExtent(nil, &shared.CreateConsumerGroupExtentRequest{
		ExtentUUID:      common.StringPtr(extentUUID),
		DestinationUUID: common.StringPtr(destUUID),
	})
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)

	singleZoneCg := &shared.ConsumerGroupDescription{
		IsMultiZone: common.BoolPtr(false),
	}
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(singleZoneCg, nil)
	err = repliator.CreateRemoteConsumerGroupExtent(nil, &shared.CreateConsumerGroupExtentRequest{
		ExtentUUID:        common.StringPtr(extentUUID),
		DestinationUUID:   common.StringPtr(destUUID),
		ConsumerGroupUUID: common.StringPtr(cgUUID),
	})
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)

	noConfigZoneCg := &shared.ConsumerGroupDescription{
		IsMultiZone: common.BoolPtr(true),
		ZoneConfigs: []*shared.ConsumerGroupZoneConfig{},
	}
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(noConfigZoneCg, nil)
	err = repliator.CreateRemoteConsumerGroupExtent(nil, &shared.CreateConsumerGroupExtentRequest{
		ExtentUUID:        common.StringPtr(extentUUID),
		DestinationUUID:   common.StringPtr(destUUID),
		ConsumerGroupUUID: common.StringPtr(cgUUID),
	})
	s.Error(err)
	assert.IsType(s.T(), &shared.BadRequestError{}, err)
}

func (s *ReplicatorSuite) TestCreateRemoteConsumerGroupExtentSuccess() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	destUUID := uuid.New()
	extentUUID := uuid.New()
	cgUUID := uuid.New()
	remoteZone := `zone2`

	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("CreateConsumerGroupExtent", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.CreateConsumerGroupExtentRequest)
		s.Equal(destUUID, req.GetDestinationUUID())
		s.Equal(extentUUID, req.GetExtentUUID())
		s.Equal(cgUUID, req.GetConsumerGroupUUID())
	})
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	err := repliator.createCgExtentRemoteCall(remoteZone, repliator.logger, &shared.CreateConsumerGroupExtentRequest{
		ExtentUUID:        common.StringPtr(extentUUID),
		DestinationUUID:   common.StringPtr(destUUID),
		ConsumerGroupUUID: common.StringPtr(cgUUID),
	})
	s.NoError(err)
	s.mockReplicatorClientFactory.AssertExpectations(s.T())
	mockReplicator.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestCreateRemoteConsumerGroupExtentFailure() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	destUUID := uuid.New()
	extentUUID := uuid.New()
	cgUUID := uuid.New()
	remoteZone := `zone2`

	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("CreateConsumerGroupExtent", mock.Anything, mock.Anything).Return(&shared.BadRequestError{Message: "test2"})
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	err := repliator.createCgExtentRemoteCall(remoteZone, repliator.logger, &shared.CreateConsumerGroupExtentRequest{
		ExtentUUID:        common.StringPtr(extentUUID),
		DestinationUUID:   common.StringPtr(destUUID),
		ConsumerGroupUUID: common.StringPtr(cgUUID),
	})
	s.Error(err)
	s.mockReplicatorClientFactory.AssertExpectations(s.T())
	mockReplicator.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestSetAckOffset() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	cgUUID := uuid.New()
	extentUUID := uuid.New()
	storeUUID := []string{uuid.New(), uuid.New(), uuid.New()}
	ackLevel := int64(20)
	req := &shared.SetAckOffsetRequest{
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		ExtentUUID:        common.StringPtr(extentUUID),
		AckLevelAddress:   common.Int64Ptr(ackLevel),
	}

	s.mockMeta.On("ReadConsumerGroupExtent", mock.Anything, mock.Anything).Return(&metadata.ReadConsumerGroupExtentResult_{
		Extent: &shared.ConsumerGroupExtent{
			StoreUUIDs: storeUUID,
		},
	}, nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*metadata.ReadConsumerGroupExtentRequest)
		s.Equal(extentUUID, req.GetExtentUUID())
		s.Equal(cgUUID, req.GetConsumerGroupUUID())
	})
	s.mockMeta.On("SetAckOffset", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.SetAckOffsetRequest)
		s.Equal(extentUUID, req.GetExtentUUID())
		s.Equal(ackLevel, req.GetAckLevelAddress())
	})
	err := repliator.SetAckOffset(nil, req)
	s.NoError(err)
	s.mockMeta.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestSetAckOffsetFailure_ReadExtentFail() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	cgUUID := uuid.New()
	extentUUID := uuid.New()
	ackLevel := int64(20)
	req := &shared.SetAckOffsetRequest{
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		ExtentUUID:        common.StringPtr(extentUUID),
		AckLevelAddress:   common.Int64Ptr(ackLevel),
	}

	s.mockMeta.On("ReadConsumerGroupExtent", mock.Anything, mock.Anything).Return(nil, &shared.InternalServiceError{Message: "test2"}).Run(func(args mock.Arguments) {
		req := args.Get(1).(*metadata.ReadConsumerGroupExtentRequest)
		s.Equal(extentUUID, req.GetExtentUUID())
		s.Equal(cgUUID, req.GetConsumerGroupUUID())
	})
	err := repliator.SetAckOffset(nil, req)
	s.Error(err)
	s.mockMeta.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestRemoteSetAckOffsetFailed() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	extentUUID := uuid.New()
	ackLevel := int64(20)
	req := &shared.SetAckOffsetRequest{
		ExtentUUID:      common.StringPtr(extentUUID),
		AckLevelAddress: common.Int64Ptr(ackLevel),
	}

	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("SetAckOffset", mock.Anything, mock.Anything).Return(&shared.InternalServiceError{Message: "test2"})
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	err := repliator.setAckOffsetRemoteCall(`zone1`, repliator.logger, req)
	s.Error(err)
	s.mockReplicatorClientFactory.AssertExpectations(s.T())
	mockReplicator.AssertExpectations(s.T())
}

func (s *ReplicatorSuite) TestRemoteSetAckOffsetSuccess() {
	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	extentUUID := uuid.New()
	ackLevel := int64(20)
	req := &shared.SetAckOffsetRequest{
		ExtentUUID:      common.StringPtr(extentUUID),
		AckLevelAddress: common.Int64Ptr(ackLevel),
	}

	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("SetAckOffset", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.SetAckOffsetRequest)
		s.Equal(extentUUID, req.GetExtentUUID())
		s.Equal(ackLevel, req.GetAckLevelAddress())
	})
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	err := repliator.setAckOffsetRemoteCall(`zone1`, repliator.logger, req)
	s.NoError(err)
	s.mockReplicatorClientFactory.AssertExpectations(s.T())
	mockReplicator.AssertExpectations(s.T())
}

// local zone is missing one destination compared to remote. Expect to create the missing destination
func (s *ReplicatorSuite) TestDestMetadataReconcileLocalMissing() {
	localZone := `zone2`
	missingDestUUID := uuid.New()
	destCreated := &shared.DestinationDescription{
		DestinationUUID: common.StringPtr(missingDestUUID),
	}

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	// setup mock
	s.mockMeta.On("CreateDestinationUUID", mock.Anything, mock.Anything).Return(destCreated, nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.CreateDestinationUUIDRequest)
		s.True(req.IsSetRequest())
		s.Equal(missingDestUUID, req.GetDestinationUUID())
	})

	var localDests []*shared.DestinationDescription
	var remoteDests []*shared.DestinationDescription
	remoteDests = append(remoteDests, &shared.DestinationDescription{
		DestinationUUID: common.StringPtr(missingDestUUID),
	})
	reconciler.reconcileDest(localDests, remoteDests)
	s.mockMeta.AssertExpectations(s.T())
}

// both local and remote zone has no destination. Expect no creation request is generated
func (s *ReplicatorSuite) TestDestMetadataReconcileLocalAndRemoteEmpty() {
	localZone := `zone2`

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	var localDests []*shared.DestinationDescription
	var remoteDests []*shared.DestinationDescription
	reconciler.reconcileDest(localDests, remoteDests)
	s.mockMeta.AssertExpectations(s.T())
}

// local zone has more destination than remote zone. Expect no creation request is generated
func (s *ReplicatorSuite) TestDestMetadataReconcileRemoteMissing() {
	localZone := `zone2`
	missingDestUUID := uuid.New()

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	var localDests []*shared.DestinationDescription
	localDests = append(localDests, &shared.DestinationDescription{
		DestinationUUID: common.StringPtr(missingDestUUID),
	})
	var remoteDests []*shared.DestinationDescription
	reconciler.reconcileDest(localDests, remoteDests)
	s.mockMeta.AssertExpectations(s.T())
}

// local zone is missing one destination compared to remote, but destination is deleted in remote zone. Expect no creation request is generated
func (s *ReplicatorSuite) TestDestMetadataReconcileLocalMissingRemoteDeleted() {
	localZone := `zone2`
	missingDestUUID := uuid.New()

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	var localDests []*shared.DestinationDescription
	var remoteDests []*shared.DestinationDescription
	remoteDests = append(remoteDests, &shared.DestinationDescription{
		DestinationUUID: common.StringPtr(missingDestUUID),
		Status:          common.InternalDestinationStatusPtr(shared.DestinationStatus_DELETING),
	})
	reconciler.reconcileDest(localDests, remoteDests)
	s.mockMeta.AssertExpectations(s.T())
}

// destination is deleted in remote zone. Expect a delete request is generated
func (s *ReplicatorSuite) TestDestMetadataReconcileRemoteDeleted() {
	localZone := `zone2`
	destUUID := uuid.New()
	destPath := `path`

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	// setup mock
	s.mockMeta.On("DeleteDestination", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.DeleteDestinationRequest)
		s.Equal(destPath, req.GetPath())
	})

	var localDests []*shared.DestinationDescription
	localDests = append(localDests, &shared.DestinationDescription{
		DestinationUUID: common.StringPtr(destUUID),
		Path:            common.StringPtr(destPath),
	})
	var remoteDests []*shared.DestinationDescription
	remoteDests = append(remoteDests, &shared.DestinationDescription{
		DestinationUUID: common.StringPtr(destUUID),
		Path:            common.StringPtr(destPath),
		Status:          common.InternalDestinationStatusPtr(shared.DestinationStatus_DELETING),
	})
	reconciler.reconcileDest(localDests, remoteDests)
	s.mockMeta.AssertExpectations(s.T())
}

// destination is deleted in both local and remote zones. Expect no deletion request is generated
func (s *ReplicatorSuite) TestDestMetadataReconcileRemoteLocalDeleted() {
	localZone := `zone2`
	destUUID := uuid.New()
	destPath := `path`

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	var localDests []*shared.DestinationDescription
	localDests = append(localDests, &shared.DestinationDescription{
		DestinationUUID: common.StringPtr(destUUID),
		Path:            common.StringPtr(destPath),
		Status:          common.InternalDestinationStatusPtr(shared.DestinationStatus_DELETED),
	})
	var remoteDests []*shared.DestinationDescription
	remoteDests = append(remoteDests, &shared.DestinationDescription{
		DestinationUUID: common.StringPtr(destUUID),
		Path:            common.StringPtr(destPath),
		Status:          common.InternalDestinationStatusPtr(shared.DestinationStatus_DELETING),
	})
	reconciler.reconcileDest(localDests, remoteDests)
	s.mockMeta.AssertExpectations(s.T())
}

// destination is updated in remote zone. Expect a update request is generated
func (s *ReplicatorSuite) TestDestMetadataReconcileRemoteUpdate() {
	localZone := `zone2`
	destUUID := uuid.New()
	destPath := `path`
	ownerRemote := `owner1`
	ownerLocal := `owner2`

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	// setup mock
	s.mockMeta.On("UpdateDestination", mock.Anything, mock.Anything).Return(shared.NewDestinationDescription(), nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.UpdateDestinationRequest)
		s.Equal(destUUID, req.GetDestinationUUID())
		s.Equal(ownerRemote, req.GetOwnerEmail())
	})

	var localDests []*shared.DestinationDescription
	localDests = append(localDests, &shared.DestinationDescription{
		DestinationUUID: common.StringPtr(destUUID),
		Path:            common.StringPtr(destPath),
		OwnerEmail:      common.StringPtr(ownerLocal),
	})
	var remoteDests []*shared.DestinationDescription
	remoteDests = append(remoteDests, &shared.DestinationDescription{
		DestinationUUID: common.StringPtr(destUUID),
		Path:            common.StringPtr(destPath),
		OwnerEmail:      common.StringPtr(ownerRemote),
	})
	reconciler.reconcileDest(localDests, remoteDests)
	s.mockMeta.AssertExpectations(s.T())
}

// destination zone config is updated in remote zone. Expect a update request is generated
func (s *ReplicatorSuite) TestDestMetadataReconcileRemoteUpdateZoneConfig() {
	localZone := `zone2`
	destUUID := uuid.New()
	destPath := `path`
	var remoteZoneConfigs []*shared.DestinationZoneConfig
	var localZoneConfigs []*shared.DestinationZoneConfig
	for _, zone := range []string{`zone1`, `zone2`} {
		remoteZoneConfigs = append(remoteZoneConfigs, &shared.DestinationZoneConfig{
			Zone:         common.StringPtr(zone),
			AllowPublish: common.BoolPtr(true),
			AllowConsume: common.BoolPtr(true),
		})
		localZoneConfigs = append(localZoneConfigs, &shared.DestinationZoneConfig{
			Zone:         common.StringPtr(zone),
			AllowPublish: common.BoolPtr(false),
			AllowConsume: common.BoolPtr(false),
		})
	}

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	// setup mock
	s.mockMeta.On("UpdateDestination", mock.Anything, mock.Anything).Return(shared.NewDestinationDescription(), nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.UpdateDestinationRequest)
		s.Equal(destUUID, req.GetDestinationUUID())
		s.True(common.AreDestinationZoneConfigsEqual(remoteZoneConfigs, req.GetZoneConfigs()))
	})

	var localDests []*shared.DestinationDescription
	localDests = append(localDests, &shared.DestinationDescription{
		DestinationUUID: common.StringPtr(destUUID),
		Path:            common.StringPtr(destPath),
		ZoneConfigs:     localZoneConfigs,
	})
	var remoteDests []*shared.DestinationDescription
	remoteDests = append(remoteDests, &shared.DestinationDescription{
		DestinationUUID: common.StringPtr(destUUID),
		Path:            common.StringPtr(destPath),
		ZoneConfigs:     remoteZoneConfigs,
	})
	reconciler.reconcileDest(localDests, remoteDests)
	s.mockMeta.AssertExpectations(s.T())
}

// local zone is missing one cg compared to remote. Expect to create the missing cg
func (s *ReplicatorSuite) TestCgMetadataReconcileLocalMissing() {
	localZone := `zone2`
	missingCgUUID := uuid.New()
	cgCreated := &shared.ConsumerGroupDescription{
		ConsumerGroupUUID: common.StringPtr(missingCgUUID),
	}

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	// setup mock
	s.mockMeta.On("CreateConsumerGroupUUID", mock.Anything, mock.Anything).Return(cgCreated, nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.CreateConsumerGroupUUIDRequest)
		s.True(req.IsSetRequest())
		s.Equal(missingCgUUID, req.GetConsumerGroupUUID())
	})

	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("ReadDestination", mock.Anything, mock.Anything).Return(&shared.DestinationDescription{Path: common.StringPtr(`dest`)}, nil)
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	var localCgs []*shared.ConsumerGroupDescription
	var remoteCgs []*shared.ConsumerGroupDescription
	remoteCgs = append(remoteCgs, &shared.ConsumerGroupDescription{
		ConsumerGroupUUID: common.StringPtr(missingCgUUID),
	})
	reconciler.reconcileCg(localCgs, remoteCgs)
	s.mockMeta.AssertExpectations(s.T())
}

// both local and remote zone has no ConsumerGroup. Expect no creation request is generated
func (s *ReplicatorSuite) TestCgMetadataReconcileLocalAndRemoteEmpty() {
	localZone := `zone2`

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	var localCgs []*shared.ConsumerGroupDescription
	var remoteCgs []*shared.ConsumerGroupDescription
	reconciler.reconcileCg(localCgs, remoteCgs)
	s.mockMeta.AssertExpectations(s.T())
}

// local zone has more ConsumerGroup than remote zone. Expect no creation request is generated
func (s *ReplicatorSuite) TestCgMetadataReconcileRemoteMissing() {
	localZone := `zone2`
	missingCgUUID := uuid.New()

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	var localCgs []*shared.ConsumerGroupDescription
	localCgs = append(localCgs, &shared.ConsumerGroupDescription{
		ConsumerGroupUUID: common.StringPtr(missingCgUUID),
	})
	var remoteCgs []*shared.ConsumerGroupDescription
	reconciler.reconcileCg(localCgs, remoteCgs)
	s.mockMeta.AssertExpectations(s.T())
}

// local zone is missing one ConsumerGroup compared to remote, but ConsumerGroup is deleted in remote zone. Expect no creation request is generated
func (s *ReplicatorSuite) TestCgMetadataReconcileLocalMissingRemoteDeleted() {
	localZone := `zone2`
	missingCgUUID := uuid.New()

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	var localCgs []*shared.ConsumerGroupDescription
	var remoteCgs []*shared.ConsumerGroupDescription
	remoteCgs = append(remoteCgs, &shared.ConsumerGroupDescription{
		ConsumerGroupUUID: common.StringPtr(missingCgUUID),
		Status:            common.InternalConsumerGroupStatusPtr(shared.ConsumerGroupStatus_DELETING),
	})
	reconciler.reconcileCg(localCgs, remoteCgs)
	s.mockMeta.AssertExpectations(s.T())
}

// ConsumerGroup is deleted in remote zone. Expect a delete request is generated
func (s *ReplicatorSuite) TestCgMetadataReconcileRemoteDeleted() {
	localZone := `zone2`
	cgUUID := uuid.New()
	destUUID := uuid.New()
	cgName := `cg`

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	// setup mock
	s.mockMeta.On("DeleteConsumerGroup", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.DeleteConsumerGroupRequest)
		s.Equal(cgName, req.GetConsumerGroupName())
		s.Equal(destUUID, req.GetDestinationUUID())
	})

	var localCgs []*shared.ConsumerGroupDescription
	localCgs = append(localCgs, &shared.ConsumerGroupDescription{
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		ConsumerGroupName: common.StringPtr(cgName),
		DestinationUUID:   common.StringPtr(destUUID),
	})
	var remoteCgs []*shared.ConsumerGroupDescription
	remoteCgs = append(remoteCgs, &shared.ConsumerGroupDescription{
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		ConsumerGroupName: common.StringPtr(cgName),
		DestinationUUID:   common.StringPtr(destUUID),
		Status:            common.InternalConsumerGroupStatusPtr(shared.ConsumerGroupStatus_DELETING),
	})
	reconciler.reconcileCg(localCgs, remoteCgs)
	s.mockMeta.AssertExpectations(s.T())
}

// ConsumerGroup is deleted in both local and remote zones. Expect no deletion request is generated
func (s *ReplicatorSuite) TestCgMetadataReconcileRemoteLocalDeleted() {
	localZone := `zone2`
	cgUUID := uuid.New()
	destUUID := uuid.New()
	cgName := `cg`

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	var localCgs []*shared.ConsumerGroupDescription
	localCgs = append(localCgs, &shared.ConsumerGroupDescription{
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		ConsumerGroupName: common.StringPtr(cgName),
		DestinationUUID:   common.StringPtr(destUUID),
		Status:            common.InternalConsumerGroupStatusPtr(shared.ConsumerGroupStatus_DELETING),
	})
	var remoteCgs []*shared.ConsumerGroupDescription
	remoteCgs = append(remoteCgs, &shared.ConsumerGroupDescription{
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		ConsumerGroupName: common.StringPtr(cgName),
		DestinationUUID:   common.StringPtr(destUUID),
		Status:            common.InternalConsumerGroupStatusPtr(shared.ConsumerGroupStatus_DELETING),
	})
	reconciler.reconcileCg(localCgs, remoteCgs)
	s.mockMeta.AssertExpectations(s.T())
}

// cg is updated in remote zone. Expect a update request is generated
func (s *ReplicatorSuite) TestCgMetadataReconcileRemoteUpdate() {
	localZone := `zone2`
	cgUUID := uuid.New()
	destUUID := uuid.New()
	destPath := `dest`
	cgName := `cg`
	updatedActiveZone := `z1`
	originalActiveZone := `z2`

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("ReadDestination", mock.Anything, mock.Anything).Return(&shared.DestinationDescription{Path: common.StringPtr(destPath)}, nil)
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	s.mockMeta.On("UpdateConsumerGroup", mock.Anything, mock.Anything).Return(shared.NewConsumerGroupDescription(), nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.UpdateConsumerGroupRequest)
		s.Equal(destPath, req.GetDestinationPath())
		s.Equal(cgName, req.GetConsumerGroupName())
		s.Equal(updatedActiveZone, req.GetActiveZone())
	})

	var localCgs []*shared.ConsumerGroupDescription
	localCgs = append(localCgs, &shared.ConsumerGroupDescription{
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		ConsumerGroupName: common.StringPtr(cgName),
		DestinationUUID:   common.StringPtr(destUUID),
		ActiveZone:        common.StringPtr(originalActiveZone),
	})
	var remoteCgs []*shared.ConsumerGroupDescription
	remoteCgs = append(remoteCgs, &shared.ConsumerGroupDescription{
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		ConsumerGroupName: common.StringPtr(cgName),
		DestinationUUID:   common.StringPtr(destUUID),
		ActiveZone:        common.StringPtr(updatedActiveZone),
	})
	reconciler.reconcileCg(localCgs, remoteCgs)
	s.mockMeta.AssertExpectations(s.T())
}

// cg's zone config is updated in remote zone. Expect a update request is generated
func (s *ReplicatorSuite) TestCgMetadataReconcileRemoteUpdateZoneConfig() {
	localZone := `zone2`
	cgUUID := uuid.New()
	destUUID := uuid.New()
	destPath := `dest`
	cgName := `cg`
	var remoteZoneConfigs []*shared.ConsumerGroupZoneConfig
	var localZoneConfigs []*shared.ConsumerGroupZoneConfig
	for _, zone := range []string{`zone1`, `zone2`} {
		remoteZoneConfigs = append(remoteZoneConfigs, &shared.ConsumerGroupZoneConfig{
			Zone:    common.StringPtr(zone),
			Visible: common.BoolPtr(true),
		})
		localZoneConfigs = append(localZoneConfigs, &shared.ConsumerGroupZoneConfig{
			Zone:    common.StringPtr(zone),
			Visible: common.BoolPtr(false),
		})
	}

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	// setup mock
	mockReplicator := new(mockreplicator.MockTChanReplicator)
	mockReplicator.On("ReadDestination", mock.Anything, mock.Anything).Return(&shared.DestinationDescription{Path: common.StringPtr(destPath)}, nil)
	s.mockReplicatorClientFactory.On("GetReplicatorClient", mock.Anything).Return(mockReplicator, nil)

	s.mockMeta.On("UpdateConsumerGroup", mock.Anything, mock.Anything).Return(shared.NewConsumerGroupDescription(), nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.UpdateConsumerGroupRequest)
		s.Equal(destPath, req.GetDestinationPath())
		s.Equal(cgName, req.GetConsumerGroupName())
		s.True(common.AreCgZoneConfigsEqual(remoteZoneConfigs, req.GetZoneConfigs()))
	})

	var localCgs []*shared.ConsumerGroupDescription
	localCgs = append(localCgs, &shared.ConsumerGroupDescription{
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		ConsumerGroupName: common.StringPtr(cgName),
		DestinationUUID:   common.StringPtr(destUUID),
		ZoneConfigs:       localZoneConfigs,
	})
	var remoteCgs []*shared.ConsumerGroupDescription
	remoteCgs = append(remoteCgs, &shared.ConsumerGroupDescription{
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		ConsumerGroupName: common.StringPtr(cgName),
		DestinationUUID:   common.StringPtr(destUUID),
		ZoneConfigs:       remoteZoneConfigs,
	})
	reconciler.reconcileCg(localCgs, remoteCgs)
	s.mockMeta.AssertExpectations(s.T())
}

// local zone is missing one destination extent compared to remote. Expect to create the missing destination extent
func (s *ReplicatorSuite) TestDestExtentMetadataReconcileLocalMissing() {
	localZone := `zone2`
	remoteZone := `zone1`
	missingExtent := uuid.New()
	dest := uuid.New()

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	// setup mock
	s.mockControllerClient.On("CreateRemoteZoneExtent", mock.Anything, mock.Anything).Return(shared.NewCreateExtentResult_(), nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.CreateExtentRequest)
		s.True(req.IsSetExtent())
		s.Equal(missingExtent, req.GetExtent().GetExtentUUID())
		s.Equal(dest, req.GetExtent().GetDestinationUUID())
		s.Equal(remoteZone, req.GetExtent().GetOriginZone())
	})

	localExtents := make(map[string]*shared.ExtentStats)
	remoteExtents := make(map[string]*shared.ExtentStats)
	remoteExtents[missingExtent] = &shared.ExtentStats{
		Status: common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN),
	}
	reconciler.reconcileDestExtent(dest, localExtents, remoteExtents, remoteZone)
	s.mockControllerClient.AssertExpectations(s.T())
}

// local zone is missing one dlq destination extent compared to remote. Expect to create the missing destination extent
func (s *ReplicatorSuite) TestDestExtentMetadataReconcileLocalMissingDLQExtent() {
	localZone := `zone2`
	remoteZone := `zone1`
	missingExtent := uuid.New()
	dest := uuid.New()
	cg := uuid.New()

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	// setup mock
	s.mockMeta.On("ReadConsumerGroupByUUID", mock.Anything, mock.Anything).Return(nil, nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.ReadConsumerGroupRequest)
		s.Equal(cg, req.GetConsumerGroupUUID())
	})
	s.mockControllerClient.On("CreateRemoteZoneExtent", mock.Anything, mock.Anything).Return(shared.NewCreateExtentResult_(), nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.CreateExtentRequest)
		s.True(req.IsSetExtent())
		s.Equal(missingExtent, req.GetExtent().GetExtentUUID())
		s.Equal(dest, req.GetExtent().GetDestinationUUID())
		s.Equal(remoteZone, req.GetExtent().GetOriginZone())
		s.Equal(cg, req.GetConsumerGroupVisibility())
	})

	localExtents := make(map[string]*shared.ExtentStats)
	remoteExtents := make(map[string]*shared.ExtentStats)
	remoteExtents[missingExtent] = &shared.ExtentStats{
		Status:                  common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN),
		ConsumerGroupVisibility: common.StringPtr(cg),
	}
	reconciler.reconcileDestExtent(dest, localExtents, remoteExtents, remoteZone)
	s.mockMeta.AssertExpectations(s.T())
	s.mockControllerClient.AssertExpectations(s.T())
}

// local zone is missing one dlq destination extent compared to remote, but that dlq extent is only for remote cg. Expect to no creation request
func (s *ReplicatorSuite) TestDestExtentMetadataReconcileLocalMissingDLQExtentRemoteOnlyCg() {
	localZone := `zone2`
	remoteZone := `zone1`
	missingExtent := uuid.New()
	dest := uuid.New()
	cg := uuid.New()

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	// setup mock
	s.mockMeta.On("ReadConsumerGroupByUUID", mock.Anything, mock.Anything).Return(nil, &shared.EntityNotExistsError{Message: `doesn't exist`}).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.ReadConsumerGroupRequest)
		s.Equal(cg, req.GetConsumerGroupUUID())
	})

	localExtents := make(map[string]*shared.ExtentStats)
	remoteExtents := make(map[string]*shared.ExtentStats)
	remoteExtents[missingExtent] = &shared.ExtentStats{
		Status:                  common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN),
		ConsumerGroupVisibility: common.StringPtr(cg),
	}
	reconciler.reconcileDestExtent(dest, localExtents, remoteExtents, remoteZone)
	s.mockMeta.AssertExpectations(s.T())
	s.mockControllerClient.AssertExpectations(s.T())
}

// local zone is missing one destination extent compared to remote but remote is consumed. Expect no action
func (s *ReplicatorSuite) TestDestExtentMetadataReconcileLocalMissingConsumedExtent() {
	localZone := `zone2`
	remoteZone := `zone1`
	missingExtent := uuid.New()
	dest := uuid.New()

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	localExtents := make(map[string]*shared.ExtentStats)
	remoteExtents := make(map[string]*shared.ExtentStats)
	remoteExtents[missingExtent] = &shared.ExtentStats{
		Status: common.MetadataExtentStatusPtr(shared.ExtentStatus_CONSUMED),
	}
	reconciler.reconcileDestExtent(dest, localExtents, remoteExtents, remoteZone)
	s.mockControllerClient.AssertExpectations(s.T())
}

// both local and remote zone has no destination extent. Expect no creation request is generated
func (s *ReplicatorSuite) TestDestExtentMetadataReconcileLocalAndRemoteEmpty() {
	localZone := `zone2`
	remoteZone := `zone1`
	dest := uuid.New()

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	localExtents := make(map[string]*shared.ExtentStats)
	remoteExtents := make(map[string]*shared.ExtentStats)
	reconciler.reconcileDestExtent(dest, localExtents, remoteExtents, remoteZone)
	s.mockControllerClient.AssertExpectations(s.T())
}

// Extent sealed in remote but not in local. Expect a update request to update the status
func (s *ReplicatorSuite) TestDestExtentMetadataReconcileInconsistentStatus() {
	localZone := `zone2`
	remoteZone := `zone1`
	extent := uuid.New()
	dest := uuid.New()

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	// setup mock
	s.mockMeta.On("UpdateExtentStats", mock.Anything, mock.Anything).Return(nil, nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*metadata.UpdateExtentStatsRequest)
		s.Equal(dest, req.GetDestinationUUID())
		s.Equal(extent, req.GetExtentUUID())
		s.Equal(shared.ExtentStatus_SEALED, req.GetStatus())
	})

	localExtents := make(map[string]*shared.ExtentStats)
	remoteExtents := make(map[string]*shared.ExtentStats)
	remoteExtents[extent] = &shared.ExtentStats{
		Status: common.MetadataExtentStatusPtr(shared.ExtentStatus_SEALED),
	}
	localExtents[extent] = &shared.ExtentStats{
		Status: common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN),
	}
	reconciler.reconcileDestExtent(dest, localExtents, remoteExtents, remoteZone)
	s.mockMeta.AssertExpectations(s.T())
}

// Extent deleted in remote but not in local. Expect a update request to update the status, and a seal extent call per store
func (s *ReplicatorSuite) TestDestExtentMetadataReconcileRemoteDeletedLocalNot() {
	localZone := `zone2`
	remoteZone := `zone1`
	extent := uuid.New()
	dest := uuid.New()
	stores := []string{uuid.New(), uuid.New()}

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	// setup mock
	s.mockMeta.On("UpdateExtentStats", mock.Anything, mock.Anything).Return(nil, nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*metadata.UpdateExtentStatsRequest)
		s.Equal(dest, req.GetDestinationUUID())
		s.Equal(extent, req.GetExtentUUID())
		s.Equal(shared.ExtentStatus_SEALED, req.GetStatus())
	})
	s.mockMeta.On("ReadExtentStats", mock.Anything, mock.Anything).Return(&metadata.ReadExtentStatsResult_{
		ExtentStats: &shared.ExtentStats{
			Extent: &shared.Extent{
				StoreUUIDs: stores,
			},
		},
	}, nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*metadata.ReadExtentStatsRequest)
		s.Equal(dest, req.GetDestinationUUID())
		s.Equal(extent, req.GetExtentUUID())
	})
	s.mockStoreClient.On("SealExtent", mock.Anything, mock.Anything).Return(nil).Times(len(stores)).Run(func(args mock.Arguments) {
		req := args.Get(1).(*store.SealExtentRequest)
		s.Equal(extent, req.GetExtentUUID())
	})

	localExtents := make(map[string]*shared.ExtentStats)
	remoteExtents := make(map[string]*shared.ExtentStats)
	remoteExtents[extent] = &shared.ExtentStats{
		Status: common.MetadataExtentStatusPtr(shared.ExtentStatus_DELETED),
	}
	localExtents[extent] = &shared.ExtentStats{
		Status: common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN),
	}
	reconciler.reconcileDestExtent(dest, localExtents, remoteExtents, remoteZone)
	s.mockMeta.AssertExpectations(s.T())
	s.mockStoreClient.AssertExpectations(s.T())
}

// Extent gone in remote but not in local.
func (s *ReplicatorSuite) TestDestExtentMetadataReconcileRemoteGoneLocalNot() {
	localZone := `zone2`
	remoteZone := `zone1`
	extent := uuid.New()
	dest := uuid.New()
	stores := []string{uuid.New(), uuid.New()}

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	// step1: found extent missing from remote. No action expected, only added to suspect list
	localExtents := make(map[string]*shared.ExtentStats)
	remoteExtents := make(map[string]*shared.ExtentStats)
	localExtents[extent] = &shared.ExtentStats{
		Status: common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN),
	}
	reconciler.reconcileDestExtent(dest, localExtents, remoteExtents, remoteZone)
	s.mockMeta.AssertExpectations(s.T())
	s.mockStoreClient.AssertExpectations(s.T())
	s.Equal(1, len(reconciler.suspectMissingExtents))

	// step2: override the missing time. Expect to seal in metadata and store
	reconciler.suspectMissingExtents[extent] = missingExtentInfo{
		destUUID:     reconciler.suspectMissingExtents[extent].destUUID,
		missingSince: time.Now().AddDate(0, -1, 0),
	}
	s.mockMeta.On("UpdateExtentStats", mock.Anything, mock.Anything).Return(nil, nil).Once().Run(func(args mock.Arguments) {
		req := args.Get(1).(*metadata.UpdateExtentStatsRequest)
		s.Equal(dest, req.GetDestinationUUID())
		s.Equal(extent, req.GetExtentUUID())
		s.Equal(shared.ExtentStatus_SEALED, req.GetStatus())
	})
	s.mockMeta.On("ReadExtentStats", mock.Anything, mock.Anything).Return(&metadata.ReadExtentStatsResult_{
		ExtentStats: &shared.ExtentStats{
			Extent: &shared.Extent{
				StoreUUIDs: stores,
			},
		},
	}, nil).Once().Run(func(args mock.Arguments) {
		req := args.Get(1).(*metadata.ReadExtentStatsRequest)
		s.Equal(dest, req.GetDestinationUUID())
		s.Equal(extent, req.GetExtentUUID())
	})
	s.mockStoreClient.On("SealExtent", mock.Anything, mock.Anything).Return(nil).Times(len(stores)).Run(func(args mock.Arguments) {
		req := args.Get(1).(*store.SealExtentRequest)
		s.Equal(extent, req.GetExtentUUID())
	})
	reconciler.reconcileDestExtent(dest, localExtents, remoteExtents, remoteZone)
	s.mockMeta.AssertExpectations(s.T())
	s.mockStoreClient.AssertExpectations(s.T())
	s.Equal(1, len(reconciler.suspectMissingExtents))

	// step3: run reconcile again, expect the same as step2
	s.mockMeta.On("UpdateExtentStats", mock.Anything, mock.Anything).Return(nil, nil).Once().Run(func(args mock.Arguments) {
		req := args.Get(1).(*metadata.UpdateExtentStatsRequest)
		s.Equal(dest, req.GetDestinationUUID())
		s.Equal(extent, req.GetExtentUUID())
		s.Equal(shared.ExtentStatus_SEALED, req.GetStatus())
	})
	s.mockMeta.On("ReadExtentStats", mock.Anything, mock.Anything).Return(&metadata.ReadExtentStatsResult_{
		ExtentStats: &shared.ExtentStats{
			Extent: &shared.Extent{
				StoreUUIDs: stores,
			},
		},
	}, nil).Once().Run(func(args mock.Arguments) {
		req := args.Get(1).(*metadata.ReadExtentStatsRequest)
		s.Equal(dest, req.GetDestinationUUID())
		s.Equal(extent, req.GetExtentUUID())
	})
	s.mockStoreClient.On("SealExtent", mock.Anything, mock.Anything).Return(nil).Times(len(stores)).Run(func(args mock.Arguments) {
		req := args.Get(1).(*store.SealExtentRequest)
		s.Equal(extent, req.GetExtentUUID())
	})
	reconciler.reconcileDestExtent(dest, localExtents, remoteExtents, remoteZone)
	s.mockMeta.AssertExpectations(s.T())
	s.mockStoreClient.AssertExpectations(s.T())
	s.Equal(1, len(reconciler.suspectMissingExtents))

	// step4: add the extent in remote zone, expect no action, and extent removed from suspect list
	remoteExtents[extent] = &shared.ExtentStats{
		Status: common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN),
	}
	reconciler.reconcileDestExtent(dest, localExtents, remoteExtents, remoteZone)
	s.mockMeta.AssertExpectations(s.T())
	s.mockStoreClient.AssertExpectations(s.T())
	s.Equal(0, len(reconciler.suspectMissingExtents))
}

// local zone is missing one consumer group extent compared to remote. Expect to create the missing consumer group extent
func (s *ReplicatorSuite) TestCgExtentMetadataReconcileLocalMissing() {
	localZone := `zone2`
	remoteZone := `zone1`
	missingExtent := uuid.New()
	dest := uuid.New()
	cg := uuid.New()

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	// setup mock
	s.mockControllerClient.On("CreateRemoteZoneConsumerGroupExtent", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.CreateConsumerGroupExtentRequest)
		s.Equal(missingExtent, req.GetExtentUUID())
		s.Equal(dest, req.GetDestinationUUID())
		s.Equal(cg, req.GetConsumerGroupUUID())
	})

	localExtents := make(map[string]*shared.ConsumerGroupExtent)
	remoteExtents := make(map[string]*shared.ConsumerGroupExtent)
	remoteExtents[missingExtent] = &shared.ConsumerGroupExtent{
		ExtentUUID:        common.StringPtr(missingExtent),
		ConsumerGroupUUID: common.StringPtr(cg),
	}
	reconciler.reconcileCgExtent(dest, cg, localExtents, remoteExtents, remoteZone)
	s.mockControllerClient.AssertExpectations(s.T())
}

// local zone is missing one consumer group extent compared to remote but remote is consumed. Expect no action
func (s *ReplicatorSuite) TestCgExtentMetadataReconcileLocalMissingConsumedExtent() {
	localZone := `zone2`
	remoteZone := `zone1`
	missingExtent := uuid.New()
	dest := uuid.New()
	cg := uuid.New()

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	localExtents := make(map[string]*shared.ConsumerGroupExtent)
	remoteExtents := make(map[string]*shared.ConsumerGroupExtent)
	remoteExtents[missingExtent] = &shared.ConsumerGroupExtent{
		ExtentUUID:        common.StringPtr(missingExtent),
		ConsumerGroupUUID: common.StringPtr(cg),
		Status:            common.CheramiConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_CONSUMED),
	}
	reconciler.reconcileCgExtent(dest, cg, localExtents, remoteExtents, remoteZone)
	s.mockControllerClient.AssertExpectations(s.T())
}

// both local and remote zone has no consumer group extent. Expect no creation request is generated
func (s *ReplicatorSuite) TestCgExtentMetadataReconcileLocalAndRemoteEmpty() {
	localZone := `zone2`
	remoteZone := `zone1`
	dest := uuid.New()
	cg := uuid.New()

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	localExtents := make(map[string]*shared.ConsumerGroupExtent)
	remoteExtents := make(map[string]*shared.ConsumerGroupExtent)
	reconciler.reconcileCgExtent(dest, cg, localExtents, remoteExtents, remoteZone)
	s.mockControllerClient.AssertExpectations(s.T())
}

// Extent consumed in remote but not in local. Expect a request to update the status
func (s *ReplicatorSuite) TestCgExtentMetadataReconcileInconsistentStatus() {
	localZone := `zone2`
	remoteZone := `zone1`
	extent := uuid.New()
	dest := uuid.New()
	cg := uuid.New()

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	// setup mock
	s.mockMeta.On("UpdateConsumerGroupExtentStatus", mock.Anything, mock.Anything).Return(nil, nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.UpdateConsumerGroupExtentStatusRequest)
		s.Equal(cg, req.GetConsumerGroupUUID())
		s.Equal(extent, req.GetExtentUUID())
		s.Equal(shared.ConsumerGroupExtentStatus_CONSUMED, req.GetStatus())
	})

	localExtents := make(map[string]*shared.ConsumerGroupExtent)
	remoteExtents := make(map[string]*shared.ConsumerGroupExtent)
	remoteExtents[extent] = &shared.ConsumerGroupExtent{
		ExtentUUID:        common.StringPtr(extent),
		ConsumerGroupUUID: common.StringPtr(cg),
		Status:            common.CheramiConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_CONSUMED),
	}
	localExtents[extent] = &shared.ConsumerGroupExtent{
		ExtentUUID:        common.StringPtr(extent),
		ConsumerGroupUUID: common.StringPtr(cg),
		Status:            common.CheramiConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_OPEN),
	}
	reconciler.reconcileCgExtent(dest, cg, localExtents, remoteExtents, remoteZone)
	s.mockMeta.AssertExpectations(s.T())
}

// Remote ack level is less than local, expect no update
func (s *ReplicatorSuite) TestCgExtentMetadataReconcileAcklevelNoUpdate() {
	localZone := `zone2`
	remoteZone := `zone1`
	extent := uuid.New()
	dest := uuid.New()
	cg := uuid.New()

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	localExtents := make(map[string]*shared.ConsumerGroupExtent)
	remoteExtents := make(map[string]*shared.ConsumerGroupExtent)
	remoteExtents[extent] = &shared.ConsumerGroupExtent{
		ExtentUUID:        common.StringPtr(extent),
		ConsumerGroupUUID: common.StringPtr(cg),
		AckLevelOffset:    common.Int64Ptr(10),
	}
	localExtents[extent] = &shared.ConsumerGroupExtent{
		ExtentUUID:        common.StringPtr(extent),
		ConsumerGroupUUID: common.StringPtr(cg),
		AckLevelOffset:    common.Int64Ptr(20),
	}
	reconciler.reconcileCgExtent(dest, cg, localExtents, remoteExtents, remoteZone)
	s.mockMeta.AssertExpectations(s.T())
}

// Remote ack level is greater than local, expect a request to update ack level
func (s *ReplicatorSuite) TestCgExtentMetadataReconcileAcklevelUpdate() {
	localZone := `zone2`
	remoteZone := `zone1`
	extent := uuid.New()
	dest := uuid.New()
	cg := uuid.New()

	repliator, _ := NewReplicator("replicator-test", s.mockService, s.mockMeta, s.mockReplicatorClientFactory, s.cfg)
	reconciler, _ := NewMetadataReconciler(repliator.metaClient, repliator, localZone, repliator.logger, repliator.m3Client).(*metadataReconciler)

	// setup mock
	s.mockMeta.On("SetAckOffset", mock.Anything, mock.Anything).Return(nil, nil).Run(func(args mock.Arguments) {
		req := args.Get(1).(*shared.SetAckOffsetRequest)
		s.Equal(cg, req.GetConsumerGroupUUID())
		s.Equal(extent, req.GetExtentUUID())
		s.Equal(int64(20), req.GetAckLevelAddress())
	})

	localExtents := make(map[string]*shared.ConsumerGroupExtent)
	remoteExtents := make(map[string]*shared.ConsumerGroupExtent)
	remoteExtents[extent] = &shared.ConsumerGroupExtent{
		ExtentUUID:        common.StringPtr(extent),
		ConsumerGroupUUID: common.StringPtr(cg),
		AckLevelOffset:    common.Int64Ptr(20),
	}
	localExtents[extent] = &shared.ConsumerGroupExtent{
		ExtentUUID:        common.StringPtr(extent),
		ConsumerGroupUUID: common.StringPtr(cg),
		AckLevelOffset:    common.Int64Ptr(10),
	}
	reconciler.reconcileCgExtent(dest, cg, localExtents, remoteExtents, remoteZone)
	s.mockMeta.AssertExpectations(s.T())
}
