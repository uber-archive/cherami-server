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

package frontendhost

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
	mockcommon "github.com/uber/cherami-server/test/mocks/common"
	mockctrl "github.com/uber/cherami-server/test/mocks/controllerhost"
	mockmeta "github.com/uber/cherami-server/test/mocks/metadata"
	c "github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/shared"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
	"golang.org/x/net/context"
)

var logHasBeenSetup bool

type FrontendHostSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
	seqNum         int32
	cfg            configure.CommonAppConfig
	mockService    *common.MockService
	mockController *mockctrl.MockControllerHost
	mockMeta       *mockmeta.TChanMetadataService
}

func TestFrontendHostSuite(t *testing.T) {
	s := new(FrontendHostSuite)
	suite.Run(t, s)
}

func destCreateRequestToDesc(createRequest *c.CreateDestinationRequest) *shared.DestinationDescription {
	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr(createRequest.GetPath())
	destDesc.DestinationUUID = common.StringPtr(uuid.New())
	destDesc.Type = common.InternalDestinationTypePtr(shared.DestinationType(createRequest.GetType()))
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED)
	destDesc.ConsumedMessagesRetention = common.Int32Ptr(createRequest.GetConsumedMessagesRetention())
	destDesc.UnconsumedMessagesRetention = common.Int32Ptr(createRequest.GetUnconsumedMessagesRetention())
	destDesc.OwnerEmail = common.StringPtr(createRequest.GetOwnerEmail())
	destDesc.ChecksumOption = common.InternalChecksumOptionPtr(shared.ChecksumOption(createRequest.GetChecksumOption()))
	destDesc.KafkaCluster = common.StringPtr(createRequest.GetKafkaCluster())
	destDesc.KafkaTopics = createRequest.KafkaTopics
	destDesc.IsMultiZone = common.BoolPtr(createRequest.GetIsMultiZone())
	if createRequest.IsSetZoneConfigs() {
		destDesc.ZoneConfigs = make([]*shared.DestinationZoneConfig, 0, len(createRequest.GetZoneConfigs().GetConfigs()))
		for _, destZoneCfg := range createRequest.GetZoneConfigs().GetConfigs() {
			destDesc.ZoneConfigs = append(destDesc.ZoneConfigs, convertDestZoneConfigToInternal(destZoneCfg))
		}
	}
	return destDesc
}

func destUpdateRequestToDesc(updateRequest *c.UpdateDestinationRequest) *shared.DestinationDescription {
	destDesc := shared.NewDestinationDescription()
	destDesc.Path = common.StringPtr(updateRequest.GetPath())
	destDesc.DestinationUUID = common.StringPtr(uuid.New())
	destDesc.Status = common.InternalDestinationStatusPtr(shared.DestinationStatus(updateRequest.GetStatus()))
	destDesc.ConsumedMessagesRetention = common.Int32Ptr(updateRequest.GetConsumedMessagesRetention())
	destDesc.UnconsumedMessagesRetention = common.Int32Ptr(updateRequest.GetUnconsumedMessagesRetention())
	destDesc.OwnerEmail = common.StringPtr(updateRequest.GetOwnerEmail())
	destDesc.ChecksumOption = common.InternalChecksumOptionPtr(shared.ChecksumOption(updateRequest.GetChecksumOption()))
	return destDesc
}

func cgCreateRequestToDesc(createRequest *c.CreateConsumerGroupRequest) *shared.ConsumerGroupDescription {
	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(uuid.New())
	cgDesc.DestinationUUID = common.StringPtr(uuid.New())
	cgDesc.ConsumerGroupName = common.StringPtr(createRequest.GetConsumerGroupName())
	cgDesc.StartFrom = common.Int64Ptr(createRequest.GetStartFrom())
	cgDesc.Status = common.InternalConsumerGroupStatusPtr(shared.ConsumerGroupStatus_ENABLED)
	cgDesc.LockTimeoutSeconds = common.Int32Ptr(createRequest.GetLockTimeoutInSeconds())
	cgDesc.MaxDeliveryCount = common.Int32Ptr(createRequest.GetMaxDeliveryCount())
	cgDesc.SkipOlderMessagesSeconds = common.Int32Ptr(createRequest.GetSkipOlderMessagesInSeconds())
	cgDesc.DelaySeconds = common.Int32Ptr(createRequest.GetDelaySeconds())
	cgDesc.DeadLetterQueueDestinationUUID = common.StringPtr(uuid.New())
	cgDesc.OwnerEmail = common.StringPtr(createRequest.GetOwnerEmail())
	cgDesc.ConsumerGroupType = common.InternalConsumerGroupTypePtr(shared.ConsumerGroupType(createRequest.GetConsumerGroupType()))
	cgDesc.IsMultiZone = common.BoolPtr(createRequest.GetIsMultiZone())
	if createRequest.IsSetZoneConfigs() {
		cgDesc.ZoneConfigs = make([]*shared.ConsumerGroupZoneConfig, 0, len(createRequest.GetZoneConfigs().GetConfigs()))
		for _, cgZoneCfg := range createRequest.GetZoneConfigs().GetConfigs() {
			cgDesc.ZoneConfigs = append(cgDesc.ZoneConfigs, convertCGZoneConfigToInternal(cgZoneCfg))
		}
	}
	cgDesc.Options = createRequest.GetOptions()
	return cgDesc
}

func cgUpdateRequestToDesc(updateRequest *c.UpdateConsumerGroupRequest) *shared.ConsumerGroupDescription {
	cgDesc := shared.NewConsumerGroupDescription()
	cgDesc.ConsumerGroupUUID = common.StringPtr(uuid.New())
	cgDesc.DestinationUUID = common.StringPtr(uuid.New())
	cgDesc.ConsumerGroupName = common.StringPtr(updateRequest.GetConsumerGroupName())
	cgDesc.Status = common.InternalConsumerGroupStatusPtr(shared.ConsumerGroupStatus(updateRequest.GetStatus()))
	cgDesc.LockTimeoutSeconds = common.Int32Ptr(updateRequest.GetLockTimeoutInSeconds())
	cgDesc.MaxDeliveryCount = common.Int32Ptr(updateRequest.GetMaxDeliveryCount())
	cgDesc.SkipOlderMessagesSeconds = common.Int32Ptr(updateRequest.GetSkipOlderMessagesInSeconds())
	cgDesc.DelaySeconds = common.Int32Ptr(updateRequest.GetDelaySeconds())
	cgDesc.OwnerEmail = common.StringPtr(updateRequest.GetOwnerEmail())
	cgDesc.Options = updateRequest.GetOptions()
	return cgDesc
}

func (s *FrontendHostSuite) generateKey(prefix string) string {
	seq := atomic.AddInt32(&s.seqNum, 1)
	return prefix + "." + strconv.Itoa(int(seq))
}

// TODO: refactor the common setup once we start adding more tests
// as it makes sense
func (s *FrontendHostSuite) SetupCommonMock() {
	rpm := common.NewMockRingpopMonitor()
	rpm.Add(common.OutputServiceName, "99999999-9999-9999-9999-999999999999", "127.0.0.1")

	s.mockService = new(common.MockService)
	s.mockController = new(mockctrl.MockControllerHost)
	s.mockMeta = new(mockmeta.TChanMetadataService)

	mockClientFactory := new(mockcommon.MockClientFactory)
	mockClientFactory.On("GetControllerClient").Return(s.mockController, nil)

	s.mockService.On("GetConfig").Return(s.cfg.GetServiceConfig(common.FrontendServiceName))
	s.mockService.On("GetTChannel").Return(tchannel.NewChannel("test-frontend", nil))
	s.mockService.On("GetHostPort").Return("inputhost:port")
	s.mockService.On("GetHostUUID").Return("99999999-9999-9999-9999-999999999999")
	s.mockService.On("GetRingpopMonitor").Return(rpm)
	s.mockService.On("GetClientFactory").Return(mockClientFactory)
	s.mockService.On("GetMetricsReporter").Return(common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig()))
	s.mockService.On("GetDConfigClient").Return(dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.FrontendServiceName))
	s.mockService.On("GetAuthManager").Return(common.NewBypassAuthManager())

}

func (s *FrontendHostSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.cfg = common.SetupServerConfig(configure.NewCommonConfigure())
	s.SetupCommonMock()
	log.Infof("Testing has begun")
}

func (s *FrontendHostSuite) TearDownTest() {
	return
}

// utility routine to get thrift context
func utilGetThriftContext() (thrift.Context, context.CancelFunc) {
	return thrift.NewContext(10 * time.Minute)
}

// utility routine to get thrift context with path
func (s *FrontendHostSuite) utilGetContextAndFrontend() (frontendHost *Frontend, ctx thrift.Context) {
	appConfig := configure.NewCommonAppConfig()
	sCfgInput := configure.NewCommonServiceConfig()
	sCfgInput.SetWebsocketPort(123)
	sCfgOutput := configure.NewCommonServiceConfig()
	sCfgOutput.SetWebsocketPort(456)
	appConfig.SetServiceConfig(common.InputServiceName, sCfgInput)
	appConfig.SetServiceConfig(common.OutputServiceName, sCfgOutput)
	sName := "cherami-test-frontend"
	frontendHost, _ = NewFrontendHost(sName, s.mockService, s.mockMeta, appConfig)
	ctx, _ = utilGetThriftContext()
	return
}

// TestFrontendHostCreateDestinationRejectNil tests that a nil request fails with BadRequestError
func (s *FrontendHostSuite) TestFrontendHostCreateDestinationRejectNil() {
	frontendHost, ctx := s.utilGetContextAndFrontend()
	dst, err := frontendHost.CreateDestination(ctx, nil)
	s.Error(err)
	s.Nil(dst)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostCreateDestinationRejectBadPath tests that a bad destination path fails
func (s *FrontendHostSuite) TestFrontendHostCreateDestinationRejectBadPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()

	req := c.NewCreateDestinationRequest()
	req.Path = common.StringPtr("/fsdf8234*@#($*")
	req.ConsumedMessagesRetention = common.Int32Ptr(15)
	req.UnconsumedMessagesRetention = common.Int32Ptr(30)
	req.OwnerEmail = common.StringPtr("test@uber.com")
	req.Type = c.DestinationTypePtr(c.DestinationType_PLAIN)

	dst, err := frontendHost.CreateDestination(ctx, req)

	s.Error(err)
	s.Nil(dst)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostCreateDestination tests that a bad destination path fails
func (s *FrontendHostSuite) TestFrontendHostCreateDestination() {
	frontendHost, ctx := s.utilGetContextAndFrontend()

	path := s.generateKey("/foo/bar")
	req := c.NewCreateDestinationRequest()
	req.Path = common.StringPtr(path)
	req.ConsumedMessagesRetention = common.Int32Ptr(15)
	req.UnconsumedMessagesRetention = common.Int32Ptr(30)
	req.OwnerEmail = common.StringPtr("test@uber.com")
	req.Type = c.DestinationTypePtr(c.DestinationType_TIMER)
	req.IsMultiZone = common.BoolPtr(true)
	req.ZoneConfigs = &c.DestinationZoneConfigs{
		Configs: []*c.DestinationZoneConfig{
			{
				Zone:                   common.StringPtr("zone1"),
				AllowPublish:           common.BoolPtr(true),
				AllowConsume:           common.BoolPtr(true),
				AlwaysReplicateTo:      common.BoolPtr(true),
				RemoteExtentReplicaNum: common.Int32Ptr(3),
			},
		},
	}

	s.mockController.On("CreateDestination", mock.Anything, mock.Anything).Return(destCreateRequestToDesc(req), nil).Once().Run(func(args mock.Arguments) {
		createReq := args.Get(1).(*shared.CreateDestinationRequest)
		s.Equal(createReq.GetPath(), req.GetPath())
		s.Equal(createReq.GetConsumedMessagesRetention(), req.GetConsumedMessagesRetention())
		s.Equal(createReq.GetUnconsumedMessagesRetention(), req.GetUnconsumedMessagesRetention())
		s.Equal(createReq.GetType(), shared.DestinationType(req.GetType()))
		s.Equal(createReq.GetOwnerEmail(), req.GetOwnerEmail())
		s.Equal(createReq.GetIsMultiZone(), req.GetIsMultiZone())
		s.Equal(len(createReq.GetZoneConfigs()), len(req.GetZoneConfigs().GetConfigs()))
		s.Equal(createReq.GetZoneConfigs()[0].GetRemoteExtentReplicaNum(), req.GetZoneConfigs().GetConfigs()[0].GetRemoteExtentReplicaNum())
		s.Equal(createReq.GetKafkaCluster(), req.GetKafkaCluster())
		s.True(common.StringSetEqual(createReq.GetKafkaTopics(), req.GetKafkaTopics()))
	})

	dst, err := frontendHost.CreateDestination(ctx, req)
	s.NoError(err)
	s.NotNil(dst)

	if dst != nil {
		s.Equal(dst.GetPath(), req.GetPath())
		s.Equal(dst.GetConsumedMessagesRetention(), req.GetConsumedMessagesRetention())
		s.Equal(dst.GetUnconsumedMessagesRetention(), req.GetUnconsumedMessagesRetention())
		s.Equal(dst.GetType(), req.GetType())
		s.Equal(dst.GetOwnerEmail(), req.GetOwnerEmail())
		s.Equal(dst.GetIsMultiZone(), req.GetIsMultiZone())
		s.Equal(len(dst.ZoneConfigs.GetConfigs()), len(req.ZoneConfigs.GetConfigs()))
		s.Equal(dst.ZoneConfigs.GetConfigs()[0].GetRemoteExtentReplicaNum(), req.ZoneConfigs.GetConfigs()[0].GetRemoteExtentReplicaNum())
		s.Equal(dst.GetKafkaCluster(), req.GetKafkaCluster())
		s.True(common.StringSetEqual(dst.GetKafkaTopics(), req.GetKafkaTopics()))
	}
}

// TestFrontendHostCreateKafkaDestination tests that kafka destination can be created
func (s *FrontendHostSuite) TestFrontendHostCreateKafkaDestination() {
	defer s.resetMocks()
	frontendHost, ctx := s.utilGetContextAndFrontend()

	path := s.generateKey("/foo/bar")
	req := c.NewCreateDestinationRequest()
	req.Path = common.StringPtr(path)
	req.ConsumedMessagesRetention = common.Int32Ptr(15)
	req.UnconsumedMessagesRetention = common.Int32Ptr(30)
	req.OwnerEmail = common.StringPtr("test@uber.com")

	for _, f := range []func() (bool, string){
		func() (bool, string) {
			req.Type = c.DestinationTypePtr(c.DestinationType_PLAIN)
			return true, `base case plain`
		}, func() (bool, string) {
			req.KafkaCluster = common.StringPtr(`foo`)
			req.KafkaTopics = nil
			return false, `Should fail because topics not set`
		}, func() (bool, string) {
			req.KafkaCluster = common.StringPtr(`foo`)
			req.KafkaTopics = make([]string, 0)
			return false, `Should fail because topics len zero`
		}, func() (bool, string) {
			req.KafkaCluster = common.StringPtr(``)
			req.KafkaTopics = []string{`bar`}
			return false, `Should fail because cluster len zero`
		}, func() (bool, string) {
			req.KafkaCluster = common.StringPtr(`foo`)
			req.KafkaTopics = []string{`bar`}
			req.IsMultiZone = common.BoolPtr(true)
			req.ZoneConfigs = &c.DestinationZoneConfigs{
				Configs: []*c.DestinationZoneConfig{
					{
						Zone:                   common.StringPtr("zone1"),
						AllowPublish:           common.BoolPtr(true),
						AllowConsume:           common.BoolPtr(true),
						AlwaysReplicateTo:      common.BoolPtr(true),
						RemoteExtentReplicaNum: common.Int32Ptr(3),
					},
				},
			}
			return false, `Should fail because multi-zone is not allowed for a KAFKA destination`
		}, func() (bool, string) {
			req.KafkaCluster = common.StringPtr(`foo`)
			req.KafkaTopics = []string{`bar`}
			req.Type = c.DestinationTypePtr(c.DestinationType_PLAIN)
			return false, `Should fail because KAFKA config only allowed with type KAFKA`
		}, func() (bool, string) {
			return false, `kafka destination requires kafka config`
		}, func() (bool, string) {
			req.KafkaCluster = common.StringPtr(`foo`)
			req.KafkaTopics = []string{`bar`, ``, `quz`}
			return false, `topics should have no empty strings`
		}, func() (bool, string) {
			req.KafkaCluster = common.StringPtr(`foo`)
			req.KafkaTopics = []string{`bar`, `baz`, `quz`}
			return true, `should work with multiple topics`
		}, func() (bool, string) {
			req.KafkaCluster = common.StringPtr(`foo`)
			req.KafkaTopics = []string{`bar`}
			return true, `should work with one topic`
		},
	} {
		// Base config reset
		req.Type = c.DestinationTypePtr(c.DestinationType_KAFKA)
		req.IsMultiZone = common.BoolPtr(false)
		req.ZoneConfigs = nil
		req.KafkaCluster = nil
		req.KafkaTopics = nil

		// Adjust for this test
		shouldPass, testMsg := f()

		// Reset mock calls, since previous iterations may have queued up a spurious return that wasn't actually delivered
		s.resetMocks()
		s.mockController.On("CreateDestination", mock.Anything, mock.Anything).Return(destCreateRequestToDesc(req), nil).Once().Run(func(args mock.Arguments) {
			createReq := args.Get(1).(*shared.CreateDestinationRequest)
			s.Equal(createReq.GetPath(), req.GetPath(), testMsg)
			s.Equal(createReq.GetConsumedMessagesRetention(), req.GetConsumedMessagesRetention(), testMsg)
			s.Equal(createReq.GetUnconsumedMessagesRetention(), req.GetUnconsumedMessagesRetention(), testMsg)
			s.Equal(createReq.GetType(), shared.DestinationType(req.GetType()), testMsg)
			s.Equal(createReq.GetOwnerEmail(), req.GetOwnerEmail(), testMsg)
			s.Equal(createReq.GetIsMultiZone(), req.GetIsMultiZone(), testMsg)
			s.Equal(createReq.GetKafkaCluster(), req.GetKafkaCluster(), testMsg)
			s.True(common.StringSetEqual(createReq.GetKafkaTopics(), req.GetKafkaTopics()), testMsg)
		})

		dst, err := frontendHost.CreateDestination(ctx, req)
		if shouldPass {
			s.NoError(err, testMsg)
			s.NotNil(dst, testMsg)
		} else {
			s.Error(err, testMsg)
			s.Nil(dst, testMsg)
		}

		if dst != nil {
			s.Equal(dst.GetPath(), req.GetPath(), testMsg)
			s.Equal(dst.GetConsumedMessagesRetention(), req.GetConsumedMessagesRetention(), testMsg)
			s.Equal(dst.GetUnconsumedMessagesRetention(), req.GetUnconsumedMessagesRetention(), testMsg)
			s.Equal(dst.GetType(), req.GetType(), testMsg)
			s.Equal(dst.GetOwnerEmail(), req.GetOwnerEmail(), testMsg)
			s.Equal(dst.GetIsMultiZone(), req.GetIsMultiZone(), testMsg)
			s.Equal(dst.GetKafkaCluster(), req.GetKafkaCluster(), testMsg)
			s.True(common.StringSetEqual(dst.GetKafkaTopics(), req.GetKafkaTopics()), testMsg)
		}

	}

}

// TestFrontendHostReadDestinationRejectNil tests that a nil request fails with BadRequestError
func (s *FrontendHostSuite) TestFrontendHostReadDestinationRejectNil() {
	frontendHost, ctx := s.utilGetContextAndFrontend()

	dst, err := frontendHost.ReadDestination(ctx, nil)

	s.Error(err)
	s.Nil(dst)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostReadDestinationRejectBadPath tests that a bad destination path fails
func (s *FrontendHostSuite) TestFrontendHostReadDestinationRejectBadPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()

	req := c.NewReadDestinationRequest()
	req.Path = common.StringPtr("/fsdf8234*@#($*")

	dst, err := frontendHost.ReadDestination(ctx, req)

	s.Error(err)
	s.Nil(dst)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostReadDestinationNoExistPath tests that a non-extant destination path fails
func (s *FrontendHostSuite) TestFrontendHostReadDestinationNoExistPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(nil, &shared.EntityNotExistsError{})

	req := c.NewReadDestinationRequest()
	req.Path = common.StringPtr(s.generateKey("/foo/bad"))

	dst, err := frontendHost.ReadDestination(ctx, req)

	s.Error(err)
	s.Nil(dst)
	assert.IsType(s.T(), &c.EntityNotExistsError{}, err)
}

// TestFrontendHostReadDestination tests that a destination can be retrieved
func (s *FrontendHostSuite) TestFrontendHostReadDestination() {
	testPath := s.generateKey("/foo/bar")
	frontendHost, ctx := s.utilGetContextAndFrontend()

	createReq := c.NewCreateDestinationRequest()
	createReq.Path = common.StringPtr(testPath)
	createReq.ConsumedMessagesRetention = common.Int32Ptr(15)
	createReq.UnconsumedMessagesRetention = common.Int32Ptr(30)
	createReq.OwnerEmail = common.StringPtr("test@uber.com")
	createReq.Type = c.DestinationTypePtr(c.DestinationType_TIMER)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destCreateRequestToDesc(createReq), nil)

	req := c.NewReadDestinationRequest()
	req.Path = common.StringPtr(testPath)

	dst, err := frontendHost.ReadDestination(ctx, req)

	s.NoError(err)
	s.NotNil(dst)

	if dst != nil {
		s.Equal(dst.GetPath(), testPath)
		s.Equal(dst.GetConsumedMessagesRetention(), int32(15))
		s.Equal(dst.GetUnconsumedMessagesRetention(), int32(30))
		s.Equal(dst.GetOwnerEmail(), "test@uber.com")
		s.Equal(dst.GetType(), c.DestinationType_TIMER)
	}
}

// TestFrontendHostUpdateDestinationRejectBadPath tests that a bad destination path fails
func (s *FrontendHostSuite) TestFrontendHostUpdateDestinationRejectBadPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()

	req := c.NewUpdateDestinationRequest()
	req.Path = common.StringPtr("/fsdf8234*@#($*")
	req.Status = c.DestinationStatusPtr(c.DestinationStatus_SENDONLY)
	req.ConsumedMessagesRetention = common.Int32Ptr(1800)
	req.UnconsumedMessagesRetention = common.Int32Ptr(3600)
	req.OwnerEmail = common.StringPtr("test_up@uber.com")

	dst, err := frontendHost.UpdateDestination(ctx, req)

	s.Error(err)
	s.Nil(dst)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostUpdateDestinationNoExistPath tests that a non-extant destination path fails
func (s *FrontendHostSuite) TestFrontendHostUpdateDestinationNoExistPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()

	req := c.NewUpdateDestinationRequest()
	req.Path = common.StringPtr(s.generateKey("/foo/bad"))
	req.Status = c.DestinationStatusPtr(c.DestinationStatus_SENDONLY)
	req.ConsumedMessagesRetention = common.Int32Ptr(1800)
	req.UnconsumedMessagesRetention = common.Int32Ptr(3600)
	req.OwnerEmail = common.StringPtr("test_up@uber.com")
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(nil, &shared.EntityNotExistsError{})

	dst, err := frontendHost.UpdateDestination(ctx, req)

	s.Error(err)
	s.Nil(dst)
	assert.IsType(s.T(), &c.EntityNotExistsError{}, err)
}

// TestFrontendHostUpdateDestination tests that a destination can be updated
func (s *FrontendHostSuite) TestFrontendHostUpdateDestination() {
	testPath := s.generateKey("/foo/bar")
	frontendHost, ctx := s.utilGetContextAndFrontend()

	req := c.NewUpdateDestinationRequest()
	req.Path = common.StringPtr(testPath)
	req.Status = c.DestinationStatusPtr(c.DestinationStatus_SENDONLY)
	req.ConsumedMessagesRetention = common.Int32Ptr(1800)
	req.UnconsumedMessagesRetention = common.Int32Ptr(3600)
	req.OwnerEmail = common.StringPtr("test_up@uber.com")
	destDesc := destUpdateRequestToDesc(req)
	s.mockController.On("UpdateDestination", mock.Anything, mock.Anything).Return(destDesc, nil)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil)

	dst, err := frontendHost.UpdateDestination(ctx, req)

	s.NoError(err)
	s.NotNil(dst)

	if dst != nil {
		s.Equal(dst.GetPath(), testPath)
		s.Equal(dst.GetConsumedMessagesRetention(), int32(1800))
		s.Equal(dst.GetUnconsumedMessagesRetention(), int32(3600))
		s.Equal(dst.GetStatus(), c.DestinationStatus_SENDONLY)
		s.Equal(dst.GetOwnerEmail(), "test_up@uber.com")
	}
}

// TestFrontendHostDeleteDestinationRejectBadPath tests that a bad destination path fails
func (s *FrontendHostSuite) TestFrontendHostDeleteDestinationRejectBadPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()

	req := c.NewDeleteDestinationRequest()
	req.Path = common.StringPtr("/fsdf8234*@#($*")

	err := frontendHost.DeleteDestination(ctx, req)

	s.Error(err)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostDeleteDestinationNoExistPath tests that a non-extant destination path fails
func (s *FrontendHostSuite) TestFrontendHostDeleteDestinationNoExistPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()
	s.mockController.On("DeleteDestination", mock.Anything, mock.Anything).Return(c.NewEntityNotExistsError())

	req := c.NewDeleteDestinationRequest()
	req.Path = common.StringPtr(s.generateKey("/foo/bad"))

	err := frontendHost.DeleteDestination(ctx, req)
	s.Error(err)
}

// TestFrontendHostDeleteDestination tests that a destination can be deleted
func (s *FrontendHostSuite) TestFrontendHostDeleteDestination() {
	testPath := s.generateKey("/foo/bar")
	frontendHost, ctx := s.utilGetContextAndFrontend()
	s.mockController.On("DeleteDestination", mock.Anything, mock.Anything).Return(nil)

	req := c.NewDeleteDestinationRequest()
	req.Path = common.StringPtr(testPath)

	err := frontendHost.DeleteDestination(ctx, req)

	s.NoError(err)
}

// TestFrontendHostReadPublisherOptionsRejectBadPath tests that a bad destination path fails
func (s *FrontendHostSuite) TestFrontendHostReadPublisherOptionsRejectBadPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()

	req := c.NewReadPublisherOptionsRequest()
	req.Path = common.StringPtr("/fsdf8234*@#($*")

	dst, err := frontendHost.ReadPublisherOptions(ctx, req)

	s.Error(err)
	s.Nil(dst)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostReadPublisherOptionsNoExistPath tests that a non-extant destination path fails
func (s *FrontendHostSuite) TestFrontendHostReadPublisherOptionsNoExistPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(nil, &shared.EntityNotExistsError{})

	req := c.NewReadPublisherOptionsRequest()
	req.Path = common.StringPtr(s.generateKey("/foo/bad"))

	dst, err := frontendHost.ReadPublisherOptions(ctx, req)

	s.Error(err)
	s.Nil(dst)
	assert.IsType(s.T(), &c.EntityNotExistsError{}, err)
}

// TestFrontendHostReadPublisherOptionsKafka tests that ReadPublisherOptions fail for Kafka destinations
func (s *FrontendHostSuite) TestFrontendHostReadPublisherOptionsKafka() {
	testPath := s.generateKey("/foo/bar")
	testUUID := uuid.New()

	frontendHost, ctx := s.utilGetContextAndFrontend()
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(&shared.DestinationDescription{
		Path:            common.StringPtr(testPath),
		DestinationUUID: common.StringPtr(testUUID),
		Type:            common.InternalDestinationTypePtr(shared.DestinationType_KAFKA),
	}, nil)

	req := c.NewReadPublisherOptionsRequest()
	req.Path = common.StringPtr(testPath)

	dst, err := frontendHost.ReadPublisherOptions(ctx, req)

	s.Error(err)
	s.Nil(dst)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostReadPublisherOptionsReceiveOnly tests that ReadPublisherOptions fail for receive-only destinations
func (s *FrontendHostSuite) TestFrontendHostReadPublisherOptionsReceiveOnly() {
	testPath := s.generateKey("/foo/bar")
	testUUID := uuid.New()

	frontendHost, ctx := s.utilGetContextAndFrontend()
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(&shared.DestinationDescription{
		Path:            common.StringPtr(testPath),
		DestinationUUID: common.StringPtr(testUUID),
		Type:            common.InternalDestinationTypePtr(shared.DestinationType_PLAIN),
		Status:          common.InternalDestinationStatusPtr(shared.DestinationStatus_RECEIVEONLY),
	}, nil)

	req := c.NewReadPublisherOptionsRequest()
	req.Path = common.StringPtr(testPath)

	dst, err := frontendHost.ReadPublisherOptions(ctx, req)

	s.Error(err)
	s.Nil(dst)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostReadPublisherOptions tests that no hosts can be retrieved for a newly created destination
func (s *FrontendHostSuite) TestFrontendHostReadPublisherOptions() {
	testPath := s.generateKey("/foo/bar")
	frontendHost, ctx := s.utilGetContextAndFrontend()

	createReq := c.NewCreateDestinationRequest()
	createReq.Path = common.StringPtr(testPath)
	createReq.ConsumedMessagesRetention = common.Int32Ptr(15)
	createReq.UnconsumedMessagesRetention = common.Int32Ptr(30)
	createReq.OwnerEmail = common.StringPtr("test@uber.com")
	createReq.ChecksumOption = c.ChecksumOption_CRC32IEEE
	createReq.Type = c.DestinationTypePtr(c.DestinationType_TIMER)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destCreateRequestToDesc(createReq), nil)
	s.mockController.On("GetInputHosts", mock.Anything, mock.Anything).Return(&controller.GetInputHostsResult_{InputHostIds: []string{"127.0.0.1:1234"}}, nil)

	req := c.NewReadPublisherOptionsRequest()
	req.Path = common.StringPtr(testPath)

	pubOptions, err := frontendHost.ReadPublisherOptions(ctx, req)

	s.NoError(err)
	s.NotNil(pubOptions)
	s.Equal(2, len(pubOptions.GetHostProtocols()))
}

// TestFrontendHostReadDestinationHostsRejectBadPath tests that a bad destination path fails
func (s *FrontendHostSuite) TestFrontendHostReadDestinationHostsRejectBadPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()

	req := c.NewReadDestinationHostsRequest()
	req.Path = common.StringPtr("/fsdf8234*@#($*")

	dst, err := frontendHost.ReadDestinationHosts(ctx, req)

	s.Error(err)
	s.Nil(dst)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostReadDestinationHostsNoExistPath tests that a non-extant destination path fails
func (s *FrontendHostSuite) TestFrontendHostReadDestinationHostsNoExistPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(nil, &shared.EntityNotExistsError{})

	req := c.NewReadDestinationHostsRequest()
	req.Path = common.StringPtr(s.generateKey("/foo/bad"))

	dst, err := frontendHost.ReadDestinationHosts(ctx, req)

	s.Error(err)
	s.Nil(dst)
	assert.IsType(s.T(), &c.EntityNotExistsError{}, err)
}

// TestFrontendHostReadDestinationHostsKafka tests that ReadDestinationHosts fail for Kafka destinations
func (s *FrontendHostSuite) TestFrontendHostReadDestinationHostsKafka() {
	testPath := s.generateKey("/foo/bar")
	testUUID := uuid.New()

	frontendHost, ctx := s.utilGetContextAndFrontend()
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(&shared.DestinationDescription{
		Path:            common.StringPtr(testPath),
		DestinationUUID: common.StringPtr(testUUID),
		Type:            common.InternalDestinationTypePtr(shared.DestinationType_KAFKA),
	}, nil)

	req := c.NewReadDestinationHostsRequest()
	req.Path = common.StringPtr(testPath)

	hosts, err := frontendHost.ReadDestinationHosts(ctx, req)

	s.Error(err)
	s.Nil(hosts)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostReadDestinationHostsReceiveOnly tests that ReadDestinationHosts fail for receive-only destinations
func (s *FrontendHostSuite) TestFrontendHostReadDestinationHostsReceiveOnly() {
	testPath := s.generateKey("/foo/bar")
	testUUID := uuid.New()

	frontendHost, ctx := s.utilGetContextAndFrontend()
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(&shared.DestinationDescription{
		Path:            common.StringPtr(testPath),
		DestinationUUID: common.StringPtr(testUUID),
		Type:            common.InternalDestinationTypePtr(shared.DestinationType_PLAIN),
		Status:          common.InternalDestinationStatusPtr(shared.DestinationStatus_RECEIVEONLY),
	}, nil)

	req := c.NewReadDestinationHostsRequest()
	req.Path = common.StringPtr(testPath)

	hosts, err := frontendHost.ReadDestinationHosts(ctx, req)

	s.Error(err)
	s.Nil(hosts)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostReadDestinationHosts tests that no hosts can be retrieved for a newly created destination
func (s *FrontendHostSuite) TestFrontendHostReadDestinationHosts() {
	testPath := s.generateKey("/foo/bar")
	frontendHost, ctx := s.utilGetContextAndFrontend()

	createReq := c.NewCreateDestinationRequest()
	createReq.Path = common.StringPtr(testPath)
	createReq.ConsumedMessagesRetention = common.Int32Ptr(15)
	createReq.UnconsumedMessagesRetention = common.Int32Ptr(30)
	createReq.OwnerEmail = common.StringPtr("test@uber.com")
	createReq.Type = c.DestinationTypePtr(c.DestinationType_TIMER)
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(destCreateRequestToDesc(createReq), nil)
	s.mockController.On("GetInputHosts", mock.Anything, mock.Anything).Return(&controller.GetInputHostsResult_{InputHostIds: []string{"127.0.0.1:1234"}}, nil)

	req := c.NewReadDestinationHostsRequest()
	req.Path = common.StringPtr(testPath)

	hosts, err := frontendHost.ReadDestinationHosts(ctx, req)

	s.NoError(err)
	s.NotNil(hosts)
	s.Equal(2, len(hosts.GetHostProtocols()))
}

// TestFrontendHostCreateConsumerGroupRejectNil tests that a nil request fails with BadRequestError
func (s *FrontendHostSuite) TestFrontendHostCreateConsumerGroupRejectNil() {
	frontendHost, ctx := s.utilGetContextAndFrontend()
	dst, err := frontendHost.CreateConsumerGroup(ctx, nil)
	s.Error(err)
	s.Nil(dst)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostCreateConsumerGroupRejectBadPath tests that a bad consumer group path fails
func (s *FrontendHostSuite) TestFrontendHostCreateConsumerGroupRejectBadPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()

	// First a bad destination path
	req := c.NewCreateConsumerGroupRequest()
	req.DestinationPath = common.StringPtr("/fsdf8234*@#($*")
	req.ConsumerGroupName = common.StringPtr(s.generateKey("/GoodName"))

	cgd, err := frontendHost.CreateConsumerGroup(ctx, req)

	s.Error(err)
	s.Nil(cgd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)

	// Second, a good destination path and bad consumer group name
	req.ConsumerGroupName = common.StringPtr("/fsdf8234*@#($*")
	req.DestinationPath = common.StringPtr(s.generateKey("/GoodName"))

	cgd, err = frontendHost.CreateConsumerGroup(ctx, req)

	s.Error(err)
	s.Nil(cgd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostCreateConsumerGroupWithBadName tests that a consumer group can't be created under an existing, good destination path, but with a bad name
func (s *FrontendHostSuite) TestFrontendHostCreateConsumerGroupWithBadName() {
	testPath := s.generateKey("/foo/bar")
	frontendHost, ctx := s.utilGetContextAndFrontend()

	req := c.NewCreateConsumerGroupRequest()
	req.DestinationPath = common.StringPtr(testPath)
	req.ConsumerGroupName = common.StringPtr("/CGBadName)(*(*&$#)")

	cgd, err := frontendHost.CreateConsumerGroup(ctx, req)

	s.Error(err)
	s.Nil(cgd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostCreateConsumerGroup tests that a consumer group can be created
func (s *FrontendHostSuite) TestFrontendHostCreateConsumerGroup() {
	testPath := "/foo/bar"
	testCG := s.generateKey("/bar/CGName")
	frontendHost, ctx := s.utilGetContextAndFrontend()

	options := make(map[string]string)
	options[common.FlagDisableNackThrottling] = "true"
	options[common.FlagEnableSmartRetry] = "false"

	req := c.NewCreateConsumerGroupRequest()
	req.DestinationPath = common.StringPtr(testPath)
	req.ConsumerGroupName = common.StringPtr(testCG)
	req.IsMultiZone = common.BoolPtr(true)
	req.ZoneConfigs = &c.ConsumerGroupZoneConfigs{
		ActiveZone: common.StringPtr("zone1"),
		Configs: []*c.ConsumerGroupZoneConfig{
			{
				Zone:    common.StringPtr("zone1"),
				Visible: common.BoolPtr(true),
			},
		},
	}
	req.Options = options

	cgDesc := cgCreateRequestToDesc(req)
	frontendHost.writeCacheDestinationPathForUUID(destinationUUID(cgDesc.GetDestinationUUID()), testPath)

	dlqPath, _ := common.GetDLQPathNameFromCGName(testCG)
	destDesc := &shared.DestinationDescription{
		Path:            common.StringPtr(dlqPath),
		DestinationUUID: common.StringPtr(uuid.New()),
	}

	// create destination is needed because of dlq destination been created at time of consumer group creation
	s.mockController.On("CreateDestination", mock.Anything, mock.Anything).Return(destDesc, nil).Run(func(args mock.Arguments) {
		s.Equal(dlqPath, args.Get(1).(*shared.CreateDestinationRequest).GetPath())
	})
	s.mockController.On("CreateConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil).Run(func(args mock.Arguments) {
		createReq := args.Get(1).(*shared.CreateConsumerGroupRequest)
		s.Equal(createReq.GetConsumerGroupName(), req.GetConsumerGroupName())
		s.Equal(createReq.GetDestinationPath(), req.GetDestinationPath())
		s.Equal(createReq.GetIsMultiZone(), req.GetIsMultiZone())
		s.Equal(len(createReq.GetZoneConfigs()), len(req.GetZoneConfigs().GetConfigs()))
		s.Equal(createReq.GetZoneConfigs()[0].GetVisible(), req.GetZoneConfigs().GetConfigs()[0].GetVisible())
		s.Equal(createReq.GetOptions(), req.GetOptions())
	})

	cgd, err := frontendHost.CreateConsumerGroup(ctx, req)

	s.NoError(err)
	s.NotNil(cgd)

	if cgd != nil {
		s.Equal(cgd.GetConsumerGroupName(), req.GetConsumerGroupName())
		s.Equal(cgd.GetDestinationPath(), req.GetDestinationPath())
		s.Equal(cgd.GetIsMultiZone(), req.GetIsMultiZone())
		s.Equal(len(cgd.ZoneConfigs.GetConfigs()), len(req.GetZoneConfigs().GetConfigs()))
		s.Equal(cgd.ZoneConfigs.GetConfigs()[0].GetVisible(), req.GetZoneConfigs().GetConfigs()[0].GetVisible())
	}
}

// TestFrontendHostCreateConsumerGroupStartFrom tests whether CreateConsumerGroup is able to
// detect and appropriately convert various startFrom units.
func (s *FrontendHostSuite) TestFrontendHostCreateConsumerGroupStartFrom() {
	defer s.resetMocks()
	testPath := uuid.New()
	testCG := s.generateKey("/bar/CGName")
	frontendHost, ctx := s.utilGetContextAndFrontend()

	testStartFrom := func(startFrom int64, willFail bool) (startFromReturn int64, err error) {

		startFromReturn = math.MaxInt64

		req := c.NewCreateConsumerGroupRequest()
		req.DestinationPath = common.StringPtr(testPath)
		req.ConsumerGroupName = common.StringPtr(testCG)
		req.StartFrom = common.Int64Ptr(startFrom)

		cgDesc := cgCreateRequestToDesc(req)
		frontendHost.writeCacheDestinationPathForUUID(destinationUUID(cgDesc.GetDestinationUUID()), testPath)

		dlqPath, _ := common.GetDLQPathNameFromCGName(testCG)
		destDesc := &shared.DestinationDescription{
			Path:            common.StringPtr(dlqPath),
			DestinationUUID: common.StringPtr(uuid.New()),
		}

		if !willFail {

			s.mockController.On("CreateDestination", mock.Anything, mock.Anything).Return(destDesc, nil)

			s.mockController.On("CreateConsumerGroup", mock.Anything, mock.Anything).Once().Return(cgDesc, nil).Run(func(args mock.Arguments) {
				startFromReturn = args.Get(1).(*shared.CreateConsumerGroupRequest).GetStartFrom()
			})
		}

		// set startFrom to the test value (of varying units)
		req.StartFrom = common.Int64Ptr(startFrom)

		_, err = frontendHost.CreateConsumerGroup(ctx, req)

		s.resetMocks() // reset mocks before the next test

		return
	}

	testStartFromTime := func(startFromTime time.Time) {

		startFromSeconds := startFromTime.Unix()
		startFromMillis := startFromSeconds * int64(1e3)
		startFromMicros := startFromSeconds * int64(1e6)
		startFromNanos := startFromSeconds * int64(1e9)

		// test startFrom with varying units
		ret, err := testStartFrom(startFromSeconds, false)
		s.Nil(err)
		s.EqualValues(startFromNanos, ret)

		ret, err = testStartFrom(startFromMillis, false)
		s.Nil(err)
		s.EqualValues(startFromNanos, ret)

		ret, err = testStartFrom(startFromMicros, false)
		s.Nil(err)
		s.EqualValues(startFromNanos, ret)

		ret, err = testStartFrom(startFromNanos, false)
		s.Nil(err)
		s.EqualValues(startFromNanos, ret)
	}

	testStartFromFail := func(startFromTime time.Time) {

		startFromSeconds := startFromTime.Unix()
		startFromMillis := startFromSeconds * int64(1e3)
		startFromMicros := startFromSeconds * int64(1e6)
		startFromNanos := startFromSeconds * int64(1e9)

		// test startFrom with varying units
		_, err := testStartFrom(startFromSeconds, true)
		s.Error(err)
		assert.IsType(s.T(), &c.BadRequestError{}, err)

		_, err = testStartFrom(startFromMillis, true)
		s.Error(err)
		assert.IsType(s.T(), &c.BadRequestError{}, err)

		_, err = testStartFrom(startFromMicros, true)
		s.Error(err)
		assert.IsType(s.T(), &c.BadRequestError{}, err)

		_, err = testStartFrom(startFromNanos, true)
		s.Error(err)
		assert.IsType(s.T(), &c.BadRequestError{}, err)
	}

	testStartFromTime(time.Now())                                             // now
	testStartFromTime(time.Now().Add(-4 * 7 * 24 * time.Hour))                // four weeks ago
	testStartFromTime(time.Now().Add(45 * time.Second))                       // 45secs from now (time skew)
	testStartFromTime(time.Now().Add(-5 * 365 * 24 * time.Hour))              // 5 years back
	testStartFromTime(time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)) // 1/1/2000

	testStartFromFail(time.Now().Add(2 * time.Minute)) // 2 mins from now
	testStartFromFail(time.Now().Add(1 * time.Hour))   // an hour from now

	ret, err := testStartFrom(0, false) // start-from of '0'
	s.Nil(err)
	s.EqualValues(0, ret)

	ret, err = testStartFrom(1, false) // start-from of '1'
	s.Nil(err)
	s.EqualValues(1e9, ret) // '1' should get interpreted in seconds = 1e9 ns

	ret, err = testStartFrom(1e9, false) // start-from of '1 * 1e9'
	s.Nil(err)
	s.EqualValues(1e18, ret) // 1e9 in seconds = 1e18 nanoseconds
}

// TestFrontendHostReadConsumerGroupRejectNil tests that a nil request fails with BadRequestError
func (s *FrontendHostSuite) TestFrontendHostReadConsumerGroupRejectNil() {
	frontendHost, ctx := s.utilGetContextAndFrontend()
	dst, err := frontendHost.ReadConsumerGroup(ctx, nil)
	s.Error(err)
	s.Nil(dst)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostReadConsumerGroupRejectBadPath tests that a bad consumer group path fails
func (s *FrontendHostSuite) TestFrontendHostReadConsumerGroupRejectBadPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()

	// First a bad destination path
	req := c.NewReadConsumerGroupRequest()
	req.DestinationPath = common.StringPtr("/fsdf8234*@#($*")
	req.ConsumerGroupName = common.StringPtr(s.generateKey("/GoodName"))

	cgd, err := frontendHost.ReadConsumerGroup(ctx, req)

	s.Error(err)
	s.Nil(cgd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)

	// Second, a good destination path and bad consumer group name
	req.ConsumerGroupName = common.StringPtr("/fsdf8234*@#($*")
	req.DestinationPath = common.StringPtr(s.generateKey("/GoodName"))

	cgd, err = frontendHost.ReadConsumerGroup(ctx, req)

	s.Error(err)
	s.Nil(cgd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostReadConsumerGroupWithBadName tests that a consumer group can't be created under an existing, good destination path, but with a bad name
func (s *FrontendHostSuite) TestFrontendHostReadConsumerGroupWithBadName() {
	testPath := s.generateKey("/foo/bax")
	frontendHost, ctx := s.utilGetContextAndFrontend()

	req := c.NewReadConsumerGroupRequest()
	req.DestinationPath = common.StringPtr(testPath)
	req.ConsumerGroupName = common.StringPtr("/CGBadName)(*(*&$#)")

	cgd, err := frontendHost.ReadConsumerGroup(ctx, req)

	s.Error(err)
	s.Nil(cgd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostReadConsumerGroup tests that a consumer group can be created
func (s *FrontendHostSuite) TestFrontendHostReadConsumerGroup() {
	testPath := s.generateKey("/foo/bax")
	frontendHost, ctx := s.utilGetContextAndFrontend()

	createReq := c.NewCreateConsumerGroupRequest()
	createReq.DestinationPath = common.StringPtr(testPath)
	createReq.ConsumerGroupName = common.StringPtr(s.generateKey("/CG/Name"))
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(&shared.DestinationDescription{Path: common.StringPtr(testPath)}, nil)
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgCreateRequestToDesc(createReq), nil)

	req := c.NewReadConsumerGroupRequest()
	req.DestinationPath = common.StringPtr(createReq.GetDestinationPath())
	req.ConsumerGroupName = common.StringPtr(createReq.GetConsumerGroupName())

	cgDest, err := frontendHost.ReadConsumerGroup(ctx, req)

	s.NoError(err)
	s.NotNil(cgDest)

	if cgDest != nil {
		s.Equal(cgDest.GetConsumerGroupName(), createReq.GetConsumerGroupName())
		s.Equal(cgDest.GetDestinationPath(), createReq.GetDestinationPath())
	}
}

// TestFrontendHostUpdateConsumerGroupRejectNil tests that a nil request fails with BadRequestError
func (s *FrontendHostSuite) TestFrontendHostUpdateConsumerGroupRejectNil() {
	frontendHost, ctx := s.utilGetContextAndFrontend()
	dst, err := frontendHost.UpdateConsumerGroup(ctx, nil)
	s.Error(err)
	s.Nil(dst)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostUpdateConsumerGroupRejectBadPath tests that a bad consumer group path fails
func (s *FrontendHostSuite) TestFrontendHostUpdateConsumerGroupRejectBadPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()

	// First a bad destination path
	req := c.NewUpdateConsumerGroupRequest()
	req.DestinationPath = common.StringPtr("/fsdf8234*@#($*")
	req.ConsumerGroupName = common.StringPtr(s.generateKey("/GoodName"))

	cgd, err := frontendHost.UpdateConsumerGroup(ctx, req)

	s.Error(err)
	s.Nil(cgd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)

	// Second, a good destination path and bad consumer group name
	req.ConsumerGroupName = common.StringPtr("/fsdf8234*@#($*")
	req.DestinationPath = common.StringPtr(s.generateKey("/GoodName"))

	cgd, err = frontendHost.UpdateConsumerGroup(ctx, req)

	s.Error(err)
	s.Nil(cgd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostUpdateConsumerGroupWithBadName tests that a consumer group can't be created under an existing, good destination path, but with a bad name
func (s *FrontendHostSuite) TestFrontendHostUpdateConsumerGroupWithBadName() {
	testPath := s.generateKey("/foo/bax")
	frontendHost, ctx := s.utilGetContextAndFrontend()

	req := c.NewUpdateConsumerGroupRequest()
	req.DestinationPath = common.StringPtr(testPath)
	req.ConsumerGroupName = common.StringPtr("/CGBadName)(*(*&$#)")

	cgd, err := frontendHost.UpdateConsumerGroup(ctx, req)

	s.Error(err)
	s.Nil(cgd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostUpdateConsumerGroup tests that a consumer group can be updated
func (s *FrontendHostSuite) TestFrontendHostUpdateConsumerGroup() {
	testPath := s.generateKey("/foo/bax")
	frontendHost, ctx := s.utilGetContextAndFrontend()

	options := make(map[string]string)
	options[common.FlagDisableNackThrottling] = "true"
	options[common.FlagEnableSmartRetry] = "false"

	req := new(c.UpdateConsumerGroupRequest)
	req.DestinationPath = common.StringPtr(testPath)
	req.ConsumerGroupName = common.StringPtr(s.generateKey("/CG/Name"))
	req.LockTimeoutInSeconds = common.Int32Ptr(1)
	req.MaxDeliveryCount = common.Int32Ptr(2)
	req.SkipOlderMessagesInSeconds = common.Int32Ptr(3)
	req.DelaySeconds = common.Int32Ptr(5)
	req.Status = c.ConsumerGroupStatusPtr(c.ConsumerGroupStatus_DISABLED)
	req.OwnerEmail = common.StringPtr("consumer_front_test@uber.com")
	req.Options = options

	newCGDesc := cgUpdateRequestToDesc(req)
	frontendHost.writeCacheDestinationPathForUUID(destinationUUID(newCGDesc.GetDestinationUUID()), testPath)
	s.mockController.On("UpdateConsumerGroup", mock.Anything, mock.Anything).Return(newCGDesc, nil)
	cgd, err := frontendHost.UpdateConsumerGroup(ctx, req)

	s.NoError(err)
	s.NotNil(cgd)

	if cgd != nil {
		s.Equal(cgd.ConsumerGroupName, req.ConsumerGroupName)
		s.Equal(cgd.DestinationPath, req.DestinationPath)
		s.Equal(cgd.LockTimeoutInSeconds, req.LockTimeoutInSeconds)
		s.Equal(cgd.MaxDeliveryCount, req.MaxDeliveryCount)
		s.Equal(cgd.DelaySeconds, req.DelaySeconds)
		s.Equal(cgd.Status, req.Status)
		s.Equal(cgd.OwnerEmail, req.OwnerEmail)
		s.Equal(cgd.Options, req.Options)
	}
}

// TestFrontendHostDeleteConsumerGroupRejectNil tests that a nil request fails with BadRequestError
func (s *FrontendHostSuite) TestFrontendHostDeleteConsumerGroupRejectNil() {
	frontendHost, ctx := s.utilGetContextAndFrontend()
	err := frontendHost.DeleteConsumerGroup(ctx, nil)
	s.Error(err)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostDeleteConsumerGroupRejectBadPath tests that a bad consumer group path fails
func (s *FrontendHostSuite) TestFrontendHostDeleteConsumerGroupRejectBadPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()

	// First a bad destination path
	req := c.NewDeleteConsumerGroupRequest()
	req.DestinationPath = common.StringPtr("/fsdf8234*@#($*")
	req.ConsumerGroupName = common.StringPtr(s.generateKey("/GoodName"))

	err := frontendHost.DeleteConsumerGroup(ctx, req)

	s.Error(err)
	assert.IsType(s.T(), &c.BadRequestError{}, err)

	// Second, a good destination path and bad consumer group name
	req.ConsumerGroupName = common.StringPtr("/fsdf8234*@#($*")
	req.DestinationPath = common.StringPtr(s.generateKey("/GoodName"))

	err = frontendHost.DeleteConsumerGroup(ctx, req)

	s.Error(err)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostDeleteConsumerGroupWithBadName tests that a consumer group can't be deleted under an existing, good destination path, but with a bad name
func (s *FrontendHostSuite) TestFrontendHostDeleteConsumerGroupWithBadName() {
	testPath := s.generateKey("/foo/bax")
	frontendHost, ctx := s.utilGetContextAndFrontend()

	req := c.NewDeleteConsumerGroupRequest()
	req.DestinationPath = common.StringPtr(testPath)
	req.ConsumerGroupName = common.StringPtr("/CGBadName)(*(*&$#)")

	err := frontendHost.DeleteConsumerGroup(ctx, req)

	s.Error(err)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostDeleteConsumerGroup tests that a consumer group can be created
func (s *FrontendHostSuite) TestFrontendHostDeleteConsumerGroup() {
	testPath := s.generateKey("/foo/bax")
	testCG := s.generateKey("/CG/Name")
	testDLQUUID := uuid.New()
	frontendHost, ctx := s.utilGetContextAndFrontend()
	s.mockController.On("DeleteConsumerGroup", mock.Anything, mock.Anything).Return(nil)

	req := c.NewDeleteConsumerGroupRequest()
	req.DestinationPath = common.StringPtr(testPath)
	req.ConsumerGroupName = common.StringPtr(testCG)

	err := frontendHost.DeleteConsumerGroup(ctx, req)
	s.NoError(err)

	// Test delete of DLQ consumer group
	req.DestinationPath = common.StringPtr(testDLQUUID)
	err = frontendHost.DeleteConsumerGroup(ctx, req)
	s.NoError(err)
}

// TestFrontendHostReadConsumerGroupHostsRejectNil tests that a nil request fails with BadRequestError
func (s *FrontendHostSuite) TestFrontendHostReadConsumerGroupHostsRejectNil() {
	frontendHost, ctx := s.utilGetContextAndFrontend()
	dst, err := frontendHost.ReadConsumerGroupHosts(ctx, nil)
	s.Error(err)
	s.Nil(dst)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostReadConsumerGroupHostsRejectBadPath tests that a bad consumer group path fails
func (s *FrontendHostSuite) TestFrontendHostReadConsumerGroupHostsRejectBadPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()

	// First a bad destination path
	req := c.NewReadConsumerGroupHostsRequest()
	req.DestinationPath = common.StringPtr("/Bad/Name#$%#$%")
	req.ConsumerGroupName = common.StringPtr(s.generateKey("/Good/Name"))

	cgd, err := frontendHost.ReadConsumerGroupHosts(ctx, req)

	s.Error(err)
	s.Nil(cgd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)

	// Second, a good destination path and bad consumer group name
	req.ConsumerGroupName, req.DestinationPath = req.DestinationPath, req.ConsumerGroupName

	cgd, err = frontendHost.ReadConsumerGroupHosts(ctx, req)

	s.Error(err)
	s.Nil(cgd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostReadConsumerGroupHostsWithBadName tests that a consumer group can't be read under an existing, good destination path, but with a bad name
func (s *FrontendHostSuite) TestFrontendHostReadConsumerGroupHostsWithBadName() {
	testPath := s.generateKey("/foo/bax")
	frontendHost, ctx := s.utilGetContextAndFrontend()
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(nil, &shared.EntityNotExistsError{})

	req := c.NewReadConsumerGroupHostsRequest()
	req.DestinationPath = common.StringPtr(testPath)
	req.ConsumerGroupName = common.StringPtr(s.generateKey("/CG/BadName"))

	cgd, err := frontendHost.ReadConsumerGroupHosts(ctx, req)

	s.Error(err)
	s.Nil(cgd)
	assert.IsType(s.T(), &c.EntityNotExistsError{}, err)
}

// TestFrontendHostReadConsumerGroupHosts tests that a consumer group can be read
func (s *FrontendHostSuite) TestFrontendHostReadConsumerGroupHosts() {
	testPath := s.generateKey("/foo/bax")
	frontendHost, ctx := s.utilGetContextAndFrontend()

	createReq := c.NewCreateConsumerGroupRequest()
	createReq.DestinationPath = common.StringPtr(testPath)
	createReq.ConsumerGroupName = common.StringPtr(s.generateKey("/CG/Name"))
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgCreateRequestToDesc(createReq), nil)
	s.mockController.On("GetOutputHosts", mock.Anything, mock.Anything).Return(&controller.GetOutputHostsResult_{OutputHostIds: []string{"127.0.0.1:1234"}}, nil)

	req := new(c.ReadConsumerGroupHostsRequest)
	req.DestinationPath = common.StringPtr(createReq.GetDestinationPath())
	req.ConsumerGroupName = common.StringPtr(createReq.GetConsumerGroupName())

	hosts, err := frontendHost.ReadConsumerGroupHosts(ctx, req)

	s.NoError(err)
	s.NotNil(hosts)
	s.Equal(2, len(hosts.GetHostProtocols()))
}

// TestFrontendHostListDestinationsRejectNil tests that a nil request fails
func (s *FrontendHostSuite) TestFrontendHostListDestinationsRejectNil() {
	frontendHost, ctx := s.utilGetContextAndFrontend()
	req := c.NewListDestinationsRequest()
	req.Prefix = common.StringPtr(s.generateKey("/foo/bad"))

	// nil as request will be rejected
	dd, err := frontendHost.ListDestinations(ctx, nil)
	s.Error(err)
	s.Nil(dd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)

	// default limit 0 will be rejected
	dd, err = frontendHost.ListDestinations(ctx, req)
	s.Error(err)
	s.Nil(dd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)

	// set limit to 0 explicitly wont help neither
	req.Limit = common.Int64Ptr(0)
	dd, err = frontendHost.ListDestinations(ctx, req)
	s.Error(err)
	s.Nil(dd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostListDestinationsNoExistPath tests that a non-extant destination path succeeds with no results
func (s *FrontendHostSuite) TestFrontendHostListDestinationsNoExistPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()
	s.mockMeta.On("ListDestinations", mock.Anything, mock.Anything).Return(shared.NewListDestinationsResult_(), nil)

	req := c.NewListDestinationsRequest()
	req.Prefix = common.StringPtr(s.generateKey("/foo/bad"))
	req.Limit = common.Int64Ptr(20)

	dd, err := frontendHost.ListDestinations(ctx, req)
	s.NoError(err)
	s.NotNil(dd)
	s.Equal(0, len(dd.GetDestinations()))
}

// TestFrontendHostListDestinations tests that a list of destinations can be listed
func (s *FrontendHostSuite) TestFrontendHostListDestinations() {
	var err error
	testPath := s.generateKey("/foo/bau")
	frontendHost, ctx := s.utilGetContextAndFrontend()

	// Generate 10 unique destinations
	createReq := c.NewCreateDestinationRequest()
	createReq.Path = common.StringPtr(testPath)
	createReq.ConsumedMessagesRetention = common.Int32Ptr(15)
	createReq.UnconsumedMessagesRetention = common.Int32Ptr(30)
	createReq.Type = c.DestinationTypePtr(c.DestinationType_TIMER)

	// An accounting map to keep track of which destinations have been added/seen
	acct := make(map[string]bool)
	listResult := &shared.ListDestinationsResult_{Destinations: make([]*shared.DestinationDescription, 0, 10)}
	for i := 0; i < 10; i++ {
		createReq.Path = common.StringPtr(testPath + fmt.Sprintf("%d", i))
		listResult.Destinations = append(listResult.Destinations, destCreateRequestToDesc(createReq))
		// Add them to the accounting map
		acct[createReq.GetPath()] = true
	}
	s.mockMeta.On("ListDestinations", mock.Anything, mock.Anything).Return(listResult, nil)

	//  Execute a ListDestinations Request to read them
	req := c.NewListDestinationsRequest()
	req.Prefix = common.StringPtr(testPath)
	req.Limit = common.Int64Ptr(20)

	dd, err := frontendHost.ListDestinations(ctx, req)
	s.NoError(err)
	s.NotNil(dd)
	s.Equal(10, len(dd.GetDestinations()))

	for _, dsts := range dd.GetDestinations() {
		// Verify that the destination path was one of the ones that we added to the map
		b, ok := acct[dsts.GetPath()]
		s.True(ok)
		s.True(b)

		// Verify that the other fields are as inserted
		s.Equal(dsts.GetConsumedMessagesRetention(), int32(15))
		s.Equal(dsts.GetUnconsumedMessagesRetention(), int32(30))
		s.Equal(dsts.GetType(), c.DestinationType_TIMER)

		// Remove the current destination from the map, so that we detect duplicated entries
		acct[dsts.GetPath()] = false
	}
}

// TestFrontendHostListConsumerGroupsRejectNil tests that a nil request fails
func (s *FrontendHostSuite) TestFrontendHostListConsumerGroupsRejectNil() {
	frontendHost, ctx := s.utilGetContextAndFrontend()
	req := c.NewListConsumerGroupRequest()
	req.DestinationPath = common.StringPtr(s.generateKey("/foo/Dest_"))

	// nil as request will be rejected
	cgd, err := frontendHost.ListConsumerGroups(ctx, nil)
	s.Error(err)
	s.Nil(cgd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)

	// default limit 0 will be rejected
	cgd, err = frontendHost.ListConsumerGroups(ctx, req)
	s.Error(err)
	s.Nil(cgd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)

	// set limit to 0 explicitly wont help neither
	req.Limit = common.Int64Ptr(0)
	cgd, err = frontendHost.ListConsumerGroups(ctx, req)
	s.Error(err)
	s.Nil(cgd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostListConsumerGroupsRejectBadPath tests that a bad consumer group path fails
func (s *FrontendHostSuite) TestFrontendHostListConsumerGroupsRejectBadPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()

	req := c.NewListConsumerGroupRequest()
	req.DestinationPath = common.StringPtr("/fsdf8234*@#($*")
	req.ConsumerGroupName = req.DestinationPath
	req.Limit = common.Int64Ptr(10)

	cgd, err := frontendHost.ListConsumerGroups(ctx, req)

	s.Error(err)
	s.Nil(cgd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)

	req.DestinationPath = common.StringPtr(s.generateKey("/good/Path")) //CGName is still bad

	cgd, err = frontendHost.ListConsumerGroups(ctx, req)

	s.Error(err)
	s.Nil(cgd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)

	req.DestinationPath, req.ConsumerGroupName = req.ConsumerGroupName, req.DestinationPath // CGName good, DP bad

	cgd, err = frontendHost.ListConsumerGroups(ctx, req)

	s.Error(err)
	s.Nil(cgd)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostListConsumerGroupsNoExistPath tests that a non-extant consumer group path succeeds with no results
func (s *FrontendHostSuite) TestFrontendHostListConsumerGroupsNoExistPath() {
	frontendHost, ctx := s.utilGetContextAndFrontend()
	s.mockMeta.On("ListConsumerGroups", mock.Anything, mock.Anything).Return(shared.NewListConsumerGroupResult_(), nil)

	req := c.NewListConsumerGroupRequest()
	req.DestinationPath = common.StringPtr(s.generateKey("/foo/bad"))
	req.ConsumerGroupName = common.StringPtr(s.generateKey("/good/name"))
	req.Limit = common.Int64Ptr(10)

	cgd, err := frontendHost.ListConsumerGroups(ctx, req)
	s.NoError(err)
	s.NotNil(cgd)
	s.Equal(0, len(cgd.GetConsumerGroups()))
}

// TestFrontendHostListConsumerGroups tests that a list of consumer groups can be listed
func (s *FrontendHostSuite) TestFrontendHostListConsumerGroups() {
	//	var err error
	testPath := s.generateKey("/foo/Dest_")
	testCG := s.generateKey("/bar/CG_")
	frontendHost, ctx := s.utilGetContextAndFrontend()

	// An accounting map to keep track of which consumer groups have been added/seen
	groups := make(map[string]bool)
	listResult := &shared.ListConsumerGroupResult_{ConsumerGroups: make([]*shared.ConsumerGroupDescription, 0, 9)}
	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			reqCG := c.NewCreateConsumerGroupRequest()
			reqCG.DestinationPath = common.StringPtr(testPath)
			reqCG.ConsumerGroupName = common.StringPtr(testCG + fmt.Sprintf("%d_%d", i, j))
			listResult.ConsumerGroups = append(listResult.ConsumerGroups, cgCreateRequestToDesc(reqCG))
			groups[reqCG.GetConsumerGroupName()] = true
		}
	}
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(&shared.DestinationDescription{Path: common.StringPtr(testPath)}, nil)
	s.mockMeta.On("ListConsumerGroups", mock.Anything, mock.Anything).Return(listResult, nil)

	// Execute a ListConsumerGroups Request to read them
	req := c.NewListConsumerGroupRequest()
	req.DestinationPath = common.StringPtr(testPath)
	req.Limit = common.Int64Ptr(20)

	cgd, err := frontendHost.ListConsumerGroups(ctx, req)

	s.NoError(err)
	s.NotNil(cgd)
	s.Equal(9, len(cgd.GetConsumerGroups()))

	for _, cg := range cgd.GetConsumerGroups() {
		// Verify that the consumer group path was one of the ones that we added to the map
		b, ok := groups[cg.GetConsumerGroupName()]
		s.True(ok)
		s.True(b)
		// Remove the current consumer group from the map, so that we detect duplicated entries
		groups[cg.GetConsumerGroupName()] = false
	}
}

// TestFrontendHostUpdateConsumerGroup tests that a consumer group can be updated
func (s *FrontendHostSuite) TestFrontendHostDLQMergePurge() {
	testPath := s.generateKey("/foo/bax")
	frontendHost, ctx := s.utilGetContextAndFrontend()

	createReq := c.NewCreateConsumerGroupRequest()
	createReq.DestinationPath = common.StringPtr(testPath)
	createReq.ConsumerGroupName = common.StringPtr(s.generateKey("/CG/Name"))
	s.mockMeta.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgCreateRequestToDesc(createReq), nil)
	s.mockMeta.On("UpdateDestinationDLQCursors", mock.Anything, mock.Anything).Return(nil, nil)

	// Test Merge-Purge lockout

	reqM := c.NewMergeDLQForConsumerGroupRequest()
	reqM.ConsumerGroupName = common.StringPtr(createReq.GetConsumerGroupName())
	reqM.DestinationPath = common.StringPtr(createReq.GetDestinationPath())
	reqP := c.NewPurgeDLQForConsumerGroupRequest()
	reqP.ConsumerGroupName = common.StringPtr(createReq.GetConsumerGroupName())
	reqP.DestinationPath = common.StringPtr(createReq.GetDestinationPath())

	// Mock it like it's actively merging in next few calls
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(&shared.DestinationDescription{DLQMergeBefore: common.Int64Ptr(1)}, nil).Times(3)

	err := frontendHost.MergeDLQForConsumerGroup(ctx, reqM)
	s.NoError(err)

	err = frontendHost.PurgeDLQForConsumerGroup(ctx, reqP) /**/
	s.Error(err)                                           // Merge followed by purge is not allowed until merge settles
	s.True(strings.Contains(err.Error(), `settle`), err.Error())
	s.IsType(&c.InternalServiceError{}, err)

	// Merge is allowed, since purge failed
	err = frontendHost.MergeDLQForConsumerGroup(ctx, reqM) /**/
	s.NoError(err)

	// Now mock it like it's actively purging in next few calls
	s.mockMeta.On("ReadDestination", mock.Anything, mock.Anything).Return(&shared.DestinationDescription{DLQPurgeBefore: common.Int64Ptr(1)}, nil).Times(3)

	// Test Purge-Merge-Purge interleave

	err = frontendHost.PurgeDLQForConsumerGroup(ctx, reqP) /**/
	s.NoError(err)

	err = frontendHost.MergeDLQForConsumerGroup(ctx, reqM)
	s.Error(err) // Merge is not allowed after purge until the purge settles
	s.True(strings.Contains(err.Error(), `settle`), err.Error())
	s.IsType(&c.InternalServiceError{}, err)

	// Purge is allowed, since merge failed
	err = frontendHost.PurgeDLQForConsumerGroup(ctx, reqP)
	s.NoError(err)
}

// TestFrontendHostGetQueueDepthInfoNil tests that a nil request fails with BadRequestError
func (s *FrontendHostSuite) TestFrontendHostGetQueueDepthInfoNil() {
	frontendHost, ctx := s.utilGetContextAndFrontend()
	info, err := frontendHost.GetQueueDepthInfo(ctx, nil)
	s.Error(err)
	s.Nil(info)
	assert.IsType(s.T(), &c.BadRequestError{}, err)
}

// TestFrontendHostGetQueueDepthInfoBad tests that a bad request fails with BadRequestError
func (s *FrontendHostSuite) TestFrontendHostGetQueueDepthInfoBad() {
	frontendHost, ctx := s.utilGetContextAndFrontend()
	r := c.NewGetQueueDepthInfoRequest()

	t := []string{``, `/foo/bar`, `foo`, `7851377b-4eb3-430b-ae46-513bc1df503` /*short by one*/}

	for _, k := range t {
		r.ConsumerGroupName = common.StringPtr(k) // Only UUID is allowed
		info, err := frontendHost.GetQueueDepthInfo(ctx, r)
		s.Error(err)
		s.Nil(info)
		assert.IsType(s.T(), &c.BadRequestError{}, err)
		s.True(strings.Contains(err.Error(), `UUID`), err.Error())
	}

	u := []*string{common.StringPtr(`7851377b-4eb3-430b-ae46-513bc1df503c`), common.StringPtr(`/foo/bar`)}
	v := []*string{common.StringPtr(`7851377b-4eb3-430b-ae46-513bc1df503c`), common.StringPtr(`/foo/bar/`), common.StringPtr(``), nil}

	for _, l := range u {
		for _, m := range v {
			r.ConsumerGroupName = m
			r.DestinationPath = l
			info, err := frontendHost.GetQueueDepthInfo(ctx, r)
			s.Error(err)
			s.Nil(info)
			assert.IsType(s.T(), &c.BadRequestError{}, err)
		}
	}
}

// TestFrontendHostGetQueueDepthInfoGood tests that a good request succeeds
func (s *FrontendHostSuite) TestFrontendHostGetQueueDepthInfoGood() {
	frontendHost, ctx := s.utilGetContextAndFrontend()
	r := c.NewGetQueueDepthInfoRequest()
	r.ConsumerGroupName = common.StringPtr(`7851377b-4eb3-430b-ae46-513bc1df503c`)
	s.mockController.On(`GetQueueDepthInfo`, mock.Anything, mock.Anything).Return(
		&controller.GetQueueDepthInfoResult_{
			Value: common.StringPtr(`foo`),
		},
		nil).Twice()
	s.mockMeta.On(`ReadConsumerGroup`, mock.Anything, mock.Anything).Return(
		&shared.ConsumerGroupDescription{
			ConsumerGroupUUID: common.StringPtr(`foo`),
		},
		nil).Once()
	s.mockMeta.On(`ReadDestination`, mock.Anything, mock.Anything).Return(
		&shared.DestinationDescription{
			Path: common.StringPtr(`bar`),
		},
		nil,
	).Once()

	info, err := frontendHost.GetQueueDepthInfo(ctx, r)
	s.NoError(err)
	s.NotNil(info)
	s.Equal(info.GetValue(), `foo`)

	r.ConsumerGroupName = common.StringPtr(`/foo/bar_cg`)
	r.DestinationPath = common.StringPtr(`/foo/bar`)

	info, err = frontendHost.GetQueueDepthInfo(ctx, r)
	s.NoError(err)
	s.NotNil(info)
	s.Equal(info.GetValue(), `foo`)
}

// resetMocks clears all pending mock calls
func (s *FrontendHostSuite) resetMocks() {
	s.mockController.Calls = nil
	s.mockController.ExpectedCalls = nil
	s.mockMeta.Calls = nil
	s.mockMeta.ExpectedCalls = nil
}
