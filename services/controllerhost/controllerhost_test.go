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

package controllerhost

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mc "github.com/uber/cherami-server/clients/metadata"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
	"github.com/uber/cherami-server/test"
	mockcommon "github.com/uber/cherami-server/test/mocks/common"
	mockreplicator "github.com/uber/cherami-server/test/mocks/replicator"
	c "github.com/uber/cherami-thrift/.generated/go/controller"
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	inputAddr  = "127.0.1.1"
	outputAddr = "127.0.2.1"
	storeAddr  = "127.0.3.1"
)

type McpSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
	seqNum         int32
	mockrpm        *common.MockRingpopMonitor
	cfg            configure.CommonAppConfig
	cdb            *mc.TestCluster
	mcp            *Mcp
	inputPort      int
	outputPort     int
	storePort      int
	mClient        m.TChanMetadataService
	mockReplicator *mockreplicator.MockTChanReplicator
}

func TestMcpSuite(t *testing.T) {
	suite.Run(t, new(McpSuite))
}

func (s *McpSuite) SetupSuite() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	// cassandra set up
	s.cdb = &mc.TestCluster{}
	s.cdb.SetupTestCluster()
	s.seqNum = rand.Int31n(10000)
}

func (s *McpSuite) TearDownSuite() {
	s.cdb.TearDownTestCluster()
}

func (s *McpSuite) generateName(prefix string) string {
	seq := int(atomic.AddInt32(&s.seqNum, 1))
	return strings.Join([]string{prefix, strconv.Itoa(seq)}, ".")
}

func (s *McpSuite) startController() {
	// first setup the port for the controller and also
	// the ringhosts properly so that we can bootstrap.
	_, listenIP, port, err := test.FindEphemeralPort()
	s.Nil(err)
	_, _, wsPort, err := test.FindEphemeralPort()
	s.Nil(err)

	ringHosts := fmt.Sprintf("%v:%d", listenIP, port)
	serviceConfig := s.cfg.GetServiceConfig(common.ControllerServiceName)
	serviceConfig.SetListenAddress(listenIP)
	serviceConfig.SetPort(port)
	serviceConfig.SetWebsocketPort(wsPort)
	serviceConfig.SetRingHosts(ringHosts)

	serviceName := common.ControllerServiceName
	reporter := common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig())
	dClient := dconfig.NewDconfigClient(serviceConfig, common.ControllerServiceName)

	sVice := common.NewService(serviceName, uuid.New(), serviceConfig, common.NewUUIDResolver(s.mClient), common.NewHostHardwareInfoReader(s.mClient), reporter, dClient)
	//serviceConfig.SetRingHosts(
	mcp, tc := NewController(s.cfg, sVice, s.mClient, common.NewDummyZoneFailoverManager())
	s.mcp = mcp

	context := s.mcp.context
	s.mcp.Service.Start(tc)
	context.rpm = s.mockrpm
	context.hostID = s.mcp.GetHostUUID()
	context.clientFactory = s.mcp.GetClientFactory()
	context.channel = s.mcp.GetTChannel()
	context.eventPipeline = NewEventPipeline(context, nEventPipelineWorkers)
	context.eventPipeline.Start()
	context.failureDetector = NewDfdd(s.mcp.context, common.NewRealTimeSource())
	s.mcp.started = 1
}

func (s *McpSuite) stopController() {
	s.mcp.context.eventPipeline.Stop()
	s.mcp.Service.Stop()
}

func (s *McpSuite) SetupTest() {
	// test cluste for in/out
	s.cfg = common.SetupServerConfig(configure.NewCommonConfigure())
	s.mClient = s.cdb.GetClient()
	s.mockrpm = common.NewMockRingpopMonitor()

	s.inputPort = 5566
	for i := 0; i < 3; i++ {
		addr := inputAddr + ":" + strconv.Itoa(s.inputPort)
		s.mockrpm.Add(common.InputServiceName, uuid.New(), addr)
		s.inputPort++
	}

	s.outputPort = 7766
	for i := 0; i < 3; i++ {
		addr := outputAddr + ":" + strconv.Itoa(s.outputPort)
		s.mockrpm.Add(common.OutputServiceName, uuid.New(), addr)
		s.outputPort++
	}

	s.storePort = 9966
	for i := 0; i < 3; i++ {
		addr := storeAddr + ":" + strconv.Itoa(s.storePort)
		s.mockrpm.Add(common.StoreServiceName, uuid.New(), addr)
		s.storePort++
	}

	s.startController()

	s.mockReplicator = new(mockreplicator.MockTChanReplicator)
	mockClientFactory := new(mockcommon.MockClientFactory)
	mockClientFactory.On("GetReplicatorClient").Return(s.mockReplicator, nil)
	s.mcp.SetClientFactory(mockClientFactory)
}

func (s *McpSuite) TearDownTest() {
	s.stopController()
}

func (s *McpSuite) createMultiZoneDestination(name string, dstType shared.DestinationType, zones []string) (*shared.DestinationDescription, error) {
	return s.createDestinationImpl(name, dstType, false, zones)
}

func (s *McpSuite) createDestination(name string, dstType shared.DestinationType) (*shared.DestinationDescription, error) {
	return s.createDestinationImpl(name, dstType, false, nil)
}

func (s *McpSuite) createDLQDestination(name string, dstType shared.DestinationType) (*shared.DestinationDescription, error) {
	return s.createDestinationImpl(name, dstType, true, nil)
}

func (s *McpSuite) createDestinationImpl(name string, dstType shared.DestinationType, isDLQ bool, zones []string) (*shared.DestinationDescription, error) {
	var isMultiZone bool
	var zoneConfigs []*shared.DestinationZoneConfig
	if len(zones) > 0 {
		isMultiZone = true
		for _, zone := range zones {
			zoneConfigs = append(zoneConfigs, &shared.DestinationZoneConfig{
				Zone:         common.StringPtr(zone),
				AllowPublish: common.BoolPtr(true),
				AllowConsume: common.BoolPtr(true),
			})
		}
	}
	mReq := &shared.CreateDestinationRequest{
		Path: common.StringPtr(name),
		Type: common.InternalDestinationTypePtr(dstType),
		ConsumedMessagesRetention:   common.Int32Ptr(300),
		UnconsumedMessagesRetention: common.Int32Ptr(600),
		OwnerEmail:                  common.StringPtr("test@email.com"),
		IsMultiZone:                 common.BoolPtr(isMultiZone),
		ZoneConfigs:                 zoneConfigs,
	}

	if isDLQ {
		mReq.DLQConsumerGroupUUID = common.StringPtr(uuid.New()) // This is the 'magic'
	}

	return s.mClient.CreateDestination(nil, mReq)
}

func (s *McpSuite) listInputHostExtents(dstUUID string, inputHostUUID string) (*m.ListInputHostExtentsStatsResult_, error) {
	mReq := &m.ListInputHostExtentsStatsRequest{
		DestinationUUID: common.StringPtr(dstUUID),
		InputHostUUID:   common.StringPtr(inputHostUUID),
	}
	return s.mClient.ListInputHostExtentsStats(nil, mReq)
}

func (s *McpSuite) createConsumerGroup(dstPath, cgName string) (*shared.ConsumerGroupDescription, error) {
	return s.createConsumerGroupWithDLQ(dstPath, cgName)
}

func (s *McpSuite) createConsumerGroupWithDLQ(dstPath, cgName string) (*shared.ConsumerGroupDescription, error) {
	mReq := &shared.CreateConsumerGroupRequest{
		DestinationPath:   common.StringPtr(dstPath),
		ConsumerGroupName: common.StringPtr(cgName),
	}

	return s.mClient.CreateConsumerGroup(nil, mReq)
}

func (s *McpSuite) TestGetInputHostsInputValidation() {
	inputs := []string{"", "A", "Ab", strings.Repeat("a", 37)}
	for _, input := range inputs {
		_, err := s.mcp.GetInputHosts(nil, &c.GetInputHostsRequest{DestinationUUID: common.StringPtr(input)})
		s.Equal(ErrMalformedUUID, err, "Invalid validation failed for input %v", input)
	}
}

type testTimeSource struct {
	currTime time.Time
}

func (ts *testTimeSource) Now() time.Time {
	return ts.currTime
}

func (s *McpSuite) listExtents(dstUUID string) ([]*shared.ExtentStats, error) {
	return s.mcp.context.mm.ListExtentsByDstIDStatus(dstUUID, nil)
}

func (s *McpSuite) TestGetInputHostsOnDeletedDest() {
	dstPath := s.generateName("/cherami/mcp-test")
	dstDesc, err := s.createDestination(dstPath, shared.DestinationType_PLAIN)
	s.Nil(err, "Failed to create destination")
	dReq := &shared.DeleteDestinationRequest{Path: common.StringPtr(dstPath)}
	err = s.mClient.DeleteDestination(nil, dReq)
	s.Nil(err)
	_, err = s.mcp.GetInputHosts(nil, &c.GetInputHostsRequest{DestinationUUID: dstDesc.DestinationUUID})
	s.NotNil(err)
	_, ok := err.(*shared.EntityNotExistsError)
	s.True(ok, "Wrong error type returned")
}

func (s *McpSuite) TestGetInputHosts() {

	totalExtents := 0
	sealedExtents := 0

	dstPath := s.generateName("/cherami/mcp-test")
	dstDesc, err := s.createDestination(dstPath, shared.DestinationType_PLAIN)
	s.Nil(err, "Failed to create destination")
	s.Equal(common.UUIDStringLength, len(dstDesc.GetDestinationUUID()), "Invalid destination uuid")

	resp, err := s.mcp.GetInputHosts(nil, &c.GetInputHostsRequest{DestinationUUID: dstDesc.DestinationUUID})
	s.Nil(err)
	s.Equal(1, len(resp.InputHostIds), "GetInputHosts() auto-created more than one extent")

	totalExtents++

	inputHost, err := s.mockrpm.FindHostForAddr(common.InputServiceName, resp.InputHostIds[0])
	s.Nil(err, "GetInputHosts() returned invalid host %v", resp.InputHostIds[0])

	dstUUID := dstDesc.GetDestinationUUID()
	mResp, err := s.listInputHostExtents(dstUUID, inputHost.UUID)
	s.Nil(err, "GetInputHosts() failed to create new extent for new destination")
	s.Equal(1, len(mResp.GetExtentStatsList()), "GetInputHosts() created more than one extent")

	extent := mResp.ExtentStatsList[0].Extent
	s.Equal(dstDesc.GetDestinationUUID(), extent.GetDestinationUUID(), "Wrong destination UUID")
	s.Equal(inputHost.UUID, extent.GetInputHostUUID(), "Wrong in host uuid")
	s.Equal(3, len(extent.StoreUUIDs), "Wrong number of replicas for extent")

	for _, u := range extent.StoreUUIDs {
		_, err = s.mockrpm.ResolveUUID(common.StoreServiceName, u)
		s.Nil(err, "GetInputHosts() created an extent with un-known store uuid")
	}

	s.mcp.context.extentSeals.inProgress.PutIfNotExist(extent.GetExtentUUID(), Boolean(true))
	sealedExtents++

	resp, err = s.mcp.GetInputHosts(nil, &c.GetInputHostsRequest{DestinationUUID: dstDesc.DestinationUUID})
	s.Nil(err)
	s.Equal(1, len(resp.InputHostIds), "GetInputHosts() returned an extent that's in progress for seal")
	inputHost, err = s.mockrpm.FindHostForAddr(common.InputServiceName, resp.InputHostIds[0])
	s.Nil(err, "GetInputHosts() returned invalid host")

	totalExtents++

	// Kill in host and make sure unhealthy host is not returned
	s.mockrpm.Remove(common.InputServiceName, inputHost.UUID)
	success := !s.mcp.GetRingpopMonitor().IsHostHealthy(common.InputServiceName, inputHost.UUID)
	s.Equal(true, success, "Killing inputhost was not discovered through ringpop")
	sealedExtents++

	oldTTL := inputCacheTTL
	inputCacheTTL = int64(time.Hour)
	resp, err = s.mcp.GetInputHosts(nil, &c.GetInputHostsRequest{DestinationUUID: dstDesc.DestinationUUID})
	inputCacheTTL = oldTTL

	s.Nil(err)
	s.Equal(1, len(resp.InputHostIds), "GetInputHosts() auto-created more than one extent")

	newInputHost, err := s.mockrpm.FindHostForAddr(common.InputServiceName, resp.InputHostIds[0])
	s.Nil(err, "GetInputHosts() returned invalid host")

	mResp, err = s.listInputHostExtents(dstUUID, newInputHost.UUID)
	s.Nil(err, "GetInputHosts() failed to create new extent for new destination")
	s.Equal(1, len(mResp.GetExtentStatsList()), "GetInputHosts() created more than one extent")

	extent = mResp.ExtentStatsList[0].Extent
	s.Equal(dstDesc.GetDestinationUUID(), extent.GetDestinationUUID(), "Wrong destination UUID")
	s.Equal(newInputHost.UUID, extent.GetInputHostUUID(), "Wrong in host uuid")
	s.Equal(3, len(extent.StoreUUIDs), "Wrong number of replicas for extent")

	totalExtents++

	// Now make sure no new extents are returned when the lock is held
	ok := s.mcp.context.dstLock.TryLock(dstUUID, 0)
	s.True(ok, "Failed to acquire dst hash lock")
	expectedExtent := extent
	resp, err = s.mcp.GetInputHosts(nil, &c.GetInputHostsRequest{DestinationUUID: dstDesc.DestinationUUID})
	s.mcp.context.dstLock.Unlock(dstUUID)
	s.Nil(err)
	s.Equal(1, len(resp.InputHostIds), "GetInputHosts() returned more than one extent")

	newInputHost, err = s.mockrpm.FindHostForAddr(common.InputServiceName, resp.InputHostIds[0])
	s.Equal(expectedExtent.GetInputHostUUID(), newInputHost.UUID, "GetInputHosts() returned wront in host")
	mResp, err = s.listInputHostExtents(dstUUID, newInputHost.UUID)
	s.Nil(err, "GetInputHosts() failed to create new extent for new destination")
	s.Equal(1, len(mResp.GetExtentStatsList()), "GetInputHosts() returned more than one extent")
	s.Equal(extent.ExtentUUID, mResp.ExtentStatsList[0].Extent.ExtentUUID, "GetInputHosts() returned wrong extent uuid")

	// make sure we create no more extents than needed
	extentStats, err := s.listExtents(dstUUID)
	s.Equal(totalExtents, len(extentStats), "Wrong number of extents for destination")
	for i := 0; i < minOpenExtentsForDst(s.mcp.context, `/`, dstTypePlain); i++ {
		resp, err = s.mcp.GetInputHosts(nil, &c.GetInputHostsRequest{DestinationUUID: dstDesc.DestinationUUID})
		s.Nil(err)
	}
	extentStats, err = s.listExtents(dstUUID)
	s.Equal(sealedExtents+minOpenExtentsForDst(s.mcp.context, `/`, dstTypePlain), len(extentStats), "Wrong number of extents for destination")

	// now verify we serve results from cache until ttl
	// seal the extents and verify we still get them
	timeSource := &testTimeSource{currTime: time.Now()}
	s.mcp.context.timeSource = timeSource
	for _, stat := range extentStats {
		ext := stat.GetExtent()
		err := s.mcp.context.mm.SealExtent(dstUUID, ext.GetExtentUUID())
		s.Nil(err, "Failed to seal extent")
	}

	resp, err = s.mcp.GetInputHosts(nil, &c.GetInputHostsRequest{DestinationUUID: dstDesc.DestinationUUID})
	s.Nil(err, "GetInputHosts() failed to serve result from cache")
	extentStats, err = s.listExtents(dstUUID)
	s.Equal(sealedExtents+minOpenExtentsForDst(s.mcp.context, `/`, dstTypePlain), len(extentStats), "Wrong number of extents for destination")

	// now advance clock and expire the cache
	timeSource.currTime = time.Now().Add(time.Hour).Add(time.Second)
	resp, err = s.mcp.GetInputHosts(nil, &c.GetInputHostsRequest{DestinationUUID: dstDesc.DestinationUUID})
	s.Nil(err, "GetInputHosts() failed to return non-empty result")
	s.Equal(1, len(resp.GetInputHostIds()), "GetInputHosts() must return only one value")
	extentStats, err = s.listExtents(dstUUID)
	s.Equal(1+sealedExtents+minOpenExtentsForDst(s.mcp.context, `/`, dstTypePlain), len(extentStats), "Wrong number of extents for destination")
}

func (s *McpSuite) TestGetInputHostsForKafkaDest() {

	dstPath := s.generateName("/cherami/mcp-test")
	dstDesc, err := s.createDestination(dstPath, shared.DestinationType_KAFKA)
	s.Nil(err, "Failed to create destination")

	_, err = s.mcp.GetInputHosts(nil, &c.GetInputHostsRequest{DestinationUUID: dstDesc.DestinationUUID})
	s.NotNil(err, "GetInputHosts should fail for Kafka destination")
	s.Equal(ErrPublishToKafkaDestination, err, "Expected ErrPublishToKafkaDestination")
}

func (s *McpSuite) TestGetInputHostsForReceiveOnlyDest() {

	dstPath := s.generateName("/cherami/mcp-test")
	dstDesc, err := s.createDestination(dstPath, shared.DestinationType_PLAIN)
	s.Nil(err, "Failed to create destination")

	updateReq := &shared.UpdateDestinationRequest{
		DestinationUUID: dstDesc.DestinationUUID,
		Status:          shared.DestinationStatusPtr(shared.DestinationStatus_RECEIVEONLY),
	}

	_, err = s.mClient.UpdateDestination(nil, updateReq)
	s.Nil(err, "Failed to update destination to receive-only")

	_, err = s.mcp.GetInputHosts(nil, &c.GetInputHostsRequest{DestinationUUID: dstDesc.DestinationUUID})
	s.NotNil(err, "GetInputHosts should fail for receive-only destination")

	s.Equal(ErrPublishToReceiveOnlyDestination, err, "Expected ErrPublishToReceiveOnlyDestination")
}

func (s *McpSuite) TestGetOutputHostsMaxOpenExtentsLimit() {

	dstTypes := []shared.DestinationType{shared.DestinationType_PLAIN, shared.DestinationType_TIMER, shared.DestinationType_KAFKA}

	for _, dstType := range dstTypes {

		path := s.generateName("/cherami/mcp-test-" + dstType.String())

		dstDesc, err := s.createDestination(path, dstType)
		s.Nil(err, "Failed to create destination")
		s.Equal(common.UUIDStringLength, len(dstDesc.GetDestinationUUID()), "Invalid destination uuid")

		cgName := s.generateName("/cherami/mcp-test-cg")
		cgDesc, err := s.createConsumerGroup(path, cgName)
		s.Nil(err, "Failed to create consumer group")

		dstUUID := dstDesc.GetDestinationUUID()
		cgUUID := cgDesc.GetConsumerGroupUUID()

		storehosts, _ := s.mcp.context.placement.PickStoreHosts(3)
		storeids := make([]string, 3)
		for i := 0; i < 3; i++ {
			storeids[i] = storehosts[i].UUID
		}
		inhost, _ := s.mcp.context.placement.PickInputHost(storehosts)

		extents := make(map[string]struct{})

		maxExtents := maxExtentsToConsumeForDst(s.mcp.context, `/`, `/`, getDstType(dstDesc), nil)

		for i := 0; i < maxExtents+1; i++ {
			extentUUID := uuid.New()
			_, err = s.mcp.context.mm.CreateExtent(dstUUID, extentUUID, inhost.UUID, storeids)
			s.Nil(err, "Failed to create new extent")

			// for kafka destinations, make these DLQ extents
			if dstType == shared.DestinationType_KAFKA {
				err = s.mcp.context.mm.SealExtent(dstUUID, extentUUID)
				s.Nil(err, fmt.Sprintf("SealExtent failed: %v", err))
				err = s.mcp.context.mm.MoveExtent(dstUUID, dstUUID, extentUUID, cgUUID)
				s.Nil(err, fmt.Sprintf("MoveExtent failed: %v", err))
			}

			extents[extentUUID] = struct{}{}
		}

		for i := 0; i < maxExtents+2; i++ {
			_, err := s.mcp.GetOutputHosts(nil, &c.GetOutputHostsRequest{DestinationUUID: common.StringPtr(dstUUID), ConsumerGroupUUID: common.StringPtr(cgUUID)})
			s.Nil(err, "GetOutputHosts() failed on a new consumer group")
		}

		// assert that we don't assign more than maxExtents
		// extents to the consumer group at any given point of time
		cge, err := s.mClient.ReadConsumerGroupExtents(nil, &shared.ReadConsumerGroupExtentsRequest{DestinationUUID: common.StringPtr(dstUUID), ConsumerGroupUUID: common.StringPtr(cgUUID), MaxResults: common.Int32Ptr(100)})
		s.Nil(err, "Failed to find consumer group extent entry for outputhost")

		switch dstType {
		case shared.DestinationType_TIMER:
			s.Equal(maxExtents, len(cge.GetExtents()), "Wrong number of extents for consumer group dst=%v", path)

		case shared.DestinationType_KAFKA:
			s.Equal(maxExtents, len(cge.GetExtents()), "Wrong number of extents for consumer group dst=%v", path)

			var phantomExtents, dlqExtents int
			for _, cgx := range cge.GetExtents() {

				if _, ok := extents[cgx.GetExtentUUID()]; ok {
					dlqExtents++
				} else {
					s.True(common.AreKafkaPhantomStores(cgx.GetStoreUUIDs()), "expected phantom stores")
					phantomExtents++
				}
			}

			s.Equal(numKafkaExtentsForDstKafka, phantomExtents, "Wrong number of kafka phantom extents for consumer group dst=%v", path)
		}
	}
}

func (s *McpSuite) TestGetOutputHostsOnDeletedDest() {

	dstPath := s.generateName("/cherami/mcp-test")
	dstDesc, err := s.createDestination(dstPath, shared.DestinationType_PLAIN)
	s.Nil(err, "Failed to create destination")

	cgName := s.generateName("/cherami/mcp-test-cg")
	cgDesc, err := s.createConsumerGroup(dstPath, cgName)
	s.Nil(err, "Failed to create consumer group")

	dReq := &shared.DeleteDestinationRequest{Path: common.StringPtr(dstPath)}
	err = s.mClient.DeleteDestination(nil, dReq)
	s.Nil(err)

	dstUUID := dstDesc.GetDestinationUUID()
	cgUUID := cgDesc.GetConsumerGroupUUID()
	_, err = s.mcp.GetOutputHosts(nil, &c.GetOutputHostsRequest{DestinationUUID: common.StringPtr(dstUUID), ConsumerGroupUUID: common.StringPtr(cgUUID)})
	s.NotNil(err)
	_, ok := err.(*shared.EntityNotExistsError)
	s.True(ok, "Wrong error type returned")
}

func (s *McpSuite) TestGetOutputHostsOnDeletedCG() {

	dstPath := s.generateName("/cherami/mcp-test")
	dstDesc, err := s.createDestination(dstPath, shared.DestinationType_PLAIN)
	s.Nil(err, "Failed to create destination")

	cgName := s.generateName("/cherami/mcp-test-cg")
	cgDesc, err := s.createConsumerGroup(dstPath, cgName)
	s.Nil(err, "Failed to create consumer group")

	dReq := &shared.DeleteConsumerGroupRequest{
		DestinationPath:   common.StringPtr(dstPath),
		ConsumerGroupName: common.StringPtr(cgName),
	}
	err = s.mClient.DeleteConsumerGroup(nil, dReq)
	s.Nil(err)

	dstUUID := dstDesc.GetDestinationUUID()
	cgUUID := cgDesc.GetConsumerGroupUUID()
	_, err = s.mcp.GetOutputHosts(nil, &c.GetOutputHostsRequest{DestinationUUID: common.StringPtr(dstUUID), ConsumerGroupUUID: common.StringPtr(cgUUID)})
	s.NotNil(err)
	_, ok := err.(*shared.EntityNotExistsError)
	s.True(ok, "Wrong error type returned")
}

func (s *McpSuite) TestGetOutputHosts() {

	path := s.generateName("/cherami/mcp-test")
	dstDesc, err := s.createDestination(path, shared.DestinationType_PLAIN)
	s.Nil(err, "Failed to create destination")
	s.Equal(common.UUIDStringLength, len(dstDesc.GetDestinationUUID()), "Invalid destination uuid")

	cgName := s.generateName("/cherami/mcp-test-cg")
	cgDesc, err := s.createConsumerGroup(path, cgName)
	s.Nil(err, "Failed to create consumer group")

	dstUUID := dstDesc.GetDestinationUUID()
	cgUUID := cgDesc.GetConsumerGroupUUID()
	extents := make(map[string]bool)

	resp, err := s.mcp.GetOutputHosts(nil, &c.GetOutputHostsRequest{DestinationUUID: common.StringPtr(dstUUID), ConsumerGroupUUID: common.StringPtr(cgUUID)})
	s.Nil(err, fmt.Sprintf("GetOutputHosts() failed on a new consumer group: %v", err))
	s.Equal(1, len(resp.GetOutputHostIds()), "GetOutputHosts() returned more than one out host")

	outputHost, err := s.mockrpm.FindHostForAddr(common.OutputServiceName, resp.OutputHostIds[0])
	s.Nil(err, "GetOutputHosts() returned invalid host")

	cge, err := s.mClient.ReadConsumerGroupExtents(nil,
		&shared.ReadConsumerGroupExtentsRequest{DestinationUUID: common.StringPtr(dstUUID), ConsumerGroupUUID: common.StringPtr(cgUUID), OutputHostUUID: common.StringPtr(outputHost.UUID), MaxResults: common.Int32Ptr(10)})
	s.Nil(err, "Failed to find consumer group extent entry for outputhost")
	s.Equal(1, len(cge.GetExtents()), "GetOutputHosts() auto-created more than one extent")

	extentUUID := cge.Extents[0].GetExtentUUID()
	extents[extentUUID] = true
	_, err = s.mClient.ReadExtentStats(nil, &m.ReadExtentStatsRequest{DestinationUUID: common.StringPtr(dstUUID), ExtentUUID: common.StringPtr(extentUUID)})
	s.Nil(err, "Failed to find extent created by GetOutputHosts()")

	// check if GetOutputHosts() correctly returns an existing cge
	resp, err = s.mcp.GetOutputHosts(nil, &c.GetOutputHostsRequest{DestinationUUID: common.StringPtr(dstUUID), ConsumerGroupUUID: common.StringPtr(cgUUID)})
	s.Nil(err, "GetOutputHosts() failed")
	s.Equal(1, len(resp.GetOutputHostIds()), "GetOutputHosts() returned more than one out host")

	outputHost, err = s.mockrpm.FindHostForAddr(common.OutputServiceName, resp.OutputHostIds[0])
	s.Nil(err, "GetOutputHosts() returned invalid host")
	s.Equal(cge.Extents[0].GetOutputHostUUID(), outputHost.UUID, "Wrong out host for consumer group")

	// mark the cge as consumed and make sure a new extent
	// gets created
	mReq := &shared.SetAckOffsetRequest{
		ConsumerGroupUUID:  common.StringPtr(cgUUID),
		ExtentUUID:         common.StringPtr(extentUUID),
		OutputHostUUID:     common.StringPtr(outputHost.UUID),
		Status:             common.MetadataConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_CONSUMED),
		ConnectedStoreUUID: common.StringPtr(uuid.New()),
	}
	err = s.mClient.SetAckOffset(nil, mReq)
	s.Nil(err, "Failed to update ack offset for consumer group extent")

	resp, err = s.mcp.GetOutputHosts(nil, &c.GetOutputHostsRequest{DestinationUUID: common.StringPtr(dstUUID), ConsumerGroupUUID: common.StringPtr(cgUUID)})
	s.Nil(err, "GetOutputHosts() failed on a new consumer group")
	s.Equal(1, len(resp.GetOutputHostIds()), "GetOutputHosts() returned more than one out host")

	cge, err = s.mClient.ReadConsumerGroupExtents(nil,
		&shared.ReadConsumerGroupExtentsRequest{DestinationUUID: common.StringPtr(dstUUID), ConsumerGroupUUID: common.StringPtr(cgUUID), OutputHostUUID: common.StringPtr(outputHost.UUID), MaxResults: common.Int32Ptr(10)})
	s.Nil(err, "Failed to find consumer group extent entry for outputhost")
	s.Equal(2, len(cge.GetExtents()), "Wrong number of extents for consumer group")
	for _, cge := range cge.Extents {
		extents[cge.GetExtentUUID()] = true
	}

	// add a new extent and check if GetOutputHosts() returns multiple out hosts
	storehosts, _ := s.mcp.context.placement.PickStoreHosts(3)
	storeids := make([]string, 3)
	for i := 0; i < 3; i++ {
		storeids[i] = storehosts[i].UUID
	}
	inhost, _ := s.mcp.context.placement.PickInputHost(storehosts)
	extentUUID = uuid.New()
	_, err = s.mcp.context.mm.CreateExtent(dstUUID, extentUUID, inhost.UUID, storeids)
	s.Nil(err, "Failed to create new extent")

	extents[extentUUID] = true
	resp, err = s.mcp.GetOutputHosts(nil, &c.GetOutputHostsRequest{DestinationUUID: common.StringPtr(dstUUID), ConsumerGroupUUID: common.StringPtr(cgUUID)})
	s.Nil(err, "GetOutputHosts() failed")

	cge, err = s.mClient.ReadConsumerGroupExtents(nil, &shared.ReadConsumerGroupExtentsRequest{DestinationUUID: common.StringPtr(dstUUID), ConsumerGroupUUID: common.StringPtr(cgUUID), MaxResults: common.Int32Ptr(10)})
	s.Nil(err, "Failed to find consumer group extent entry for outputhost")
	s.Equal(3, len(cge.GetExtents()), "Wrong number of extents for consumer group")

	toKill := ""
	for _, e := range cge.GetExtents() {
		_, ok := extents[e.GetExtentUUID()]
		s.True(ok, "Unknown extent for consumer group")
		ok = false
		for _, h := range resp.GetOutputHostIds() {
			oh, _ := s.mockrpm.FindHostForAddr(common.OutputServiceName, h)
			toKill = oh.UUID
			if strings.Compare(oh.UUID, e.GetOutputHostUUID()) == 0 {
				ok = true
				break
			}
		}
		s.True(ok, "Unknown out host %v found in consumer group extent table", e.GetOutputHostUUID())
	}

	s.mockrpm.Remove(common.OutputServiceName, toKill)
	success := !s.mcp.GetRingpopMonitor().IsHostHealthy(common.OutputServiceName, toKill)
	s.Equal(true, success, "Killing outputhost was not discovered through ringpop")

	oldTTL := outputCacheTTL
	outputCacheTTL = time.Hour
	resp, err = s.mcp.GetOutputHosts(nil, &c.GetOutputHostsRequest{DestinationUUID: common.StringPtr(dstUUID), ConsumerGroupUUID: common.StringPtr(cgUUID)})
	outputCacheTTL = oldTTL

	s.Nil(err, "GetOutputHosts() failed")

	cge, err = s.mClient.ReadConsumerGroupExtents(nil, &shared.ReadConsumerGroupExtentsRequest{DestinationUUID: common.StringPtr(dstUUID), ConsumerGroupUUID: common.StringPtr(cgUUID), MaxResults: common.Int32Ptr(10)})
	s.Nil(err, "Failed to find consumer group extent entry for outputhost")
	s.Equal(3, len(cge.GetExtents()), "Wrong number of extents for consumer group")

	for _, e := range cge.GetExtents() {
		_, ok := extents[e.GetExtentUUID()]
		s.True(ok, "Unknown extent for consumer group")
		ok = false
		if e.GetStatus() != shared.ConsumerGroupExtentStatus_OPEN {
			continue
		}
		for _, h := range resp.GetOutputHostIds() {
			oh, er := s.mockrpm.FindHostForAddr(common.OutputServiceName, h)
			s.Nil(er, "GetOutputHosts() failed to reassign unhealthy output host")
			if strings.Compare(oh.UUID, e.GetOutputHostUUID()) == 0 {
				ok = true
				break
			}
		}
		s.True(ok, "Unknown out host %v found in consumer group extent table", e.GetOutputHostUUID())
	}
}

func (s *McpSuite) createDlqExtent(dstUUID, cgUUID string) (extentUUID string) {

	storehosts, _ := s.mcp.context.placement.PickStoreHosts(3)
	storeids := make([]string, 3)
	for i := 0; i < 3; i++ {
		storeids[i] = storehosts[i].UUID
	}
	inhost, _ := s.mcp.context.placement.PickInputHost(storehosts)

	extentUUID = uuid.New()
	_, err := s.mcp.context.mm.CreateExtent(dstUUID, extentUUID, inhost.UUID, storeids)
	s.Nil(err, "Failed to create new extent")

	// for kafka destinations, make these DLQ extents
	err = s.mcp.context.mm.SealExtent(dstUUID, extentUUID)
	s.Nil(err, fmt.Sprintf("SealExtent failed: %v", err))
	err = s.mcp.context.mm.MoveExtent(dstUUID, dstUUID, extentUUID, cgUUID)
	s.Nil(err, fmt.Sprintf("MoveExtent failed: %v", err))

	return
}

// simple set of strings
type stringSet struct {
	m map[string]struct{}
}

func newStringSet() *stringSet {
	return &stringSet{m: make(map[string]struct{})}
}

func (t *stringSet) clear() {
	t.m = make(map[string]struct{})
}

func (t *stringSet) empty() bool {
	return len(t.m) == 0
}

func (t *stringSet) insert(key string) {
	t.m[key] = struct{}{}
}

func (t *stringSet) contains(key string) bool {
	_, ok := t.m[key]
	return ok
}

func (t *stringSet) remove(key string) {
	delete(t.m, key)
}

func (t *stringSet) keys() map[string]struct{} {
	return t.m
}

func (t *stringSet) equals(o *stringSet) bool {

	if len(o.m) != len(t.m) {
		return false
	}

	for k := range t.m {
		_, ok := o.m[k]
		if !ok {
			return false
		}
	}

	return true
}

func (t *stringSet) subset(o *stringSet) bool {

	if len(o.m) < len(t.m) {
		return false
	}

	for k := range t.m {
		_, ok := o.m[k]
		if !ok {
			return false
		}
	}

	return true
}

func (s *McpSuite) TestGetOutputHostsKafka() {

	path := s.generateName("/cherami/mcp-test-kafka")
	dstDesc, err := s.createDestination(path, shared.DestinationType_KAFKA)
	s.Nil(err, "Failed to create destination")
	s.Equal(common.UUIDStringLength, len(dstDesc.GetDestinationUUID()), "Invalid destination uuid")

	cgName := s.generateName("/cherami/mcp-test-kafka-cg")
	cgDesc, err := s.createConsumerGroup(path, cgName)
	s.Nil(err, "Failed to create consumer group")

	dstUUID := dstDesc.GetDestinationUUID()
	cgUUID := cgDesc.GetConsumerGroupUUID()

	cgExtents := make(map[string]*shared.ConsumerGroupExtent)
	cgOutputHosts := newStringSet()
	outputHosts := newStringSet()
	dlqExtents := newStringSet()

	originOutputCacheTTL := outputCacheTTL
	outputCacheTTL = time.Microsecond

	// verify GetOutputHosts returns valid outputhosts
	outputHosts.clear()
	resp, err := s.mcp.GetOutputHosts(nil, &c.GetOutputHostsRequest{DestinationUUID: common.StringPtr(dstUUID), ConsumerGroupUUID: common.StringPtr(cgUUID)})
	s.Nil(err, fmt.Sprintf("GetOutputHosts() failed on a new consumer group: %v", err))
	s.True(len(resp.GetOutputHostIds()) <= numKafkaExtentsForDstKafka, "GetOutputHosts() returned more than expected number of outhosts")

	for _, hostID := range resp.GetOutputHostIds() {
		host, err := s.mockrpm.FindHostForAddr(common.OutputServiceName, hostID)
		s.Nil(err, "GetOutputHosts() returned invalid host")
		outputHosts.insert(host.UUID)
	}

	// verify GetOutputHosts created phantom extents
	cgOutputHosts.clear()
	cge, err := s.mClient.ReadConsumerGroupExtents(nil,
		&shared.ReadConsumerGroupExtentsRequest{
			DestinationUUID:   common.StringPtr(dstUUID),
			ConsumerGroupUUID: common.StringPtr(cgUUID),
			MaxResults:        common.Int32Ptr(10),
		})
	s.Nil(err, fmt.Sprintf("ReadConsumerGroupExtents failed: %v", err))
	s.Equal(numKafkaExtentsForDstKafka, len(cge.GetExtents()), "GetOutputHosts() did not auto-create more than one extent")

	for _, cgx := range cge.GetExtents() {
		extentUUID := cgx.GetExtentUUID()
		cgExtents[extentUUID] = cgx
		cgOutputHosts.insert(cgx.GetOutputHostUUID())
		_, err = s.mClient.ReadExtentStats(nil, &m.ReadExtentStatsRequest{DestinationUUID: common.StringPtr(dstUUID), ExtentUUID: common.StringPtr(extentUUID)})
		s.Nil(err, "Failed to find extent created by GetOutputHosts()")

		// validate all returned extents are 'phantom' extents
		s.True(common.AreKafkaPhantomStores(cgx.GetStoreUUIDs()), "expected phantom stores")
	}

	s.True(outputHosts.subset(cgOutputHosts), "invalid outputhost returned")

	// - create one DLQ extent
	dlqExtents.insert(s.createDlqExtent(dstUUID, cgUUID))

	// - do GetOutputHosts, that triggers assignment of the newly available DLQ extent
	outputHosts.clear()
	resp, err = s.mcp.GetOutputHosts(nil, &c.GetOutputHostsRequest{DestinationUUID: common.StringPtr(dstUUID), ConsumerGroupUUID: common.StringPtr(cgUUID)})
	s.Nil(err, fmt.Sprintf("GetOutputHosts() failed on a new consumer group: %v", err))
	s.True(len(resp.GetOutputHostIds()) <= maxExtentsToConsumeForDstKafka, "GetOutputHosts() returned more than expected number of outhosts")

	for _, hostID := range resp.GetOutputHostIds() {
		host, err := s.mockrpm.FindHostForAddr(common.OutputServiceName, hostID)
		s.Nil(err, "GetOutputHosts() returned invalid host")
		outputHosts.insert(host.UUID)
	}

	// - do ReadConsumerGroupExtents to update map of assigned cg-extents
	cgOutputHosts.clear()
	cge, err = s.mClient.ReadConsumerGroupExtents(nil,
		&shared.ReadConsumerGroupExtentsRequest{
			DestinationUUID:   common.StringPtr(dstUUID),
			ConsumerGroupUUID: common.StringPtr(cgUUID),
			MaxResults:        common.Int32Ptr(10),
		})
	s.Nil(err, fmt.Sprintf("ReadConsumerGroupExtents failed: %v", err))
	s.Equal(numKafkaExtentsForDstKafka+1, len(cge.GetExtents()), "GetOutputHosts() did not assign all possible extents")

	var nPhantom, nDlq int

	nPhantom, nDlq = 0, 0
	for _, cgx := range cge.GetExtents() {
		extentUUID := cgx.GetExtentUUID()
		cgExtents[extentUUID] = cgx
		cgOutputHosts.insert(cgx.GetOutputHostUUID())
		_, err = s.mClient.ReadExtentStats(nil, &m.ReadExtentStatsRequest{DestinationUUID: common.StringPtr(dstUUID), ExtentUUID: common.StringPtr(extentUUID)})
		s.Nil(err, "Failed to find extent created by GetOutputHosts()")

		// count extent by type
		if dlqExtents.contains(extentUUID) {
			nDlq++
		} else {
			s.True(common.AreKafkaPhantomStores(cgx.GetStoreUUIDs()), "expected phantom stores")
			nPhantom++
		}
	}

	s.Equal(1, nDlq, "Did not assign the one DLQ extent")
	s.Equal(numKafkaExtentsForDstKafka, nPhantom, "Did not create/assign expected phanom extents")

	s.True(outputHosts.subset(cgOutputHosts), "invalid outputhost returned")

	// - create a few more DLQ extents (totally one more than maxDlqExtentsForDstKafka)
	for i := 0; i < maxDlqExtentsForDstKafka; i++ {
		dlqExtents.insert(s.createDlqExtent(dstUUID, cgUUID))
	}

	// - do GetOutputHosts, that triggers assignment of the newly available DLQ extent
	outputHosts.clear()
	resp, err = s.mcp.GetOutputHosts(nil, &c.GetOutputHostsRequest{DestinationUUID: common.StringPtr(dstUUID), ConsumerGroupUUID: common.StringPtr(cgUUID)})
	s.Nil(err, fmt.Sprintf("GetOutputHosts() failed on a new consumer group: %v", err))
	s.True(len(resp.GetOutputHostIds()) <= maxExtentsToConsumeForDstKafka, "GetOutputHosts() returned more than expected number of outhosts")

	for _, hostID := range resp.GetOutputHostIds() {
		host, err := s.mockrpm.FindHostForAddr(common.OutputServiceName, hostID)
		s.Nil(err, "GetOutputHosts() returned invalid host")
		outputHosts.insert(host.UUID)
	}

	// - do ReadConsumerGroupExtents to update map of assigned cg-extents
	cgOutputHosts.clear()
	cge, err = s.mClient.ReadConsumerGroupExtents(nil,
		&shared.ReadConsumerGroupExtentsRequest{
			DestinationUUID:   common.StringPtr(dstUUID),
			ConsumerGroupUUID: common.StringPtr(cgUUID),
			MaxResults:        common.Int32Ptr(10),
		})
	s.Nil(err, fmt.Sprintf("ReadConsumerGroupExtents failed: %v", err))
	s.Equal(maxExtentsToConsumeForDstKafka, len(cge.GetExtents()), "GetOutputHosts() did not assign all possible extents")

	nPhantom, nDlq = 0, 0
	for _, cgx := range cge.GetExtents() {
		extentUUID := cgx.GetExtentUUID()
		cgExtents[extentUUID] = cgx
		cgOutputHosts.insert(cgx.GetOutputHostUUID())
		_, err = s.mClient.ReadExtentStats(nil, &m.ReadExtentStatsRequest{DestinationUUID: common.StringPtr(dstUUID), ExtentUUID: common.StringPtr(extentUUID)})
		s.Nil(err, "Failed to find extent created by GetOutputHosts()")

		// count extent by type
		if dlqExtents.contains(extentUUID) {
			nDlq++
		} else {
			s.True(common.AreKafkaPhantomStores(cgx.GetStoreUUIDs()), "expected phantom stores")
			nPhantom++
		}
	}

	s.Equal(numKafkaExtentsForDstKafka, nPhantom, "Did not create/assign expected phanom extents")
	s.Equal(maxDlqExtentsForDstKafka, nDlq, "Did not assign all possible DLQ extents")

	// pick a random DLQ extent and mark it consumed; this should trigger a new extent to be placed on a GetOutputHosts
	var pickDlqExtent0 string

	for extentUUID := range dlqExtents.keys() {
		pickDlqExtent0 = extentUUID
		// ensure the selected DLQ-extent was selected to be consumed from
		if _, ok := cgExtents[pickDlqExtent0]; ok {
			break
		}
	}

	err = s.mClient.SetAckOffset(nil, &shared.SetAckOffsetRequest{
		ConsumerGroupUUID:  common.StringPtr(cgUUID),
		ExtentUUID:         common.StringPtr(pickDlqExtent0),
		OutputHostUUID:     cgExtents[pickDlqExtent0].OutputHostUUID,
		Status:             common.MetadataConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_CONSUMED),
		ConnectedStoreUUID: common.StringPtr(cgExtents[pickDlqExtent0].StoreUUIDs[0]),
	})
	s.Nil(err, "Failed to update ack offset for consumer group extent")

	// - do GetOutputHosts, that should trigger assignment of a new DLQ extent
	outputHosts.clear()
	resp, err = s.mcp.GetOutputHosts(nil, &c.GetOutputHostsRequest{DestinationUUID: common.StringPtr(dstUUID), ConsumerGroupUUID: common.StringPtr(cgUUID)})
	s.Nil(err, fmt.Sprintf("GetOutputHosts() failed on a new consumer group: %v", err))
	s.True(len(resp.GetOutputHostIds()) <= maxExtentsToConsumeForDstKafka, "GetOutputHosts() returned more than expected number of outhosts")

	for _, hostID := range resp.GetOutputHostIds() {
		host, err := s.mockrpm.FindHostForAddr(common.OutputServiceName, hostID)
		s.Nil(err, "GetOutputHosts() returned invalid host")
		outputHosts.insert(host.UUID)
	}

	// - do ReadConsumerGroupExtents to update map of assigned cg-extents
	cgOutputHosts.clear()
	cge, err = s.mClient.ReadConsumerGroupExtents(nil,
		&shared.ReadConsumerGroupExtentsRequest{
			DestinationUUID:   common.StringPtr(dstUUID),
			ConsumerGroupUUID: common.StringPtr(cgUUID),
			MaxResults:        common.Int32Ptr(10),
		})
	s.Nil(err, fmt.Sprintf("ReadConsumerGroupExtents failed: %v", err))
	s.Equal(maxExtentsToConsumeForDstKafka, len(cge.GetExtents()), "GetOutputHosts() did not assign all possible extents")

	nPhantom, nDlq = 0, 0
	for _, cgx := range cge.GetExtents() {
		extentUUID := cgx.GetExtentUUID()
		cgExtents[extentUUID] = cgx
		cgOutputHosts.insert(cgx.GetOutputHostUUID())
		_, err = s.mClient.ReadExtentStats(nil, &m.ReadExtentStatsRequest{DestinationUUID: common.StringPtr(dstUUID), ExtentUUID: common.StringPtr(extentUUID)})
		s.Nil(err, "Failed to find extent created by GetOutputHosts()")

		// count extent by type
		if dlqExtents.contains(extentUUID) {
			nDlq++
		} else {
			s.True(common.AreKafkaPhantomStores(cgx.GetStoreUUIDs()), "expected phantom stores")
			nPhantom++
		}
	}

	s.Equal(numKafkaExtentsForDstKafka, nPhantom, "Did not create/assign expected phanom extents")
	s.Equal(maxDlqExtentsForDstKafka, nDlq, "Did not assign all possible DLQ extents")
	s.True(outputHosts.subset(cgOutputHosts), "invalid outputhost returned")

	outputCacheTTL = originOutputCacheTTL
}

func (s *McpSuite) TestMultiZoneDestCUD() {
	/*********TEST CREATION*****************/
	var destUUID string
	destPath := s.generateName("/cherami/mcp-test")
	var zoneConfigs []*shared.DestinationZoneConfig
	for _, zone := range []string{`zone1`, `zone2`} {
		zoneConfigs = append(zoneConfigs, &shared.DestinationZoneConfig{
			Zone:         common.StringPtr(zone),
			AllowPublish: common.BoolPtr(true),
			AllowConsume: common.BoolPtr(true),
		})
	}
	createReq := &shared.CreateDestinationRequest{
		Path:        common.StringPtr(destPath),
		IsMultiZone: common.BoolPtr(true),
		ZoneConfigs: zoneConfigs,
	}

	// verify remote operation
	s.mockReplicator.On("CreateRemoteDestinationUUID", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		createRemoteReq := args.Get(1).(*shared.CreateDestinationUUIDRequest)
		destUUID = createRemoteReq.GetDestinationUUID()
		s.True(createRemoteReq.IsSetRequest())
		s.True(createRemoteReq.GetRequest().GetIsMultiZone())
		s.Equal(destPath, createRemoteReq.GetRequest().GetPath())
	})

	// issue create request
	destDesc, err := s.mcp.CreateDestination(nil, createReq)
	s.NoError(err)
	s.NotNil(destDesc)
	s.Equal(destUUID, destDesc.GetDestinationUUID())
	s.Equal(destPath, destDesc.GetPath())
	s.True(destDesc.GetIsMultiZone())

	// verify local operation
	destDesc, err = s.mClient.ReadDestination(nil, &shared.ReadDestinationRequest{Path: common.StringPtr(destPath)})
	s.NoError(err)
	s.NotNil(destDesc)
	s.Equal(destUUID, destDesc.GetDestinationUUID())
	s.Equal(destPath, destDesc.GetPath())
	s.True(destDesc.GetIsMultiZone())

	/*********TEST UPDATE*****************/
	newOwnerEmail := "updated@email.com"
	updateReq := &shared.UpdateDestinationRequest{
		DestinationUUID: common.StringPtr(destUUID),
		OwnerEmail:      common.StringPtr(newOwnerEmail),
	}

	// verify remote operation
	s.mockReplicator.On("UpdateRemoteDestination", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		updateRemoteReq := args.Get(1).(*shared.UpdateDestinationRequest)
		s.Equal(destUUID, updateRemoteReq.GetDestinationUUID())
		s.Equal(newOwnerEmail, updateRemoteReq.GetOwnerEmail())
	})

	// issue request
	destDesc, err = s.mcp.UpdateDestination(nil, updateReq)
	s.NoError(err)
	s.NotNil(destDesc)
	s.Equal(destUUID, destDesc.GetDestinationUUID())
	s.Equal(destPath, destDesc.GetPath())
	s.Equal(newOwnerEmail, destDesc.GetOwnerEmail())
	s.True(destDesc.GetIsMultiZone())

	// verify local operation
	destDesc, err = s.mClient.ReadDestination(nil, &shared.ReadDestinationRequest{Path: common.StringPtr(destPath)})
	s.NoError(err)
	s.NotNil(destDesc)
	s.Equal(destUUID, destDesc.GetDestinationUUID())
	s.Equal(destPath, destDesc.GetPath())
	s.Equal(newOwnerEmail, destDesc.GetOwnerEmail())
	s.True(destDesc.GetIsMultiZone())

	/*********TEST DELETION*****************/
	deleteReq := &shared.DeleteDestinationRequest{
		Path: common.StringPtr(destPath),
	}

	// verify remote operation
	s.mockReplicator.On("DeleteRemoteDestination", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		deleteRemoteReq := args.Get(1).(*shared.DeleteDestinationRequest)
		s.Equal(destPath, deleteRemoteReq.GetPath())
	})

	// issue request
	err = s.mcp.DeleteDestination(nil, deleteReq)
	s.NoError(err)

	// verify local operation
	destDesc, err = s.mClient.ReadDestination(nil, &shared.ReadDestinationRequest{Path: common.StringPtr(destPath)})
	s.Error(err)
	s.Nil(destDesc)
	assert.IsType(s.T(), &shared.EntityNotExistsError{}, err)
}

func (s *McpSuite) TestCreateConsumerGroup() {
	destPath := s.generateName("/cherami/mcp-test")
	_, err := s.createDestination(destPath, shared.DestinationType_PLAIN)
	s.Nil(err, "Failed to create destination")

	var cgUUID string
	cgName := s.generateName("/cherami/mcp-test-cg")
	createReq := &shared.CreateConsumerGroupRequest{
		DestinationPath:   common.StringPtr(destPath),
		ConsumerGroupName: common.StringPtr(cgName),
	}

	// verify remote operation
	s.mockReplicator.On("CreateRemoteConsumerGroupUUID", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		createRemoteReq := args.Get(1).(*shared.CreateConsumerGroupUUIDRequest)
		cgUUID = createRemoteReq.GetConsumerGroupUUID()
		s.True(createRemoteReq.IsSetRequest())
		s.Equal(createReq.GetDestinationPath(), createRemoteReq.GetRequest().GetDestinationPath())
		s.Equal(createReq.GetConsumerGroupName(), createRemoteReq.GetRequest().GetConsumerGroupName())
	})

	// issue request
	cgDesc, err := s.mcp.CreateConsumerGroup(nil, createReq)
	s.NoError(err)
	s.NotNil(cgDesc)

	// verify local operation
	cgDesc, err = s.mClient.ReadConsumerGroup(nil, &shared.ReadConsumerGroupRequest{
		DestinationPath:   common.StringPtr(destPath),
		ConsumerGroupName: common.StringPtr(cgName),
	})
	s.NoError(err)
	s.NotNil(cgDesc)
	//s.Equal(cgUUID, cgDesc.GetConsumerGroupUUID())
}

func (s *McpSuite) TestUpdateConsumerGroup() {
	destPath := s.generateName("/cherami/mcp-test")
	cgName := s.generateName("/cherami/mcp-test-cg")
	_, err := s.createDestination(destPath, shared.DestinationType_PLAIN)
	s.Nil(err, "Failed to create destination")
	_, err = s.createConsumerGroup(destPath, cgName)
	s.Nil(err, "Failed to create consumer group")

	newOwnerEmail := "updated@email.com"
	updateReq := &shared.UpdateConsumerGroupRequest{
		DestinationPath:   common.StringPtr(destPath),
		ConsumerGroupName: common.StringPtr(cgName),
		OwnerEmail:        common.StringPtr(newOwnerEmail),
	}

	// verify remote operation
	s.mockReplicator.On("UpdateRemoteConsumerGroup", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		updateRemoteReq := args.Get(1).(*shared.UpdateConsumerGroupRequest)
		s.Equal(destPath, updateRemoteReq.GetDestinationPath())
		s.Equal(cgName, updateRemoteReq.GetConsumerGroupName())
		s.Equal(newOwnerEmail, updateRemoteReq.GetOwnerEmail())
	})

	// issue request
	cgDesc, err := s.mcp.UpdateConsumerGroup(nil, updateReq)
	s.NoError(err)
	s.NotNil(cgDesc)

	// verify local operation
	cgDesc, err = s.mClient.ReadConsumerGroup(nil, &shared.ReadConsumerGroupRequest{
		DestinationPath:   common.StringPtr(destPath),
		ConsumerGroupName: common.StringPtr(cgName),
	})
	s.NoError(err)
	s.NotNil(cgDesc)
	s.Equal(newOwnerEmail, cgDesc.GetOwnerEmail())
}

func (s *McpSuite) TestDeleteConsumerGroup() {
	destPath := s.generateName("/cherami/mcp-test")
	cgName := s.generateName("/cherami/mcp-test-cg")
	_, err := s.createDestination(destPath, shared.DestinationType_PLAIN)
	s.Nil(err, "Failed to create destination")
	_, err = s.createConsumerGroup(destPath, cgName)
	s.Nil(err, "Failed to create consumer group")

	deleteReq := &shared.DeleteConsumerGroupRequest{
		DestinationPath:   common.StringPtr(destPath),
		ConsumerGroupName: common.StringPtr(cgName),
	}

	// verify remote operation
	s.mockReplicator.On("DeleteRemoteConsumerGroup", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		deleteRemoteReq := args.Get(1).(*shared.DeleteConsumerGroupRequest)
		s.Equal(destPath, deleteRemoteReq.GetDestinationPath())
		s.Equal(cgName, deleteRemoteReq.GetConsumerGroupName())
	})

	// issue request
	err = s.mcp.DeleteConsumerGroup(nil, deleteReq)
	s.NoError(err)

	// verify local operation
	cgDesc, err := s.mClient.ReadConsumerGroup(nil, &shared.ReadConsumerGroupRequest{
		DestinationPath:   common.StringPtr(destPath),
		ConsumerGroupName: common.StringPtr(cgName),
	})
	s.Error(err)
	s.Nil(cgDesc)
	assert.IsType(s.T(), &shared.EntityNotExistsError{}, err)
}

func (s *McpSuite) TestCreateRemoteZoneExtent() {
	destUUID := uuid.New()
	extentUUID := uuid.New()
	originZone := `zone1`

	createReq := &shared.CreateExtentRequest{
		Extent: &shared.Extent{
			DestinationUUID: common.StringPtr(destUUID),
			ExtentUUID:      common.StringPtr(extentUUID),
			OriginZone:      common.StringPtr(originZone),
		},
	}
	// issue request
	res, err := s.mcp.CreateRemoteZoneExtent(nil, createReq)
	s.NoError(err)
	s.NotNil(res)

	// verify local operation
	extentStats, err := s.mClient.ReadExtentStats(nil, &m.ReadExtentStatsRequest{
		DestinationUUID: common.StringPtr(destUUID),
		ExtentUUID:      common.StringPtr(extentUUID),
	})
	s.NoError(err)
	s.NotNil(extentStats)
	s.Equal(extentUUID, extentStats.GetExtentStats().GetExtent().GetExtentUUID())
	s.Equal(destUUID, extentStats.GetExtentStats().GetExtent().GetDestinationUUID())
	s.Equal(originZone, extentStats.GetExtentStats().GetExtent().GetOriginZone())

	primary := extentStats.GetExtentStats().GetExtent().GetRemoteExtentPrimaryStore()
	s.True(len(primary) > 0)

	primaryValid := false
	stores := extentStats.GetExtentStats().GetExtent().GetStoreUUIDs()
	for _, store := range stores {
		if store == primary {
			primaryValid = true
			break
		}
	}
	s.True(primaryValid)
}

func (s *McpSuite) TestCreateRemoteZoneCgExtent() {
	destUUID := uuid.New()
	extentUUID := uuid.New()
	cgUUID := uuid.New()
	originZone := `zone1`

	createExtentReq := &shared.CreateExtentRequest{
		Extent: &shared.Extent{
			DestinationUUID: common.StringPtr(destUUID),
			ExtentUUID:      common.StringPtr(extentUUID),
			OriginZone:      common.StringPtr(originZone),
		},
	}
	// issue create extent request
	res, err := s.mcp.CreateRemoteZoneExtent(nil, createExtentReq)
	s.NoError(err)
	s.NotNil(res)

	createCgExtentReq := &shared.CreateConsumerGroupExtentRequest{
		DestinationUUID:   common.StringPtr(destUUID),
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		ExtentUUID:        common.StringPtr(extentUUID),
	}
	// issue create cg extent request
	err = s.mcp.CreateRemoteZoneConsumerGroupExtent(nil, createCgExtentReq)
	s.NoError(err)

	// verify local operation
	cgExtent, err := s.mClient.ReadConsumerGroupExtent(nil, &m.ReadConsumerGroupExtentRequest{
		DestinationUUID:   common.StringPtr(destUUID),
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		ExtentUUID:        common.StringPtr(extentUUID),
	})
	s.NoError(err)
	s.NotNil(cgExtent)
	s.Equal(extentUUID, cgExtent.GetExtent().GetExtentUUID())
	s.Equal(cgUUID, cgExtent.GetExtent().GetConsumerGroupUUID())
	s.True(cgExtent.GetExtent().IsSetOutputHostUUID())
	s.Equal(shared.ConsumerGroupExtentStatus_OPEN, cgExtent.GetExtent().GetStatus())
}

func (s *McpSuite) TestGetDstType() {

	dstType := shared.DestinationType_PLAIN

	dstDesc := &shared.DestinationDescription{
		Path:            common.StringPtr("/unit/desttype"),
		DestinationUUID: common.StringPtr(uuid.New()),
		Type:            &dstType,
	}

	s.Equal(dstTypePlain, getDstType(dstDesc), "getDstType(PLAIN) failed")

	dstType = shared.DestinationType_TIMER
	s.Equal(dstTypeTimer, getDstType(dstDesc), "getDstType(TIMER) failed")

	dstType = shared.DestinationType_KAFKA
	s.Equal(dstTypeKafka, getDstType(dstDesc), "getDstType(KAFKA) failed")

	// test dlq
	dstType = shared.DestinationType_PLAIN
	dstDesc.Path = common.StringPtr("/unit/desttype.dlq")
	s.Equal(dstTypeDLQ, getDstType(dstDesc), "getDstType(TIMER) failed")
}

func (s *McpSuite) TestKafkaPhantomExtentChecks() {

	context := s.mcp.context

	// setup a Kafka extent with 'phantom' input/stores
	inputUUID := common.KafkaPhantomExtentInputhost
	storeUUIDs := []string{common.KafkaPhantomExtentStorehost}

	kafkaExt := &m.DestinationExtent{
		ExtentUUID:    common.StringPtr(uuid.New()),
		InputHostUUID: common.StringPtr(inputUUID),
		StoreUUIDs:    storeUUIDs,
	}

	// stores/input for Kafka extents should be assumed to be "healthy"
	s.True(isInputHealthy(context, kafkaExt))
	s.True(isAnyStoreHealthy(context, kafkaExt.GetStoreUUIDs()))
	s.True(areExtentStoresHealthy(context, kafkaExt))

	// setup an extent with 'invalid' input/stores
	inputUUID = uuid.New()
	storeUUIDs = []string{uuid.New(), uuid.New(), uuid.New()}

	dlqExt := &m.DestinationExtent{
		ExtentUUID:              common.StringPtr(uuid.New()),
		InputHostUUID:           common.StringPtr(inputUUID),
		StoreUUIDs:              storeUUIDs,
		ConsumerGroupVisibility: common.StringPtr(uuid.New()),
	}

	s.False(isInputHealthy(context, dlqExt))
	s.False(isAnyStoreHealthy(context, dlqExt.GetStoreUUIDs()))
	s.False(areExtentStoresHealthy(context, dlqExt))

	storehosts, _ := s.mcp.context.placement.PickStoreHosts(3)
	inputhost, _ := s.mcp.context.placement.PickInputHost(storehosts)

	// make input healthy
	dlqExt.InputHostUUID = common.StringPtr(inputhost.UUID)

	s.True(isInputHealthy(context, dlqExt))
	s.False(isAnyStoreHealthy(context, dlqExt.GetStoreUUIDs()))
	s.False(areExtentStoresHealthy(context, dlqExt))

	// make one store healthy
	dlqExt.StoreUUIDs[0] = storehosts[0].UUID

	s.True(isInputHealthy(context, dlqExt))
	s.True(isAnyStoreHealthy(context, dlqExt.GetStoreUUIDs()))
	s.False(areExtentStoresHealthy(context, dlqExt))

	// make all three stores healthy
	for i := 0; i < 3; i++ {
		dlqExt.StoreUUIDs[i] = storehosts[i].UUID
	}

	s.True(isInputHealthy(context, dlqExt))
	s.True(isAnyStoreHealthy(context, dlqExt.GetStoreUUIDs()))
	s.True(areExtentStoresHealthy(context, dlqExt))
}
