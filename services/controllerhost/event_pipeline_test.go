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
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	mc "github.com/uber/cherami-server/clients/metadata"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
	localMetrics "github.com/uber/cherami-server/common/metrics"
	storeStream "github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

type (
	EventPipelineSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
		seqNum  int32
		cfg     configure.CommonAppConfig
		cdb     *mc.TestCluster
		mcp     *Mcp
		mClient m.TChanMetadataService
	}
)

func TestEventPipelineSuite(t *testing.T) {
	suite.Run(t, new(EventPipelineSuite))
}

func (s *EventPipelineSuite) SetupSuite() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	// cassandra set up
	s.cdb = &mc.TestCluster{}
	s.cdb.SetupTestCluster()
	s.seqNum = rand.Int31n(10000)
}

func (s *EventPipelineSuite) TearDownSuite() {
	s.cdb.TearDownTestCluster()
}

func (s *EventPipelineSuite) SetupTest() {
	// test cluste for in/out
	s.cfg = common.SetupServerConfig(configure.NewCommonConfigure())
	s.mClient = s.cdb.GetClient()

	serviceConfig := s.cfg.GetServiceConfig(common.ControllerServiceName)
	serviceConfig.SetPort(0)

	serviceName := common.ControllerServiceName
	reporter := common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig())
	dClient := dconfig.NewDconfigClient(serviceConfig, common.ControllerServiceName)
	sVice := common.NewService(serviceName, uuid.New(), serviceConfig, common.NewUUIDResolver(s.mClient), common.NewHostHardwareInfoReader(s.mClient), reporter, dClient, common.NewBypassAuthManager())
	mcp, _ := NewController(s.cfg, sVice, s.mClient, common.NewDummyZoneFailoverManager())
	mcp.context.m3Client = &MockM3Metrics{}
	s.mcp = mcp
	ch, err := tchannel.NewChannel("event-pipeline-test", nil)
	s.Nil(err, "Failed to create tchannel")
	s.mcp.context.channel = ch
	s.mcp.context.resultCache = newResultCache(s.mcp.context)
	s.mcp.context.extentSeals.tokenBucket = common.NewTokenBucket(100, common.NewRealTimeSource())
	s.mcp.context.clientFactory = common.NewClientFactory(nil, ch, bark.NewLoggerFromLogrus(log.New()))
	s.mcp.context.eventPipeline = NewEventPipeline(s.mcp.context, nEventPipelineWorkers)
	s.mcp.context.extentMonitor = newExtentStateMonitor(s.mcp.context)
	s.mcp.context.eventPipeline.Start()
}

func (s *EventPipelineSuite) TearDownTest() {
	s.mcp.context.eventPipeline.Stop()
}

func (s *EventPipelineSuite) generateName(prefix string) string {
	seq := int(atomic.AddInt32(&s.seqNum, 1))
	return strings.Join([]string{prefix, strconv.Itoa(seq)}, ".")
}

type unknownEvent struct {
	eventBase
}

func (s *EventPipelineSuite) TestEventStream() {
	nWorkers := 16
	pipeline := NewEventPipeline(s.mcp.context, nWorkers)
	pipeline.Start()
	for i := 0; i < nWorkers+1; i++ {
		succ := pipeline.Add(&unknownEvent{})
		s.True(succ, "Failed to enqueue event")
	}
	pipeline.Stop()
}

func (s *EventPipelineSuite) TestExtentCreatedEvent() {
	var succ bool
	path := s.generateName("/cherami/event-test")
	dstDesc, err := s.createDestination(path)
	s.Nil(err, "Failed to create destination")
	s.Equal(common.UUIDStringLength, len(dstDesc.GetDestinationUUID()), "Invalid destination uuid")

	dstID := dstDesc.GetDestinationUUID()
	inHostIDs := []string{uuid.New(), uuid.New()}
	extentIDs := []string{uuid.New(), uuid.New()}
	storeIDs := []string{uuid.New(), uuid.New(), uuid.New()}

	for i := 0; i < len(extentIDs); i++ {
		_, err := s.mcp.context.mm.CreateExtent(dstID, extentIDs[i], inHostIDs[i], storeIDs)
		s.Nil(err, "Failed to create extent")
	}

	outHostID := uuid.New()
	// set up for result cache. On a extent
	// created event, the result cache must
	// be invalidated
	cacheAddrs := make([]string, 10)
	for i := 0; i < 10; i++ {
		cacheAddrs[i] = outHostID
	}

	now := time.Now().UnixNano()
	cgNames := []string{s.generateName("cons-1"), s.generateName("cons-2")}
	cgIDs := make([]string, len(cgNames))
	for i, name := range cgNames {
		cgDesc, errCCG := s.createConsumerGroup(dstDesc.GetPath(), name)
		s.Nil(errCCG, "Failed to create consumer group")
		cgIDs[i] = cgDesc.GetConsumerGroupUUID()
		for _, extID := range extentIDs {
			s.mcp.context.mm.AddExtentToConsumerGroup(dstID, cgIDs[i], extID, outHostID, storeIDs)
		}

		s.mcp.context.resultCache.write(cgIDs[i],
			resultCacheParams{
				dstType:    dstTypePlain,
				nExtents:   10,
				maxExtents: 10,
				hostIDs:    cacheAddrs,
				expiry:     now + int64(time.Hour),
			})
	}

	outputService := NewMockInputOutputService(common.OutputServiceName)
	thriftService := admin.NewTChanOutputHostAdminServer(outputService)
	outputService.Start(common.OutputServiceName, thriftService)

	inputService := NewMockInputOutputService(common.InputServiceName)
	thriftService = admin.NewTChanInputHostAdminServer(inputService)
	inputService.Start(common.InputServiceName, thriftService)

	rpm := common.NewMockRingpopMonitor()
	rpm.Add(common.OutputServiceName, outHostID, outputService.hostPort)
	for _, h := range inHostIDs {
		rpm.Add(common.InputServiceName, h, inputService.hostPort)
	}
	s.mcp.context.rpm = rpm

	for _, cgID := range cgIDs {
		result := s.mcp.context.resultCache.readOutputHosts(cgID, time.Now().UnixNano())
		s.True(result.cacheHit, "Unexpected resultCache miss")
		s.False(result.refreshCache, "resultCache refresh unexpected")
	}

	for i, extentID := range extentIDs {

		event := NewExtentCreatedEvent(dstID, inHostIDs[i], extentID, storeIDs)
		succ = s.mcp.context.eventPipeline.Add(event)
		s.True(succ, "Failed to enqueue ExtentCreatedEvent")

		for _, cgID := range cgIDs {
			event = NewConsGroupUpdatedEvent(dstID, cgID, extentID, outHostID)
			succ = s.mcp.context.eventPipeline.Add(event)
			s.True(succ, "Failed to enqueue ExtentCreatedEvent")
		}
	}

	cond := func() bool {
		count := inputService.GetUpdatedCount(dstID)
		return (count == len(inHostIDs)*len(extentIDs))
	}

	succ = common.SpinWaitOnCondition(cond, 60*time.Second)
	s.True(succ, "Input host failed to receive notification on time")

	for _, cgID := range cgIDs {
		cond = func() bool {
			count := outputService.GetUpdatedCount(cgID)
			return (count == 2*len(extentIDs)) // once for extentcreated and once for consgroupupdated
		}
		succ = common.SpinWaitOnCondition(cond, 60*time.Second)
		s.True(succ, "Output host failed to receive notification within timeout")
		result := s.mcp.context.resultCache.readOutputHosts(cgID, time.Now().UnixNano())
		s.True(result.cacheHit, "Unexpected resultCache miss")
		s.True(result.refreshCache, "Result cache invalidation failed")
	}

	outputService.Stop()
	inputService.Stop()
}

func (s *EventPipelineSuite) TestStoreHostFailedEvent() {

	path := s.generateName("/cherami/event-test")
	dstDesc, err := s.createDestination(path)
	s.Nil(err, "Failed to create destination")
	s.Equal(common.UUIDStringLength, len(dstDesc.GetDestinationUUID()), "Invalid destination uuid")

	dstID := dstDesc.GetDestinationUUID()
	inHostIDs := []string{uuid.New(), uuid.New()}
	extentIDs := []string{uuid.New(), uuid.New()}
	storeIDs := []string{uuid.New(), uuid.New(), uuid.New()}

	for i := 0; i < len(extentIDs); i++ {
		_, err := s.mcp.context.mm.CreateExtent(dstID, extentIDs[i], inHostIDs[i], storeIDs)
		s.Nil(err, "Failed to create extent")
	}

	rpm := common.NewMockRingpopMonitor()

	stores := make([]*MockStoreService, len(storeIDs))
	for i := 0; i < len(storeIDs); i++ {
		stores[i] = NewMockStoreService()
		stores[i].Start()
		rpm.Add(common.StoreServiceName, storeIDs[i], stores[i].hostPort)
	}
	s.mcp.context.rpm = rpm

	event := NewStoreHostFailedEvent(storeIDs[0])
	s.mcp.context.eventPipeline.Add(event)

	for i := 0; i < len(extentIDs); i++ {
		cond := func() bool {
			succ := true
			for j := 0; j < len(storeIDs); j++ {
				if !stores[j].isSealed(extentIDs[i]) {
					succ = false
					break
				}
			}
			return succ
		}
		succ := common.SpinWaitOnCondition(cond, 60*time.Second)
		s.True(succ, "Timed out waiting for exent to be sealed")
	}

	cond := func() bool {
		stats, err := s.mcp.context.mm.ListExtentsByDstIDStatus(dstID, nil)
		if err != nil {
			return false
		}
		for _, stat := range stats {
			if stat.GetStatus() != shared.ExtentStatus_SEALED {
				return false
			}
		}
		return true
	}

	succ := common.SpinWaitOnCondition(cond, 60*time.Second)
	s.True(succ, "Failed to update extent metadata after SEALing extent")

	for i := 0; i < len(stores); i++ {
		stores[i].Stop()
	}
}

func (s *EventPipelineSuite) TestRemoteExtentPrimaryStoreDownEvent() {

	path := s.generateName("/cherami/event-test")
	dstDesc, err := s.createDestination(path)
	s.Nil(err, "Failed to create destination")
	s.Equal(common.UUIDStringLength, len(dstDesc.GetDestinationUUID()), "Invalid destination uuid")

	dstID := dstDesc.GetDestinationUUID()
	inHostIDs := []string{uuid.New(), uuid.New()}
	extentIDs := []string{uuid.New()}
	storeIDs := []string{uuid.New(), uuid.New(), uuid.New()}
	primaryStoreIdx := 1
	originZone := `zone1`

	for i := 0; i < len(extentIDs); i++ {
		_, err := s.mcp.context.mm.CreateRemoteZoneExtent(dstID, extentIDs[i], inHostIDs[i], storeIDs, originZone, storeIDs[primaryStoreIdx], ``)
		s.Nil(err, "Failed to create extent")
	}

	// verify the primary store is correctly set
	stats, err := s.mcp.context.mm.ListExtentsByDstIDStatus(dstID, nil)
	s.Nil(err, "Failed to list extents")
	for _, stat := range stats {
		s.Equal(storeIDs[primaryStoreIdx], stat.GetExtent().GetRemoteExtentPrimaryStore())
	}

	rpm := common.NewMockRingpopMonitor()

	stores := make([]*MockStoreService, len(storeIDs))
	for i := 0; i < len(storeIDs); i++ {
		stores[i] = NewMockStoreService()
		stores[i].Start()
		rpm.Add(common.StoreServiceName, storeIDs[i], stores[i].hostPort)
	}
	s.mcp.context.rpm = rpm

	event := NewRemoteExtentPrimaryStoreDownEvent(storeIDs[1], extentIDs[0])
	s.mcp.context.eventPipeline.Add(event)

	cond := func() bool {
		newStats, err := s.mcp.context.mm.ListExtentsByDstIDStatus(dstID, nil)
		if err != nil {
			return false
		}
		for _, stat := range newStats {
			if stat.GetExtent().GetRemoteExtentPrimaryStore() == storeIDs[primaryStoreIdx] {
				return false
			}
		}
		return true
	}
	succ := common.SpinWaitOnCondition(cond, 60*time.Second)
	s.True(succ, "Timed out waiting for primary store to be changed")

	for i := 0; i < len(stores); i++ {
		stores[i].Stop()
	}
}

func (s *EventPipelineSuite) TestInputHostFailedEvent() {

	path := s.generateName("/cherami/event-test")
	dstDesc, err := s.createDestination(path)
	s.Nil(err, "Failed to create destination")
	s.Equal(common.UUIDStringLength, len(dstDesc.GetDestinationUUID()), "Invalid destination uuid")

	dstID := dstDesc.GetDestinationUUID()
	inHostIDs := []string{uuid.New(), uuid.New()}
	extentIDs := []string{uuid.New(), uuid.New()}
	storeIDs := []string{uuid.New(), uuid.New(), uuid.New()}

	for i := 0; i < len(extentIDs); i++ {
		_, err := s.mcp.context.mm.CreateExtent(dstID, extentIDs[i], inHostIDs[i], storeIDs)
		s.Nil(err, "Failed to create extent")
	}

	rpm := common.NewMockRingpopMonitor()

	inputs := make([]*MockInputOutputService, len(inHostIDs))
	for i := 0; i < len(inHostIDs); i++ {
		inputs[i] = NewMockInputOutputService(common.InputServiceName)
		thriftService := admin.NewTChanInputHostAdminServer(inputs[i])
		inputs[i].Start(common.InputServiceName, thriftService)
		rpm.Add(common.InputServiceName, inHostIDs[i], inputs[i].hostPort)
	}

	for _, e := range extentIDs {
		inputs[0].failDrainExtentFor(e)
	}

	stores := make([]*MockStoreService, len(storeIDs))
	for i := 0; i < len(storeIDs); i++ {
		stores[i] = NewMockStoreService()
		stores[i].Start()
		rpm.Add(common.StoreServiceName, storeIDs[i], stores[i].hostPort)
	}
	s.mcp.context.rpm = rpm

	for _, h := range inHostIDs {
		event := NewInputHostFailedEvent(h)
		s.mcp.context.eventPipeline.Add(event)
	}

	for i := 0; i < len(extentIDs); i++ {
		cond := func() bool {
			succ := true
			for j := 0; j < len(storeIDs); j++ {
				if !stores[j].isSealed(extentIDs[i]) {
					succ = false
					break
				}
			}
			if !succ {
				return false
			}
			for j := 0; j < len(inHostIDs); j++ {
				if inputs[j].drainExtentsRcvdFor(extentIDs[j]) != 1 {
					succ = false
					break
				}
			}
			return succ
		}
		succ := common.SpinWaitOnCondition(cond, 60*time.Second)
		s.True(succ, "Timed out waiting for exent to be sealed")
	}

	cond := func() bool {
		stats, err := s.mcp.context.mm.ListExtentsByDstIDStatus(dstID, nil)
		if err != nil {
			return false
		}
		for _, stat := range stats {
			if stat.GetStatus() != shared.ExtentStatus_SEALED {
				return false
			}
		}
		return true
	}

	succ := common.SpinWaitOnCondition(cond, 60*time.Second)
	s.True(succ, "Failed to update extent metadata after SEALing extent")

	for i := 0; i < len(stores); i++ {
		stores[i].Stop()
	}
}

func (s *EventPipelineSuite) TestRemoteZoneExtentCreatedEvent() {
	destID := uuid.New()
	extentID := uuid.New()
	storeIDs := []string{uuid.New(), uuid.New(), uuid.New()}

	rpm := common.NewMockRingpopMonitor()
	stores := make([]*MockStoreService, len(storeIDs))
	for i := 0; i < len(storeIDs); i++ {
		stores[i] = NewMockStoreService()
		stores[i].Start()
		rpm.Add(common.StoreServiceName, storeIDs[i], stores[i].hostPort)
	}
	s.mcp.context.rpm = rpm

	event := NewStartReplicationForRemoteZoneExtent(destID, extentID, storeIDs, storeIDs[0])
	s.mcp.context.eventPipeline.Add(event)

	// The first store is expected to be remote replicated
	cond := func() bool {
		return stores[0].isExtentRemoteReplicated(extentID) && !stores[0].isExtentReReplicated(extentID)
	}
	succ := common.SpinWaitOnCondition(cond, 2*time.Second)
	s.True(succ, "Timed out waiting for exent to be remote replicated")

	// The other two stores are expected to be re-replicated
	for i := 1; i < len(storeIDs); i++ {
		cond = func() bool {
			return stores[i].isExtentReReplicated(extentID) && !stores[i].isExtentRemoteReplicated(extentID)
		}
		succ = common.SpinWaitOnCondition(cond, 2*time.Second)
		s.True(succ, "Timed out waiting for exent to be rereplicated")
	}
}

func (s *EventPipelineSuite) TestRetryableExecutor() {
	executor := NewRetryableEventExecutor(s.mcp.context, 10)
	for i := 0; i < 10; i++ {
		succ := executor.Submit(&eventBase{})
		s.True(succ, "Failed to enqueue task to retryable event executor")
	}
	succ := executor.Submit(&eventBase{})
	s.False(succ, "Retryable even executor failed to enforce max workers")
	executor.Stop()
	succ = executor.Submit(&eventBase{})
	s.False(succ, "Retryable even executor failed to enforce max workers")
}

func (s *EventPipelineSuite) createDestination(name string) (*shared.DestinationDescription, error) {
	mReq := &shared.CreateDestinationRequest{
		Path: common.StringPtr(name),
		ConsumedMessagesRetention:   common.Int32Ptr(300),
		UnconsumedMessagesRetention: common.Int32Ptr(600),
		OwnerEmail:                  common.StringPtr("test@uber.com"),
	}
	return s.mClient.CreateDestination(nil, mReq)
}

func (s *EventPipelineSuite) createConsumerGroup(dstPath string, cgName string) (*shared.ConsumerGroupDescription, error) {
	mReq := &shared.CreateConsumerGroupRequest{
		DestinationPath:   common.StringPtr(dstPath),
		ConsumerGroupName: common.StringPtr(cgName),
	}
	return s.mClient.CreateConsumerGroup(nil, mReq)
}

// MOCKS

type MockM3Metrics struct{}
type MockStopwatch struct{}

func (m *MockM3Metrics) IncCounter(op int, idx int)                       {}
func (m *MockM3Metrics) AddCounter(op int, idx int, delta int64)          {}
func (m *MockM3Metrics) UpdateGauge(op int, idx int, delta int64)         {}
func (m *MockM3Metrics) RecordTimer(op int, idx int, delta time.Duration) {}
func (m *MockM3Metrics) StartTimer(scopeIdx int, timerIdx int) localMetrics.Stopwatch {
	watch := &MockStopwatch{}
	return watch
}
func (m *MockM3Metrics) GetParentReporter() localMetrics.Reporter { return nil }
func (m *MockStopwatch) Stop() time.Duration {
	return time.Second
}

type MockStoreService struct {
	server                   *thrift.Server
	ch                       *tchannel.Channel
	hostPort                 string
	mu                       sync.Mutex
	sealedExtents            map[string]bool
	sealFailedCount          int
	remoteReplicationExtents []string
	reReplicationExtents     []string
}

func NewMockStoreService() *MockStoreService {
	return &MockStoreService{
		sealedExtents:            make(map[string]bool),
		remoteReplicationExtents: make([]string, 0),
		reReplicationExtents:     make([]string, 0),
	}
}

func setupTestTChannelUtil(sName string, thriftService thrift.TChanServer) (*tchannel.Channel, *thrift.Server) {
	ch, err := tchannel.NewChannel(sName, &tchannel.ChannelOptions{
		DefaultConnectionOptions: tchannel.ConnectionOptions{
			FramePool: tchannel.NewSyncFramePool(),
		},
	})
	server := thrift.NewServer(ch)
	server.Register(thriftService)

	if err = ch.ListenAndServe("127.0.0.1:0"); err != nil {
		log.Fatalf("Unable to start tchannel server: %v", err)
	}

	return ch, server
}

func (service *MockStoreService) Start() {
	thriftService := store.NewTChanBStoreServer(service)
	service.ch, service.server = setupTestTChannelUtil(common.StoreServiceName, thriftService)
	service.hostPort = service.ch.PeerInfo().HostPort
}

func (service *MockStoreService) Stop() {
	service.ch.Close()
}

func (service *MockStoreService) OpenAppendStream(ctx thrift.Context, sic storeStream.BStoreOpenAppendStreamInCall) error {
	return errors.New("Not implemented")
}
func (service *MockStoreService) OpenReadStream(ctx thrift.Context, cfs storeStream.BStoreOpenReadStreamInCall) error {
	return errors.New("Not implemented")
}
func (service *MockStoreService) GetAddressFromTimestamp(ctx thrift.Context, req *store.GetAddressFromTimestampRequest) (r *store.GetAddressFromTimestampResult_, err error) {
	return nil, errors.New("Not implemented")
}
func (service *MockStoreService) GetExtentInfo(ctx thrift.Context, eir *store.GetExtentInfoRequest) (r *store.ExtentInfo, err error) {
	return nil, errors.New("Not implemented")
}
func (service *MockStoreService) PurgeMessages(ctx thrift.Context, req *store.PurgeMessagesRequest) (*store.PurgeMessagesResult_, error) {
	return nil, errors.New("Not implemented")
}
func (service *MockStoreService) ReadMessages(ctx thrift.Context, readMessagesRequest *store.ReadMessagesRequest) (*store.ReadMessagesResult_, error) {
	return nil, errors.New("Not implemented")
}
func (service *MockStoreService) ReplicateExtent(ctx thrift.Context, req *store.ReplicateExtentRequest) error {
	service.mu.Lock()
	service.reReplicationExtents = append(service.reReplicationExtents, req.GetExtentUUID())
	service.mu.Unlock()
	return nil
}

func (service *MockStoreService) RemoteReplicateExtent(ctx thrift.Context, req *store.RemoteReplicateExtentRequest) error {
	service.mu.Lock()
	service.remoteReplicationExtents = append(service.remoteReplicationExtents, req.GetExtentUUID())
	service.mu.Unlock()
	return nil
}

func (service *MockStoreService) SealExtent(ctx thrift.Context, req *store.SealExtentRequest) (err error) {
	succ := false
	extentID := req.GetExtentUUID()
	service.mu.Lock()
	_, ok := service.sealedExtents[extentID]
	if !ok {
		succ = true
		service.sealedExtents[extentID] = true
	} else {
		service.sealFailedCount++
	}
	service.mu.Unlock()
	if !succ {
		return store.NewExtentSealedError()
	}
	return nil
}

func (service *MockStoreService) ListExtents(ctx thrift.Context) (*store.ListExtentsResult_, error) {
	return nil, errors.New("Not implemented")
}

func (service *MockStoreService) isSealed(extentID string) bool {
	sealed := false
	service.mu.Lock()
	_, sealed = service.sealedExtents[extentID]
	service.mu.Unlock()
	return sealed
}

func (service *MockStoreService) getSealFailedCount() int {
	count := 0
	service.mu.Lock()
	count = service.sealFailedCount
	service.mu.Unlock()
	return count
}

func (service *MockStoreService) isExtentRemoteReplicated(extentID string) bool {
	replicated := false
	service.mu.Lock()
	for _, id := range service.remoteReplicationExtents {
		if id == extentID {
			replicated = true
			break
		}
	}
	service.mu.Unlock()
	return replicated
}

func (service *MockStoreService) isExtentReReplicated(extentID string) bool {
	replicated := false
	service.mu.Lock()
	for _, id := range service.reReplicationExtents {
		if id == extentID {
			replicated = true
			break
		}
	}
	service.mu.Unlock()
	return replicated
}

type MockInputOutputService struct {
	name                  string
	server                *thrift.Server
	ch                    *tchannel.Channel
	hostPort              string
	mu                    sync.Mutex
	keyToUpdateCount      map[string]int
	drainExtentsRcvdCount map[string]int
	drainExtentsToFail    map[string]struct{}
}

func NewMockInputOutputService(name string) *MockInputOutputService {
	return &MockInputOutputService{
		name: name,
		drainExtentsRcvdCount: make(map[string]int),
		keyToUpdateCount:      make(map[string]int),
		drainExtentsToFail:    make(map[string]struct{}),
	}
}

func (service *MockInputOutputService) Start(sName string, thriftService thrift.TChanServer) {
	service.ch, service.server = setupTestTChannelUtil(sName, thriftService)
	service.hostPort = service.ch.PeerInfo().HostPort
}

func (service *MockInputOutputService) Stop() {
	service.ch.Close()
}

func (service *MockInputOutputService) ConsumerGroupsUpdated(ctx thrift.Context, request *admin.ConsumerGroupsUpdatedRequest) error {
	log.Debugf("Notification received by output host")
	service.mu.Lock()
	for _, update := range request.GetUpdates() {
		count, ok := service.keyToUpdateCount[update.GetConsumerGroupUUID()]
		if !ok {
			count = 0
		}
		count++
		service.keyToUpdateCount[update.GetConsumerGroupUUID()] = count
	}
	service.mu.Unlock()
	return nil
}

func (service *MockInputOutputService) DestinationsUpdated(ctx thrift.Context, request *admin.DestinationsUpdatedRequest) error {
	log.Debugf("Notification received by input host")
	service.mu.Lock()
	for _, update := range request.GetUpdates() {
		count, ok := service.keyToUpdateCount[update.GetDestinationUUID()]
		if !ok {
			count = 0
		}
		count++
		service.keyToUpdateCount[update.GetDestinationUUID()] = count
	}
	service.mu.Unlock()
	return nil
}

func (service *MockInputOutputService) DrainExtent(ctx thrift.Context, request *admin.DrainExtentsRequest) error {
	service.mu.Lock()
	defer service.mu.Unlock()
	var err error
	for _, extent := range request.GetExtents() {
		count, ok := service.drainExtentsRcvdCount[extent.GetExtentUUID()]
		if !ok {
			count = 0
		}
		count++
		service.drainExtentsRcvdCount[extent.GetExtentUUID()] = count
		if _, ok := service.drainExtentsToFail[extent.GetExtentUUID()]; ok {
			err = errors.New("drain failed")
		}
	}
	return err
}

func (service *MockInputOutputService) drainExtentsRcvdFor(extID string) int {
	service.mu.Lock()
	defer service.mu.Unlock()
	count, ok := service.drainExtentsRcvdCount[extID]
	if !ok {
		return 0
	}
	return count
}

func (service *MockInputOutputService) failDrainExtentFor(extID string) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.drainExtentsToFail[extID] = struct{}{}
}

func (service *MockInputOutputService) GetUpdatedCount(key string) int {
	count := 0
	service.mu.Lock()
	count, ok := service.keyToUpdateCount[key]
	if !ok {
		count = 0
	}
	service.mu.Unlock()
	return count
}

func (service *MockInputOutputService) UnloadConsumerGroups(ctx thrift.Context, request *admin.UnloadConsumerGroupsRequest) error {
	return fmt.Errorf("mock not implemented")
}

func (service *MockInputOutputService) ListLoadedConsumerGroups(ctx thrift.Context) (*admin.ListConsumerGroupsResult_, error) {
	return nil, fmt.Errorf("mock not implemented")
}

func (service *MockInputOutputService) ReadCgState(ctx thrift.Context, req *admin.ReadConsumerGroupStateRequest) (*admin.ReadConsumerGroupStateResult_, error) {
	return nil, fmt.Errorf("mock not implemented")
}

func (service *MockInputOutputService) UnloadDestinations(ctx thrift.Context, request *admin.UnloadDestinationsRequest) error {
	return fmt.Errorf("mock not implemented")
}

func (service *MockInputOutputService) ListLoadedDestinations(ctx thrift.Context) (*admin.ListLoadedDestinationsResult_, error) {
	return nil, fmt.Errorf("mock not implemented")
}

func (service *MockInputOutputService) ReadDestState(ctx thrift.Context, req *admin.ReadDestinationStateRequest) (*admin.ReadDestinationStateResult_, error) {
	return nil, fmt.Errorf("mock not implemented")
}
