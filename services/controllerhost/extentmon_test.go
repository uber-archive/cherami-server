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
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	mc "github.com/uber/cherami-server/clients/metadata"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/tchannel-go"
)

type (
	ExtentStateMonitorSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
		seqNum  int32
		cfg     configure.CommonAppConfig
		cdb     *mc.TestCluster
		mcp     *Mcp
		mClient m.TChanMetadataService
	}
)

func TestExtentStateMonitorSuite(t *testing.T) {
	suite.Run(t, new(ExtentStateMonitorSuite))
}

func (s *ExtentStateMonitorSuite) SetupSuite() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	// cassandra set up
	s.cdb = &mc.TestCluster{}
	s.cdb.SetupTestCluster()
	s.seqNum = rand.Int31n(10000)
}

func (s *ExtentStateMonitorSuite) TearDownSuite() {
	s.cdb.TearDownTestCluster()
}

func (s *ExtentStateMonitorSuite) generateName(prefix string) string {
	seq := int(atomic.AddInt32(&s.seqNum, 1))
	return strings.Join([]string{prefix, strconv.Itoa(seq)}, ".")
}

func (s *ExtentStateMonitorSuite) SetupTest() {
	// test cluste for in/out
	s.cfg = common.SetupServerConfig(configure.NewCommonConfigure())
	s.mClient = s.cdb.GetClient()

	serviceConfig := s.cfg.GetServiceConfig(common.ControllerServiceName)
	serviceConfig.SetPort(0)

	serviceName := common.ControllerServiceName
	reporter := common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig())
	dClient := dconfig.NewDconfigClient(serviceConfig, common.ControllerServiceName)

	sVice := common.NewService(serviceName, uuid.New(), serviceConfig, common.NewUUIDResolver(s.mClient), common.NewHostHardwareInfoReader(s.mClient), reporter, dClient)

	mcp, _ := NewController(s.cfg, sVice, s.mClient, common.NewDummyZoneFailoverManager())
	mcp.context.m3Client = &MockM3Metrics{}
	s.mcp = mcp
	ch, err := tchannel.NewChannel("extent-state-monitor-test", nil)
	s.Nil(err, "Failed to create tchannel")
	s.mcp.context.hostID = uuid.New()
	s.mcp.context.channel = ch
	s.mcp.context.clientFactory = common.NewClientFactory(nil, ch, bark.NewLoggerFromLogrus(log.New()))
	s.mcp.context.eventPipeline = NewEventPipeline(s.mcp.context, 2)
	s.mcp.context.eventPipeline.Start()
	s.mcp.context.extentMonitor = newExtentStateMonitor(s.mcp.context)
	s.mcp.context.failureDetector = NewDfdd(s.mcp.context, common.NewRealTimeSource())
	s.mcp.context.m3Client = metrics.NewClient(common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig()), metrics.Controller)
}

func (s *ExtentStateMonitorSuite) TearDownTest() {
	s.mcp.context.eventPipeline.Stop()
}

func (s *ExtentStateMonitorSuite) createDestination(name string) (*shared.DestinationDescription, error) {
	mReq := &shared.CreateDestinationRequest{
		Path: common.StringPtr(name),
		ConsumedMessagesRetention:   common.Int32Ptr(300),
		UnconsumedMessagesRetention: common.Int32Ptr(600),
		OwnerEmail:                  common.StringPtr("test@uber.com"),
	}
	return s.mClient.CreateDestination(nil, mReq)
}

func (s *ExtentStateMonitorSuite) TestStoreExtentMetadataCache() {

	context := s.mcp.context

	path := s.generateName("/extentmon/store-metadatacache")
	desc, err := s.createDestination(path)
	s.Nil(err, "failed to create destination")

	extID := uuid.New()
	storeIDs := []string{uuid.New(), uuid.New(), uuid.New()}

	_, err = context.mm.CreateExtent(desc.GetDestinationUUID(), extID, uuid.New(), storeIDs)
	s.Nil(err, "error creating extent")

	cache := newStoreExtentMetadataCache(s.mcp.context.mm, bark.NewLoggerFromLogrus(log.New()))
	s.True(nil == cache.get(storeID(uuid.New()), extentID(uuid.New())), "unexpected cache hit")
	s.True(nil == cache.get(storeID(storeIDs[0]), extentID(uuid.New())), "unexpected cache hit")
	s.True(nil == cache.get(storeID(uuid.New()), extentID(extID)), "unexpected cache hit")
	s.True(nil == cache.get(storeID(storeIDs[1]), extentID(uuid.New())), "unexpected cache hit")
	s.True(nil == cache.get(storeID(storeIDs[2]), extentID(uuid.New())), "unexpected cache hit")

	for _, st := range storeIDs {
		val := cache.get(storeID(st), extentID(extID))
		s.NotNil(val, "unexpected cache miss for store %v", s)
		s.Equal(st, val.storeID, "wrong store id on cache hit")
		s.Equal(int64(0), val.beginSequence)
		s.Equal(int64(0), val.lastSequence)
		s.Equal(float64(0), val.availableSequenceRate)
		s.Equal(float64(0), val.lastSequenceRate)
		s.True(val.createdTime > 0)
	}
}

func (s *ExtentStateMonitorSuite) TestStoreExtentStatusOutOfSync() {

	path := s.generateName("/storeoutofsync/test1")
	desc, err := s.createDestination(path)
	s.Nil(err, "Failed to create destination")

	extentID := uuid.New()
	inHostID := uuid.New()
	storeIDs := []string{uuid.New(), uuid.New(), uuid.New()}

	rpm := common.NewMockRingpopMonitor()
	stores := make([]*MockStoreService, len(storeIDs))
	for i := 0; i < len(storeIDs); i++ {
		stores[i] = NewMockStoreService()
		stores[i].Start()
		rpm.Add(common.StoreServiceName, storeIDs[i], stores[i].hostPort)
	}

	s.mcp.context.rpm = rpm
	// simulate one of the store host reporting itself as out of sync
	s.mcp.context.extentMonitor.RecvStoreExtentHeartbeat(storeIDs[0], extentID, shared.ExtentStatus_OPEN)
	// now invoke the extent monitor to make sure it enqueues the right event
	s.mcp.context.extentMonitor.mi.publishEvent(eIterStart, nil)
	s.mcp.context.extentMonitor.mi.publishEvent(eDestStart, desc)
	s.mcp.context.extentMonitor.mi.publishEvent(eExtentIterStart, nil)
	s.mcp.context.extentMonitor.handleDestinationExtent(desc, false, &m.DestinationExtent{
		Status:        common.MetadataExtentStatusPtr(shared.ExtentStatus_SEALED),
		ExtentUUID:    common.StringPtr(extentID),
		InputHostUUID: common.StringPtr(inHostID),
		StoreUUIDs:    storeIDs,
	})
	s.mcp.context.extentMonitor.mi.publishEvent(eExtentIterEnd, nil)
	s.mcp.context.extentMonitor.mi.publishEvent(eDestEnd, desc)
	s.mcp.context.extentMonitor.mi.publishEvent(eIterEnd, nil)

	succ := false
	expiry := time.Now().Add(time.Minute).UnixNano()

	for {
		if stores[0].isSealed(extentID) {
			succ = true
			break
		}
		if time.Now().UnixNano() >= expiry {
			break
		}
		time.Sleep(time.Millisecond)
	}

	s.True(succ, "ExtentMonitor failed to fix out of sync store host extent")
	s.False(stores[1].isSealed(extentID), "SealExtent() call landed on store host unexpectedly")
	s.False(stores[2].isSealed(extentID), "SealExtent() call landed on store host unexpectedly")
}

func (s *ExtentStateMonitorSuite) TestStoreRemoteExtentReplicatorDownTrigger() {

	path := s.generateName("/storedown/remote")
	desc, err := s.createDestination(path)
	s.Nil(err, "failed to create destination")

	extentID := uuid.New()
	inHostID := uuid.New()
	storeIDs := []string{uuid.New(), uuid.New(), uuid.New()}

	context := s.mcp.context
	_, err = context.mm.CreateRemoteZoneExtent(desc.GetDestinationUUID(), extentID, inHostID, storeIDs, "origin", storeIDs[0], ``)
	s.Nil(err, "failed to create remote zone extent")

	// just have one store as healthy
	rpm := common.NewMockRingpopMonitor()
	rpm.Add(common.StoreServiceName, storeIDs[2], "127.0.0.1:0")

	context.rpm = rpm
	timeSource := common.NewMockTimeSource()
	context.failureDetector = NewDfdd(s.mcp.context, timeSource)
	dfdd := context.failureDetector.(*dfddImpl)

	// make dfdd think that store0 is failed,
	// store1 is unknown and store2 is healthy
	for i := range []int{0, 2} {
		event := &common.RingpopListenerEvent{
			Key:  storeIDs[i],
			Type: common.HostAddedEvent,
		}
		dfdd.handleHostAddedEvent(common.StoreServiceName, event)
	}

	event := &common.RingpopListenerEvent{Key: storeIDs[0], Type: common.HostRemovedEvent}
	dfdd.handleHostRemovedEvent(common.StoreServiceName, event)
	timeSource.Advance(maxHostRestartDuration)

	// now invoke the extent monitor to make sure it enqueues the right event
	s.mcp.context.extentMonitor.mi.publishEvent(eIterStart, nil)
	s.mcp.context.extentMonitor.mi.publishEvent(eDestStart, desc)
	s.mcp.context.extentMonitor.mi.publishEvent(eExtentIterStart, nil)
	s.mcp.context.extentMonitor.handleDestinationExtent(desc, false, &m.DestinationExtent{
		Status:        common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN),
		ExtentUUID:    common.StringPtr(extentID),
		InputHostUUID: common.StringPtr(inHostID),
		StoreUUIDs:    storeIDs,
		OriginZone:    common.StringPtr("origin"),
	})
	s.mcp.context.extentMonitor.mi.publishEvent(eExtentIterEnd, nil)
	s.mcp.context.extentMonitor.mi.publishEvent(eDestEnd, desc)
	s.mcp.context.extentMonitor.mi.publishEvent(eIterEnd, nil)

	cond := func() bool {
		stats, err := context.mm.ReadExtentStats(desc.GetDestinationUUID(), extentID)
		if err != nil {
			return false
		}
		return stats.GetExtent().GetRemoteExtentPrimaryStore() == storeIDs[2] // only healthy store
	}
	succ := common.SpinWaitOnCondition(cond, 60*time.Second)
	s.True(succ, "timed out waiting for extentMon to fix remoteZoneReplicationJob")
}

func (s *ExtentStateMonitorSuite) TestExtentMonitor() {

	var err error
	var destinations []*shared.DestinationDescription

	for i := 0; i < 2; i++ {
		path := s.generateName("/test/extent.mon/dest-" + strconv.Itoa(i))
		desc, errCD := s.createDestination(path)
		s.Nil(errCD, "Failed to create destination")
		destinations = append(destinations, desc)
	}

	outHostIDs := []string{uuid.New(), uuid.New(), uuid.New()}
	inHostIDs := []string{uuid.New(), uuid.New()}
	storeIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}

	for _, dst := range destinations {

		dstID := dst.GetDestinationUUID()

		extentIDs := []string{uuid.New(), uuid.New(), uuid.New()}

		// healthy extent
		_, err = s.mcp.context.mm.CreateExtent(dstID, extentIDs[0], inHostIDs[0], storeIDs[0:3])
		s.Nil(err, "Failed to create extent")
		// bad input host
		_, err = s.mcp.context.mm.CreateExtent(dstID, extentIDs[1], inHostIDs[1], storeIDs[0:3])
		s.Nil(err, "Failed to create extent")
		// bad store host
		_, err = s.mcp.context.mm.CreateExtent(dstID, extentIDs[2], inHostIDs[0], storeIDs[1:4])
		s.Nil(err, "Failed to create extent")

		name := dst.GetPath() + "/consumer"
		cgd, e := s.mcp.mClient.CreateConsumerGroup(nil, &shared.CreateConsumerGroupRequest{
			ConsumerGroupName: common.StringPtr(name),
			DestinationPath:   common.StringPtr(dst.GetPath()),
		})
		s.Nil(e, "Failed to create consumer group")

		s.mcp.context.mm.AddExtentToConsumerGroup(dstID, cgd.GetConsumerGroupUUID(), extentIDs[0], outHostIDs[0], storeIDs[0:3])
		s.mcp.context.mm.AddExtentToConsumerGroup(dstID, cgd.GetConsumerGroupUUID(), extentIDs[1], outHostIDs[1], storeIDs[0:3])
		s.mcp.context.mm.AddExtentToConsumerGroup(dstID, cgd.GetConsumerGroupUUID(), extentIDs[2], outHostIDs[2], storeIDs[1:4])
	}

	s.mClient.DeleteDestination(nil, &shared.DeleteDestinationRequest{Path: common.StringPtr(destinations[1].GetPath())})

	rpm := common.NewMockRingpopMonitor()
	dfdd := s.mcp.context.failureDetector.(*dfddImpl)

	stores := make([]*MockStoreService, len(storeIDs))
	for i := 0; i < len(storeIDs)-1; i++ {
		stores[i] = NewMockStoreService()
		stores[i].Start()
		rpm.Add(common.StoreServiceName, storeIDs[i], stores[i].hostPort)
		event := &common.RingpopListenerEvent{
			Key:  storeIDs[i],
			Type: common.HostAddedEvent,
		}
		dfdd.handleHostAddedEvent(common.StoreServiceName, event)
	}

	inputService := NewMockInputOutputService("inputhost")
	thriftService := admin.NewTChanInputHostAdminServer(inputService)
	inputService.Start(common.InputServiceName, thriftService)

	rpm.Add(common.InputServiceName, inHostIDs[0], inputService.hostPort)
	event := &common.RingpopListenerEvent{
		Key:  inHostIDs[0],
		Type: common.HostAddedEvent,
	}
	dfdd.handleHostAddedEvent(common.InputServiceName, event)

	outputService := NewMockInputOutputService("outputhost")
	thriftService = admin.NewTChanOutputHostAdminServer(outputService)
	outputService.Start(common.OutputServiceName, thriftService)

	rpm.Add(common.OutputServiceName, outHostIDs[0], outputService.hostPort)

	// Extent stat monitor run if its not primary
	rpm.Add(common.ControllerServiceName, s.mcp.context.hostID, "127.0.0.1")

	s.mcp.context.rpm = rpm

	oldTimeBtwnScans := IntervalBtwnScans
	IntervalBtwnScans = time.Millisecond * 10 // override for unit test

	oldTimeBtwnRetries := intervalBtwnRetries
	intervalBtwnRetries = time.Millisecond * 10

	s.mcp.context.extentMonitor.Start()

	retries := 0
	succ := false

	for retries < 600 {

		succ = true

		for i := 0; i < len(destinations); i++ {

			desc, err := s.mcp.mClient.ReadDestination(nil, &shared.ReadDestinationRequest{DestinationUUID: common.StringPtr(destinations[i].GetDestinationUUID())})
			s.Nil(err, "Failed to read destination")
			stats, err := s.mcp.context.mm.ListExtentsByDstIDStatus(desc.GetDestinationUUID(), nil)
			s.Nil(err, "Failed to list extents")

			for _, stat := range stats {

				var expectedStatus = shared.ExtentStatus_OPEN

				extent := stat.GetExtent()

				if desc.GetStatus() == shared.DestinationStatus_DELETING {
					expectedStatus = shared.ExtentStatus_SEALED
				}

				if extent.GetInputHostUUID() == inHostIDs[1] {
					expectedStatus = shared.ExtentStatus_SEALED
				}

				for _, store := range extent.GetStoreUUIDs() {
					if store == storeIDs[3] {
						expectedStatus = shared.ExtentStatus_SEALED
					}
				}

				if stat.GetStatus() != expectedStatus {
					log.Infof("Extent not in expected state dst=%v, ext=%v", desc.GetDestinationUUID(), extent.GetExtentUUID())
					succ = false
					goto retry
				}
			}

			if desc.GetStatus() == shared.DestinationStatus_DELETING {
				cg, e := s.mcp.mClient.ListConsumerGroups(nil, &shared.ListConsumerGroupRequest{
					DestinationUUID:   common.StringPtr(desc.GetDestinationUUID()),
					ConsumerGroupName: common.StringPtr(desc.GetPath() + "/consumer"),
				})
				s.Nil(e)
				if len(cg.GetConsumerGroups()) != 0 {
					succ = false
					log.Infof("Consumer group belonging to DELETING destination must be DELETED")
					goto retry
				}
			}
		}

		if succ {
			log.Infof("Test success ")
			break
		}

	retry:
		time.Sleep(time.Millisecond * 100)
		retries++
	}

	s.mcp.context.extentMonitor.Stop()
	s.True(succ, "ExtentStateMonitor failed to make state changes within timeout")

	IntervalBtwnScans = oldTimeBtwnScans
	intervalBtwnRetries = oldTimeBtwnRetries
}
