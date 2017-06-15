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
	"sync"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/services/controllerhost/load"
	c "github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

type LoadReportAPISuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
	mcp   *Mcp
	rand  *rand.Rand
	clock *common.MockTimeSource
}

func TestLoadReportAPISuite(t *testing.T) {
	suite.Run(t, new(LoadReportAPISuite))
}

func (s *LoadReportAPISuite) SetupTest() {
	logger := bark.NewLoggerFromLogrus(log.New())
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil

	s.rand = rand.New(rand.NewSource(time.Now().UnixNano()))

	s.mcp = new(Mcp)
	s.mcp.context = new(Context)
	s.mcp.context.log = logger
	s.mcp.context.m3Client = &MockM3Metrics{}
	s.mcp.context.extentMonitor = newExtentStateMonitor(s.mcp.context)

	s.clock = common.NewMockTimeSource()
	s.mcp.context.loadMetrics = load.NewTimeSlotAggregator(s.clock, s.mcp.context.log)
	s.mcp.context.loadMetrics.Start()
	s.mcp.started = 1 // force started to true (or the api calls will fail)
}

func (s *LoadReportAPISuite) TearDownTest() {
	s.mcp.context.loadMetrics.Stop()
}

func (s *LoadReportAPISuite) awaitAsyncAggregation() {

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		aggr, _ := s.mcp.context.loadMetrics.(*load.TimeslotAggregator)
		for aggr.DataChannelLength() > 0 {
			time.Sleep(time.Millisecond * 2)
		}
	}()

	s.True(common.AwaitWaitGroup(wg, time.Minute), "Metrics aggregation timed out")
}

func (s *LoadReportAPISuite) TestReportHostMetric() {

	hostIDs := []string{uuid.New(), uuid.New()}
	reports := make([]*c.NodeMetrics, 0, len(hostIDs))

	for _, hostID := range hostIDs {

		m := &c.NodeMetrics{
			CPU:                     common.Int64Ptr(s.rand.Int63n(100)),
			Memory:                  common.Int64Ptr(s.rand.Int63n(1024 * 1024 * 1024)),
			RemainingDiskSpace:      common.Int64Ptr(s.rand.Int63n(1024 * 1024 * 1024 * 1024)),
			NumberOfActiveExtents:   common.Int64Ptr(s.rand.Int63n(10000)),
			NumberOfConnections:     common.Int64Ptr(s.rand.Int63n(2000)),
			IncomingMessagesCounter: common.Int64Ptr(s.rand.Int63n(1000)),
			OutgoingMessagesCounter: common.Int64Ptr(s.rand.Int63n(3000)),
			IncomingBytesCounter:    common.Int64Ptr(s.rand.Int63n(1024 * 1024 * 2048)),
			OutgoingBytesCounter:    common.Int64Ptr(s.rand.Int63n(1024 * 1024 * 4096)),
		}

		req := &c.ReportNodeMetricRequest{
			HostId:    common.StringPtr(hostID),
			Timestamp: common.Int64Ptr(s.clock.Now().UnixNano()),
			Metrics:   m,
		}
		for i := 0; i < 10; i++ {
			err := s.mcp.ReportNodeMetric(nil, req)
			s.Nil(err, "ReportNodeMetric failed with error")
		}
		reports = append(reports, m)
	}

	s.awaitAsyncAggregation()
	s.clock.Advance(time.Minute)

	for i, hostID := range hostIDs {

		report := reports[i]

		value, err := s.mcp.context.loadMetrics.Get(hostID, load.EmptyTag, load.RemDiskSpaceBytes, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetRemainingDiskSpace(), value, "Wrong value for RemDiskSpaceBytes")

		value, err = s.mcp.context.loadMetrics.Get(hostID, load.EmptyTag, load.NumExtentsActive, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetNumberOfActiveExtents(), value, "Wrong value for NumExtentsActive")

		value, err = s.mcp.context.loadMetrics.Get(hostID, load.EmptyTag, load.NumConns, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetNumberOfConnections(), value, "Wrong value for NumConns")

		value, err = s.mcp.context.loadMetrics.Get(hostID, load.EmptyTag, load.MsgsInPerSec, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetIncomingMessagesCounter(), value, "Wrong value for MsgsInPerSec")

		value, err = s.mcp.context.loadMetrics.Get(hostID, load.EmptyTag, load.MsgsOutPerSec, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetOutgoingMessagesCounter(), value, "Wrong value for MsgsOutPerSec")

		value, err = s.mcp.context.loadMetrics.Get(hostID, load.EmptyTag, load.BytesInPerSec, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetIncomingBytesCounter(), value, "Wrong value for BytesInPerSec")

		value, err = s.mcp.context.loadMetrics.Get(hostID, load.EmptyTag, load.BytesOutPerSec, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetOutgoingBytesCounter(), value, "Wrong value for BytesOutPerSec")
	}
}

func (s *LoadReportAPISuite) TestReportDestinationMetric() {

	dstID := uuid.New()
	hostIDs := []string{uuid.New(), uuid.New()}
	reports := make([]*c.DestinationMetrics, 0, len(hostIDs))

	for _, hostID := range hostIDs {

		m := &c.DestinationMetrics{
			NumberOfActiveExtents:   common.Int64Ptr(s.rand.Int63n(10000)),
			NumberOfConnections:     common.Int64Ptr(s.rand.Int63n(2000)),
			IncomingMessagesCounter: common.Int64Ptr(s.rand.Int63n(1000)),
			IncomingBytesCounter:    common.Int64Ptr(s.rand.Int63n(1024 * 1024 * 2048)),
		}

		req := &c.ReportDestinationMetricRequest{
			HostId:          common.StringPtr(hostID),
			DestinationUUID: common.StringPtr(dstID),
			Timestamp:       common.Int64Ptr(s.clock.Now().UnixNano()),
			Metrics:         m,
		}
		for i := 0; i < 10; i++ {
			err := s.mcp.ReportDestinationMetric(nil, req)
			s.Nil(err, "ReportDestinationMetric failed with error")
		}
		reports = append(reports, m)
	}

	s.awaitAsyncAggregation()
	s.clock.Advance(time.Minute)

	for i, hostID := range hostIDs {

		report := reports[i]

		value, err := s.mcp.context.loadMetrics.Get(hostID, dstID, load.NumExtentsActive, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetNumberOfActiveExtents(), value, "Wrong value for NumExtentsActive")

		value, err = s.mcp.context.loadMetrics.Get(hostID, dstID, load.NumConns, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetNumberOfConnections(), value, "Wrong value for NumConns")

		value, err = s.mcp.context.loadMetrics.Get(hostID, dstID, load.MsgsInPerSec, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetIncomingMessagesCounter(), value, "Wrong value for MsgsInPerSec")

		value, err = s.mcp.context.loadMetrics.Get(hostID, dstID, load.BytesInPerSec, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetIncomingBytesCounter(), value, "Wrong value for BytesInPerSec")
	}
}

func (s *LoadReportAPISuite) TestReportDestinationExtentMetric() {

	extID := uuid.New()
	dstID := uuid.New()
	hostIDs := []string{uuid.New(), uuid.New()}
	reports := make([]*c.DestinationExtentMetrics, 0, len(hostIDs))

	for _, hostID := range hostIDs {

		m := &c.DestinationExtentMetrics{
			IncomingMessagesCounter: common.Int64Ptr(s.rand.Int63n(1000)),
			IncomingBytesCounter:    common.Int64Ptr(s.rand.Int63n(1024 * 1024 * 2048)),
		}

		req := &c.ReportDestinationExtentMetricRequest{
			HostId:          common.StringPtr(hostID),
			DestinationUUID: common.StringPtr(dstID),
			ExtentUUID:      common.StringPtr(extID),
			Timestamp:       common.Int64Ptr(s.clock.Now().UnixNano()),
			Metrics:         m,
		}
		for i := 0; i < 10; i++ {
			err := s.mcp.ReportDestinationExtentMetric(nil, req)
			s.Nil(err, "ReportDestinationExtentMetric failed with error")
		}
		reports = append(reports, m)
	}

	s.awaitAsyncAggregation()
	s.clock.Advance(time.Minute)

	for i, hostID := range hostIDs {

		report := reports[i]

		value, err := s.mcp.context.loadMetrics.Get(hostID, extID, load.MsgsInPerSec, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetIncomingMessagesCounter(), value, "Wrong value for MsgsInPerSec")

		value, err = s.mcp.context.loadMetrics.Get(hostID, extID, load.BytesInPerSec, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetIncomingBytesCounter(), value, "Wrong value for BytesInPerSec")
	}
}

func (s *LoadReportAPISuite) TestReportConsumerGroupMetric() {

	cgID := uuid.New()
	dstID := uuid.New()
	hostIDs := []string{uuid.New(), uuid.New()}
	reports := make([]*c.ConsumerGroupMetrics, 0, len(hostIDs))

	for _, hostID := range hostIDs {

		m := &c.ConsumerGroupMetrics{
			NumberOfActiveExtents:   common.Int64Ptr(s.rand.Int63n(10000)),
			NumberOfConnections:     common.Int64Ptr(s.rand.Int63n(2000)),
			OutgoingMessagesCounter: common.Int64Ptr(s.rand.Int63n(1000)),
			OutgoingBytesCounter:    common.Int64Ptr(s.rand.Int63n(1024 * 1024 * 2048)),
		}

		req := &c.ReportConsumerGroupMetricRequest{
			HostId:            common.StringPtr(hostID),
			DestinationUUID:   common.StringPtr(dstID),
			ConsumerGroupUUID: common.StringPtr(cgID),
			Timestamp:         common.Int64Ptr(s.clock.Now().UnixNano()),
			Metrics:           m,
		}
		for i := 0; i < 10; i++ {
			err := s.mcp.ReportConsumerGroupMetric(nil, req)
			s.Nil(err, "ReportConsumerGroupMetric failed with error")
		}
		reports = append(reports, m)
	}

	s.awaitAsyncAggregation()
	s.clock.Advance(time.Minute)

	for i, hostID := range hostIDs {

		report := reports[i]

		value, err := s.mcp.context.loadMetrics.Get(hostID, cgID, load.NumExtentsActive, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetNumberOfActiveExtents(), value, "Wrong value for NumExtentsActive")

		value, err = s.mcp.context.loadMetrics.Get(hostID, cgID, load.NumConns, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetNumberOfConnections(), value, "Wrong value for NumConns")

		value, err = s.mcp.context.loadMetrics.Get(hostID, cgID, load.MsgsOutPerSec, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetOutgoingMessagesCounter(), value, "Wrong value for MsgsOutPerSec")

		value, err = s.mcp.context.loadMetrics.Get(hostID, cgID, load.BytesOutPerSec, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetOutgoingBytesCounter(), value, "Wrong value for BytesOutPerSec")
	}
}

func (s *LoadReportAPISuite) TestReportConsumerGroupExtentMetric() {

	cgID := uuid.New()
	dstID := uuid.New()
	extID := uuid.New()
	hostIDs := []string{uuid.New(), uuid.New()}
	reports := make([]*c.ConsumerGroupExtentMetrics, 0, len(hostIDs))

	for _, hostID := range hostIDs {

		m := &c.ConsumerGroupExtentMetrics{
			OutgoingMessagesCounter: common.Int64Ptr(s.rand.Int63n(1000)),
			OutgoingBytesCounter:    common.Int64Ptr(s.rand.Int63n(1024 * 1024 * 2048)),
		}

		req := &c.ReportConsumerGroupExtentMetricRequest{
			HostId:            common.StringPtr(hostID),
			ExtentUUID:        common.StringPtr(extID),
			DestinationUUID:   common.StringPtr(dstID),
			ConsumerGroupUUID: common.StringPtr(cgID),
			Timestamp:         common.Int64Ptr(s.clock.Now().UnixNano()),
			Metrics:           m,
		}
		for i := 0; i < 10; i++ {
			err := s.mcp.ReportConsumerGroupExtentMetric(nil, req)
			s.Nil(err, "ReportConsumerGroupExtentMetric failed with error")
		}
		reports = append(reports, m)
	}

	s.awaitAsyncAggregation()
	s.clock.Advance(time.Minute)

	for i, hostID := range hostIDs {

		report := reports[i]

		value, err := s.mcp.context.loadMetrics.Get(hostID, extID, load.MsgsOutPerSec, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetOutgoingMessagesCounter(), value, "Wrong value for MsgsOutPerSec")

		value, err = s.mcp.context.loadMetrics.Get(hostID, extID, load.BytesOutPerSec, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetOutgoingBytesCounter(), value, "Wrong value for BytesOutPerSec")
	}
}

func (s *LoadReportAPISuite) TestReportStoreExtentMetric() {

	status := shared.ExtentStatus(shared.ExtentStatus_SEALED)
	extentIDs := []string{uuid.New(), uuid.New()}
	hostIDs := []string{uuid.New(), uuid.New()}
	reports := make([]*c.StoreExtentMetrics, 0, len(hostIDs))

	for i, hostID := range hostIDs {

		m := &c.StoreExtentMetrics{
			NumberOfConnections:     common.Int64Ptr(s.rand.Int63n(2000)),
			IncomingMessagesCounter: common.Int64Ptr(s.rand.Int63n(1000)),
			OutgoingMessagesCounter: common.Int64Ptr(s.rand.Int63n(3000)),
			IncomingBytesCounter:    common.Int64Ptr(s.rand.Int63n(1024 * 1024 * 2048)),
			OutgoingBytesCounter:    common.Int64Ptr(s.rand.Int63n(1024 * 1024 * 4096)),
			ExtentStatus:            &status,
		}

		req := &c.ReportStoreExtentMetricRequest{
			StoreId:    common.StringPtr(hostID),
			ExtentUUID: common.StringPtr(extentIDs[i]),
			Timestamp:  common.Int64Ptr(s.clock.Now().UnixNano()),
			Metrics:    m,
		}
		for i := 0; i < 10; i++ {
			err := s.mcp.ReportStoreExtentMetric(nil, req)
			s.Nil(err, "ReportNodeMetric failed with error")
		}
		reports = append(reports, m)
	}

	s.awaitAsyncAggregation()
	s.clock.Advance(time.Minute)

	for i, hostID := range hostIDs {

		report := reports[i]

		value, err := s.mcp.context.loadMetrics.Get(hostID, extentIDs[i], load.NumConns, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetNumberOfConnections(), value, "Wrong value for NumConns")

		value, err = s.mcp.context.loadMetrics.Get(hostID, extentIDs[i], load.MsgsInPerSec, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetIncomingMessagesCounter(), value, "Wrong value for MsgsInPerSec")

		value, err = s.mcp.context.loadMetrics.Get(hostID, extentIDs[i], load.MsgsOutPerSec, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetOutgoingMessagesCounter(), value, "Wrong value for MsgsOutPerSec")

		value, err = s.mcp.context.loadMetrics.Get(hostID, extentIDs[i], load.BytesInPerSec, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetIncomingBytesCounter(), value, "Wrong value for BytesInPerSec")

		value, err = s.mcp.context.loadMetrics.Get(hostID, extentIDs[i], load.BytesOutPerSec, load.OneMinAvg)
		s.Nil(err, "Unexpected error from metrics aggregator")
		s.Equal(report.GetOutgoingBytesCounter(), value, "Wrong value for BytesOutPerSec")

		entry, ok := s.mcp.context.extentMonitor.storeExtents.Get(buildExtentCacheKey(hostID, extentIDs[i])).(*extentCacheEntry)
		s.True(ok, "ReportStoreExtentMetric failed to heartbeat to extent monitor")
		s.Equal(shared.ExtentStatus_SEALED, entry.status, "ReportStoreExtentMetric reported wrong extent status")
	}
}
