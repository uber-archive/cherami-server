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
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/outputhost/load"
	"github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/shared"

	"testing"
)

type (
	LoadReporterSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}
)

func TestLoadReporterSuite(t *testing.T) {
	suite.Run(t, new(LoadReporterSuite))
}

func (s *LoadReporterSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *LoadReporterSuite) TestHostLoadReport() {

	hostMetrics := load.NewHostMetrics()
	hostMetrics.Add(load.HostMetricNumOpenConns, 666)
	hostMetrics.Add(load.HostMetricNumOpenExtents, 777)
	hostMetrics.Add(load.HostMetricMsgsOut, 8888)
	hostMetrics.Add(load.HostMetricBytesOut, 9999999)

	outputHost := &OutputHost{
		hostMetrics: hostMetrics,
		m3Client:    metrics.NewClient(common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig()), metrics.Outputhost),
	}

	reporter := new(outputTestLoadReporter)
	outputHost.Report(reporter)

	got := reporter.nodeMetrics
	s.Equal(int64(666), got.GetNumberOfConnections())
	s.Equal(int64(666), hostMetrics.Get(load.HostMetricNumOpenConns))

	s.Equal(int64(777), got.GetNumberOfActiveExtents())
	s.Equal(int64(777), hostMetrics.Get(load.HostMetricNumOpenExtents))

	s.Equal(int64(8888), got.GetOutgoingMessagesCounter())
	s.Equal(int64(0), hostMetrics.Get(load.HostMetricMsgsOut))

	s.Equal(int64(9999999), got.GetOutgoingBytesCounter())
	s.Equal(int64(0), hostMetrics.Get(load.HostMetricBytesOut))
}

func (s *LoadReporterSuite) TestConsumerGroupLoadReport() {

	cgMetrics := load.NewCGMetrics()
	cgMetrics.Add(load.CGMetricNumOpenConns, 666)
	cgMetrics.Add(load.CGMetricNumOpenExtents, 777)
	cgMetrics.Add(load.CGMetricMsgsOut, 8888)
	cgMetrics.Add(load.CGMetricBytesOut, 9999999)

	cgCache := &consumerGroupCache{
		cachedCGDesc: shared.ConsumerGroupDescription{
			DestinationUUID:   common.StringPtr(uuid.New()),
			ConsumerGroupUUID: common.StringPtr(uuid.New()),
		},
		cgMetrics: cgMetrics,
	}

	m3Client := metrics.NewClient(common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig()), metrics.Outputhost)
	cgCache.consumerM3Client = metrics.NewClientWithTags(m3Client, metrics.Outputhost, nil)

	reporter := new(outputTestLoadReporter)
	cgCache.Report(reporter)

	got := reporter.cgMetrics
	s.Equal(int64(666), got.GetNumberOfConnections())
	s.Equal(int64(666), cgMetrics.Get(load.HostMetricNumOpenConns))

	s.Equal(int64(777), got.GetNumberOfActiveExtents())
	s.Equal(int64(777), cgMetrics.Get(load.HostMetricNumOpenExtents))

	s.Equal(int64(8888), got.GetOutgoingMessagesCounter())
	s.Equal(int64(0), cgMetrics.Get(load.CGMetricMsgsOut))

	s.Equal(int64(9999999), got.GetOutgoingBytesCounter())
	s.Equal(int64(0), cgMetrics.Get(load.CGMetricBytesOut))
}

func (s *LoadReporterSuite) TestConsumerGroupExtentLoadReport() {

	extMetrics := load.NewExtentMetrics()
	extMetrics.Add(load.ExtentMetricMsgsOut, 8888)
	extMetrics.Add(load.ExtentMetricBytesOut, 9999999)

	extCache := &extentCache{
		cgUUID:      uuid.New(),
		extUUID:     uuid.New(),
		destUUID:    uuid.New(),
		loadMetrics: extMetrics,
	}

	reporter := new(outputTestLoadReporter)
	extCache.Report(reporter)

	got := reporter.cgExtentMetrics
	s.Equal(int64(8888), got.GetOutgoingMessagesCounter())
	s.Equal(int64(0), extMetrics.Get(load.ExtentMetricMsgsOut))

	s.Equal(int64(9999999), got.GetOutgoingBytesCounter())
	s.Equal(int64(0), extMetrics.Get(load.ExtentMetricBytesOut))
}

type outputTestLoadReporter struct {
	nodeMetrics     *controller.NodeMetrics
	cgMetrics       *controller.ConsumerGroupMetrics
	cgExtentMetrics *controller.ConsumerGroupExtentMetrics
}

func (r *outputTestLoadReporter) ReportHostMetric(metrics controller.NodeMetrics) error {
	r.nodeMetrics = &metrics
	return nil

}
func (r *outputTestLoadReporter) ReportDestinationMetric(destinationUUID string, metrics controller.DestinationMetrics) error {
	return nil
}
func (r *outputTestLoadReporter) ReportDestinationExtentMetric(destinationUUID string, extentUUID string, metrics controller.DestinationExtentMetrics) error {
	return nil
}
func (r *outputTestLoadReporter) ReportConsumerGroupMetric(destinationUUID string, consumerGroupUUID string, metrics controller.ConsumerGroupMetrics) error {
	r.cgMetrics = &metrics
	return nil
}
func (r *outputTestLoadReporter) ReportConsumerGroupExtentMetric(destinationUUID string, consumerGroupUUID string, extentUUID string, metrics controller.ConsumerGroupExtentMetrics) error {
	r.cgExtentMetrics = &metrics
	return nil
}
func (r *outputTestLoadReporter) ReportStoreExtentMetric(extentUUID string, metrics controller.StoreExtentMetrics) error {
	return nil
}
