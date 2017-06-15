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

package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"

	log "github.com/sirupsen/logrus"

	"github.com/uber/cherami-thrift/.generated/go/controller"

	"errors"

	"github.com/stretchr/testify/mock"
	mockcommon "github.com/uber/cherami-server/test/mocks/common"
	mockcontroller "github.com/uber/cherami-server/test/mocks/controllerhost"
)

type (
	LoadReporterSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}

	mockTickerFactory struct {
		mockSource *mockTickerSource
	}

	mockTokenBucketFactory struct {
		tb *mockcommon.MockTokenBucket
	}

	mockTickerSource struct {
		C chan time.Time
	}

	testReporterSource struct {
		f func(LoadReporter)
	}
)

func TestLoadReporterSuite(t *testing.T) {
	suite.Run(t, new(LoadReporterSuite))
}

func (s *LoadReporterSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *LoadReporterSuite) TestNodeMetricReporter() {
	mockClientFactory := new(mockcommon.MockClientFactory)
	mockTicker := newMockTicker()
	mockTokenBucket := new(mockcommon.MockTokenBucket)
	controllerClient := new(mockcontroller.MockControllerHost)
	source := newTestReporterSource(func(reporter LoadReporter) {
		metric := controller.NodeMetrics{
			CPU:                Int64Ptr(100),
			Memory:             Int64Ptr(10),
			RemainingDiskSpace: Int64Ptr(1),
		}

		reporter.ReportHostMetric(metric)
	})

	mockTokenBucket.On("TryConsume", mock.Anything).Return(true, time.Millisecond)
	mockClientFactory.On("GetControllerClient").Return(controllerClient, nil)
	controllerClient.On("ReportNodeMetric", mock.Anything, mock.Anything).Return(nil)

	factory := NewLoadReporterDaemonFactory("test", controller.SKU_Machine1, controller.Role_IN, mockClientFactory,
		newMockTickerFactory(mockTicker), newMockTokenBucketFactory(mockTokenBucket), bark.NewLoggerFromLogrus(log.New()))

	reporter := factory.CreateReporter(time.Second, source, bark.NewLoggerFromLogrus(log.New()))
	reporter.Start()
	mockTicker.tick(time.Now())
	reporter.Stop()

	mockClientFactory.AssertExpectations(s.T())
	mockTokenBucket.AssertExpectations(s.T())
	controllerClient.AssertExpectations(s.T())
}

func (s *LoadReporterSuite) TestDestinationMetricReporter() {
	mockClientFactory := new(mockcommon.MockClientFactory)
	mockTicker := newMockTicker()
	mockTokenBucket := new(mockcommon.MockTokenBucket)
	controllerClient := new(mockcontroller.MockControllerHost)
	source := newTestReporterSource(func(reporter LoadReporter) {
		metric := controller.DestinationMetrics{
			NumberOfActiveExtents: Int64Ptr(10),
			NumberOfConnections:   Int64Ptr(100),
		}

		reporter.ReportDestinationMetric("testDestination", metric)
	})

	mockTokenBucket.On("TryConsume", mock.Anything).Return(true, time.Millisecond)
	mockClientFactory.On("GetControllerClient").Return(controllerClient, nil)
	controllerClient.On("ReportDestinationMetric", mock.Anything, mock.Anything).Return(nil)

	factory := NewLoadReporterDaemonFactory("test", controller.SKU_Machine1, controller.Role_IN, mockClientFactory,
		newMockTickerFactory(mockTicker), newMockTokenBucketFactory(mockTokenBucket), bark.NewLoggerFromLogrus(log.New()))

	reporter := factory.CreateReporter(time.Second, source, bark.NewLoggerFromLogrus(log.New()))
	reporter.Start()
	mockTicker.tick(time.Now())
	reporter.Stop()

	mockClientFactory.AssertExpectations(s.T())
	mockTokenBucket.AssertExpectations(s.T())
	controllerClient.AssertExpectations(s.T())
}

func (s *LoadReporterSuite) TestDestinationExtentMetricReporter() {
	mockClientFactory := new(mockcommon.MockClientFactory)
	mockTicker := newMockTicker()
	mockTokenBucket := new(mockcommon.MockTokenBucket)
	controllerClient := new(mockcontroller.MockControllerHost)
	source := newTestReporterSource(func(reporter LoadReporter) {
		metric := controller.DestinationExtentMetrics{
			IncomingBytesCounter:    Int64Ptr(1000),
			IncomingMessagesCounter: Int64Ptr(100),
			PutMessageLatency:       Int64Ptr(2000),
		}

		reporter.ReportDestinationExtentMetric("testDestination", "testExtent", metric)
	})

	mockTokenBucket.On("TryConsume", mock.Anything).Return(true, time.Millisecond)
	mockClientFactory.On("GetControllerClient").Return(controllerClient, nil)
	controllerClient.On("ReportDestinationExtentMetric", mock.Anything, mock.Anything).Return(nil)

	factory := NewLoadReporterDaemonFactory("test", controller.SKU_Machine1, controller.Role_IN, mockClientFactory,
		newMockTickerFactory(mockTicker), newMockTokenBucketFactory(mockTokenBucket), bark.NewLoggerFromLogrus(log.New()))

	reporter := factory.CreateReporter(time.Second, source, bark.NewLoggerFromLogrus(log.New()))
	reporter.Start()
	mockTicker.tick(time.Now())
	reporter.Stop()

	mockClientFactory.AssertExpectations(s.T())
	mockTokenBucket.AssertExpectations(s.T())
	controllerClient.AssertExpectations(s.T())
}

func (s *LoadReporterSuite) TestConsumerGroupMetricReporter() {
	mockClientFactory := new(mockcommon.MockClientFactory)
	mockTicker := newMockTicker()
	mockTokenBucket := new(mockcommon.MockTokenBucket)
	controllerClient := new(mockcontroller.MockControllerHost)
	source := newTestReporterSource(func(reporter LoadReporter) {
		metric := controller.ConsumerGroupMetrics{
			NumberOfActiveExtents: Int64Ptr(10),
			NumberOfConnections:   Int64Ptr(100),
		}

		reporter.ReportConsumerGroupMetric("testDestination", "testConsumer", metric)
	})

	mockTokenBucket.On("TryConsume", mock.Anything).Return(true, time.Millisecond)
	mockClientFactory.On("GetControllerClient").Return(controllerClient, nil)
	controllerClient.On("ReportConsumerGroupMetric", mock.Anything, mock.Anything).Return(nil)

	factory := NewLoadReporterDaemonFactory("test", controller.SKU_Machine1, controller.Role_OUT, mockClientFactory,
		newMockTickerFactory(mockTicker), newMockTokenBucketFactory(mockTokenBucket), bark.NewLoggerFromLogrus(log.New()))

	reporter := factory.CreateReporter(time.Second, source, bark.NewLoggerFromLogrus(log.New()))
	reporter.Start()
	mockTicker.tick(time.Now())
	reporter.Stop()

	mockClientFactory.AssertExpectations(s.T())
	mockTokenBucket.AssertExpectations(s.T())
	controllerClient.AssertExpectations(s.T())
}

func (s *LoadReporterSuite) TestConsumerGroupExtentMetricReporter() {
	mockClientFactory := new(mockcommon.MockClientFactory)
	mockTicker := newMockTicker()
	mockTokenBucket := new(mockcommon.MockTokenBucket)
	controllerClient := new(mockcontroller.MockControllerHost)
	source := newTestReporterSource(func(reporter LoadReporter) {
		metric := controller.ConsumerGroupExtentMetrics{
			OutgoingBytesCounter:    Int64Ptr(1000),
			OutgoingMessagesCounter: Int64Ptr(100),
		}

		reporter.ReportConsumerGroupExtentMetric("testDestination", "testConsumer", "testExtent", metric)
	})

	mockTokenBucket.On("TryConsume", mock.Anything).Return(true, time.Millisecond)
	mockClientFactory.On("GetControllerClient").Return(controllerClient, nil)
	controllerClient.On("ReportConsumerGroupExtentMetric", mock.Anything, mock.Anything).Return(nil)

	factory := NewLoadReporterDaemonFactory("test", controller.SKU_Machine1, controller.Role_OUT, mockClientFactory,
		newMockTickerFactory(mockTicker), newMockTokenBucketFactory(mockTokenBucket), bark.NewLoggerFromLogrus(log.New()))

	reporter := factory.CreateReporter(time.Second, source, bark.NewLoggerFromLogrus(log.New()))
	reporter.Start()
	mockTicker.tick(time.Now())
	reporter.Stop()

	mockClientFactory.AssertExpectations(s.T())
	mockTokenBucket.AssertExpectations(s.T())
	controllerClient.AssertExpectations(s.T())
}

func (s *LoadReporterSuite) TestThrottling() {
	mockClientFactory := new(mockcommon.MockClientFactory)
	mockTicker := newMockTicker()
	mockTokenBucket := new(mockcommon.MockTokenBucket)
	source := newTestReporterSource(func(reporter LoadReporter) {
		metric := controller.NodeMetrics{
			CPU:                Int64Ptr(100),
			Memory:             Int64Ptr(10),
			RemainingDiskSpace: Int64Ptr(1),
		}

		reporter.ReportHostMetric(metric)
	})

	mockTokenBucket.On("TryConsume", mock.Anything).Return(false, time.Millisecond)

	factory := NewLoadReporterDaemonFactory("test", controller.SKU_Machine1, controller.Role_IN, mockClientFactory,
		newMockTickerFactory(mockTicker), newMockTokenBucketFactory(mockTokenBucket), bark.NewLoggerFromLogrus(log.New()))

	reporter := factory.CreateReporter(time.Second, source, bark.NewLoggerFromLogrus(log.New()))
	reporter.Start()
	mockTicker.tick(time.Now())
	reporter.Stop()

	mockClientFactory.AssertExpectations(s.T())
}

func (s *LoadReporterSuite) TestControllerClientFailure() {
	mockClientFactory := new(mockcommon.MockClientFactory)
	mockTicker := newMockTicker()
	mockTokenBucket := new(mockcommon.MockTokenBucket)
	source := newTestReporterSource(func(reporter LoadReporter) {
		metric := controller.NodeMetrics{
			CPU:                Int64Ptr(100),
			Memory:             Int64Ptr(10),
			RemainingDiskSpace: Int64Ptr(1),
		}

		reporter.ReportHostMetric(metric)
	})

	mockTokenBucket.On("TryConsume", mock.Anything).Return(true, time.Millisecond)
	mockClientFactory.On("GetControllerClient").Return(nil, errors.New("error getting client"))

	factory := NewLoadReporterDaemonFactory("test", controller.SKU_Machine1, controller.Role_IN, mockClientFactory,
		newMockTickerFactory(mockTicker), newMockTokenBucketFactory(mockTokenBucket), bark.NewLoggerFromLogrus(log.New()))

	reporter := factory.CreateReporter(time.Second, source, bark.NewLoggerFromLogrus(log.New()))
	reporter.Start()
	mockTicker.tick(time.Now())
	reporter.Stop()

	mockClientFactory.AssertExpectations(s.T())
	mockTokenBucket.AssertExpectations(s.T())
}

func (s *LoadReporterSuite) TestMultipleReports() {
	mockClientFactory := new(mockcommon.MockClientFactory)
	mockTicker := newMockTicker()
	mockTokenBucket := new(mockcommon.MockTokenBucket)
	controllerClient := new(mockcontroller.MockControllerHost)
	source := newTestReporterSource(func(reporter LoadReporter) {
		metric := controller.DestinationMetrics{
			NumberOfActiveExtents: Int64Ptr(10),
			NumberOfConnections:   Int64Ptr(100),
		}

		reporter.ReportDestinationMetric("testDestination", metric)
	})

	mockTokenBucket.On("TryConsume", mock.Anything).Return(true, time.Millisecond).Twice()
	mockClientFactory.On("GetControllerClient").Return(controllerClient, nil).Twice()
	controllerClient.On("ReportDestinationMetric", mock.Anything, mock.Anything).Return(nil).Twice()

	factory := NewLoadReporterDaemonFactory("test", controller.SKU_Machine1, controller.Role_IN, mockClientFactory,
		newMockTickerFactory(mockTicker), newMockTokenBucketFactory(mockTokenBucket), bark.NewLoggerFromLogrus(log.New()))

	reporter := factory.CreateReporter(time.Second, source, bark.NewLoggerFromLogrus(log.New()))
	reporter.Start()
	mockTicker.tick(time.Now())
	mockTicker.tick(time.Now())
	reporter.Stop()

	mockClientFactory.AssertExpectations(s.T())
	mockTokenBucket.AssertExpectations(s.T())
	controllerClient.AssertExpectations(s.T())
}

func (s *LoadReporterSuite) TestMultipleReporters() {
	mockClientFactory := new(mockcommon.MockClientFactory)
	mockTicker := newMockTicker()
	mockTokenBucket := new(mockcommon.MockTokenBucket)
	controllerClient := new(mockcontroller.MockControllerHost)
	hostSource := newTestReporterSource(func(reporter LoadReporter) {
		metric := controller.NodeMetrics{
			CPU:                Int64Ptr(100),
			Memory:             Int64Ptr(10),
			RemainingDiskSpace: Int64Ptr(1),
		}

		reporter.ReportHostMetric(metric)
	})

	destinationSource := newTestReporterSource(func(reporter LoadReporter) {
		metric := controller.DestinationMetrics{
			NumberOfActiveExtents: Int64Ptr(10),
			NumberOfConnections:   Int64Ptr(100),
		}

		reporter.ReportDestinationMetric("testDestination", metric)
	})

	destinationExtentSource := newTestReporterSource(func(reporter LoadReporter) {
		metric := controller.DestinationExtentMetrics{
			IncomingBytesCounter:    Int64Ptr(1000),
			IncomingMessagesCounter: Int64Ptr(100),
			PutMessageLatency:       Int64Ptr(2000),
		}

		reporter.ReportDestinationExtentMetric("testDestination", "testExtent", metric)
	})

	mockTokenBucket.On("TryConsume", mock.Anything).Return(true, time.Millisecond).Times(3)
	mockClientFactory.On("GetControllerClient").Return(controllerClient, nil).Times(3)
	controllerClient.On("ReportNodeMetric", mock.Anything, mock.Anything).Return(nil).Once()
	controllerClient.On("ReportDestinationMetric", mock.Anything, mock.Anything).Return(nil).Once()
	controllerClient.On("ReportDestinationExtentMetric", mock.Anything, mock.Anything).Return(nil).Once()

	factory := NewLoadReporterDaemonFactory("test", controller.SKU_Machine1, controller.Role_IN, mockClientFactory,
		newMockTickerFactory(mockTicker), newMockTokenBucketFactory(mockTokenBucket), bark.NewLoggerFromLogrus(log.New()))

	hostReporter := factory.CreateReporter(time.Second, hostSource, bark.NewLoggerFromLogrus(log.New()))
	hostReporter.Start()
	mockTicker.tick(time.Now())
	hostReporter.Stop()

	destinationReporter := factory.CreateReporter(time.Second, destinationSource, bark.NewLoggerFromLogrus(log.New()))
	destinationReporter.Start()
	mockTicker.tick(time.Now())
	destinationReporter.Stop()

	destinationExtentReporter := factory.CreateReporter(time.Second, destinationExtentSource, bark.NewLoggerFromLogrus(log.New()))
	destinationExtentReporter.Start()
	mockTicker.tick(time.Now())
	destinationExtentReporter.Stop()

	mockClientFactory.AssertExpectations(s.T())
	mockTokenBucket.AssertExpectations(s.T())
	controllerClient.AssertExpectations(s.T())
}

func newMockTickerFactory(mockSource *mockTickerSource) TickerSourceFactory {
	return &mockTickerFactory{
		mockSource: mockSource,
	}
}

func newMockTokenBucketFactory(tb *mockcommon.MockTokenBucket) TokenBucketFactory {
	return &mockTokenBucketFactory{
		tb: tb,
	}
}

func newMockTicker() *mockTickerSource {
	return &mockTickerSource{
		C: make(chan time.Time),
	}
}

func newTestReporterSource(f func(LoadReporter)) LoadReporterSource {
	return &testReporterSource{
		f: f,
	}
}

func (f *mockTickerFactory) CreateTicker(interval time.Duration) TickerSource {
	return f.mockSource
}

func (tbf *mockTokenBucketFactory) CreateTokenBucket(rps int, timeSource TimeSource) TokenBucket {
	return tbf.tb
}

func (t *mockTickerSource) Ticks() <-chan time.Time {
	return t.C
}

func (t *mockTickerSource) Stop() {
}

func (t *mockTickerSource) tick(tickTime time.Time) {
	t.C <- tickTime
}

func (r *testReporterSource) Report(reporter LoadReporter) {
	r.f(reporter)
}
