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
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cherami-thrift/.generated/go/controller"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"
)

var (
	// ErrLoadReportThrottled is the error returned by LoadReporter when it runs out of tokens to serve the request
	ErrLoadReportThrottled = errors.New("too many load reports from the host")
)

type (
	// LoadReporterSource is implemented by any component reporting load to controller
	LoadReporterSource interface {
		Report(reporter LoadReporter)
	}

	// LoadReporterDaemon is used for periodically reporting load to controller
	LoadReporterDaemon interface {
		Daemon
		GetReporter() LoadReporter
	}

	// LoadReporterDaemonFactory is used to create a daemon task for reporting load to controller
	LoadReporterDaemonFactory interface {
		CreateReporter(interval time.Duration, source LoadReporterSource, logger bark.Logger) LoadReporterDaemon
	}

	// LoadReporter is used by each component interested in reporting load to Controller
	LoadReporter interface {
		ReportHostMetric(metrics controller.NodeMetrics) error
		ReportDestinationMetric(destinationUUID string, metrics controller.DestinationMetrics) error
		ReportDestinationExtentMetric(destinationUUID string, extentUUID string, metrics controller.DestinationExtentMetrics) error
		ReportConsumerGroupMetric(destinationUUID string, consumerGroupUUID string, metrics controller.ConsumerGroupMetrics) error
		ReportConsumerGroupExtentMetric(destinationUUID string, consumerGroupUUID string, extentUUID string, metrics controller.ConsumerGroupExtentMetrics) error
		ReportStoreExtentMetric(extentUUID string, metrics controller.StoreExtentMetrics) error
	}

	loadReporterDaemonFactoryImpl struct {
		reporter      LoadReporter
		tickerFactory TickerSourceFactory
		logger        bark.Logger
	}

	loadReporterDaemonImpl struct {
		interval      time.Duration
		tickerFactory TickerSourceFactory
		source        LoadReporterSource
		reporter      LoadReporter
		started       int32
		reported      bool
		shutdownCh    chan struct{}
		shutdownWG    sync.WaitGroup
		logger        bark.Logger
	}

	loadReporterImpl struct {
		hostUUID         string
		sku              controller.SKU
		role             controller.Role
		requestThrottler TokenBucket
		clientFactory    ClientFactory
		logger           bark.Logger
	}
)

// ReportExtentMetric rate limiting: throttle requests beyond 1000 every second
const (
	reportLoadMetricThrottleRequests = 1000
	reportLoadMetricThriftTimeout    = 2 * time.Second
)

// NewLoadReporterDaemonFactory is used to create a factory for creating LoadReporterDaemon
func NewLoadReporterDaemonFactory(hostUUID string, sku controller.SKU, role controller.Role,
	clientFactory ClientFactory, tickerFactory TickerSourceFactory, tokenBucketFactory TokenBucketFactory, logger bark.Logger) LoadReporterDaemonFactory {
	r := &loadReporterImpl{
		hostUUID:         hostUUID,
		sku:              sku,
		role:             role,
		clientFactory:    clientFactory,
		requestThrottler: tokenBucketFactory.CreateTokenBucket(reportLoadMetricThrottleRequests, NewRealTimeSource()),
		logger:           logger,
	}

	return &loadReporterDaemonFactoryImpl{
		reporter:      r,
		tickerFactory: tickerFactory,
		logger:        logger,
	}
}

// CreateReporter creates a daemon task for reporting load metric
func (f *loadReporterDaemonFactoryImpl) CreateReporter(interval time.Duration, source LoadReporterSource, logger bark.Logger) LoadReporterDaemon {
	return &loadReporterDaemonImpl{
		interval:      interval,
		tickerFactory: f.tickerFactory,
		source:        source,
		reporter:      f.reporter,
		shutdownCh:    make(chan struct{}),
		logger:        logger,
	}
}

// Start is used to start the daemon task to start reporting metric at regular interval
func (d *loadReporterDaemonImpl) Start() {
	if !atomic.CompareAndSwapInt32(&d.started, 0, 1) {
		return
	}

	d.shutdownWG.Add(1)
	go d.reporterPump()

	d.logger.Info("Load reporter started.")
}

// Stop is used to stop the daemon task from reporting metric at regular interval
func (d *loadReporterDaemonImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&d.started, 1, 0) {
		return
	}

	close(d.shutdownCh)
	if success := AwaitWaitGroup(&d.shutdownWG, time.Minute); !success {
		d.logger.Warn("Timed out waiting to shutdown load reporter.")
	}

	d.logger.Info("Load reporter stopped.")
}

// GetReporter is used to get the underneath load reporter
func (d *loadReporterDaemonImpl) GetReporter() LoadReporter {
	return d.reporter
}

func (d *loadReporterDaemonImpl) reporterPump() {

	defer d.shutdownWG.Done()

	reportTicker := d.tickerFactory.CreateTicker(d.interval)
	defer reportTicker.Stop()

	for {
		select {
		case <-reportTicker.Ticks():

			d.source.Report(d.reporter)
			d.reported = true

		case <-d.shutdownCh:

			// ensure we report at least once
			if !d.reported {
				d.source.Report(d.reporter)
				d.reported = true
			}

			d.logger.Info("Load reporter pump shutting down.")
			return
		}
	}
}

// ReportHostMetric is the API exposed by LoadReporter for reporting host load to controller
func (d *loadReporterImpl) ReportHostMetric(metrics controller.NodeMetrics) error {
	controllerClient, err := d.getControllerClient()
	if err != nil {
		return err
	}

	request := controller.NewReportNodeMetricRequest()
	request.HostId = StringPtr(d.hostUUID)
	request.Sku = SKUPtr(d.sku)
	request.Role = RolePtr(d.role)
	request.Metrics = NodeMetricsPtr(metrics)
	request.Timestamp = Int64Ptr(time.Now().UnixNano())

	ctx, cancel := thrift.NewContext(reportLoadMetricThriftTimeout)
	defer cancel()

	return controllerClient.ReportNodeMetric(ctx, request)
}

// ReportDestinationMetric is the API exposed by LoadReporter for reporting destination load to controller
func (d *loadReporterImpl) ReportDestinationMetric(destinationUUID string, metrics controller.DestinationMetrics) error {
	controllerClient, err := d.getControllerClient()
	if err != nil {
		return err
	}

	request := controller.NewReportDestinationMetricRequest()
	request.HostId = StringPtr(d.hostUUID)
	request.Sku = SKUPtr(d.sku)
	request.DestinationUUID = StringPtr(destinationUUID)
	request.Metrics = DestinationMetricsPtr(metrics)
	request.Timestamp = Int64Ptr(time.Now().UnixNano())

	ctx, cancel := thrift.NewContext(reportLoadMetricThriftTimeout)
	defer cancel()

	return controllerClient.ReportDestinationMetric(ctx, request)
}

// ReportDestinationExtentMetric is the API exposed by LoadReporter for reporting destination extent load to controller
func (d *loadReporterImpl) ReportDestinationExtentMetric(destinationUUID string, extentUUID string, metrics controller.DestinationExtentMetrics) error {
	controllerClient, err := d.getControllerClient()
	if err != nil {
		return err
	}

	request := controller.NewReportDestinationExtentMetricRequest()
	request.HostId = StringPtr(d.hostUUID)
	request.Sku = SKUPtr(d.sku)
	request.DestinationUUID = StringPtr(destinationUUID)
	request.ExtentUUID = StringPtr(extentUUID)
	request.Metrics = DestinationExtentMetricsPtr(metrics)
	request.Timestamp = Int64Ptr(time.Now().UnixNano())

	ctx, cancel := thrift.NewContext(reportLoadMetricThriftTimeout)
	defer cancel()

	return controllerClient.ReportDestinationExtentMetric(ctx, request)
}

// ReportConsumerGroupMetric is the API exposed by LoadReporter for reporting consumer group load to controller
func (d *loadReporterImpl) ReportConsumerGroupMetric(destinationUUID string, consumerGroupUUID string, metrics controller.ConsumerGroupMetrics) error {
	controllerClient, err := d.getControllerClient()
	if err != nil {
		return err
	}

	request := controller.NewReportConsumerGroupMetricRequest()
	request.HostId = StringPtr(d.hostUUID)
	request.Sku = SKUPtr(d.sku)
	request.DestinationUUID = StringPtr(destinationUUID)
	request.ConsumerGroupUUID = StringPtr(consumerGroupUUID)
	request.Metrics = ConsumerGroupMetricsPtr(metrics)
	request.Timestamp = Int64Ptr(time.Now().UnixNano())

	ctx, cancel := thrift.NewContext(reportLoadMetricThriftTimeout)
	defer cancel()

	return controllerClient.ReportConsumerGroupMetric(ctx, request)
}

// ReportConsumerGroupExtentMetric is the API exposed by LoadReporter for reporting consumer group extent load to controller
func (d *loadReporterImpl) ReportConsumerGroupExtentMetric(destinationUUID string, consumerGroupUUID string, extentUUID string, metrics controller.ConsumerGroupExtentMetrics) error {
	controllerClient, err := d.getControllerClient()
	if err != nil {
		return err
	}

	request := controller.NewReportConsumerGroupExtentMetricRequest()
	request.HostId = StringPtr(d.hostUUID)
	request.Sku = SKUPtr(d.sku)
	request.DestinationUUID = StringPtr(destinationUUID)
	request.ConsumerGroupUUID = StringPtr(consumerGroupUUID)
	request.ExtentUUID = StringPtr(extentUUID)
	request.Metrics = ConsumerGroupExtentMetricsPtr(metrics)
	request.Timestamp = Int64Ptr(time.Now().UnixNano())

	ctx, cancel := thrift.NewContext(reportLoadMetricThriftTimeout)
	defer cancel()

	return controllerClient.ReportConsumerGroupExtentMetric(ctx, request)
}

// ReportStoreExtentMetric is the API exposed by LoadReporter for reporting store extent load to controller
func (d *loadReporterImpl) ReportStoreExtentMetric(extentUUID string, metrics controller.StoreExtentMetrics) error {
	controllerClient, err := d.getControllerClient()
	if err != nil {
		return err
	}

	request := controller.NewReportStoreExtentMetricRequest()
	request.StoreId = StringPtr(d.hostUUID)
	request.Sku = SKUPtr(d.sku)
	request.ExtentUUID = StringPtr(extentUUID)
	request.Metrics = StoreExtentMetricsPtr(metrics)
	request.Timestamp = Int64Ptr(time.Now().UnixNano())

	ctx, cancel := thrift.NewContext(reportLoadMetricThriftTimeout)
	defer cancel()

	return controllerClient.ReportStoreExtentMetric(ctx, request)
}

func (d *loadReporterImpl) getControllerClient() (controller.TChanController, error) {
	if ok, _ := d.requestThrottler.TryConsume(1); !ok {
		return nil, ErrLoadReportThrottled
	}

	return d.clientFactory.GetControllerClient()
}
