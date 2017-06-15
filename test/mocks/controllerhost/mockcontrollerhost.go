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
	"github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/shared"

	"github.com/stretchr/testify/mock"
	"github.com/uber/tchannel-go/thrift"
)

type (
	// MockControllerHost is the mock implementation for controller host and used for testing
	MockControllerHost struct {
		mock.Mock
	}
)

// GetCapacities is the mock for corresponding ControllerHost API
func (m *MockControllerHost) GetCapacities(ctx thrift.Context, getCapacitiesRequest *controller.GetCapacitiesRequest) (*controller.GetCapacitiesResult_, error) {
	args := m.Called(ctx, getCapacitiesRequest)
	return args.Get(0).(*controller.GetCapacitiesResult_), args.Error(1)
}

// GetInputHosts is the mock for corresponding ControllerHost API
func (m *MockControllerHost) GetInputHosts(ctx thrift.Context, getHostsRequest *controller.GetInputHostsRequest) (*controller.GetInputHostsResult_, error) {
	args := m.Called(ctx, getHostsRequest)
	return args.Get(0).(*controller.GetInputHostsResult_), args.Error(1)
}

// GetOutputHosts is the mock for corresponding ControllerHost API
func (m *MockControllerHost) GetOutputHosts(ctx thrift.Context, getHostsRequest *controller.GetOutputHostsRequest) (*controller.GetOutputHostsResult_, error) {
	args := m.Called(ctx, getHostsRequest)
	return args.Get(0).(*controller.GetOutputHostsResult_), args.Error(1)
}

// CreateDestination is the mock for corresponding ControllerHost API
func (m *MockControllerHost) CreateDestination(ctx thrift.Context, createRequest *shared.CreateDestinationRequest) (*shared.DestinationDescription, error) {
	args := m.Called(ctx, createRequest)
	return args.Get(0).(*shared.DestinationDescription), args.Error(1)
}

// GetQueueDepthInfo to return queue depth backlog infor for consumer group
func (m *MockControllerHost) GetQueueDepthInfo(ctx thrift.Context, infoRequest *controller.GetQueueDepthInfoRequest) (*controller.GetQueueDepthInfoResult_, error) {
	args := m.Called(ctx, infoRequest)
	return args.Get(0).(*controller.GetQueueDepthInfoResult_), args.Error(1)
}

// UpdateDestination is the mock for corresponding ControllerHost API
func (m *MockControllerHost) UpdateDestination(ctx thrift.Context, updateRequest *shared.UpdateDestinationRequest) (*shared.DestinationDescription, error) {
	args := m.Called(ctx, updateRequest)
	return args.Get(0).(*shared.DestinationDescription), args.Error(1)
}

// DeleteDestination is the mock for corresponding ControllerHost API
func (m *MockControllerHost) DeleteDestination(ctx thrift.Context, deleteRequest *shared.DeleteDestinationRequest) error {
	args := m.Called(ctx, deleteRequest)
	return args.Error(0)
}

// CreateConsumerGroup is the mock for corresponding ControllerHost API
func (m *MockControllerHost) CreateConsumerGroup(ctx thrift.Context, createRequest *shared.CreateConsumerGroupRequest) (*shared.ConsumerGroupDescription, error) {
	args := m.Called(ctx, createRequest)
	return args.Get(0).(*shared.ConsumerGroupDescription), args.Error(1)
}

// UpdateConsumerGroup is the mock for corresponding ControllerHost API
func (m *MockControllerHost) UpdateConsumerGroup(ctx thrift.Context, updateRequest *shared.UpdateConsumerGroupRequest) (*shared.ConsumerGroupDescription, error) {
	args := m.Called(ctx, updateRequest)
	return args.Get(0).(*shared.ConsumerGroupDescription), args.Error(1)
}

// DeleteConsumerGroup is the mock for corresponding ControllerHost API
func (m *MockControllerHost) DeleteConsumerGroup(ctx thrift.Context, deleteRequest *shared.DeleteConsumerGroupRequest) error {
	args := m.Called(ctx, deleteRequest)
	return args.Error(0)
}

// CreateRemoteZoneExtent is the mock for corresponding ControllerHost API
func (m *MockControllerHost) CreateRemoteZoneExtent(ctx thrift.Context, createRequest *shared.CreateExtentRequest) (*shared.CreateExtentResult_, error) {
	args := m.Called(ctx, createRequest)
	return args.Get(0).(*shared.CreateExtentResult_), args.Error(1)
}

// CreateRemoteZoneExtent is the mock for corresponding ControllerHost API
func (m *MockControllerHost) CreateRemoteZoneConsumerGroupExtent(ctx thrift.Context, createRequest *shared.CreateConsumerGroupExtentRequest) error {
	args := m.Called(ctx, createRequest)
	return args.Error(0)
}

// RemoveCapacities is the mock for corresponding ControllerHost API
func (m *MockControllerHost) RemoveCapacities(ctx thrift.Context, removeCapacitiesRequest *controller.RemoveCapacitiesRequest) error {
	args := m.Called(ctx, removeCapacitiesRequest)
	return args.Error(0)
}

// ReportConsumerGroupExtentMetric is the mock for corresponding ControllerHost API
func (m *MockControllerHost) ReportConsumerGroupExtentMetric(ctx thrift.Context, reportMetricRequest *controller.ReportConsumerGroupExtentMetricRequest) error {
	args := m.Called(ctx, reportMetricRequest)
	return args.Error(0)
}

// ReportConsumerGroupMetric is the mock for corresponding ControllerHost API
func (m *MockControllerHost) ReportConsumerGroupMetric(ctx thrift.Context, reportMetricRequest *controller.ReportConsumerGroupMetricRequest) error {
	args := m.Called(ctx, reportMetricRequest)
	return args.Error(0)
}

// ReportDestinationExtentMetric is the mock for corresponding ControllerHost API
func (m *MockControllerHost) ReportDestinationExtentMetric(ctx thrift.Context, reportMetricRequest *controller.ReportDestinationExtentMetricRequest) error {
	args := m.Called(ctx, reportMetricRequest)
	return args.Error(0)
}

// ReportDestinationMetric is the mock for corresponding ControllerHost API
func (m *MockControllerHost) ReportDestinationMetric(ctx thrift.Context, reportMetricRequest *controller.ReportDestinationMetricRequest) error {
	args := m.Called(ctx, reportMetricRequest)
	return args.Error(0)
}

// ReportNodeMetric is the mock for corresponding ControllerHost API
func (m *MockControllerHost) ReportNodeMetric(ctx thrift.Context, reportMetricRequest *controller.ReportNodeMetricRequest) error {
	args := m.Called(ctx, reportMetricRequest)
	return args.Error(0)
}

// ReportStoreExtentMetric is the mock for corresponding ControllerHost API
func (m *MockControllerHost) ReportStoreExtentMetric(ctx thrift.Context, reportMetricRequest *controller.ReportStoreExtentMetricRequest) error {
	args := m.Called(ctx, reportMetricRequest)
	return args.Error(0)
}

// UpsertInputHostCapacities is the mock for corresponding ControllerHost API
func (m *MockControllerHost) UpsertInputHostCapacities(ctx thrift.Context, upsertCapacitiesRequest *controller.UpsertInputHostCapacitiesRequest) error {
	args := m.Called(ctx, upsertCapacitiesRequest)
	return args.Error(0)
}

// UpsertOutputHostCapacities is the mock for corresponding ControllerHost API
func (m *MockControllerHost) UpsertOutputHostCapacities(ctx thrift.Context, upsertCapacitiesRequest *controller.UpsertOutputHostCapacitiesRequest) error {
	args := m.Called(ctx, upsertCapacitiesRequest)
	return args.Error(0)
}

// UpsertStoreCapacities is the mock for corresponding ControllerHost API
func (m *MockControllerHost) UpsertStoreCapacities(ctx thrift.Context, upsertCapacitiesRequest *controller.UpsertStoreCapacitiesRequest) error {
	args := m.Called(ctx, upsertCapacitiesRequest)
	return args.Error(0)
}
