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

package mocks

import (
	"github.com/stretchr/testify/mock"
	cli "github.com/uber/cherami-client-go/client/cherami"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
)

// MockCheramiClient is the mock of cli.MockCheramiClient interface
type MockCheramiClient struct {
	mock.Mock
}

// MockPublisher is the mock of cli.Publisher interface
type MockPublisher struct {
	mock.Mock
}

// MockConsumer is the mock of cli.Consumer interface
type MockConsumer struct {
	mock.Mock
}

// MockDelivery is the mock of cli.Delivery interface
type MockDelivery struct {
	mock.Mock
}

func (m *MockCheramiClient) Close() {

}

func (m *MockCheramiClient) CreateConsumerGroup(request *cherami.CreateConsumerGroupRequest) (*cherami.ConsumerGroupDescription, error) {
	args := m.Called(request)
	return args.Get(0).(*cherami.ConsumerGroupDescription), args.Error(1)
}

func (m *MockCheramiClient) CreateDestination(request *cherami.CreateDestinationRequest) (*cherami.DestinationDescription, error) {
	args := m.Called(request)
	return args.Get(0).(*cherami.DestinationDescription), args.Error(1)
}

func (m *MockCheramiClient) CreateConsumer(request *cli.CreateConsumerRequest) cli.Consumer {
	args := m.Called(request)
	return args.Get(0).(cli.Consumer)
}

func (m *MockCheramiClient) CreatePublisher(request *cli.CreatePublisherRequest) cli.Publisher {
	args := m.Called(request)
	return args.Get(0).(cli.Publisher)
}

func (m *MockCheramiClient) CreateTaskScheduler(request *cli.CreateTaskSchedulerRequest) cli.TaskScheduler {
	args := m.Called(request)
	return args.Get(0).(cli.TaskScheduler)
}

func (m *MockCheramiClient) CreateTaskExecutor(request *cli.CreateTaskExecutorRequest) cli.TaskExecutor {
	args := m.Called(request)
	return args.Get(0).(cli.TaskExecutor)
}

func (m *MockCheramiClient) DeleteConsumerGroup(request *cherami.DeleteConsumerGroupRequest) error {
	args := m.Called(request)
	return args.Error(0)
}

func (m *MockCheramiClient) DeleteDestination(request *cherami.DeleteDestinationRequest) error {
	args := m.Called(request)
	return args.Error(0)
}

func (m *MockCheramiClient) ReadConsumerGroup(request *cherami.ReadConsumerGroupRequest) (*cherami.ConsumerGroupDescription, error) {
	args := m.Called(request)
	return args.Get(0).(*cherami.ConsumerGroupDescription), args.Error(1)
}

func (m *MockCheramiClient) ReadDestination(request *cherami.ReadDestinationRequest) (*cherami.DestinationDescription, error) {
	args := m.Called(request)
	return args.Get(0).(*cherami.DestinationDescription), args.Error(1)
}

func (m *MockCheramiClient) UpdateConsumerGroup(request *cherami.UpdateConsumerGroupRequest) (*cherami.ConsumerGroupDescription, error) {
	args := m.Called(request)
	return args.Get(0).(*cherami.ConsumerGroupDescription), args.Error(1)
}

func (m *MockCheramiClient) UpdateDestination(request *cherami.UpdateDestinationRequest) (*cherami.DestinationDescription, error) {
	args := m.Called(request)
	return args.Get(0).(*cherami.DestinationDescription), args.Error(1)
}

func (m *MockCheramiClient) ListDestinations(request *cherami.ListDestinationsRequest) (*cherami.ListDestinationsResult_, error) {
	args := m.Called(request)
	return args.Get(0).(*cherami.ListDestinationsResult_), args.Error(1)
}

func (m *MockCheramiClient) ListConsumerGroups(request *cherami.ListConsumerGroupRequest) (*cherami.ListConsumerGroupResult_, error) {
	args := m.Called(request)
	return args.Get(0).(*cherami.ListConsumerGroupResult_), args.Error(1)
}

func (c *MockCheramiClient) GetQueueDepthInfo(request *cherami.GetQueueDepthInfoRequest) (*cherami.GetQueueDepthInfoResult_, error) {
	args := m.Called(request)
	return args.Get(0).(*cherami.GetQueueDepthInfo), args.Error(1)
}

func (m *MockPublisher) Open() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockPublisher) Close() {
}

func (m *MockPublisher) Publish(message *cli.PublisherMessage) *cli.PublisherReceipt {
	args := m.Called(message)
	return args.Get(0).(*cli.PublisherReceipt)
}

func (m *MockPublisher) PublishAsync(message *cli.PublisherMessage, done chan<- *cli.PublisherReceipt) (string, error) {
	args := m.Called(message, done)
	return args.Get(0).(string), args.Error(1)
}

func (m *MockConsumer) Open(deliveryCh chan cli.Delivery) (chan cli.Delivery, error) {
	args := m.Called(deliveryCh)
	return args.Get(0).(chan cli.Delivery), args.Error(1)
}

func (m *MockConsumer) Close() {
}

func (m *MockConsumer) AckDelivery(deliveryToken string) error {
	args := m.Called(deliveryToken)
	return args.Error(0)
}

func (m *MockConsumer) NackDelivery(deliveryToken string) error {
	args := m.Called(deliveryToken)
	return args.Error(0)
}

func (m *MockDelivery) GetMessage() *cherami.ConsumerMessage {
	args := m.Called()
	return args.Get(0).(*cherami.ConsumerMessage)
}

func (m *MockDelivery) GetDeliveryToken() string {
	args := m.Called()
	return args.Get(0).(string)
}

func (m *MockDelivery) Ack() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockDelivery) Nack() error {
	args := m.Called()
	return args.Error(0)
}
