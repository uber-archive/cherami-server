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

package cherami

import (
	"github.com/uber/cherami-thrift/.generated/go/cherami"

	"github.com/stretchr/testify/mock"
	"github.com/uber/tchannel-go/thrift"
)

// MockBOutOpenConsumerStreamOutCall is a mock for BOutOpenConsumerStreamOutCall used for unit testing of cherami client
type MockTChanBOutClient struct {
	mock.Mock
}

func (m *MockTChanBOutClient) AckMessages(ctx thrift.Context, ackRequest *cherami.AckMessagesRequest) error {
	args := m.Called(ctx, ackRequest)

	return args.Error(0)
}

func (m *MockTChanBOutClient) OpenConsumerStream(ctx thrift.Context) (cherami.BOutOpenConsumerStreamOutCall, error) {
	args := m.Called(ctx)

	var stream cherami.BOutOpenConsumerStreamOutCall
	if args.Get(0) != nil {
		stream = args.Get(0).(cherami.BOutOpenConsumerStreamOutCall)
	}

	return stream, args.Error(1)
}

func (m *MockTChanBOutClient) ReceiveMessageBatch(ctx thrift.Context, request *cherami.ReceiveMessageBatchRequest) (*cherami.ReceiveMessageBatchResult_, error) {
	args := m.Called(ctx, request)

	var result *cherami.ReceiveMessageBatchResult_
	if args.Get(0) != nil {
		result = args.Get(0).(*cherami.ReceiveMessageBatchResult_)
	}

	return result, args.Error(1)
}
