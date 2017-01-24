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

// MockTChanBInClient is a mock for TChanBInClient used for unit testing of cherami client
type MockTChanBInClient struct {
	mock.Mock
}

func (m *MockTChanBInClient) OpenPublisherStream(ctx thrift.Context) (cherami.BInOpenPublisherStreamOutCall, error) {
	args := m.Called(ctx)

	var stream cherami.BInOpenPublisherStreamOutCall
	if args.Get(0) != nil {
		stream = args.Get(0).(cherami.BInOpenPublisherStreamOutCall)
	}

	return stream, args.Error(1)
}

func (m *MockTChanBInClient) PutMessageBatch(ctx thrift.Context, request *cherami.PutMessageBatchRequest) (*cherami.PutMessageBatchResult_, error) {
	args := m.Called(ctx, request)

	var result *cherami.PutMessageBatchResult_
	if args.Get(0) != nil {
		result = args.Get(0).(*cherami.PutMessageBatchResult_)
	}

	return result, args.Error(1)
}
