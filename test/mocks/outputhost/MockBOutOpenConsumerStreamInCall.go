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
	"github.com/uber/cherami-thrift/.generated/go/cherami"

	"github.com/stretchr/testify/mock"
)

// MockBOutOpenConsumerStreamInCall is a mock of the cherami.BOutOpenConsumerStreamInCall
// the purpose of this is to avoid needing go through tchannel and
// unittest outputhost directly
type MockBOutOpenConsumerStreamInCall struct {
	mock.Mock
}

// Write is a mock of the corresponding cherami. routinee
func (m *MockBOutOpenConsumerStreamInCall) Write(msg *cherami.OutputHostCommand) error {
	args := m.Called(msg)
	return args.Error(0)
}

// Flush is a mock of the corresponding cherami. routinee
func (m *MockBOutOpenConsumerStreamInCall) Flush() error {
	return m.Called().Error(0)
}

// Done is a mock of the corresponding cherami. routinee
func (m *MockBOutOpenConsumerStreamInCall) Done() error {
	return m.Called().Error(0)
}

// Read is a mock of the corresponding cherami. routinee
func (m *MockBOutOpenConsumerStreamInCall) Read() (*cherami.ControlFlow, error) {
	args := m.Called()
	var retMsg *cherami.ControlFlow
	if args.Error(1) == nil {
		retMsg = args.Get(0).(*cherami.ControlFlow)
	}
	return retMsg, args.Error(1)
}

// SetResponseHeaders is a mock of the corresponding cherami. routinee
func (m *MockBOutOpenConsumerStreamInCall) SetResponseHeaders(headers map[string]string) error {
	args := m.Called(headers)
	return args.Error(0)
}
