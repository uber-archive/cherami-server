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

package inputhost

import (
	"github.com/uber/cherami-thrift/.generated/go/cherami"

	"github.com/stretchr/testify/mock"
)

// MockBInOpenPublisherStreamInCall is a mock of the cherami.BInOpenPublisherStreamInCall
// the purpose of this is to avoid needing go through tchannel and
// unittest inputhost directly
type MockBInOpenPublisherStreamInCall struct {
	mock.Mock
}

// Write is a mock of the corresponding cherami. routinee
func (m *MockBInOpenPublisherStreamInCall) Write(cmd *cherami.InputHostCommand) error {
	args := m.Called(cmd)

	if rf, ok := args.Get(0).(func(*cherami.InputHostCommand) error); ok {
		return rf(cmd)
	}

	return args.Error(0)
}

// Flush is a mock of the corresponding cherami. routinee
func (m *MockBInOpenPublisherStreamInCall) Flush() error {
	return m.Called().Error(0)
}

// Done is a mock of the corresponding cherami. routinee
func (m *MockBInOpenPublisherStreamInCall) Done() error {
	return m.Called().Error(0)
}

// Read is a mock of the corresponding cherami. routinee
func (m *MockBInOpenPublisherStreamInCall) Read() (*cherami.PutMessage, error) {
	args := m.Called()
	var retMsg *cherami.PutMessage
	if args.Error(1) == nil {
		retMsg = args.Get(0).(*cherami.PutMessage)
	}
	return retMsg, args.Error(1)
}

// SetResponseHeaders is a mock of the corresponding cherami. routinee
func (m *MockBInOpenPublisherStreamInCall) SetResponseHeaders(headers map[string]string) error {
	args := m.Called(headers)
	return args.Error(0)
}
