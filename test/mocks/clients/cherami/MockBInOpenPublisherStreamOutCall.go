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
)

// MockBInOpenPublisherStreamOutCall is a mock for BInOpenPublisherStreamOutCall used for unit testing of cherami client
type MockBInOpenPublisherStreamOutCall struct {
	mock.Mock
}

// Write writes an argument to the request stream. The written items may not
// be sent till Flush or Done is called. routinee
func (m *MockBInOpenPublisherStreamOutCall) Write(arg *cherami.PutMessage) error {
	args := m.Called(arg)
	return args.Error(0)
}

// Flush flushes all written arguments. routinee
func (m *MockBInOpenPublisherStreamOutCall) Flush() error {
	args := m.Called()
	return args.Error(0)
}

// Done closes the request stream and should be called after all arguments have been written. routinee
func (m *MockBInOpenPublisherStreamOutCall) Done() error {
	args := m.Called()
	return args.Error(0)
}

// Read returns the next result, if any is available. If there are no more
// results left, it will return io.EOF. routinee
func (m *MockBInOpenPublisherStreamOutCall) Read() (*cherami.InputHostCommand, error) {
	args := m.Called()
	var cmd *cherami.InputHostCommand
	if args.Get(0) != nil {
		cmd = args.Get(0).(*cherami.InputHostCommand)
	}
	return cmd, args.Error(1)
}

// ResponseHeaders returns the response headers sent from the server. This will
// block until server headers have been received. routinee
func (m *MockBInOpenPublisherStreamOutCall) ResponseHeaders() (map[string]string, error) {
	args := m.Called()
	var headers map[string]string
	if args.Error(1) == nil {
		headers = args.Get(0).(map[string]string)
	}
	return headers, args.Error(1)
}
