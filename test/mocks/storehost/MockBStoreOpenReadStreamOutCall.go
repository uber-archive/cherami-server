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

package storehost

import (
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/store"

	"github.com/stretchr/testify/mock"
)

// MockBStoreOpenReadStreamOutCall is the mock of store.BStoreOpenReadStreamOutCall
// this can be used by all clients of storehost to write messages
// for example: inputhost will use this to test its write path
type MockBStoreOpenReadStreamOutCall struct {
	mock.Mock
}

// Write is the mock of the corresponding store. method
func (m *MockBStoreOpenReadStreamOutCall) Write(msg *cherami.ControlFlow) error {
	args := m.Called(msg)
	return args.Error(0)
}

// Flush is the mock of the corresponding store. method
func (m *MockBStoreOpenReadStreamOutCall) Flush() error {
	return m.Called().Error(0)
}

// Done is the mock of the corresponding store. method
func (m *MockBStoreOpenReadStreamOutCall) Done() error {
	return m.Called().Error(0)
}

// Read is the mock of the corresponding store. method
func (m *MockBStoreOpenReadStreamOutCall) Read() (*store.ReadMessageContent, error) {
	args := m.Called()
	var retMsg *store.ReadMessageContent
	if args.Error(1) == nil {
		retMsg = args.Get(0).(*store.ReadMessageContent)
	}
	return retMsg, args.Error(1)
}

// ResponseHeaders is the mock of the corresponding store. method
func (m *MockBStoreOpenReadStreamOutCall) ResponseHeaders() (map[string]string, error) {
	args := m.Called()
	return args.Get(0).(map[string]string), args.Error(1)
}
