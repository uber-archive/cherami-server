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

package replicator

import (
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/store"

	"github.com/stretchr/testify/mock"
)

// MockBStoreOpenReadStreamInCallForReplicator is the mock of store.BStoreOpenReadStreamInCall
type MockBStoreOpenReadStreamInCallForReplicator struct {
	mock.Mock
}

// Write is the mock of the corresponding store. method
func (m *MockBStoreOpenReadStreamInCallForReplicator) Write(msg *store.ReadMessageContent) error {
	args := m.Called(msg)
	return args.Error(0)
}

// Flush is the mock of the corresponding store. method
func (m *MockBStoreOpenReadStreamInCallForReplicator) Flush() error {
	return nil
}

// Done is the mock of the corresponding store. method
func (m *MockBStoreOpenReadStreamInCallForReplicator) Done() error {
	return nil
}

// Read is the mock of the corresponding store. method
func (m *MockBStoreOpenReadStreamInCallForReplicator) Read() (*cherami.ControlFlow, error) {
	args := m.Called()
	var ret *cherami.ControlFlow
	if args.Error(1) == nil {
		ret = args.Get(0).(*cherami.ControlFlow)
	}
	return ret, args.Error(1)
}

// SetResponseHeaders is the mock of the corresponding store. method
func (m *MockBStoreOpenReadStreamInCallForReplicator) SetResponseHeaders(headers map[string]string) error {
	args := m.Called(headers)
	return args.Error(0)
}
