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
	serverStream "github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/store"

	"github.com/stretchr/testify/mock"
	"github.com/uber/tchannel-go/thrift"
)

// MockStoreHost is a mock of the storehost used for testing
type MockStoreHost struct {
	mock.Mock
}

// OpenAppendStream is a mock of the corresponding storehost routinee
func (m *MockStoreHost) OpenAppendStream(ctx thrift.Context) (serverStream.BStoreOpenAppendStreamOutCall, error) {
	args := m.Called(ctx)
	return args.Get(0).(serverStream.BStoreOpenAppendStreamOutCall), args.Error(1)
}

// GetAddressFromTimestamp is a mock of the corresponding storehost routinee
func (m *MockStoreHost) GetAddressFromTimestamp(ctx thrift.Context, getAddressRequest *store.GetAddressFromTimestampRequest) (*store.GetAddressFromTimestampResult_, error) {
	args := m.Called(ctx, getAddressRequest)
	return args.Get(0).(*store.GetAddressFromTimestampResult_), args.Error(1)
}

// GetExtentInfo is a mock of the corresponding storehost routinee
func (m *MockStoreHost) GetExtentInfo(ctx thrift.Context, extentInfoRequest *store.GetExtentInfoRequest) (*store.ExtentInfo, error) {
	args := m.Called(ctx, extentInfoRequest)
	return args.Get(0).(*store.ExtentInfo), args.Error(1)
}

// SealExtent is a mock of the corresponding storehost routinee
func (m *MockStoreHost) SealExtent(ctx thrift.Context, sealRequest *store.SealExtentRequest) error {
	args := m.Called(ctx, sealRequest)
	return args.Error(0)
}

// OpenReadStream is a mock of the corresponding storehost routinee
func (m *MockStoreHost) OpenReadStream(ctx thrift.Context) (serverStream.BStoreOpenReadStreamOutCall, error) {
	args := m.Called(ctx)
	return args.Get(0).(serverStream.BStoreOpenReadStreamOutCall), args.Error(1)
}

// PurgeMessages is a mock of the corresponding storehost routine
func (m *MockStoreHost) PurgeMessages(ctx thrift.Context, purgeMessagesRequest *store.PurgeMessagesRequest) (*store.PurgeMessagesResult_, error) {
	args := m.Called(ctx, purgeMessagesRequest)
	return args.Get(0).(*store.PurgeMessagesResult_), args.Error(1)
}

// ReadMessages is a mock of the corresponding storehost routine
func (m *MockStoreHost) ReadMessages(ctx thrift.Context, readMessagesRequest *store.ReadMessagesRequest) (*store.ReadMessagesResult_, error) {
	args := m.Called(ctx, readMessagesRequest)
	return args.Get(0).(*store.ReadMessagesResult_), args.Error(1)
}

// ReplicateExtent is a mock of the corresponding storehost routine
func (m *MockStoreHost) ReplicateExtent(ctx thrift.Context, replicateExtentRequest *store.ReplicateExtentRequest) error {
	args := m.Called(ctx, replicateExtentRequest)
	return args.Error(0)
}

// RemoteReplicateExtent is a mock of the corresponding storehost routine
func (m *MockStoreHost) RemoteReplicateExtent(ctx thrift.Context, request *store.RemoteReplicateExtentRequest) error {
	args := m.Called(ctx, request)
	return args.Error(0)
}

// ListExtents is a mock of the corresponding storehost routine
func (m *MockStoreHost) ListExtents(ctx thrift.Context) (*store.ListExtentsResult_, error) {
	args := m.Called(ctx)
	return args.Get(0).(*store.ListExtentsResult_), args.Error(1)
}
