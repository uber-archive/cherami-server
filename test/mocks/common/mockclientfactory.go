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

package common

import (
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/replicator"
	"github.com/uber/cherami-thrift/.generated/go/store"

	"github.com/stretchr/testify/mock"
)

// MockClientFactory is the mock of common.ClientFactory interface
type MockClientFactory struct {
	mock.Mock
}

// GetAdminControllerClient is a mock of the corresponding common. routine
func (m *MockClientFactory) GetAdminControllerClient() (admin.TChanControllerHostAdmin, error) {
	args := m.Called()
	var retMsg admin.TChanControllerHostAdmin

	if args.Error(1) == nil {
		retMsg = args.Get(0).(admin.TChanControllerHostAdmin)
	}
	return retMsg, args.Error(1)
}

// GetThriftStoreClient is a mock of the corresponding common. routine
func (m *MockClientFactory) GetThriftStoreClient(replica string, id string) (store.TChanBStore, error) {
	args := m.Called(replica, id)
	var retMsg store.TChanBStore

	if args.Error(1) == nil {
		retMsg = args.Get(0).(store.TChanBStore)
	}
	return retMsg, args.Error(1)
}

// GetThriftStoreClientUUID is a mock of the corresponding common. routine
func (m *MockClientFactory) GetThriftStoreClientUUID(storeUUID string, contextID string) (store.TChanBStore, string, error) {
	args := m.Called(storeUUID, contextID)
	return args.Get(0).(store.TChanBStore), args.String(1), args.Error(2)
}

// ReleaseThriftStoreClient is a mock of the corresponding common. routine
func (m *MockClientFactory) ReleaseThriftStoreClient(id string) {
	m.Called(id)
	return
}

// GetControllerClient is a mock of the corresponding common. routine
func (m *MockClientFactory) GetControllerClient() (controller.TChanController, error) {
	args := m.Called()
	var retMsg controller.TChanController

	if args.Error(1) == nil {
		retMsg = args.Get(0).(controller.TChanController)
	}

	return retMsg, args.Error(1)
}

// GetReplicatorClient is a mock of the corresponding common. routine
func (m *MockClientFactory) GetReplicatorClient() (replicator.TChanReplicator, error) {
	args := m.Called()
	var retMsg replicator.TChanReplicator

	if args.Error(1) == nil {
		retMsg = args.Get(0).(replicator.TChanReplicator)
	}

	return retMsg, args.Error(1)
}
