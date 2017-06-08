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
	"github.com/uber/cherami-thrift/.generated/go/shared"

	"github.com/stretchr/testify/mock"
	"github.com/uber/tchannel-go/thrift"
)

type (
	// MockTChanReplicator is the mock implementation for replicator and used for testing
	MockTChanReplicator struct {
		mock.Mock
	}
)

// CreateConsumerGroupUUID is the mock for corresponding replicator API
func (m *MockTChanReplicator) CreateConsumerGroupUUID(ctx thrift.Context, createRequest *shared.CreateConsumerGroupUUIDRequest) (*shared.ConsumerGroupDescription, error) {
	ret := m.Called(ctx, createRequest)

	var r0 *shared.ConsumerGroupDescription
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.CreateConsumerGroupUUIDRequest) *shared.ConsumerGroupDescription); ok {
		r0 = rf(ctx, createRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.ConsumerGroupDescription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.CreateConsumerGroupUUIDRequest) error); ok {
		r1 = rf(ctx, createRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CreateDestinationUUID is the mock for corresponding replicator API
func (m *MockTChanReplicator) CreateDestinationUUID(ctx thrift.Context, createRequest *shared.CreateDestinationUUIDRequest) (*shared.DestinationDescription, error) {
	ret := m.Called(ctx, createRequest)

	var r0 *shared.DestinationDescription
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.CreateDestinationUUIDRequest) *shared.DestinationDescription); ok {
		r0 = rf(ctx, createRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.DestinationDescription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.CreateDestinationUUIDRequest) error); ok {
		r1 = rf(ctx, createRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CreateRemoteConsumerGroupUUID is the mock for corresponding replicator API
func (m *MockTChanReplicator) CreateRemoteConsumerGroupUUID(ctx thrift.Context, createRequest *shared.CreateConsumerGroupUUIDRequest) error {
	ret := m.Called(ctx, createRequest)

	var r0 error
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.CreateConsumerGroupUUIDRequest) error); ok {
		r0 = rf(ctx, createRequest)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CreateRemoteDestinationUUID is the mock for corresponding replicator API
func (m *MockTChanReplicator) CreateRemoteDestinationUUID(ctx thrift.Context, createRequest *shared.CreateDestinationUUIDRequest) error {
	ret := m.Called(ctx, createRequest)

	var r0 error
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.CreateDestinationUUIDRequest) error); ok {
		r0 = rf(ctx, createRequest)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteConsumerGroup is the mock for corresponding replicator API
func (m *MockTChanReplicator) DeleteConsumerGroup(ctx thrift.Context, deleteRequest *shared.DeleteConsumerGroupRequest) error {
	ret := m.Called(ctx, deleteRequest)

	var r0 error
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.DeleteConsumerGroupRequest) error); ok {
		r0 = rf(ctx, deleteRequest)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteDestination is the mock for corresponding replicator API
func (m *MockTChanReplicator) DeleteDestination(ctx thrift.Context, deleteRequest *shared.DeleteDestinationRequest) error {
	ret := m.Called(ctx, deleteRequest)

	var r0 error
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.DeleteDestinationRequest) error); ok {
		r0 = rf(ctx, deleteRequest)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteRemoteConsumerGroup is the mock for corresponding replicator API
func (m *MockTChanReplicator) DeleteRemoteConsumerGroup(ctx thrift.Context, deleteRequest *shared.DeleteConsumerGroupRequest) error {
	ret := m.Called(ctx, deleteRequest)

	var r0 error
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.DeleteConsumerGroupRequest) error); ok {
		r0 = rf(ctx, deleteRequest)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteRemoteDestination is the mock for corresponding replicator API
func (m *MockTChanReplicator) DeleteRemoteDestination(ctx thrift.Context, deleteRequest *shared.DeleteDestinationRequest) error {
	ret := m.Called(ctx, deleteRequest)

	var r0 error
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.DeleteDestinationRequest) error); ok {
		r0 = rf(ctx, deleteRequest)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UpdateConsumerGroup is the mock for corresponding replicator API
func (m *MockTChanReplicator) UpdateConsumerGroup(ctx thrift.Context, updateRequest *shared.UpdateConsumerGroupRequest) (*shared.ConsumerGroupDescription, error) {
	ret := m.Called(ctx, updateRequest)

	var r0 *shared.ConsumerGroupDescription
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.UpdateConsumerGroupRequest) *shared.ConsumerGroupDescription); ok {
		r0 = rf(ctx, updateRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.ConsumerGroupDescription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.UpdateConsumerGroupRequest) error); ok {
		r1 = rf(ctx, updateRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UpdateDestination is the mock for corresponding replicator API
func (m *MockTChanReplicator) UpdateDestination(ctx thrift.Context, updateRequest *shared.UpdateDestinationRequest) (*shared.DestinationDescription, error) {
	ret := m.Called(ctx, updateRequest)

	var r0 *shared.DestinationDescription
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.UpdateDestinationRequest) *shared.DestinationDescription); ok {
		r0 = rf(ctx, updateRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.DestinationDescription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.UpdateDestinationRequest) error); ok {
		r1 = rf(ctx, updateRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UpdateRemoteConsumerGroup is the mock for corresponding replicator API
func (m *MockTChanReplicator) UpdateRemoteConsumerGroup(ctx thrift.Context, updateRequest *shared.UpdateConsumerGroupRequest) error {
	ret := m.Called(ctx, updateRequest)

	var r0 error
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.UpdateConsumerGroupRequest) error); ok {
		r0 = rf(ctx, updateRequest)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UpdateRemoteDestination is the mock for corresponding replicator API
func (m *MockTChanReplicator) UpdateRemoteDestination(ctx thrift.Context, updateRequest *shared.UpdateDestinationRequest) error {
	ret := m.Called(ctx, updateRequest)

	var r0 error
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.UpdateDestinationRequest) error); ok {
		r0 = rf(ctx, updateRequest)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (m *MockTChanReplicator) CreateExtent(ctx thrift.Context, createRequest *shared.CreateExtentRequest) (*shared.CreateExtentResult_, error) {
	ret := m.Called(ctx, createRequest)

	var r0 *shared.CreateExtentResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.CreateExtentRequest) *shared.CreateExtentResult_); ok {
		r0 = rf(ctx, createRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.CreateExtentResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.CreateExtentRequest) error); ok {
		r1 = rf(ctx, createRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (m *MockTChanReplicator) CreateRemoteExtent(ctx thrift.Context, createRequest *shared.CreateExtentRequest) error {
	ret := m.Called(ctx, createRequest)

	var r0 error
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.CreateExtentRequest) error); ok {
		r0 = rf(ctx, createRequest)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (m *MockTChanReplicator) ListDestinations(ctx thrift.Context, listRequest *shared.ListDestinationsRequest) (*shared.ListDestinationsResult_, error) {
	ret := m.Called(ctx, listRequest)

	var r0 *shared.ListDestinationsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.ListDestinationsRequest) *shared.ListDestinationsResult_); ok {
		r0 = rf(ctx, listRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.ListDestinationsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.ListDestinationsRequest) error); ok {
		r1 = rf(ctx, listRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (m *MockTChanReplicator) ListDestinationsByUUID(ctx thrift.Context, listRequest *shared.ListDestinationsByUUIDRequest) (*shared.ListDestinationsResult_, error) {
	ret := m.Called(ctx, listRequest)

	var r0 *shared.ListDestinationsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.ListDestinationsByUUIDRequest) *shared.ListDestinationsResult_); ok {
		r0 = rf(ctx, listRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.ListDestinationsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.ListDestinationsByUUIDRequest) error); ok {
		r1 = rf(ctx, listRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (m *MockTChanReplicator) ListExtentsStats(ctx thrift.Context, listRequest *shared.ListExtentsStatsRequest) (*shared.ListExtentsStatsResult_, error) {
	ret := m.Called(ctx, listRequest)

	var r0 *shared.ListExtentsStatsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.ListExtentsStatsRequest) *shared.ListExtentsStatsResult_); ok {
		r0 = rf(ctx, listRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.ListExtentsStatsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.ListExtentsStatsRequest) error); ok {
		r1 = rf(ctx, listRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (m *MockTChanReplicator) ReadDestination(ctx thrift.Context, listRequest *shared.ReadDestinationRequest) (*shared.DestinationDescription, error) {
	ret := m.Called(ctx, listRequest)

	var r0 *shared.DestinationDescription
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.ReadDestinationRequest) *shared.DestinationDescription); ok {
		r0 = rf(ctx, listRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.DestinationDescription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.ReadDestinationRequest) error); ok {
		r1 = rf(ctx, listRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (m *MockTChanReplicator) ReadDestinationInRemoteZone(ctx thrift.Context, listRequest *shared.ReadDestinationInRemoteZoneRequest) (*shared.DestinationDescription, error) {
	ret := m.Called(ctx, listRequest)

	var r0 *shared.DestinationDescription
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.ReadDestinationInRemoteZoneRequest) *shared.DestinationDescription); ok {
		r0 = rf(ctx, listRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.DestinationDescription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.ReadDestinationInRemoteZoneRequest) error); ok {
		r1 = rf(ctx, listRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (m *MockTChanReplicator) ListConsumerGroups(ctx thrift.Context, listRequest *shared.ListConsumerGroupRequest) (*shared.ListConsumerGroupResult_, error) {
	ret := m.Called(ctx, listRequest)

	var r0 *shared.ListConsumerGroupResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.ListConsumerGroupRequest) *shared.ListConsumerGroupResult_); ok {
		r0 = rf(ctx, listRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.ListConsumerGroupResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.ListConsumerGroupRequest) error); ok {
		r1 = rf(ctx, listRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (m *MockTChanReplicator) ReadConsumerGroupExtents(ctx thrift.Context, listRequest *shared.ReadConsumerGroupExtentsRequest) (*shared.ReadConsumerGroupExtentsResult_, error) {
	ret := m.Called(ctx, listRequest)

	var r0 *shared.ReadConsumerGroupExtentsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.ReadConsumerGroupExtentsRequest) *shared.ReadConsumerGroupExtentsResult_); ok {
		r0 = rf(ctx, listRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.ReadConsumerGroupExtentsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.ReadConsumerGroupExtentsRequest) error); ok {
		r1 = rf(ctx, listRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (m *MockTChanReplicator) CreateConsumerGroupExtent(ctx thrift.Context, createRequest *shared.CreateConsumerGroupExtentRequest) error {
	ret := m.Called(ctx, createRequest)

	var r0 error
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.CreateConsumerGroupExtentRequest) error); ok {
		r0 = rf(ctx, createRequest)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (m *MockTChanReplicator) CreateRemoteConsumerGroupExtent(ctx thrift.Context, createRequest *shared.CreateConsumerGroupExtentRequest) error {
	ret := m.Called(ctx, createRequest)

	var r0 error
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.CreateConsumerGroupExtentRequest) error); ok {
		r0 = rf(ctx, createRequest)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (m *MockTChanReplicator) SetAckOffset(ctx thrift.Context, request *shared.SetAckOffsetRequest) error {
	ret := m.Called(ctx, request)

	var r0 error
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.SetAckOffsetRequest) error); ok {
		r0 = rf(ctx, request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (m *MockTChanReplicator) SetAckOffsetInRemote(ctx thrift.Context, request *shared.SetAckOffsetRequest) error {
	ret := m.Called(ctx, request)

	var r0 error
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.SetAckOffsetRequest) error); ok {
		r0 = rf(ctx, request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (m *MockTChanReplicator) ReadConsumerGroup(ctx thrift.Context, listRequest *shared.ReadConsumerGroupRequest) (*shared.ConsumerGroupDescription, error) {
	ret := m.Called(ctx, listRequest)

	var r0 *shared.ConsumerGroupDescription
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.ReadConsumerGroupRequest) *shared.ConsumerGroupDescription); ok {
		r0 = rf(ctx, listRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.ConsumerGroupDescription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.ReadConsumerGroupRequest) error); ok {
		r1 = rf(ctx, listRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (m *MockTChanReplicator) ReadConsumerGroupInRemoteZone(ctx thrift.Context, listRequest *shared.ReadConsumerGroupInRemoteRequest) (*shared.ConsumerGroupDescription, error) {
	ret := m.Called(ctx, listRequest)

	var r0 *shared.ConsumerGroupDescription
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.ReadConsumerGroupInRemoteRequest) *shared.ConsumerGroupDescription); ok {
		r0 = rf(ctx, listRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.ConsumerGroupDescription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.ReadConsumerGroupInRemoteRequest) error); ok {
		r1 = rf(ctx, listRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
