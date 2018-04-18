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

package retentionmgr

import (
	"github.com/stretchr/testify/mock"
)

type mockMetadataDep struct {
	mock.Mock
}

func (_m *mockMetadataDep) GetDestinations() ([]*destinationInfo, error) {
	ret := _m.Called()

	var r0 []*destinationInfo
	if rf, ok := ret.Get(0).(func() []*destinationInfo); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*destinationInfo)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
func (_m *mockMetadataDep) GetExtents(destID destinationID) ([]*extentInfo, error) {
	ret := _m.Called(destID)

	var r0 []*extentInfo
	if rf, ok := ret.Get(0).(func(destinationID) []*extentInfo); ok {
		r0 = rf(destID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*extentInfo)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(destinationID) error); ok {
		r1 = rf(destID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (_m *mockMetadataDep) GetExtentsForConsumerGroup(destID destinationID, cgID consumerGroupID) ([]extentID, error) {
	ret := _m.Called(destID, cgID)

	var r0 []extentID
	if rf, ok := ret.Get(0).(func(destinationID, consumerGroupID) []extentID); ok {
		r0 = rf(destID, cgID)
	} else {
		r0 = ret.Get(0).([]extentID)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(destinationID, consumerGroupID) error); ok {
		r1 = rf(destID, cgID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
func (_m *mockMetadataDep) GetConsumerGroups(destID destinationID) ([]*consumerGroupInfo, error) {
	ret := _m.Called(destID)

	var r0 []*consumerGroupInfo
	if rf, ok := ret.Get(0).(func(destinationID) []*consumerGroupInfo); ok {
		r0 = rf(destID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*consumerGroupInfo)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(destinationID) error); ok {
		r1 = rf(destID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
func (_m *mockMetadataDep) DeleteExtent(destID destinationID, extID extentID) error {
	ret := _m.Called(destID, extID)

	var r0 error
	if rf, ok := ret.Get(0).(func(destinationID, extentID) error); ok {
		r0 = rf(destID, extID)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
func (_m *mockMetadataDep) MarkExtentConsumed(destID destinationID, extID extentID) error {
	ret := _m.Called(destID, extID)

	var r0 error
	if rf, ok := ret.Get(0).(func(destinationID, extentID) error); ok {
		r0 = rf(destID, extID)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *mockMetadataDep) DeleteConsumerGroupUUID(destID destinationID, cgID consumerGroupID) error {
	ret := _m.Called(destID, cgID)

	var r0 error
	if rf, ok := ret.Get(0).(func(destinationID, consumerGroupID) error); ok {
		r0 = rf(destID, cgID)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *mockMetadataDep) DeleteDestination(destID destinationID) error {
	ret := _m.Called(destID)

	var r0 error
	if rf, ok := ret.Get(0).(func(destinationID) error); ok {
		r0 = rf(destID)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *mockMetadataDep) DeleteConsumerGroupExtent(destID destinationID, cgID consumerGroupID, extID extentID) error {
	ret := _m.Called(destID, cgID, extID)

	var r0 error
	if rf, ok := ret.Get(0).(func(destinationID, consumerGroupID, extentID) error); ok {
		r0 = rf(destID, cgID, extID)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *mockMetadataDep) GetAckLevel(destID destinationID, extID extentID, cgID consumerGroupID) (int64, error) {
	ret := _m.Called(destID, extID, cgID)

	var r0 int64
	if rf, ok := ret.Get(0).(func(destinationID, extentID, consumerGroupID) int64); ok {
		r0 = rf(destID, extID, cgID)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(destinationID, extentID, consumerGroupID) error); ok {
		r1 = rf(destID, extID, cgID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (_m *mockMetadataDep) GetExtentInfo(destID destinationID, extID extentID) (*extentInfo, error) {
	ret := _m.Called(destID, extID)

	var r0 *extentInfo
	if rf, ok := ret.Get(0).(func(destinationID, extentID) *extentInfo); ok {
		r0 = rf(destID, extID)
	} else {
		r0 = ret.Get(0).(*extentInfo)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(destinationID, extentID) error); ok {
		r1 = rf(destID, extID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockStorehostDep struct {
	mock.Mock
}

func (_m *mockStorehostDep) GetAddressFromTimestamp(storeID storehostID, extID extentID, timestamp int64) (int64, bool, error) {
	ret := _m.Called(storeID, extID, timestamp)

	var r0 int64
	if rf, ok := ret.Get(0).(func(storehostID, extentID, int64) int64); ok {
		r0 = rf(storeID, extID, timestamp)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 bool
	if rf, ok := ret.Get(1).(func(storehostID, extentID, int64) bool); ok {
		r1 = rf(storeID, extID, timestamp)
	} else {
		r1 = ret.Get(1).(bool)
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(storehostID, extentID, int64) error); ok {
		r2 = rf(storeID, extID, timestamp)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

func (_m *mockStorehostDep) PurgeMessages(storeID storehostID, extID extentID, retentionAddr int64) (int64, error) {
	ret := _m.Called(storeID, extID, retentionAddr)

	var r0 int64
	if rf, ok := ret.Get(0).(func(storehostID, extentID, int64) int64); ok {
		r0 = rf(storeID, extID, retentionAddr)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(storehostID, extentID, int64) error); ok {
		r1 = rf(storeID, extID, retentionAddr)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
