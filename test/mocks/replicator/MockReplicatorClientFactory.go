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
	"github.com/uber/cherami-thrift/.generated/go/replicator"

	"github.com/stretchr/testify/mock"
	"github.com/uber/tchannel-go"
)

type (
	// MockReplicatorClientFactory is the mock implementation for ReplicatorClientFactory and used for testing
	MockReplicatorClientFactory struct {
		mock.Mock
	}
)

// GetReplicatorClient is the mock for corresponding replicator API
func (_m *MockReplicatorClientFactory) GetReplicatorClient(zone string) (replicator.TChanReplicator, error) {
	ret := _m.Called(zone)

	var r0 replicator.TChanReplicator
	if rf, ok := ret.Get(0).(func(string) replicator.TChanReplicator); ok {
		r0 = rf(zone)
	} else {
		r0 = ret.Get(0).(replicator.TChanReplicator)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(zone)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// SetTChannel is the mock for corresponding replicator API
func (_m *MockReplicatorClientFactory) SetTChannel(ch *tchannel.Channel) {
	_m.Called(ch)
}
