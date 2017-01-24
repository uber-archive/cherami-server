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

import "github.com/stretchr/testify/mock"

import "net/http"

import serverStream "github.com/uber/cherami-server/stream"

import clientStream "github.com/uber/cherami-client-go/stream"

// MockWSConnector is the mock of common.WSConnector interface
type MockWSConnector struct {
	mock.Mock
}

// OpenPublisherStream is the mock for corresponding method on WSConnector
func (_m *MockWSConnector) OpenPublisherStream(hostPort string, requestHeader http.Header) (clientStream.BInOpenPublisherStreamOutCall, error) {
	ret := _m.Called(hostPort, requestHeader)

	var r0 clientStream.BInOpenPublisherStreamOutCall
	if rf, ok := ret.Get(0).(func(string, http.Header) clientStream.BInOpenPublisherStreamOutCall); ok {
		r0 = rf(hostPort, requestHeader)
	} else {
		r0 = ret.Get(0).(clientStream.BInOpenPublisherStreamOutCall)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, http.Header) error); ok {
		r1 = rf(hostPort, requestHeader)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AcceptPublisherStream is the mock for corresponding method on WSConnector
func (_m *MockWSConnector) AcceptPublisherStream(w http.ResponseWriter, r *http.Request) (serverStream.BInOpenPublisherStreamInCall, error) {
	ret := _m.Called(w, r)

	var r0 serverStream.BInOpenPublisherStreamInCall
	if rf, ok := ret.Get(0).(func(http.ResponseWriter, *http.Request) serverStream.BInOpenPublisherStreamInCall); ok {
		r0 = rf(w, r)
	} else {
		r0 = ret.Get(0).(serverStream.BInOpenPublisherStreamInCall)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(http.ResponseWriter, *http.Request) error); ok {
		r1 = rf(w, r)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// OpenConsumerStream is the mock for corresponding method on WSConnector
func (_m *MockWSConnector) OpenConsumerStream(hostPort string, requestHeader http.Header) (clientStream.BOutOpenConsumerStreamOutCall, error) {
	ret := _m.Called(hostPort, requestHeader)

	var r0 clientStream.BOutOpenConsumerStreamOutCall
	if rf, ok := ret.Get(0).(func(string, http.Header) clientStream.BOutOpenConsumerStreamOutCall); ok {
		r0 = rf(hostPort, requestHeader)
	} else {
		r0 = ret.Get(0).(clientStream.BOutOpenConsumerStreamOutCall)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, http.Header) error); ok {
		r1 = rf(hostPort, requestHeader)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AcceptConsumerStream is the mock for corresponding method on WSConnector
func (_m *MockWSConnector) AcceptConsumerStream(w http.ResponseWriter, r *http.Request) (serverStream.BOutOpenConsumerStreamInCall, error) {
	ret := _m.Called(w, r)

	var r0 serverStream.BOutOpenConsumerStreamInCall
	if rf, ok := ret.Get(0).(func(http.ResponseWriter, *http.Request) serverStream.BOutOpenConsumerStreamInCall); ok {
		r0 = rf(w, r)
	} else {
		r0 = ret.Get(0).(serverStream.BOutOpenConsumerStreamInCall)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(http.ResponseWriter, *http.Request) error); ok {
		r1 = rf(w, r)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// OpenAppendStream is the mock for corresponding method on WSConnector
func (_m *MockWSConnector) OpenAppendStream(hostPort string, requestHeader http.Header) (serverStream.BStoreOpenAppendStreamOutCall, error) {
	ret := _m.Called(hostPort, requestHeader)

	var r0 serverStream.BStoreOpenAppendStreamOutCall
	if rf, ok := ret.Get(0).(func(string, http.Header) serverStream.BStoreOpenAppendStreamOutCall); ok {
		r0 = rf(hostPort, requestHeader)
	} else {
		r0 = ret.Get(0).(serverStream.BStoreOpenAppendStreamOutCall)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, http.Header) error); ok {
		r1 = rf(hostPort, requestHeader)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AcceptAppendStream is the mock for corresponding method on WSConnector
func (_m *MockWSConnector) AcceptAppendStream(w http.ResponseWriter, r *http.Request) (serverStream.BStoreOpenAppendStreamInCall, error) {
	ret := _m.Called(w, r)

	var r0 serverStream.BStoreOpenAppendStreamInCall
	if rf, ok := ret.Get(0).(func(http.ResponseWriter, *http.Request) serverStream.BStoreOpenAppendStreamInCall); ok {
		r0 = rf(w, r)
	} else {
		r0 = ret.Get(0).(serverStream.BStoreOpenAppendStreamInCall)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(http.ResponseWriter, *http.Request) error); ok {
		r1 = rf(w, r)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// OpenReadStream is the mock for corresponding method on WSConnector
func (_m *MockWSConnector) OpenReadStream(hostPort string, requestHeader http.Header) (serverStream.BStoreOpenReadStreamOutCall, error) {
	ret := _m.Called(hostPort, requestHeader)

	var r0 serverStream.BStoreOpenReadStreamOutCall
	if rf, ok := ret.Get(0).(func(string, http.Header) serverStream.BStoreOpenReadStreamOutCall); ok {
		r0 = rf(hostPort, requestHeader)
	} else {
		r0 = ret.Get(0).(serverStream.BStoreOpenReadStreamOutCall)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, http.Header) error); ok {
		r1 = rf(hostPort, requestHeader)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AcceptReadStream is the mock for corresponding method on WSConnector
func (_m *MockWSConnector) AcceptReadStream(w http.ResponseWriter, r *http.Request) (serverStream.BStoreOpenReadStreamInCall, error) {
	ret := _m.Called(w, r)

	var r0 serverStream.BStoreOpenReadStreamInCall
	if rf, ok := ret.Get(0).(func(http.ResponseWriter, *http.Request) serverStream.BStoreOpenReadStreamInCall); ok {
		r0 = rf(w, r)
	} else {
		r0 = ret.Get(0).(serverStream.BStoreOpenReadStreamInCall)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(http.ResponseWriter, *http.Request) error); ok {
		r1 = rf(w, r)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// OpenReplicationReadStream is the mock for corresponding method on WSConnector
func (_m *MockWSConnector) OpenReplicationReadStream(hostPort string, requestHeader http.Header) (serverStream.BStoreOpenReadStreamOutCall, error) {
	ret := _m.Called(hostPort, requestHeader)

	var r0 serverStream.BStoreOpenReadStreamOutCall
	if rf, ok := ret.Get(0).(func(string, http.Header) serverStream.BStoreOpenReadStreamOutCall); ok {
		r0 = rf(hostPort, requestHeader)
	} else {
		r0 = ret.Get(0).(serverStream.BStoreOpenReadStreamOutCall)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, http.Header) error); ok {
		r1 = rf(hostPort, requestHeader)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AcceptReplicationReadStream is the mock for corresponding method on WSConnector
func (_m *MockWSConnector) AcceptReplicationReadStream(w http.ResponseWriter, r *http.Request) (serverStream.BStoreOpenReadStreamInCall, error) {
	ret := _m.Called(w, r)

	var r0 serverStream.BStoreOpenReadStreamInCall
	if rf, ok := ret.Get(0).(func(http.ResponseWriter, *http.Request) serverStream.BStoreOpenReadStreamInCall); ok {
		r0 = rf(w, r)
	} else {
		r0 = ret.Get(0).(serverStream.BStoreOpenReadStreamInCall)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(http.ResponseWriter, *http.Request) error); ok {
		r1 = rf(w, r)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// OpenReplicationRemoteReadStream is the mock for corresponding method on WSConnector
func (_m *MockWSConnector) OpenReplicationRemoteReadStream(hostPort string, requestHeader http.Header) (serverStream.BStoreOpenReadStreamOutCall, error) {
	ret := _m.Called(hostPort, requestHeader)

	var r0 serverStream.BStoreOpenReadStreamOutCall
	if rf, ok := ret.Get(0).(func(string, http.Header) serverStream.BStoreOpenReadStreamOutCall); ok {
		r0 = rf(hostPort, requestHeader)
	} else {
		r0 = ret.Get(0).(serverStream.BStoreOpenReadStreamOutCall)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, http.Header) error); ok {
		r1 = rf(hostPort, requestHeader)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AcceptReplicationRemoteReadStream is the mock for corresponding method on WSConnector
func (_m *MockWSConnector) AcceptReplicationRemoteReadStream(w http.ResponseWriter, r *http.Request) (serverStream.BStoreOpenReadStreamInCall, error) {
	ret := _m.Called(w, r)

	var r0 serverStream.BStoreOpenReadStreamInCall
	if rf, ok := ret.Get(0).(func(http.ResponseWriter, *http.Request) serverStream.BStoreOpenReadStreamInCall); ok {
		r0 = rf(w, r)
	} else {
		r0 = ret.Get(0).(serverStream.BStoreOpenReadStreamInCall)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(http.ResponseWriter, *http.Request) error); ok {
		r1 = rf(w, r)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
