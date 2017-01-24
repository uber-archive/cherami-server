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

// MockHTTPResponseWriter is the mock of http.ResponseWriter interface
type MockHTTPResponseWriter struct {
	mock.Mock
}

// Header is the mock for corresponding method on ResponseWriter
func (_m *MockHTTPResponseWriter) Header() http.Header {
	ret := _m.Called()

	var r0 http.Header
	if rf, ok := ret.Get(0).(func() http.Header); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(http.Header)
	}

	return r0
}

// Write is the mock for corresponding method on ResponseWriter
func (_m *MockHTTPResponseWriter) Write(_a0 []byte) (int, error) {
	ret := _m.Called(_a0)

	var r0 int
	if rf, ok := ret.Get(0).(func([]byte) int); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Get(0).(int)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func([]byte) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// WriteHeader is the mock for corresponding method on ResponseWriter
func (_m *MockHTTPResponseWriter) WriteHeader(_a0 int) {
	_m.Called(_a0)
}
