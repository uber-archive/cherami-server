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
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/stretchr/testify/mock"
	"github.com/uber-common/bark"
)

// MockLoadReporterDaemonFactory is the mock of common.LoadReporterDaemonFactory interface
type MockLoadReporterDaemonFactory struct {
	mock.Mock
}

// CreateReporter is the mock implementation for CreateReporter function on common.LoadReporterDaemonFactory
func (m *MockLoadReporterDaemonFactory) CreateReporter(interval time.Duration, source LoadReporterSource, logger bark.Logger) LoadReporterDaemon {
	// Ignore the logger parameter and create a new one as it causes data race with mock library
	// force the LoadReporterSource to nil as it also introduces a data race with mock lib
	args := m.Called(interval, nil, bark.NewLoggerFromLogrus(log.New()))
	return args.Get(0).(LoadReporterDaemon)
}
