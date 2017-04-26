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
	"net/http"

	"github.com/uber/cherami-server/common/configure"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
	"github.com/uber/cherami-server/common/metrics"

	"github.com/stretchr/testify/mock"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

// MockService is the mock of common.SCommon interface
type MockService struct {
	mock.Mock
}

// GetTChannel is a mock of the corresponding common. routine
func (m *MockService) GetTChannel() *tchannel.Channel {
	args := m.Called()
	return args.Get(0).(*tchannel.Channel)
}

// GetRingpopMonitor is a mock of the corresponding common. routine
func (m *MockService) GetRingpopMonitor() RingpopMonitor {
	args := m.Called()
	return args.Get(0).(RingpopMonitor)
}

// GetClientFactory is a mock of the corresponding common. routine
func (m *MockService) GetClientFactory() ClientFactory {
	args := m.Called()
	return args.Get(0).(ClientFactory)
}

// SetClientFactory is a mock of the corresponding common. routine
func (m *MockService) SetClientFactory(cf ClientFactory) {
	m.Called(cf)
	return
}

// GetWSConnector is a mock of the corresponding common. routine
func (m *MockService) GetWSConnector() WSConnector {
	args := m.Called()
	return args.Get(0).(WSConnector)
}

// GetConfig returns the AppConfig for this service
func (m *MockService) GetConfig() configure.CommonServiceConfig {
	args := m.Called()
	return args.Get(0).(configure.CommonServiceConfig)
}

// GetHostPort is a mock of the corresponding common. routine
func (m *MockService) GetHostPort() string {
	args := m.Called()
	return args.Get(0).(string)
}

// GetHostName returns the name of host running the service
func (m *MockService) GetHostName() string {
	args := m.Called()
	return args.Get(0).(string)
}

// GetHostUUID returns the uuid of host running the service
func (m *MockService) GetHostUUID() string {
	args := m.Called()
	return args.Get(0).(string)
}

// GetMetricsReporter is a mock of the corresponding common. routine
func (m *MockService) GetMetricsReporter() metrics.Reporter {
	args := m.Called()
	return args.Get(0).(metrics.Reporter)
}

// GetDConfigClient is a mock of the corresponding common. routine
func (m *MockService) GetDConfigClient() dconfig.Client {
	args := m.Called()
	return args.Get(0).(dconfig.Client)
}

// Start is a mock of the corresponding common. routine
func (m *MockService) Start(thriftService []thrift.TChanServer) {
	m.Called(thriftService)
	return
}

// Stop is a mock of the corresponding common. routine
func (m *MockService) Stop() {
	m.Called()
	return
}

// GetLoadReporterDaemonFactory is the factory interface for creating load reporters
func (m *MockService) GetLoadReporterDaemonFactory() LoadReporterDaemonFactory {
	args := m.Called()
	return args.Get(0).(LoadReporterDaemonFactory)
}

// Report is the implementation for reporting host specific load to controller
func (m *MockService) Report(reporter LoadReporter) {
	m.Called(reporter)
	return
}

// UpgradeHandler is the implementation for upgrade handler
func (m *MockService) UpgradeHandler(w http.ResponseWriter, r *http.Request) {
	m.Called(w, r)
	return
}

// IsLimitsEnabled is the implementation of the corresponding routine
func (m *MockService) IsLimitsEnabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

// GetAuthManager returns the auth manager
func (m *MockService) GetAuthManager() AuthManager {
	args := m.Called()
	return args.Get(0).(AuthManager)
}
