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
	"sync"

	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common/configure"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/ringpop-go"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

type (

	// Service contains the objects specific to this service
	Service struct {
		bLimitsEnabled         bool
		sName                  string
		hostPort               string
		hostName               string
		hostUUID               string
		mReporter              metrics.Reporter
		runtimeMetricsReporter *metrics.RuntimeMetricsReporter
		dClient                dconfig.Client
		server                 *thrift.Server
		ch                     *tchannel.Channel
		rp                     *ringpop.Ringpop
		cfg                    configure.CommonServiceConfig
		rpm                    RingpopMonitor
		resolver               UUIDResolver
		hostHWInfoReader       HostHardwareInfoReader
		cFactory               ClientFactory
		wsConnector            WSConnector
		rFactory               LoadReporterDaemonFactory
		startWg                sync.WaitGroup
		logger                 bark.Logger
		authManager            AuthManager
	}

	// WSService is the interface which should be implemented by websocket service
	WSService interface {
		RegisterWSHandler() *http.ServeMux
	}

	// SCommon is the interface which must be implemented by all the services
	SCommon interface {
		// GetTChannel returns the tchannel for this service
		GetTChannel() *tchannel.Channel

		// Returns Ringpop monitor for this service
		GetRingpopMonitor() RingpopMonitor

		// GetHostPort returns the host port for this service
		GetHostPort() string

		// GetHostName returns the name of host running the service
		GetHostName() string

		// GetHostUUID returns the uuid of this host
		GetHostUUID() string

		// Start creates the channel, starts & boots ringpop on the given streaming server
		Start(thriftService []thrift.TChanServer)

		// Stop stops the service
		Stop()

		// GetConfig returns the AppConfig for this service
		GetConfig() configure.CommonServiceConfig

		// GetMetricsReporter() returns the root metric reporter for this service
		GetMetricsReporter() metrics.Reporter

		// GetDConfigClient() returns the dynamic config client for this service
		GetDConfigClient() dconfig.Client

		// GetClientFactory returns the thrift client interface for getting thrift clients for this service
		GetClientFactory() ClientFactory

		// SetClientFactory allowes change of client factory for getting thrift clients
		SetClientFactory(cf ClientFactory)

		// GetWSConnector returns websocket connector for establishing websocket connections
		GetWSConnector() WSConnector

		// GetLoadReporterDaemonFactory is the factory interface for creating load reporters
		GetLoadReporterDaemonFactory() LoadReporterDaemonFactory

		// Report is the implementation for reporting host specific load to controller
		Report(reporter LoadReporter)

		// IsLimitsEnabled is used to see if we need to enforce limits on this service
		IsLimitsEnabled() bool

		// UpgradeHandler is the handler for the upgrade end point for this service
		UpgradeHandler(w http.ResponseWriter, r *http.Request)

		GetAuthManager() AuthManager
	}
)
