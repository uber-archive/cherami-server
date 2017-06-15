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
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common/configure"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

// roleKey label is set by every single service as soon as it bootstraps its
// ringpop instance. The data for this key is the service name
const roleKey = "serviceName"

// NewService instantiates a ServiceInstance
// TODO: have a better name for Service.
// this is the object which holds all the common stuff
// shared by all the services.
func NewService(serviceName string, uuid string, cfg configure.CommonServiceConfig, resolver UUIDResolver, hostHWInfoReader HostHardwareInfoReader, reporter metrics.Reporter, dClient dconfig.Client, authManager AuthManager) *Service {
	sVice := &Service{
		sName:                  serviceName,
		cfg:                    cfg,
		resolver:               resolver,
		hostHWInfoReader:       hostHWInfoReader,
		hostUUID:               uuid,
		bLimitsEnabled:         true, // limits are enabled by default
		mReporter:              reporter,
		runtimeMetricsReporter: metrics.NewRuntimeMetricsReporter(reporter, time.Minute, cfg.GetLogger()),
		dClient:                dClient,
		authManager:            authManager,
	}

	// Get the host name and set it on the service.  This is used for emitting metric with a tag for hostname
	if hostName, e := os.Hostname(); e != nil {
		log.Fatal("Error getting hostname")
	} else {
		sVice.hostName = hostName
	}

	sVice.startWg.Add(1)
	return sVice
}

// UpdateAdvertisedName is used to update the advertised name to be
// deployment specific
func (h *Service) UpdateAdvertisedName(deploymentName string) {
	// We'll have multiple deployments(prod,staging,etc) in same datacenter, so we need to advertise
	// frontend with different name for different deployments
	if h.sName == FrontendServiceName {
		if IsDevelopmentEnvironment(deploymentName) {
			h.sName = FrontendServiceName
		} else {
			parts := strings.Split(deploymentName, "_")
			if len(parts) != 2 {
				log.Fatal("Unexpected deployment name")
			}
			tenancy := strings.ToLower(parts[0])
			if tenancy == TenancyProd {
				h.sName = FrontendServiceName
			} else {
				h.sName = fmt.Sprintf("%v_%v", FrontendServiceName, tenancy)
			}
		}
	}
}

// GetTChannel returns the tchannel for this service
func (h *Service) GetTChannel() *tchannel.Channel {
	h.startWg.Wait()
	return h.ch
}

// GetConfig returns the AppConfig for this service
func (h *Service) GetConfig() configure.CommonServiceConfig {
	return h.cfg
}

// GetRingpopMonitor returns the RingpopMonitor for this service
func (h *Service) GetRingpopMonitor() RingpopMonitor {
	h.startWg.Wait()
	return h.rpm
}

// GetClientFactory returns the ClientFactory interface for this service
func (h *Service) GetClientFactory() ClientFactory {
	h.startWg.Wait()
	return h.cFactory
}

// SetClientFactory allowes change of client factory for getting thrift clients
func (h *Service) SetClientFactory(cf ClientFactory) {
	h.cFactory = cf
}

// GetWSConnector returns websocket connector for establishing websocket connections
func (h *Service) GetWSConnector() WSConnector {
	h.startWg.Wait()
	return h.wsConnector
}

// GetLoadReporterDaemonFactory is the factory interface for creating load reporters
func (h *Service) GetLoadReporterDaemonFactory() LoadReporterDaemonFactory {
	h.startWg.Wait()
	return h.rFactory
}

// GetHostPort returns the host port for this service
func (h *Service) GetHostPort() string {
	h.startWg.Wait()
	return h.hostPort
}

// GetHostName returns the name of host running the service
func (h *Service) GetHostName() string {
	return h.hostName
}

// GetHostUUID returns the uuid for this service
func (h *Service) GetHostUUID() string {
	return h.hostUUID
}

// GetMetricsReporter returns the metrics metrics for this service
func (h *Service) GetMetricsReporter() metrics.Reporter {
	return h.mReporter
}

// IsLimitsEnabled returns whether limits are enabled
func (h *Service) IsLimitsEnabled() bool {
	return h.bLimitsEnabled
}

// GetDConfigClient returns the dconfig client
func (h *Service) GetDConfigClient() dconfig.Client {
	return h.dClient
}

// Start starts a TChannel-Thrift service
func (h *Service) Start(thriftServices []thrift.TChanServer) {
	// bump restart counter (should already have service tagged)
	h.GetMetricsReporter().InitMetrics(metrics.ServiceMetrics)
	h.GetMetricsReporter().IncCounter(metrics.RestartCount, nil, 1)
	h.runtimeMetricsReporter.Start()

	h.ch, h.server = h.cfg.SetupTChannelServer(h.sName, thriftServices)
	h.logger = h.cfg.GetLogger()
	h.bLimitsEnabled = h.cfg.GetLimitsEnabled()

	// use actual listen port (in case service is bound to :0 or 0.0.0.0:0)
	h.hostPort = h.ch.PeerInfo().HostPort
	host, port, _ := SplitHostPort(h.hostPort)
	h.rp = CreateRingpop(h.sName, h.ch, host, port)
	if h.rp == nil {
		h.logger.Fatal("Ringpop creation failed")
	}

	err := BootstrapRingpop(h.rp, host, port, h.cfg)
	if err != nil {
		h.logger.WithFields(bark.Fields{TagErr: err}).Fatal("Ringpop bootstrap failed")
	}

	h.rpm = NewRingpopMonitor(h.rp, services, h.resolver, h.hostHWInfoReader, h.logger)
	// set the role for this service even before we start the rpm
	if err = h.rpm.SetMetadata(roleKey, h.sName); err != nil {
		// there is no point in proceeding further if we cannot set the role label
		h.logger.WithFields(bark.Fields{TagErr: err}).Fatal("Ringpop setting role label failed")
	}
	h.rpm.Start()

	// create the client factory interface
	h.cFactory = NewClientFactory(h.rpm, h.ch, h.logger)

	// create websocket connector
	h.wsConnector = NewWSConnector()

	lg := h.logger.WithField(TagService, FmtService(h.sName)).WithField(TagHostPort, FmtHostPort(h.hostPort))

	// create the LoadReporterDaemonFactory used by downstream components for reporting load
	h.rFactory = NewLoadReporterDaemonFactory(h.hostUUID, controller.SKU_Machine1, controller.Role_IN, h.cFactory,
		NewRealTimeTickerFactory(), NewTokenBucketFactory(), lg)

	// The service is now started up and registered with hyperbahn
	lg.Info("service started")

	// seed the random generator once for this service
	rand.Seed(time.Now().UTC().UnixNano())

	// Now decrements the counter. This should be the last step of this function
	// All getters are blocked until this step
	// This is needed to prevent overzealous TChannel clients from crashing us,
	// since hyperbahn advertisement is done in early stage of this function
	h.startWg.Done()
}

// Stop destroys ringpop for that service, and closes the associated listening tchannel
func (h *Service) Stop() {

	if h.rpm != nil {
		h.rpm.Stop()
	}

	if h.rp != nil {
		h.rp.Destroy()
	}

	if h.ch != nil {
		h.ch.Close()
	}

	h.runtimeMetricsReporter.Stop()
}

// Report is used for reporting Host specific load to controller
func (h *Service) Report(reporter LoadReporter) {
	// TODO: Report Host specific load here like CPU, Memory and Diskspace
}

// UpgradeHandler is used to implement the upgrade endpoint
func (h *Service) UpgradeHandler(w http.ResponseWriter, r *http.Request) {
	// register service specific upgrade handler
}

// GetAuthManager returns the auth manager
func (h *Service) GetAuthManager() AuthManager {
	return h.authManager
}

// IsDevelopmentEnvironment detects if we are running in a development environment
func IsDevelopmentEnvironment(deploymentName string) (devEnv bool) {
	// If deploymentName is empty or "development", meaning the service is running locally, use the default service name
	if len(deploymentName) == 0 || strings.ToLower(deploymentName) == "development" {
		devEnv = true
	}

	return
}
