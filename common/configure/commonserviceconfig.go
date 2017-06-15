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

package configure

import (
	"fmt"
	"net"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

// TChannelConfig holds the tchannel related config
type TChannelConfig struct {
	// DisableHyperbahn is a flag to disable advertising with Hyperbahn.
	DisableHyperbahn bool `yaml:"disableHyperbahn"`
}

// ServiceConfig holds service specific config
type ServiceConfig struct {
	Port                   int            `yaml:"Port"`
	ListenAddress          string         `yaml:"ListenAddress"`
	WebsocketPort          int            `yaml:"WebsocketPort"`
	RingHosts              string         `yaml:"RingHosts"`
	LimitsEnabled          bool           `yaml:"EnableLimits"`
	HyperbahnBootstrapFile string         `yaml:"HyperbahnBootstrapFile"`
	Metrics                *MetricsConfig `yaml:"Metrics"`
	TChannelConfig         TChannelConfig `yaml:"tchannel"`
}

// NewCommonServiceConfig instantiates the common service config
func NewCommonServiceConfig() *ServiceConfig {
	return &ServiceConfig{
		Metrics: NewCommonMetricsConfig(),
	}
}

// GetDynamicConfig returns the dynamic config for this service, if any
func (r *ServiceConfig) GetDynamicConfig() interface{} {
	// pass
	return nil
}

// GetRingHosts returns returns the ring hosts for this service
func (r *ServiceConfig) GetRingHosts() string {
	return r.RingHosts
}

// GetWebsocketPort returns the websocket port for this service
func (r *ServiceConfig) GetWebsocketPort() int {
	return r.WebsocketPort
}

// GetMetricsConfig returns the metrics config for this service
func (r *ServiceConfig) GetMetricsConfig() interface{} {
	return r.Metrics
}

// GetLimitsEnabled returns if limits are enabled
func (r *ServiceConfig) GetLimitsEnabled() bool {
	return r.LimitsEnabled
}

// GetHyperbahnBootstrapFile returns the hyperbahn bootstrap file for service
func (r *ServiceConfig) GetHyperbahnBootstrapFile() string {
	return r.HyperbahnBootstrapFile
}

// GetLogger returns the configured logger
func (r *ServiceConfig) GetLogger() bark.Logger {
	return bark.NewLoggerFromLogrus(logrus.StandardLogger())
}

// GetListenAddress returns the listen address for this service
func (r *ServiceConfig) GetListenAddress() net.IP {
	return net.ParseIP(r.ListenAddress)
}

// SetListenAddress sets the listen address for this service
func (r *ServiceConfig) SetListenAddress(a string) {
	r.ListenAddress = a
}

// GetPort returns the port for this service
func (r *ServiceConfig) GetPort() int {
	return r.Port
}

// SetHyperbahnBootstrapFile sets the hyperbahn bootstrap file
func (r *ServiceConfig) SetHyperbahnBootstrapFile(fName string) {
	r.HyperbahnBootstrapFile = fName
}

// SetRingHosts sets the ringhosts for this service
func (r *ServiceConfig) SetRingHosts(ringHosts string) {
	r.RingHosts = ringHosts
}

// SetLimitsEnabled sets the limits enabled parameter
func (r *ServiceConfig) SetLimitsEnabled(limit bool) {
	r.LimitsEnabled = limit
}

// SetupTChannelServer sets up the tchannel server based on the given port
// TODO: hookup with hyperbahn based on the config
func (r *ServiceConfig) SetupTChannelServer(sName string, thriftServices []thrift.TChanServer) (*tchannel.Channel, *thrift.Server) {
	ch, err := tchannel.NewChannel(sName, &tchannel.ChannelOptions{
		DefaultConnectionOptions: tchannel.ConnectionOptions{
			FramePool: tchannel.NewSyncFramePool(),
		},
	})
	if err != nil {
		log.Fatal("Failed to create tchannel")
	}
	server := thrift.NewServer(ch)

	for _, thriftService := range thriftServices {
		server.Register(thriftService)
	}

	ip := r.GetListenAddress()
	log.Info(fmt.Sprintf("tChannel listening %s:%d", ip.String(), r.Port))
	if err = ch.ListenAndServe(fmt.Sprintf("%s:%d", ip.String(), r.Port)); err != nil {
		log.Warnf("Unable to start tchannel server: %v", err)
	}
	return ch, server
}

// SetPort sets the port for this service
func (r *ServiceConfig) SetPort(port int) {
	r.Port = port
}

// SetWebsocketPort sets the websocket port for this service
func (r *ServiceConfig) SetWebsocketPort(port int) {
	r.WebsocketPort = port
}

// SetDynamicConfig sets the dynamic config for this service, if any
func (r *ServiceConfig) SetDynamicConfig(cfg interface{}) {
	// pass
}

// GetDeploymentName gets the deployment name, if any
func (r *ServiceConfig) GetDeploymentName() string {
	// pass
	return ""
}

// SetDeploymentName sets the deployment name, if any
func (r *ServiceConfig) SetDeploymentName(dName string) error {
	// pass
	return nil
}
