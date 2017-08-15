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
	"net"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

type (
	// Configure is the interface for set cherami configuration
	// This is the factory interface which is used to implement
	// a specific configurator. The configurator can be
	// either yaml based config or any other config which implements
	// the following methods.
	Configure interface {
		// GetHostname returns the hostname
		GetHostname() string

		// GetEnvironment returns the environment
		GetEnvironment() string

		// GetDatacenter returns the datacenter
		GetDatacenter() string

		// Load loads configuration based on environment variables
		Load(config interface{}) error

		// LoadFile loads and validates a configuration object contained in a file
		LoadFile(config interface{}, fname string) error

		// LoadFiles loads a list of files, deep-merging values.
		LoadFiles(config interface{}, fnames ...string) error

		// SetupServerConfig setsup the app config for the given configurator and
		// returns the loaded config
		SetupServerConfig() CommonAppConfig
	}

	// CommonAppConfig is the interface exposed to implement app config related
	// to cherami.
	// Any config object which satisfies this interface can be used for configuration
	// By default, our config is based on yaml files which has all the necessary config
	// to bootstrap cherami services.
	CommonAppConfig interface {
		// GetServiceConfig is used to get the service config specific to a service
		// this is mainly boottsrap config
		GetServiceConfig(serviceName string) CommonServiceConfig
		// GetMetadataConfig is used to retrieve the config related to metadata
		GetMetadataConfig() CommonMetadataConfig
		// GetControllerConfig is used to retrieve the config related to controller
		GetControllerConfig() CommonControllerConfig
		// GetFrontendConfig is used to retrieve the config related to frontend
		GetFrontendConfig() CommonFrontendConfig
		// GetStorageConfig is used to retrieve the config related to Store
		GetStorageConfig() CommonStorageConfig
		// GetReplicatorConfig is used to retrieve the config related to replicator
		GetReplicatorConfig() CommonReplicatorConfig
		// GetLoggingConfig returns the logging config to be used
		GetLoggingConfig() interface{}
		// GetDestinationConfig returns the destination config
		GetDestinationConfig() CommonDestinationConfig
		// GetDefaultServiceConfig is used to retrieve the default bootstrap service config
		GetDefaultServiceConfig() CommonServiceConfig
		// GetKafkaConfig gets the Kafka configuration
		GetKafkaConfig() CommonKafkaConfig
		// SetServiceConfig is used to set the service config specific to the service
		SetServiceConfig(serviceName string, cfg CommonServiceConfig)
	}

	// CommonLogConfig holds the logging related config
	CommonLogConfig interface {
		// Configure configures the log level based on the config
		Configure(interface{})

		// GetDefaultLogger returns the logger object
		GetDefaultLogger() bark.Logger
	}

	// CommonServiceConfig holds the bootstrap config for a service
	CommonServiceConfig interface {
		// GetDynamicConfig returns the dynamic config object
		GetDynamicConfig() interface{}
		// GetMetricsConfig returns the metrics config object
		GetMetricsConfig() interface{}
		// GetRingHosts returns the ring hosts for bootstrap
		GetRingHosts() string
		// GetLimitsEnabled is used to find if limits are enabled or not
		GetLimitsEnabled() bool
		// GetWebsocketPort returns the websocket port configured for this service
		GetWebsocketPort() int
		// GetHyperbahnBootstrapFile returns the bootstrap file
		GetHyperbahnBootstrapFile() string
		// GetPort returns the port configured for this service
		GetPort() int
		// GetListenAddress returns the address this service should listen on
		GetListenAddress() net.IP
		// SetListenAddress sets the listen address for this service
		SetListenAddress(string)
		// GetLogger returns the configured logger for this service
		GetLogger() bark.Logger
		// SetLimitsEnabled sets the limits enabled config
		SetLimitsEnabled(limit bool)
		// SetHyperbahnBootstrapFile sets the hyperbahn bootstrap file
		SetHyperbahnBootstrapFile(fname string)
		// SetRinghosts sets the ring hosts for this service
		SetRingHosts(string)
		// SetupTChannelServer is used to start the tchannel/thrift server and returns the
		// appropriate channel and the thrift server
		SetupTChannelServer(sName string, thriftServices []thrift.TChanServer) (*tchannel.Channel, *thrift.Server)
		// SetPort sets the port to the given value
		SetPort(int)
		// SetWebsocketPort sets the web socket port to the given value
		SetWebsocketPort(int)
		// SetDynamicConfig sets the dynamic config object
		SetDynamicConfig(interface{})
		// GetDeploymentName is used to get the deployment name within a DC
		GetDeploymentName() string
		// SetDeploymentName is used to set the deployment name
		SetDeploymentName(dName string) error
	}

	// CommonMetadataConfig holds the metadata specific config
	CommonMetadataConfig interface {
		// GetCassandraHosts gets the cassandra seed hosts
		GetCassandraHosts() string
		// GetPort() gets the cassandra host port
		GetPort() int
		// GetKeyspace returns the keyspace for our cassandra cluster
		GetKeyspace() string
		// GetAuthentication returns the authentication info for our cassandra cluster
		GetAuthentication() Authentication
		// GetConsistency returns the configured consistency level
		GetConsistency() string
		// GetDcFilter returns the dc filter map for the cassandra cluster
		GetDcFilter() map[string]string
		// SetCassandraHosts sets the cassandra seed list to the given set of hosts
		SetCassandraHosts(hosts string)
		// GetClusterName gets the cassandra cluster name
		GetClusterName() string
		// GetNumConns returns the desired number of
		// conns from the client to every cassandra
		// server
		GetNumConns() int
	}

	// CommonControllerConfig holds the controller related config
	CommonControllerConfig interface {
		// GetTopologyFile gets the topology file to be used for placement
		GetTopologyFile() string
		// GetMinInputToStoreDistance gets the minimum input<->store distance to use
		GetMinInputToStoreDistance() uint16
		// GetMaxInputToStoreDistance gets the maximum input<->store distance to use
		GetMaxInputToStoreDistance() uint16
		// GetMinInputToStoreFallbackDistance gets the minimum fallback input<->store distance
		GetMinInputToStoreFallbackDistance() uint16
		// GetMaxInputToStoreFallbackDistance gets the maximum fallback input<->store distance
		GetMaxInputToStoreFallbackDistance() uint16
		// GetMinOutputToStoreDistance gets the minimum output<->store distance to use
		GetMinOutputToStoreDistance() uint16
		// GetMaxOutputToStoreDistance gets the maximum output<->store distance to use
		GetMaxOutputToStoreDistance() uint16
		// GetMinOutputToStoreFallbackDistance gets the minimum fallback output<->store distance
		GetMinOutputToStoreFallbackDistance() uint16
		// GetMaxOutputToStoreFallbackDistance gets the maximum fallback output<->store distance
		GetMaxOutputToStoreFallbackDistance() uint16
		// GetMinStoreToStoreDistance gets the min store<->store distance
		GetMinStoreToStoreDistance() uint16
		// GetMaxStoreToStoreDistance gets the max store<->store distance
		GetMaxStoreToStoreDistance() uint16
		// GetMinStoreToStoreFallbackDistance gets the minimum fallback store<->store distance
		GetMinStoreToStoreFallbackDistance() uint16
		// GetMaxStoreToStoreFallbackDistance gets the maximum fallback store<->store distance
		GetMaxStoreToStoreFallbackDistance() uint16
	}

	// CommonReplicatorConfig holds the replicator related config
	CommonReplicatorConfig interface {
		// GetReplicatorHosts returns the replicator hosts map
		GetReplicatorHosts() map[string]string
		// GetDefaultAuthoritativeZone returns the default authoritative zone from config
		GetDefaultAuthoritativeZone() string
		// GetUseStandalone checks whether a specific deployment is using standalone deployment
		GetUseStandalone(deployment string) bool
	}

	// CommonFrontendConfig holds the frontend related config
	CommonFrontendConfig interface {
		// GetMutatePathRegex returns the regex for path mutation
		GetMutatePathRegex() string
		// GetMutatePathPassword returns the regex for path password
		GetMutatePathPassword() string
	}

	// CommonStorageConfig holds the storage related config
	CommonStorageConfig interface {
		// GetHostUUID returns the hostuuid of the store
		GetHostUUID() string
		// GetStore returns the type of the store
		GetStore() string
		// GetBaseDir returns the base dir for storing the files
		GetBaseDir() string
		// SetHostUUID sets the host uuid for this store
		SetHostUUID(string)
	}

	// CommonDestinationConfig holds the destination related config
	CommonDestinationConfig interface {
		// GetReplicas returns the no: of replicas to be used for destinations
		GetReplicas() int16
	}

	// CommonKafkaConfig holds the Kafka-related config
	CommonKafkaConfig interface {
		GetKafkaClusters() []string
		GetKafkaClusterConfig(cluster string) (ClusterConfig, bool)
	}
)
