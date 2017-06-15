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
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path"
	"runtime"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/uber/tchannel-go"
	"gopkg.in/validator.v2"
	"gopkg.in/yaml.v2"
)

//Possible environment
const (
	EnvProduction  = "production"
	EnvDevelopment = "development"
	EnvTest        = "test"
)

const (
	configDir = "config"
	baseFile  = "base"
)

const (
	// InputServiceName refers to the name of the cherami in service
	InputServiceName = "cherami-inputhost"
	// OutputServiceName refers to the name of the cherami out service
	OutputServiceName = "cherami-outputhost"
	// FrontendServiceName refers to the name of the cherami frontend service
	FrontendServiceName = "cherami-frontendhost"
	// ControllerServiceName refers to the name of the cherami controller service
	ControllerServiceName = "cherami-controllerhost"
	// StoreServiceName refers to the name of the cherami store service
	StoreServiceName = "cherami-storehost"
	// ReplicatorServiceName refers to the name of the cherami replicator service
	ReplicatorServiceName = "cherami-replicator"
)

// ErrNoFilesToLoad is return when you attemp to call LoadFiles with no file paths
var ErrNoFilesToLoad = errors.New("attempt to load configuration with no files")

// ValidationError is the returned when a configuration fails to pass validation
type ValidationError struct {
	errorMap validator.ErrorMap
}

// ErrForField returns the validation error for the given field
func (e ValidationError) ErrForField(name string) error {
	return e.errorMap[name]
}

func (e ValidationError) Error() string {
	var w bytes.Buffer

	fmt.Fprintf(&w, "validation failed")
	for f, err := range e.errorMap {
		fmt.Fprintf(&w, "   %s: %v\n", f, err)
	}

	return w.String()
}

//CommonConfigure is the config implementation with yaml parse
type CommonConfigure struct {
	environmentKey string
	datacenterKey  string
	configDirKey   string
}

//NewCommonConfigure return an configure for common open source use
func NewCommonConfigure() Configure {
	return &CommonConfigure{
		//environmentKey: "UBER_ENVIRONMENT",
		//datacenterKey:  "UBER_CENTER",
		//configDirKey:   "UBER_CONFIG_DIR",
		environmentKey: "CHERAMI_ENVIRONMENT",
		datacenterKey:  "CHERAMI_CENTER",
		configDirKey:   "CHERAMI_CONFIG_DIR",
	}
}

// GetHostname returns the hostname
func (r *CommonConfigure) GetHostname() string {
	host, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return host
}

// GetEnvironment returns the environment
func (r *CommonConfigure) GetEnvironment() string {
	env := os.Getenv(r.environmentKey)
	if env == "" {
		env = EnvDevelopment
	}
	return env
}

//getDefaultConfigFiles get the default config file locations
func (r *CommonConfigure) getDefaultConfigFiles() []string {
	env := r.GetEnvironment()
	dc := r.GetDatacenter()

	candidates := []string{
		baseFile,
		env,
		fmt.Sprintf("%s-%s", env, dc),
	}

	realConfigDir := configDir
	// Allow overriding the directory config is loaded from, useful for tests
	// inside subdirectories when the config/ dir is in the top-level of a project.
	if configRoot := os.Getenv(r.configDirKey); configRoot != "" {
		realConfigDir = configRoot
	}
	paths := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		configFile := path.Join(realConfigDir, fmt.Sprintf("%s.yaml", candidate))
		if _, err := os.Stat(configFile); err == nil {
			paths = append(paths, configFile)
		}
	}
	return paths
}

// GetDatacenter returns the datacenter
func (r *CommonConfigure) GetDatacenter() string {
	return os.Getenv(r.datacenterKey)
}

// Load loads configuration based on environment variables
func (r *CommonConfigure) Load(config interface{}) error {
	return r.LoadFiles(config, r.getDefaultConfigFiles()...)
}

// LoadFile loads and validates a configuration object contained in a file
func (r *CommonConfigure) LoadFile(config interface{}, fname string) error {
	return r.LoadFiles(config, fname)
}

// LoadFiles loads a list of files, deep-merging values.
func (r *CommonConfigure) LoadFiles(config interface{}, fnames ...string) error {
	if len(fnames) == 0 {
		return ErrNoFilesToLoad
	}
	for _, fname := range fnames {
		data, err := ioutil.ReadFile(fname)
		if err != nil {
			return err
		}

		if err = yaml.Unmarshal(data, config); err != nil {
			return err
		}

	}

	// Validate on the merged config at the end.
	if err := validator.Validate(config); err != nil {
		return ValidationError{
			errorMap: err.(validator.ErrorMap),
		}
	}
	return nil
}

// SetupServerConfig sets up the server config
func (r *CommonConfigure) SetupServerConfig() CommonAppConfig {
	// instantiate a common config object
	cfg := NewCommonAppConfig()
	var services = []string{InputServiceName, OutputServiceName, StoreServiceName, ControllerServiceName, FrontendServiceName, ReplicatorServiceName}
	runtime.GOMAXPROCS(runtime.NumCPU())

	if err := r.Load(cfg); err != nil {
		log.Fatalf("Error initializing configuration: %v", err)
	} else {
		lCfg := cfg.GetLoggingConfig().(*LogConfiguration)
		lCfg.Configure(cfg)
	}

	if cfg.GetMetadataConfig().GetCassandraHosts() == `` {
		if cfg.GetDefaultServiceConfig().GetListenAddress().IsLoopback() {
			cfg.GetMetadataConfig().SetCassandraHosts(cfg.GetDefaultServiceConfig().GetListenAddress().String())
		} else {
			cfg.GetMetadataConfig().SetCassandraHosts(`127.0.0.1`)
		}
	}

	if id := uuid.Parse(cfg.GetStorageConfig().GetHostUUID()); id == nil {
		// only generate a new uuid if the uuid from the
		// config is empty or malformed
		cfg.GetStorageConfig().SetHostUUID(uuid.New())
	}

	for _, k := range services {
		sCfg := cfg.GetServiceConfig(k)
		if sCfg.GetHyperbahnBootstrapFile() == "" {
			sCfg.SetHyperbahnBootstrapFile(cfg.GetDefaultServiceConfig().GetHyperbahnBootstrapFile())
		}

		if sCfg.GetListenAddress() == nil {
			// service-specific ListenAddress is not configured, try DefaultServiceConfig
			listenAddress := cfg.GetDefaultServiceConfig().GetListenAddress().String()
			if net.ParseIP(listenAddress) == nil {
				// default ListenAddress is not set, try obtain from TChannel
				ip, _ := tchannel.ListenIP()
				log.Infof(`No listen address specified; setting %v instead`, ip)
				if ip == nil {
					log.Panic(`Could not get listen address`)
				}

				listenAddress = ip.String()
			}

			sCfg.SetListenAddress(listenAddress)
		}

		if sCfg.GetRingHosts() == `` {
			sCfg.SetRingHosts(cfg.GetDefaultServiceConfig().GetRingHosts())
		}

		sCfg.SetLimitsEnabled(cfg.GetDefaultServiceConfig().GetLimitsEnabled())
		cfg.SetServiceConfig(k, sCfg)
	}

	return cfg
}
