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

// AppConfig stores the common app config for Cherami
type AppConfig struct {
	LogConfig            *LogConfiguration         `yaml:"logging"`
	DefaultServiceConfig *ServiceConfig            `yaml:"DefaultServiceConfig"`
	ServiceConfig        map[string]*ServiceConfig `yaml:"ServiceConfig"`
	MetadataConfig       *MetadataConfig           `yaml:"MetadataConfig"`
	ControllerConfig     *ControllerConfig         `yaml:"ControllerConfig"`
	FrontendConfig       *FrontendConfig           `yaml:"FrontendConfig"`
	StorageConfig        *StorageConfig            `yaml:"StorageConfig"`
	ReplicatorConfig     *ReplicatorConfig         `yaml:"ReplicatorConfig"`
	KafkaConfig          *KafkaConfig              `yaml:"KafkaConfig"`

	DefaultDestinationConfig *DestinationConfig `yaml:"DefaultDestinationConfig"`
}

// NewCommonAppConfig instantiates a new app config
func NewCommonAppConfig() CommonAppConfig {
	return &AppConfig{
		LogConfig:                NewCommonLogConfig(),
		DefaultServiceConfig:     NewCommonServiceConfig(),
		ServiceConfig:            make(map[string]*ServiceConfig),
		MetadataConfig:           NewCommonMetadataConfig(),
		ControllerConfig:         NewCommonControllerConfig(),
		FrontendConfig:           NewCommonFrontendConfig(),
		StorageConfig:            NewCommonStorageConfig(),
		ReplicatorConfig:         NewCommonReplicatorConfig(),
		DefaultDestinationConfig: NewDestinationConfig(),
		KafkaConfig:              NewCommonKafkaConfig(),
	}
}

// GetServiceConfig returns the config specific to thegiven service
func (r *AppConfig) GetServiceConfig(sName string) CommonServiceConfig {
	sCfg, _ := r.ServiceConfig[sName]

	return sCfg
}

// GetMetadataConfig returns the metadata config
func (r *AppConfig) GetMetadataConfig() CommonMetadataConfig {
	return r.MetadataConfig
}

// GetControllerConfig returns the controller config
func (r *AppConfig) GetControllerConfig() CommonControllerConfig {
	return r.ControllerConfig
}

// GetFrontendConfig returns the frontend config
func (r *AppConfig) GetFrontendConfig() CommonFrontendConfig {
	return r.FrontendConfig
}

// GetStorageConfig returns the storage config
func (r *AppConfig) GetStorageConfig() CommonStorageConfig {
	return r.StorageConfig
}

// GetReplicatorConfig returns the replicator config
func (r *AppConfig) GetReplicatorConfig() CommonReplicatorConfig {
	return r.ReplicatorConfig
}

// GetDestinationConfig returns the destination config
func (r *AppConfig) GetDestinationConfig() CommonDestinationConfig {
	return r.DefaultDestinationConfig
}

// GetLoggingConfig returns the logging config
func (r *AppConfig) GetLoggingConfig() interface{} {
	return r.LogConfig
}

// SetServiceConfig sets the service config corresponding to the service name
func (r *AppConfig) SetServiceConfig(sName string, sCfg CommonServiceConfig) {
	r.ServiceConfig[sName] = sCfg.(*ServiceConfig)
}

// GetDefaultServiceConfig returns the default service config
func (r *AppConfig) GetDefaultServiceConfig() CommonServiceConfig {
	return r.DefaultServiceConfig
}

// GetKafkaConfig returns the default service config
func (r *AppConfig) GetKafkaConfig() CommonKafkaConfig {
	return r.KafkaConfig
}
