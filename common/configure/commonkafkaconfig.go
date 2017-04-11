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
	log "github.com/Sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

// KafkaConfig holds the configuration for the Kafka client
type KafkaConfig struct {
	KafkaClusterConfigFile string `yaml:"kafkaClusterConfigFile"`
	ClustersConfig         ClustersConfig
}

// ClustersConfig holds the configuration for the Kafka clusters
type ClustersConfig struct {
	Clusters map[string]ClusterConfig `yaml:"clusters"`
}

// ClusterConfig holds the configuration for a single Kafka cluster
type ClusterConfig struct {
	Brokers    []string `yaml:"brokers"`
	Zookeepers []string `yaml:"zookeepers"`
	Chroot     string   `yaml:"chroot"`
}

// NewCommonKafkaConfig instantiates a Kafka config
func NewCommonKafkaConfig() *KafkaConfig {
	return &KafkaConfig{}
}

// GetKafkaClusters returns all kafka cluster names
func (r *KafkaConfig) GetKafkaClusters() []string {
	r.loadClusterConfigFileIfNecessary()

	ret := make([]string, 0, len(r.ClustersConfig.Clusters))
	for key := range r.ClustersConfig.Clusters {
		ret = append(ret, key)
	}
	return ret
}

// GetKafkaClusterConfig returns all kafka cluster names
func (r *KafkaConfig) GetKafkaClusterConfig(cluster string) (ClusterConfig, bool) {
	r.loadClusterConfigFileIfNecessary()

	val, ok := r.ClustersConfig.Clusters[cluster]
	return val, ok
}

func (r *KafkaConfig) loadClusterConfigFileIfNecessary() {
	if len(r.ClustersConfig.Clusters) > 0 {
		return
	}

	r.loadClusterConfigFile()
}

func (r *KafkaConfig) loadClusterConfigFile() {
	// TODO do we need to detect file change and reload on file change
	if len(r.KafkaClusterConfigFile) == 0 {
		log.Warnf("Could not load kafka configu because kafka cluster config file is not configured")
		return
	}

	contents, err := ioutil.ReadFile(r.KafkaClusterConfigFile)
	if err != nil {
		log.Warnf("Failed to load kafka cluster config file %s: %v", r.KafkaClusterConfigFile, err)
	}

	log.Println("Parsing kafka cluster config file", r.KafkaClusterConfigFile, ":", string(contents))
	clusters := ClustersConfig{}
	if err := yaml.Unmarshal(contents, &clusters); err != nil {
		log.Warnf("Failed to parse kafka cluster config file %s: %v", r.KafkaClusterConfigFile, err)
	} else {
		r.ClustersConfig = clusters
	}
}
