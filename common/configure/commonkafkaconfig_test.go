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
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func TestLoadKafkaConfig(t *testing.T) {
	KafkaConfig := NewCommonKafkaConfig()
	KafkaConfig.KafkaClusterConfigFile = "../../config/local_kafka_clusters.yaml"
	clusters := KafkaConfig.GetKafkaClusters()
	sort.Strings(clusters)
	assert.Equal(t, 2, len(clusters))
	assert.Equal(t, "local", clusters[0])
	assert.Equal(t, "test", clusters[1])

	clusterConfig, ok := KafkaConfig.GetKafkaClusterConfig("local")
	assert.True(t, ok)
	assert.Equal(t, 1, len(clusterConfig.Brokers))
	assert.Equal(t, "localhost:9092", clusterConfig.Brokers[0])
	assert.Equal(t, 1, len(clusterConfig.Zookeepers))
	assert.Equal(t, "localhost", clusterConfig.Zookeepers[0])
	assert.Equal(t, 0, len(clusterConfig.Chroot))

	clusterConfig2, ok2 := KafkaConfig.GetKafkaClusterConfig("not_existing_cluster")
	assert.False(t, ok2)
	assert.Equal(t, 0, len(clusterConfig2.Brokers))
	assert.Equal(t, 0, len(clusterConfig2.Zookeepers))
	assert.Equal(t, 0, len(clusterConfig2.Chroot))

	clusterConfig3, ok3 := KafkaConfig.GetKafkaClusterConfig("test")
	assert.True(t, ok3)
	assert.Equal(t, 2, len(clusterConfig3.Brokers))
	assert.Equal(t, "server1:9092", clusterConfig3.Brokers[0])
	assert.Equal(t, "server2:9092", clusterConfig3.Brokers[1])
	assert.Equal(t, 2, len(clusterConfig3.Zookeepers))
	assert.Equal(t, "server3", clusterConfig3.Zookeepers[0])
	assert.Equal(t, "server4", clusterConfig3.Zookeepers[1])
	assert.Equal(t, "test", clusterConfig3.Chroot)
}
