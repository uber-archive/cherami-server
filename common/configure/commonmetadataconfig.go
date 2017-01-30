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

// MetadataConfig holds the config info related to our metadata
type MetadataConfig struct {
	CassandraHosts string            `yaml:"CassandraHosts"`
	Keyspace       string            `yaml:"Keyspace"`
	Consistency    string            `yaml:"Consistency"`
	ClusterName    string            `yaml:"ClusterName"`
	NumConns       int               `yaml:"NumConns"`
	DcFilter       map[string]string `yaml:"DcFilter"`
}

// NewCommonMetadataConfig instantiates the metadata config for cherami
func NewCommonMetadataConfig() *MetadataConfig {
	return &MetadataConfig{
		DcFilter: make(map[string]string),
	}
}

// GetCassandraHosts returns the cassandra seed hosts for our cluster
func (r *MetadataConfig) GetCassandraHosts() string {
	return r.CassandraHosts
}

// GetKeyspace returns the keyspace to be used for cherami cluster
func (r *MetadataConfig) GetKeyspace() string {
	return r.Keyspace
}

// GetConsistency returns the consistency level to be used for cherami cluster
func (r *MetadataConfig) GetConsistency() string {
	return r.Consistency
}

// GetDcFilter returns the dc filter map
func (r *MetadataConfig) GetDcFilter() map[string]string {
	return r.DcFilter
}

// SetCassandraHosts sets the cassandra hosts for the cherami cluster
func (r *MetadataConfig) SetCassandraHosts(cHosts string) {
	r.CassandraHosts = cHosts
}

// GetClusterName gets the cassandra cluster name
func (r *MetadataConfig) GetClusterName() string {
	return r.ClusterName
}

// GetNumConns returns the desired number of
// conns from the client to every cassandra
// server
func (r *MetadataConfig) GetNumConns() int {
	return r.NumConns
}
