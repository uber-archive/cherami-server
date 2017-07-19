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
	"strings"
)

// ReplicatorConfig -- contains config info passed to replicator
type ReplicatorConfig struct {
	DefaultAuthoritativeZone string            `yaml:"DefaultAuthoritativeZone"`
	ReplicatorHosts          map[string]string `yaml:"ReplicatorHosts"`
	UseStandalone            string            `yaml:"UseStandalone"`
}

// NewCommonReplicatorConfig returns the replicator config
func NewCommonReplicatorConfig() *ReplicatorConfig {
	return &ReplicatorConfig{
		ReplicatorHosts: make(map[string]string),
	}
}

// GetReplicatorHosts returns the map of all replicator hosts
func (r *ReplicatorConfig) GetReplicatorHosts() map[string]string {
	return r.ReplicatorHosts
}

// GetDefaultAuthoritativeZone returns the default authoritative zone
func (r *ReplicatorConfig) GetDefaultAuthoritativeZone() string {
	return r.DefaultAuthoritativeZone
}

// GetUseStandalone checks whether a specific deployment is using standalone deployment
func (r *ReplicatorConfig) GetUseStandalone(deployment string) bool {
	standaloneDeployments := strings.Split(r.UseStandalone, `,`)
	for _, d := range standaloneDeployments {
		if strings.EqualFold(deployment, d) {
			return true
		}
	}
	return false
}
