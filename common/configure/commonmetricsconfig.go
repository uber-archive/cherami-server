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
	"time"
)

// MetricsConfig holds the statsd config
type MetricsConfig struct {
	Statsd *StatsdConfiguration `yaml:"statsd"`
}

// StatsdConfiguration reports metrics through statsd
type StatsdConfiguration struct {
	// The host and port of the statsd server
	HostPort string `yaml:"hostPort" validate:"nonzero"`

	// The prefix to use in reporting to statsd
	Prefix string `yaml:"prefix" validate:"nonzero"`

	// FlushInterval is the maximum interval for sending packets.
	// If it is not specified, it defaults to 1 second.
	FlushInterval time.Duration `yaml:"flushInterval"`

	// FlushBytes specifies the maximum udp packet size you wish to send.
	// If FlushBytes is unspecified, it defaults  to 1432 bytes, which is
	// considered safe for local traffic.
	FlushBytes int `yaml:"flushBytes"`
}

// NewCommonMetricsConfig instantiates the metrics config
func NewCommonMetricsConfig() *MetricsConfig {
	return &MetricsConfig{}
}
