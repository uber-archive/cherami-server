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

package load

import (
	"github.com/uber/cherami-server/common"
)

const (
	// ExtentMetricNumReadConns indicates number of connections from outputhost for extent
	ExtentMetricNumReadConns = iota
	// ExtentMetricNumWriteConns indicates number of connections from inputhost for extent
	ExtentMetricNumWriteConns
	// ExtentMetricReadLatency indicates message read latency for extent
	ExtentMetricReadLatency
	// ExtentMetricWriteLatency indicates message write latency for extent
	ExtentMetricWriteLatency
	// ExtentMetricMsgsRead indicates number of messages for extent
	ExtentMetricMsgsRead
	// ExtentMetricMsgsWritten indicates number of messages written for extent
	ExtentMetricMsgsWritten
	// ExtentMetricBytesRead indicates number of bytes read for extent
	ExtentMetricBytesRead
	// ExtentMetricBytesWritten indicates number of bytes written for extent
	ExtentMetricBytesWritten
	// numExtentMetrics gives the number of extent level metrics
	numExtentMetrics
)

// ExtentMetrics is the type that
// contains metrics aggregated at
// the extent level
type ExtentMetrics struct {
	*common.CounterBank
}

// NewExtentMetrics returns a new instance
// of ExtentMetrics, a group of metrics
// aggregated at extent level
func NewExtentMetrics() *ExtentMetrics {
	return &ExtentMetrics{
		CounterBank: common.NewCounterBank(numExtentMetrics),
	}
}

const (
	// HostMetricNumReadConns indicates number of connections from outputhost for this store
	HostMetricNumReadConns = iota
	// HostMetricNumWriteConns indicates number of connects from inputhost for this store
	HostMetricNumWriteConns
	// HostMetricMsgsRead indicates number of messages read from this store
	HostMetricMsgsRead
	// HostMetricMsgsWritten indicates number of messages written to this store
	HostMetricMsgsWritten
	// HostMetricBytesRead indicates number of bytes read from this store
	HostMetricBytesRead
	// HostMetricBytesWritten indicates number of bytes written to this store
	HostMetricBytesWritten
	// HostMetricFreeDiskSpaceBytes indicates remaining disk space
	HostMetricFreeDiskSpaceBytes
	// numHostMetrics gives the number of store level metrics
	numHostMetrics
)

// HostMetrics is the type that
// contains metrics aggregated at
// the host level
type HostMetrics struct {
	*common.CounterBank
}

// NewHostMetrics returns a new instance
// of HostMetrics, a group of metrics
// aggregated at store host level
func NewHostMetrics() *HostMetrics {
	return &HostMetrics{
		CounterBank: common.NewCounterBank(numHostMetrics),
	}
}
