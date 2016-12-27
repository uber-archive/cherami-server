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
	// HostMetricNumOpenConns represents number of open conns per host
	HostMetricNumOpenConns = iota
	// HostMetricNumOpenExtents represents number of open extents per host
	HostMetricNumOpenExtents
	// HostMetricMsgsOut represents count of outgoing messages per host
	HostMetricMsgsOut
	// HostMetricBytesOut represents count of outgoing bytes per host
	HostMetricBytesOut
	// numHostMetric gives the number of host metrics
	numHostMetrics // must be the last value
)

// HostMetrics represents the load metrics
// that are aggregated at the input host level
type HostMetrics struct {
	*common.CounterBank
}

// NewHostMetrics returns a new instance of
// input host level load metrics. Should be
// one instance per input host
func NewHostMetrics() *HostMetrics {
	return &HostMetrics{
		CounterBank: common.NewCounterBank(numHostMetrics),
	}
}

const (
	// CGMetricNumOpenConns represents number of open conns per consumer group
	CGMetricNumOpenConns = iota
	// CGMetricNumOpenExtents represents number of open extents per consgroup
	CGMetricNumOpenExtents
	// CGMetricMsgsOut represents count of outgoing messages per consgroup
	CGMetricMsgsOut
	// CGMetricBytesOut represents count of outgoing bytes per consgroup
	CGMetricBytesOut
	// CGMetricSmartRetryOn is a 0/1 counter that represents if the smart retry is on/off
	CGMetricSmartRetryOn
	// numCGMetrics gives the number of output host consumergroup level metrics
	numCGMetrics
)

// CGMetrics represents load metrics
// aggregated at a consumergroup level
type CGMetrics struct {
	*common.CounterBank
}

// NewCGMetrics returns a new instance
// of CGMetrics. Should be one instance
// per consumer group UUID
func NewCGMetrics() *CGMetrics {
	return &CGMetrics{
		CounterBank: common.NewCounterBank(numCGMetrics),
	}
}

const (
	// ExtentMetricMsgsOut represents count of outgoing messages per extent
	ExtentMetricMsgsOut = iota
	// ExtentMetricBytesOut represents count of outgoing bytes per extent
	ExtentMetricBytesOut
	// numExtentMetrics gives the number of output host extent metrics
	numExtentMetrics
)

// ExtentMetrics represents load metrics
// that are aggregated at extent level
type ExtentMetrics struct {
	*common.CounterBank
}

// NewExtentMetrics returns a new instance
// of ExtentMetrics. Should be one instance
// per extent uuid.
func NewExtentMetrics() *ExtentMetrics {
	return &ExtentMetrics{
		CounterBank: common.NewCounterBank(numExtentMetrics),
	}
}
