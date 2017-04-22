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
	// HostMetricMsgsIn represents count of incoming messages per host
	HostMetricMsgsIn
	// HostMetricBytesIn represents count of incoming bytes per host
	HostMetricBytesIn
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
	// DstMetricNumOpenConns represents number of open conns per dst
	DstMetricNumOpenConns = iota
	// DstMetricNumOpenExtents represents number of open extents per dst
	DstMetricNumOpenExtents
	// DstMetricNumWritableExtents represents number of open and *writable* extents per dst
	// A draining extent will decrement this and this is used to trigger client connection drain
	DstMetricNumWritableExtents
	// DstMetricMsgsIn represents count of incoming messages per dst
	DstMetricMsgsIn
	// DstMetricBytesIn represents count of incoming bytes per dst
	DstMetricBytesIn
	// DstMetricNumAcks represents count of number of acked messages per dst
	DstMetricNumAcks
	// DstMetricNumNacks represents count of number of nacked messages per dst
	DstMetricNumNacks
	// DstMetricNumThrottled represents count of number of throttled messages per dst
	DstMetricNumThrottled
	// DstMetricNumFailed represents count of number of failed messages per dst
	DstMetricNumFailed
	// DstMetricOverallNumMsgs represents count of the overall number of messages per dst
	// Note: this is different than the MsgsIn above since that maintains the per second msgs
	DstMetricOverallNumMsgs
	// numDstMetrics gives the number of inpust host dst level metrics
	numDstMetrics
)

// DstMetrics represents load metrics
// aggregated at a destination level
type DstMetrics struct {
	*common.CounterBank
}

// NewDstMetrics returns a new instance
// of DstMetrics. Should be one instance
// per destination UUID
func NewDstMetrics() *DstMetrics {
	return &DstMetrics{
		CounterBank: common.NewCounterBank(numDstMetrics),
	}
}

const (
	// ExtentMetricMsgsIn represents count of incoming messages per extent
	ExtentMetricMsgsIn = iota
	// ExtentMetricBytesIn represents count of incoming bytes per extent
	ExtentMetricBytesIn
	// numExtentMetrics gives the number of input host extent metrics
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
