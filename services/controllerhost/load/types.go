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
	"errors"
)

type (
	// AggregateType is an enum type that refers
	// to some aggregation function. Examples are
	// Average, Sum etc
	AggregateType int

	// MetricName is a string type that
	// represents a metric name
	MetricName string

	// MetricsAggregator collects and aggregates metrics,
	// usually by time. Any implementation of such time
	// series aggregator MUST support the following
	// methods.
	MetricsAggregator interface {
		// Start starts the metrics aggregator
		Start()
		// Stop stops the metrics aggregator
		Stop()
		// Put collects a metrics data point
		// for aggregation. Returns an error
		// if the data point cannot be accepted
		Put(hostID string, groupTag string, metricName MetricName, metricValue int64, timestamp int64) error
		// Get returns an aggregated metric value
		// identified by the given parameters
		// resultType refers to the aggregation
		// operation that needs to be performed for
		// obtaining the result. Examples are
		// OneMinAvg or FiveMinAvg.
		Get(hostID string, groupTag string, metricName MetricName, resultType AggregateType) (int64, error)
	}
)

const (
	// OneMinAvg is an aggregation operation
	// that does average of all data points
	// in the past minute
	OneMinAvg AggregateType = iota
	// OneMinSum gives the sum of
	// all data points over the
	// past minute
	OneMinSum
	// FiveMinAvg is an aggregation function
	// that does average of all data points
	// across the past five minutes
	FiveMinAvg
	// FiveMinSum gives the sum of
	// all data points over the
	// past five minutes
	FiveMinSum
)

const (
	// EmptyTag represents an empty tag
	EmptyTag string = ""
)

const (
	// NumConns is a guage that refers to number of connections
	NumConns MetricName = "numConns"
	// NumExtentsActive is a guage that refers to number of active extents
	NumExtentsActive = "numExtentsActive"
	// RemDiskSpaceBytes is a guage that refers to remaining disk space
	RemDiskSpaceBytes = "remDiskSpaceBytes"

	// MsgsInPerSec refers to incoming messages per sec counter
	MsgsInPerSec MetricName = "msgsInPerSec"
	// MsgsOutPerSec refers to outgoing messages per sec counter
	MsgsOutPerSec = "msgsOutPerSec"
	// BytesInPerSec refers to incoming bytes per sec counter
	BytesInPerSec = "bytesInPerSec"
	// BytesOutPerSec refers to outgoing messages per sec counter
	BytesOutPerSec = "bytesOutPerSec"
	// SmartRetryOn is a 0/1 counter that indicates if a
	// consumer group has smart retry on or off
	SmartRetryOn = "smartRetryOn"
)

// ErrNoData is returned when there is not enough
// data to perform the requested aggregation operation
var ErrNoData = errors.New("No data found for input")

// ErrBackpressure is returned when a data point
// cannot be accepted due to downstream backpressure
var ErrBackpressure = errors.New("Data channel full")
