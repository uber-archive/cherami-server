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

package metrics

import "time"

type (
	// Reporter is the the interface used to report stats.
	Reporter interface {
		// InitMetrics is used to initialize the metrics map
		// with the respective type
		InitMetrics(metricMap map[MetricName]MetricType)

		// GetChildReporter is used to get a child reporter from the parent
		// this also makes sure we have all the tags from the parent in
		// addition to the tags supplied here
		GetChildReporter(tags map[string]string) Reporter

		// GetTags gets the tags for this reporter object
		GetTags() map[string]string

		// IncCounter should be used for Counter style metrics
		IncCounter(name string, tags map[string]string, delta int64)

		// UpdateGauge should be used for Gauge style metrics
		UpdateGauge(name string, tags map[string]string, value int64)

		// StartTimer should be used for measuring latency.
		// this returns a Stopwatch which can be used to stop the timer
		StartTimer(name string, tags map[string]string) Stopwatch

		// RecordTimer should be used for measuring latency when you cannot start the stop watch.
		RecordTimer(name string, tags map[string]string, d time.Duration)
	}

	// Client is  the interface used to report metrics to m3 backend.
	Client interface {
		// IncCounter increments a counter and emits
		// to m3 backend
		IncCounter(scope int, counter int)
		// AddCounter adds delta to the counter and
		// emits to the m3 backend
		AddCounter(scope int, counter int, delta int64)
		// StartTimer starts a timer for the given
		// metric name
		StartTimer(scope int, timer int) Stopwatch
		// RecordTimer starts a timer for the given
		// metric name
		RecordTimer(scope int, timer int, d time.Duration)
		// UpdateGauge reports Gauge type metric to M3
		UpdateGauge(scope int, gauge int, delta int64)
		// GetParentReporter return the parentReporter
		GetParentReporter() Reporter
	}

	// Stopwatch is the interface to stop the timer
	Stopwatch interface {
		Stop() time.Duration
	}
)
