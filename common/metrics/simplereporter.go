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
	// SimpleReporter is the reporter used to dump metric to console for stress runs
	SimpleReporter struct {
		tags map[string]string
	}

	simpleStopWatch struct {
		metricName string
		reporter   *SimpleReporter
		startTime  time.Time
		elasped    time.Duration
	}
)

// NewSimpleReporter create an instance of Reporter which can be used for driver to emit metric to console
func NewSimpleReporter(tags map[string]string) Reporter {
	reporter := &SimpleReporter{
		tags: make(map[string]string),
	}

	if tags != nil {
		mergeDictoRight(tags, reporter.tags)
	}

	return reporter
}

// InitMetrics is used to initialize the metrics map with the respective type
func (r *SimpleReporter) InitMetrics(metricMap map[MetricName]MetricType) {
	// This is a no-op for simple reporter as it is already have a static list of metric to work with
}

// GetChildReporter creates the child reporter for this parent reporter
func (r *SimpleReporter) GetChildReporter(tags map[string]string) Reporter {

	sr := &SimpleReporter{
		tags: make(map[string]string),
	}

	// copy the parent tags as well
	mergeDictoRight(r.GetTags(), sr.GetTags())

	if tags != nil {
		mergeDictoRight(tags, sr.tags)
	}

	return sr
}

// GetTags returns the tags for this reporter object
func (r *SimpleReporter) GetTags() map[string]string {
	return r.tags
}

// IncCounter reports Counter metric to M3
func (r *SimpleReporter) IncCounter(name string, tags map[string]string, delta int64) {
	// not implemented
}

// UpdateGauge reports Gauge type metric
func (r *SimpleReporter) UpdateGauge(name string, tags map[string]string, value int64) {
	// Not implemented
}

func newSimpleStopWatch(metricName string, reporter *SimpleReporter) *simpleStopWatch {
	watch := &simpleStopWatch{
		metricName: metricName,
		reporter:   reporter,
	}

	return watch
}

func (w *simpleStopWatch) Start() {
	w.startTime = time.Now()
}

func (w *simpleStopWatch) Stop() time.Duration {
	w.elasped = time.Since(w.startTime)

	return w.elasped
}

// StartTimer returns a Stopwatch which when stopped will report the metric to M3
func (r *SimpleReporter) StartTimer(name string, tags map[string]string) Stopwatch {
	w := newSimpleStopWatch(name, r)
	w.Start()
	return w
}

// RecordTimer should be used for measuring latency when you cannot start the stop watch.
func (r *SimpleReporter) RecordTimer(name string, tags map[string]string, d time.Duration) {
	// Record the time as counter of time in milliseconds
	// not implemented
}
