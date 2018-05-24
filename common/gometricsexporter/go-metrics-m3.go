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

package gometricsexporter

import (
	gometrics "github.com/rcrowley/go-metrics"
	m "github.com/uber/cherami-server/common/metrics"
	"time"
)

const (
	delayBetweenExports = time.Second * 10
)

// GoMetricsExporter allows an rcrowley/go-metrics registry to be
// periodically exported to the Cherami metrics system
type GoMetricsExporter struct {
	m     m.Client
	scope int

	// metricNameToMetric maps a metric name to the metric symbol,
	// e.g. "request-rate" -> OutputhostCGKafkaRequestRate
	metricNameToMetric  map[string]int
	closeCh             chan struct{}
	metricNameToLastVal map[string]int64
	registry            gometrics.Registry // Registry to be exported
}

// NewGoMetricsExporter starts the exporter loop to export go-metrics to the Cherami
// metrics system and returns the go-metrics registry that should be used
func NewGoMetricsExporter(
	m m.Client,
	scope int,
	metricNameToMetric map[string]int,
) (*GoMetricsExporter, gometrics.Registry) {
	// Make a new registy and give it to the caller. This allows the exporters to operate independently
	registry := gometrics.NewRegistry()
	r := &GoMetricsExporter{
		m:                   m,
		scope:               scope,
		registry:            registry,
		metricNameToMetric:  metricNameToMetric,
		closeCh:             make(chan struct{}, 0),
		metricNameToLastVal: make(map[string]int64),
	}
	return r, registry
}

// Stop stops the exporter
func (r *GoMetricsExporter) Stop() {
	close(r.closeCh)
}

// Run runs the exporter. It blocks until Stop() is called, so it should probably be run in a go-routine
func (r *GoMetricsExporter) Run() {
	t := time.NewTicker(delayBetweenExports)
	defer t.Stop()

exportLoop:
	for {
		r.export()

		select {
		case <-t.C:
		case <-r.closeCh:
			break exportLoop
		}
	}
}

// export solves a problem in exporting from rcrowley/go-metrics to m3 metrics. Both of these packages want to do the
// local aggregation step of the local-reporting->local-aggregation->remote-reporting metrics pipeline. We have to
// de-aggregate from rcrowley in order to properly report in m3. There is also some shoe-horning that must occur, because
// m3 does not have a histogram type, but exposes that functionality in its timers.
func (r *GoMetricsExporter) export() {
	r.registry.Each(func(name string, i interface{}) {
		if metricID, ok := r.metricNameToMetric[name]; ok {
			switch metric := i.(type) {
			case gometrics.Histogram:
				// The timer metric type most closely approximates the histogram type. Milliseconds is the default unit, so
				// we scale our values as such. Extract the values from the registry, since we aggregate on our own.
				for _, v := range metric.Snapshot().Sample().Values() {
					if v == 0 { // Zero values are sometimes emitted, due to an issue with go-metrics
						continue
					}

					// Default unit for our timers is milliseconds, so this prevents a need to scale the metric for display
					r.m.RecordTimer(r.scope, metricID, time.Duration(v)*time.Millisecond)
				}

				metric.Clear()
			case gometrics.Meter:
				// Nominally, the meter is a rate-type metric, but we can extract the
				// underlying count and let our reporter aggregate
				count := metric.Snapshot().Count()

				// Last value is needed because our Cherami counter is incremental only
				lastVal := r.metricNameToLastVal[name]
				r.metricNameToLastVal[name] = count

				r.m.AddCounter(r.scope, metricID, count-lastVal)
			}
		}
	})
}
