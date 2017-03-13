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

import (
	"regexp"
	"strings"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
)

const samplingRate = 1.0

//StatsdReporter is a implementation of Reporter interface with statsd
type StatsdReporter struct {
	client statsd.Statter
	addr   string
	tags   map[string]string
}

// StatsdStopwatch is the struct which implements the Stopwatch interface to
// start and stop the timer
type StatsdStopwatch struct {
	client    statsd.Statter
	startTime time.Time
	name      string
	tags      map[string]string
}

//NewStatsdReporter return a report which reports to statsd on the given addr
func NewStatsdReporter(HostPort string, Prefix string, FlushInterval time.Duration, FlushBytes int, tags map[string]string) Reporter {
	client, err := statsd.NewBufferedClient(HostPort, Prefix, FlushInterval, FlushBytes)
	if err != nil {
		return nil
	}
	return &StatsdReporter{client: client, addr: HostPort, tags: tags}
}

// InitMetrics is used to initialize the metrics map with the respective type
func (r *StatsdReporter) InitMetrics(metricMap map[MetricName]MetricType) {
	//pass
}

// GetChildReporter is used to get a child reporter from the parent
// this also makes sure we have all the tags from the parent in
// addition to the tags supplied here
func (r *StatsdReporter) GetChildReporter(tags map[string]string) Reporter {
	client, err := statsd.NewBufferedClient(r.addr, "cherami", time.Second, 0)
	if err != nil {
		return nil
	}
	statsdReporter := &StatsdReporter{client: client, addr: r.addr, tags: tags}
	mergeDictoRight(r.tags, statsdReporter.tags)
	return statsdReporter
}

// GetTags gets the tags for this reporter object
func (r *StatsdReporter) GetTags() map[string]string {
	return r.tags
}

// IncCounter should be used for Counter style metrics
func (r *StatsdReporter) IncCounter(name string, tags map[string]string, delta int64) {
	reporterTags := mergeDicts(r.tags, tags)
	r.client.Inc(metricstoPrefix(name, reporterTags), delta, samplingRate)
}

// UpdateGauge should be used for Gauge style metrics
func (r *StatsdReporter) UpdateGauge(name string, tags map[string]string, value int64) {
	reporterTags := mergeDicts(r.tags, tags)
	r.client.Gauge(metricstoPrefix(name, reporterTags), value, samplingRate)
}

func newStatsdStopWatch(client statsd.Statter, name string, tags map[string]string) Stopwatch {
	return &StatsdStopwatch{client: client, name: name, startTime: time.Now(), tags: tags}
}

// StartTimer should be used for measuring latency.
// this returns a Stopwatch which can be used to stop the timer
func (r *StatsdReporter) StartTimer(name string, tags map[string]string) Stopwatch {
	reporterTags := mergeDicts(r.tags, tags)
	return newStatsdStopWatch(r.client, name, reporterTags)
}

// RecordTimer should be used for measuring latency when you cannot start the stop watch.
func (r *StatsdReporter) RecordTimer(name string, tags map[string]string, d time.Duration) {
	reporterTags := mergeDicts(r.tags, tags)
	r.client.TimingDuration(metricstoPrefix(name, reporterTags), d, samplingRate)
}

// Stop stops the stop watch and records the latency to M3
func (s *StatsdStopwatch) Stop() time.Duration {
	d := time.Since(s.startTime)
	s.client.TimingDuration(metricstoPrefix(s.name, s.tags), d, samplingRate)
	return d
}

func mergeDictoRight(src map[string]string, dest map[string]string) {
	for k, v := range src {
		dest[k] = v
	}
}

func mergeDicts(dic1 map[string]string, dic2 map[string]string) (resultDict map[string]string) {
	resultDict = make(map[string]string)
	mergeDictoRight(dic1, resultDict)
	mergeDictoRight(dic2, resultDict)
	return
}

// MetricWithPrefix is the default mapping for metrics to statsd keys.
func metricstoPrefix(name string, tags map[string]string) string {
	parts := []string{name}

	for _, tag := range tagsFlattenOrder {
		if v, ok := tags[tag]; ok {
			parts = append(parts, clean(v))
		}
	}

	return strings.Join(parts, ".")
}

var specialChars = regexp.MustCompile(`[{}/\\:\s.]`)

// clean replaces special characters [{}/\\:\s.] with '-'
func clean(keyPart string) string {
	return specialChars.ReplaceAllString(keyPart, "-")
}
