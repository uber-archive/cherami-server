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

// ClientImpl is for m3 emits within inputhost
type ClientImpl struct {
	//parentReporter is the parent for the metrics Reporters
	parentReporter Reporter
	// childReporters is the children for the metrics Reporters
	childReporters map[int]Reporter
}

// metricNames holds the global mapping from metric-index to metric-name
var metricNames []string

// init is called when the module is initialized
func init() {

	// initialize the global metricNames lookup array
	metricNames = make([]string, numMetrics)

	for _, serviceMetricDefs := range metricDefs {
		for idx, def := range serviceMetricDefs {
			metricNames[idx] = def.metricName
		}
	}

	for _, serviceMetricDefs := range dynamicMetricDefs {
		for idx, def := range serviceMetricDefs {
			metricNames[idx] = def.metricName
		}
	}
}

// NewClient creates and returns a new instance of
// Client implementation
// reporter holds the common tags for the servcie
// serviceIdx indicates the service type in (InputhostIndex, ... StorageIndex)
func NewClient(reporter Reporter, serviceIdx ServiceIdx) Client {

	// create metrics map used for all the scopes that are going to be initialized here
	commonMetricDefs := metricDefs[Common]
	serviceMetricDefs := metricDefs[serviceIdx]

	metricsMap := make(map[MetricName]MetricType, len(commonMetricDefs)+len(serviceMetricDefs))

	for _, m := range commonMetricDefs {
		metricsMap[MetricName(m.metricName)] = MetricType(m.metricType)
	}

	for _, m := range serviceMetricDefs {
		metricsMap[MetricName(m.metricName)] = MetricType(m.metricType)
	}

	childReporters := make(map[int]Reporter)

	initChildReporter := func(def scopeDefinition) Reporter {

		scopeTags := map[string]string{
			OperationTagName: def.operation,
		}

		mergeDictoRight(def.tags, scopeTags) // include additional tags, if any

		return reporter.GetChildReporter(scopeTags)
	}

	// initialize reporters for Common operations
	for scope, def := range scopeDefs[Common] {

		childReporters[scope] = initChildReporter(def)
		childReporters[scope].InitMetrics(metricsMap)
	}

	// initialize scope reporters for service operations
	for scope, def := range scopeDefs[serviceIdx] {

		childReporters[scope] = initChildReporter(def)
		childReporters[scope].InitMetrics(metricsMap)
	}

	return &ClientImpl{
		parentReporter: reporter,
		childReporters: childReporters,
	}
}

// NewClientWithTags creates and returens a new metrics.Client, with the new tags added to scope
// Use get client only for per destination or per consumer group right now.
// For other metrics, use NewClient.
func NewClientWithTags(m3Client Client, serviceIdx ServiceIdx, tags map[string]string) Client {

	reporter := m3Client.GetParentReporter()

	// create metrics map used for all the scopes that are going to be initialized here
	commonMetricDefs := dynamicMetricDefs[Common]
	serviceMetricDefs := dynamicMetricDefs[serviceIdx]

	metricsMap := make(map[MetricName]MetricType, len(commonMetricDefs)+len(serviceMetricDefs))

	for _, m := range commonMetricDefs {
		metricsMap[MetricName(m.metricName)] = MetricType(m.metricType)
	}

	for _, m := range serviceMetricDefs {
		metricsMap[MetricName(m.metricName)] = MetricType(m.metricType)
	}

	childReporters := make(map[int]Reporter)

	initChildReporter := func(def scopeDefinition, tags map[string]string) Reporter {

		scopeTags := map[string]string{
			OperationTagName: def.operation,
		}

		mergeDictoRight(def.tags, scopeTags) // include additional tags, if any
		mergeDictoRight(tags, scopeTags)     // include tags passed in, if any

		return reporter.GetChildReporter(scopeTags)
	}

	// initialize reporters for Common operations
	for scope, def := range dynamicScopeDefs[Common] {
		childReporters[scope] = initChildReporter(def, tags)
		childReporters[scope].InitMetrics(metricsMap)
	}

	// initialize scope reporters for service operations
	for scope, def := range dynamicScopeDefs[serviceIdx] {
		childReporters[scope] = initChildReporter(def, tags)
		childReporters[scope].InitMetrics(metricsMap)
	}

	return &ClientImpl{
		parentReporter: reporter,
		childReporters: childReporters,
	}
}

// IncCounter increments one for a counter and emits to m3 backend
func (m *ClientImpl) IncCounter(scopeIdx int, metricIdx int) {
	m.childReporters[scopeIdx].IncCounter(metricNames[metricIdx], nil, 1)
}

// AddCounter adds delta to the counter and emits to the m3 backend
func (m *ClientImpl) AddCounter(scopeIdx int, metricIdx int, delta int64) {
	m.childReporters[scopeIdx].IncCounter(metricNames[metricIdx], nil, delta)
}

// StartTimer starts a timer for the given metric name
func (m *ClientImpl) StartTimer(scopeIdx int, metricIdx int) Stopwatch {
	return m.childReporters[scopeIdx].StartTimer(metricNames[metricIdx], nil)
}

// RecordTimer record and emit a timer for the given metric name
func (m *ClientImpl) RecordTimer(scopeIdx int, metricIdx int, d time.Duration) {
	m.childReporters[scopeIdx].RecordTimer(metricNames[metricIdx], nil, d)
}

// UpdateGauge reports Gauge type metric to M3
func (m *ClientImpl) UpdateGauge(scopeIdx int, metricIdx int, delta int64) {
	m.childReporters[scopeIdx].UpdateGauge(metricNames[metricIdx], nil, delta)
}

// GetParentReporter return the parentReporter
func (m *ClientImpl) GetParentReporter() Reporter {
	return m.parentReporter
}

func (m *ClientImpl) getScopeReporter(scopeIdx int) Reporter {
	return m.childReporters[scopeIdx]
}
