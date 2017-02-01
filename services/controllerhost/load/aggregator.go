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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
)

type (

	// metricData contains all the info
	// related to a single metric
	// data point
	metricData struct {
		// hostID represents the host
		// that this metrics corresponds
		// to. All load metrics MUST
		// correspond to a host
		hostID string
		// groupTag is any tag that uniquely identifies
		// a metric group, examples are extentUUID,
		// dstUUID or hostUUID
		groupTag string
		// metricName is the name of the metric
		metricName MetricName
		// metricValue is the value of the metric
		metricValue int64
		// timestamp refers to the time the metric was
		// emitted, in unix nanos
		timestamp int64
	}

	// timeslotData is the data stored per
	// metric within each time slot
	timeslotData struct {
		n   int64 // number of data points
		sum int64 // sum of all data points
	}
	// timeslot refers to a single window of
	// time during which metrics are aggregated
	timeslot struct {
		beginTime int64                    // the time when the 1st metric was added to this slot
		data      map[string]*timeslotData // data indexed by groupTag.metricName
	}

	// TimeslotAggregator is an implementation
	// of metrics aggregator that aggregates
	// data by time slots
	TimeslotAggregator struct {
		currIdx   int64 // index of the current timeslot in the circular buffer of timeslots
		started   int64
		timeslots [historySize]timeslot // circular buffer of timeslots, each entry is a minute
		dataCh    chan *metricData      // input data stream
		sigCh     chan struct{}         // channel to signal processor
		stopCh    chan struct{}
		stopWG    sync.WaitGroup
		clock     common.TimeSource
		logger    bark.Logger
	}
)

const historySize = 10 // 10 mins of history
const maxClockSkew = int64(time.Second * 5)
const aggregationPeriod = int64(time.Minute) // each timeslot is this interval
const dataChannelSize = 256                  // input stream channel size

// NewTimeSlotAggregator creates and returns an instance of
// MetricsAggregator that aggregates data at fixed interval
// timeslots. Internally, the TimeSlotAggregator maintains
// a circular buffer of timeslots, where each timeslot is
// typically, a minute. The size of the buffer determines
// the history that's stored by the aggregator. Incoming
// data is always added to the current timeslot whereas readers
// always start reading from the timeslot preceding the current
// one (and go back). This allows lockless access to the hashmaps
// that hold the aggregated metric data.
//
// MetricGroups:
//    Metrics reported to the aggregator are composed of a
// timesamp, metricName, metricValue and a groupTag. A group
// tag allows for aggregation of same metric at different
// levels. For example, when a store reports an extent metric
// nExtentsActive, this metric can be aggregated by many levels,
// i.e. storeHostUUID, extentUUID, dstUUID. This problem is
// solved by the group tag.
//
// Inside a TimeSlot:
// Each timeslot internally maintains a single hashmap that's
// indexed by a key representing a specific metric. Since the
// same metric can be used by multiple metric groups, the
// indexed string is a combination of groupTag and metricName.
//
// SigChannel:
// As mentioned before, timeslots are only advanced when there is
// incoming data. So, when there is no incoming data for a while,
// the data in the current time slot will not be consumed by readers.
// To overcome this problem, we have a signchannel that will force
// advancement of timeslot when the input stream is idle.
func NewTimeSlotAggregator(clock common.TimeSource, logger bark.Logger) *TimeslotAggregator {
	aggr := new(TimeslotAggregator)
	aggr.clock = clock
	aggr.logger = logger
	aggr.stopCh = make(chan struct{})
	aggr.sigCh = make(chan struct{})
	aggr.dataCh = make(chan *metricData, dataChannelSize)
	return aggr
}

// Start stats the timeslot aggregator
func (tsaggr *TimeslotAggregator) Start() {
	if !atomic.CompareAndSwapInt64(&tsaggr.started, 0, 1) {
		return
	}
	tsaggr.stopWG.Add(1)
	go tsaggr.processor()
	tsaggr.logger.Infof("Timeslot metrics aggregator started")
}

// Stop stops the time slot aggregator
func (tsaggr *TimeslotAggregator) Stop() {
	close(tsaggr.stopCh)
	if !common.AwaitWaitGroup(&tsaggr.stopWG, time.Second) {
		tsaggr.logger.Warn("Timed out waiting for timeslotAggregator to stop")
	}
	tsaggr.logger.Infof("Timeslot metrics aggregator stopped")
}

// Put adds a data point to the aggregator
func (tsaggr *TimeslotAggregator) Put(hostID string, groupTag string, metricName MetricName, metricValue int64, timestamp int64) error {

	data := new(metricData)
	data.hostID = hostID
	data.groupTag = groupTag
	data.metricName = metricName
	data.metricValue = metricValue
	data.timestamp = timestamp

	select {
	case tsaggr.dataCh <- data:
		return nil
	default:
		return ErrBackpressure
	}
}

// Get returns an aggregated value for a metric
// identified by the given inputs
func (tsaggr *TimeslotAggregator) Get(hostID string, groupTag string, metricName MetricName, resultType AggregateType) (int64, error) {

	now := tsaggr.clock.Now().UnixNano()
	currIdx := atomic.LoadInt64(&tsaggr.currIdx)
	currSlot := &tsaggr.timeslots[currIdx]

	// A timeslot is expired when the time since
	// the first data point is greater than a minute
	if tsaggr.isTimeslotExpired(currSlot, now) {
		// If we are idle and no data flows in
		// for a while, signal the processor
		// to advance the timeslot
		tsaggr.signal() //blocking
		// now reload the current index
		currIdx = atomic.LoadInt64(&tsaggr.currIdx)
		currSlot = &tsaggr.timeslots[currIdx]
	}

	// Always consume from timeslot preceding the
	// current slot. Current slot is for write.
	currIdx = prevSlotIdx(currIdx)
	currSlot = &tsaggr.timeslots[currIdx]

	// How far back in history should be look at
	oldestDataTime := tsaggr.getOldestDataTimeForAggrType(resultType, now)

	key := buildKey(hostID, groupTag, metricName)
	minSlots := tsaggr.minSlotsForAggrType(resultType)
	maxSlots := tsaggr.maxSlotsForAggrType(resultType)

	var nSlots int
	var result timeslotData

	for nSlots < maxSlots {

		slotEndTime := currSlot.beginTime + aggregationPeriod
		if slotEndTime <= oldestDataTime {
			break // no more slots in the desired time window
		}

		if d, ok := currSlot.data[key]; ok {
			result.n += d.n
			result.sum += d.sum
			nSlots++
		}

		currIdx = prevSlotIdx(currIdx)
		currSlot = &tsaggr.timeslots[currIdx]
	}

	if nSlots < minSlots {
		return 0, ErrNoData
	}

	if resultType == OneMinSum || resultType == FiveMinSum {
		return result.sum, nil
	}

	if result.n < tsaggr.minDataPointsForAggrType(resultType) {
		return 0, ErrNoData
	}

	return result.sum / result.n, nil
}

// processor is the main loop that processes
// the input stream data
func (tsaggr *TimeslotAggregator) processor() {

	defer tsaggr.stopWG.Done()

	for {
		select {
		case data := <-tsaggr.dataCh:
			tsaggr.add(data)
		case <-tsaggr.sigCh:
			// a signal can be received when the
			// input stream is idle for more than
			// a minute, to advance the timeslot
			tsaggr.advanceTimeslotIfExpired(tsaggr.clock.Now().UnixNano())
		case <-tsaggr.stopCh:
			return
		}
	}
}

// add adds a metric data point to the current timeslot
func (tsaggr *TimeslotAggregator) add(data *metricData) {

	now := tsaggr.clock.Now().UnixNano()
	diff := now - data.timestamp

	if diff > maxClockSkew {
		// drop the sample on floor, the clocks differ too much
		return
	}

	currSlot := tsaggr.advanceTimeslotIfExpired(now)
	if getBeginTime(currSlot) == 0 {
		// beginTime of zero indicates this is the first
		// metric for this time slot, so record the
		// beginTime
		setBeginTime(currSlot, now)
		currSlot.data = make(map[string]*timeslotData)
	}

	key := buildKey(data.hostID, data.groupTag, data.metricName)
	tsdata, ok := currSlot.data[key]
	if !ok {
		tsdata = new(timeslotData)
		currSlot.data[key] = tsdata
	}

	tsdata.n++
	tsdata.sum += data.metricValue
}

func (tsaggr *TimeslotAggregator) advanceTimeslotIfExpired(now int64) *timeslot {

	currIdx := atomic.LoadInt64(&tsaggr.currIdx)
	currSlot := &tsaggr.timeslots[currIdx]

	if tsaggr.isTimeslotExpired(currSlot, now) {
		currIdx = nextSlotIdx(currIdx)
		atomic.StoreInt64(&tsaggr.currIdx, currIdx)
		currSlot = &tsaggr.timeslots[currIdx]
		setBeginTime(currSlot, 0)
	}

	return currSlot
}

// signal wakes up the processor routine
// to advance the timeslot, if needed. Only
// the processor can do this, because its the
// only writer to the data structure
func (tsaggr *TimeslotAggregator) signal() {
	tsaggr.sigCh <- struct{}{} // signal to advance if needed
	tsaggr.sigCh <- struct{}{} // when this call returns, previous sigaction is complete
}

func (tsaggr *TimeslotAggregator) isTimeslotExpired(ts *timeslot, now int64) bool {
	beginTime := getBeginTime(ts) // can be accessed by Get()
	return (beginTime != 0 && (now-beginTime >= aggregationPeriod))
}

func (tsaggr *TimeslotAggregator) getOldestDataTimeForAggrType(aggrType AggregateType, now int64) int64 {
	switch aggrType {
	case FiveMinAvg:
		return now - int64(time.Minute*5)
	case OneMinAvg:
		return now - int64(time.Minute)
	case OneMinSum:
		return now - int64(time.Minute)
	case FiveMinSum:
		return now - int64(time.Minute*5)
	default:
		tsaggr.logger.Fatalf("Unknown aggregate type %v", aggrType)
	}
	return now
}

func (tsaggr *TimeslotAggregator) minDataPointsForAggrType(aggrType AggregateType) int64 {
	switch aggrType {
	case FiveMinAvg:
		return 5 * 10
	case OneMinAvg:
		return 1 * 10
	default:
		tsaggr.logger.Fatalf("Unknown aggregate type %v", aggrType)
	}
	return 0
}

func (tsaggr *TimeslotAggregator) minSlotsForAggrType(aggrType AggregateType) int {
	switch aggrType {
	case FiveMinAvg:
		return 5
	case FiveMinSum:
		return 1
	case OneMinAvg:
		return 1
	case OneMinSum:
		return 1
	default:
		tsaggr.logger.Fatalf("Unknown aggregate type %v", aggrType)
	}
	return 0
}

func (tsaggr *TimeslotAggregator) maxSlotsForAggrType(aggrType AggregateType) int {
	switch aggrType {
	case FiveMinAvg:
		return 5
	case FiveMinSum:
		return 5
	case OneMinAvg:
		return 1
	case OneMinSum:
		return 1
	default:
		tsaggr.logger.Fatalf("Unknown aggregate type %v", aggrType)
	}
	return 0
}

// DataChannelLength returns the length of the internal
// data channel, this method should only be used
// by unit tests.
func (tsaggr *TimeslotAggregator) DataChannelLength() int {
	return len(tsaggr.dataCh)
}

func getBeginTime(slot *timeslot) int64 {
	return atomic.LoadInt64(&slot.beginTime)
}

func setBeginTime(slot *timeslot, value int64) {
	atomic.StoreInt64(&slot.beginTime, value)
}

func buildKey(hostID string, groupTag string, metricName MetricName) string {
	// concat of hostUUID(first 8-bytes).groupTag.metricName
	if len(groupTag) < 1 {
		groupTag = "*"
	}
	return strings.Join([]string{hostID[:18], groupTag, string(metricName)}, ".")
}

func prevSlotIdx(idx int64) int64 {
	idx--
	return (idx + historySize) % historySize
}

func nextSlotIdx(idx int64) int64 {
	return (idx + 1) % historySize
}
