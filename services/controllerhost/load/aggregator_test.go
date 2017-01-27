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
	"math/rand"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
)

type (
	MetricsAggregatorSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}

	aggrTestData struct {
		tagCounts [][]int64 // []int64 counters indexed by tag ids
	}
)

func TestMetricsAggregatorSuite(t *testing.T) {
	suite.Run(t, new(MetricsAggregatorSuite))
}

func (s *MetricsAggregatorSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *MetricsAggregatorSuite) TestTimeSlotAggregation() {

	clock := common.NewMockTimeSource()
	maggr := NewTimeSlotAggregator(clock, (common.GetDefaultLogger()).WithFields(bark.Fields{"testSuite": "MetricsAggregatorSuite"}))
	maggr.Start()
	defer maggr.Stop()

	const nTags = 2
	const nMins = 20
	const nCounters = 3

	var hostID = uuid.New()
	var tags = [nTags]string{uuid.New(), uuid.New()}
	var counterNames = [nCounters]MetricName{NumConns, MsgsInPerSec, MsgsOutPerSec}

	var timeslotData [nMins * 60]*aggrTestData // 20 mins of per second data

	for min := 0; min < nMins; min++ {

		ts := new(aggrTestData)
		ts.tagCounts = make([][]int64, nTags)
		for i := 0; i < nTags; i++ {
			ts.tagCounts[i] = make([]int64, nCounters)
		}

		timeslotData[min] = ts

		for sec := 0; sec < 60; sec++ {
			// Publish per second data for every tag
			for t := 0; t < nTags; t++ {
				// Generate some counter data randomly for this timeslot
				counts := [nCounters]int64{int64(rand.Int31()), int64(rand.Int31()), int64(rand.Int31())}

				for c := 0; c < nCounters; c++ {

					metricData := &metricData{
						hostID:      hostID,
						groupTag:    tags[t],
						metricName:  counterNames[c],
						metricValue: counts[c],
						timestamp:   clock.Now().UnixNano(),
					}

					maggr.add(metricData)
					ts.tagCounts[t][c] += counts[c]
				}
			}

			clock.Advance(time.Second)
		}

		s.validateGet(maggr, timeslotData[:], hostID, tags[:], counterNames[:], min+1)
	}
}

func (s *MetricsAggregatorSuite) validateGet(maggr MetricsAggregator, timeslotData []*aggrTestData, hostID string, tags []string, metricNames []MetricName, min int) {

	for t := range tags {

		for m := range metricNames {

			var expected int64

			// validate 1MinAvg
			result, err := maggr.Get(hostID, tags[t], metricNames[m], OneMinAvg)
			if min < 1 {
				s.Equal(ErrNoData, err, "Get() expected to return error when data is missing")
				continue
			}

			s.Nil(err, "Get(%v, %v, %v) returned error", tags[t], metricNames[m], OneMinAvg)
			expected = timeslotData[min-1].tagCounts[t][m] / int64(60) // should give the avg in the last time slot
			s.Equal(expected, result, "Get(%v, %v, %v) returned wrong result", tags[t], metricNames[m], OneMinAvg)

			result, err = maggr.Get(hostID, tags[t], metricNames[m], OneMinSum)
			if min < 1 {
				s.Equal(ErrNoData, err, "Get() expected to return error when data is missing")
				continue
			}

			expected = timeslotData[min-1].tagCounts[t][m]
			s.Equal(expected, result, "Get(%v, %v, %v) returned wrong result", tags[t], metricNames[m], OneMinSum)

			// validate 5min avg
			result, err = maggr.Get(hostID, tags[t], metricNames[m], FiveMinAvg)
			if min < 5 {
				s.Equal(ErrNoData, err, "Get() expected to return error when data is missing")
				continue
			}

			s.Nil(err, "Get(%v, %v, %v) returned error", tags[t], metricNames[m], FiveMinAvg)

			sum := int64(0)
			for i := 5; i > 0; i-- {
				sum += timeslotData[min-i].tagCounts[t][m]
			}
			expected = sum / (5 * 60) // 5 mins * 60 secs of data points
			s.Equal(expected, result, "Get(%v, %v, %v) returned wrong result", tags[t], metricNames[m], FiveMinAvg)

			result, err = maggr.Get(hostID, tags[t], metricNames[m], FiveMinSum)
			if min < 1 {
				s.Equal(ErrNoData, err, "Get() expected to return error when data is missing")
				continue
			}

			s.Nil(err, "Get(%v, %v, %v) returned error", tags[t], metricNames[m], FiveMinSum)
			s.Equal(sum, result, "Get(%v, %v, %v) returned wrong result", tags[t], metricNames[m], FiveMinAvg)
		}
	}
}

func (s *MetricsAggregatorSuite) TestOldDataGetsDropped() {

	clock := common.NewMockTimeSource()
	maggr := NewTimeSlotAggregator(clock, (common.GetDefaultLogger()).WithFields(bark.Fields{"testSuite": "MetricsAggregatorSuite"}))
	maggr.Start()
	defer maggr.Stop()

	metricData := &metricData{
		hostID:      uuid.New(),
		groupTag:    "foobar",
		metricName:  MsgsInPerSec,
		metricValue: 1,
		timestamp:   clock.Now().UnixNano() - int64(time.Second)*10,
	}

	maggr.add(metricData) // add old data, should be dropped
	metricData.timestamp = clock.Now().UnixNano()
	// now add valid data points
	for i := 0; i < 30; i++ {
		maggr.add(metricData)
	}

	clock.Advance(time.Minute)

	result, err := maggr.Get(metricData.hostID, metricData.groupTag, MsgsInPerSec, OneMinAvg)
	s.Nil(err, "Get() unexpectedly failed")
	s.Equal(int64(1), result, "Wrong onemin-avg returned by aggregator")
}
