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

package outputhost

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	ThrottlerSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}
)

func TestThrottlerSuite(t *testing.T) {
	suite.Run(t, new(ThrottlerSuite))
}

func (s *ThrottlerSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *ThrottlerSuite) TestBasicThrottle() {
	t := newThrottler()
	minSleep := 1 * time.Millisecond
	maxSleep := 3 * time.Millisecond
	backoffCoefficient := 2.0

	// set all the stuff for the throttler
	t.SetMinThrottleDuration(minSleep)
	t.SetMaxThrottleDuration(maxSleep)
	t.SetBackoffCoefficient(backoffCoefficient)

	currDuration := t.GetCurrentSleepDuration()
	s.Equal(currDuration, defaultNoSleepDuration, "the default duration is not the expected no sleep duration!")

	// Get the next duration
	currDuration = t.GetNextSleepDuration()
	s.Equal(currDuration, minSleep, "first sleep duration is not equal to min sleep time")

	currDuration = t.GetNextSleepDuration()
	s.Equal(currDuration, time.Duration(float64(minSleep)*backoffCoefficient), "next sleep duration is not the expected value")

	currDuration = t.GetNextSleepDuration()
	s.Equal(currDuration, maxSleep, "next sleep duration is not the expected max sleep duration")

	// reset
	currDuration = t.ResetSleepDuration()
	s.Equal(currDuration, defaultNoSleepDuration, "reset didn't work!")
}
