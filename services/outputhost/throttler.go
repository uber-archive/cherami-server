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
	"time"
)

const (
	// Note that since defaultMaxThrottleSleepDuration/defaultMinThrottleSleepDuration = 60,000,
	// and log(60000)/log(1.1) ≈ 116, so it will take 116 rounds to reach the maximum throttle
	// Reaching maximum throttle will take at least 11½ minutes (http://www.wolframalpha.com/input/?i=sigma(0.001*1.1%5En,+0,+116)

	defaultMinThrottleSleepDuration = 1 * time.Millisecond // defaultMinThrottleSleepDuration is the minimum duration to sleep when we get a throttled message
	defaultMaxThrottleSleepDuration = 1 * time.Minute      // defaultMaxThrottleSleepDuration is the max duration to sleep when we get a throttled message
	defaultBackoffCoefficient       = 1.1                  // defaultBackoffCoefficient to compute the next sleep duration. By default, we increase the sleep duration by 10% more every time
	defaultNoSleepDuration          = 0 * time.Second
)

type (
	// Throttler is the interface is to get the duration to sleep based on a simple backoff formulae
	// sleepDuration = time.Duration(currentSleepDuration * backoffCoefficient)
	// Note: this is lock free and is *not* thread safe. This is intentional because the user is expected
	// to be a single goroutine. If we need this to be thread safe we need to add some synchronization.
	Throttler interface {
		// SetBackoffCoefficient is the routine to set the backoff coefficient for the throttler
		SetBackoffCoefficient(backoffCoefficient float64)

		// SetMaxThrottleDuration is used to set the max duration to return for the throttler
		SetMaxThrottleDuration(max time.Duration)

		// SetMinThrottleDuration is used to set the min duration to return for the throttler
		SetMinThrottleDuration(min time.Duration)

		// GetCurrentSleepDuration returns the current duration for this throttler
		GetCurrentSleepDuration() time.Duration

		// GetNextSleepDuration returns the sleep duration based on the backoff coefficient
		GetNextSleepDuration() time.Duration

		// ResetSleepDuration resets the current sleep duration for this throttler
		ResetSleepDuration() time.Duration

		// IsCurrentSleepDurationAtMaximum tells whether the current sleep duration is maxed out
		IsCurrentSleepDurationAtMaximum() bool
	}

	throttlerImpl struct {
		minThrottleDuration     time.Duration
		maxThrottleDuration     time.Duration
		backoffCoefficient      float64
		currentThrottleDuration time.Duration
	}
)

// newThrottler returns a Throttler object
func newThrottler() Throttler {
	return &throttlerImpl{
		minThrottleDuration:     defaultMinThrottleSleepDuration,
		maxThrottleDuration:     defaultMaxThrottleSleepDuration,
		backoffCoefficient:      defaultBackoffCoefficient,
		currentThrottleDuration: defaultNoSleepDuration,
	}
}

// GetNextSleepDuration returns the next sleep duration
func (t *throttlerImpl) GetNextSleepDuration() time.Duration {
	// just calculate the sleep time.
	// the sleep time slowly ramps up between (minThrottleDuration,
	// maxThrottleDuration) by backoffCoefficient
	slowDown := t.currentThrottleDuration

	// if this is the first, time just set it to min duration
	if slowDown <= defaultNoSleepDuration {
		slowDown = t.minThrottleDuration
	} else {
		// use the coefficient to calculate
		slowDown = time.Duration(float64(slowDown) * t.backoffCoefficient)
	}

	// if the duration is greater than the max duration, reset!
	if slowDown > t.maxThrottleDuration {
		slowDown = t.maxThrottleDuration
	}

	// update the current value
	t.currentThrottleDuration = slowDown

	return t.currentThrottleDuration
}

// SetBackoffCoefficient is to set the backoff coefficient for calculating the sleep duration
func (t *throttlerImpl) SetBackoffCoefficient(backoffCoefficient float64) {
	t.backoffCoefficient = backoffCoefficient
}

// SetMaxThrottleDuration is used to set the max duration
func (t *throttlerImpl) SetMaxThrottleDuration(max time.Duration) {
	t.maxThrottleDuration = max
}

// SetMinThrottleDuration is used to set the min duration
func (t *throttlerImpl) SetMinThrottleDuration(min time.Duration) {
	t.minThrottleDuration = min
}

// GetCurrentSleepDuration returns the current duration
func (t *throttlerImpl) GetCurrentSleepDuration() time.Duration {
	return t.currentThrottleDuration
}

// ResetSleepDuration is used to reset the sleep time for this throttler
func (t *throttlerImpl) ResetSleepDuration() time.Duration {
	t.currentThrottleDuration = defaultNoSleepDuration

	return t.currentThrottleDuration
}

// IsCurrentSleepDurationAtMaximum returns whether the current duration is at the maximum throttle level
func (t *throttlerImpl) IsCurrentSleepDurationAtMaximum() bool {
	return t.currentThrottleDuration >= t.maxThrottleDuration
}
