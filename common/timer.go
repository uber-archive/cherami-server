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

package common

import (
	"time"
)

type (
	// Timer is a wrapper for time.Timer which does a
	// safe reset of the timer.
	Timer struct {
		*time.Timer
	}

	// OneShotTimer is an interface around
	// golang Timer to help with unit tests
	OneShotTimer interface {
		// Chan returns the underlying channel
		// for receiving expired timer event
		Chan() <-chan time.Time
		// Reset resets the timer with the
		// new duration
		Reset(d time.Duration) bool
		// Stop stops the timer
		Stop() bool
	}

	// TimerFactory vends OneShotTimers
	TimerFactory interface {
		NewTimer(d time.Duration) OneShotTimer
	}

	timerFactoryImpl struct{}
)

// NewTimer returns a Timer object to be used to
// perform timer functionality
func NewTimer(d time.Duration) *Timer {
	return &Timer{Timer: time.NewTimer(d)}
}

// Reset resets the Timer to expire after duration 'd' reliably
func (t *Timer) Reset(d time.Duration) bool {
	// A simple timer reset is not guaranteed to work all the time.
	// It is because timer.C is buffered and we could have the
	// old timer's expired value in the channel. As a result the next time when
	// we do a reset, we could immediately trigger timer.C.
	// see: https://github.com/golang/go/issues/11513

	// To solve this and to reliably reset the timer, we need to "drain" the channel
	// if it is not already drained.
	// We need to do 2 things here to safely reset and to prevent deadlock.
	// 1. call timer.Stop() which will return false if it has already expired
	// 2. If the timer has expired *and* we have not already read the channel,
	//    we need to drain the channel, so do it in a non-blocking way to avoid
	//    blocking indefinitely.
	ret := t.Timer.Stop()
	if !ret {
		// the timer already expired but nobody has read the channel, read it now
		select {
		case <-t.C:
		default:
		}
	}
	// now reset the timer and set alreadyRead to false
	t.Timer.Reset(d)
	return ret
}

// Stop stops the timer
func (t *Timer) Stop() bool {
	return t.Timer.Stop()
}

// Chan returns the underlying channel
// for receiving timer expired events
func (t *Timer) Chan() <-chan time.Time {
	return t.C
}

// NewTimerFactory creates and returns a new
// factory for OneShotTimers
func NewTimerFactory() TimerFactory {
	return &timerFactoryImpl{}
}

// NewTimer returns a new OneShotTimer
func (factory *timerFactoryImpl) NewTimer(d time.Duration) OneShotTimer {
	return NewTimer(d)
}
