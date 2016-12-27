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
	// TickerSourceFactory is the interface mainly used for injecting mock implementations of Ticker for unit testing
	TickerSourceFactory interface {
		CreateTicker(interval time.Duration) TickerSource
	}
	// TickerSource is an interface created over time.Ticker so we can easily unit test any component which uses
	// time.Ticker functionality.
	TickerSource interface {
		Ticks() <-chan time.Time
		Stop()
	}

	realTimeTickerFactory struct{}

	realTimeTicker struct {
		ticker *time.Ticker
	}
)

// NewRealTimeTickerFactory creates and instance of TickerSourceFactory used by service code
func NewRealTimeTickerFactory() TickerSourceFactory {
	return &realTimeTickerFactory{}
}

// CreateTicker is the factory method exposed by TickerSourceFactory to create an instance of TickerSource
func (f *realTimeTickerFactory) CreateTicker(interval time.Duration) TickerSource {
	return &realTimeTicker{
		ticker: time.NewTicker(interval),
	}
}

// Ticks is used within a select to wait on ticker to fire
func (t *realTimeTicker) Ticks() <-chan time.Time {
	return t.ticker.C
}

// Stop is used to stop the ticker instance
func (t *realTimeTicker) Stop() {
	t.ticker.Stop()
}
