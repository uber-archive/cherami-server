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
	"sync/atomic"
)

// CounterBank represents a set of
// counters that all belong to the
// same group ex - dst or extent counters
// A counterBank supports methods to
// inc/dec/get counter values. Each of
// these methods takes an index, that
// represents the offset of the counter
// within the counter bank. All operations
// supported by counterBank are atomic
type CounterBank struct {
	size     int
	counters []int64
}

// NewCounterBank returns a new instance
// of counterBank containing size number of
// counters.
func NewCounterBank(size int) *CounterBank {
	return &CounterBank{
		size:     size,
		counters: make([]int64, size),
	}
}

// Increment increments the counter at the
// given offset index and returns the new
// value
func (c *CounterBank) Increment(idx int) int64 {
	if idx < c.size {
		return atomic.AddInt64(&c.counters[idx], 1)
	}
	return 0
}

// Decrement decrements the counter at the
// given offset index and returns the new
// value
func (c *CounterBank) Decrement(idx int) int64 {
	if idx < c.size {
		return atomic.AddInt64(&c.counters[idx], -1)
	}
	return 0
}

// Add adds the given value to the counter
// at the given offset.
func (c *CounterBank) Add(idx int, delta int64) int64 {
	if idx < c.size {
		return atomic.AddInt64(&c.counters[idx], delta)
	}
	return 0
}

// Set sets the counter at the given
// offset to the specified value
func (c *CounterBank) Set(idx int, value int64) int64 {
	if idx < c.size {
		atomic.StoreInt64(&c.counters[idx], value)
		return value
	}
	return 0
}

// Get returns the counter value at the
// given offset index
func (c *CounterBank) Get(idx int) int64 {
	if idx < c.size {
		return atomic.LoadInt64(&c.counters[idx])
	}
	return 0
}

// GetAndReset resets the counter value for
// given offset to zero and returns the old
// value atomically
func (c *CounterBank) GetAndReset(idx int) int64 {
	if idx < c.size {
		return atomic.SwapInt64(&c.counters[idx], 0)
	}
	return 0
}
