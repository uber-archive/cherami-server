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

import "time"
import "sync"

// MockTimeSource is a time source
// for unit tests
type MockTimeSource struct {
	sync.RWMutex
	now time.Time
}

// NewMockTimeSource returns a new instance
// of a controllable time source
func NewMockTimeSource() *MockTimeSource {
	return &MockTimeSource{now: time.Now()}
}

// Now returns the current time
func (ts *MockTimeSource) Now() time.Time {
	ts.RLock()
	defer ts.RUnlock()
	return ts.now
}

// Advance advances the clock by the
// specified duration
func (ts *MockTimeSource) Advance(d time.Duration) {
	ts.Lock()
	defer ts.Unlock()
	ts.now = ts.now.Add(d)
}
