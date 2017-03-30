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

import "sync"

// UnboundedSemaphore operates similarly to traditional Go channel-based semaphores,
// but without any limitation on the number of resources that can be released
type UnboundedSemaphore struct {
	resCount    int
	once        sync.Once
	c           *sync.Cond
	m           sync.Mutex // Don't use this directly, use through s.c.L
	acquireLock sync.Mutex // Used by acquirer for credit accumulation
}

// Release releases n resources
func (s *UnboundedSemaphore) Release(n int) {
	s.once.Do(s.init)
	s.c.L.Lock()
	s.resCount += n
	s.c.L.Unlock()
	s.c.Broadcast() // Need broadcast, not signal, as we may satisfy many acquisitions
}

// Acquire acquires n resources. If n is negative, there is no effect.
func (s *UnboundedSemaphore) Acquire(n int) {
	s.once.Do(s.init)
	if n <= 0 {
		return
	}

	if n == 1 { // Non-accumulating case
		s.c.L.Lock()
		for s.resCount <= 0 {
			s.c.Wait()
		}
		s.resCount--
		s.c.L.Unlock()
		return
	}

	// Ensure that only one thread is accumulating resources at one time;
	// Prevents acquire(1000)+acquire(1000)+release(500)+release(500) from deadlocking
	// This may not work very well with high contention; it may take a long time to accumulate resources if there are
	// many threads acquiring without accumulation
	s.acquireLock.Lock()
	for n > 0 { // While we are waiting for more resources
		s.c.L.Lock()
		for s.resCount <= 0 { // Wait for resources to be available
			s.c.Wait()
		}
		if n >= s.resCount { // If we have just enough or too few resources, grab them all and maybe keep accumulating
			n -= s.resCount
			s.resCount = 0
		} else { // If we have more resources than required, just subtract what we need and exit
			s.resCount -= n
			n = 0
		}
	}
	s.acquireLock.Unlock()
	s.c.L.Unlock()
}

func (s *UnboundedSemaphore) init() {
	s.c = sync.NewCond(&s.m)
}
