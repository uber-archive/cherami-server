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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type UnboundedSemaphoreSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
}

func TestUnboundedSemaphoreSuite(t *testing.T) {
	suite.Run(t, new(UnboundedSemaphoreSuite))
}

func (s *UnboundedSemaphoreSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

const longTime = time.Second
const shortTime = time.Millisecond * 10

func (s *UnboundedSemaphoreSuite) TestNoRelease() {
	var u UnboundedSemaphore

	u.Release(10)
	u.Acquire(5)
	u.Acquire(5)

	closeCh := make(chan struct{}, 0)

	go func() {
		u.Acquire(1)
		close(closeCh)
	}()

	select {
	case <-time.After(shortTime):
	case <-closeCh:
		s.Fail(`Acquire should block with insufficient resources`)
	}

	u.Release(1)
	<-closeCh
}

func (s *UnboundedSemaphoreSuite) TestNegativeRelease() {
	var u UnboundedSemaphore

	u.Release(-10)

	closeCh := make(chan struct{}, 0)

	go func() {
		u.Acquire(1)
		close(closeCh)
	}()

	select {
	case <-time.After(shortTime):
	case <-closeCh:
		s.Fail(`Acquire should block with insufficient resources`)
	}

	u.Release(1)
	select {
	case <-time.After(shortTime):
		s.Fail(`Acquire should have succeeded`)
	case <-closeCh:
	}
}

func (s *UnboundedSemaphoreSuite) TestNegativeAcquire() {
	var u UnboundedSemaphore
	var closeCh chan struct{}

	acquireAndClose := func() {
		u.Acquire(1)
		close(closeCh)
	}

	u.Release(1)
	u.Acquire(-10000)

	closeCh = make(chan struct{}, 0)
	go acquireAndClose()
	select {
	case <-time.After(shortTime):
		s.Fail(`Acquire should have succeeded`)
	case <-closeCh:
	}

	closeCh = make(chan struct{}, 0)
	go acquireAndClose()
	select {
	case <-time.After(shortTime):
	case <-closeCh:
		s.Fail(`Acquire should have failed`)
	}

	u.Release(1)
	<-closeCh
}

func (s *UnboundedSemaphoreSuite) TestAccumulatorWins() {
	contentionScale := 100
	var u UnboundedSemaphore

	closeCh := make(chan struct{}, 0)

	// Make a bunch of concurrent non-accumulating contenders
	for i := 0; i < contentionScale; i++ {
		go func() {
			for {
				select {
				case <-closeCh:
					return
				default:
					u.Acquire(1)
					u.Release(1)
				}
			}
		}()
	}

	// Also concurrently, try to acquire everything
	go func() {
		u.Acquire(contentionScale)
		close(closeCh)
		u.Release(contentionScale)
	}()

	// Slowly release the full resources
	for i := 0; i < contentionScale; i++ {
		u.Release(1)
	}

	select {
	case <-time.After(longTime):
		s.Fail(`Accumulative acquire should win in reasonable time`)
		close(closeCh)
		u.Release(contentionScale * 10)
	case <-closeCh:
	}
}

func (s *UnboundedSemaphoreSuite) TestManyAccumulatorsWin() {
	contentionScale := 100
	var u UnboundedSemaphore
	var wg sync.WaitGroup

	closeCh := make(chan struct{}, 0)

	// one-shot contender
	catchAndRelease := func(i int) {
		u.Acquire(i)
		u.Release(i)
		wg.Done()
		return
	}

	// Make a bunch of concurrent accumulating and non-accumulating contenders
	for i := 0; i < contentionScale; i++ {
		wg.Add(2)
		go catchAndRelease(contentionScale)
		go catchAndRelease(1)
	}

	// Close the closeCh when all contenders have finished
	go func() {
		wg.Wait()
		close(closeCh)
	}()

	// Slowly release the full resources
	for i := 0; i < contentionScale; i++ {
		u.Release(1)
	}

	select {
	case <-time.After(longTime):
		s.Fail(`Accumulative acquirers should win in reasonable time`)
		close(closeCh)
		u.Release(contentionScale * contentionScale)
	case <-closeCh:
	}
}

func (s *UnboundedSemaphoreSuite) TestOneShotAccumulativeAcquires() {
	var u UnboundedSemaphore
	var closeCh chan struct{}

	acquireAndClose := func(i int, c chan struct{}) {
		u.Acquire(i)
		close(c)
	}

	u.Release(1000)
	u.Acquire(700) // Both of these Acquires are fully satisfied in one-pass
	u.Acquire(300)

	closeCh = make(chan struct{}, 0)
	go acquireAndClose(1000, closeCh)
	select {
	case <-time.After(shortTime):
	case <-closeCh:
		s.Fail(`Acquire should have failed`)
	}

	closeCh = make(chan struct{}, 0)
	go acquireAndClose(1, closeCh)
	select {
	case <-time.After(shortTime):
	case <-closeCh:
		s.Fail(`Acquire should have failed`)
	}

	closeCh = make(chan struct{}, 0)
	go acquireAndClose(1000, closeCh)
	u.Release(2001)
	select {
	case <-time.After(shortTime):
		s.Fail(`Acquire should have succeeded`)
	case <-closeCh:
	}

	u.Release(100000)
	<-closeCh
}

func (s *UnboundedSemaphoreSuite) TestConcurrentReleases() {
	contentionScale := 1000
	total := 0
	var u UnboundedSemaphore
	var wg sync.WaitGroup
	closeCh := make(chan struct{}, 0)

	acquireAndClose := func(i int, c chan struct{}) {
		u.Acquire(i)
		close(c)
	}

	// one-shot synchronized releaser
	release := func(i int) {
		wg.Wait()
		u.Release(i)
		return
	}

	wg.Add(1)

	// Make a bunch of concurrent releasers
	for i := 0; i < contentionScale; i++ {
		total += i
		go release(i)
	}

	// Try to acquire the full amount in one shot
	go acquireAndClose(total, closeCh)

	// Bang!
	wg.Done()

	select {
	case <-time.After(longTime):
		s.Fail(`Synchornized releasers should release an acquirer in reasonable time`)
		close(closeCh)
		u.Release(contentionScale * contentionScale)
	case <-closeCh:
	}

	closeCh = make(chan struct{}, 0)
	go acquireAndClose(1, closeCh)
	select {
	case <-time.After(shortTime):
	case <-closeCh:
		s.Fail(`Acquire should have failed`)
	}
}
