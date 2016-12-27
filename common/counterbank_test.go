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

type CounterBankSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
}

func TestCounterBankSuite(t *testing.T) {
	suite.Run(t, new(CounterBankSuite))
}

func (s *CounterBankSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *CounterBankSuite) TestIncrement() {

	var nIters = 100

	adder := func(bank *CounterBank, startC chan struct{}, stopWG *sync.WaitGroup) {
		defer stopWG.Done()
		<-startC // wait until asked to start
		for i := 0; i < nIters; i++ {
			for idx := 0; idx < bank.size; idx++ {
				bank.Increment(idx)
			}
		}
	}

	var nAdders = 5
	var ch = make(chan struct{})
	var wg = sync.WaitGroup{}
	var bank = NewCounterBank(5)

	for i := 0; i < nAdders; i++ {
		wg.Add(1)
		go adder(bank, ch, &wg)
	}

	var expectedSum = int64(nAdders * nIters)
	var sums = make([]int64, bank.size)

	close(ch) // kick off the adders

	for {
		for i := 0; i < bank.size; i++ {
			sums[i] += bank.GetAndReset(i)
		}
		if AwaitWaitGroup(&wg, 10*time.Millisecond) {
			break
		}
	}

	for i := 0; i < bank.size; i++ {
		sums[i] += bank.GetAndReset(i)
		s.Equal(expectedSum, sums[i], "Wrong value after concurrent increment")
	}
}

func (s *CounterBankSuite) TestIncDec() {

	var nIters = 100

	fuzzer := func(bank *CounterBank, startC chan struct{}, stopWG *sync.WaitGroup) {
		defer stopWG.Done()
		<-startC
		for i := 0; i < nIters; i++ {
			for idx := 0; idx < bank.size; idx++ {
				bank.Add(idx, 1)
				bank.Decrement(idx)
				bank.Increment(idx)
			}
		}
	}

	var nFuzzers = 5
	var ch = make(chan struct{})
	var wg = sync.WaitGroup{}
	var bank = NewCounterBank(5)

	for i := 0; i < nFuzzers; i++ {
		wg.Add(1)
		go fuzzer(bank, ch, &wg)
	}

	var expectedSum = int64(nFuzzers * nIters)

	close(ch) // kick off the fuzzers
	wg.Wait()
	for i := 0; i < bank.size; i++ {
		s.Equal(expectedSum, bank.Get(i), "Wrong value after concurrent inc/dec")
		// make sure another read returns the same value (i.e. not reset)
		s.Equal(expectedSum, bank.Get(i), "Wrong value after concurrent inc/dec")
	}
}
