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

package throttler

import (
	"sync/atomic"
	"time"
)

// Throttler imlpements a lockless token-bucket rate limiter, that provides
// a fast/efficient mechanism to throttle requests.

// Throttler holds the context for a throttler instance.
type Throttler struct {
	tokens  uint64 // currently available 'tokens'
	updated int64  // time (in UnixNano) when last refilled

	refill   uint64 // amount to refill tokens by, each time
	interval int64  // duration (in nanoseconds) at which to refill
}

// New instantiates a new Throttler object
// - refillTokens: number of tokens to issue every refillInterval (below)
// - refillInterval: interval at which 'refillTokens' should be issued
func New(refillTokens uint64, refillInterval time.Duration) *Throttler {
	return &Throttler{refill: refillTokens, interval: refillInterval.Nanoseconds()}
}

// Allow checks to see if a particular request should be allowed; ie, it
// would returns true if we are under the throttling threshold, otherwise false.
func (t *Throttler) Allow() bool {
	return t.AllowN(1)
}

// AllowN checks to see if we will be allowed to do a batch of 'n' tokens; it would
// return true if we are under the throttling threshold, otherwise false.
func (t *Throttler) AllowN(n uint64) bool {

	tokens := atomic.LoadUint64(&t.tokens)

getTokens:
	for {
		// check if we have enough tokens available
		for tokens >= n {

			// atomically consume the tokens
			if atomic.CompareAndSwapUint64(&t.tokens, tokens, tokens-n) {
				return true // not throttled
			}

			// retry, since we lost the CAS race
			tokens = atomic.LoadUint64(&t.tokens)
		}

		// we didn't have enough tokens; check if we are due for
		// a refill; if so, refill and try again.

		for tokens < n {

			now := time.Now().UnixNano()

			// check if we are due for a refill
			if (atomic.LoadInt64(&t.updated) + t.interval) > now {

				// check again, in case another thread refilled tokens
				if tokens = atomic.LoadUint64(&t.tokens); tokens < n {
					return false // throttle request
				}

				continue getTokens // retry, since we have enough tokens
			}

			// attempt to refill
			if atomic.CompareAndSwapUint64(&t.tokens, tokens, t.refill) {

				// refilled successfully; remember timestamp and retry
				atomic.StoreInt64(&t.updated, now)
				tokens = t.refill

				continue getTokens // retry, since we have refilled
			}

			// failed cas of 'tokens', reload and retry
			tokens = atomic.LoadUint64(&t.tokens)
		}
	}
}
