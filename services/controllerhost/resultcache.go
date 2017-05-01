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

package controllerhost

import (
	"time"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/cache"
)

type (
	// resultCache represents an in-memory
	// cache of input hosts / output hosts
	// correspoding to a destination or
	// consumer group.
	resultCache struct {
		cache.Cache
		context *Context
	}

	resultCacheEntry struct {
		expiry     int64
		nExtents   int
		maxExtents int
		dstType
		hostIDs         []string
		consumeDisabled bool
	}

	resultCacheReadResult struct {
		cachedResult    []string
		consumeDisabled bool
		cacheHit        bool
		refreshCache    bool
		*resultCacheEntry
	}

	resultCacheParams struct {
		dstType         dstType
		nExtents        int
		maxExtents      int
		hostIDs         []string
		consumeDisabled bool
		expiry          int64
	}
)

const (
	resultCacheInitialCap = 1000
	resultCacheMaxSize    = 10000 // If the cache entries are 1KB, this is 100MB
	// we are better off serving a stale entry from the
	// cache when cassandra is down, but lets not do it
	// for more than an hour
	resultCacheTTL time.Duration = time.Hour
)

// newResultCache returns an in-memory LRU cache for
// quick lookup of input hosts / output hosts corresponding
// to a destination / consumer group. The purpose of the
// cache is two fold:
//   a. Avoid too many calls to cassandra
//   b. Work around cassandra's async hinted handoff
//       - For ex, when an extent is created, a
//         subsequent call made to cassandra, say
//         within the next 10ms might not return the
//         new extent (due to async replication).
func newResultCache(context *Context) *resultCache {
	opts := &cache.Options{}
	opts.InitialCapacity = resultCacheInitialCap
	opts.TTL = resultCacheTTL
	return &resultCache{
		context: context,
		Cache:   cache.New(resultCacheMaxSize, opts),
	}
}

// readInputHosts returns the input hosts corresponding to
// the destination, if it exists.
func (cache *resultCache) readInputHosts(dstUUID string, now int64) *resultCacheReadResult {
	return readResultCache(cache, dstUUID, common.InputServiceName, now)
}

// readInputHosts returns the output hosts corresponding to
// the consumer group, if it exists.
func (cache *resultCache) readOutputHosts(cgUUID string, now int64) *resultCacheReadResult {
	return readResultCache(cache, cgUUID, common.OutputServiceName, now)
}

// write puts an entry into the result cache
func (cache *resultCache) write(key string, write resultCacheParams) {
	var cacheEntry = new(resultCacheEntry)
	cacheEntry.dstType = write.dstType
	cacheEntry.expiry = write.expiry
	cacheEntry.hostIDs = write.hostIDs
	cacheEntry.consumeDisabled = write.consumeDisabled
	cacheEntry.nExtents = write.nExtents
	cacheEntry.maxExtents = write.maxExtents
	cache.Put(key, cacheEntry)
}

func readResultCache(cache *resultCache, key string, svc string, now int64) *resultCacheReadResult {
	var refreshCache bool
	var cachedResult = make([]string, 0, 4)
	cacheEntry, cacheHit := cache.Get(key).(*resultCacheEntry)
	if cacheHit {
		hostFailed := false
		for _, id := range cacheEntry.hostIDs {
			addr, e := cache.context.rpm.ResolveUUID(svc, id)
			if e != nil {
				hostFailed = true
				continue
			}
			cachedResult = append(cachedResult, string(addr))
		}
		if hostFailed ||
			now >= cacheEntry.expiry ||
			cacheEntry.nExtents < cacheEntry.maxExtents {
			refreshCache = true
		}

		return &resultCacheReadResult{
			cachedResult:     cachedResult,
			consumeDisabled:  cacheEntry.consumeDisabled,
			cacheHit:         cacheHit,
			refreshCache:     refreshCache,
			resultCacheEntry: cacheEntry,
		}
	}
	return &resultCacheReadResult{
		cachedResult:     cachedResult,
		consumeDisabled:  false,
		cacheHit:         cacheHit,
		refreshCache:     refreshCache,
		resultCacheEntry: &resultCacheEntry{},
	}
}
