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

// NOTE: This is an MRU (most-recently-used) cache, which is the opposite of the more common LRU cache. The reason to use
// an MRU cache, is that we are using this during a scan. If the working set of the scan exceeds the cache size, an LRU
// cache will begin evicting the oldest entries, which will ensure that the next scan has a zero cache hit rate.
//
// MRU will fill up at some point and simply stop accepting entries until it has been manually cleared.

import (
	"fmt"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	lruCache "github.com/uber/cherami-server/common/cache"

	"github.com/uber/cherami-thrift/.generated/go/shared"
)

type extentID string
type storeID string
type perStoreReplicaStatsMap struct {
	singleCGVisibility string // has this DLQ extent been merged or purged? which CG is it visible to?
	m                  map[storeID]*compactExtentStats
}
type replicaStatsMRUCache struct {
	cache lruCache.Cache
	mm    MetadataMgr
	log   bark.Logger
	count int64
}

func newReplicaStatsMRUCache(mm MetadataMgr,
	log bark.Logger, size int) *replicaStatsMRUCache {
	return &replicaStatsMRUCache{
		mm:    mm,
		log:   log,
		cache: lruCache.NewLRUWithInitialCapacity(1, size),
	}
}

func (c *replicaStatsMRUCache) get(extent extentID, store storeID) (val *compactExtentStats) {
	var lclLg bark.Logger
	var err error
	var extStats *shared.ExtentStats
	var val2 *compactExtentStats
	var foundStore, dedupStore storeID
	var intrfc interface{}
	var ok bool
	var psmap *perStoreReplicaStatsMap

	if len(extent) == 0 {
		c.log.
			WithField(common.TagExt, string(extent)). /* Don't call common.Fmt with an empty string ! */
			WithField(common.TagStor, string(store)).
			Warn(`Queue depth skipped empty ID`)
		return nil
	}

	// Check for cache hit
	intrfc = c.cache.Get(string(extent))
	if intrfc == nil {
		// Race between list ConsumerGroupExtents and list DestinationExtents
		c.log.
			WithField(common.TagExt, string(extent)).
			WithField(common.TagStor, string(store)).
			Warn(`extent cache not prepopulated`)
		return nil
	}

	psmap = intrfc.(*perStoreReplicaStatsMap)
	if val, ok = psmap.m[store]; ok { // store == `` will fall through here
		if val != nil {
			goto ttlCheck
		}
	}

	// Get any one store's replica stats; see if it is sealed
oneStore:
	for foundStore, val = range psmap.m {
		if val != nil {
			break oneStore
		}
	}

	// The particular store stats that we return isn't important unless the extent has been sealed
	// In that case, the exact count of messages available depends on which store is being read from,
	// since the seal address will sometimes be different on each replica. Open extents don't have this
	// problem, since they will only temporarily disagree on the message counts until the seal occurs
	if val != nil && val.status == shared.ExtentStatus_OPEN {
		goto ttlCheck
	}

	if len(store) == 0 && val != nil {
		goto ttlCheck // return any found store if none specified, if applicable
	}

	// Cache miss, perform lookup and try to put
	if len(store) == 0 {
		store = foundStore //pick a random store if none specified
	}

	extStats, err = c.mm.ReadStoreExtentStats(string(extent), string(store))
	if err != nil {
		lclLg = c.log.
			WithField(common.TagExt, common.FmtExt(string(extent))).
			WithField(common.TagStor, common.FmtStor(string(store))).
			WithField(common.TagErr, err)

		// Try to provide reasonable stats even if there are temporary failures. The model is 'eventually consistent'
		// so it is better to return the wrong replica's stats than no stats at all, at least on a temporary basis
		if len(foundStore) != 0 {
			lclLg.WithField(`substituteStore`, common.FmtStor(string(foundStore))).
				Error(`Could not get stats for specified store; substituting`)
			return psmap.m[foundStore] // Might be nil
		}

		lclLg.Error(`Could not get stats for specified store`)
		return nil
	} else if extStats != nil {
		val = makeCompactExtentStats(extStats)
		// In the typical case, all store extents had planned sealing, meaning that their available sequences are the same
		// Map the value pointer to the same compactExtentStats in this typical case, to reduce memory usage by nearly 66%
	dedup:
		for dedupStore, val2 = range psmap.m {
			if store != dedupStore && val2 != nil && val2.seqAvailable == val.seqAvailable {
				val = val2
				break dedup
			}
		}

		// psmap is a pointer into the cache, so we can simply modify it here
		val.consumerGroupVisibility = psmap.singleCGVisibility
		psmap.m[store] = val
	}

	return

ttlCheck:

	// Open extents should only be valid for a single scan; All types of sealed extent should have indefinite retention
	if val != nil && val.status == shared.ExtentStatus_OPEN && time.Since(val.cacheTime) > IntervalBtwnScans {
		// All the store exents will have the roughly the same cache time, so just delete all of them,
		// but preserve the singleCGVisibility and the list of storeIDs
		for s, _ := range psmap.m {
			psmap.m[s] = nil
		}

		return c.get(extent, store)
	}

	return
}

func (c *replicaStatsMRUCache) putPlaceholders(extent extentID, singleCGVisibility string, storeIDs []string) {
	if len(extent) == 0 {
		return
	}

	// Check for a cache hit; just add any new storeIDs or change in singleCGVisibility as placeholders if we already have this extent in memory
	intrfc := c.cache.Get(string(extent))
	if intrfc != nil {
		psmap := intrfc.(*perStoreReplicaStatsMap)
		if psmap != nil { // Should always be true
			psmap.singleCGVisibility = singleCGVisibility // This can change as a result of merge/purge operations
			// Populate store IDs; note that re-replication could cause us to have both new and existing cached data here
			for _, id := range storeIDs {
				if val, ok := psmap.m[storeID(id)]; !ok {
					psmap.m[storeID(id)] = nil
				} else if val != nil {
					val.consumerGroupVisibility = singleCGVisibility
				}
			}
			return
		}
	}

	c.cache.Put(string(extent), newPerStoreReplicaStatsMap(singleCGVisibility, &storeIDs))
}

func newPerStoreReplicaStatsMap(singleCGVisibility string, storeIDs *[]string) (psmap *perStoreReplicaStatsMap) {
	psmap = &perStoreReplicaStatsMap{}
	psmap.singleCGVisibility = singleCGVisibility
	psmap.m = make(map[storeID]*compactExtentStats)
	if storeIDs != nil { // Populate empty store placeholders
		for _, s := range *storeIDs {
			psmap.m[storeID(s)] = nil
		}
	}
	return
}

func (c *replicaStatsMRUCache) getStoreIDs(extent extentID) (storeIDs []string) {
	storeIDs = make([]string, 0)

	intrfc := c.cache.Get(string(extent))
	if intrfc != nil {
		psmap := intrfc.(*perStoreReplicaStatsMap)
		for s, _ := range psmap.m {
			storeIDs = append(storeIDs, string(s))
		}
	}
	return
}

type compactExtentStats struct {
	consumerGroupVisibility string
	createTime              common.UnixNanoTime
	status                  shared.ExtentStatus
	seqAvailable            int64
	seqAvailableRate        float64
	seqBegin                int64
	writeTime               common.UnixNanoTime
	cacheTime               time.Time
}

func (c *compactExtentStats) String() string {
	if c == nil {
		return `nil-compactExtentStats`
	}

	return fmt.Sprintf("[[s=%v crtT=%.4g wrtT=%.4g cchT=%.4g avl=%d avlR=%.4g beg=%v]]",
		c.status,
		(common.Now() - c.createTime).ToSeconds(),
		(common.Now() - c.writeTime).ToSeconds(),
		common.UnixNanoTime(time.Since(c.cacheTime)).ToSeconds(),
		c.seqAvailable,
		c.seqAvailableRate,
		c.seqBegin,
	)
}

func makeCompactExtentStats(s *shared.ExtentStats) *compactExtentStats {
	r := s.GetReplicaStats()[0]
	return &compactExtentStats{
		//consumerGroupVisibility: s.GetConsumerGroupVisibility(), // Field is never populated on store extents
		createTime:       common.UnixNanoTime(s.GetCreatedTimeMillis() * 1000 * 1000),
		status:           s.GetStatus(),
		seqAvailable:     r.GetAvailableSequence(),
		seqAvailableRate: r.GetAvailableSequenceRate(),
		seqBegin:         r.GetBeginSequence(),
		writeTime:        common.UnixNanoTime(r.GetWriteTime()),
		cacheTime:        time.Now(),
	}
}
