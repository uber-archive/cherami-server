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
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

// Replica stats MRU cache types
const replicaStatsMRUCacheSizeLimit = 10000

type extentID string
type storeID string
type perStoreReplicaStatsMap struct {
	singleCGVisibility string
	m                  map[storeID]*shared.ExtentStats
}
type extentCoverMap map[extentID]bool
type replicaStatsMRUCache struct {
	cache map[extentID]*perStoreReplicaStatsMap
	mm    MetadataMgr
	log   bark.Logger
	count int64
}

func newReplicaStatsMRUCache(mm MetadataMgr,
	log bark.Logger) *replicaStatsMRUCache {
	return &replicaStatsMRUCache{
		mm:  mm,
		log: log,
	}
}

func (c *replicaStatsMRUCache) clear() {
	c.cache = make(map[extentID]*perStoreReplicaStatsMap)
	c.count = 0
}

func (c *replicaStatsMRUCache) get(extent extentID, store storeID) (val *shared.ExtentStats) {
	var ok bool
	var foundStore storeID
	var psmap *perStoreReplicaStatsMap
	var err error

	if len(extent) == 0 {
		c.log.
			WithField(common.TagExt, string(extent)). /* Don't call common.Fmt with an empty string ! */
			WithField(common.TagStor, string(store)).
			Warn(`Queue depth skipped empty ID`)
		return nil
	}

	// Check for cache hit
	if psmap, ok = c.cache[extent]; ok {
		if val, ok = psmap.m[store]; ok { // store == `` will fall through here
			if val != nil {
				return
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
		// since the seal address will likely be different on each replica. Open extents don't have this
		// problem, since they will only temporarily disagree on the message counts until the seal occurs
		if val != nil && val.GetStatus() != shared.ExtentStatus_SEALED {
			return
		}

		if len(store) == 0 && val != nil {
			return // return any found store if none specified, if applicable
		}
	}

	// Cache miss, perform lookup and try to put

	if len(store) == 0 {
		store = foundStore //pick a random store if none specified
	}

	if len(store) == 0 {
		return // This happens when a new extent comes online and there are various race conditions in Cassandra
	}

	val, err = c.mm.ReadStoreExtentStats(string(extent), string(store))
	if err != nil {
		lclLg := c.log.
			WithField(common.TagExt, common.FmtExt(string(extent))).
			WithField(common.TagStor, common.FmtStor(string(store))).
			WithField(common.TagErr, err)

		// Try to provide reasonable stats even if there are temporary failures. The model is 'eventually consistent'
		// so it is better to return the wrong replica's stats than no stats at all, at least on a temporary basis
		if len(foundStore) != 0 {
			lclLg.WithField(`substituteStore`, common.FmtStor(string(foundStore))).
				Error(`Could not get stats for specified store; substituting`)
			return c.get(extent, foundStore)
		}

		lclLg.Error(`Could not get stats for specified store`)
		return nil
	} else if val != nil {
		c.put(extent, store, val)
	}

	return
}

func (c *replicaStatsMRUCache) putSingleCGVisibility(extent extentID, singleCGVisibility string) {
	if len(extent) == 0 || len(singleCGVisibility) == 0 {
		return
	}

	c.cache[extent] = newPerStoreReplicaStatsMap(&singleCGVisibility, nil, nil)
}

func (c *replicaStatsMRUCache) put(extent extentID, store storeID, val *shared.ExtentStats) {
	var ok bool
	var psmap *perStoreReplicaStatsMap

	if len(extent) == 0 || len(store) == 0 {
		c.log.
			WithField(common.TagExt, string(extent)). /* Don't call common.Fmt with an empty string ! */
			WithField(common.TagStor, string(store)).
			Warn(`Queue depth skipped empty ID`)
		return
	}

	// This is the MRU aspect. We simply don't cache results that would exceed our limit
	if val != nil && c.count >= replicaStatsMRUCacheSizeLimit {
		return
	}

	if psmap, ok = c.cache[extent]; ok {

		// Push the single CG visibility into the store extent. It is only present on destination extents, but is needed for queue depth
		if len(psmap.singleCGVisibility) > 0 && val != nil {
			val.ConsumerGroupVisibility = common.StringPtr(psmap.singleCGVisibility)
		}

		// If this is not an update/replacement, increase the count
		if _, ok = psmap.m[store]; !ok {
			if val != nil {
				c.count++
			}
		}

		psmap.m[store] = val
	} else {
		c.cache[extent] = newPerStoreReplicaStatsMap(nil, &store, val)
		if val != nil {
			c.count++
		}
	}
}

func (c *replicaStatsMRUCache) getExtentCoverMap() extentCoverMap {
	m := make(extentCoverMap)
	for store := range c.cache {
		m[store] = true
	}
	return m
}

func newPerStoreReplicaStatsMap(singleCGVisibility *string, store *storeID, val *shared.ExtentStats) (psmap *perStoreReplicaStatsMap) {
	psmap = &perStoreReplicaStatsMap{}
	psmap.m = make(map[storeID]*shared.ExtentStats)
	if singleCGVisibility != nil && *singleCGVisibility != `` {
		psmap.singleCGVisibility = *singleCGVisibility
	}
	if store != nil {
		psmap.m[*store] = val
	}
	return
}
