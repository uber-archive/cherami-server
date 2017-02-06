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
	"math/rand"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	lruCache "github.com/uber/cherami-server/common/cache"

	"github.com/uber/cherami-thrift/.generated/go/shared"
)

const (
	maxForcedExpireTime = time.Hour * 24
)

type extentID string
type storeID string
type perStoreReplicaStatsMap struct {
	singleCGVisibility string // has this DLQ extent been merged or purged? which CG is it visible to?
	forceExpireTime    time.Time
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

// get will try to retrieve a compactExtentStats object for a given extentID, (and optional storeID)
// store is optional, because the particular replica (specified by the store) doesn't matter for the
// eventually-consistent open extent case. It does sometimes matter for sealed extents, where certain
// replica stores will have different available sequences for the same extent, when the seal was unplanned
// debugLog allows optional verbose logging
func (c *replicaStatsMRUCache) get(extent extentID, store storeID, debugLog bark.Logger) (val *compactExtentStats) {
	var err error
	var extStats *shared.ExtentStats
	var val2 *compactExtentStats
	var foundStore, dedupStore storeID
	var intrfc interface{}
	var ok bool
	var psmap *perStoreReplicaStatsMap

	// Lazy log init; note that the value of extent/store/err is captured at the time of the call, not here
	logFn := func() bark.Logger {
		l := c.log
		if debugLog != nil {
			l = debugLog
		}
		l = l.WithFields(bark.Fields{common.TagExt: string(extent), common.TagStor: string(store)})
		if err != nil {
			l = l.WithField(common.TagErr, err)
		}
		if val != nil {
			l = l.WithField(`val`, val)
		}
		return l
	}

	if len(extent) == 0 {
		logFn().Warn(`Queue depth skipped empty ID`)
		return nil
	}

	// Check for cache hit
	intrfc = c.cache.Get(string(extent))
	if intrfc == nil {
		// Race between list ConsumerGroupExtents and list DestinationExtents
		logFn().Warn(`extent cache not prepopulated`)
		return nil
	}

	// Note that we never store (*perStoreReplicaStatsMap)(nil) in the cache. Doing so will cause a panic here.
	psmap = intrfc.(*perStoreReplicaStatsMap)
	if psmap == nil { // should never happen
		logFn().Error(`nil *perStoreReplicaStatsMap inserted in cache`)
		return
	}
	if val, ok = psmap.m[store]; ok { // store == `` will fall through here
		if val != nil {
			if debugLog != nil {
				logFn().Debug(`cache hit for specified store`)
			}
			goto ttlCheck
		}
	}

	// Get any one store's replica stats; if the extent is not sealed, then any available replica is eventually consistent
oneStore:
	for foundStore, val = range psmap.m {
		if val != nil {
			break oneStore
		}
	}
	// At this point:
	// * psmap != nil
	// * foundStore != `` (because putPlaceholders doesn't allow empty storeIDs or creation without storeIDs)
	// * val might be nil

	// The particular store stats that we return isn't important unless the extent has been sealed
	// In that case, the exact count of messages available depends on which store is being read from,
	// since the seal address will sometimes be different on each replica. Open extents don't have this
	// problem, since they will only temporarily disagree on the message counts until the seal occurs
	if val != nil && val.status == shared.ExtentStatus_OPEN {
		if debugLog != nil {
			logFn().Debug(`cache hit for any store (open extent)`)
		}
		goto ttlCheck
	}

	if len(store) == 0 && val != nil {
		if debugLog != nil {
			logFn().Debug(`cache hit for any store (no store specified)`)
		}
		goto ttlCheck // return any found store if none specified, if applicable
	}

	// Cache miss, perform lookup and try to put
	if len(store) == 0 {
		store = foundStore //pick a random store if none specified
	}

	extStats, err = c.mm.ReadStoreExtentStats(string(extent), string(store))
	if err != nil {

		// Try to provide reasonable stats even if there are temporary failures. The model is 'eventually consistent'
		// so it is better to return the wrong replica's stats than no stats at all, at least on a temporary basis
		if len(foundStore) != 0 && store != foundStore && psmap.m[foundStore] != nil {
			val = psmap.m[foundStore]
			logFn().WithField(`substituteStore`, common.FmtStor(string(foundStore))).
				Error(`Could not get stats for specified store; substituting`)
			return
		}

		logFn().Error(`Could not get stats for specified store`)
		return nil
	} else if extStats != nil {
		val = makeCompactExtentStats(extStats)
		if debugLog != nil {
			logFn().Debug(`cache miss, lookup success`)
		}

		// In the typical case, our extent had planned sealing, which means that each replica sealed at the same address
		// For this case, there is no reason to store a distinct copy of the extent stats for each replica-store, since
		// the most important value, the available sequence, will be the same between all three replicas. The other values,
		// e.g. begin sequence, are eventually-consistent among all three replicas, so this distinction can be neglected.
		//
		// Map the value pointer to the same compactExtentStats in this typical case, to reduce memory usage by nearly 66%
		//
		// NOTE: this deduplication is all for a single extent, which has multiple replica store extents; we are not
		// deduplicating across extents, which would not be valid
	dedup:
		for dedupStore, val2 = range psmap.m {
			if store != dedupStore && val2 != nil && val2.seqAvailable == val.seqAvailable {
				if debugLog != nil {
					logFn().WithField(`val2`, val2).WithField(`val2Store`, dedupStore).Debug(`cache entry deduplicated`)
				}
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
	// Forced expiry ensures that phantom backlog due to retention changes is limited
	if val != nil &&
		(val.status == shared.ExtentStatus_OPEN && time.Since(val.cacheTime) > IntervalBtwnScans) ||
		time.Now().After(psmap.forceExpireTime) {
		newForceExpireTime := getForceExpireTime()
		if debugLog != nil {
			logFn().WithFields(bark.Fields{ // Negative time-since means the time is in the future
				`sinceCacheTime`:          common.UnixNanoTime(time.Since(val.cacheTime)).ToSeconds(),
				`sinceForceExpireTime`:    common.UnixNanoTime(time.Since(psmap.forceExpireTime)).ToSeconds(),
				`sinceNewForceExpireTime`: common.UnixNanoTime(time.Since(newForceExpireTime)).ToSeconds()}).
				Debug(`cache TTL expired, refreshing`)
		}

		// All the store exents will have the roughly the same cache time, so just delete all of them,
		// but preserve the singleCGVisibility and the list of storeIDs, and update the force expire time
		psmap.forceExpireTime = newForceExpireTime
		for s, _ := range psmap.m {
			psmap.m[s] = nil
		}

		return c.get(extent, store, debugLog)
	}

	return
}

// putPlaceholders is used by processDestination to pre-populate/update some data from the destination extent stats on every pass
// * The single consumer group visibility, which changes even on sealed extents due to DLQ merge/purge operations
// * The list of owning store IDs, which (in the future) will change with re-replication, archival activities, etc.
func (c *replicaStatsMRUCache) putPlaceholders(extent extentID, singleCGVisibility string, storeIDs []string) {

	// These checks allow us to make assumptions about psmaps later. They ensure that:
	// * There will always be at least one non-empty store ID in the map
	// * extentID(``) will never be inserted
	if len(extent) == 0 || len(storeIDs) == 0 {
		return
	}
	for _, s := range storeIDs {
		if s == `` {
			return
		}
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

func getForceExpireTime() time.Time {
	return time.Now().Add(IntervalBtwnScans + time.Duration(float64(maxForcedExpireTime)*rand.Float64()))
}

func newPerStoreReplicaStatsMap(singleCGVisibility string, storeIDs *[]string) (psmap *perStoreReplicaStatsMap) {
	psmap = &perStoreReplicaStatsMap{
		singleCGVisibility: singleCGVisibility,

		// This forces this entry to expire at some random time between now and the maxForcedExpireTime;
		// this ensures that there is some limit to phantom backlog caused by ongoing retention, while
		// also ensuring that both the work and phantom adjustments are evenly spread over some time, preventing spikes
		// NOTE: this time needs to be in the psmap, since having a different time for each replica would make the effective
		// time much shorter (e.g. maxForcedExpireTime = 24 hours, 3 replicas, expected time is 6 hours, not 12 hours)
		forceExpireTime: getForceExpireTime(),
	}
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
