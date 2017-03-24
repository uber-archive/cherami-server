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
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/shared"

	"math"
	"sync"
)

type (
	// storeExtentMetadata contains metadata that store
	// maintains for an extent that it owns. This is used
	// in extent backlog calculation
	storeExtentMetadata struct {
		storeID               string
		beginSequence         int64
		lastSequence          int64
		availableSequence     int64
		availableSequenceRate float64
		lastSequenceRate      float64
		beginTime             int64
		endTime               int64
		beginEnqueueTimeUtc   int64
		lastEnqueueTimeUtc    int64
		writeTime             common.UnixNanoTime
		createdTime           common.UnixNanoTime
	}

	// storeExtentMetadataCache is a demand filled cache
	// for metadata corresponding to a bunch of (store, extent)
	// tuple.
	storeExtentMetadataCache struct {
		logger     bark.Logger
		datasource MetadataMgr                         // the data source from where to demand fill
		entries    map[extentID][]*storeExtentMetadata // extentID -> {store1Metadata, store2Metadta... }
	}
)

// newStoreExtentMetadataCache returns a new instance of storeExtentMetadataCache
func newStoreExtentMetadataCache(datasource MetadataMgr, logger bark.Logger) *storeExtentMetadataCache {
	return &storeExtentMetadataCache{
		logger:     logger,
		datasource: datasource,
		entries:    make(map[extentID][]*storeExtentMetadata, 16),
	}
}

// get returns the metadata corresponding to the given (store, extent) tuple
// On a cache miss, a request will be made to data source to fetch the data
// this method will only return nil if the data source also returns nil
func (cache *storeExtentMetadataCache) get(store storeID, extID extentID) *storeExtentMetadata {

	var entry []*storeExtentMetadata
	entry = cache.entries[extID]
	result := findStoreMetadata(entry, string(store))
	if result != nil {
		return result
	}
	// cache miss, lets fetch the data from source
	result = cache.fetchStoreExtentMetadata(string(extID), string(store))
	if result != nil {
		entry = append(entry, result)
		cache.entries[extID] = entry
	}
	return result
}

func (cache *storeExtentMetadataCache) fetchStoreExtentMetadata(extID string, store string) *storeExtentMetadata {
	stats, err := cache.datasource.ReadStoreExtentStats(extID, store)
	if err != nil {
		cache.logger.WithField(common.TagExt, common.FmtExt(extID)).
			WithField(common.TagStor, common.FmtStor(store)).
			WithField(common.TagErr, err).Error("ReadStoreExtentStats error")
		return nil
	}

	rs := stats.GetReplicaStats()[0]

	// fix illegal values, if needed
	cache.fixReplicaStatsIfBroken(rs)

	return &storeExtentMetadata{
		storeID:               store,
		beginSequence:         rs.GetBeginSequence(),
		lastSequence:          rs.GetLastSequence(),
		lastSequenceRate:      rs.GetLastSequenceRate(),
		availableSequence:     rs.GetAvailableSequence(),
		availableSequenceRate: rs.GetAvailableSequenceRate(),
		beginTime:             rs.GetBeginTime(),
		endTime:               rs.GetEndTime(),
		beginEnqueueTimeUtc:   rs.GetBeginEnqueueTimeUtc(),
		lastEnqueueTimeUtc:    rs.GetLastEnqueueTimeUtc(),
		writeTime:             common.UnixNanoTime(rs.GetWriteTime()),
		createdTime:           common.UnixNanoTime(stats.GetCreatedTimeMillis() * 1000 * 1000),
	}
}

// T471438 -- Fix meta-values leaked by storehost
// T520701 -- Fix massive int64 negative values
func (cache *storeExtentMetadataCache) fixReplicaStatsIfBroken(rs *shared.ExtentReplicaStats) {
	var fixed bool
	for _, val := range []*int64{rs.AvailableSequence, rs.BeginSequence, rs.LastSequence} {
		if val != nil && (*val >= storageMetaValuesLimit || *val < 0) {
			*val = math.MaxInt64
			fixed = true
		}
	}
	if fixed {
		cache.logger.WithFields(bark.Fields{
			common.TagStor: rs.GetStoreUUID(),
		}).Info(`Queue Depth Temporarily Fixed Replica-Stats`)
	}
}

func findStoreMetadata(list []*storeExtentMetadata, store string) *storeExtentMetadata {
	for _, elem := range list {
		if elem.storeID == store {
			return elem
		}
	}
	return nil
}

type (
	// QueueDepthCache caches the queueDepth results
	// for every consumer-group based on the previous
	// run on the queueDepth calculator.
	QueueDepthCache struct {
		sync.RWMutex
		entries  map[string]QueueDepthCacheEntry
		capacity int
	}

	// QueueDepthCacheEntry is a cache structure for testing queue depth
	QueueDepthCacheEntry struct {
		// Time is the cache entry time
		Time common.UnixNanoTime
		// BacklogAvailable is the available backlog
		BacklogAvailable int64
		// BacklogUnavailable is the unavailable backlog (only useful for timer queues)
		BacklogUnavailable int64
		// BacklogInflight is the in flight message count
		BacklogInflight int64
		// BacklogDLQ is the number of messages in DLQ
		BacklogDLQ int64
	}

	// QueueDepthCacheJSONFields is the json fields for QueueDepthCacheEntry
	QueueDepthCacheJSONFields struct {
		CacheTime        common.UnixNanoTime `json:"cache_time,omitempty"`
		BacklogAvailable int64               `json:"backlog_available"`
		BacklogInflight  int64               `json:"backlog_inflight"`
		BacklogDLQ       int64               `json:"backlog_dlq"`
	}
)

const queueDepthCacheLimit = 1 << 19

func newQueueDepthCache() *QueueDepthCache {
	return &QueueDepthCache{
		capacity: queueDepthCacheLimit,
		entries:  make(map[string]QueueDepthCacheEntry),
	}
}

func (cache *QueueDepthCache) get(key string) (QueueDepthCacheEntry, bool) {
	cache.RLock()
	defer cache.RUnlock()
	if entry, ok := cache.entries[key]; ok {
		return entry, ok
	}
	return QueueDepthCacheEntry{}, false
}

func (cache *QueueDepthCache) put(key string, value *QueueDepthCacheEntry) {
	cache.Lock()
	defer cache.Unlock()
	if len(cache.entries) > cache.capacity { // Random eviction if the cache has grown too large;
		for k := range cache.entries {
			delete(cache.entries, k)
			break
		}
	}
	cache.entries[key] = *value
}
