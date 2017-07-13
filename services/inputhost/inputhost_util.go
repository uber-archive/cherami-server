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

package inputhost

import (
	"sync"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/inputhost/load"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/tchannel-go/thrift"
)

type extentUUID string
type connectionID int
type destinationPath string
type storeHostPort string

const (
	// defaultMetaCtxTimeout is the timeout used for the thrift context for talking to metadata client when
	// we get a reconfigure request
	defaultMetaCtxTimeout = 10 * time.Minute

	// defaultExtCloseNotifyChSize is the buffer size for the notification channel when an extent is closed
	defaultExtCloseNotifyChSize = 50

	// defaultConnsCloseBufSize is the buffer size for the notification channel when a connection is closed
	defaultConnsCloseChSize = 500

	// defaultWGTimeout is the timeout for the waitgroup during shutdown
	defaultWGTimeout = 10 * time.Minute

	// dstLoadReportingInterval is the interval destination load is reported to controller
	dstLoadReportingInterval = 2 * time.Second
)

var errPathCacheUnloading = &cherami.InternalServiceError{Message: "InputHost pathCache is being unloaded"}

func (h *InputHost) checkAndLoadPathCache(destPath string, destUUID string, destType shared.DestinationType, logger bark.Logger, m3Client metrics.Client, hostMetrics *load.HostMetrics) (pathCache *inPathCache, exists bool) {

	h.pathMutex.Lock()
	if pathCache, exists = h.pathCache[destUUID]; !exists {
		pathCache = &inPathCache{
			destinationPath:         destPath,
			destUUID:                destUUID,
			destType:                destType,
			extentCache:             make(map[extentUUID]*inExtentCache),
			loadReporterFactory:     h.GetLoadReporterDaemonFactory(),
			reconfigureCh:           make(chan inReconfigInfo, defaultBufferSize),
			putMsgCh:                make(chan *inPutMessage, defaultBufferSize),
			connections:             make(map[connectionID]*pubConnection),
			closeCh:                 make(chan struct{}),
			notifyExtHostCloseCh:    make(chan string, defaultExtCloseNotifyChSize),
			notifyExtHostUnloadCh:   make(chan string, defaultExtCloseNotifyChSize),
			notifyConnsCloseCh:      make(chan connectionID, defaultConnsCloseChSize),
			logger:                  logger,
			m3Client:                m3Client,
			lastDisconnectTime:      time.Now(),
			dstMetrics:              load.NewDstMetrics(),
			hostMetrics:             hostMetrics,
			lastDstLoadReportedTime: time.Now().UnixNano(),
			inputHost:               h,
		}
		h.pathCache[destUUID] = pathCache
		h.pathCacheByDestPath[destPath] = destUUID
		h.shutdownWG.Add(1)
		pathCache.loadReporter = h.GetLoadReporterDaemonFactory().CreateReporter(dstLoadReportingInterval, pathCache, logger)
		pathCache.loadReporter.Start()
		//  the destM3Client is the destination specific client to report destination specific metrics
		//  the m3Client above is the overall host client to report host-level metrics.
		pathCache.destM3Client = metrics.NewClientWithTags(pathCache.m3Client, metrics.Inputhost, common.GetDestinationTags(destPath, logger))
		pathCache.startEventLoop()
	}
	h.pathMutex.Unlock()
	return
}

// loadPath loads the extent stuff into the respective caches and opens up the replica stream
func (h *InputHost) loadPath(extents []*extentInfo, destPath string, destUUID string, destType shared.DestinationType, m3Client metrics.Client) *inPathCache {
	// First make sure we have the path cached.
	pathCache, exists := h.checkAndLoadPathCache(destPath, destUUID, destType,
		h.logger.WithField(common.TagDstPth, common.FmtDstPth(destPath)).
			WithField(common.TagDst, common.FmtDst(destUUID)), m3Client, h.hostMetrics)

	foundOne := false

	// Now we have the pathCache. check and load all extents
	for _, extent := range extents {
		err := pathCache.checkAndLoadExtent(destUUID, extentUUID(extent.uuid), extent.replicas)
		// if we are able to successfully load *atleast* one extent, then we are good
		if err == nil {
			foundOne = true
		}
	}

	// if we didn't load any extent and we just loaded the pathCache, unload it
	if !foundOne && !exists && pathCache != nil {
		pathCache.logger.Error("unable to load any extent for the given destination")
		pathCache.Lock()
		pathCache.prepareForUnload()
		pathCache.Unlock()
		go pathCache.unload()
		pathCache = nil
	}

	return pathCache
}

func (h *InputHost) updatePathCache(destinationsUpdated *admin.DestinationUpdatedNotification, destPath string) {
	ctx, cancel := thrift.NewContext(defaultMetaCtxTimeout)
	defer cancel()

	destUUID := destinationsUpdated.GetDestinationUUID()
	extentUUID := destinationsUpdated.GetExtentUUID()
	info := h.createExtentInfo(extentUUID, destinationsUpdated.GetStoreIds())
	if info == nil {
		// DestinationUpdatedNotification does not have enough data to create ExtentInfo
		// Let's try to load ExtentInfo from metadata and load any missing extents
		_, err := h.getExtentsAndLoadPathCache(ctx, destPath, destUUID, shared.DestinationType_UNKNOWN)
		if err != nil {
			h.logger.WithField(common.TagErr, err).Error(`unable to reload the path cache`)
		}
	} else {
		h.loadPath([]*extentInfo{info}, "", destUUID, shared.DestinationType_UNKNOWN, h.m3Client)
	}
}

// getPathCache returns the pathCache, given the path
func (h *InputHost) getPathCacheByDestPath(path string) (retPathCache *inPathCache, ok bool) {
	h.pathMutex.RLock()
	// first get the path
	destUUID, found := h.pathCacheByDestPath[path]
	if found {
		retPathCache, ok = h.pathCache[destUUID]
	}
	h.pathMutex.RUnlock()

	return retPathCache, ok
}

// getPathCacheByUUID returns the pathCache, given the destUUID
func (h *InputHost) getPathCacheByDestUUID(destUUID string) (retPathCache *inPathCache, ok bool) {
	h.pathMutex.RLock()
	retPathCache, ok = h.pathCache[destUUID]
	h.pathMutex.RUnlock()

	return retPathCache, ok
}

// removeFromCache removes this pathCache from both the
// pathCache map.
// This method only removes the entry, if the existing entry
// is the same as the passed reference
func (h *InputHost) removeFromCaches(pathCache *inPathCache) {
	h.pathMutex.Lock()
	if curr, ok := h.pathCache[pathCache.destUUID]; ok && curr == pathCache {
		delete(h.pathCache, pathCache.destUUID)
		delete(h.pathCacheByDestPath, pathCache.destinationPath)
	}
	h.pathMutex.Unlock()
}

// unloadAll tries to unload everything
func (h *InputHost) unloadAll() {
	h.pathMutex.Lock()
	for _, pathCache := range h.pathCache {
		pathCache.logger.Info("inputhost: closing streams on path")
		pathCache.Lock()
		pathCache.prepareForUnload()
		pathCache.Unlock()
		go pathCache.unload()
	}
	h.pathMutex.Unlock()
}

func (h *InputHost) drainAll() {
	h.pathMutex.RLock()
	defer h.pathMutex.RUnlock()
	var drainWG sync.WaitGroup
	for _, pathCache := range h.pathCache {
		drainWG.Add(1)
		go pathCache.drain(&drainWG)
	}

	if ok := common.AwaitWaitGroup(&drainWG, common.DefaultUpgradeTimeout); !ok {
		h.logger.Warn("inputhost: drain all timed out")
	}
}

// updateExtTokenBucket update the token bucket for the extents msgs limit rate per second
func (h *InputHost) updateExtTokenBucket(connLimit int32) {
	h.pathMutex.RLock()
	for _, inPath := range h.pathCache {
		inPath.RLock()
		for _, extCache := range inPath.extentCache {
			extCache.connection.SetMsgsLimitPerSecond(connLimit)
		}
		inPath.RUnlock()
	}
	h.pathMutex.RUnlock()
	h.logger.WithField("UpdateExtTokenBucket", connLimit).
		Info("update extent TB for new connLimit")
}

// updateConnTokenBucket update the token bucket for the extents msgs limit rate per second
func (h *InputHost) updateConnTokenBucket(connLimit int32) {
	h.pathMutex.RLock()
	for _, inPath := range h.pathCache {
		inPath.RLock()
		for _, conn := range inPath.connections {
			conn.SetMsgsLimitPerSecond(connLimit)
		}
		inPath.RUnlock()
	}
	h.pathMutex.RUnlock()
	h.logger.WithField("UpdateConnTokenBucket", connLimit).
		Info("update connection TB for new connLimit")
}
