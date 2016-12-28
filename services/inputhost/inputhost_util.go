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
	"net"
	"net/http"
	"os"
	"time"

	"golang.org/x/net/context"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"

	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/inputhost/load"
	serverStream "github.com/uber/cherami-server/stream"
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

	// defaultWGTimeout is the timeout for the waitgroup during shutdown
	defaultWGTimeout = 10 * time.Minute

	// metaPollTimeout is the interval to poll metadata
	metaPollTimeout = 1 * time.Minute

	// unloadTicker is the interval to unload the pathCache
	unloadTickerTimeout = 10 * time.Minute

	// idleTimeout is the idle time after the last client got disconnected
	idleTimeout = 15 * time.Minute

	// dstLoadReportingInterval is the interval destination load is reported to controller
	dstLoadReportingInterval = 2 * time.Second
)

func (h *InputHost) getDestinationTags(destPath string) map[string]string {
	destTagValue, tagErr := common.GetTagsFromPath(destPath)
	if tagErr != nil {
		destTagValue = metrics.UnknownDirectoryTagValue
		h.logger.WithField(common.TagDstPth, destPath).
			WithField(common.TagUnknowPth, metrics.UnknownDirectoryTagValue).
			Error("unknow destination path, return default name")
	}
	tags := map[string]string{
		metrics.DestinationTagName: destTagValue,
	}
	return tags
}

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
			notifyCloseCh:           make(chan connectionID),
			notifyExtHostCloseCh:    make(chan string, defaultExtCloseNotifyChSize),
			notifyExtHostUnloadCh:   make(chan string, defaultExtCloseNotifyChSize),
			connections:             make(map[connectionID]*pubConnection),
			closeCh:                 make(chan struct{}),
			logger:                  logger,
			m3Client:                m3Client,
			lastDisconnectTime:      time.Now(),
			dstMetrics:              load.NewDstMetrics(),
			hostMetrics:             hostMetrics,
			lastDstLoadReportedTime: time.Now().UnixNano(),
		}
		h.pathCache[destUUID] = pathCache
		h.pathCacheByDestPath[destPath] = destUUID
		h.shutdownWG.Add(1)
		pathCache.loadReporter = h.GetLoadReporterDaemonFactory().CreateReporter(dstLoadReportingInterval, pathCache, logger)
		pathCache.loadReporter.Start()
		//  the destM3Client is the destination specific client to report destination specific metrics
		//  the m3Client above is the overall host client to report host-level metrics.
		pathCache.destM3Client = metrics.NewClientWithTags(pathCache.m3Client, metrics.Inputhost, h.getDestinationTags(destPath))
		go h.managePath(pathCache)
	}
	h.pathMutex.Unlock()
	return
}

func (h *InputHost) checkAndLoadExtentCache(pathCache *inPathCache, destUUID string, extUUID extentUUID, replicas []string) (err error) {
	pathCache.extMutex.Lock()
	defer pathCache.extMutex.Unlock()
	if extCache, exists := pathCache.extentCache[extUUID]; !exists {
		extCache = &inExtentCache{
			extUUID: extUUID,
			connection: newExtConnection(
				destUUID,
				pathCache,
				string(extUUID),
				len(replicas),
				pathCache.loadReporterFactory,
				pathCache.logger.WithField(common.TagExt, common.FmtExt(string(extUUID))),
				h.GetClientFactory(),
				&h.shutdownWG,
				h.IsLimitsEnabled()),
		}

		err = h.checkAndLoadReplicaStreams(extCache.connection, extUUID, replicas)
		if err != nil {
			// error loading replica stream
			extCache.connection.logger.Error("error loading replica streams for extent")
			return err
		}

		pathCache.extentCache[extUUID] = extCache
		// all open connections should be closed before shutdown
		h.shutdownWG.Add(1)
		extCache.connection.open()

		// make sure the number of loaded extents is incremented
		pathCache.dstMetrics.Increment(load.DstMetricNumOpenExtents)
		pathCache.hostMetrics.Increment(load.HostMetricNumOpenExtents)
	}
	return
}

func (h *InputHost) checkAndLoadReplicaStreams(conn *extHost, extUUID extentUUID, replicas []string /*storehostPort*/) (err error) {
	conn.lk.Lock()
	defer conn.lk.Unlock()
	var call serverStream.BStoreOpenAppendStreamOutCall
	var cancel context.CancelFunc
	var ok bool
	for i := 0; i < len(replicas); i++ {
		if _, ok = conn.streams[storeHostPort(replicas[i])]; !ok {

			cDestType, _ := common.CheramiDestinationType(conn.destType)

			req := &store.OpenAppendStreamRequest{
				DestinationUUID: common.StringPtr(string(conn.destUUID)),
				DestinationType: cherami.DestinationTypePtr(cDestType),
				ExtentUUID:      common.StringPtr(string(extUUID)),
			}
			reqHeaders := common.GetOpenAppendStreamRequestHeaders(req)

			host, _, _ := net.SplitHostPort(replicas[i])
			port := os.Getenv("CHERAMI_STOREHOST_WS_PORT")
			if len(port) == 0 {
				port = "6191"
			} else if port == "test" {
				// XXX: this is a hack to get the wsPort specific to this hostport.
				// this is needed specifically for benchmark tests and other tests which
				// try to start multiple replicas on the same local machine.
				// this is a temporary workaround until we have ringpop labels
				// if we have the label feature we can set the websocket port corresponding
				// to a replica as a metadata rather than the env variables
				envVar := common.GetEnvVariableFromHostPort(replicas[i])
				port = os.Getenv(envVar)
			}

			httpHeaders := http.Header{}
			for k, v := range reqHeaders {
				httpHeaders.Add(k, v)
			}

			hostPort := net.JoinHostPort(host, port)
			conn.logger.WithField(`replica`, hostPort).Info(`inputhost: Using websocket to connect to store replica`)
			call, err = h.GetWSConnector().OpenAppendStream(hostPort, httpHeaders)
			if err != nil {
				conn.logger.WithFields(bark.Fields{`replicas[i]`: hostPort, common.TagErr: err}).Error(`inputhost: Websocket dial store replica: failed`)
				return
			}
			cancel = nil

			repl := newReplicaConnection(call, cancel,
				conn.logger.
					WithField(common.TagInReplicaHost, common.FmtInReplicaHost(replicas[i])))
			conn.setReplicaInfo(storeHostPort(replicas[i]), repl)
			repl.open()
		}
	}
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
		err := h.checkAndLoadExtentCache(pathCache, destUUID, extentUUID(extent.uuid), extent.replicas)
		// if we are able to successfully load *atleast* one extent, then we are good
		if err == nil {
			foundOne = true
		}
	}

	// if we didn't load any extent and we just loaded the pathCache, unload it
	if !foundOne && !exists && pathCache != nil {
		pathCache.logger.Error("unable to load any extent for the given destination")
		h.pathMutex.Lock()
		h.unloadPathCache(pathCache)
		h.pathMutex.Unlock()
		pathCache = nil
	}

	return pathCache
}

func (h *InputHost) reconfigureClients(pathCache *inPathCache, updateUUID string) {

	var notified, dropped int

	pathCache.extMutex.Lock()
	for _, conn := range pathCache.connections {
		select {
		case conn.reconfigureClientCh <- updateUUID:
			notified++
		default:
			dropped++
		}
	}
	pathCache.extMutex.Unlock()

	pathCache.logger.WithFields(bark.Fields{
		common.TagUpdateUUID: updateUUID,
		`notified`:           notified,
		`dropped`:            dropped,
	}).Info(`reconfigureClients: notified clients`)
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

func (h *InputHost) managePath(pathCache *inPathCache) {
	defer h.shutdownWG.Done()

	refreshTicker := time.NewTicker(metaPollTimeout) // start ticker to refresh metadata
	defer refreshTicker.Stop()

	unloadTicker := time.NewTicker(unloadTickerTimeout) // start ticker to unload pathCache
	defer unloadTicker.Stop()

	for {
		select {
		case conn := <-pathCache.notifyCloseCh:
			h.pathMutex.Lock()
			pathCache.extMutex.Lock()
			if _, ok := pathCache.connections[conn]; ok {
				pathCache.logger.WithField(`conn`, conn).Info(`updating path cache to remove the connection with ID`)
				delete(pathCache.connections, conn)
			}
			if len(pathCache.connections) <= 0 {
				pathCache.lastDisconnectTime = time.Now()
			}
			pathCache.extMutex.Unlock()
			h.pathMutex.Unlock()
		case extUUID := <-pathCache.notifyExtHostCloseCh:
			// if all the extents for this path went down, no point in keeping the connection open
			h.pathMutex.Lock()
			pathCache.extMutex.Lock()
			if _, ok := pathCache.extentCache[extentUUID(extUUID)]; ok {
				pathCache.logger.
					WithField(common.TagExt, common.FmtExt(string(extUUID))).
					Info("updating path cache to decrement the active extents, since extent is closed")
				// decrement the number of open extents and if we don't even have 1 open extent disconnect clients
				pathCache.hostMetrics.Decrement(load.HostMetricNumOpenExtents)
				if pathCache.dstMetrics.Decrement(load.DstMetricNumOpenExtents) <= 0 {
					// Note: Make sure we don't race with a load here.
					// It is safe to unload the pathCache completely here,
					// since there are no open extents.
					// Incase the client reconfigures, it will load a fresh
					// path cache and will continue as usual.
					// Remove it from the map first so that a load doesn't interfere
					// with the unload.
					delete(h.pathCache, pathCache.destUUID)
					pathCache.logger.Info("unloading empty pathCache")
					go h.unloadSpecificPath(pathCache)
				}
			}
			pathCache.extMutex.Unlock()
			h.pathMutex.Unlock()
		case extUUID := <-pathCache.notifyExtHostUnloadCh:
			// now we can safely unload the extent completely
			h.pathMutex.Lock()
			pathCache.extMutex.Lock()
			if _, ok := pathCache.extentCache[extentUUID(extUUID)]; ok {
				pathCache.logger.
					WithField(common.TagExt, common.FmtExt(string(extUUID))).
					Info("updating path cache to unload extent")
				delete(pathCache.extentCache, extentUUID(extUUID))
			}
			pathCache.extMutex.Unlock()
			h.pathMutex.Unlock()
		case reconfigInfo := <-pathCache.reconfigureCh:
			// we need to reload the cache, if the notification type is either
			// HOST or ALL
			reconfigType := reconfigInfo.req.GetType()
			h.pathMutex.RLock()
			pathCache.extMutex.RLock()
			extentCacheSize := len(pathCache.extentCache)
			pathCache.extMutex.RUnlock()
			h.pathMutex.RUnlock()
			pathCache.logger.WithFields(bark.Fields{
				common.TagReconfigureID:   common.FmtReconfigureID(reconfigInfo.updateUUID),
				common.TagReconfigureType: common.FmtReconfigureType(reconfigType),
				common.TagExtentCacheSize: extentCacheSize,
			}).Debugf("reconfiguring inputhost")
			switch reconfigType {
			case admin.NotificationType_CLIENT:
				h.reconfigureClients(pathCache, reconfigInfo.updateUUID)
			case admin.NotificationType_HOST:
				h.updatePathCache(reconfigInfo.req, pathCache.destinationPath)
			case admin.NotificationType_ALL:
				h.updatePathCache(reconfigInfo.req, pathCache.destinationPath)
				h.reconfigureClients(pathCache, reconfigInfo.updateUUID)
			default:
				pathCache.logger.
					WithField(common.TagReconfigureID, common.FmtReconfigureID(reconfigInfo.updateUUID)).
					WithField(common.TagReconfigureType, common.FmtReconfigureType(reconfigType)).
					Error("Invalid reconfigure type")
			}
			pathCache.logger.
				WithField(common.TagReconfigureID, common.FmtReconfigureID(reconfigInfo.updateUUID)).
				WithField(common.TagReconfigureType, common.FmtReconfigureType(reconfigType)).
				Debug("finished reconfiguration of inputhost")
		case <-refreshTicker.C:
			pathCache.logger.Debug("refreshing all extents")
			h.getExtentsAndLoadPathCache(nil, "", pathCache.destUUID, shared.DestinationType_UNKNOWN)

		case <-unloadTicker.C:
			h.pathMutex.Lock()
			pathCache.extMutex.Lock()
			if len(pathCache.connections) <= 0 && time.Since(pathCache.lastDisconnectTime) > idleTimeout {
				pathCache.logger.Info("unloading idle pathCache")
				// Note: remove from the map so that a load doesn't race with unload
				h.removeFromCaches(pathCache)
				go h.unloadSpecificPath(pathCache)
			}
			pathCache.extMutex.Unlock()
			h.pathMutex.Unlock()
		case <-pathCache.closeCh:
			return
		case <-h.shutdown:
			return
		}
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

// unloadPathCache is the routine to unload this destUUID from the
// inputhost's pathCache.
// It should be called with the pathMutex held and it will stop
// all connections and extents from this pathCache
func (h *InputHost) unloadPathCache(pathCache *inPathCache) {
	pathCache.unloadInProgress = true
	close(pathCache.closeCh)
	// close all connections
	pathCache.extMutex.Lock()
	for _, conn := range pathCache.connections {
		go conn.close()
	}
	for _, extCache := range pathCache.extentCache {
		extCache.cacheMutex.Lock()
		// call shutdown of the extCache to unload without the timeout
		go extCache.connection.shutdown()
		extCache.cacheMutex.Unlock()
	}
	pathCache.loadReporter.Stop()
	pathCache.extMutex.Unlock()

	// since we already stopped the load reporter above and
	// we close the connections asynchronously,
	// make sure the number of connections is explicitly marked as 0
	pathCache.destM3Client.UpdateGauge(metrics.PubConnectionScope, metrics.InputhostDestPubConnection, 0)
	h.removeFromCaches(pathCache)
	pathCache.logger.Info("pathCache successfully unloaded")
}

// removeFromCache removes this pathCache from both the caches
func (h *InputHost) removeFromCaches(pathCache *inPathCache) {
	delete(h.pathCache, pathCache.destUUID)
	delete(h.pathCacheByDestPath, pathCache.destinationPath)
}

// unloadSpecificPath unloads the specific path cache
func (h *InputHost) unloadSpecificPath(pathCache *inPathCache) {
	h.pathMutex.Lock()
	// unload only if we are not already in the process of unloading
	if !pathCache.unloadInProgress {
		h.unloadPathCache(pathCache)
	}
	h.pathMutex.Unlock()
}

// unloadAll tries to unload everything
func (h *InputHost) unloadAll() {
	h.pathMutex.Lock()
	for _, pathCache := range h.pathCache {
		pathCache.logger.Info("inputhost: closing streams on path")
		if !pathCache.unloadInProgress {
			h.unloadPathCache(pathCache)
		}
	}
	h.pathMutex.Unlock()
}

// updateExtTokenBucket update the token bucket for the extents msgs limit rate per second
func (h *InputHost) updateExtTokenBucket(connLimit int32) {
	h.pathMutex.RLock()
	for _, inPath := range h.pathCache {
		// TODO: pathMutex or extMutex which one is better here?
		inPath.extMutex.RLock()
		for _, extCache := range inPath.extentCache {
			extCache.connection.SetMsgsLimitPerSecond(connLimit)
		}
		inPath.extMutex.RUnlock()
	}
	h.pathMutex.RUnlock()
	h.logger.Infof("The size of pathCache in extent is %v", len(h.pathCache))
	h.logger.WithField("UpdateExtTokenBucket", connLimit).
		Info("update extent TB for new connLimit")
}

// updateConnTokenBucket update the token bucket for the extents msgs limit rate per second
func (h *InputHost) updateConnTokenBucket(connLimit int32) {
	h.pathMutex.RLock()
	for _, inPath := range h.pathCache {
		for _, conn := range inPath.connections {
			conn.SetMsgsLimitPerSecond(connLimit)
		}
	}
	h.pathMutex.RUnlock()
	h.logger.Infof("The size of pathCache in conn is %v", len(h.pathCache))
	h.logger.WithField("UpdateConnTokenBucket", connLimit).
		Info("update connection TB for new connLimit")
}

// Report is used for reporting Destination specific load to controller
func (p *inPathCache) Report(reporter common.LoadReporter) {

	now := time.Now().UnixNano()
	diffSecs := (now - p.lastDstLoadReportedTime) / int64(time.Second)
	if diffSecs < 1 {
		return
	}

	numConnections := p.dstMetrics.Get(load.DstMetricNumOpenConns)
	numExtents := p.dstMetrics.Get(load.DstMetricNumOpenExtents)
	numMsgsInPerSec := p.dstMetrics.GetAndReset(load.DstMetricMsgsIn) / diffSecs

	metric := controller.DestinationMetrics{
		NumberOfConnections:     common.Int64Ptr(numConnections),
		NumberOfActiveExtents:   common.Int64Ptr(numExtents),
		IncomingMessagesCounter: common.Int64Ptr(numMsgsInPerSec),
	}

	p.lastDstLoadReportedTime = now
	reporter.ReportDestinationMetric(p.destUUID, metric)
	// Also update the metrics reporter to make sure the connection gauge is updated
	p.destM3Client.UpdateGauge(metrics.PubConnectionScope, metrics.InputhostDestPubConnection, numConnections)
}

// updateLastDisconnectTime is used to update the last disconnect time for
// this path
func (p *inPathCache) updateLastDisconnectTime() {
	p.extMutex.Lock()
	defer p.extMutex.Unlock()

	p.lastDisconnectTime = time.Now()
}
