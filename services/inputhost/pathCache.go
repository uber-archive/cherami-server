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
	"sync"
	"time"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/inputhost/load"
	serverStream "github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"

	"github.com/uber-common/bark"
	"golang.org/x/net/context"
)

type (
	// inPathCache holds all the extents for this path
	inPathCache struct {
		sync.RWMutex
		destinationPath       string
		destUUID              string
		destType              shared.DestinationType
		currID                connectionID
		state                 pathCacheState // state of this pathCache
		loadReporterFactory   common.LoadReporterDaemonFactory
		loadReporter          common.LoadReporterDaemon
		reconfigureCh         chan inReconfigInfo
		putMsgCh              chan *inPutMessage
		connections           map[connectionID]*pubConnection
		extentCache           map[extentUUID]*inExtentCache
		inputHost             *InputHost
		closeCh               chan struct{} // this is the channel which is used to unload path cache
		notifyConnsCloseCh    chan connectionID
		notifyExtHostCloseCh  chan string
		notifyExtHostUnloadCh chan string
		logger                bark.Logger
		lastDisconnectTime    time.Time
		// m3Client for mertics per host
		m3Client metrics.Client
		//destM3Client for metrics per destination path
		destM3Client metrics.Client

		// destination level load metrics reported
		// to the controller periodically. int32
		// should suffice for counts, because these
		// metrics get zero'd out every few seconds
		dstMetrics  *load.DstMetrics
		hostMetrics *load.HostMetrics

		// unix nanos when the last time
		// dstMetrics were reported to the
		// controller
		lastDstLoadReportedTime int64

		// connsWG is used to wait for all the connections (including ext) to go away before stopping the manage routine.
		connsWG sync.WaitGroup
	}

	pathCacheState int
)

const (
	pathCacheActive pathCacheState = iota
	pathCacheUnloading
	pathCacheInactive
)

const (
	// metaPollTimeout is the interval to poll metadata
	metaPollTimeout = 1 * time.Minute
	// unloadTicker is the interval to unload the pathCache
	unloadTickerTimeout = 10 * time.Minute
	// idleTimeout is the idle time after the last client got disconnected
	idleTimeout = 15 * time.Minute
)

// isActive is called with the pathCache lock held
// returns true if the pathCache is still active
func (pathCache *inPathCache) isActive() bool {
	return pathCache.state == pathCacheActive
}

func (pathCache *inPathCache) startEventLoop() {
	go pathCache.eventLoop()
}

// isActiveNoLock is called without the lock held
// checks and returns true if the pathCache is indeed active
// after acquiring the lock.
func (pathCache *inPathCache) isActiveNoLock() bool {
	var active bool
	pathCache.RLock()
	active = pathCache.isActive()
	pathCache.RUnlock()

	return active
}

// updateLastDisconnectTime is used to update the last disconnect time for
// this path
func (pathCache *inPathCache) updateLastDisconnectTime() {
	pathCache.Lock()
	defer pathCache.Unlock()
	pathCache.lastDisconnectTime = time.Now()
}

func (pathCache *inPathCache) isIdleTimedOut() bool {
	ans := false
	if pathCache.isActive() && len(pathCache.connections) == 0 &&
		time.Since(pathCache.lastDisconnectTime) > idleTimeout {
		ans = true
	}
	return ans
}

// must be called while holding the pathCache.Lock
func (pathCache *inPathCache) changeState(newState pathCacheState) {
	pathCache.state = newState
}

// eventLoop is the main worker loop for pathCache
func (pathCache *inPathCache) eventLoop() {

	h := pathCache.inputHost

	defer h.shutdownWG.Done()

	refreshTicker := time.NewTicker(metaPollTimeout) // start ticker to refresh metadata
	defer refreshTicker.Stop()

	unloadTicker := time.NewTicker(unloadTickerTimeout) // start ticker to unload pathCache
	defer unloadTicker.Stop()

	for {
		select {
		case conn := <-pathCache.notifyConnsCloseCh:
			pathCache.pubConnectionClosed(conn)
		case extUUID := <-pathCache.notifyExtHostCloseCh:
			pathCache.extCacheClosed(extUUID)
		case extUUID := <-pathCache.notifyExtHostUnloadCh:
			pathCache.extCacheUnloaded(extUUID)
		case reconfigInfo := <-pathCache.reconfigureCh:
			// we need to reload the cache, if the notification type is either
			// HOST or ALL
			reconfigType := reconfigInfo.req.GetType()
			pathCache.RLock()
			extentCacheSize := len(pathCache.extentCache)
			pathCache.RUnlock()

			pathCache.logger.WithFields(bark.Fields{
				common.TagReconfigureID:   common.FmtReconfigureID(reconfigInfo.updateUUID),
				common.TagReconfigureType: common.FmtReconfigureType(reconfigType),
				common.TagExtentCacheSize: extentCacheSize,
			}).Debugf("reconfiguring inputhost")
			switch reconfigType {
			case admin.NotificationType_CLIENT:
				pathCache.reconfigureClients(reconfigInfo.updateUUID)
			case admin.NotificationType_HOST:
				h.updatePathCache(reconfigInfo.req, pathCache.destinationPath)
			case admin.NotificationType_ALL:
				h.updatePathCache(reconfigInfo.req, pathCache.destinationPath)
				pathCache.reconfigureClients(reconfigInfo.updateUUID)
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
			unload := false
			pathCache.RLock()
			unload = pathCache.isIdleTimedOut()
			pathCache.RUnlock()
			if unload {
				pathCache.Lock()
				if pathCache.isIdleTimedOut() {
					pathCache.prepareForUnload()
					go pathCache.unload()
				}
				pathCache.Unlock()
			}
		case <-pathCache.closeCh:
			return
		case <-h.shutdown:
			return
		}
	}
}

// unload unloads the path cache by shutting
// down all the connections / extents and
// go-routines. Must be called after calling
// pathCache.prepareForUnload(). No locks
// must be held.
func (pathCache *inPathCache) unload() {
	// first remove the path from cache
	pathCache.inputHost.removeFromCaches(pathCache)
	// close all connections
	pathCache.Lock()
	if pathCache.state != pathCacheUnloading {
		pathCache.Unlock()
		return
	}

	for _, conn := range pathCache.connections {
		go conn.close()
	}
	for _, extCache := range pathCache.extentCache {
		// call shutdown of the extCache to unload without the timeout
		go extCache.connection.shutdown()
	}
	pathCache.loadReporter.Stop()
	pathCache.state = pathCacheInactive
	pathCache.Unlock()

	// wait for all the above to go away
	pathCache.connsWG.Wait()

	// now close the closeChannel which will stop the manage routine
	close(pathCache.closeCh)
	// since we already stopped the load reporter above and
	// we close the connections asynchronously,
	// make sure the number of connections is explicitly marked as 0
	pathCache.destM3Client.UpdateGauge(metrics.PubConnectionScope, metrics.InputhostDestPubConnection, 0)
	pathCache.logger.Info("pathCache successfully unloaded")
}

// prepareForUnload prepares the pathCache for unload.
// This func must be called with the following lock held:
//      pathCache.Lock()
//
// Sets the state to Unloading to prevent races from load
func (pathCache *inPathCache) prepareForUnload() {
	if !pathCache.isActive() {
		pathCache.logger.Info("pathCache is already not active")
		return
	}
	pathCache.changeState(pathCacheUnloading)
}

// patch cache is closed
func (pathCache *inPathCache) pubConnectionClosed(connID connectionID) {
	pathCache.Lock()
	if _, ok := pathCache.connections[connID]; ok {
		pathCache.logger.WithField(`conn`, connID).Info(`updating path cache to remove the connection with ID`)
		delete(pathCache.connections, connID)
	}
	if len(pathCache.connections) == 0 {
		pathCache.lastDisconnectTime = time.Now()
	}
	pathCache.Unlock()
}

// extCacheClosed is the routine that cleans up
// the state associated with this extent on this
// pathCache after it is closed.
func (pathCache *inPathCache) extCacheClosed(extUUID string) {

	active := true

	pathCache.Lock()
	if _, ok := pathCache.extentCache[extentUUID(extUUID)]; ok {
		pathCache.hostMetrics.Decrement(load.HostMetricNumOpenExtents)
		pathCache.dstMetrics.Decrement(load.DstMetricNumOpenExtents)
		pathCache.logger.
			WithField(common.TagExt, common.FmtExt(string(extUUID))).
			Info("updating path cache to decrement the active extents, since extent is closed")
	}
	active = pathCache.isActive()
	pathCache.Unlock()

	if !active {
		return
	}

	// initiate unloading of pathCache if all extents
	// for this cache are gone
	pathCache.Lock()
	if pathCache.isActive() && pathCache.dstMetrics.Get(load.DstMetricNumOpenExtents) <= 0 {
		pathCache.prepareForUnload()
		go pathCache.unload()
	}
	pathCache.Unlock()
}

// drainConnections is the routine that decides if we need to notify the
// clients to DRAIN the connection
func (pathCache *inPathCache) drainConnections(updateUUID string, connDrainWG *sync.WaitGroup) {
	// initiate sending DRAIN command to the clients to let them drain their pumps and potentially
	// retry on some other extent
	notified := 0
	dropped := 0
	if pathCache.isActive() && pathCache.dstMetrics.Get(load.DstMetricNumWritableExtents) <= 0 {
		for _, conn := range pathCache.connections {
			connDrainWG.Add(1)
			select {
			case conn.reconfigureClientCh <- &reconfigInfo{updateUUID, cherami.InputHostCommandType_DRAIN, connDrainWG}:
				notified++
			default:
				connDrainWG.Done()
				dropped++
			}

		}
	}
	pathCache.logger.WithFields(bark.Fields{
		common.TagUpdateUUID: updateUUID,
		`notified`:           notified,
		`dropped`:            dropped,
	}).Info(`drainConnections: notified clients`)
}

// extCacheUnloaded is the routine that is called to completely
// remove the extent from this pathCache
func (pathCache *inPathCache) extCacheUnloaded(extUUID string) {
	pathCache.Lock()
	if _, ok := pathCache.extentCache[extentUUID(extUUID)]; ok {
		pathCache.logger.
			WithField(common.TagExt, common.FmtExt(string(extUUID))).
			Info("updating path cache to unload extent")
		delete(pathCache.extentCache, extentUUID(extUUID))
	}
	pathCache.Unlock()
}

func (pathCache *inPathCache) reconfigureClients(updateUUID string) {

	var notified, dropped int

	pathCache.RLock()
	for _, conn := range pathCache.connections {
		select {
		case conn.reconfigureClientCh <- &reconfigInfo{updateUUID, cherami.InputHostCommandType_RECONFIGURE, nil}:
			notified++
		default:
			dropped++
		}
	}
	pathCache.RUnlock()

	pathCache.logger.WithFields(bark.Fields{
		common.TagUpdateUUID: updateUUID,
		`notified`:           notified,
		`dropped`:            dropped,
	}).Info(`reconfigureClients: notified clients`)
}

func (pathCache *inPathCache) checkAndLoadReplicaStreams(conn *extHost, extUUID extentUUID, replicas []string /*storehostPort*/) (err error) {

	h := pathCache.inputHost

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

			repl := newReplicaConnection(call, cancel, pathCache.destM3Client,
				conn.logger.
					WithField(common.TagInReplicaHost, common.FmtInReplicaHost(replicas[i])))
			conn.setReplicaInfo(storeHostPort(replicas[i]), repl)
			repl.open()
		}
	}
	return
}

func (pathCache *inPathCache) checkAndLoadExtent(destUUID string, extUUID extentUUID, replicas []string) (err error) {
	pathCache.Lock()
	defer pathCache.Unlock()

	if !pathCache.isActive() {
		return errPathCacheUnloading
	}

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
				pathCache.inputHost.GetClientFactory(),
				&pathCache.connsWG,
				pathCache.inputHost.IsLimitsEnabled()),
		}

		err = pathCache.checkAndLoadReplicaStreams(extCache.connection, extUUID, replicas)
		if err != nil {
			// error loading replica stream
			extCache.connection.logger.Error("error loading replica streams for extent")
			return err
		}

		pathCache.extentCache[extUUID] = extCache
		// all open connections should be closed before shutdown
		pathCache.connsWG.Add(1)
		extCache.connection.open()

		// make sure the number of loaded extents is incremented
		pathCache.dstMetrics.Increment(load.DstMetricNumOpenExtents)
		pathCache.dstMetrics.Increment(load.DstMetricNumWritableExtents)
		pathCache.hostMetrics.Increment(load.HostMetricNumOpenExtents)
	}
	return
}

// Report is used for reporting Destination specific load to controller
func (pathCache *inPathCache) Report(reporter common.LoadReporter) {

	now := time.Now().UnixNano()
	diffSecs := (now - pathCache.lastDstLoadReportedTime) / int64(time.Second)
	if diffSecs < 1 {
		return
	}

	numConnections := pathCache.dstMetrics.Get(load.DstMetricNumOpenConns)
	numExtents := pathCache.dstMetrics.Get(load.DstMetricNumOpenExtents)
	numMsgsInPerSec := pathCache.dstMetrics.GetAndReset(load.DstMetricMsgsIn) / diffSecs
	// We just report the delta for the bytes in counter. so get the value and
	// reset it.
	bytesInSinceLastReport := pathCache.dstMetrics.GetAndReset(load.DstMetricBytesIn)

	metric := controller.DestinationMetrics{
		NumberOfConnections:     common.Int64Ptr(numConnections),
		NumberOfActiveExtents:   common.Int64Ptr(numExtents),
		IncomingMessagesCounter: common.Int64Ptr(numMsgsInPerSec),
		IncomingBytesCounter:    common.Int64Ptr(bytesInSinceLastReport),
	}

	pathCache.lastDstLoadReportedTime = now
	reporter.ReportDestinationMetric(pathCache.destUUID, metric)
	// Also update the metrics reporter to make sure the connection gauge is updated
	pathCache.destM3Client.UpdateGauge(metrics.PubConnectionScope, metrics.InputhostDestPubConnection, numConnections)
}

func (pathCache *inPathCache) getState() *admin.DestinationState {
	pathCache.RLock()
	defer pathCache.RUnlock()
	destState := admin.NewDestinationState()
	destState.DestUUID = common.StringPtr(pathCache.destUUID)
	destState.MsgsChSize = common.Int64Ptr(int64(len(pathCache.putMsgCh)))
	destState.NumMsgsIn = common.Int64Ptr(pathCache.dstMetrics.Get(load.DstMetricOverallNumMsgs))
	destState.NumConnections = common.Int64Ptr(pathCache.dstMetrics.Get(load.DstMetricNumOpenConns))
	destState.NumSentAcks = common.Int64Ptr(pathCache.dstMetrics.Get(load.DstMetricNumAcks))
	destState.NumSentNacks = common.Int64Ptr(pathCache.dstMetrics.Get(load.DstMetricNumNacks))
	destState.NumFailed = common.Int64Ptr(pathCache.dstMetrics.Get(load.DstMetricNumFailed))
	destState.NumThrottled = common.Int64Ptr(pathCache.dstMetrics.Get(load.DstMetricNumThrottled))

	destState.DestExtents = make([]*admin.InputDestExtent, len(pathCache.extentCache))
	count := 0
	// get all extent state now
	for _, extCache := range pathCache.extentCache {
		extState := extCache.connection.getState()
		destState.DestExtents[count] = extState
		count++
	}

	return destState
}

func (pathCache *inPathCache) drainExtent(extUUID string, updateUUID string, drainWG *sync.WaitGroup, drainTimeout time.Duration) {
	defer drainWG.Done()
	pathCache.RLock()

	extCache, ok := pathCache.extentCache[extentUUID(extUUID)]
	if !ok {
		// draining called for extent which is not present
		// just return
		pathCache.RUnlock()
		return
	}

	if extCache.connection.prepForClose() {
		var connDrainWG sync.WaitGroup
		// first send DRAIN command to all connections
		// then start draining extents
		pathCache.drainConnections(updateUUID, &connDrainWG)
		// while drain no need to hold the lock to make sure we
		// can proceed forward
		pathCache.RUnlock()
		// do best effort waiting to notify all connections here
		common.AwaitWaitGroup(&connDrainWG, connWGTimeout)
		extCache.connection.stopWrite(drainTimeout)
	} else {
		// we need to unlock the lock held above
		pathCache.RUnlock()
	}
}

func (pathCache *inPathCache) drain(drainWG *sync.WaitGroup) {
	defer drainWG.Done() // for this routine
	pathCache.RLock()
	defer pathCache.RUnlock()

	// drain all extents
	for extUUID := range pathCache.extentCache {
		drainWG.Add(1) //for all the extents
		go pathCache.drainExtent(string(extUUID), drainAllUUID, drainWG, common.DefaultUpgradeTimeout)
	}
}
