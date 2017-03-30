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

package outputhost

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/outputhost/load"
	serverStream "github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

// extentCache is the object which maintains an extent.
// It has 2 functionalities:
//	1. AckManager : which maintains and updates ackLevel offset with the metadata
//	2. Connection Lifecycle: makes sure the connection to the replica is valid and in case of errors
//	   picks a different replica to connect to
type extentCache struct {
	// mutex to protect shutdown and load
	cacheMutex sync.RWMutex

	// extent cache always belong to a consumer group
	// cgUUID the uuid of the consumer group correspoding to this extent
	cgUUID string

	// extUUID is the extent UUID which is managed by this object
	extUUID string

	// destUUID is the destination UUID
	destUUID string

	// destType is the type of the destination
	destType shared.DestinationType

	// storeUUIDs is the list of replicas for this extent
	storeUUIDs []string

	// connectedStoreUUID is the currently connected replica
	connectedStoreUUID string

	// singleCGVisible tells if this is a single CG Visible (i.e. merged DLQ) extent
	singleCGVisible bool

	// pickedIndex is the current replica index for the storeUUIDs slice to which we are connected
	pickedIndex int

	// numExtents is the total number of extents within the CG. this is used to determine the initial credits
	numExtents int

	// startFrom is the offset to start from
	startFrom int64

	// msgsCh is the channel where we write the message to the client as we read from replica
	msgsCh chan<- *cherami.ConsumerMessage

	// connectionsClosedCh is the channel used to notify the consumer group when all the replicas have gone down
	connectionsClosedCh chan<- string

	// notifyReplicaCloseCh is the channel used to get closed streams to replicas
	notifyReplicaCloseCh chan error

	// closeChannel is the local channel used to stop the manage routine
	closeChannel chan struct{}

	// waitConsumedCh is the channel which will signal if the extent is completely consumed
	waitConsumedCh chan bool

	// ackMgr is the ack manager running for this extent. This will be stopped when the
	// cache gets unloaded
	ackMgr *ackManager

	// connection is the current replica connection
	connection *replicaConnection

	// tClients is the factory to get the store clients
	tClients common.ClientFactory

	// wsConnector takes care of establishing connection via websocket stream
	wsConnector common.WSConnector

	// shutdownWG is the wait group protecting shutdown
	shutdownWG *sync.WaitGroup

	// logger is the tag based logger used for logging
	logger bark.Logger

	// creditNotifyCh is the channel to listen for credits. This is driven by the message cache
	creditNotifyCh chan int32

	// creditRequestCh is the channel to request credits from the redelivery cache.
	creditRequestCh chan string

	// initialCredits is the credits that should be given to the stores at the start
	initialCredits int32

	// loadReporter to report metrics to controller
	loadReporter common.LoadReporterDaemon

	// extMetrics represents extent level load metrics reported to controller
	loadMetrics *load.ExtentMetrics

	// consumerM3Client for metrics per consumer group
	consumerM3Client metrics.Client
}

// extentLoadReportingInterval is the freq which load
// metrics are reported to the controller
const extentLoadReportingInterval = 2 * time.Second

func (extCache *extentCache) load(outputHostUUID string, cgUUID string, metaClient metadata.TChanMetadataService, cge *metadata.ConsumerGroupExtent) (err error) {
	// it is ok to take the local lock for this extent which will not affect
	// others
	extCache.cacheMutex.Lock()
	defer extCache.cacheMutex.Unlock()

	// first start the ack manager
	extCache.ackMgr.start()

	// now try to load the replica streams
	extCache.connection, extCache.pickedIndex, err =
		extCache.loadReplicaStream(cge.GetAckLevelOffset(), common.SequenceNumber(cge.GetAckLevelSeqNo()), rand.Intn(len(extCache.storeUUIDs)))
	if err != nil {
		// Exhausted all replica streams.. giving up
		extCache.logger.Error(`unable to load replica stream for extent`)
		// stop the extCache which will do 2 things:
		// 1. stop the ack mgr and
		// 2. notify the cg that this extent is done
		extCache.stop(true)
		// make sure we don't block shutdown
		extCache.shutdownWG.Done()
		return
	}

	extCache.loadReporter.Start()

	go extCache.manageExtentCache()
	return
}

func (extCache *extentCache) loadReplicaStream(startAddress int64, startSequence common.SequenceNumber, startIndex int) (repl *replicaConnection, pickedIndex int, err error) {
	var call serverStream.BStoreOpenReadStreamOutCall
	var cancel context.CancelFunc
	extUUID := extCache.extUUID

	startIndex--

	for count := 0; count < len(extCache.storeUUIDs); count++ {

		startIndex = (startIndex + 1) % len(extCache.storeUUIDs)

		// pick the replica to connect to
		storeUUID := extCache.storeUUIDs[startIndex]
		extCache.connectedStoreUUID = storeUUID

		logger := extCache.logger.WithField(common.TagStor, storeUUID)

		client, hostPort, errGTSCU := extCache.tClients.GetThriftStoreClientUUID(storeUUID, extCache.cgUUID)
		if errGTSCU != nil {
			// save the error code here, so that we error our properly
			// If we manage to pick a storehost successfully in the next round,
			// then we will reset this below when we open stream to replica.
			err = errGTSCU
			logger.WithField(common.TagErr, errGTSCU).Error(`error creating store client`)
			continue
		}

		// First try to start from the already set offset in metadata
		if startAddress == 0 {
			// If consumer group wants to start from a timestamp, get the address from the store
			startFrom := extCache.startFrom

			// TODO: if the consumer group wants to start from the beginning, we should still calculate the earliest address
			//       that they can get. To do otherwise means that will will get spurious 'skipped messages' warnings
			// NOTE: audit will have to handle an uneven 'virtual startFrom' across all of the extents that a zero startFrom
			//       consumer group is reading
			// NOTE: there is a race between consumption and retention here!
			if startFrom > 0 {
				// GetAddressFromTimestamp() from the store using startFrom
				// use a tmp context whose timeout is shorter
				tmpCtx, cancelGAFT := thrift.NewContext(getAddressCtxTimeout)
				defer cancelGAFT()
				getReq := store.GetAddressFromTimestampRequest{
					ExtentUUID: common.StringPtr(extUUID),
					Timestamp:  common.Int64Ptr(startFrom),
				}
				getResp, tmpErr := client.GetAddressFromTimestamp(tmpCtx, &getReq)
				if tmpErr != nil {
					// FIXME: If GetAddressFromTimestamp fails, for now just starting from the beginning.
					// TODO: Retry on a different replica if this fails
					logger.WithField(common.TagErr, tmpErr).Warn(`loadReplicaStream: GetAddressFromTimestamp failed`)
					startAddress = 0
				} else {
					startAddress = getResp.GetAddress()
					// FIXME: T471157 Timer queues don't give an accurate sequence number
					if extCache.destType != shared.DestinationType_TIMER {
						startSequence = common.SequenceNumber(getResp.GetSequenceNumber())
					}
				}
			} else {
				startAddress = 0
				startSequence = 0
			}
		}

		logger.WithFields(bark.Fields{
			`startAddress`:  startAddress,
			`startSequence`: startSequence,
		}).Info(`loadReplicaStream: starting`)

		cDestType, _ := common.CheramiDestinationType(extCache.destType)

		req := &store.OpenReadStreamRequest{
			DestinationUUID:   common.StringPtr(extCache.destUUID),
			DestinationType:   cherami.DestinationTypePtr(cDestType),
			ExtentUUID:        common.StringPtr(extUUID),
			ConsumerGroupUUID: common.StringPtr(extCache.cgUUID),
			Address:           common.Int64Ptr(startAddress),
			Inclusive:         common.BoolPtr(false),
		}
		reqHeaders := common.GetOpenReadStreamRequestHeaders(req)

		host, _, _ := net.SplitHostPort(hostPort)
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
			envVar := common.GetEnvVariableFromHostPort(hostPort)
			port = os.Getenv(envVar)
		}

		httpHeaders := http.Header{}
		for k, v := range reqHeaders {
			httpHeaders.Add(k, v)
		}

		wsHostPort := net.JoinHostPort(host, port)
		logger.WithField(`replica`, wsHostPort).Info(`outputhost: Using websocket to connect to store replica`)
		call, err = extCache.wsConnector.OpenReadStream(wsHostPort, httpHeaders)
		if err != nil {
			logger.WithField(common.TagErr, err).Error(`outputhost: Websocket dial store replica: failed`)
			return
		}
		cancel = nil

		// successfully opened read stream on the replica; save this index
		if startSequence != 0 {
			extCache.ackMgr.addLevelOffset(startSequence) // Let ack manager know that the first message received is not sequence zero
		}

		logger.WithField(`startIndex`, startIndex).Debug(`opened read stream`)
		pickedIndex = startIndex
		replicaConnectionName := fmt.Sprintf(`replicaConnection{Extent: %s, Store: %s}`, extUUID, storeUUID)
		repl = newReplicaConnection(call, extCache, cancel, replicaConnectionName, logger, startSequence)
		// all open connections should be closed before shutdown
		extCache.shutdownWG.Add(1)
		repl.open()
		break
	}

	if err != nil {
		extCache.connectedStoreUUID = ""
	}

	return
}

// stop the extentCache stops the ackMgr and notifies the cgCache that this extent is done
// Notification to the CG happens only when extent is closed after it is consumed.
// If it is being unloaded by the CG, then no need to notify again
func (extCache *extentCache) stop(notify bool) {
	extCache.ackMgr.stop()
	if notify {
		// notify cgCache and stop
		extCache.connectionsClosedCh <- extCache.extUUID
	}
	// stop the load reporter pump as well
	extCache.loadReporter.Stop()
}

func (extCache *extentCache) manageExtentCache() {

	extCache.logger.Info(`initialized`)
	defer extCache.shutdownWG.Done()
	for {
		select {
		case err := <-extCache.notifyReplicaCloseCh:
			// When we see a replica connection being closed, we need to
			// retry on another replica, if the extent is still open.
			// If the extent is sealed, no need to connect to another
			// replica
			extCache.cacheMutex.Lock()
			if _, ok := err.(*store.ExtentSealedError); ok {
				// Extent is sealed.
				// notify the ackMgr and wait for it to be consumed
				extCache.ackMgr.notifySealed()
				extCache.logger.Info(`extent sealed`)
				// reset the err so that we don't unload the extent
				err = nil
			} else {
				// this means a replica stream was closed. try another replica
				extCache.logger.Info(`trying another replica`)
				// first make sure the ackMgr updates its current ack level
				extCache.ackMgr.updateAckLevel()
				// TODO: Fix small race between the offset and seqNo calls
				extCache.connection, extCache.pickedIndex, err =
					extCache.loadReplicaStream(
						extCache.ackMgr.getCurrentAckLevelOffset(),
						extCache.ackMgr.getCurrentAckLevelSeqNo(),
						(extCache.pickedIndex+1)%len(extCache.storeUUIDs))
			}
			extCache.cacheMutex.Unlock()
			if err != nil {
				// Exhausted all replica streams.. giving up
				extCache.logger.WithField(common.TagErr, err).Info(`removing extent (OpenReadStream failed on all replicas)`)
				extCache.stop(true)
				return
			}
		case <-extCache.waitConsumedCh:
			// ackMgr has signalled that this extent is consumed. we can unload this extent now
			extCache.logger.Info(`extent consumed`)
			extCache.stop(true)
			return
		case <-extCache.closeChannel:
			extCache.logger.Info(`closed`)
			extCache.stop(false)
			if extCache.connection != nil {
				// This check is necessary because extCache.connection could be nil as
				// a result of failed loading of any replicas, for example, when store
				// hosts are all unreachable.
				go extCache.connection.close(nil)
			}
			return
		}
	}
}

// requestCredits is used to request credits to the redelivery cache
// we do this in a non-blocking way because it's ok to not satisfy now
// eventually the caller will re-request credits again
func (extCache *extentCache) requestCredits() {
	extCache.cacheMutex.RLock()
	defer extCache.cacheMutex.RUnlock()

	select {
	case extCache.creditRequestCh <- extCache.extUUID:
	default:
		extCache.logger.Warn("requesting credits failed")
	}
}

// grantCredits is to used to grant credits to the replica connection
// this is called when we specifically request credits
func (extCache *extentCache) grantCredits(credits int32) bool {
	extCache.cacheMutex.RLock()
	defer extCache.cacheMutex.RUnlock()

	return extCache.connection.grantCredits(credits)
}

// Report is used for reporting ConsumerGroup specific load to controller
func (extCache *extentCache) Report(reporter common.LoadReporter) {

	msgsOut := extCache.loadMetrics.GetAndReset(load.ExtentMetricMsgsOut)
	bytesOut := extCache.loadMetrics.GetAndReset(load.ExtentMetricBytesOut)

	extMetrics := controller.ConsumerGroupExtentMetrics{
		OutgoingMessagesCounter: common.Int64Ptr(msgsOut),
		OutgoingBytesCounter:    common.Int64Ptr(bytesOut),
	}

	reporter.ReportConsumerGroupExtentMetric(extCache.destUUID, extCache.cgUUID, extCache.extUUID, extMetrics)
}

// unload is called only when we unload the cgCache
func (extCache *extentCache) unload() {
	extCache.cacheMutex.Lock()
	close(extCache.closeChannel)
	extCache.cacheMutex.Unlock()
}

func (extCache *extentCache) getState() *admin.OutputCgExtent {
	cge := admin.NewOutputCgExtent()
	cge.ExtentUUID = common.StringPtr(extCache.extUUID)
	cge.ConnectedStoreUUID = common.StringPtr(extCache.connectedStoreUUID)
	cge.NumCreditsSentToStore = common.Int32Ptr(int32(extCache.connection.sentCreds))
	cge.NumMsgsReadFromStore = common.Int32Ptr(int32(extCache.connection.recvMsgs))
	cge.StartSequence = common.Int64Ptr(int64(extCache.connection.startingSequence))
	cge.AckMgrState = extCache.ackMgr.getAckMgrState()

	return cge
}
