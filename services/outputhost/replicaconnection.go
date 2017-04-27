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
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/uber-common/bark"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/outputhost/load"
	storeStream "github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

const (
	// if we accumulate enough credits, immediately send it to replica
	minCreditBatchSize = 10

	minReadBatchSize = 10

	replicaConnectionCreditsChBuffer = 10

	creditRequestTimeout = 30 * time.Second // this is the timeout for the ticker in case we run out of credits
)

type (
	replicaConnection struct {
		call                storeStream.BStoreOpenReadStreamOutCall
		msgsCh              chan<- *cherami.ConsumerMessage
		initialCredits      int32
		cancel              context.CancelFunc
		closeChannel        chan struct{}
		connectionsClosedCh chan<- error
		readMsgsCh          chan int32
		extCache            *extentCache
		name                string // Name of this replicaConnection for logging/heartbeat
		waitWG              sync.WaitGroup
		shutdownWG          *sync.WaitGroup
		logger              bark.Logger
		creditNotifyCh      <-chan int32 // read only channel to get credits
		localCreditCh       chan int32   // local credit channel which is used to get credits from redelivery cache
		startingSequence    common.SequenceNumber

		lk     sync.RWMutex
		opened bool
		closed bool

		// these are incremented in the read/write pumps that handle
		// one message at a time, and read on close after the pumps
		// are done; therefore, these do not require to be protected
		// for concurrent access
		sentCreds int64 // total credits sent
		recvMsgs  int64 // total messages received

		// consumerM3Client for metrics per consumer group
		consumerM3Client metrics.Client
	}
)

func newReplicaConnection(stream storeStream.BStoreOpenReadStreamOutCall, extCache *extentCache, cancel context.CancelFunc,
	replicaConnectionName string, logger bark.Logger, startingSequence common.SequenceNumber) *replicaConnection {

	conn := &replicaConnection{
		call:                stream,
		msgsCh:              extCache.msgsCh,
		initialCredits:      extCache.initialCredits,
		cancel:              cancel,
		shutdownWG:          extCache.shutdownWG,
		closeChannel:        make(chan struct{}),
		readMsgsCh:          make(chan int32, replicaConnectionCreditsChBuffer),
		connectionsClosedCh: extCache.notifyReplicaCloseCh,
		extCache:            extCache,
		name:                replicaConnectionName,
		creditNotifyCh:      extCache.creditNotifyCh,
		localCreditCh:       make(chan int32, 5),
		logger:              logger.WithFields(bark.Fields{common.TagModule: `replConn`}),
		startingSequence:    startingSequence,
		consumerM3Client:    extCache.consumerM3Client,
	}

	return conn
}

func (conn *replicaConnection) open() {
	conn.lk.Lock()
	defer conn.lk.Unlock()

	if !conn.opened {
		conn.waitWG.Add(2)
		go conn.readMessagesPump()
		go conn.writeCreditsPump()

		conn.opened = true
		conn.logger.Info("replConn opened")
	}
}

func (conn *replicaConnection) close(err error) {
	conn.lk.Lock()
	if !conn.closed {
		conn.closed = true
		close(conn.closeChannel)
		conn.waitWG.Wait()
		conn.extCache.tClients.ReleaseThriftStoreClient(conn.extCache.cgUUID)
		// set the shutdownWG to be done here
		conn.shutdownWG.Done()

		select {
		case conn.connectionsClosedCh <- err:
		default:
		}

		conn.logger.WithFields(bark.Fields{
			`sentCreds`: conn.sentCreds,
			`recvMsgs`:  conn.recvMsgs,
		}).Info("replConn closed")
	}
	conn.lk.Unlock()
}

func (conn *replicaConnection) sendCreditsToStore(credits int32) error {
	cFlow := cherami.NewControlFlow()
	cFlow.Credits = common.Int32Ptr(credits)
	//conn.logger.WithField(`Credits`,cFlow.GetCredits()).Debug(`Sending credits!!`)
	err := conn.call.Write(cFlow)
	if err == nil {
		err = conn.call.Flush()
	}

	conn.sentCreds += int64(credits)

	return err
}

// updateSuccessfulSendToMsgCh updates the local counter and records metric as well
func (conn *replicaConnection) updateSuccessfulSendToMsgsCh(localMsgs *int32, length int64) {
	*localMsgs++
	conn.extCache.loadMetrics.Increment(load.ExtentMetricMsgsOut)
	conn.extCache.loadMetrics.Add(load.ExtentMetricBytesOut, length)
	conn.recvMsgs++
	conn.consumerM3Client.AddCounter(metrics.ConsConnectionScope, metrics.OutputhostCGMessageReceivedBytes, length)
}

// grantCredits is called by the redelivery cache to grant credits just for this
// connection when this one runs out of credits.
// returns true, if we wrote the credits to the credit channel
// returns false otherwise
// Note: uses the connection lk Mutex to make sure we don't race with close
func (conn *replicaConnection) grantCredits(credits int32) bool {
	conn.lk.RLock()
	defer conn.lk.RUnlock()

	ret := false
	if !conn.closed {
		select {
		case conn.localCreditCh <- credits:
			ret = true
		default:
			conn.logger.Warn("replicaConnection: grantCredits: nobody is alive to receive credits")
		}
	}

	return ret
}

// readMessagesPump actually writes the message to the msgsCh which is eventually returned to the client
// As soon as a message is read from the store, it is sent to the consumer group
// messages channel and if it is successfully written without blocking, we will
// accumulate a credit.
// We send the credits to the store when we have accumulated a
// "batch" of credits as determined above
func (conn *replicaConnection) readMessagesPump() {
	defer conn.waitWG.Done()
	var localReadMsgs int32
	hb := common.NewHeartbeat(&conn.name)
	defer hb.CloseHeartbeat()

	// lastSeqNum is used to track whether our sequence numbers are
	// monotonically increasing
	var lastSeqNum = int64(conn.startingSequence)
	// readBatchSize is the number of messages to accumulate before
	// we notify the other pump to ask for credits
	// this should be less than or equal to the creditBatchSize because we
	// need to make sure we are listening for credits on the other pump properly
	readBatchSize := minReadBatchSize

	if readBatchSize < int(conn.initialCredits/10) {
		readBatchSize = int(conn.initialCredits / 10)
	}

	isFirstMsg := true

	for {
		select {
		default:
			if localReadMsgs >= minReadBatchSize {
				// Issue more credits
				select {
				case conn.readMsgsCh <- int32(localReadMsgs):
					localReadMsgs = 0
				default:
					// if we are unable to renew credits at this time accumulate it
					conn.logger.WithField(`credits`, localReadMsgs).
						Debug("readMessagesPump: blocked sending credits; accumulating credits to send later")
				}
			}

			rmc, err := conn.call.Read()
			if err != nil {
				// any error here means our stream is done. close the connection
				conn.logger.WithField(common.TagErr, err).Error(`Error reading msg from store`)
				go conn.close(err)
				<-conn.closeChannel
				return
			}

			hb.Beat()

			switch rmc.GetType() {
			case store.ReadMessageContentType_MESSAGE:

				msg := rmc.GetMessage()
				correctSequenceNumber := common.SequenceNumber(msg.Message.GetSequenceNumber())

				if isFirstMsg {
					isFirstMsg = false
					conn.logger.WithFields(bark.Fields{
						`address`:    msg.GetAddress(),
						`msgSeqNum`:  msg.Message.GetSequenceNumber(),
						`lastSeqNum`: lastSeqNum,
					}).Info("replConn: first message from store to output")
				}

				// XXX: Sequence number check to make sure we get monotonically increasing
				// sequence number.
				// We just log and move forward
				// XXX: Note we skip the first message check here because we can start from
				// a bigger sequence number in case of restarts
				if conn.extCache.destType != shared.DestinationType_TIMER {
					if lastSeqNum+1 != int64(correctSequenceNumber) {
						// FIXME: add metric to help alert this case
						expectedSeqNum := 1 + lastSeqNum
						skippedMessages := int64(correctSequenceNumber) - lastSeqNum

						conn.logger.WithFields(bark.Fields{
							"correctSequenceNumber": correctSequenceNumber,
							"expectedSeqNum":        expectedSeqNum,
							"skippedMessages":       skippedMessages,
						}).Error("sequence number out of order")
					}
				} else {
					// T471157 For timers, do not signal discontinuities to ack manager, since discontinuities are frequent
					correctSequenceNumber = 0
					conn.logger.WithField("expectedSeqNum", lastSeqNum+1).Info("Forcing msg sequence number to zero for timer destination")
				}

				// update the lastSeqNum to this value
				lastSeqNum = msg.Message.GetSequenceNumber()

				// convert this to an outMessage
				cMsg := cherami.NewConsumerMessage()
				cMsg.EnqueueTimeUtc = msg.Message.EnqueueTimeUtc
				cMsg.Payload = msg.Message.Payload

				cMsg.AckId = common.StringPtr(conn.extCache.ackMgr.getNextAckID(msg.GetAddress(), correctSequenceNumber))
				// write the message to the msgsCh so that it can be delivered
				// after being stored on the cache.
				// 1. either there are no listeners
				// 2. the buffer is full
				// Wait until the there are some listeners or we are shutting down
				select {
				case conn.msgsCh <- cMsg:
					// written successfully. Now accumulate credits
					// TODO: one message is now assumed to take one credit
					// we might need to change it to be based on size later.
					// we accumulate a bunch of credits and then send it in a batch to store
					conn.updateSuccessfulSendToMsgsCh(&localReadMsgs, int64(len(cMsg.Payload.GetData())))
				default:
					// we were unable to write it above which probably means the channel is full
					// now do it in a blocking way except shutdown.
					select {
					case conn.msgsCh <- cMsg:
						// written successfully. Now accumulate credits
						conn.updateSuccessfulSendToMsgsCh(&localReadMsgs, int64(len(cMsg.Payload.GetData())))
					// TODO: Make sure we listen on the close channel if and only if, all the
					// consumers are gone as well. (i.e, separate out the close channel).
					case <-conn.closeChannel:
						conn.logger.WithFields(bark.Fields{
							common.TagMsgID: common.FmtMsgID(cMsg.GetAckId()),
							`Offset`:        msg.GetAddress(),
						}).Info("writing msg to the client channel failed because of shutdown.")
						// We need to update the ackMgr here to *not* have this message in the local map
						// since we couldn't even write this message out to the msgsCh.
						// Note: we need to just update this last message because this is a synchronous
						// pump which reads message after message and once we are here we immediately break
						// the pump. So there is absolute guarantee that we cannot call getNextAckID() on this
						// pump parallely.
						conn.extCache.ackMgr.resetMsg(msg.GetAddress())
						return
					}
				}

			case store.ReadMessageContentType_SEALED:

				seal := rmc.GetSealed()
				conn.logger.WithField(common.TagSeq, seal.GetSequenceNumber()).Info(`extent seal`)
				// Notify the extent cache with an extent sealed error so that
				// it can notify the ackMgr and wait for the extent to be consumed
				go conn.close(seal)
				return
			case store.ReadMessageContentType_ERROR:

				msgErr := rmc.GetError()
				conn.logger.WithField(common.TagErr, msgErr.GetMessage()).Error(`received error from storehost`)
				// close the connection
				go conn.close(err)
				return
			default:
				conn.logger.WithField(`Type`, rmc.GetType()).Error(`received ReadMessageContent with unrecognized type`)
			}
		}
	}
}

func (conn *replicaConnection) utilSendCredits(credits int32, numMsgsRead *int32, totalCreditsSent *int32) {
	// conn.logger.WithField(`credits`, credits).Debug(`Sending credits to store.`)
	if err := conn.sendCreditsToStore(credits); err != nil {
		conn.logger.WithField(common.TagErr, err).Error(`error writing credits to store`)

		go conn.close(nil)
	}
	*numMsgsRead = *numMsgsRead - credits
	*totalCreditsSent = *totalCreditsSent + credits
}

func (conn *replicaConnection) writeCreditsPump() {
	defer conn.waitWG.Done()
	defer conn.call.Done()
	if conn.cancel != nil {
		defer conn.cancel()
	}

	totalCreditsSent := conn.initialCredits

	// send initial credits to the store.. so that we can
	// actually start reading
	conn.logger.WithField(`initialCredits`, conn.initialCredits).Info(`writeCreditsPump: sending initial credits to store`)
	if err := conn.sendCreditsToStore(conn.initialCredits); err != nil {
		conn.logger.WithField(common.TagErr, err).Error(`error writing initial credits to store`)

		go conn.close(nil)
		return
	}

	// creditBatchSize is proportionate to the number of initial credits
	// with 10 being the minimum batchsize
	creditBatchSize := (conn.initialCredits / 10)
	if creditBatchSize < minCreditBatchSize {
		creditBatchSize = minCreditBatchSize
	}

	var numMsgsRead int32
	creditRequestTicker := time.NewTicker(creditRequestTimeout)
	defer creditRequestTicker.Stop()

	// Start the write pump
	for {
		// listen for credits only if we satisfy the batch size
		if numMsgsRead > creditBatchSize {
			select {
			case credits := <-conn.creditNotifyCh: // this is the common credit channel from redelivery cache
				conn.utilSendCredits(credits, &numMsgsRead, &totalCreditsSent)
			case credits := <-conn.localCreditCh: // this is the local credits channel only for this connection
				conn.utilSendCredits(credits, &numMsgsRead, &totalCreditsSent)
			case msgsRead := <-conn.readMsgsCh:
				numMsgsRead += msgsRead
			case <-creditRequestTicker.C:
				// if we didn't get any credits for a long time and we are starving for
				// credits, then request some credits
				if numMsgsRead >= totalCreditsSent {
					conn.logger.Warn("WritecreditsPump starving for credits: requesting more")
					conn.extCache.requestCredits()
				}
			case <-conn.closeChannel:
				conn.logger.Info("WriteCreditsPump closing due to connection closed.")
				return
			}
		} else {
			select {
			case msgsRead := <-conn.readMsgsCh:
				numMsgsRead += msgsRead
			// we should listen on the local credits ch, just in case we requested for some credits
			// above and we are just getting it now
			case credits := <-conn.localCreditCh:
				conn.utilSendCredits(credits, &numMsgsRead, &totalCreditsSent)
			case <-conn.closeChannel:
				conn.logger.Info("WriteCreditsPump closing due to connection closed.")
				return
			}
		}
	}
}
