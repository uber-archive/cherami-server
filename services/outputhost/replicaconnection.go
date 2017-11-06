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
		sentCreds     int32 // total credits sent
		recvMsgs      int32 // total messages received
		skipOlderMsgs int32 // messages skipped

		// consumerM3Client for metrics per consumer group
		consumerM3Client metrics.Client
	}
)

func newReplicaConnection(stream storeStream.BStoreOpenReadStreamOutCall, extCache *extentCache,
	replicaConnectionName string, logger bark.Logger, startingSequence common.SequenceNumber) *replicaConnection {

	conn := &replicaConnection{
		call:                stream,
		msgsCh:              extCache.msgsCh,
		initialCredits:      extCache.initialCredits,
		shutdownWG:          extCache.shutdownWG,
		closeChannel:        make(chan struct{}),
		readMsgsCh:          make(chan int32, replicaConnectionCreditsChBuffer),
		connectionsClosedCh: extCache.notifyReplicaCloseCh,
		extCache:            extCache,
		name:                replicaConnectionName,
		creditNotifyCh:      extCache.creditNotifyCh,
		localCreditCh:       make(chan int32, 5),
		logger:              logger.WithField(common.TagModule, `replConn`),
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
		conn.logger.WithFields(bark.Fields{
			`delay`:            conn.extCache.delay,
			`skipOlder`:        conn.extCache.skipOlder,
			`startingSequence`: conn.startingSequence,
		}).Info("replConn opened")
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
			`sentCreds`:     conn.sentCreds,
			`recvMsgs`:      conn.recvMsgs,
			`skipOlderMsgs`: conn.skipOlderMsgs,
		}).Info("replConn closed")
	}
	conn.lk.Unlock()
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

	var skipOlderNanos = int64(conn.extCache.skipOlder)
	var delayNanos = int64(conn.extCache.delay)
	var delayTimer = common.NewTimer(time.Hour)
loop:
	for {
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
			msgSeqNum := common.SequenceNumber(msg.Message.GetSequenceNumber())
			msgAddr := msg.GetAddress()
			visibilityTime := msg.Message.GetEnqueueTimeUtc()

			// XXX: Sequence number check to make sure we get monotonically increasing
			// sequence number.
			// We just log and move forward
			// XXX: Note we skip the first message check here because we can start from
			// a bigger sequence number in case of restarts
			if conn.extCache.destType == shared.DestinationType_TIMER {

				// T471157 For timers, do not signal discontinuities to ack manager, since discontinuities are frequent
				msgSeqNum = 0

			} else if lastSeqNum+1 != int64(msgSeqNum) {

				// FIXME: add metric to help alert this case
				expectedSeqNum := 1 + lastSeqNum
				skippedMessages := int64(msgSeqNum) - lastSeqNum

				conn.logger.WithFields(bark.Fields{
					"msgSeqNum":       msgSeqNum,
					"expectedSeqNum":  expectedSeqNum,
					"skippedMessages": skippedMessages,
				}).Error("sequence number out of order")
			}

			// update the lastSeqNum to this value
			lastSeqNum = msg.Message.GetSequenceNumber()

			// convert this to an outMessage
			cMsg := cherami.NewConsumerMessage()
			cMsg.EnqueueTimeUtc = msg.Message.EnqueueTimeUtc
			cMsg.Payload = msg.Message.Payload

			cMsg.AckId = common.StringPtr(conn.extCache.ackMgr.getNextAckID(storeHostAddress(msgAddr), msgSeqNum))

			if conn.extCache.destType != shared.DestinationType_TIMER && visibilityTime > 0 {

				// offset visibilityTime by the specified delay, if any
				if delayNanos > 0 {
					visibilityTime += delayNanos
				}

				now := time.Now().UnixNano()

				// check if the messages have already 'expired'; ie, if it falls outside the
				// skip-older window. ignore (ie, don't skip any messages), if skip-older is '0'.
				// NB: messages coming from KFC may not have enqueue-time set; the logic below
				// ignores (ie, does not skip) messages that have no/zero enqueue-time.
				if skipOlderNanos > 0 && (visibilityTime < (now - skipOlderNanos)) {

					conn.skipOlderMsgs++
					continue loop
				}

				// check if this is a delayed-cg, and if so delay messages appropriately
				// compute visibility time based on the enqueue-time and specified delay
				if delayNanos > 0 && visibilityTime > now {

					delayTimer.Reset(time.Duration(visibilityTime - now))

					// wait unil the message should be made 'visible'
					select {
					case <-delayTimer.C: // sleep until delay expiry
						// continue down, to send message to cache

					case <-conn.closeChannel:
						conn.logger.WithFields(bark.Fields{
							common.TagMsgID: common.FmtMsgID(cMsg.GetAckId()),
							`Offset`:        msgAddr,
						}).Info("aborting delay and failing msg because of shutdown")
						// We need to update the ackMgr here to *not* have this message in the local map
						// since we couldn't even write this message out to the msgsCh.
						// Note: we need to just update this last message because this is a synchronous
						// pump which reads message after message and once we are here we immediately break
						// the pump. So there is absolute guarantee that we cannot call getNextAckID() on this
						// pump parallely.
						conn.extCache.ackMgr.resetMsg(msgAddr)
						return
					}
				}
			}

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
						`Offset`:        msgAddr,
					}).Info("writing msg to the client channel failed because of shutdown.")
					// We need to update the ackMgr here to *not* have this message in the local map
					// since we couldn't even write this message out to the msgsCh.
					// Note: we need to just update this last message because this is a synchronous
					// pump which reads message after message and once we are here we immediately break
					// the pump. So there is absolute guarantee that we cannot call getNextAckID() on this
					// pump parallely.
					conn.extCache.ackMgr.resetMsg(msgAddr)
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

func (conn *replicaConnection) writeCreditsPump() {
	defer conn.waitWG.Done()
	defer conn.call.Done()

	var localCredits int32 // credits accumulated
	var creditRefresh time.Time

	sendCreditsToStore := func(credits int32) (err error) {

		//conn.logger.WithField(`Credits`,cFlow.GetCredits()).Debug(`Sending credits!!`)

		cFlow := cherami.NewControlFlow()
		cFlow.Credits = common.Int32Ptr(credits)
		if err = conn.call.Write(cFlow); err != nil {
			conn.logger.WithField(common.TagErr, err).Error(`error writing credits to store`)
			return
		}

		err = conn.call.Flush()
		conn.sentCreds += credits
		localCredits -= credits
		creditRefresh = time.Now()
		return
	}

	localCredits = conn.initialCredits

	// send initial credits to the store.. so that we can actually start reading
	conn.logger.WithField(`initialCredits`, conn.initialCredits).Info(`writeCreditsPump: sending initial credits to store`)
	if err := sendCreditsToStore(conn.initialCredits); err != nil {
		conn.logger.WithField(common.TagErr, err).Error(`writeCreditsPump: error writing initial credits to store`)

		// if sending of initial credits failed, then close this connection
		go conn.close(nil)
		return
	}

	// creditBatchSize is proportionate to the number of initial credits
	// with 10 being the minimum batchsize
	creditBatchSize := (conn.initialCredits / 10)
	if creditBatchSize < minCreditBatchSize {
		creditBatchSize = minCreditBatchSize
	}

	creditRequestTicker := time.NewTicker(creditRequestTimeout)
	defer creditRequestTicker.Stop()

	// Start the write pump
	for {
		// listen for credits only if we satisfy the batch size
		if localCredits > creditBatchSize {
			select {
			case credits := <-conn.creditNotifyCh: // this is the common credit channel from redelivery cache
				if err := sendCreditsToStore(credits); err != nil {
					return
				}
			case credits := <-conn.localCreditCh: // this is the local credits channel only for this connection
				if err := sendCreditsToStore(credits); err != nil {
					return
				}
			case msgsRead := <-conn.readMsgsCh:
				localCredits += msgsRead
			case <-creditRequestTicker.C:
				// if we didn't get any credits for a long time and we are starving for
				// credits, then request some credits
				if time.Since(creditRefresh) > creditRequestTimeout {
					conn.logger.Warn("writeCreditsPump: starving for credits, requesting more")
					conn.extCache.requestCredits()
				}
			case <-conn.closeChannel:
				conn.logger.Info("writeCreditsPump: connection closed")
				return
			}
		} else {
			select {
			case msgsRead := <-conn.readMsgsCh:
				localCredits += msgsRead
			// we should listen on the local credits ch, just in case we requested for some credits
			// above and we are just getting it now
			case credits := <-conn.localCreditCh:
				if err := sendCreditsToStore(credits); err != nil {
					return
				}
			case <-conn.closeChannel:
				conn.logger.Info("writeCreditsPump: connection closed")
				return
			}
		}
	}
}
