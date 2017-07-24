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
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/common/throttler"
	"github.com/uber/cherami-server/services/inputhost/load"
	serverStream "github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
)

// logHighLatencyThreshold is the time threshold for logging
const logHighLatencyThreshold = time.Second

var logThrottler = throttler.New(1, 5*time.Second) // log only once every 5 seconds at max
var logsThrottled int32

type (
	pubConnection struct {
		connID              connectionID
		destinationPath     string
		stream              serverStream.BInOpenPublisherStreamInCall
		logger              bark.Logger
		reconfigureClientCh chan *reconfigInfo
		putMsgCh            chan *inPutMessage
		cacheTimeout        time.Duration
		ackChannel          chan *cherami.PutMessageAck
		replyCh             chan response
		closeChannel        chan struct{} // this is the channel which is used to actually close the stream
		waitWG              sync.WaitGroup
		notifyCloseCh       chan connectionID // this is used to notify the path cache to remove us from its list
		doneCh              chan bool         // this is used to unblock the OpenPublisherStream()

		recvMsgs      int64 // total msgs received
		sentAcks      int64 // total acks sent out
		sentNacks     int64 // total n-acks sent out
		sentThrottled int64 // total msgs that were throttled
		failedMsgs    int64 // total inflight msgs that were 'failed'

		connTokenBucketValue   atomic.Value // Value to controll access for TB for rate limit Num of Msgs received per sec
		connMsgsLimitPerSecond int32        //per second rate limit for this connection
		lk                     sync.Mutex
		opened                 bool
		closed                 bool
		limitsEnabled          bool
		pathCache              *inPathCache
		pathWG                 *sync.WaitGroup
	}

	response struct {
		ackID       string            // this is unique identifier of message
		userContext map[string]string // this is user specified context to pass through

		putMsgRecvTime time.Time // this is the msg receive time, used for latency metrics
	}

	// inPutMessage is the wrapper struct which holds the actual message and
	// the channel to get the reply
	// XXX: Note any changes to this struct should be made on the
	// PutMessageBatch() API in services/inputhost/inpthost.go
	inPutMessage struct {
		putMsg         *cherami.PutMessage
		putMsgAckCh    chan *cherami.PutMessageAck
		putMsgRecvTime time.Time
	}

	earlyReplyAck struct {
		// time when we receive ack from replica
		ackReceiveTime time.Time

		// time when we send ack back to stream
		ackSentTime time.Time
	}

	reconfigInfo struct {
		updateUUID string
		cmdType    cherami.InputHostCommandType
		drainWG    *sync.WaitGroup
	}

	pubConnectionClosedCb func(connectionID)
)

// failTimeout is the timeout to wait for acks from the store when a
// stream is closed
// if we don't get any acks back fail the messages
const failTimeout = 3 * time.Second

// reconfigClientChSize is the size of the reconfigClientCh
const reconfigClientChSize = 50

// perConnMsgsLimitPerSecond is the rate limit per connection
const perConnMsgsLimitPerSecond = 10000

func newPubConnection(destinationPath string, stream serverStream.BInOpenPublisherStreamInCall, pathCache *inPathCache, m3Client metrics.Client, limitsEnabled bool, timeout time.Duration, doneCh chan bool) *pubConnection {
	conn := &pubConnection{
		connID:          pathCache.currID,
		destinationPath: destinationPath,
		logger: pathCache.logger.WithFields(bark.Fields{
			common.TagInPubConnID: common.FmtInPubConnID(int(pathCache.currID)),
			common.TagModule:      `pubConn`,
		}),
		stream:       stream,
		putMsgCh:     pathCache.putMsgCh,
		cacheTimeout: timeout,
		//perConnTokenBucket:  common.NewTokenBucket(perConnMsgsLimitPerSecond, common.NewRealTimeSource()),
		replyCh:             make(chan response, defaultBufferSize),
		reconfigureClientCh: make(chan *reconfigInfo, reconfigClientChSize),
		ackChannel:          make(chan *cherami.PutMessageAck, defaultBufferSize),
		closeChannel:        make(chan struct{}),
		notifyCloseCh:       pathCache.notifyConnsCloseCh,
		doneCh:              doneCh,
		limitsEnabled:       limitsEnabled,
		pathCache:           pathCache,
		pathWG:              &pathCache.connsWG,
	}
	conn.SetMsgsLimitPerSecond(common.HostPerConnMsgsLimitPerSecond)
	return conn
}

func (conn *pubConnection) open() {
	conn.lk.Lock()
	defer conn.lk.Unlock()

	if !conn.opened {
		conn.waitWG.Add(2)
		conn.pathWG.Add(1) // this makes the manage routine in the pathCache is alive
		go conn.readRequestStream()
		go conn.writeAcksStream()

		conn.opened = true

		conn.logger.Info("pubConn opened")
	}
}

func (conn *pubConnection) close() {

	conn.lk.Lock()
	if conn.closed {
		conn.lk.Unlock()
		return
	}

	close(conn.closeChannel)
	conn.closed = true
	conn.waitWG.Wait()

	// we have successfully closed the connection
	// make sure we update the ones who are waiting for us
	select {
	case conn.doneCh <- true:
	default:
	}

	conn.lk.Unlock()

	// notify the patch cache to remove this conn
	// from the cache. No need to hold the lock
	// for this.
	conn.notifyCloseCh <- conn.connID

	// set the wait group for the pathCache to be done
	conn.pathWG.Done()

	conn.logger.WithFields(bark.Fields{
		`sentAcks`:      conn.sentAcks,
		`sentNacks`:     conn.sentNacks,
		`sentThrottled`: conn.sentThrottled,
		`failedMsgs`:    conn.failedMsgs,
	}).Info("pubConn closed")
}

// readRequestStream is the pump which reads messages from the client stream.
// this sends the message to the next layer (extHost) and then also prepares
// the writeAcksStream by sending a message to the intermediary "replyCh"
func (conn *pubConnection) readRequestStream() {
	defer conn.waitWG.Done()
	var msgLength int64

	// Setup the connIdleTimer
	connIdleTimer := common.NewTimer(conn.cacheTimeout)
	defer connIdleTimer.Stop()

	for {
		connIdleTimer.Reset(conn.cacheTimeout)
		select {
		case <-connIdleTimer.C:
			conn.logger.WithField(`cacheTimeout`, conn.cacheTimeout).Info(`client connection idle for : ; closing it`)
			go conn.close()
			return
		default:
			msg, err := conn.stream.Read()
			if err != nil {
				conn.logger.WithField(common.TagErr, err).Info(`inputhost: PublisherStream closed on read error`)
				// conn.close() can block waiting for all routines to
				// go away. make sure we don't deadlock
				go conn.close()
				return
			}

			// record the counter metric
			conn.pathCache.m3Client.IncCounter(metrics.PubConnectionStreamScope, metrics.InputhostMessageReceived)
			conn.pathCache.destM3Client.IncCounter(metrics.PubConnectionScope, metrics.InputhostDestMessageReceived)
			conn.pathCache.dstMetrics.Increment(load.DstMetricOverallNumMsgs)

			// Note: we increment the destination bytes in counter here because we could throttle this message
			// even before it reaches any of the extents (which increments the extent specific bytes in counter)
			msgLength = int64(len(msg.Data))
			conn.pathCache.dstMetrics.Add(load.DstMetricBytesIn, msgLength)
			conn.pathCache.hostMetrics.Add(load.HostMetricBytesIn, msgLength)
			conn.pathCache.destM3Client.AddCounter(metrics.PubConnectionScope, metrics.InputhostDestMessageReceivedBytes, msgLength)

			conn.recvMsgs++

			inMsg := &inPutMessage{
				putMsg:         msg,
				putMsgAckCh:    conn.ackChannel,
				putMsgRecvTime: time.Now(),
			}

			throttled := false
			if conn.limitsEnabled {
				consumed, _ := conn.GetConnTokenBucketValue().TryConsume(1)
				throttled = !consumed
				if throttled {
					// just send a THROTTLED status back to the client
					conn.logger.Warn("throttling due to rate violation")
					conn.pathCache.m3Client.IncCounter(metrics.PubConnectionStreamScope, metrics.InputhostMessageLimitThrottled)
					conn.pathCache.destM3Client.IncCounter(metrics.PubConnectionScope, metrics.InputhostDestMessageLimitThrottled)

					inMsg.putMsgAckCh <- &cherami.PutMessageAck{
						ID:          common.StringPtr(msg.GetID()),
						UserContext: msg.GetUserContext(),
						Status:      common.CheramiStatusPtr(cherami.Status_THROTTLED),
						Message:     common.StringPtr("throttling; inputhost is busy"),
					}
				}
			}

			if !throttled {
				if conn.limitsEnabled {
					// if sending to this channel is blocked we need to return a throttle error to the client
					select {
					case conn.putMsgCh <- inMsg:
						// populate the inflight map
						conn.replyCh <- response{msg.GetID(), msg.GetUserContext(), inMsg.putMsgRecvTime}
					default:
						conn.pathCache.m3Client.IncCounter(metrics.PubConnectionStreamScope, metrics.InputhostMessageChannelFullThrottled)
						conn.pathCache.destM3Client.IncCounter(metrics.PubConnectionScope, metrics.InputhostDestMessageChannelFullThrottled)

						// just send a THROTTLED status back to the client
						conn.logger.Warn("throttling due to putMsgCh being filled")
						inMsg.putMsgAckCh <- &cherami.PutMessageAck{
							ID:          common.StringPtr(msg.GetID()),
							UserContext: msg.GetUserContext(),
							Status:      common.CheramiStatusPtr(cherami.Status_THROTTLED),
							Message:     common.StringPtr("throttling; inputhost is busy"),
						}
					}
				} else {
					select {
					case conn.putMsgCh <- inMsg:
						conn.replyCh <- response{msg.GetID(), msg.GetUserContext(), inMsg.putMsgRecvTime}
					case <-conn.closeChannel:
						// we are shutting down here. just return
						return
					}
				}
			}
		}
	}
}

// writeAcksStream is the pump which sends acks back to the client.
// we read from the intermediary "replyCh", and populate the
// "inflightMessages" map. this is done to serve 2 purposes.
// 1. read from ackChannel only of necessary
// 2. make sure we respond failure back to the client in case something
//    happens and we close the streams underneath
func (conn *pubConnection) writeAcksStream() {
	earlyReplyAcks := make(map[string]earlyReplyAck) // map of all the early acks
	inflightMessages := make(map[string]response)
	defer conn.failInflightMessages(inflightMessages, earlyReplyAcks)

	flushTicker := time.NewTicker(common.FlushTimeout) // start ticker to flush tchannel stream
	defer flushTicker.Stop()

	unflushedWrites := 0

	for {
		select {
		case resCh := <-conn.replyCh:
			// First check if we have already seen the ack for this ID
			if _, ok := earlyReplyAcks[resCh.ackID]; ok {
				// We already received the ack for this msgID.  Complete the request immediately.
				conn.updateEarlyReplyAcks(resCh, earlyReplyAcks)
			} else {
				inflightMessages[resCh.ackID] = resCh
			}
		case <-flushTicker.C:
			if unflushedWrites > 0 {
				if err := conn.flushCmdToClient(unflushedWrites); err != nil {
					// since flush failed, trigger a close of the connection which will fail inflight messages
					go conn.close()
				}
				unflushedWrites = 0
			}
		default:
			if len(inflightMessages) == 0 {
				select {
				case resCh := <-conn.replyCh:
					// First check if we have already seen the ack for this ID
					if _, ok := earlyReplyAcks[resCh.ackID]; ok {
						// We already received the ack for this msgID.  Complete the request immediately.
						conn.updateEarlyReplyAcks(resCh, earlyReplyAcks)
					} else {
						inflightMessages[resCh.ackID] = resCh
					}
				case rInfo := <-conn.reconfigureClientCh:
					// record the counter metric
					conn.pathCache.m3Client.IncCounter(metrics.PubConnectionStreamScope, metrics.InputhostReconfClientRequests)
					cmd := createReconfigureCmd(rInfo)
					if err := conn.writeCmdToClient(cmd, rInfo.drainWG); err != nil {
						// trigger a close of the connection
						go conn.close()
						return
					}
					unflushedWrites++
					// we will flush this in our next interval. Since this is just a reconfig
				case <-flushTicker.C:
					if unflushedWrites > 0 {
						if err := conn.flushCmdToClient(unflushedWrites); err != nil {
							// since flush failed, trigger a close of the connection which will fail inflight messages
							go conn.close()
							return
						}
						unflushedWrites = 0
					}
				case <-conn.closeChannel:
					return
				}
			} else {

				select {
				case ack, ok := <-conn.ackChannel:
					if ok {
						ackReceiveTime := time.Now()
						exists, err := conn.writeAckToClient(inflightMessages, ack, ackReceiveTime)
						if err != nil {
							// trigger a close of the connection
							go conn.close()
							return
						}

						if !exists {
							// we received an ack even before we populated the inflight map
							// put it in the earlyReplyAcks and remove it when we get the inflight map
							// XXX: log disabled to reduce spew
							// conn.logger.
							//	WithField(common.TagInPutAckID, common.FmtInPutAckID(ack.GetID())).
							//	Debug("received an ack even before we populated the inflight map")
							earlyReplyAcks[ack.GetID()] = earlyReplyAck{
								ackReceiveTime: ackReceiveTime,
								ackSentTime:    time.Now(),
							}
						}

						unflushedWrites++
						if unflushedWrites > common.FlushThreshold {
							if err = conn.flushCmdToClient(unflushedWrites); err != nil {
								// since flush failed, trigger a close of the connection which will fail inflight messages
								go conn.close()
								return
							}
							unflushedWrites = 0
						}
					} else {
						return
					}
				case rInfo := <-conn.reconfigureClientCh:
					// record the counter metric
					conn.pathCache.m3Client.IncCounter(metrics.PubConnectionStreamScope, metrics.InputhostReconfClientRequests)
					cmd := createReconfigureCmd(rInfo)
					if err := conn.writeCmdToClient(cmd, rInfo.drainWG); err != nil {
						// trigger a close of the connection
						go conn.close()
						return
					}
					unflushedWrites++
					// we will flush this in our next interval. Since this is just a reconfig
				case <-flushTicker.C:
					if unflushedWrites > 0 {
						if err := conn.flushCmdToClient(unflushedWrites); err != nil {
							// since flush failed, trigger a close of the connection which will fail inflight messages
							go conn.close()
							return
						}
						unflushedWrites = 0
					}
				case <-conn.closeChannel:
					return
				}
			}
		}
	}
}

func (conn *pubConnection) failInflightMessages(inflightMessages map[string]response, earlyReplyAcks map[string]earlyReplyAck) {
	defer conn.stream.Done()
	failTimer := common.NewTimer(failTimeout)
	defer failTimer.Stop()
	// make sure we wait for all the messages for some timeout period and fail only if necessary
	// we only iterate through the inflightMessages map because the earlyAcksMap is
	// only there for updating metrics properly and since we are failing here, we don't care.
	for quit := false; !quit && len(inflightMessages) > 0; {
		select {
		case ack, ok := <-conn.ackChannel:
			if ok {
				conn.writeAckToClient(inflightMessages, ack, time.Now())
				// ignore error above since we are anyway failing
				// Since we are anyway failing here, we don't care about the
				// early acks map since the only point for that map is to
				// update metric
			}
		case resCh := <-conn.replyCh:
			// First check if we have already seen the ack for this ID
			// We do the check here to make sure we don't incorrectly populate
			// the inflight messages map and timeout those messages down below
			// after out failTimeout elapses.
			// This situation can happen, if we just sent an ack above in the normal
			// path and have not yet populated the infight map and closed the connection
			if _, ok := earlyReplyAcks[resCh.ackID]; ok {
				// We already received the ack for this msgID.  Complete the request immediately.
				conn.updateEarlyReplyAcks(resCh, earlyReplyAcks)
			} else {
				inflightMessages[resCh.ackID] = resCh
			}
		case <-failTimer.C:
			conn.logger.WithField(`inflightMessages`, len(inflightMessages)).Info(`inputhost: timing out messages`)
			quit = true
		}
	}

	// send a failure to all the remaining inflight messages
	for id, resp := range inflightMessages {
		if _, ok := earlyReplyAcks[id]; !ok {
			putMsgAck := &cherami.PutMessageAck{
				ID:          common.StringPtr(id),
				UserContext: resp.userContext,
				Status:      common.CheramiStatusPtr(cherami.Status_FAILED),
				Message:     common.StringPtr("inputhost: timing out unacked message"),
			}
			d := time.Since(resp.putMsgRecvTime)
			conn.pathCache.m3Client.RecordTimer(metrics.PubConnectionStreamScope, metrics.InputhostWriteMessageBeforeAckLatency, d)
			conn.pathCache.destM3Client.RecordTimer(metrics.PubConnectionScope, metrics.InputhostDestWriteMessageBeforeAckLatency, d)

			conn.stream.Write(createAckCmd(putMsgAck))

			d = time.Since(resp.putMsgRecvTime)
			conn.pathCache.m3Client.RecordTimer(metrics.PubConnectionStreamScope, metrics.InputhostWriteMessageLatency, d)
			conn.pathCache.destM3Client.RecordTimer(metrics.PubConnectionScope, metrics.InputhostDestWriteMessageLatency, d)
			if d > logHighLatencyThreshold {
				if logThrottler.Allow() {
					conn.logger.WithFields(bark.Fields{
						common.TagDstPth:     common.FmtDstPth(conn.destinationPath),
						common.TagInPutAckID: common.FmtInPutAckID(id),
						`duration`:           d,
						`putMsgChanLen`:      len(conn.putMsgCh),
						`putMsgAckChanLen`:   len(conn.ackChannel),
						`replyChanLen`:       len(conn.replyCh),
						`throttled`:          atomic.SwapInt32(&logsThrottled, 0),
					}).Error(`failInflightMessages: publish message latency`)
				} else {
					atomic.AddInt32(&logsThrottled, 1)
				}
			}
			// Record the number of failed messages
			conn.pathCache.m3Client.IncCounter(metrics.PubConnectionStreamScope, metrics.InputhostMessageFailures)
			conn.pathCache.destM3Client.IncCounter(metrics.PubConnectionScope, metrics.InputhostDestMessageFailures)
			conn.failedMsgs++
			conn.pathCache.dstMetrics.Increment(load.DstMetricNumFailed)
		}
	}

	// flush whatever we have
	conn.stream.Flush()
	conn.waitWG.Done()
}

func (conn *pubConnection) flushCmdToClient(unflushedWrites int) (err error) {
	if err = conn.stream.Flush(); err != nil {
		conn.logger.WithFields(bark.Fields{common.TagErr: err, `unflushedWrites`: unflushedWrites}).Error(`inputhost: error flushing messages to client stream failed`)
		// since flush failed, trigger a close of the connection which will fail inflight messages
	}

	return
}

func (conn *pubConnection) writeCmdToClient(cmd *cherami.InputHostCommand, drainWG *sync.WaitGroup) (err error) {
	if err = conn.stream.Write(cmd); err != nil {
		conn.logger.WithFields(bark.Fields{`cmd`: cmd, common.TagErr: err}).Info(`inputhost: Unable to Write cmd back to client`)
	}
	if cmd.GetType() == cherami.InputHostCommandType_DRAIN {
		// if this is a DRAIN command wait for some timeout period and then
		// just close the connection
		// We do this to make sure the connection object doesn't hang around
		// at this point we have sent the DRAIN command to the client
		// XXX:  assert non-nil WG
		go conn.waitForDrain(drainWG)
	}

	return
}

// waitForDrain is a safety routine to make sure we don't hold onto the connection object
// In a normal scenario, when all the extents hosted on this inputhost for the destination is
// drained properly, it will automatically trigger a connection close, which will clean up the
// connection object.
// But if another extent gets assigned to this inputhost before the drain of an older extent
// completes, then the pathCache will not unload which means this object won't be closed, while the
// client would have already stopped writing on this connection. In that case, we will hit the
// timeout and close the object anyway.
func (conn *pubConnection) waitForDrain(drainWG *sync.WaitGroup) {
	// wait for some time to set the WG to be done.
	connWGTimer := common.NewTimer(connWGTimeout)
	defer connWGTimer.Stop()
	select {
	case <-conn.closeChannel:
		drainWG.Done()
		return
	case <-connWGTimer.C:
		drainWG.Done()
	}

	// now the WG is *unblocked*; wait for the actual drain
	// from the exthost layer to finish for a timeout period
	drainTimer := common.NewTimer(defaultDrainTimeout)
	defer drainTimer.Stop()
	// just wait for a minute, for all the inflight messages to drain
	select {
	case <-conn.closeChannel:
		// if it is closed already, just return
		return
	case <-drainTimer.C:
		conn.logger.Warn("timed out waiting for drain to finish. just closing the connection")
		go conn.close()
	}
}

func (conn *pubConnection) writeAckToClient(inflightMessages map[string]response, ack *cherami.PutMessageAck, ackReceiveTime time.Time) (exists bool, err error) {
	cmd := createAckCmd(ack)
	err = conn.writeCmdToClient(cmd, nil)
	if err != nil {
		conn.logger.
			WithField(common.TagInPutAckID, common.FmtInPutAckID(ack.GetID())).
			WithField(common.TagErr, err).Error(`inputhost: writing ack Id failed`)
	}
	exists = conn.updateInflightMap(inflightMessages, ack.GetID(), ackReceiveTime)

	// update the failure metric, if needed
	if ack.GetStatus() == cherami.Status_FAILED || err != nil {
		conn.pathCache.m3Client.IncCounter(metrics.PubConnectionStreamScope, metrics.InputhostMessageFailures)
		conn.pathCache.destM3Client.IncCounter(metrics.PubConnectionScope, metrics.InputhostDestMessageFailures)
	}

	if err == nil {
		switch ack.GetStatus() {
		case cherami.Status_OK:
			conn.sentAcks++
			conn.pathCache.dstMetrics.Increment(load.DstMetricNumAcks)
		case cherami.Status_FAILED:
			conn.sentNacks++
			conn.pathCache.dstMetrics.Increment(load.DstMetricNumNacks)
		case cherami.Status_THROTTLED:
			conn.sentThrottled++
			conn.pathCache.dstMetrics.Increment(load.DstMetricNumThrottled)
		}
	}

	return
}

func (conn *pubConnection) updateInflightMap(inflightMessages map[string]response, ackID string, ackReceiveTime time.Time) bool {
	if resp, ok := inflightMessages[ackID]; ok {

		// record the latency
		d := time.Since(resp.putMsgRecvTime)
		conn.pathCache.m3Client.RecordTimer(metrics.PubConnectionStreamScope, metrics.InputhostWriteMessageLatency, d)
		conn.pathCache.destM3Client.RecordTimer(metrics.PubConnectionScope, metrics.InputhostDestWriteMessageLatency, d)

		conn.pathCache.m3Client.RecordTimer(metrics.PubConnectionStreamScope, metrics.InputhostWriteMessageBeforeAckLatency, ackReceiveTime.Sub(resp.putMsgRecvTime))
		conn.pathCache.destM3Client.RecordTimer(metrics.PubConnectionScope, metrics.InputhostDestWriteMessageBeforeAckLatency, ackReceiveTime.Sub(resp.putMsgRecvTime))

		if d > logHighLatencyThreshold {
			if logThrottler.Allow() {
				conn.logger.WithFields(bark.Fields{
					common.TagDstPth:     common.FmtDstPth(conn.destinationPath),
					common.TagInPutAckID: common.FmtInPutAckID(ackID),
					`d`:                  d,
					`putMsgChanLen`:      len(conn.putMsgCh),
					`putMsgAckChanLen`:   len(conn.ackChannel),
					`replyChanLen`:       len(conn.replyCh),
					`throttled`:          atomic.SwapInt32(&logsThrottled, 0),
				}).Info(`publish message latency at updateInflightMap`)
			} else {
				atomic.AddInt32(&logsThrottled, 1)
			}
		}
		delete(inflightMessages, ackID)
		return ok
	}

	// didn't find it in the inflight map, which means we got an ack even before we populated it
	return false
}

func (conn *pubConnection) updateEarlyReplyAcks(resCh response, earlyReplyAcks map[string]earlyReplyAck) {
	// make sure we account for the time when we sent the ack as well
	d := time.Since(resCh.putMsgRecvTime)
	ack, _ := earlyReplyAcks[resCh.ackID]
	actualDuration := d - time.Since(ack.ackSentTime)
	if d > logHighLatencyThreshold {
		if logThrottler.Allow() {
			conn.logger.WithFields(bark.Fields{
				common.TagDstPth:     common.FmtDstPth(conn.destinationPath),
				common.TagInPutAckID: common.FmtInPutAckID(resCh.ackID),
				`d`:                  d,
				`actualDuration`:     actualDuration,
				`putMsgChanLen`:      len(conn.putMsgCh),
				`putMsgAckChanLen`:   len(conn.ackChannel),
				`replyChanLen`:       len(conn.replyCh),
				`throttled`:          atomic.SwapInt32(&logsThrottled, 0),
			}).Info(`publish message latency at updateEarlyReplyAcks and actualDuration`)
		} else {
			atomic.AddInt32(&logsThrottled, 1)
		}
	}
	conn.pathCache.m3Client.RecordTimer(metrics.PubConnectionStreamScope, metrics.InputhostWriteMessageLatency, actualDuration)
	conn.pathCache.destM3Client.RecordTimer(metrics.PubConnectionScope, metrics.InputhostDestWriteMessageLatency, actualDuration)

	// also record the time that excludes the time for sending ack back to socket
	actualDurationExcludeSocket := d - time.Since(ack.ackReceiveTime)
	conn.pathCache.m3Client.RecordTimer(metrics.PubConnectionStreamScope, metrics.InputhostWriteMessageBeforeAckLatency, actualDurationExcludeSocket)
	conn.pathCache.destM3Client.RecordTimer(metrics.PubConnectionScope, metrics.InputhostDestWriteMessageBeforeAckLatency, actualDurationExcludeSocket)

	delete(earlyReplyAcks, resCh.ackID)
	// XXX: Disabled due to log noise
	// conn.logger.WithField(common.TagInPutAckID, common.FmtInPutAckID(resCh.ackID)).Info("Found ack for this response in earlyReplyAcks map. Not adding it to the inflight map")
}

func createReconfigureCmd(rInfo *reconfigInfo) *cherami.InputHostCommand {
	cmd := cherami.NewInputHostCommand()
	cmd.Reconfigure = &cherami.ReconfigureInfo{UpdateUUID: common.StringPtr(rInfo.updateUUID)}
	cmd.Type = common.CheramiInputHostCommandTypePtr(rInfo.cmdType)

	return cmd
}

func createAckCmd(ack *cherami.PutMessageAck) *cherami.InputHostCommand {
	cmd := cherami.NewInputHostCommand()
	cmd.Ack = ack
	cmd.Type = common.CheramiInputHostCommandTypePtr(cherami.InputHostCommandType_ACK)

	return cmd
}

// GetMsgsLimitPerSecond gets msgs rate limit per second for this connection
func (conn *pubConnection) GetMsgsLimitPerSecond() int {
	return int(atomic.LoadInt32(&conn.connMsgsLimitPerSecond))
}

// SetMsgsLimitPerSecond sets msgs rate limit per second for this connection
func (conn *pubConnection) SetMsgsLimitPerSecond(connLimit int32) {
	atomic.StoreInt32(&conn.connMsgsLimitPerSecond, connLimit)
	conn.SetConnTokenBucketValue(int32(connLimit))
}

// GetConnTokenBucketValue gets token bucket for connMsgsLimitPerSecond
func (conn *pubConnection) GetConnTokenBucketValue() common.TokenBucket {
	return conn.connTokenBucketValue.Load().(common.TokenBucket)
}

// SetConnTokenBucketValue sets token bucket for connMsgsLimitPerSecond
func (conn *pubConnection) SetConnTokenBucketValue(connLimit int32) {
	tokenBucket := common.NewTokenBucket(int(connLimit), common.NewRealTimeSource())
	conn.connTokenBucketValue.Store(tokenBucket)
}
