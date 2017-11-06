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
	"strings"
	"sync"
	"time"

	"github.com/uber-common/bark"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/outputhost/load"
	serverStream "github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
)

type (
	consConnection struct {
		connID                   int
		stream                   serverStream.BOutOpenConsumerStreamInCall
		creditsCh                chan int32
		msgsCh                   <-chan *cherami.ConsumerMessage // read only channel getting messages from all extents
		msgsRedeliveryCh         <-chan *cherami.ConsumerMessage // read only channel getting messages from redelivery cache
		priorityMsgsRedeliveryCh <-chan *cherami.ConsumerMessage // read only channel getting priority messages from redelivery cache
		msgCacheCh               chan<- cacheMsg                 // write only channel to write the message to the cache
		msgCacheRedeliveredCh    chan<- cacheMsg                 // write only channel to write the message to the cache after redelivery
		reconfigureClientCh      chan string                     // readonly channel getting reconfigure notifications
		expiration               time.Time
		cacheTimeout             time.Duration
		closeChannel             chan struct{} // this is the channel which is used to actually close the stream
		waitWG                   sync.WaitGroup
		connectionsClosedCh      chan<- int     // this channel is used to notify the CG cache to remove us from its list
		notifyCh                 chan NotifyMsg // this is the channel which is used to receive notifications from the notifier for throttling
		logger                   bark.Logger
		cgCache                  *consumerGroupCache

		// the following are incremented in the read/write pumps (which handle
		// one message at a time) and are read during connection close after
		// the pumps have closed -- therefore these don't need to be protected
		// for concurrent access.
		recvCreds      int64 // total credits received
		sentMsgs       int64 // total messages sent out
		reSentMsgs     int64 // count of sent messages that were re-deliveries
		sentToMsgCache int64 // count of message sent to message cache

		unflushedWrites int   // messages written to stream since last flush
		unflushedSize   int64 // total size of messages yet to be flushed

		lk     sync.Mutex
		opened bool
		closed bool
	}
)

const (
	// creditFlushTimeout is the time to flush accumulated credits to the write pump
	creditFlushTimeout time.Duration = 5 * time.Second
)

const smartRetryTestPath = `runner.SmartRetry`

func newConsConnection(id int, cgCache *consumerGroupCache, stream serverStream.BOutOpenConsumerStreamInCall, timeout time.Duration, logger bark.Logger) *consConnection {
	conn := &consConnection{
		connID:                   id,
		msgsCh:                   cgCache.msgsCh,
		msgsRedeliveryCh:         cgCache.msgsRedeliveryCh,
		msgCacheCh:               cgCache.msgCacheCh,
		msgCacheRedeliveredCh:    cgCache.msgCacheRedeliveredCh,
		priorityMsgsRedeliveryCh: cgCache.priorityMsgsRedeliveryCh,
		connectionsClosedCh:      cgCache.notifyConsCloseCh,
		stream:                   stream,
		reconfigureClientCh:      make(chan string, 5),
		creditsCh:                make(chan int32, 5),
		notifyCh:                 make(chan NotifyMsg, 5),
		cacheTimeout:             timeout,
		closeChannel:             make(chan struct{}),
		expiration:               time.Now().Add(timeout),
		logger:                   logger.WithField(common.TagModule, `consConn`),
		cgCache:                  cgCache,
	}

	return conn
}

func (conn *consConnection) open() *sync.WaitGroup {
	conn.lk.Lock()
	defer conn.lk.Unlock()

	if !conn.opened {
		// register with the notifier first
		conn.cgCache.notifier.Register(conn.connID, conn.notifyCh)
		conn.waitWG.Add(2)          // this WG is the local WG for the pumps
		conn.cgCache.connsWG.Add(1) // this WG is for the cgCache
		go conn.writeMsgsStream()
		go conn.readCreditsStream()

		conn.opened = true
		conn.logger.Info("consConn opened")
	}

	return &conn.waitWG
}

func (conn *consConnection) close() {
	conn.lk.Lock()
	if !conn.closed {
		// unregister from the notifier
		conn.cgCache.notifier.Unregister(conn.connID)
		// close order should be:
		// 1. close the WriteMsgs stream, which will call Done() on the stream and close
		// the stream completely
		// 2. close the credits stream
		close(conn.closeChannel)
		conn.closed = true
		conn.waitWG.Wait() // wait until the pumps have closed
		// now notify the CGCache to update its connections map
		conn.connectionsClosedCh <- conn.connID

		// now set the WG to unblock cgCache unload
		conn.cgCache.connsWG.Done()
		conn.logger.WithFields(bark.Fields{
			`sentMsgs`:       conn.sentMsgs,
			`reSentMsgs`:     conn.reSentMsgs,
			`recvCreds`:      conn.recvCreds,
			`sentToMsgCache`: conn.sentToMsgCache,
		}).Info("consConn closed")
	}
	conn.lk.Unlock()
}

func (conn *consConnection) isExpired() (ret bool) {
	ret = conn.expiration.Before(time.Now())
	return
}

// sendCreditsToWritePump is used to send credits to the write pump in a non-blocking way
// If we successfully send the credits on the channel, reset the local counter
func (conn *consConnection) sendCreditsToWritePump(localCredits *int32) {
	select {
	case conn.creditsCh <- *localCredits:
		// we sent the credits succesfully. Reset the counter.
		*localCredits = 0
	default:
		// If we cannot send the credits at this time, just accumulate it
		// but don't reset the counter
		// update M3 to record this
		conn.cgCache.m3Client.UpdateGauge(metrics.ConsConnectionStreamScope, metrics.OutputhostCreditsAccumulated, int64(*localCredits))
		conn.cgCache.consumerM3Client.UpdateGauge(metrics.ConsConnectionScope, metrics.OutputhostCGCreditsAccumulated, int64(*localCredits))
	}

}

// readCreditsStream is the stream which keeps reading the credits from the client
func (conn *consConnection) readCreditsStream() {
	// start ticker to send accumulated credits to the write pump
	// This is needed because imagine we are not able to send credits
	// received from the client immediately. In that case we need to
	// accumulate those credits and make sure we send those out to the
	// write pump periodically to prevent unnecessary starvation.
	creditTicker := time.NewTicker(creditFlushTimeout)
	defer creditTicker.Stop()

	// localCredits is used to accumulate the credits received from the client
	var localCredits int32
	defer conn.waitWG.Done()
	for {
		select {
		case <-conn.closeChannel:
			return
		case <-creditTicker.C:
			// If we have accumulated credits already, try to send it to the
			// write pump
			if localCredits > 0 {
				// try to send the credits to the write pump
				conn.sendCreditsToWritePump(&localCredits)
			}
		default:
			msg, err := conn.stream.Read()
			if err != nil {
				conn.logger.
					WithField(common.TagErr, err).
					Info("stream Read failed")
				// conn.close() can block waiting for all routines to
				// go away. make sure we don't deadlock
				go conn.close()
				return
			}

			// conn.logger.WithField(`Credits`, msg.GetCredits()).Debug(`outputhost: Received credits`)
			conn.cgCache.m3Client.AddCounter(metrics.ConsConnectionStreamScope, metrics.OutputhostCreditsReceived, int64(msg.GetCredits()))
			conn.cgCache.consumerM3Client.AddCounter(metrics.ConsConnectionScope, metrics.OutputhostCGCreditsReceived, int64(msg.GetCredits()))

			localCredits += msg.GetCredits()
			conn.recvCreds += int64(msg.GetCredits())

			// send this to writeMsgsPump which keeps track of the local credits
			// Make this non-blocking because writeMsgsPump could be closed before this
			conn.sendCreditsToWritePump(&localCredits)
		}
	}
}

// writeMsgsStream is the pump which sends msgs back to the client.
// this pump will be broken by the above pump as part of the
// close, by closing the creditsCh
func (conn *consConnection) writeMsgsStream() {
	defer conn.waitWG.Done()
	var localCredits int32

	flushTicker := time.NewTicker(common.FlushTimeout) // start ticker to flush tchannel stream
	defer flushTicker.Stop()

	// get the throttler
	throttler := newThrottler()

	// For smart retry runner, cap the throttle at 1/10th of a second
	if strings.Contains(conn.cgCache.destPath, smartRetryTestPath) {
		throttler.SetMaxThrottleDuration(time.Second / 10)
	}

	// get the initial duration
	slowDown := throttler.GetCurrentSleepDuration()
	throttleTimer := common.NewTimer(slowDown)
	defer throttleTimer.Stop()

	for {
		// Before we begin always sleep for the slowDown duration.
		// We need to make sure a shutdown preempts us by listening on
		// the close channel as well.
		// if we get more notifications during this time we will read it
		// later down the line and update the sleep duration
		if slowDown > 0 {
			conn.cgCache.consumerM3Client.IncCounter(metrics.ConsConnectionScope, metrics.OutputhostCGMessagesThrottled)
			throttleTimer.Reset(slowDown)
			select {
			case <-throttleTimer.C:
				// do nothing
			case <-conn.closeChannel:
				conn.stream.Done()
				return
			}
		}
		if localCredits == 0 {
			select {
			case crInc := <-conn.creditsCh:
				conn.expiration = time.Now().Add(conn.cacheTimeout)
				localCredits += crInc
			case <-time.After(conn.cacheTimeout):
				// if client doesn't give any credits and we are timed out, then close the connection
				if conn.isExpired() {
					go conn.close()
				}
			case updateUUID := <-conn.reconfigureClientCh:
				conn.logger.WithField(`updateUUID`, updateUUID).Debug(`reconfiguring client (with 0 credits)`)
				cmd := createReconfigureCmd(updateUUID)
				conn.writeToClient(cmd)
			case <-flushTicker.C:
				conn.flushToClient()
			case <-conn.closeChannel:
				// we should call Done() here which will in turn trigger a close of the above stream
				conn.stream.Done()
				return
			}
		} else {
			select {
			case crInc := <-conn.creditsCh:
				conn.expiration = time.Now().Add(conn.cacheTimeout)
				localCredits += crInc
			case msg := <-conn.priorityMsgsRedeliveryCh:
				// XXX: Disable this log later
				conn.logger.WithField(common.TagAckID, common.FmtAckID(msg.GetAckId())).
					WithField(common.TagSlowDownSeconds, common.FmtSlowDown(slowDown)).
					Info("redelivering priority msg back to client")
				conn.createMsgAndWriteToClientUtil(msg, &localCredits, conn.msgCacheRedeliveredCh)
				conn.reSentMsgs++
			case msg := <-conn.msgsRedeliveryCh:
				//conn.logger.WithField(common.TagAckID, common.FmtAckID(msg.GetAckId())).
				//	WithField(`throttle`, float64(slowDown)/float64(time.Second)).
				//	Info("consconnection: redelivering msg back to the client")
				conn.createMsgAndWriteToClientUtil(msg, &localCredits, conn.msgCacheRedeliveredCh)
				conn.reSentMsgs++
			case msg := <-conn.msgsCh:
				conn.createMsgAndWriteToClientUtil(msg, &localCredits, conn.msgCacheCh)
			case f := <-flushTicker.C:
				conn.flushToClient()
				if f.Second() == 0 && f.Nanosecond() < int(common.FlushTimeout*2) { // Record every minute or so
					conn.cgCache.consumerM3Client.UpdateGauge(metrics.ConsConnectionScope, metrics.OutputhostCGMessageCacheSize, int64(len(conn.msgsCh)))
				}
			case updateUUID := <-conn.reconfigureClientCh:
				conn.logger.WithField(`updateUUID`, updateUUID).Debug(`reconfiguring client with updateUUID`)
				cmd := createReconfigureCmd(updateUUID)
				conn.writeToClient(cmd)
			case nMsg := <-conn.notifyCh:
				// we received some notification, check if we need to throttle
				if nMsg.notifyType == ThrottleUp {
					slowDown = throttler.GetNextSleepDuration()
					if throttler.IsCurrentSleepDurationAtMaximum() {
						conn.logger.WithField(`throttle`, float64(slowDown)/float64(time.Second)).
							Warn("throttle at maximum")
					}
				} else {
					// the connection is deemed good! reset the throttler
					if throttler.IsCurrentSleepDurationAtMaximum() {
						conn.logger.WithField(`throttle`, float64(defaultNoSleepDuration)/float64(time.Second)).
							Info("throttle recovered from maximum")
					}
					slowDown = throttler.ResetSleepDuration()
				}
			case <-conn.closeChannel:
				conn.stream.Done()
				return
			}
		}
	}
}

func (conn *consConnection) writeToClient(cmd *cherami.OutputHostCommand) error {

	if err := conn.stream.Write(cmd); err != nil {

		// any error here means the stream is done, close the stream
		conn.logger.
			WithField(`cmd-type`, cmd.GetType()).
			WithField(common.TagErr, err).
			Error("write msg failed")

		go conn.close() // teardown the connection on error
		return err
	}

	// Update outgoing metric counter
	conn.sentMsgs++

	conn.unflushedWrites++

	var flushImmediately = false

	if cmd.GetType() == cherami.OutputHostCommandType_MESSAGE {

		// update various metrics
		conn.cgCache.hostMetrics.Increment(load.HostMetricMsgsOut)
		conn.cgCache.cgMetrics.Increment(load.CGMetricMsgsOut)
		msgLength := len(cmd.GetMessage().GetPayload().GetData())
		conn.cgCache.hostMetrics.Add(load.HostMetricBytesOut, int64(msgLength))
		conn.cgCache.cgMetrics.Add(load.CGMetricBytesOut, int64(msgLength))

		conn.cgCache.m3Client.IncCounter(metrics.ConsConnectionStreamScope, metrics.OutputhostMessageSent)
		conn.cgCache.consumerM3Client.IncCounter(metrics.ConsConnectionScope, metrics.OutputhostCGMessageSent)

		conn.unflushedSize += int64(msgLength)

	} else {

		// flush out all non-'message' commands immediately
		flushImmediately = true
	}

	// flush if we have reached the threshold
	if flushImmediately || conn.unflushedWrites > common.FlushThreshold {
		return conn.flushToClient()
	}

	return nil
}

func (conn *consConnection) flushToClient() error {

	if conn.unflushedWrites > 0 { // no-op, if nothing to be done

		if err := conn.stream.Flush(); err != nil {

			conn.logger.WithFields(bark.Fields{
				`unflushedWrites`: conn.unflushedWrites,
				common.TagErr:     err,
			}).Error("flush failed")

			go conn.close() // teardown the connection
			return err
		}

		// update metrics only on flush
		conn.cgCache.consumerM3Client.AddCounter(metrics.ConsConnectionScope, metrics.OutputhostCGMessageSentBytes, conn.unflushedSize)

		conn.unflushedWrites, conn.unflushedSize = 0, 0
	}

	return nil
}

func (conn *consConnection) createMsgAndWriteToClientUtil(msg *cherami.ConsumerMessage, localCredits *int32, msgCacheCh chan<- cacheMsg) {
	cmd := createMsgCmd(msg)
	if err := conn.writeToClient(cmd); err == nil {
		*localCredits--
	}
	// Always add to the messageCache regardless of whether the write
	// to the client succeeded or not. If we don't add it to the
	// message cache, the message is *lost* and will never be redelivered.
	// This can cause the CG to be stuck forever waiting for an ACK from client.
	// The blocking operation below can delay the closing of stream when a
	// conn close signal is received, the assumption here is that the
	// operation if it blocks, will only do so for less than a second
	msgCacheCh <- cacheMsg{msg: msg, connID: conn.connID}
	conn.sentToMsgCache++
}

func createMsgCmd(msg *cherami.ConsumerMessage) *cherami.OutputHostCommand {
	cmd := cherami.NewOutputHostCommand()
	cmd.Message = msg
	cmd.Type = common.CheramiOutputHostCommandTypePtr(cherami.OutputHostCommandType_MESSAGE)

	return cmd
}

func createReconfigureCmd(updateUUID string) *cherami.OutputHostCommand {
	cmd := cherami.NewOutputHostCommand()
	cmd.Reconfigure = &cherami.ReconfigureInfo{
		UpdateUUID: common.StringPtr(updateUUID),
	}
	cmd.Type = common.CheramiOutputHostCommandTypePtr(cherami.OutputHostCommandType_RECONFIGURE)

	return cmd
}
