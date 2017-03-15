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

package storehost

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/storage"
	storeStream "github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/store"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
)

const inConnDebug = false

// inConnArgs are the arguments passed to inConn, including details on the
// extent to open and the tchannel-stream to use.
type inConnArgs struct {
	extentID uuid.UUID
	destID   uuid.UUID
	destType cherami.DestinationType
	mode     Mode // based on destType
}

type inConn struct {
	*inConnArgs // args passed to inConn

	stream   storeStream.BStoreOpenAppendStreamInCall // tchannel stream (to inputhost)
	xMgr     *ExtentManager
	m3Client metrics.Client
	log      bark.Logger

	stopC chan struct{}  // internal channel to trigger stoppage
	doneC chan error     // channel that notifies when we are done
	err   error          // used to remember any error encountered
	wg    sync.WaitGroup // internal waitgroup to wait to be done
}

func newInConn(args *inConnArgs, stream storeStream.BStoreOpenAppendStreamInCall, xMgr *ExtentManager, m3Client metrics.Client, log bark.Logger) *inConn {
	return &inConn{
		inConnArgs: args,
		stream:     stream,
		xMgr:       xMgr,
		m3Client:   m3Client,
		log:        log,
		doneC:      make(chan error),
		stopC:      make(chan struct{}),
	}
}

// Start initializes and starts this connection (asynchronously)
func (t *inConn) Start() {

	t.wg.Add(1) // for the go-routine below

	// start a go-routine that completes only when done
	go func() {

		t.wg.Add(1) // for the writeMessagesPump below

		// start the pump that reads in messages from msgC, writes to store, and sends acks to ackC.
		// NB: getAppendStreamChans creates incoming and outgoing chans and the corresponding pumps that
		// wrap go channels around the tchannel-stream interface.
		// (since we are already in a go-routine, call writeMessagesPump synchronously)

		switch t.mode {
		case TimerQueue:
			t.err = t.writeMessagesPumpTimerQueue(t.getAppendStreamChans(t.stream))

		case AppendOnly:
			fallthrough
		case Log:
			t.err = t.writeMessagesPumpAppendOnly(t.getAppendStreamChans(t.stream))
		}

		t.wg.Done() // for this go-routine

		t.wg.Wait()      // wait until all the (other) pumps are done
		t.doneC <- t.err // propagate error, if any, over doneC
		close(t.doneC)
	}()
}

// Stop initiates cleanup/closure of this connection; returns only when done
func (t *inConn) Stop() error {

	close(t.stopC) // should initiate the chain of closures

	t.wg.Wait() // wait for completion
	return t.err
}

// Done returns a channel that is notified  when connection is "done"
func (t *inConn) Done() <-chan error {
	return t.doneC // returns channel to wait on for completion
}

// inMessage contains the per-message context that is used to compute metrics
type inMessage struct {
	t0 time.Time // time the message was received
	*store.AppendMessage
}

func (t *inConn) newInMessage(msg *store.AppendMessage) *inMessage {

	t.m3Client.IncCounter(metrics.InConnScope, metrics.StorageMessageReceived)
	return &inMessage{
		t0:            time.Now(),
		AppendMessage: msg,
	}
}

type inMessageAck struct {
	t0 time.Time
	*store.AppendMessageAck
}

// newAck creates an 'ack' packet corresponding to the message
func (t *inMessage) newAck(status cherami.Status, addr int64) *inMessageAck {

	ack := store.NewAppendMessageAck()
	ack.SequenceNumber = common.Int64Ptr(t.GetSequenceNumber()) // include seq-num in ack
	ack.Status = common.CheramiStatusPtr(status)
	ack.Address = common.Int64Ptr(addr)

	return &inMessageAck{
		t0:               t.t0, // copy over time
		AppendMessageAck: ack,
	}
}

func (t *inConn) writeMessagesPumpAppendOnly(msgC <-chan *inMessage, ackC chan<- *inMessageAck) (err error) {

	defer t.wg.Done() // release wg ref
	defer close(ackC) // close outbound channel

	log := t.log // get "local" logger (that already contains extent info)

	log.Debug("writeMessagesPumpAppendOnly: started")

	var recvMsgs int64 // total messages written

	// open extent for write
	x, err := t.xMgr.OpenExtent(t.extentID, t.mode, OpenIntentAppendStream)

	if err != nil {
		log.WithFields(bark.Fields{
			"reason":      "OpenExtent error",
			common.TagErr: err,
			"recvMsgs":    recvMsgs,
		}).Error("writeMessagesPumpAppendOnly done")

		t.m3Client.IncCounter(metrics.InConnScope, metrics.StorageFailures)
		return newInternalServiceError(fmt.Sprintf("OpenExtent(%v) error: %v", t.extentID, err))
	}

	defer x.Close() // cleanup/close DB
	defer x.storeSync()

	var firstMsg = true            // flag to check if first message
	var lastMessageKey storage.Key // track the previous message's key to ensure that it strictly increasing

	// start ticker to check for extent seal. if the "seal" request comes in
	// when messages are being received, we will handle the seal request
	// there. the ticker is used for the case when the "seal" came in when
	// there are no messages coming in.
	sealCheckTicker := time.NewTicker(sealCheckInterval)
	defer sealCheckTicker.Stop()

	for {
		select {
		case msg, ok := <-msgC: // wait for new messages
			if !ok { // msgC closed
				log.WithFields(bark.Fields{
					"reason":   "read stream closed",
					"recvMsgs": recvMsgs,
				}).Info("writeMessagesPumpAppendOnly done")
				return nil
			}
			if msg.FullyReplicatedWatermark != nil && msg.Payload == nil { // fully replicated watermark
				// TODO: Handle watermark message
				t.m3Client.IncCounter(metrics.InConnScope, metrics.WatermarksReceived)
				break
			}
			msgSeqNum := msg.GetSequenceNumber()

			if msgSeqNum >= x.maxSeqNum() { // check if we are at max allowed seqnum

				ackC <- msg.newAck(cherami.Status_FAILED, -1) // send n-ack over channel

				log.WithFields(bark.Fields{
					"reason":    "reached max allowed seq-num",
					"maxSeqNum": x.maxSeqNum(),
					"recvMsgs":  recvMsgs,
				}).Error("writeMessagesPumpAppendOnly done")

				t.m3Client.IncCounter(metrics.InConnScope, metrics.StorageFailures)
				return newInternalServiceError(fmt.Sprintf("%v reached max allowed seq-num: %d", t.extentID, x.maxSeqNum()))
			}

			// compute message visibility time (based on the mode)
			visibilityTime := x.messageVisibilityTime(msg.AppendMessage)

			var key storage.Key      // key to write this messages against
			var val storage.Value    // message converted to byte-array
			var addr storage.Address // address for the message (returned by storage)

			// construct 64-bit key for the message (using callback that depends on "mode")
			key = x.constructKey(visibilityTime, msgSeqNum)

			// ensure that the keys are strictly increasing; this also ensures that we
			// never overwrite an existing message
			if key <= lastMessageKey {

				// FIXME: add metric to help alert this case
				log.WithFields(bark.Fields{
					"reason":   "msg key less than previous",
					"key":      key,
					"prev-key": lastMessageKey,
					"recvMsgs": recvMsgs,
				}).Error("writeMessagesPumpAppendOnly done")

				t.m3Client.IncCounter(metrics.InConnScope, metrics.StorageFailures)
				return newInternalServiceError(fmt.Sprintf("%v msg key=%x less than last-key=%x", t.extentID, key, lastMessageKey))
			}

			lastMessageKey = key

			if inConnDebug {
				log.WithFields(bark.Fields{ // #perfdisable
					"seq":   msgSeqNum,                                   // #perfdisable
					"enq":   msg.GetEnqueueTimeUtc(),                     // #perfdisable
					"delay": msg.GetPayload().GetDelayMessageInSeconds(), // #perfdisable
					"len":   len(msg.GetPayload().GetData()),             // #perfdisable
					"key":   key,                                         // #perfdisable
				}).Debug("writeMessagesPumpAppendOnly: recv msg") // #perfdisable
			}

			// serialize message into byte-array for storage
			val, err = serializeMessage(msg.AppendMessage)

			if err != nil { // we should never really see this error!

				ackC <- msg.newAck(cherami.Status_FAILED, -1) // send ack over channel

				// log.WithFields(log.Fields{
				// 	common.TagErr: err,
				// 	`key`:         key,
				// 	`msg`:         msg,
				// }).Fatal(`writeMessagesPumpAppendOnly: serializeMessage msg failed`) // Don't Panic.

				log.WithFields(bark.Fields{
					"reason":      "serialize-message error",
					"key":         key,
					common.TagErr: err,
					"recvMsgs":    recvMsgs,
				}).Error("writeMessagesPumpAppendOnly done")

				t.m3Client.IncCounter(metrics.InConnScope, metrics.StorageFailures)
				return newInternalServiceError(fmt.Sprintf("%v error serializing message (seqnum=%x key=%x): %v", t.extentID, msgSeqNum, key, err))
			}

			// get the extent lock, ensuring the extent will not get sealed, if it wasn't already!
			x.extentLock()

			// check if extent has been sealed
			if x.getSealSeqNum() != seqNumNotSealed {
				x.extentUnlock()

				log.WithFields(bark.Fields{
					"reason":      "extent sealed",
					"seal-seqnum": x.getSealSeqNum(),
					"recvMsgs":    recvMsgs,
				}).Info("writeMessagesPumpAppendOnly done")

				return newExtentSealedError(t.extentID, x.getSealSeqNum(), "extent sealed")
			}

			// if seqNum is not in expected (continuously increasing) order, log error as FYI
			if x.getLastSeqNum()+1 != msgSeqNum && x.getLastSeqNum() != SeqnumInvalid {
				// FIXME: add metric to help alert this case
				expectedSeqNum := 1 + x.getLastSeqNum()
				x.extentUnlock() // we only needed the lock held while reading lastSeqNum

				log.WithFields(bark.Fields{
					"reason":          "msg seqnum out of order",
					"msg-seqnum":      msg.GetSequenceNumber(),
					"expected-seqnum": expectedSeqNum,
					"recvMsgs":        recvMsgs,
				}).Error("writeMessagesPumpAppendOnly done")

				t.m3Client.IncCounter(metrics.InConnScope, metrics.StorageFailures)
				return newInternalServiceError(fmt.Sprintf("%v msg seqnum=%x out of order (expected=%x)", t.extentID, msg.GetSequenceNumber(), expectedSeqNum))
			}

			tWrite := time.Now()

			// write message to storage
			addr, err = x.storePut(key, val)

			t.m3Client.RecordTimer(metrics.InConnScope, metrics.StorageWriteStoreLatency, time.Since(tWrite))

			if err != nil {

				x.extentUnlock()

				// FIXME: this requires more thought/design on how to handle errors from low-level storage!
				ackC <- msg.newAck(cherami.Status_FAILED, -1) // send (n)ack over channel

				log.WithFields(bark.Fields{
					"reason":      "extent.Put error",
					common.TagErr: err,
					"key":         key,
					"len":         len(val),
					"recvMsgs":    recvMsgs,
				}).Error("writeMessagesPumpAppendOnly done")

				t.m3Client.IncCounter(metrics.InConnScope, metrics.StorageFailures)
				t.m3Client.IncCounter(metrics.InConnScope, metrics.StorageStoreFailures)
				return newInternalServiceError(fmt.Sprintf("%v error writing message to store (key=%x, val=%d bytes): %v", t.extentID, key, len(val), err))
			}

			recvMsgs++ // keep a count of the messages written during this "session"

			// if first msg written to this extent, update first-seqnum, etc
			if firstMsg {
				x.setFirstMsg(int64(key), msgSeqNum, visibilityTime)
				firstMsg = false
			}

			// update last-seqnum, etc (for SealExtent to use)
			x.setLastMsg(int64(key), msgSeqNum, visibilityTime)

			// safe to drop the sealExtent lock now, since we have written message to storage
			// and have updated the lastSeqNum thereby guaranteeing that any seal operation
			// would seal this extent at or greater than lastSeqNum.
			x.extentUnlock()

			ackC <- msg.newAck(cherami.Status_OK, int64(addr)) // send ack over channel

		case <-sealCheckTicker.C:
			// check at regular intervals if extent got sealed, and if so, close this connection
			if x.getSealSeqNum() != seqNumNotSealed {

				log.WithFields(bark.Fields{
					"reason":      "extent sealed",
					"seal-seqnum": x.getSealSeqNum(),
					"extra":       "seal-check ticker",
					"recvMsgs":    recvMsgs,
				}).Info("writeMessagesPumpAppendOnly done")

				return newExtentSealedError(t.extentID, x.getSealSeqNum(), "extent sealed")
			}

		case <-t.stopC: // we were asked to stop
			log.WithFields(bark.Fields{
				"reason":   "stopped",
				"recvMsgs": recvMsgs,
			}).Info("writeMessagesPumpAppendOnly done")
			return nil // get out of the pump
		}
	}
}

func (t *inConn) writeMessagesPumpTimerQueue(msgC <-chan *inMessage, ackC chan<- *inMessageAck) (err error) {

	defer t.wg.Done() // release wg ref
	defer close(ackC) // close outbound channel

	log := t.log // get "local" logger (that already contains extent info)

	log.Debug("writeMessagesPumpTimerQueue: started")

	var recvMsgs int64 // total messages written

	// open extent for write
	x, err := t.xMgr.OpenExtent(t.extentID, t.mode, OpenIntentAppendStream)

	if err != nil {
		log.WithFields(bark.Fields{
			"reason":      "OpenExtent error",
			common.TagErr: err,
			"recvMsgs":    recvMsgs,
		}).Error("writeMessagesPumpTimerQueue done")

		t.m3Client.IncCounter(metrics.InConnScope, metrics.StorageFailures)
		return newInternalServiceError(fmt.Sprintf("OpenExtent(%v) error: %v", t.extentID, err))
	}

	defer x.Close() // cleanup/close DB

	var firstMsg = true // flag to check if first message

	// start ticker to check for extent seal. if the "seal" request comes in
	// when messages are being received, we will handle the seal request
	// there. the ticker is used for the case when the "seal" came in when
	// there are no messages coming in.
	sealCheckTicker := time.NewTicker(sealCheckInterval)
	defer sealCheckTicker.Stop()

	for {
		select {
		case msg, ok := <-msgC: // wait for new messages
			if !ok { // msgC closed
				log.WithFields(bark.Fields{
					"reason":   "read stream closed",
					"recvMsgs": recvMsgs,
				}).Info("writeMessagesPumpTimerQueue done")
				return nil
			}

			if msg.FullyReplicatedWatermark != nil && msg.Payload == nil { // fully replicated watermark
				// TODO: Handle watermark message
				t.m3Client.IncCounter(metrics.InConnScope, metrics.WatermarksReceived)
				break
			}

			msgSeqNum := msg.GetSequenceNumber()

			if msgSeqNum >= x.maxSeqNum() { // check if we are at max allowed seqnum

				ackC <- msg.newAck(cherami.Status_FAILED, -1) // send n-ack over channel

				log.WithFields(bark.Fields{
					"reason":    "reached max allowed seq-num",
					"maxSeqNum": x.maxSeqNum(),
					"recvMsgs":  recvMsgs,
				}).Error("writeMessagesPumpTimerQueue done")

				t.m3Client.IncCounter(metrics.InConnScope, metrics.StorageFailures)
				return newInternalServiceError(fmt.Sprintf("%v reached max allowed seq-num: %d", t.extentID, x.maxSeqNum()))
			}

			// compute message visibility time (based on the mode)
			visibilityTime := x.messageVisibilityTime(msg.AppendMessage)

			var key storage.Key      // key to write this messages against
			var val storage.Value    // message converted to byte-array
			var addr storage.Address // address for the message (returned by storage)

			// construct 64-bit key for the message (using callback that depends on "mode")
			key = x.constructKey(visibilityTime, msgSeqNum)

			// ensure that the keys are strictly greater than that corresponding to the current time.
			// this ensures that we never insert into the "past" -- this ensures that a consumer that
			// is current, will not potentially miss this message (aka, avoid data loss).

			tNow := time.Now().UnixNano()

			if key <= x.constructKey(tNow, 0) {

				// FIXME: add metric to help alert this case
				log.WithFields(bark.Fields{
					"reason":       "msg key in the past",
					"key":          time.Unix(0, int64(key)),
					"now":          time.Unix(0, tNow),
					"msg-seqnum":   msg.GetSequenceNumber(),
					"msg-enq":      msg.GetEnqueueTimeUtc(),
					"msg-enq-time": time.Unix(0, msg.GetEnqueueTimeUtc()),
					"msg-delay":    msg.GetPayload().GetDelayMessageInSeconds(),
					"recvMsgs":     recvMsgs,
				}).Error("writeMessagesPumpTimerQueue done")

				t.m3Client.IncCounter(metrics.InConnScope, metrics.StorageFailures)
				return newInternalServiceError(fmt.Sprintf("%v msg key=%x less than now=%x", t.extentID, key, time.Now().UnixNano()))
			}

			if inConnDebug {
				log.WithFields(bark.Fields{ // #perfdisable
					"key":        time.Unix(0, int64(key)),                    // #perfdisable
					"msg-seqnum": msgSeqNum,                                   // #perfdisable
					"msg-enq":    msg.GetEnqueueTimeUtc(),                     // #perfdisable
					"msg-delay":  msg.GetPayload().GetDelayMessageInSeconds(), // #perfdisable
					"msg-len":    len(msg.GetPayload().GetData()),             // #perfdisable
				}).Debug("writeMessagesPumpTimerQueue recv msg") // #perfdisable
			}

			// serialize message into byte-array for storage
			val, err = serializeMessage(msg.AppendMessage)

			if err != nil { // we should never really see this error!

				ackC <- msg.newAck(cherami.Status_FAILED, -1) // send ack over channel

				log.WithFields(bark.Fields{
					"reason":      "serialize-message failed",
					common.TagErr: err,
					"recvMsgs":    recvMsgs,
				}).Error("writeMessagesPumpTimerQueue done")

				t.m3Client.IncCounter(metrics.InConnScope, metrics.StorageFailures)
				return newInternalServiceError(fmt.Sprintf("%v error serializing message (seqnum=%x key=%x): %v", t.extentID, msgSeqNum, key, err))
			}

			// get the extent lock, ensuring the extent will not get sealed, if it wasn't already!
			x.extentLock()

			// check if extent has been sealed
			if x.getSealSeqNum() != seqNumNotSealed {
				x.extentUnlock()

				log.WithFields(bark.Fields{
					"reason":     "extent sealed",
					"sealSeqNum": x.getSealSeqNum(),
					"recvMsgs":   recvMsgs,
				}).Info("writeMessagesPumpTimerQueue done")

				return newExtentSealedError(t.extentID, x.getSealSeqNum(), "extent sealed")
			}

			// if seqNum is not in expected (continuously increasing) order, log error as FYI
			if x.getLastSeqNum()+1 != msgSeqNum && x.getLastSeqNum() != SeqnumInvalid {
				// FIXME: add metric to help alert this case
				expectedSeqNum := 1 + x.getLastSeqNum()
				x.extentUnlock() // we only needed the lock held while reading lastSeqNum

				log.WithFields(bark.Fields{
					"reason":          "msg seqnum out of order",
					"msg-seqnum":      msg.GetSequenceNumber(),
					"expected-seqnum": expectedSeqNum,
					"recvMsgs":        recvMsgs,
				}).Error("writeMessagesPumpTimerQueue done")

				t.m3Client.IncCounter(metrics.InConnScope, metrics.StorageFailures)
				return newInternalServiceError(fmt.Sprintf("%v msg seqnum=%x out of order (expected=%x)", t.extentID, msg.GetSequenceNumber(), expectedSeqNum))
			}

			// tWrite := time.Now() // #perfdisable

			// write message to storage
			addr, err = x.storePut(key, val)

			// msg.m3Client.RecordTimer(metrics.InConnScope, metrics.StorageWriteStoreLatency, time.Since(tWrite)) // #perfdisable

			if err != nil {

				x.extentUnlock()

				// FIXME: this requires more thought/design on how to handle errors from low-level storage!
				ackC <- msg.newAck(cherami.Status_FAILED, -1) // send (n)ack over channel

				log.WithFields(bark.Fields{
					"error":       "extent.Put error",
					common.TagErr: err,
					"key":         key,
					"len":         len(val),
					"recvMsgs":    recvMsgs,
				}).Error("writeMessagesPumpTimerQueue done")

				t.m3Client.IncCounter(metrics.InConnScope, metrics.StorageFailures)
				t.m3Client.IncCounter(metrics.InConnScope, metrics.StorageStoreFailures)
				return newInternalServiceError(fmt.Sprintf("%v error writing message to store (key=%x, val=%d bytes): %v", t.extentID, key, len(val), err))
			}

			recvMsgs++ // keep a count of the messages written during this "session"

			// if first msg written to this extent, update first-seqnum, etc
			if firstMsg {
				x.setFirstMsg(int64(key), msgSeqNum, visibilityTime)
				firstMsg = false
			}

			// update lastSeqNum with this seqnum (for SealExtent to use)
			// update last-seqnum, etc (for SealExtent to use)
			x.setLastMsg(int64(key), msgSeqNum, visibilityTime)

			// safe to drop the sealExtent lock now, since we have written message to storage
			// and have updated the lastSeqNum thereby guaranteeing that any seal operation
			// would seal this extent at or greater than lastSeqNum.
			x.extentUnlock()

			ackC <- msg.newAck(cherami.Status_OK, int64(addr)) // send ack over channel

		case <-sealCheckTicker.C:
			// check at regular intervals if extent got sealed, and if so, close this connection
			if x.getSealSeqNum() != seqNumNotSealed {
				log.WithFields(bark.Fields{
					"reason":     "extent sealed",
					"sealSeqNum": x.getSealSeqNum(),
					"recvMsgs":   recvMsgs,
				}).Info("writeMessagesPumpTimerQueue done")

				return newExtentSealedError(t.extentID, x.getSealSeqNum(), "extent sealed")
			}

		case <-t.stopC: // we were asked to stop
			log.WithFields(bark.Fields{
				"reason":   "stopped",
				"recvMsgs": recvMsgs,
			}).Info("writeMessagesPumpTimerQueue done")
			return nil // get out of the pump
		}
	}
}

// getAppendStreamChans creates two pumps that wrap the tchannel-stream and provide
// "in" and "out" go-channels to read/write from it
func (t *inConn) getAppendStreamChans(stream storeStream.BStoreOpenAppendStreamInCall) (<-chan *inMessage, chan<- *inMessageAck) {

	// create buffered in/out channels
	msgC := make(chan *inMessage, writeMsgChanBuf)
	ackC := make(chan *inMessageAck, writeAckChanBuf)

	t.wg.Add(2)

	// start the send/recv pumps that wrap around the read/write interfaces
	// of the tchannel-stream, and send/recv over go-channels
	go t.sendPump(ackC, stream) // read from ackC, write to stream
	go t.recvPump(stream, msgC) // read from stream, write to msgC

	return msgC, ackC
}

// recvPump reads from the stream and send out over recvC
func (t *inConn) recvPump(stream storeStream.BStoreOpenAppendStreamInCall, recvC chan<- *inMessage) {

	defer t.wg.Done()
	defer close(recvC) // close outbound channel

	log := t.log // get "local" logger (that already contains extent info)

pump:
	for {
		// read (blocking) from tchannel-stream
		msg, err := stream.Read()

		if err != nil {
			log.WithField(common.TagErr, err).Error(`inConn.recvPump: AppendMessage stream closed`)
			break pump
		}

		if inConnDebug {
			log.WithFields(bark.Fields{ // #perfdisable
				common.TagSeq: msg.GetSequenceNumber(),                        // #perfdisable
				`enq`:         strconv.FormatInt(msg.GetEnqueueTimeUtc(), 16), // #perfdisable
				`delay`:       msg.GetPayload().GetDelayMessageInSeconds(),    // #perfdisable
			}).Debug(`inConn.recvPump: recv msg`) // #perfdisable
		}

		recvC <- t.newInMessage(msg) // send to go-channel
	}
}

// sendPump reads from 'sendC' and writes out to 'stream'
func (t *inConn) sendPump(sendC <-chan *inMessageAck, stream storeStream.BStoreOpenAppendStreamInCall) {

	defer t.wg.Done()   // drop ref on waitgroup
	defer stream.Done() // close outbound stream

	var unflushedWrites int                            // count of writes since the last flush (FIXME: make it "size" based?)
	flushTicker := time.NewTicker(common.FlushTimeout) // start ticker to flush tchannel stream
	defer flushTicker.Stop()

	log := t.log // get "local" logger (that already contains extent info)

pump:
	for {
		select {
		case ack, ok := <-sendC: // read from the go-channel

			if !ok {
				if inConnDebug {
					log.Debug(`incConn.sendPump: sendC closed`) // #perfdisable
				}
				break pump
			}

			t.m3Client.RecordTimer(metrics.InConnScope, metrics.StorageWriteMessageBeforeAckLatency, time.Since(ack.t0))

			// write out (blocking) to stream
			if err := stream.Write(ack.AppendMessageAck); err != nil {
				log.WithField(common.TagErr, err).Error(`inConn.sendPump: stream.Write error`)
				break pump
			}

			t.m3Client.RecordTimer(metrics.InConnScope, metrics.StorageWriteMessageLatency, time.Since(ack.t0))

			if inConnDebug {
				log.WithFields(bark.Fields{ // #perfdisable
					common.TagSeq: ack.GetSequenceNumber(),                 // #perfdisable
					`addr`:        strconv.FormatInt(ack.GetAddress(), 16), // #perfdisable
					`status`:      ack.GetStatus(),                         // #perfdisable
				}).Debug(`inConn.sendPump: sent ack`) // #perfdisable
			}

			if unflushedWrites++; unflushedWrites >= common.FlushThreshold {

				if err := stream.Flush(); err != nil {
					log.WithField(common.TagErr, err).Error(`inConn.sendPump: <threshold> stream.Flush error`)
					break pump
				}

				if inConnDebug {
					log.WithFields(bark.Fields{ // #perfdisable
						`unflushed-writes`: unflushedWrites,
					}).Debug(`inConn.sendPump: <threshold> flushed ack stream`) // #perfdisable
				}

				unflushedWrites = 0
			}

		case <-flushTicker.C: // the flush "ticker" has fired

			if unflushedWrites > 0 {

				if err := stream.Flush(); err != nil {
					log.WithField(common.TagErr, err).Error(`inConn.sendPump: <ticker> stream.Flush error`)
					break pump
				}

				if inConnDebug {
					log.WithFields(bark.Fields{ // #perfdisable
						`unflushed-writes`: unflushedWrites,
					}).Debug(`inConn.sendPump: <ticker> flushed ack stream`) // #perfdisable
				}

				unflushedWrites = 0
			}
		}
	}
}
