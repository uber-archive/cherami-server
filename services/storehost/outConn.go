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
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/storage"
	storeStream "github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

const outConnDebug = false

// outConnArgs are the arguments passed to outConn, including details on the
// extent to open and the tchannel-stream to use.
type outConnArgs struct {
	extentID    uuid.UUID
	destID      uuid.UUID
	destType    cherami.DestinationType
	mode        Mode // based on destType
	consGroupID uuid.UUID
	address     int64 // address to start reading from
	inclusive   bool  // -> start at/after address
}

type outConn struct {
	*outConnArgs // args passed to outConn
	credits      credits

	stream   storeStream.BStoreOpenReadStreamInCall // tchannel stream (to outputhost)
	xMgr     *ExtentManager
	m3Client metrics.Client
	log      bark.Logger

	stopC chan struct{}  // internal channel to trigger stoppage
	doneC chan struct{}  // channel that we notify when done
	wg    sync.WaitGroup // internal waitgroup to wait to be done
}

type outConnExtentContext struct {
	extObj  *ExtentObj
	outConn *outConn
	addr    storage.Address // 'addr': address of last message sent
	next    storage.Address // 'next': address of next message to send
	nextKey storage.Key     // 'nextKey': "key" of the next message
	notifyC chan struct{}
}

func openExtent(outConn *outConn) (*outConnExtentContext, error) {
	ext := &outConnExtentContext{
		outConn: outConn,
	}

	// open extent for read
	var err error
	ext.extObj, err = outConn.xMgr.OpenExtent(outConn.extentID, outConn.mode, OpenIntentReadStream)

	if err != nil {
		return nil, fmt.Errorf("ExtentNotFoundError: %v OpenExtent error: %v", outConn.extentID, err)
	}

	// create and register a notification channel to listen on for any new messages on this extent.
	// use a buffer of '1' to ensure we have the notification queued up, in case we weren't listening.
	ext.notifyC = make(chan struct{}, 1)
	ext.extObj.listen(ext.notifyC)

	if !outConn.inclusive {

		ext.addr = storage.Address(outConn.address)

		// do a 'SeekCeil' of addr+1 to find the address to start from
		if ext.next, ext.nextKey, err = ext.extObj.storeNext(ext.addr); err != nil {
			return nil, fmt.Errorf("InternalServiceError: %v Next(%x) error: %v", outConn.extentID, ext.addr, err)
		}

	} else {

		ext.addr = storage.Address(outConn.address - 1) // FIXME: this should ideally be initialized to x.storePrev(next)

		ext.next = storage.Address(outConn.address)
		ext.nextKey, err = ext.extObj.storeGetKey(ext.next)

		if err != nil {
			return nil, fmt.Errorf("InvalidAddressError: %v GetKey(%x) error: %v", outConn.extentID, ext.next, err)
		}
	}
	return ext, nil
}

func (t *outConnExtentContext) Close() {
	t.extObj.Close() // cleanup/close extent (unlistens automatically)
}

type credits int32

func (t *credits) available() bool {
	return atomic.LoadInt32((*int32)(t)) > 0
}

func (t *credits) closed() bool {
	return atomic.LoadInt32((*int32)(t)) == -1
}

// Called from a goroutine that processes control messages coming from output connection
func (t *credits) add(credits int32) {
	atomic.AddInt32((*int32)(t), credits)
}

func (t *credits) close() {
	// set cache to '-1' before leaving, to indicate this was closed
	atomic.StoreInt32((*int32)(t), -1)
}

// Decrement credit count by 1
func (t *credits) use() (closed bool, err error) {
	credits := atomic.AddInt32((*int32)(t), -1)
	switch {
	case credits >= 0:
		return false, nil

	case credits == -1:
		atomic.StoreInt32((*int32)(t), 0) // reset back to 0
		return true, fmt.Errorf("use called with no credits")

	case credits < -1:
		atomic.StoreInt32((*int32)(t), -1) // reset back to -1
		return true, nil
	default:
		return true, fmt.Errorf("unexpected")
	}
}

func (t *credits) String() string {
	return strconv.FormatUint(uint64(atomic.LoadInt32((*int32)(t))), 10)
}

func newOutConn(args *outConnArgs, stream storeStream.BStoreOpenReadStreamInCall, xMgr *ExtentManager, m3Client metrics.Client, log bark.Logger) *outConn {

	return &outConn{
		outConnArgs: args,
		stream:      stream,
		xMgr:        xMgr,
		m3Client:    m3Client,
		log:         log,
		doneC:       make(chan struct{}),
		stopC:       make(chan struct{}),
	}
}

// Start: starts this connection (asynchronously)
func (t *outConn) Start() {

	t.wg.Add(1) // for the go-routine below

	// start a go-routine that completes only when done
	go func() {

		t.wg.Add(1) // for the readMessages* pump below

		// start the pump that reads in credits from credC, reads messages from store, and sends them over msgC
		// NB: getReadStreamChans creates incoming and outgoing chans, and the corresponding pumps that wrap
		// go channels around the tchannel-stream interface.
		t.readMessagesPump(t.getReadStreamChans(t.stream))

		t.wg.Done() // for this go-routine

		t.wg.Wait() // wait until all the pumps are done
		close(t.doneC)
	}()
}

// Stop: initiates cleanup/closure of this connection; returns only when done
func (t *outConn) Stop() {

	close(t.stopC) // should initiate the chain of closures

	<-t.doneC // wait for completion
}

func (t *outConn) Done() <-chan struct{} {
	return t.doneC // returns channel to wait on for completion
}

// outMessage contains the per-message context that is used to compute metrics
type outMessage struct {
	t0 time.Time // time the message was created
	*store.ReadMessageContent
}

// newOutMessage initializes a new message with the timestamp
func (t *outConn) newOutMessage() *outMessage {

	t.m3Client.IncCounter(metrics.OutConnScope, metrics.StorageMessageSent)
	return &outMessage{
		t0: time.Now(), // note the time
	}
}

// read indicates when the message has been read from low-level storage
func (t *outMessage) read(addr storage.Address, payload *store.AppendMessage) *outMessage {

	t.ReadMessageContent = newReadMessageContent(addr, payload)
	return t
}

func newOutMessageSealed(extentID uuid.UUID, seqnum int64) *outMessage {

	return &outMessage{
		t0:                 time.Now(),
		ReadMessageContent: newReadMessageContentSealed(extentID, seqnum),
	}
}

func newOutMessageError(err string) *outMessage {

	return &outMessage{
		t0:                 time.Now(),
		ReadMessageContent: newReadMessageContentError(err),
	}
}

type gate interface {
	setEoxReached()
	setNextKey(nextKey storage.Key) // unsets EOX flag
	beforeSleep() <-chan struct{}
	engaged() bool // returns true when gate is engaged
	close()
	String() string
}

type timeGate struct { // Implements gate
	ext               *outConnExtentContext
	tNext, tNow, tEnd int64       // time (in 'UnixNano' units) for next, (last) now and end
	timer             *time.Timer // timer used to wake us up when the next message is ready to deliver
	gateC             chan struct{}
	closeC            chan struct{}
}

func newTimeGate(ext *outConnExtentContext) gate {

	tNow := time.Now()

	// setup timeGate with timer set to fire at the 'end of time'
	t := &timeGate{
		ext:    ext,
		tNow:   tNow.UnixNano(),
		tEnd:   math.MaxInt64,
		gateC:  make(chan struct{}),
		closeC: make(chan struct{}),
		timer:  time.NewTimer(time.Unix(0, math.MaxInt64).Sub(tNow)),
	}

	// "Cast" chan Time to chan struct{}.
	// Unfortunately go doesn't have a common channel supertype.
	go func() {
		defer close(t.gateC)
		defer t.timer.Stop()
	loop:
		for {
			select {
			case <-t.timer.C:
				t.gateC <- struct{}{} // re-transmit on gateC
			case <-t.closeC: // closed; cleanup and quit
				break loop
			}
		}
	}()

	return t
}

func (t *timeGate) setEoxReached() {
	t.tNext = t.tEnd
}

func (t *timeGate) beforeSleep() <-chan struct{} {
	if t.engaged() && t.tNext != t.tEnd {
		// reset timer to fire when the next message should be made 'visible'
		tNow := time.Now()
		t.tNow = tNow.UnixNano()
		t.timer.Reset(time.Unix(0, t.tNext).Sub(tNow))
	}
	return t.gateC
}

func (t *timeGate) engaged() bool {
	t.tNow = time.Now().UnixNano()
	return t.tNext > t.tNow
}

func (t *timeGate) setNextKey(nextKey storage.Key) {
	visibilityTime, _ := t.ext.extObj.deconstructKey(nextKey) // extract delivery timestamp
	t.tNext = visibilityTime
}

func (t *timeGate) close() {
	close(t.closeC)
}

func (t *timeGate) String() string {
	return fmt.Sprintf("timeGate [engaged=%v eox=%v tNext=%x tNow=%x]", t.engaged(), t.tNext == t.tEnd, t.tNext, t.tNow)
}

// Gate that is never engaged. Used for append only mode which doesn't need gating.
type noopGate struct{} // Implements gate

func newNoopGate(ext *outConnExtentContext) gate {
	return noopGate{}
}

func (t noopGate) setEoxReached() {}

func (t noopGate) engaged() bool {
	return false
}

func (t noopGate) setNextKey(nextKey storage.Key) {}

func (t noopGate) beforeSleep() <-chan struct{} {
	return nil
}

func (t noopGate) close() {}

func (t noopGate) String() string {
	return "noopGate [engaged=false]"
}

func newGate(mode Mode, ext *outConnExtentContext) gate {
	switch mode {
	case TimerQueue:
		return newTimeGate(ext)

	case AppendOnly:
		return newNoopGate(ext)

	case Log:
		// TODO: Implement gating on fully replicated watermark
		return newNoopGate(ext)

	default:
		return nil // keeps compiler happy
	}
}

// Start the pump that reads messages and sends back to the outputhost
func (t *outConn) readMessagesPump(credC <-chan struct{}, msgC chan<- *outMessage) {
	defer t.wg.Done() // release waitgroup ref
	defer close(msgC) // close outbound channel

	log := t.log // get "local" logger (that already contains extent info)

	if outConnDebug {
		log.Debug("readMessagesPump: started")
	}

	var sentMsgs int64

	ext, err := openExtent(t)

	if err != nil {
		log.WithFields(bark.Fields{
			`reason`:      `OpenExtent error`,
			common.TagErr: err,
			`sentMsgs`:    sentMsgs,
		}).Error(`readMessagesPump done`)
		msgC <- newOutMessageError(fmt.Sprintf("ExtentNotFoundError: %v OpenExtent error: %v", t.extentID, err))
		return
	}

	defer ext.Close() // cleanup/close extent (unlistens automatically)

	gateVal := newGate(t.mode, ext)

	defer gateVal.close()

	if ext.next == storage.EOX {

		gateVal.setEoxReached()

	} else {

		gateVal.setNextKey(ext.nextKey)

		if ext.extObj.isSealExtentKey(ext.nextKey) {
			// check if we found the seal marker
			log.WithFields(bark.Fields{
				`reason`:   `found seal-key`,
				`nextKey`:  ext.nextKey,
				`sentMsgs`: sentMsgs,
			}).Info("readMessagesPump done")
			msgC <- newOutMessageSealed(t.extentID, ext.extObj.deconstructSealExtentKey(ext.nextKey))
			return
		}
	}

	var wakeup wakeupReason
	var sleep sleepReason

	var spuriousWakeups stats
	var batchedMessages stats

	wakeupReasons := make(map[wakeupReason]int)
	sleepReasons := make(map[sleepReason]int)

	if outConnDebug {
		log.WithFields(bark.Fields{ // #perfdisable
			`addr`:    ext.addr,    // #perfdisable
			`next`:    ext.next,    // #perfdisable
			`nextKey`: ext.nextKey, // #perfdisable
			`gate`:    gateVal,     // #perfdisable
		}).Debug(`readMessagesPump: starting message loop`) // #perfdisable
	}

msgPump:
	for {
		wakeup = wakeupNone
		spurious := 0 // spurious wakeup

		// wait until we have credits and at least one message ready to be delivered
		for !t.credits.available() || ext.next == storage.EOX || gateVal.engaged() {

			if outConnDebug {
				// DBG: collect stats on sleep/wakeup reasons
				if wakeup != wakeupNone {
					spurious++
				}

				if t.credits.available() {
					sleep = 0
				} else {
					sleep = sleepNoCredits
				}

				if ext.next == storage.EOX {
					sleep |= sleepNoMessages
				} else if gateVal.engaged() {
					sleep |= sleepGate
				}

				sleepReasons[sleep]++

				log.WithFields(bark.Fields{ // #perfdisable
					`sleep`:   sleep,     // #perfdisable
					`gate`:    gateVal,   // #perfdisable
					`credits`: t.credits, // #perfdisable
					`next`:    ext.next,  // #perfdisable
				}).Debug(`readMessagesPump: sleeping`) // #perfdisable
			}

			gateC := gateVal.beforeSleep()

			// wait until one of four things occurs:
			// 1. new credits to come in
			// 2. we get notified of a new message
			// 3. the timer fires (message scheduled to be delivered)
			// 4. shutdown was triggered
			//
			// for 1, 2, 3 we would re-query the store to find out the current
			// 'next' message; if that precedes the old 'next' message we would
			// reset the timer appropriately, and recheck (top of the loop)
			// whether we are ready to deliver any messages.
			// for 4 (or in 3, if we detect the credC is closed), we would quit
			// the loop and close the outgoing channel to initiate cleanup.

			// TODO(maxim): Add watermark wakeup
			select {
			case <-gateC: // message is ready for delivery

				if outConnDebug {
					wakeup = wakeupGate
					log.WithField(`Now`, time.Now()).Debug(`readMessagesPump: timer fired`) // #perfdisable
				}

			case <-ext.notifyC: // new message published to the extent

				if outConnDebug {
					wakeup = wakeupMessage
					log.Debug("readMessagesPump: new message notification") // #perfdisable
				}

			case <-credC: // cha-ching! new credits ..

				if outConnDebug {
					wakeup = wakeupCredits
					log.Debug("readMessagesPump: credits updated notification") // #perfdisable
				}

				if t.credits.closed() {
					log.WithFields(bark.Fields{
						`reason`:   `credit stream closed`,
						`sentMsgs`: sentMsgs,
					}).Info("readMessagesPump done")

					if outConnDebug {
						wakeup = wakeupClose
					}

					break msgPump // credC closed; break out
				}

			case <-t.stopC: // stop triggered

				if outConnDebug {
					wakeup = wakeupStop
					wakeupReasons[wakeup]++
				}

				log.WithFields(bark.Fields{
					`reason`:   `stop triggered`,
					`sentMsgs`: sentMsgs,
				}).Info("readMessagesPump done")
				break msgPump
			}

			if outConnDebug {
				log.WithFields(bark.Fields{ // #perfdisable
					`wakeup`: wakeup, // #perfdisable
				}).Debug(`readMessagesPump: woken up`) // #perfdisable
			}

			// find the new 'next' message to be delivered, and check to see if we need to update our state
			if newNext, newNextKey, e := ext.extObj.storeNext(ext.addr); e == nil {

				if outConnDebug {
					log.WithFields(bark.Fields{ // #perfdisable
						`addr`:       ext.addr,   // #perfdisable
						`next`:       ext.next,   // #perfdisable
						`newNext`:    newNext,    // #perfdisable
						`newNextKey`: newNextKey, // #perfdisable
					}).Debug(`readMessagesPump: store next`) // #perfdisable
				}

				if ext.extObj.isSealExtentKey(newNextKey) { // check if we found the seal marker

					log.WithFields(bark.Fields{
						`reason`:   `sealed`,
						`sealKey`:  newNextKey,
						`sentMsgs`: sentMsgs,
					}).Info("readMessagesPump done")

					msgC <- newOutMessageSealed(t.extentID, ext.extObj.deconstructSealExtentKey(newNextKey))
					break msgPump
				}

				if newNext != storage.EOX {

					// if the 'newNext' is earlier than 'next', update 'next'
					if (ext.next == storage.EOX) || (newNext < ext.next) {

						ext.next = newNext
						gateVal.setNextKey(newNextKey)
						// TODO: NO GATING gate
						//} else {
						//	// TODO: decide if avoiding call to deconstructKey is worth not checking sequenceNumber here.
						//	tNext++
						//}
					}

				} else {
					// assert( next == storage.EOX ) //
				}

			} else {
				msgC <- newOutMessageError(fmt.Sprintf("InternalServiceError: %v store.Next(addr=%x) error: %v", t.extentID, ext.addr, e))

				log.WithFields(bark.Fields{
					`reason`:      "ext.Next failed",
					common.TagErr: err,
					`sentMsgs`:    sentMsgs,
				}).Info("readMessagesPump done")

				break msgPump
			}
		}

		if outConnDebug {
			spuriousWakeups.put(float64(spurious))
			wakeupReasons[wakeup]++
		}

		// when we get out of the loop above, we have at least one message ready to deliver, and
		// at least have one credit available.  NB: 'next', 'tNext' and 'tNow' should be current here.

		var batchSent int

		// send out as many messages as we can, that are ready to deliver
		for t.credits.available() && ext.next != storage.EOX && !gateVal.engaged() {

			msg := t.newOutMessage()

			ext.addr = ext.next

			if outConnDebug {
				log.WithFields(bark.Fields{ // #perfdisable
					`addr`:    ext.next,  // #perfdisable
					`gate`:    gateVal,   // #perfdisable
					`credits`: t.credits, // #perfdisable
				}).Debug(`readMessagesPump: send-msg loop`) // #perfdisable
			}

			t0 := time.Now() // start timer to compute read-store latency

			var val storage.Value
			_, val, ext.next, ext.nextKey, err = ext.extObj.storeGet(ext.addr)

			if err != nil { // read error?

				// retry, in case the error was because we were racing with a
				// 'PurgeMessages' call, and if so push the 'cursor' to the
				// first available message.

				// find next of 'addr - 1' and check if we get an address that's not 'addr'
				if ext.next, ext.nextKey, err = ext.extObj.storeNext(ext.addr - 1); err != nil {
					log.WithFields(bark.Fields{
						`reason`:      "ext.Next error",
						common.TagErr: err,
						`addr`:        ext.addr - 1,
						`extra`:       `[purge race]`,
						"sentMsgs":    sentMsgs,
					}).Info("readMessagesPump done")
					msgC <- newOutMessageError(fmt.Sprintf("InternalServiceError: %v store.Next(addr=%x) error: %v", t.extentID, ext.addr-1, err))
					break msgPump
				}

				if ext.next != ext.addr { // this indicates that perhaps some messages were purged, so retry

					// check if we found a sealExtentKey; if so close stream
					if ext.next != storage.EOX {

						if ext.extObj.isSealExtentKey(ext.nextKey) {
							log.WithFields(bark.Fields{
								`reason`:   `sealed`,
								`sealKey`:  ext.nextKey,
								`extra`:    `[purge race]`,
								`sentMsgs`: sentMsgs,
							}).Info("readMessagesPump done")
							msgC <- newOutMessageSealed(t.extentID, ext.extObj.deconstructSealExtentKey(ext.nextKey))
							break msgPump
						}

						gateVal.setNextKey(ext.nextKey)
					} else {
						gateVal.setEoxReached()
					}

					continue // retry the Get
				}

				// FIXME: requires more thought/design on how to handle errors from low-level storage
				log.WithFields(bark.Fields{
					`reason`:      `ext.Get error`,
					common.TagErr: err,
					`addr`:        ext.addr,
					`extra`:       `[purge race]`,
					`sentMsgs`:    sentMsgs,
				}).Error(`readMessagesPump done`)
				msgC <- newOutMessageError(fmt.Sprintf("InternalServiceError: %v store.Get(addr=%x) error: %v", t.extentID, ext.addr, err))
				break msgPump
			}

			t.m3Client.RecordTimer(metrics.OutConnScope, metrics.StorageReadStoreLatency, time.Since(t0)) // compute read store metrics

			var appMsg *store.AppendMessage
			appMsg, err = deserializeMessage(val)

			if err != nil { // corrupt message?
				log.WithFields(bark.Fields{
					`reason`:      `deserializeMessage error`,
					common.TagErr: err,
					`addr`:        ext.addr,
					`len`:         len(val),
					`sentMsgs`:    sentMsgs,
				}).Error("readMessagesPump done")
				msgC <- newOutMessageError(fmt.Sprintf("InternalServiceError: %v error deserializing message (addr=%x, len=%d bytes): %v", t.extentID, ext.addr, len(val), err))
				break msgPump
			}

			select {
			case msgC <- msg.read(ext.addr, appMsg): // (try) send out message

			default:
				// if the channel is blocked (ie, it is full), wait until some
				// messages are drained out or until the connection is closed
			retry:
				for {
					select {
					case msgC <- msg.read(ext.addr, appMsg): // (try) send out message
						break retry

					case _, ok := <-credC: // if the stream is closed, break out
						if !ok {
							log.WithFields(bark.Fields{
								`reason`:   "credits closed (msg send retry loop)",
								`sentMsgs`: sentMsgs,
							}).Error("readMessagesPump done")
							break msgPump
						}
					}
				}
			}

			// FIXME: temporary check/log to help root-cause misfiring of timer messages
			if t.mode == TimerQueue {

				tFired := time.Now().UnixNano()
				tETA := appMsg.GetEnqueueTimeUtc() + (int64(appMsg.GetPayload().GetDelayMessageInSeconds()) * int64(time.Second))

				if (tETA - tFired) >= int64(time.Second) {

					log.WithFields(bark.Fields{
						`message`: fmt.Sprintf("seqnum=%d enq=%x delay=%ds", appMsg.GetSequenceNumber(), appMsg.GetEnqueueTimeUtc(), appMsg.GetPayload().GetDelayMessageInSeconds()),
						`address`: fmt.Sprintf("addr=%v next=%v nextKey=%v", ext.addr, ext.next, ext.nextKey),
						`gate`:    fmt.Sprintf("credits=%v eox=%v gate=%v", t.credits, ext.next == storage.EOX, gateVal),
						`context`: fmt.Sprintf("sentMsgs=%d sleepReason=%v wakeupReason=%v spuriousWakeups=%d batchSent=%d", sentMsgs, sleep, wakeup, spurious, batchSent),
						`error`:   fmt.Sprintf("timer misfired: ETA=%v > NOW=%v (tETA=%x tFired=%x)", time.Unix(0, tETA), time.Unix(0, tFired), tETA, tFired),
					}).Error("readMessagesPumpTimerQueue: message delivered before ETA")
				}
			}

			// FIXME: change currency to be proportional to 'message size'
			if closed, useErr := t.credits.use(); useErr != nil {

				msgC <- newOutMessageError(fmt.Sprintf("InternalServiceError: %v", useErr))
				log.WithFields(bark.Fields{
					`reason`:      "error using credits",
					common.TagErr: useErr,
					"sentMsgs":    sentMsgs,
				}).Error("readMessagesPump done")
				break msgPump

			} else if closed {

				log.WithFields(bark.Fields{
					`reason`:   "credits closed",
					`sentMsgs`: sentMsgs,
				}).Error("readMessagesPump done")
				break msgPump
			}

			sentMsgs++ // track total messages sent

			if outConnDebug {
				batchSent++
			}

			if outConnDebug {
				log.WithFields(bark.Fields{ // #perfdisable
					`addr`:      ext.addr,  // #perfdisable
					`next`:      ext.next,  // #perfdisable
					`val-bytes`: len(val),  // #perfdisable
					`credits`:   t.credits, // #perfdisable
				}).Debug(`readMessagesPump: sent message`) // #perfdisable
			}

			if ext.next == storage.EOX {
				gateVal.setEoxReached()
				break
			}

			// check if we found the seal marker; if so close stream
			if ext.extObj.isSealExtentKey(ext.nextKey) {

				msgC <- newOutMessageSealed(t.extentID, ext.extObj.deconstructSealExtentKey(ext.nextKey))

				log.WithFields(bark.Fields{
					`reason`:   "sealed",
					`sealKey`:  ext.nextKey,
					`sentMsgs`: sentMsgs,
				}).Info("readMessagesPump done")

				break msgPump
			}

			gateVal.setNextKey(ext.nextKey)
		}

		if outConnDebug {
			batchedMessages.put(float64(batchSent))
		}
	}

	log.WithFields(bark.Fields{
		`wakeupReasons`:   wakeupReasons,
		`sleepReasons`:    sleepReasons,
		`spuriousWakeups`: spuriousWakeups,
		`batchedMessages`: batchedMessages,
	}).Info(`readMessagesPump: stats`)

	return
}

// getReadStreamChans creates two pumps that wrap the tchannel-stream and provide
// "in" and "out" go-channels to read/write from it
func (t *outConn) getReadStreamChans(stream storeStream.BStoreOpenReadStreamInCall) (<-chan struct{}, chan<- *outMessage) {

	// create buffered in/out channels
	credC := make(chan struct{}, 1) // buffer of one, since this is just a notification channel
	msgC := make(chan *outMessage, writeMsgChanBuf)

	t.wg.Add(2)

	// start the send/recv pumps

	go t.sendPump(msgC, stream)
	go t.recvPump(stream, credC)

	return credC, msgC
}

// recvPump reads from the stream and send out over credC
func (t *outConn) recvPump(stream storeStream.BStoreOpenReadStreamInCall, credC chan<- struct{}) {
	log := t.log // get "local" logger (that already contains extent info)

	defer t.wg.Done()
	defer t.credits.close()
	defer close(credC)

pump:
	for {
		cf, err := stream.Read()

		if err != nil {
			if outConnDebug {
				log.WithField(common.TagErr, err).Debug(`outConn.recvPump: stream.Read error`)
			}
			break pump
		}

		t.credits.add(cf.GetCredits())

		// do a non-blocking notify on the new credits
		select {
		case credC <- struct{}{}:
		default:
		}

		if outConnDebug {
			log.WithFields(bark.Fields{ // #perfdisable
				`received`: cf.GetCredits(), // #perfdisable
				`credits`:  t.credits,       // #perfdisable
			}).Debug(`outConn.recvPump: credits updated`) // #perfdisable
		}
	}
}

// sendPump reads from 'msgC' and writes out to stream
func (t *outConn) sendPump(msgC <-chan *outMessage, stream storeStream.BStoreOpenReadStreamInCall) {
	defer t.wg.Done()   // drop ref on waitgroup
	defer stream.Done() // close "out" stream

	var unflushedWrites int                            // count of writes since the last flush (FIXME: make it "size" based?)
	flushTicker := time.NewTicker(common.FlushTimeout) // start ticker to flush tchannel stream
	defer flushTicker.Stop()

	log := t.log // get "local" logger (that already contains extent info)

pump:
	for {
		select {
		case msg, ok := <-msgC: // read from the go-channel

			if !ok {
				if outConnDebug {
					log.Debug(`outConn.sendPump: msgC closed`) // #perfdisable
				}
				break pump
			}

			// write out (blocking) to stream
			if err := stream.Write(msg.ReadMessageContent); err != nil {
				if outConnDebug {
					log.WithField(common.TagErr, err).Debug(`outConn.sendPump: stream.Write error`)
				}
				break pump
			}

			t.m3Client.RecordTimer(metrics.OutConnScope, metrics.StorageReadMessageLatency, time.Since(msg.t0))

			if outConnDebug {
				switch msg.GetType() { // #perfdisable
				case store.ReadMessageContentType_MESSAGE: // #perfdisable
					log.WithFields(bark.Fields{ // #perfdisable
						common.TagSeq: msg.GetMessage().GetMessage().GetSequenceNumber(),    // #perfdisable
						`addr`:        strconv.FormatInt(msg.GetMessage().GetAddress(), 16), // #perfdisable
					}).Debug(`outConn.sendPump: sent msg`) // #perfdisable
				case store.ReadMessageContentType_SEALED: // #perfdisable
					log.WithFields(bark.Fields{ // #perfdisable
						`sealSeqNum`: msg.GetSealed().GetSequenceNumber(), // #perfdisable
					}).Debug(`outConn.sendPump: sent msg sealed`) // #perfdisable
				case store.ReadMessageContentType_ERROR: // #perfdisable
					log.WithFields(bark.Fields{ // #perfdisable
						`error`: msg.GetError().GetMessage(), // #perfdisable
					}).Debug(`outConn.sendPump: sent msg error`) // #perfdisable
				} // #perfdisable
			}

			if unflushedWrites++; unflushedWrites >= common.FlushThreshold {

				if err := stream.Flush(); err != nil {
					log.WithField(common.TagErr, err).Error(`outConn.sendPump: <threshold> stream.Flush error`)
					break pump
				}

				if outConnDebug {
					log.WithFields(bark.Fields{ // #perfdisable
						`unflushed-writes`: unflushedWrites,
					}).Debug(`outConn.sendPump: <threshold> flushed ack stream`) // #perfdisable
				}

				unflushedWrites = 0
			}

		case <-flushTicker.C: // the flush "ticker" has fired

			if unflushedWrites > 0 {

				if err := stream.Flush(); err != nil {
					log.WithField(common.TagErr, err).Error(`outConn.sendPump: <ticker> stream.Flush error`)
					break pump
				}

				if outConnDebug {
					log.WithFields(bark.Fields{ // #perfdisable
						`unflushed-writes`: unflushedWrites,
					}).Debug(`outConn.sendPump: <ticker> flushed ack stream`) // #perfdisable
				}

				unflushedWrites = 0
			}
		}
	}
}

// wakeupReason
type wakeupReason int

const (
	wakeupNone = iota
	wakeupCredits
	wakeupMessage
	wakeupGate
	wakeupClose
	wakeupStop
)

func (w wakeupReason) String() string {
	switch w {
	case wakeupNone:
		return "none"
	case wakeupCredits:
		return "credits"
	case wakeupMessage:
		return "message"
	case wakeupGate:
		return "gate"
	case wakeupClose:
		return "close"
	case wakeupStop:
		return "stop"
	default:
		return "unknown"
	}
}

type sleepReason int

const (
	sleepNoCredits = 1 << iota
	sleepNoMessages
	sleepGate
)

func (s sleepReason) String() (str string) {

	sm := int(s)

	first := true

	for m := 1; sm != 0; m <<= 1 {

		var add string

		switch sm & m {
		case sleepNoCredits:
			add = "nocreds"
		case sleepNoMessages:
			add = "nomsgs"
		case sleepGate:
			add = "gate"
		}

		if add != "" {
			if first {
				str = "("
				first = false
			} else {
				str += ","
			}
			str += add
		}

		sm &= ^m
	}

	if !first {
		str += ")"
	}

	return
}
