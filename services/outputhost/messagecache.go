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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/outputhost/load"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

//                                                                   +----------------------------------+
//                                                                   | consConnection.writeMsgsStream() |
//                                                                   +----------------------------------+
//                                                                                     |
//                                                                                     | msgCache.msgCacheCh (*msg)
//                                                                                     v
//  ...........................      ###################################################################################################
//  :      msgMap (*msg)      :      #                                                                                                 #
//  :   timerCache (AckID)    :      #                                   manageMessageDeliveryCache                                    #
//  : redeliveryCount (AckID) : <--> #                                                                                                 #
//  :.........................:      ###################################################################################################
//                                                 :                       ^                              ^
//                                                 :                       : msgCache.ackMsgCh (AckID)    |
//                                                 : dlq.publishCh (*msg)  : msgCache.nackMsgCh (AckID)   | msgCache.redeliveryTicker.C
//                                                 v                       :                              |
//                                   +----------------------------------+  :                            +------------------------------+
//                                   |         dlq.publisher()          |  :                            |         time.Ticker          |
//                                   +----------------------------------+  :                            +------------------------------+
//                                                    |                    :
//                                                    |                    :
//                                                    v                    :
//                                   +----------------------------------+  :
//                                   |  TChanBOutputHost.AckMessages()  | ..
//                                   +----------------------------------+
//
// ~/perl5/bin/graph-easy --input=MessageCache.dot -as_ascii | sed "s#^#//  #" | pbcopy

//   +----------------------------------------------------------------+
//   |                                                                |
//   |                                                                |
//   |       +---------------------+                                  |
//   |       |                     |                                  |
//   | ACK   |                     |                                  |
//   |       | ACK       +---------+-----------------------------+    |
//   |       v           |         |                             |    |
//   |     +----------+  |       ####################            |    |
//   +---> | EARLYACK | -+       #        NX        # -+         |    |
//         +----------+          ####################  |         |    |
//           |                     |                   |         |    |
//           |                     | NACK              |         |    |
//           |                     v                   |         |    |
//           |                   +------------------+  |         |    |
//           |           +------ |    EARLYNACK     | -+---------+----+
//           |           |       +------------------+  |         |
//           |           |         |                   |         |
//   +-------+           |         | CACHE             | CACHE   |
//   |                   |         v                   |         |
//   |                   |       +------------------+  |         |
//   |       +-----------+------ |   DELIVERED(N)   | <+         |
//   |       |           |       +------------------+            |
//   |       |           |         |                             |
//   |       |           |         | TIMER(N>=LIMIT)             |
//   |       |           |         v                             |
//   |       |           |       +------------------+            |
//   |       | ACK       |       |   DLQDELIVERED   |            |
//   |       |           |       +------------------+            |
//   |       |           |         |                             |
//   |       |           |         | ACK                         |
//   |       |           |         v                             |
//   |       |           |       +------------------+  CACHE     |
//   |       +-----------+-----> |     CONSUMED     | <----------+
//   |                   |       +------------------+
//   |                   |         |
//   |                   | TIMER   | TIMER
//   |                   |         v
//   |                   |       ####################
//   |                   +-----> #     DELETED      #
//   |                           ####################
//   |      TIMER                  ^
//   +-----------------------------+
//
//  NX = (Start) Not eXisting
//  EARLYACK = ACK arrived before cached message (race)
//  EARLYNACK = NACK arrived before cached message (race, will redeliver to consumer when cache arrives)
//  DELIVERED(N) = message has been (re)DELIVERED N times; starts at N=1
//  DLQDELIVERED = message has been added to the DLQ's channel buffer. When it is delivered, an ACK will arrive.
//  CONSUMED = message is somehow acknowledged as consumed. This state exists to absorb extra ACKs/NACKs
//  DELETED = (Sink) message is removed from the system. Equivalent to NX.
//
//  ACK -> AckID arrives on ACK channel
//  NACK -> AckID arrives on NACK channel
//  Cache -> Message payload + AckID arrives on the cache channel
//  Timer -> Timer subsystem processes the message
//
//  Events not shown loop back to the same state
//
// ~/perl5/bin/graph-easy --input=MessageCacheStates.dot -as_ascii | sed "s#^#// #" | pbcopy

type msgState int8

const (
	stateNX = msgState(iota)
	stateDelivered
	stateDLQDelivered
	stateConsumed
	stateDeleted
	stateEarlyACK
	stateEarlyNACK
)

type msgEvent int8

const (
	eventCache = msgEvent(iota)
	eventACK
	eventNACK
	eventTimer
	eventRedelivery
	eventInjection
)

type m3HealthState int64

const (
	stateStalled = m3HealthState(iota)
	stateIdle
	stateProgressing
)

const ackChannelSize = 1000

// AckID is an acknowledgement ID; not the same as common.AckID, which decomposes this string
// Capitalized because otherwise the type name conflicts horribly with local variables
type AckID string

const defaultLockTimeoutInSeconds int32 = 42
const defaultMaxDeliveryCount int32 = 2
const blockCheckingTimeout time.Duration = time.Minute
const redeliveryInterval = time.Second / 2

// SmartRetryDisableString can be added to a destination or CG owner email to request smart retry to be disabled
// Note that Google allows something like this: gbailey+smartRetryDisable@uber.com
// The above is still a valid email and will be delivered to gbailey@uber.com
const SmartRetryDisableString = `smartRetryDisable`

type cachedMessage struct {
	currentState   msgState
	previousState  msgState
	msg            *cherami.ConsumerMessage // MAY BE NIL; pointer to the cached message, if available
	n              int32                    // count of passes through this state; used to count delivery attempts, maybe for periodic logging on timer fire
	prevN          int32                    // previous value of n
	lastConnID     int                      // the last connection where this message was delivered to
	fireTime       common.UnixNanoTime      // Ensures that this message has only one active fire time
	createTime     common.UnixNanoTime
	dlqInhibit     int  // Inhibits DLQ delivery for this many rounds. 'Extra lives'
	dlqInhibitOnce bool // Don't allow a life extention more than once
}

type timerCacheEntry struct {
	AckID
	fireTime common.UnixNanoTime
}

type timestampedAckID struct {
	AckID
	ts common.UnixNanoTime
}

type consumerHealth struct {
	badConns           map[int]int // this is the map of all bad connections, i.e, connections which get Nacks and timeouts
	lastAckTime        common.UnixNanoTime
	lastRedeliveryTime common.UnixNanoTime
	lastInjectionTime  common.UnixNanoTime
	lastAckMsg         *cherami.ConsumerMessage // 'good' message to be reinjected to test downstream health.
	lastProgressing    bool
	lastIdle           bool
}

type stateMachineHealth struct {
	countStateDelivered    int
	countStateDLQDelivered int
	countStateConsumed     int
	countStateEarlyACK     int
	countStateEarlyNACK    int
}

type pumpHealth struct {
	lastMsgsRedeliveryChFull         common.UnixNanoTime
	lastPriorityMsgsRedeliveryChFull common.UnixNanoTime
	lastMsgCacheChFull               common.UnixNanoTime
	lastMsgCacheRedeliveredChFull    common.UnixNanoTime
	lastAckMsgChFull                 common.UnixNanoTime
	lastNackMsgChFull                common.UnixNanoTime
	lastDlqPublishChFull             common.UnixNanoTime
	lastCreditNotifyChFull           common.UnixNanoTime
	lastCreditRequestChFull          common.UnixNanoTime
	lastRedeliveryTickerFull         common.UnixNanoTime
}

type messageCacheHealth struct {
	stateMachineHealth
	pumpHealth
}

// cgMsgCache maintains the message cache for this consumer group and helps in the
// redelivery of messages.
type cgMsgCache struct {
	msgMap                   map[AckID]*cachedMessage
	redeliveryTimerCache     []*timerCacheEntry // Timer cache for things delayed by the lock timeout (user configured)
	cleanupTimerCache        []*timerCacheEntry // Timer cache for things delayed by the defaultLockTimeout
	zeroTimerCache           []*timerCacheEntry // Timer cache for things delayed by zero
	closeChannel             chan struct{}
	redeliveryTicker         *time.Ticker
	msgsRedeliveryCh         chan<- *cherami.ConsumerMessage // channel to resend messages to the client connectons
	priorityMsgsRedeliveryCh chan<- *cherami.ConsumerMessage // channel to resend messages to the client connectons
	msgCacheCh               <-chan cacheMsg                 // channel to get messages to be stored on the cache
	msgCacheRedeliveredCh    <-chan cacheMsg                 // channel to get redelivered messages to update the cache
	ackMsgCh                 <-chan timestampedAckID         // channel to get ackIDs from client and update the cache
	nackMsgCh                <-chan timestampedAckID         // channel to get ackIDs from client, deliver to DLQ, and update the cache
	dlq                      *deadLetterQueue
	dlqPublishCh             chan<- *cherami.ConsumerMessage // channel to deliver things to DLQ
	lclLg                    bark.Logger
	blockCheckingTimer       *common.Timer
	caseStartTime            time.Time     // time when the currently running select case started processing
	caseEvent                msgEvent      // event of the currently running case
	caseHighWaterMark        time.Duration // worst time any case has taken
	caseAvgTime              common.GeometricRollingAverage
	caseCount                int
	ackAvgTime               common.GeometricRollingAverage
	ackHighWaterMark         common.Seconds
	consumerM3Client         metrics.Client
	m3Client                 metrics.Client
	notifier                 Notifier // this notifier is used to slow down cons connections based on NACKs
	consumerHealth
	messageCacheHealth
	creditNotifyCh     chan<- int32        // this is the notify ch to notify credits to extents
	creditRequestCh    <-chan string       // read-only channel used by the extents to request credits specifically for that extent.
	maxOutstandingMsgs int32               // max allowed outstanding messages
	numAcks            int32               // num acks we received
	cgCache            *consumerGroupCache // just a reference to the cgCache to grant credits to a local extent directly
	shared.ConsumerGroupDescription
}

func getStateString(state msgState) string {
	switch state {
	case stateNX:
		return "NX"
	case stateDelivered:
		return "Delivered"
	case stateDLQDelivered:
		return "DLQ-Delivered"
	case stateConsumed:
		return "Consumed"
	case stateDeleted:
		return "Deleted"
	case stateEarlyACK:
		return "Early-ACK"
	case stateEarlyNACK:
		return "Early-NACK"
	default:
		panic("unhandled state" + fmt.Sprintf(" %d", state))
	}
}

func getEventString(event msgEvent) string {
	switch event {
	case eventACK:
		return "ACK"
	case eventCache:
		return "CACHE"
	case eventNACK:
		return "NACK"
	case eventTimer:
		return "TIMER"
	case eventRedelivery:
		return "REDELIVERY"
	default:
		panic("unhandled event" + fmt.Sprintf(" %d", event))
	}
}

func (msgCache *cgMsgCache) utilHandleDeliveredMsg(cMsg cacheMsg) {
	msgCache.startTimer(eventCache)

	msg := cMsg.msg
	// Write the message to the message cache & the timer cache
	// map[ackID] = msg
	// timer[time.Now() + cg.lockTimeoutInSeconds] = ackID
	// If the message is already acked before being added to the cache,
	// no need to add the message here
	ackID := AckID(msg.GetAckId())

	cm := msgCache.getState(ackID)
	cm.lastConnID = cMsg.connID
	switch cm.currentState {
	case stateNX: // Happy path
		msgCache.changeState(ackID, stateDelivered, msg, eventCache)
		// Abnormal paths
	case stateEarlyACK:
		//lclLg.WithField("AckID", common.ShortenGUIDString(msg.GetAckId())).Debug("manageMessageDeliveryCache: Early ACKed message (no need to add to cache)")
		msgCache.changeState(ackID, stateConsumed, msg, eventCache)
	case stateEarlyNACK:
		//lclLg.WithField("AckID", common.ShortenGUIDString(msg.GetAckId())).Debug("manageMessageDeliveryCache: Early NACKed message (delivering to DLQ)")
		msgCache.changeState(ackID, stateDelivered, msg, eventCache) // Mark one delivery as complete, eligible for redelivery depending on max deliveries
		cm.fireTime = msgCache.addTimer(0, ackID)                    // Try to redeliver immediately, rather than wait for the lock timeout
	case stateDelivered:
		break // this happens on redelivery
	case stateConsumed:
		break // this also can happen on redelivery
	case stateDLQDelivered:
		break // these cache events can happen anytime after the first Delivery state
	default:
		panic("Unhandled msgCache state: " + getStateString(cm.currentState) + " <- " + getStateString(cm.previousState))
	}

}

func (msgCache *cgMsgCache) utilHandleRedeliveredMsg(cMsg cacheMsg) {
	msgCache.startTimer(eventRedelivery)
	msg := cMsg.msg

	// This means we redelivered the message successfully.
	ackID := AckID(msg.GetAckId())

	cm := msgCache.getState(ackID)
	cm.lastConnID = cMsg.connID
	switch cm.currentState {
	case stateDelivered:
		// update the state to increase the count of delivery
		msgCache.changeState(ackID, stateDelivered, nil, eventTimer)
	case stateConsumed:
		break // we already got an ack. it is ok
	default:
		msgCache.lclLg.WithFields(bark.Fields{
			common.TagAckID:  common.FmtAckID(string(ackID)),
			"prevState":      getStateString(cm.previousState),
			"prevStateN":     cm.prevN,
			"curState":       getStateString(cm.currentState),
			"curStateN":      cm.n,
			"dlqInhibit":     cm.dlqInhibit,
			"dlqInhibitOnce": cm.dlqInhibitOnce}).Info("unexpected state after redelivery")

	// These can happen with the good message injection
	case stateNX:
		break
	case stateEarlyACK:
		break
	case stateEarlyNACK:
		break

	}
}

func (msgCache *cgMsgCache) utilHandleRedeliveryTicker() {
	msgCache.startTimer(eventTimer)

	var redeliveries int64

	now := common.Now()

	timerCaches := []*[]*timerCacheEntry{
		&msgCache.redeliveryTimerCache,
		&msgCache.cleanupTimerCache,
		&msgCache.zeroTimerCache,
	}

	stalled := msgCache.isStalled()

	// good message injection when consumer group is unhealthy (stalled). This allows us to get
	// at least one ack and be unstalled if/when the consumer group is healthy
	if stalled {
		// TODO: record a stalled metric (boolean guage)
		msgCache.reinjectlastAckMsg() // injects the last acked message if it is not already injected
	}

	for _, cache := range timerCaches {

		nExpired := 0

	thisCache:
		for _, entry := range *cache {

			// When we reach the first entry that shouldn't be fired,
			// break and remove all prior entries
			if entry.fireTime > now {
				break thisCache
			}

			nExpired++

			ackID := entry.AckID
			cm := msgCache.getState(ackID)

			// Check if the fire time was reassigned
			if cm.fireTime == 0 || cm.fireTime > now {
				continue thisCache
			}

			switch cm.currentState {
			case stateDelivered:
				// Check if we need to put the message to DLQ or if we need to redeliver.
				// We put the msg to DLQ on these conditions
				// 1. We have already redelivered upto the max delivery count
				// 2. We are not stalled
				if cm.n > msgCache.GetMaxDeliveryCount() && !stalled && cm.dlqInhibit <= 0 {
					msgCache.changeState(ackID, stateDLQDelivered, nil, eventTimer)
					msgCache.publishToDLQ(cm)
				} else {
					select {
					case msgCache.msgsRedeliveryCh <- cm.msg:
						redeliveries++
						msgCache.consumerHealth.lastRedeliveryTime = now // don't use variable 'now', since processing may be very slow in degenerate cases; consider moving to utilHandleRedeliveredMsg

						if !stalled && cm.dlqInhibit > 0 {
							cm.dlqInhibit-- // Use up one of the 'extra lives', but only if the consumer group seems healthy
						}

					default:
						// we will try to redeliver next time
						// lclLg.WithField(common.TagAckID, common.FmtAckID(string(ackID))).
						//	Info("manageMessageDeliveryCache: no listeners on the redelivery channel. We will redeliver this message next time")

						cm.fireTime = msgCache.addTimer(int(msgCache.GetLockTimeoutSeconds()), ackID)
					}
				}
			case stateEarlyACK:
				fallthrough
			case stateEarlyNACK:
				fallthrough
			case stateConsumed:
				msgCache.changeState(ackID, stateDeleted, nil, eventTimer)
			default:
				panic("Unhandled msgCache state: " + getStateString(cm.currentState) + " <- " + getStateString(cm.previousState) + ` (` + string(ackID) + `)`)
			} // switch cm.currentState
		} // for thisCache

		if nExpired > 0 {
			// remove all the entries that expired
			// above by simply advancing the slice
			*cache = (*cache)[nExpired:]
		}

	} // for timercaches

	// Report redelivery metrics; consider moving to utilHandleRedeliveredMsg
	if redeliveries > 0 {
		msgCache.m3Client.AddCounter(metrics.ConsConnectionStreamScope, metrics.OutputhostMessageRedelivered, redeliveries)
		msgCache.consumerM3Client.AddCounter(metrics.ConsConnectionScope, metrics.OutputhostCGMessageRedelivered, redeliveries)
	}
}

func (msgCache *cgMsgCache) utilHandleNackMsg(ackID timestampedAckID) {
	lclLg := msgCache.lclLg
	msgCache.startTimer(eventNACK)

	if len(msgCache.nackMsgCh) >= ackChannelSize-1 {
		msgCache.lclLg.Error(`Nack channel is full`)
	}

	i := int64(1) // we always handle at least one ack, even if the channel is empty afterwards
nackDrain:
	for ; i < ackChannelSize*2; i++ { // Empty the channel twice, at most, to prevent starvation
		msgCache.handleNack(ackID, lclLg)
		select {
		case ackID = <-msgCache.nackMsgCh:
			continue nackDrain
		default:
			break nackDrain
		}
	}

	msgCache.consumerM3Client.AddCounter(metrics.ConsConnectionScope, metrics.OutputhostCGMessageSentNAck, i)
}

func (msgCache *cgMsgCache) utilHandleAckMsg(ackID timestampedAckID) {
	lclLg := msgCache.lclLg
	msgCache.startTimer(eventACK)

	if len(msgCache.ackMsgCh) >= ackChannelSize-1 {
		msgCache.lclLg.Error(`Ack channel is full`)
	}

	i := int64(1) // we always handle at least one ack, even if the channel is empty afterwards
ackDrain:
	for ; i < ackChannelSize*2; i++ { // Empty the channel twice, at most, to prevent starvation
		msgCache.handleAck(ackID, lclLg)
		select {
		case ackID = <-msgCache.ackMsgCh:
			continue ackDrain
		default:
			break ackDrain
		}
	}

	msgCache.consumerM3Client.AddCounter(metrics.ConsConnectionScope, metrics.OutputhostCGMessageSentAck, i)
}

func (msgCache *cgMsgCache) utilRenewCredits() {
	// now we can send credits to the extents, so that they can renew them
	// to the appropriate store
	select {
	case msgCache.creditNotifyCh <- msgCache.numAcks:
		// XXX: logs disabled due to CPU cost
		//msgCache.lclLg.WithField("credits", msgCache.numAcks).
		//	Debug("msgCache renewing credits")
		msgCache.numAcks = 0
	default:
		// we are blocked sending credits. Don't reset
	}
}

func (msgCache *cgMsgCache) addTimer(delaySeconds int, id AckID) common.UnixNanoTime {
	now := common.Now()
	entry := &timerCacheEntry{
		AckID:    id,
		fireTime: now + common.UnixNanoTime(int64(time.Second)*int64(delaySeconds)),
	}

	if delaySeconds == int(msgCache.GetLockTimeoutSeconds()*2) {
		msgCache.cleanupTimerCache = append(msgCache.cleanupTimerCache, entry)
	} else if delaySeconds == 0 {
		msgCache.zeroTimerCache = append(msgCache.zeroTimerCache, entry)
	} else if delaySeconds == int(msgCache.GetLockTimeoutSeconds()) {
		msgCache.redeliveryTimerCache = append(msgCache.redeliveryTimerCache, entry)
	} else {
		msgCache.lclLg.Panic(`Don't have a timer queue to handle this delay`)
	}
	return entry.fireTime
}

func (msgCache *cgMsgCache) getState(id AckID) *cachedMessage {
	if cm, ok := msgCache.msgMap[id]; ok {
		return cm
	}
	return &cachedMessage{
		currentState:  stateNX,
		previousState: stateNX,
		n:             0,
		prevN:         0,
		msg:           nil,
		createTime:    common.Now(),
	}
}

func (msgCache *cgMsgCache) decrState(state msgState) {
	switch state {
	case stateDelivered:
		msgCache.countStateDelivered--
	case stateDLQDelivered:
		msgCache.countStateDLQDelivered--
	case stateConsumed:
		msgCache.countStateConsumed--
	case stateEarlyACK:
		msgCache.countStateEarlyACK--
	case stateEarlyNACK:
		msgCache.countStateEarlyNACK--
	}
}

func (msgCache *cgMsgCache) incrState(state msgState) {
	switch state {
	case stateDelivered:
		msgCache.countStateDelivered++
	case stateDLQDelivered:
		msgCache.countStateDLQDelivered++
	case stateConsumed:
		msgCache.countStateConsumed++
	case stateEarlyACK:
		msgCache.countStateEarlyACK++
	case stateEarlyNACK:
		msgCache.countStateEarlyNACK++
	}
}

func (msgCache *cgMsgCache) changeState(id AckID, newState msgState, msg *cherami.ConsumerMessage, event msgEvent) {
	var cm *cachedMessage
	var ok bool
	if cm, ok = msgCache.msgMap[id]; !ok {
		if newState == stateDeleted {
			return // Not going to create it just to delete it
		}

		cm = &cachedMessage{
			currentState:  newState,
			previousState: stateNX,
			n:             0, // n++ below
			prevN:         0,
			msg:           msg,
			createTime:    common.Now(),
		}

		msgCache.incrState(cm.currentState)
		msgCache.msgMap[id] = cm
	}

	// Delete msg
	if newState == stateDeleted {
		delete(msgCache.msgMap, id)
		msgCache.decrState(cm.currentState)
		return
	}

	// Update state and/or state counter
	if cm.currentState == newState { // Increment the state counter if we have already been in this state
		cm.n++
	} else { // Else, keep state history and reset the counter to 1
		cm.previousState = cm.currentState
		cm.currentState = newState
		cm.prevN = cm.n
		cm.n = 1

		// Update message state counters (note that the current and previous state are already updated above)
		msgCache.incrState(cm.currentState)
		msgCache.decrState(cm.previousState)
	}

	// Update msg pointer
	if msg != nil {
		cm.msg = msg
	}

	// Adjust timer as appropriate
	switch cm.currentState {
	case stateEarlyACK:
		fallthrough
	case stateEarlyNACK:
		fallthrough
	case stateConsumed:
		cm.fireTime = msgCache.addTimer(int(msgCache.GetLockTimeoutSeconds()*2), id)
	case stateDelivered:
		cm.fireTime = msgCache.addTimer(int(msgCache.GetLockTimeoutSeconds()), id)
	case stateDLQDelivered:
		cm.fireTime = 0 // Rely on the ACK from the DLQ to cleanup; No ACK = LEAK
	default:
		panic("Unhandled msgCache state: " + getStateString(cm.currentState) + " <- " + getStateString(cm.previousState))
	}

	isAbnormal := func(state msgState, n int32) bool {
		switch state {
		case stateConsumed:
			if n > 1 {
				return true
			}
		case stateDeleted:
			return true
		case stateDelivered:
			if n > 1 {
				return false // Disabled to reduce logs
			}
		case stateDLQDelivered:
			return true
		case stateEarlyACK:
			return false // Disabled to reduce the amount of logs
		case stateEarlyNACK:
			return true
		}
		return false
	}

	// partially disabled to reduce logs; we will only know if we got into an abnormal state, but not if/when we get out of it.
	if isAbnormal(cm.currentState, cm.n) /* || isAbnormal(cm.previousState, cm.prevN) */ {
		if cm.fireTime != 0 {
			now := common.Now()

			// Gives the firing time for this timer, compared to now, in seconds
			timerFiringOffset := (cm.fireTime - now).ToSeconds()
			msgCache.lclLg.WithFields(bark.Fields{
				common.TagAckID:  common.FmtAckID(string(id)),
				"event":          getEventString(event),
				"prevState":      getStateString(cm.previousState),
				"prevStateN":     cm.prevN,
				"curState":       getStateString(cm.currentState),
				"curStateN":      cm.n,
				"dlqInhibit":     cm.dlqInhibit,
				"dlqInhibitOnce": cm.dlqInhibitOnce,
				"timerOffset":    timerFiringOffset}).Info("message state change")
		} else {
			msgCache.lclLg.WithFields(bark.Fields{
				common.TagAckID:  common.FmtAckID(string(id)),
				"event":          getEventString(event),
				"prevState":      getStateString(cm.previousState),
				"prevStateN":     cm.prevN,
				"curState":       getStateString(cm.currentState),
				"curStateN":      cm.n,
				"dlqInhibit":     cm.dlqInhibit,
				"dlqInhibitOnce": cm.dlqInhibitOnce}).Info("message state change")
		}
	}
}

func (msgCache *cgMsgCache) stop() {
	close(msgCache.closeChannel)
	if msgCache.dlq != nil {
		msgCache.dlq.close()
	}
	msgCache.redeliveryTicker.Stop()
}

func (msgCache *cgMsgCache) start() {
	go msgCache.manageMessageDeliveryCache()
}

// this is the routine responsible for managing the delivery cache
// There are 4 scenarios to consider here:
// 1. A message is delivered (we get a message on the msgCacheCh). This means
//    add the message to the delivery cache and the timer cache.
// 2. The redelivery ticker fired, which means we need to go through the timer map
//    and redeliver messages which are timed out.
// 3. Message gets acked, which means we need to remove that message from the caches
// 4. shutdown => quit
func (msgCache *cgMsgCache) manageMessageDeliveryCache() {
	msgCache.blockCheckingTimer = common.NewTimer(blockCheckingTimeout)
	msgCache.dlqPublishCh = msgCache.dlq.getPublishCh()
	lastPumpHealthLog := common.UnixNanoTime(0)
	var creditBatchSize int32

	for {
		fullCount := msgCache.updatePumpHealth()
		// calculate the creditBatchSize here, since we could have updated the
		// maxOutstandingMsgs dynamically.
		// This is the number of acks we need to get before renewing credits
		creditBatchSize = msgCache.maxOutstandingMsgs / 10

		if fullCount > 3 && common.Now()-lastPumpHealthLog > common.UnixNanoTime(time.Minute) {
			msgCache.logMessageCacheHealth()
			lastPumpHealthLog = common.Now()
		}

		numOutstandingMsgs := msgCache.getOutstandingMsgs()

		if msgCache.numAcks > creditBatchSize && numOutstandingMsgs < msgCache.maxOutstandingMsgs {
			// now we can send credits to the extents, so that they can renew them
			// to the appropriate store
			msgCache.utilRenewCredits()
		}

		select {
		case cMsg := <-msgCache.msgCacheCh:
			// this means we got a new message
			msgCache.utilHandleDeliveredMsg(cMsg)
		case cMsg := <-msgCache.msgCacheRedeliveredCh:
			msgCache.utilHandleRedeliveredMsg(cMsg)
		case <-msgCache.redeliveryTicker.C:
			msgCache.utilHandleRedeliveryTicker()
			// Incase we have a very low throughput destination, we might not
			// accumulate enough messages. So renew credits irrespective if we have
			// accumulated some acks.
			if msgCache.numAcks > 0 && numOutstandingMsgs < msgCache.maxOutstandingMsgs {
				msgCache.utilRenewCredits()
			}
			msgCache.refreshCgConfig(msgCache.maxOutstandingMsgs)
		case ackID := <-msgCache.nackMsgCh:
			msgCache.utilHandleNackMsg(ackID)
		case ackID := <-msgCache.ackMsgCh:
			msgCache.utilHandleAckMsg(ackID)
		case extUUID := <-msgCache.creditRequestCh:
			// an extent is requesting credits, which means even if we have some
			// credits to grant, grant it to this extent
			if msgCache.numAcks > 0 && numOutstandingMsgs < msgCache.maxOutstandingMsgs {
				if ok := msgCache.cgCache.utilSendCreditsToExtent(extUUID, msgCache.numAcks); ok {
					// reset the count only if we were able to grant credits
					msgCache.numAcks = 0
				}
			}
		case <-msgCache.closeChannel:
			// now cleanup any existing entries in cache
			// this is done to make sure we drop any references
			// to the payload
			msgCache.shutdownCleanupEntries()
			msgCache.cgCache.manageMsgCacheWG.Done()
			return
		} // select
		msgCache.checkTimer()
	} // for
}

// shutdownCleanupEntries is used to cleanup the entire map in case of
// shutdown.
// this is needed to make sure we drop all references to the payload
func (msgCache *cgMsgCache) shutdownCleanupEntries() {
	for id := range msgCache.msgMap {
		delete(msgCache.msgMap, id)
	}
}

func (msgCache *cgMsgCache) reinjectlastAckMsg() bool {
	if msgCache.consumerHealth.lastAckMsg == nil {
		msgCache.lclLg.Warn(`Couldn't inject good message: no message to inject`)
		return false
	}

	// nextInjectionTime is half the lock timeout after the previous injection time
	nextInjectionTime := msgCache.consumerHealth.lastInjectionTime + common.UnixNanoTime(int64(time.Second/2)*int64(msgCache.GetLockTimeoutSeconds()))

	if common.Now() < nextInjectionTime {
		return false // not time yet; don't inject
	}

	ackID := msgCache.consumerHealth.lastAckMsg.GetAckId()
	cm := msgCache.getState(AckID(ackID)) // This will add it back to the message map if it has already been deleted
	cm.msg = msgCache.consumerHealth.lastAckMsg

	select {
	case msgCache.priorityMsgsRedeliveryCh <- cm.msg:

		// changeState adds the message to the msgMap if it's not already there. Note that there's a
		// possibility that we are reanimating a deleted state message here. changeState handles this.  If the
		// cleanup timer for the consumed state fires before we get the ack, we might also transition from
		// EarlyACK here.
		msgCache.changeState(AckID(ackID), stateConsumed, msgCache.consumerHealth.lastAckMsg, eventTimer)
		msgCache.lclLg.WithField(common.TagAckID, common.FmtAckID(ackID)).Warn(`Injected known good message`)
		msgCache.consumerHealth.lastInjectionTime = common.Now()
		return true

	default: // Priority channel is full. Wait to deliver the message first.
	}

	// Don't need to do anything if the message is already in the delivered state, since we will redeliver endlessly until an Ack adjusts the lastAckTime
	msgCache.lclLg.WithField(common.TagAckID, common.FmtAckID(ackID)).Warn(`Waiting to clear known good message`)
	return false
}

func (msgCache *cgMsgCache) getOutstandingMsgs() int32 {
	// TODO: make this limit the distance from the lowest un-acked message as well
	return int32(msgCache.countStateDelivered + msgCache.countStateEarlyNACK)
}

func (msgCache *cgMsgCache) handleAck(ackID timestampedAckID, lclLg bark.Logger) {
	var badMessage bool
	cm := msgCache.getState(ackID.AckID)
	now := common.Now()

	delay := (now - ackID.ts).ToSeconds()
	msgCache.ackAvgTime.SetGeometricRollingAverage(float64(delay))

	if msgCache.ackHighWaterMark < delay {
		msgCache.ackHighWaterMark = delay
	}

	// msgCache.lclLg.WithField(`ackid`, string(ackID.AckID)).Warn(`handleAck`)

	switch cm.currentState {
	case stateDLQDelivered:
		badMessage = true
		fallthrough
	case stateDelivered:
		msgCache.changeState(ackID.AckID, stateConsumed, nil, eventACK)

	case stateNX:
		fallthrough
	case stateEarlyNACK:
		msgCache.changeState(ackID.AckID, stateEarlyACK, nil, eventACK)

	case stateEarlyACK:
		break

	case stateConsumed:
		break

	default:
		panic("Unhandled msgCache state: " + getStateString(cm.currentState) + " <- " + getStateString(cm.previousState))
	}

	//  this is a successful ack, we need to make sure we notify the appropriate connection to
	//  stop throttling and also drop this connection from the list of bad connections.
	//
	//  lastAckMsg is a known good message, we keep around to figure out if either:
	//
	//  a. We have a continuous stream of poison pill messages. That is, there is a problem with the messages and we need to put
	//     these messages in DLQ.
	//
	//  (Or)
	//
	//  b. There is a problem with the consumer system as a whole and there is nobody alive to process these messages which
	//     means we keep retrying.
	//
	//  Since we have a limit on the number of outstanding messages, it is possible that all of the outstanding messages are
	//  poison pills and we can't retrieve any good message. This situation seems to be indistinguishable from a global problem
	//  with the consumer group. We would therefore not know whether to deliver these messages to DLQ, or to continue retrying
	//  them until the consumer group is healthy again.
	//
	//  If we replay this known good message and we get an ACK back, then we can assume that these messages can be put in the
	//  DLQ. If we don't get any ACK, we just keep retrying, since it appears that the consumer group is unhealthy.

	if !badMessage {
		msgCache.consumerHealth.lastAckTime = now
		if cm.msg != nil {
			msgCache.consumerHealth.lastAckMsg = cm.msg
		}

		// TODO: don't updateConn if we already had this ACK (stateConsumed); a worker in a bad loop could just ack the same message over and over
		msgCache.updateConn(cm.lastConnID, eventACK)
	}

	msgCache.numAcks++
}

func (msgCache *cgMsgCache) handleNack(ackID timestampedAckID, lclLg bark.Logger) {
	cm := msgCache.getState(ackID.AckID)

	switch cm.currentState {
	case stateDelivered:
		// just update the fire time to be immediate, for immediate redelivery
		// utilHandleRedeliveredMsg will handle updating the redelivery count as appropriate
		if cm.n+1 < msgCache.GetMaxDeliveryCount() { // If the next redelivery won't be the last before DLQ delivery, retry immediately
			cm.fireTime = msgCache.addTimer(0, ackID.AckID)
		} else { // if this will be the final delivery attempt, delay for the lock timeout so that we can detect a stall before delivering to DLQ
			cm.fireTime = msgCache.addTimer(int(msgCache.GetLockTimeoutSeconds()), ackID.AckID)
		}
	case stateNX:
		msgCache.changeState(ackID.AckID, stateEarlyNACK, nil, eventNACK)
	case stateEarlyNACK:
		break
	case stateDLQDelivered:
		break
	case stateEarlyACK:
		break
	case stateConsumed:
		break

	default:
		panic("Unhandled msgCache state: " + getStateString(cm.currentState) + " <- " + getStateString(cm.previousState))
	}
	// find the connection id of the message and then throttle that connection
	// the notifier interface will let the appropriate connection know about this.
	// the connections will take care of throttling based on the number of nacks
	// received per second.
	msgCache.updateConn(cm.lastConnID, eventNACK)
}

// updateConn is the utility routine to notify the connection
// to either throttle up or throttle down.
// If we get a NACK on this connection, we ask the connection to
// slow down (throttle up), except it's disabled explicitly
// If we get an ACK on this connection and we asked it to throttle
// earlier, we ask the connection to stop throttling (throttle down!).
func (msgCache *cgMsgCache) updateConn(connID int, event msgEvent) {
	if event == eventACK {
		msgCache.notifier.Notify(connID, NotifyMsg{notifyType: ThrottleDown})
	} else {
		// if NACK throttling is disabled for this cg, then no-op
		throttlingDisabled, ok := msgCache.cgCache.cachedCGDesc.Options[common.FlagDisableNackThrottling]
		if ok && throttlingDisabled == "true" { // no-op
			return
		}

		// make sure to throttle the appropriate connection and update the
		// bad connections map to keep track of the number of nacks
		msgCache.notifier.Notify(connID, NotifyMsg{notifyType: ThrottleUp})
	}
}

func (msgCache *cgMsgCache) refreshCgConfig(oldOutstandingMessages int32) {
	outstandingMsgs := oldOutstandingMessages
	cfg, err := msgCache.cgCache.getDynamicCgConfig()
	if err == nil {
		outstandingMsgs = msgCache.cgCache.getMessageCacheSize(cfg, oldOutstandingMessages)
	}

	msgCache.maxOutstandingMsgs = outstandingMsgs
}

// TODO: Make the delivery cache shared among all consumer groups
func newMessageDeliveryCache(
	dlq *deadLetterQueue,
	defaultNumOutstandingMsgs int32,
	cgCache *consumerGroupCache) *cgMsgCache {
	if cgCache.cachedCGDesc.GetLockTimeoutSeconds() == 0 {
		cgCache.cachedCGDesc.LockTimeoutSeconds = common.Int32Ptr(defaultLockTimeoutInSeconds)
	}

	if cgCache.cachedCGDesc.GetMaxDeliveryCount() == 0 {
		cgCache.cachedCGDesc.MaxDeliveryCount = common.Int32Ptr(defaultMaxDeliveryCount)
	}

	msgCache := &cgMsgCache{
		msgMap:                   make(map[AckID]*cachedMessage),
		redeliveryTimerCache:     make([]*timerCacheEntry, 0),
		cleanupTimerCache:        make([]*timerCacheEntry, 0),
		dlq:                      dlq,
		msgsRedeliveryCh:         cgCache.msgsRedeliveryCh,
		priorityMsgsRedeliveryCh: cgCache.priorityMsgsRedeliveryCh,
		msgCacheCh:               cgCache.msgCacheCh,
		msgCacheRedeliveredCh:    cgCache.msgCacheRedeliveredCh,
		ackMsgCh:                 cgCache.ackMsgCh,
		nackMsgCh:                cgCache.nackMsgCh,
		closeChannel:             make(chan struct{}),
		redeliveryTicker:         time.NewTicker(redeliveryInterval),
		ConsumerGroupDescription: cgCache.cachedCGDesc,
		consumerM3Client:         cgCache.consumerM3Client,
		m3Client:                 cgCache.m3Client,
		notifier:                 cgCache.notifier,
		creditNotifyCh:           cgCache.creditNotifyCh,
		creditRequestCh:          cgCache.creditRequestCh,
		cgCache:                  cgCache, // just a reference to the cgCache
		consumerHealth: consumerHealth{
			badConns:        make(map[int]int),
			lastAckTime:     common.Now(), // Need to start in the non-stalled state
			lastProgressing: true,
		},
	}

	msgCache.lclLg = cgCache.logger
	msgCache.refreshCgConfig(defaultNumOutstandingMsgs)

	return msgCache
}

func (msgCache *cgMsgCache) publishToDLQ(cm *cachedMessage) {
	msg := cm.msg
	msgCache.blockCheckingTimer.Reset(blockCheckingTimeout)
	select {
	case msgCache.dlqPublishCh <- msg:
	case <-msgCache.blockCheckingTimer.C:
		panic("this should never block")
	}
	// find the connection id of the message and then throttle that connection
	// the notifier interface will let the appropriate connection know about this.
	// the connections will take care of throttling based on the number of nacks
	// received per second.
	msgCache.updateConn(cm.lastConnID, eventNACK)
}

func (msgCache *cgMsgCache) startTimer(e msgEvent) {
	msgCache.caseStartTime = time.Now()
	msgCache.caseEvent = e
}

func (msgCache *cgMsgCache) checkTimer() {
	msgCache.caseCount++
	d := time.Since(msgCache.caseStartTime)
	e := msgCache.caseEvent
	ds := common.DurationToSeconds(d)

	msgCache.caseAvgTime.SetGeometricRollingAverage(float64(ds))

	if msgCache.caseCount%1000000 == 0 {
		msgCache.lclLg.WithFields(bark.Fields{"event": getEventString(e), "duration": msgCache.caseAvgTime.GetGeometricRollingAverage(), "casecount": msgCache.caseCount}).Error(`messagecache case geometric average time`)
		msgCache.lclLg.WithFields(bark.Fields{"duration": msgCache.ackAvgTime.GetGeometricRollingAverage(), "casecount": msgCache.caseCount}).Error(`ack message geometric average time`)
		msgCache.lclLg.WithFields(bark.Fields{"duration": msgCache.ackHighWaterMark, "casecount": msgCache.caseCount}).Error(`ack message record time`)
	}

	if d > msgCache.caseHighWaterMark {
		msgCache.caseHighWaterMark = d + time.Millisecond
		msgCache.lclLg.WithFields(bark.Fields{"event": getEventString(e), "duration": ds, "casecount": msgCache.caseCount}).Error(`messagecache case new record time`)
	}
}

func (msgCache *cgMsgCache) isStalled() bool {
	var m3St m3HealthState
	var smartRetryEnabled bool

	smartRetryEnabledFlag, ok := msgCache.cgCache.cachedCGDesc.Options[common.FlagEnableSmartRetry]
	if ok && smartRetryEnabledFlag == "true" {
		smartRetryEnabled = true
	}

	if atomic.LoadInt32(&msgCache.cgCache.dlqMerging) > 0 ||
		strings.Contains(msgCache.GetOwnerEmail(), SmartRetryDisableString) {
		smartRetryEnabled = false
	}

	now := common.Now()

	// Determines that no progress has been made in the recent past; this is half the lock timeout to be
	// conservative so that we ensure that things don't go to DLQ needlessly.  We are biased toward
	// indicating a stall. This happens about 50% of the time even with a good message injected, since we
	// detect a stall with no acks after half a timeout period. The practical effect is that we are not
	// 100% efficient in delivering to DLQ when we have a large run of poison pills
	stalledThreshold :=
		now -
			common.UnixNanoTime(
				(time.Second/2)*
					time.Duration(
						msgCache.GetLockTimeoutSeconds()))

	// Verifies that deliveries are occuring (i.e. not idle).
	// The main purpose for this is to prevent reinjection on idle consumer groups.
	// TODO: make this better by simply keeping a count of messages in the EarlyNack/Delivered states.
	idleThreshold :=
		now -
			common.UnixNanoTime(
				common.MaxInt64(
					int64(3*time.Second/2)*int64(msgCache.GetLockTimeoutSeconds()),
					int64(3*defaultMaxThrottleSleepDuration/2)))

	idleByTime := msgCache.consumerHealth.lastRedeliveryTime < idleThreshold // was there not a recent redelivery?

	// Double heuristic for idle. Both the last redelivery time and a lack of messages being processed. The
	// reason to have this is that cache delivery may be slow, so lack of messages in the EarlyNACK or Delivered
	// states doesn't necessarily mean that the system is idle. The consumer group may simply be caught up.
	var idleByCount bool
	if idleByTime {
		idleByCount = true
	idleVerifier:
		for _, cm := range msgCache.msgMap {
			if cm.currentState == stateEarlyNACK || cm.currentState == stateDelivered {
				idleByCount = false
				break idleVerifier
			}
		}
	}

	idle := idleByTime && idleByCount
	progressing := msgCache.consumerHealth.lastAckTime > stalledThreshold // was there a recent ack?
	stalled := !progressing && !idle                                      // are we not making progress?

	if idle != msgCache.consumerHealth.lastIdle || progressing != msgCache.consumerHealth.lastProgressing {
		lastStalled := !msgCache.lastProgressing && !msgCache.lastIdle
		msgCache.consumerHealth.lastProgressing = progressing
		msgCache.consumerHealth.lastIdle = idle

		if lastStalled != stalled {
			msgCache.lclLg.
				WithField(`stalled`, stalled).
				WithField(`idle`, idle).
				WithField(`idleByTime`, idleByTime).
				WithField(`idleByCount`, idleByCount).
				WithField(`progressing`, progressing).
				WithField(`lastAckTime`, (now-msgCache.consumerHealth.lastAckTime).ToSecondsFmt()).
				WithField(`lastRedeliveryTime`, (now - msgCache.consumerHealth.lastRedeliveryTime).ToSecondsFmt()).
				Warn("consumer group health state change")
		}

		// If we just went to unstalled, inhibit DLQ on all DLQ-eligible messages to ensure that we try to redeliver these messages
		if !stalled && lastStalled {
			var ackID AckID
			var cm *cachedMessage
			count := 0
			for ackID, cm = range msgCache.msgMap {
				if cm.currentState == stateDelivered || cm.currentState == stateEarlyNACK {
					if cm.dlqInhibitOnce == false {
						count++
						cm.dlqInhibit = int(msgCache.GetMaxDeliveryCount())
						cm.dlqInhibitOnce = true
					}
				}
			}

			if len(ackID) != 0 {
				msgCache.lclLg.WithField(common.TagAckID, common.FmtAckID(string(ackID))).
					WithField(`countMessages`, count).
					Info("set dlqInhibit")
			}
		}

		msgCache.logMessageCacheHealth()
	}

	// Assign M3 state, preferring progressing over idle. It is possible for both
	// progressing and idle to be true if there are no redeliveries happening
	switch {
	case stalled:
		m3St = stateStalled
	case progressing:
		m3St = stateProgressing
	case idle:
		m3St = stateIdle
	}

	// These metrics are reported to customers, and they don't reflect the possibility of smart retry being disabled.
	// It's a feedback mechanism to the customer to give an idea of the health of their consumer group
	msgCache.consumerM3Client.UpdateGauge(metrics.ConsConnectionScope, metrics.OutputhostCGHealthState, int64(m3St))
	msgCache.consumerM3Client.UpdateGauge(metrics.ConsConnectionScope, metrics.OutputhostCGOutstandingDeliveries, int64(msgCache.countStateDelivered+msgCache.countStateEarlyNACK))

	if !smartRetryEnabled {
		if stalled {
			msgCache.lclLg.Warn("Smart Retry disabled while consumer group stalled")
		}
		stalled = false
	}

	// These metrics are reported to the controller, and represent the true state of smart retry
	if stalled {
		msgCache.cgCache.cgMetrics.Set(load.CGMetricSmartRetryOn, 1)
	} else {
		msgCache.cgCache.cgMetrics.Set(load.CGMetricSmartRetryOn, 0)
	}

	return stalled
}

func (msgCache *cgMsgCache) logMessageCacheHealth() {
	msgCache.logStateMachineHealth()
	msgCache.logPumpHealth()
}

func (msgCache *cgMsgCache) logStateMachineHealth() {
	msgCache.lclLg.WithFields(bark.Fields{
		`Delivered`:    msgCache.countStateDelivered,
		`DLQDelivered`: msgCache.countStateDLQDelivered,
		`Consumed`:     msgCache.countStateConsumed,
		`EarlyACK`:     msgCache.countStateEarlyACK,
		`EarlyNACK`:    msgCache.countStateEarlyNACK,
		`Total`:        len(msgCache.msgMap),
	}).Info(`state machine health`)
}

var logPumpHealthOnce sync.Once

func (msgCache *cgMsgCache) logPumpHealth() {
	logPumpHealthOnce.Do(func() {
		msgCache.lclLg.WithFields(bark.Fields{
			`MsgsRedeliveryChCap`:         cap(msgCache.msgsRedeliveryCh),
			`PriorityMsgsRedeliveryChCap`: cap(msgCache.priorityMsgsRedeliveryCh),
			`MsgCacheChCap`:               cap(msgCache.msgCacheCh),
			`MsgCacheRedeliveredChCap`:    cap(msgCache.msgCacheRedeliveredCh),
			`AckMsgChCap`:                 cap(msgCache.ackMsgCh),
			`NackMsgChCap`:                cap(msgCache.nackMsgCh),
			`DlqPublishChCap`:             cap(msgCache.dlqPublishCh),
			`CreditNotifyChCap`:           cap(msgCache.creditNotifyCh),
			`CreditRequestChCap`:          cap(msgCache.creditRequestCh),
			`RedeliveryTickerCap`:         cap(msgCache.redeliveryTicker.C),
		}).Info(`cache pump health capacities`)
	})

	msgCache.lclLg.WithFields(bark.Fields{
		`MsgsRedeliveryChLen`:         len(msgCache.msgsRedeliveryCh),
		`PriorityMsgsRedeliveryChLen`: len(msgCache.priorityMsgsRedeliveryCh),
		`MsgCacheChLen`:               len(msgCache.msgCacheCh),
		`MsgCacheRedeliveredChLen`:    len(msgCache.msgCacheRedeliveredCh),
		`AckMsgChLen`:                 len(msgCache.ackMsgCh),
		`NackMsgChLen`:                len(msgCache.nackMsgCh),
		`DlqPublishChLen`:             len(msgCache.dlqPublishCh),
		`CreditNotifyChLen`:           len(msgCache.creditNotifyCh),
		`CreditRequestChLen`:          len(msgCache.creditRequestCh),
		`RedeliveryTickerLen`:         len(msgCache.redeliveryTicker.C),
	}).Info(`cache pump health`)

	lg := msgCache.lclLg
	now := common.Now()

	if msgCache.lastMsgsRedeliveryChFull > 0 {
		lg = lg.WithField(`MsgsRedeliveryCh`, common.UnixNanoTime(now-msgCache.lastMsgsRedeliveryChFull).ToSeconds())
	}
	if msgCache.lastPriorityMsgsRedeliveryChFull > 0 {
		lg = lg.WithField(`PriorityMsgsRedeliveryCh`, common.UnixNanoTime(now-msgCache.lastPriorityMsgsRedeliveryChFull).ToSeconds())
	}
	if msgCache.lastMsgCacheChFull > 0 {
		lg = lg.WithField(`MsgCacheCh`, common.UnixNanoTime(now-msgCache.lastMsgCacheChFull).ToSeconds())
	}
	if msgCache.lastMsgCacheRedeliveredChFull > 0 {
		lg = lg.WithField(`MsgCacheRedeliveredCh`, common.UnixNanoTime(now-msgCache.lastMsgCacheRedeliveredChFull).ToSeconds())
	}
	if msgCache.lastAckMsgChFull > 0 {
		lg = lg.WithField(`AckMsgCh`, common.UnixNanoTime(now-msgCache.lastAckMsgChFull).ToSeconds())
	}
	if msgCache.lastNackMsgChFull > 0 {
		lg = lg.WithField(`NackMsgCh`, common.UnixNanoTime(now-msgCache.lastNackMsgChFull).ToSeconds())
	}
	if msgCache.lastDlqPublishChFull > 0 {
		lg = lg.WithField(`DlqPublishCh`, common.UnixNanoTime(now-msgCache.lastDlqPublishChFull).ToSeconds())
	}
	if msgCache.lastCreditNotifyChFull > 0 {
		lg = lg.WithField(`CreditNotifyCh`, common.UnixNanoTime(now-msgCache.lastCreditNotifyChFull).ToSeconds())
	}
	if msgCache.lastCreditRequestChFull > 0 {
		lg = lg.WithField(`CreditRequestCh`, common.UnixNanoTime(now-msgCache.lastCreditRequestChFull).ToSeconds())
	}
	if msgCache.lastRedeliveryTickerFull > 0 {
		lg = lg.WithField(`RedeliveryTicker`, common.UnixNanoTime(now-msgCache.lastRedeliveryTickerFull).ToSeconds())
	}

	lg.Info(`cache pump health last channel full times`)
}

func (msgCache *cgMsgCache) updatePumpHealth() int {
	now := common.Now()
	fullCount := 0
	if cap(msgCache.msgsRedeliveryCh) <= len(msgCache.msgsRedeliveryCh) {
		msgCache.lastMsgsRedeliveryChFull = now
		fullCount++
	}
	if cap(msgCache.priorityMsgsRedeliveryCh) <= len(msgCache.priorityMsgsRedeliveryCh) {
		msgCache.lastPriorityMsgsRedeliveryChFull = now
		fullCount++
	}
	if cap(msgCache.msgCacheCh) <= len(msgCache.msgCacheCh) {
		msgCache.lastMsgCacheChFull = now
		fullCount++
	}
	if cap(msgCache.msgCacheRedeliveredCh) <= len(msgCache.msgCacheRedeliveredCh) {
		msgCache.lastMsgCacheRedeliveredChFull = now
	}
	if cap(msgCache.ackMsgCh) <= len(msgCache.ackMsgCh) {
		msgCache.lastAckMsgChFull = now
		fullCount++
	}
	if cap(msgCache.nackMsgCh) <= len(msgCache.nackMsgCh) {
		msgCache.lastNackMsgChFull = now
		fullCount++
	}
	if cap(msgCache.dlqPublishCh) <= len(msgCache.dlqPublishCh) {
		msgCache.lastDlqPublishChFull = now
		fullCount++
	}
	if cap(msgCache.creditNotifyCh) <= len(msgCache.creditNotifyCh) {
		msgCache.lastCreditNotifyChFull = now
		fullCount++
	}
	if cap(msgCache.creditRequestCh) <= len(msgCache.creditRequestCh) {
		msgCache.lastCreditRequestChFull = now
		fullCount++
	}
	if cap(msgCache.redeliveryTicker.C) <= len(msgCache.redeliveryTicker.C) {
		msgCache.lastRedeliveryTickerFull = now
		fullCount++
	}
	return fullCount
}
