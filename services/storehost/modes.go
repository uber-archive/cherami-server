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
	"math"
	"time"

	"github.com/uber/cherami-server/storage"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

const (
	// special sequence numbers

	// used as the seal-seqnum that indicates that the extent is
	// not sealed
	seqNumNotSealed = int64(math.MaxInt64)

	// used as the seal-seqnum to indicate that extent was sealed
	// at an unspecified seqnum
	seqNumUnspecifiedSeal = int64(math.MaxInt64 - 1)

	// the max allowed seqnum (subject to mode-specific bitmask)
	seqNumMax = int64(math.MaxInt64 - 2)
)

func getModeForDestinationType(destType cherami.DestinationType) Mode {

	switch destType {
	case cherami.DestinationType_PLAIN:
		return AppendOnly

	case cherami.DestinationType_TIMER:
		return TimerQueue

	case cherami.DestinationType_LOG:
		return Log

	case cherami.DestinationType_KAFKA:
		return AppendOnly

	default:
		return Mode(0)
	}
}

// The 64-bit key (or 'address') needs to be _unique_ within the DB for a given
// extent.  To ensure the key is unique, we encode it using the "visibility"
// (or deliver-at) time and the (unique) sequence-number.
//
// For timer-queues, the visibility time would be the sum of the enqueue-time
// on the message and the specified delay. For append-queues, the visibility
// time could be thought to be equal to the enqueue-time, since the messages
// are ready to be delivered as soon as they were received.

// Encoding the timestamp in the key helps with two things:
// 1. we are able to determine if the message is ready to be delivered; which
//    is more important in the timer-queue scenario, since with append-queue
//    the timestamp would always be in the past.
// 2. when we receive a GetAddressFromTimestamp call, we would do a "Seek()"
//    on the underlying storage with a key that uses the provided timestamp
//    (which, btw, is in "seconds" granularity) and a "0" seqNum.

// NB: More the number of bits we use for seq-num, the less 'precise' would the
// visibility time be. That said, the number of bits for seq-num should never be
// greater than '30' (1 second ~= 2^30 nanoseconds), since we would lose the
// "second" level granularity on the delay)

type modeSpecificCallbacks interface {
	messageVisibilityTime(msg *store.AppendMessage) (visibilityTime int64)
	constructKey(visibilityTime int64, seqNum int64) (key storage.Key)
	deconstructKey(key storage.Key) (visibilityTime int64, seqNum int64)
	maxSeqNum() (seqNum int64)
	constructSealExtentKey(seqNum int64) (key storage.Key)
	deconstructSealExtentKey(key storage.Key) (seqNum int64)
	isSealExtentKey(key storage.Key) bool
}

const (
	// for timer-queues, use 38 bits of (visibiilty) timestamp, 26 bits of seqnum
	timerQueueSeqNumBits = 26

	// for append-queues, use 38 bits of timestamp, 26 bits of seqnum
	appendQueueSeqNumBits = 26 // 38 bits timestamp, 26 bits seqnum

)

// -- timer queue helper routines -- //

type timerQueueCallbacks struct {
	seqNumMax        int64 // max seq-num
	seqNumBitmask    int64 // bitmask for seqnum
	timestampBitmask int64 // bitmask for timestamp
}

func getTimerQueueCallbacks(seqNumBits uint) modeSpecificCallbacks {

	seqNumBitmask := (int64(1) << seqNumBits) - 1

	return timerQueueCallbacks{
		seqNumBitmask:    seqNumBitmask,
		timestampBitmask: math.MaxInt64 &^ seqNumBitmask,
		seqNumMax:        seqNumMax & seqNumBitmask,
	}
}

func (t timerQueueCallbacks) messageVisibilityTime(msg *store.AppendMessage) (visibilityTime int64) {
	// make 'visible' after the given delay beyond the enqueue time
	return int64(msg.GetEnqueueTimeUtc() + int64(msg.GetPayload().GetDelayMessageInSeconds())*int64(time.Second))
}

func (t timerQueueCallbacks) constructKey(visibilityTime int64, seqNum int64) storage.Key {
	return storage.Key((visibilityTime & t.timestampBitmask) | (seqNum & t.seqNumBitmask))
}

func (t timerQueueCallbacks) deconstructKey(key storage.Key) (visibilityTime int64, seqNum int64) {
	return int64(int64(key) & t.timestampBitmask), int64(int64(key) & t.seqNumBitmask)
}

func (t timerQueueCallbacks) maxSeqNum() (seqNum int64) {
	return int64(t.seqNumMax)
}

func (t timerQueueCallbacks) constructSealExtentKey(seqNum int64) storage.Key {
	// seal-keys have a timestamp of 'MaxInt64'
	return storage.Key((math.MaxInt64 & t.timestampBitmask) | (seqNum & t.seqNumBitmask))
}

func (t timerQueueCallbacks) deconstructSealExtentKey(key storage.Key) (seqNum int64) {

	seqNum = int64(key) & t.seqNumBitmask

	// we use the special seqnum ('MaxInt64 - 1') when the extent has been sealed
	// at an "unspecified" seqnum; check for this case, and return appropriate value
	if seqNum == (t.seqNumBitmask - 1) {
		seqNum = seqNumUnspecifiedSeal
	}

	return
}

func (t timerQueueCallbacks) isSealExtentKey(key storage.Key) bool {
	return key != storage.InvalidKey && (int64(key)&t.timestampBitmask) == t.timestampBitmask
}

// -- append queue helper routines -- //

type appendQueueCallbacks struct {
	seqNumMax        int64 // max seq-num
	seqNumBitmask    int64 // bitmask for seqnum
	timestampBitmask int64 // bitmask for timestamp
}

func getAppendQueueCallbacks(seqNumBits uint) modeSpecificCallbacks {

	seqNumBitmask := (int64(1) << seqNumBits) - 1

	return appendQueueCallbacks{
		seqNumBitmask:    seqNumBitmask,
		timestampBitmask: math.MaxInt64 &^ seqNumBitmask,
		seqNumMax:        seqNumMax & seqNumBitmask,
	}
}

func (t appendQueueCallbacks) messageVisibilityTime(msg *store.AppendMessage) (visibilityTime int64) {
	// make 'visible' at the enqueue time of the message
	return int64(msg.GetEnqueueTimeUtc())
}

func (t appendQueueCallbacks) constructKey(visibilityTime int64, seqNum int64) storage.Key {
	return storage.Key((visibilityTime & t.timestampBitmask) | (seqNum & t.seqNumBitmask))
}

func (t appendQueueCallbacks) deconstructKey(key storage.Key) (visibilityTime int64, seqNum int64) {
	return int64(int64(key) & t.timestampBitmask), int64(int64(key) & t.seqNumBitmask)
}

func (t appendQueueCallbacks) maxSeqNum() (seqNum int64) {
	return int64(t.seqNumMax)
}

func (t appendQueueCallbacks) constructSealExtentKey(seqNum int64) storage.Key {
	return storage.Key((math.MaxInt64 & t.timestampBitmask) | (seqNum & t.seqNumBitmask))
}

func (t appendQueueCallbacks) deconstructSealExtentKey(key storage.Key) (seqNum int64) {

	seqNum = int64(key) & t.seqNumBitmask

	// we use the special seqnum ('MaxInt64 - 1') when the extent has been sealed
	// at an "unspecified" seqnum; check for this case, and return appropriate value
	if seqNum == (t.seqNumBitmask - 1) {
		seqNum = seqNumUnspecifiedSeal
	}

	return
}

func (t appendQueueCallbacks) isSealExtentKey(key storage.Key) bool {
	return key != storage.InvalidKey && (int64(key)&t.timestampBitmask) == t.timestampBitmask
}
