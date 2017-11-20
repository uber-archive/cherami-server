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
	// "encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

// FIXME: disabling test due to flakiness
func (s *StoreHostSuite) _TestStoreHostTimerQueueWriteWithRead() {

	var numIterations int            // number of iterations of the whole test
	var numExtents int               // number of concurrent extents to use in each iteration
	var minMessages, maxMessages int // number of messages to send (randomly picked from range, per extent)
	var minMsgSize, maxMsgSize int   // size of message payload (randomly picked from range, per message)
	var minMsgDelay, maxMsgDelay int // delay in seconds, to use on message (randomly picked from range, per message)
	var minRandWait, maxRandWait int // wait in milliseconds, between publishing messages (randomly picked from range, per message)

	switch {
	default:
		numIterations = 1
		numExtents = 5                      // number of concurrent extents
		minMessages, maxMessages = 0, 256   // messages to send on an extent
		minMsgSize, maxMsgSize = 1024, 1024 // size of message
		minMsgDelay, maxMsgDelay = 3, 5     // delay to use on messages (in seconds)
		minRandWait, maxRandWait = 0, 100   // wait between messages published (in milliseconds)

	case s.RunShort():
		numIterations = 1
		numExtents = 5                     // number of concurrent extents
		minMessages, maxMessages = 0, 100  // messages to send on an extent
		minMsgSize, maxMsgSize = 512, 2048 // size of message
		minMsgDelay, maxMsgDelay = 2, 4    // delay to use on messages (in seconds)
		minRandWait, maxRandWait = 0, 100  // wait between messages published (in milliseconds)

	case s.RunLong():
		numIterations = 8
		numExtents = 256                    // number of concurrent extents
		minMessages, maxMessages = 0, 1000  // messages to send on an extent
		minMsgSize, maxMsgSize = 512, 2048  // size of message
		minMsgDelay, maxMsgDelay = 5, 300   // delay to use on messages (in seconds)
		minRandWait, maxRandWait = 0, 10000 // wait between messages published (in milliseconds)
	}

	mode := TimerQueue // mode to open extents in

	storehost := s.storehost0

	var wgAll sync.WaitGroup

	for iter := 0; iter < numIterations; iter++ {

		var extent = make([]uuid.UUID, numExtents)
		var statsLatency = make([]*stats, numExtents)
		var statsDrift = make([]*stats, numExtents)

		wgAll.Add(numExtents)
		var readerTimeout int32
		for x := 0; x < numExtents; x++ {
			go func(i int) {

				defer wgAll.Done()

				extent[i] = uuid.NewRandom()

				log.Infof("extent[%d]=%v", i, extent[i])

				statsLatency[i] = &stats{}
				statsDrift[i] = &stats{}

				// randSeed := int64(binary.BigEndian.Uint64([]byte(extent[i])))
				numMessages := minMessages + rand.Intn(maxMessages-minMessages+1)
				var numMsgsWritten int64

				var wgReader sync.WaitGroup

				// -- start the consumer -- //
				{
					out, _ := storehost.OpenReadStreamBuffered(extent[i], mode, 1, numMessages+1)

					timerQueueSeqNumBitmask := (int64(1) << timerQueueSeqNumBits) - 1

					wgReader.Add(1)
					go func() {

						defer wgReader.Done()

						for msg := range out.recvC {

							if msg.GetType() != store.ReadMessageContentType_MESSAGE {
								continue
							}

							appMsg := msg.GetMessage().GetMessage()

							tNow := time.Now().UnixNano()

							// compute the delta from the actual ETA (from user perspective)
							tETA := appMsg.GetEnqueueTimeUtc() + int64(appMsg.GetPayload().GetDelayMessageInSeconds())*time.Second.Nanoseconds()
							tTarget := tETA & ^timerQueueSeqNumBitmask // clear out the seq-num part

							statsDrift[i].put(float64(tNow - tETA))

							// compute the "ETA" that cherami was working towards, to compute the actual latency
							statsLatency[i].put(float64(tNow - tTarget)) // -> this is the time used by store manager

							seqNum := int(appMsg.GetSequenceNumber())

							rnd := rand.New(rand.NewSource(int64(seqNum)))

							dataSize := minMsgSize + rnd.Intn(maxMsgSize-minMsgSize+1)
							delay := minMsgDelay + rnd.Intn(maxMsgDelay-minMsgDelay+1)

							if !validateAppendMessage(appMsg, seqNum, delay, dataSize, rnd) {
								s.Fail("corrupted data")
								log.Errorf("%v: data corrupted; msg seqNum:%x", extent[i], appMsg.GetSequenceNumber())
							}

							s.True(tNow > tTarget)

							if tNow < tTarget {
								s.Fail(fmt.Sprintf("%v: timer fired earlier than expected: tNow=%x tTarget=%x tETA=%x", extent[i], tNow, tTarget, tETA))
								log.Panicf("%v: timer fired earlier than expected: tNow=%x tTarget=%x tETA=%x", extent[i], tNow, tTarget, tETA)
							}

							log.Debugf("%v: recv msg: seq:%d tETA:%x delay:%d (drift:%v)",
								extent[i], appMsg.GetSequenceNumber(), tETA, appMsg.GetPayload().GetDelayMessageInSeconds(),
								time.Unix(0, tNow).Sub(time.Unix(0, tETA)))

							rnd.Int63() // 'pull' a random number (corresponding to the 'wait')
						}

						// ensure we have received all the messages that were written
						s.True(waitFor(10000, func() bool {
							return out.msgsRecv() == int(atomic.LoadInt64(&numMsgsWritten))
						}), "msgsRecv != numMessages")

						log.Infof("%v: recv %d msgs", extent[i], out.recvMsgs)
						log.Infof("%v: latency=%v drift=%v", extent[i], statsLatency[i].timeString(), statsDrift[i].timeString())
					}()

					// send enough credits to get all messages
					out.sendC <- newControlFlow(numMessages)
				}

				// -- start the publisher -- //
				in, inErrC := storehost.OpenAppendStream(extent[i], mode)

				{
					var seqNum int

					for n := 0; n < numMessages; n++ {

						seqNum++

						rnd := rand.New(rand.NewSource(int64(seqNum)))

						dataSize := minMsgSize + rnd.Intn(maxMsgSize-minMsgSize+1)
						delay := minMsgDelay + rnd.Intn(maxMsgDelay-minMsgDelay+1)

						appMsg := newAppendMessage(seqNum, delay, dataSize, rnd)

						in.sendC <- appMsg

						tETA := appMsg.GetEnqueueTimeUtc() + int64(appMsg.GetPayload().GetDelayMessageInSeconds())*time.Second.Nanoseconds()

						wait := minRandWait + rnd.Intn(maxRandWait-minRandWait+1)

						log.Debugf("%v: sent msg: seq:%d delay:%d tETA:%x (wait:%dms)",
							extent[i], appMsg.GetSequenceNumber(), appMsg.GetPayload().GetDelayMessageInSeconds(), tETA, wait)

						time.Sleep(time.Duration(wait) * time.Millisecond)
					}

					// wait until we receive acks for all the messages -- or until OpenAppendStream is completed.
					// when golang's race detector is on, some messages seem to get delayed more than the actual
					// delay on message -- causing inConn to sometimes reject messages when it finds the message
					// delivery time to be in the 'past', which results in the OpenAppendStream failing/completing.
					s.True(waitFor((maxMsgDelay+1)*1000+maxMessages*100, func() bool {
						return in.isDone() || in.acksRecv() == numMessages
					}), "acksRecv != numMessages")

					// update numMsgsWritten to the number of acks received
					atomic.StoreInt64(&numMsgsWritten, int64(in.acksRecv()))

					// if the OpenAppendStream completed, seal extent at the seqnum of the
					// last message that was acked
					if in.isDone() {
						seqNum = in.acksRecv()
					}

					log.Infof("%v: sent %d msgs; recv %d acks (%d naks)", extent[i], in.msgsSent(), in.recvAcks, in.recvNaks)

					// now seal the extent at the seqNum of the last message sent
					waitFor(10000, func() bool {
						return storehost.SealExtent(extent[i], int64(seqNum)) == nil
					})

					// wait 3s for the OpenAppendStream to complete
					s.True(waitFor(3000, func() bool { return isDone(inErrC) }))
				}

				log.Debugf("%v: waiting for OpenReadStream to complete", extent[i])

				timedOut := !common.AwaitWaitGroup(&wgReader, 25*time.Second)
				if timedOut {
					atomic.AddInt32(&readerTimeout, 1)
					fmt.Printf("WgReader wait timeout for extent %d, iter %d\n", i, iter)
				}

				log.Infof("[%d] %v: done", i, extent[i])
			}(x)
		}

		allTimeout := !common.AwaitWaitGroup(&wgAll, 30*time.Second)
		s.Equal(int32(0), atomic.LoadInt32(&readerTimeout), "at least one reader timed out")
		s.False(allTimeout, fmt.Sprintf("wgAll wait timeout for iter %d", iter))

		for i := 0; i < numExtents; i++ {
			log.Debugf("[%d] %v: latency=%v drift=%v", i, extent[i], statsLatency[i].timeString(), statsDrift[i].timeString())
		}
	}
}

// FIXME: disabling test due to flakiness
func (s *StoreHostSuite) _TestStoreHostTimerQueueWriteThenRead() {

	var numIterations int            // number of iterations of the whole test
	var numExtents int               // number of concurrent extents
	var minMessages, maxMessages int // number of messages to send (randomly picked from range, per extent)
	var minMsgSize, maxMsgSize int   // size of message payload (randomly picked from range, per message)
	var minMsgDelay, maxMsgDelay int // delay in seconds, to use on message (randomly picked from range, per message)
	var minRandWait, maxRandWait int // wait in milliseconds, between publishing messages (randomly picked from range, per message)

	switch {
	default:
		numIterations = 1
		numExtents = 5
		minMessages, maxMessages = 0, 256   // messages to send on an extent
		minMsgSize, maxMsgSize = 1024, 1024 // size of message
		minMsgDelay, maxMsgDelay = 3, 5     // delay to use on messages
		minRandWait, maxRandWait = 0, 0     // wait between messages published

	case s.RunShort():
		numIterations = 1
		numExtents = 5
		minMessages, maxMessages = 0, 100  // messages to send on an extent
		minMsgSize, maxMsgSize = 512, 2048 // size of message
		minMsgDelay, maxMsgDelay = 2, 4    // delay to use on messages
		minRandWait, maxRandWait = 0, 100  // wait between messages published

	case s.RunLong():
		numIterations = 8
		numExtents = 256
		minMessages, maxMessages = 0, 1000 // messages to send on an extent
		minMsgSize, maxMsgSize = 512, 2048 // size of message
		minMsgDelay, maxMsgDelay = 5, 300  // delay to use on messages
		minRandWait, maxRandWait = 0, 5000 // wait between messages published
	}

	mode := TimerQueue // mode to open extents in

	storehost := s.storehost0

	var wgAll sync.WaitGroup

	for iter := 0; iter < numIterations; iter++ {

		var extent = make([]uuid.UUID, numExtents)
		var statsLatency = make([]*stats, numExtents)
		var statsDrift = make([]*stats, numExtents)

		wgAll.Add(numExtents)
		var readerTimeout int32
		for x := 0; x < numExtents; x++ {

			go func(i int) {

				defer wgAll.Done()

				extent[i] = uuid.NewRandom()

				log.Infof("extent[%d]=%v", i, extent[i])

				statsLatency[i] = &stats{}
				statsDrift[i] = &stats{}

				// randSeed := int64(binary.BigEndian.Uint64([]byte(extent[i])))
				numMessages := minMessages + rand.Intn(maxMessages-minMessages+1)

				// -- start the publisher -- //
				in, inErrC := storehost.OpenAppendStream(extent[i], mode)

				// -- start the publisher -- //
				{

					var seqNum int

					for n := 0; n < numMessages; n++ {

						seqNum++

						rnd := rand.New(rand.NewSource(int64(seqNum)))

						dataSize := minMsgSize + rnd.Intn(maxMsgSize-minMsgSize+1)
						delay := minMsgDelay + rnd.Intn(maxMsgDelay-minMsgDelay+1)

						appMsg := newAppendMessage(seqNum, delay, dataSize, rnd)

						in.sendC <- appMsg

						tETA := appMsg.GetEnqueueTimeUtc() + int64(appMsg.GetPayload().GetDelayMessageInSeconds())*time.Second.Nanoseconds()

						wait := minRandWait + rnd.Intn(maxRandWait-minRandWait+1)

						log.Debugf("%v: sent msg: seq:%d delay:%d tETA:%x (wait:%dms)",
							extent[i], appMsg.GetSequenceNumber(), appMsg.GetPayload().GetDelayMessageInSeconds(), tETA, wait)

						time.Sleep(time.Duration(wait) * time.Millisecond)
					}

					// wait until we receive acks for all the messages -- or until OpenAppendStream is completed.
					// when golang's race detector is on, some messages seem to get delayed more than the actual
					// delay on message -- causing inConn to sometimes reject messages when it finds the message
					// delivery time to be in the 'past', which results in the OpenAppendStream failing/completing.
					s.True(waitFor((maxMsgDelay+1)*1000+maxMessages*100, func() bool {
						return in.isDone() || in.acksRecv() == numMessages
					}), "acksRecv != numMessages")

					// if the OpenAppendStream completed, seal extent at the seqnum of the
					// last message that was acked
					if in.isDone() {
						seqNum = in.acksRecv()
						numMessages = in.acksRecv()
					}

					log.Infof("%v: sent %d msgs; recv %d acks (%d naks)", extent[i], in.msgsSent(), in.recvAcks, in.recvNaks)

					// now seal the extent at the seqNum of the last message sent
					s.True(waitFor(10000, func() bool {
						return storehost.SealExtent(extent[i], int64(seqNum)) == nil
					}))

					// wait 3s for the OpenAppendStream to complete
					s.True(waitFor(3000, func() bool { return isDone(inErrC) }))
				}

				// -- start the consumer -- //
				{
					out, _ := storehost.OpenReadStreamBuffered(extent[i], mode, 1, numMessages+1)

					timerQueueSeqNumBitmask := (int64(1) << timerQueueSeqNumBits) - 1

					var wgReader sync.WaitGroup

					wgReader.Add(1)
					go func() {

						defer wgReader.Done()

						for msg := range out.recvC {

							if msg.GetType() != store.ReadMessageContentType_MESSAGE {
								continue
							}

							appMsg := msg.GetMessage().GetMessage()

							tNow := time.Now().UnixNano()

							// compute the delta from the actual ETA (from user perspective)
							tETA := appMsg.GetEnqueueTimeUtc() + int64(appMsg.GetPayload().GetDelayMessageInSeconds())*time.Second.Nanoseconds()
							tTarget := tETA & ^timerQueueSeqNumBitmask // clear out the seq-num part

							statsDrift[i].put(float64(tNow - tETA))

							// compute the "ETA" that cherami was working towards, to compute the actual latency
							statsLatency[i].put(float64(tNow - tTarget)) // -> this is the time used by store manager

							seqNum := int(appMsg.GetSequenceNumber())

							rnd := rand.New(rand.NewSource(int64(seqNum)))

							dataSize := minMsgSize + rnd.Intn(maxMsgSize-minMsgSize+1)
							delay := minMsgDelay + rnd.Intn(maxMsgDelay-minMsgDelay+1)

							if !validateAppendMessage(appMsg, seqNum, delay, dataSize, rnd) {
								log.Errorf("%v: data corrupted; msg seqNum:%x", extent[i], appMsg.GetSequenceNumber())
								s.Fail("corrupted data")
							}

							s.True(tNow > tTarget)

							if tNow < tTarget {
								s.Fail(fmt.Sprintf("%v: timer fired earlier than expected: tNow=%x tTarget=%x tETA=%x", extent[i], tNow, tTarget, tETA))
								log.Panicf("%v: timer fired earlier than expected: tNow=%x tTarget=%x tETA=%x", extent[i], tNow, tTarget, tETA)
							}

							log.Debugf("%v: recv msg: seq:%d tETA:%x delay:%d (drift:%v)",
								extent[i], appMsg.GetSequenceNumber(), tETA, appMsg.GetPayload().GetDelayMessageInSeconds(),
								time.Unix(0, tNow).Sub(time.Unix(0, tETA)))
						}

						s.True(waitFor(10000, func() bool { return out.msgsRecv() == numMessages }), "msgsRecv != numMessages")

						log.Infof("%v: recv %d msgs", extent[i], out.recvMsgs)
						log.Infof("%v: latency=%v drift=%v", extent[i], statsLatency[i].timeString(), statsDrift[i].timeString())
					}()

					// send enough credits to get all messages
					out.sendC <- newControlFlow(numMessages)

					log.Debugf("%v: waiting for OpenReadStream to complete", extent[i])
					timedOut := !common.AwaitWaitGroup(&wgReader, 25*time.Second)
					if timedOut {
						atomic.AddInt32(&readerTimeout, 1)
						fmt.Printf("WgReader wait timeout for extent %d, iter %d\n", i, iter)
					}

					log.Infof("[%d] %v: done", i, extent[i])
				}
			}(x)
		}

		allTimeout := !common.AwaitWaitGroup(&wgAll, 30*time.Second)
		s.Equal(int32(0), atomic.LoadInt32(&readerTimeout), "at least one reader timed out")
		s.False(allTimeout, fmt.Sprintf("wgAll wait timeout for iter %d", iter))

		for i := 0; i < numExtents; i++ {
			log.Debugf("[%d] %v: latency=%v drift=%v", i, extent[i], statsLatency[i].timeString(), statsDrift[i].timeString())
		}
	}
}

func (s *StoreHostSuite) TestStoreHostAppendOnlyWriteWithRead() {

	var numIterations int            // number of iterations of the whole test
	var numExtents int               // number of concurrent extents
	var minMessages, maxMessages int // number of messages to send (randomly picked from range, per extent)
	var minMsgSize, maxMsgSize int   // size of message payload (randomly picked from range, per message)
	var minRandWait, maxRandWait int // wait in milliseconds, between publishing messages (randomly picked from range, per message)

	switch {
	default:
		numIterations = 1
		numExtents = 64                     // number of concurrent extents
		minMessages, maxMessages = 0, 1000  // messages to send on an extent
		minMsgSize, maxMsgSize = 1024, 1024 // size of message
		minRandWait, maxRandWait = 0, 0     // wait between messages published (in milliseconds)

	case s.RunShort():
		numIterations = 1
		numExtents = 5
		minMessages, maxMessages = 0, 100  // messages to send on an extent
		minMsgSize, maxMsgSize = 512, 2048 // size of message
		minRandWait, maxRandWait = 0, 0    // wait between messages published (in milliseconds)

	case s.RunLong():
		numIterations = 1
		numExtents = 256                    // 64
		minMessages, maxMessages = 0, 10000 // messages to send on an extent
		minMsgSize, maxMsgSize = 512, 2048  // size of message
		minRandWait, maxRandWait = 0, 10    // wait between messages published (in milliseconds)
	}

	mode := AppendOnly // mode to open extents in

	storehost := s.storehost0

	var wgAll sync.WaitGroup

	for iter := 0; iter < numIterations; iter++ {

		var extent = make([]uuid.UUID, numExtents)
		var statsLatency = make([]*stats, numExtents)
		// FIXME: also compute read/write speeds

		for x := 0; x < numExtents; x++ {

			wgAll.Add(1)

			go func(i int) {

				defer wgAll.Done()

				extent[i] = uuid.NewRandom()

				log.Infof("extent[%d]=%v", i, extent[i])

				statsLatency[i] = &stats{}

				// randSeed := int64(binary.BigEndian.Uint64([]byte(extent[i])))
				numMessages := minMessages + rand.Intn(maxMessages-minMessages+1)

				var wgReader sync.WaitGroup

				// -- start the consumer -- //
				{
					out, _ := storehost.OpenReadStreamBuffered(extent[i], mode, 1, numMessages+1)

					wgReader.Add(1)
					go func() {

						defer wgReader.Done()

						for msg := range out.recvC {

							if msg.GetType() != store.ReadMessageContentType_MESSAGE {
								continue
							}

							appMsg := msg.GetMessage().GetMessage()

							tNow := time.Now().UnixNano()

							// compute the delta from the actual ETA (from user perspective)
							tETA := appMsg.GetEnqueueTimeUtc() // assume the enqueue-time should be the ETA

							statsLatency[i].put(float64(tNow - tETA))

							seqNum := int(appMsg.GetSequenceNumber())

							rnd := rand.New(rand.NewSource(int64(seqNum)))

							dataSize := minMsgSize + rnd.Intn(maxMsgSize-minMsgSize+1)
							delay := 10 + rnd.Intn(10) // random delay between 10-20s (test it has no any effect)

							if !validateAppendMessage(appMsg, seqNum, delay, dataSize, rnd) {
								log.Errorf("%v: data corrupted; msg seqNum:%x", extent[i], appMsg.GetSequenceNumber())
								s.Fail("corrupted data")
							}

							log.Debugf("%v: recv msg: seq:%d enq:%x delay:%ds (latency:%v)",
								extent[i], appMsg.GetSequenceNumber(), tETA, appMsg.GetPayload().GetDelayMessageInSeconds(),
								time.Unix(0, tNow).Sub(time.Unix(0, tETA)))
						}

						s.True(waitFor(10000, func() bool { return out.msgsRecv() == numMessages }), "msgRecv != numMessages")

						log.Infof("%v: recv %d msgs", extent[i], out.recvMsgs)
						log.Infof("%v: latency: %v", extent[i], statsLatency[i].timeString())
					}()

					// send enough credits to get all messages
					out.sendC <- newControlFlow(numMessages)
				}

				// -- start the publisher -- //
				in, inErrC := storehost.OpenAppendStream(extent[i], mode)

				{
					var seqNum int

					for n := 0; n < numMessages; n++ {

						seqNum++

						rnd := rand.New(rand.NewSource(int64(seqNum)))

						dataSize := minMsgSize + rnd.Intn(maxMsgSize-minMsgSize+1)
						delay := 10 + rnd.Intn(10) // use a random delay between 10-20s (test it has no any effect)

						appMsg := newAppendMessage(seqNum, delay, dataSize, rnd)

						in.sendC <- appMsg

						wait := minRandWait + rnd.Intn(maxRandWait-minRandWait+1)

						log.Debugf("%v: sent msg: seq:%d enq:%x delay:%ds (wait:%dms)",
							extent[i], appMsg.GetSequenceNumber(), appMsg.GetEnqueueTimeUtc(),
							appMsg.GetPayload().GetDelayMessageInSeconds(), wait)

						time.Sleep(time.Duration(wait) * time.Millisecond)
					}

					// wait until we receive acks for all the messages
					s.True(waitFor(1000+maxMessages*100, func() bool { return in.acksRecv() == numMessages }), "acksRecv != numMessages")

					log.Infof("%v: sent %d msgs; recv %d acks (%d naks)", extent[i], in.msgsSent(), in.recvAcks, in.recvNaks)

					// now seal the extent at the seqNum of the last message sent
					waitFor(5000, func() bool {
						return storehost.SealExtent(extent[i], int64(seqNum)) == nil
					})

					// wait 3s for the OpenAppendStream to complete
					s.True(waitFor(3000, func() bool { return isDone(inErrC) }), "isDone(inErrC)")
				}

				log.Debugf("%v: waiting for OpenReadStream to complete", extent[i])
				wgReader.Wait()

				log.Infof("%v: done", extent[i])
			}(x)
		}

		wgAll.Wait()

		for i := 0; i < numExtents; i++ {
			log.Debugf("[%d] %v: latency=%v", i, extent[i], statsLatency[i].timeString())
		}
	}
}

func (s *StoreHostSuite) TestStoreHostAppendOnlyWriteThenRead() {

	var numIterations int            // number of iterations of the whole test
	var numExtents int               // number of concurrent extents
	var minMessages, maxMessages int // number of messages to send (randomly picked from range, per extent)
	var minMsgSize, maxMsgSize int   // size of message payload (randomly picked from range, per message)
	var minRandWait, maxRandWait int // wait in milliseconds, between publishing messages (randomly picked from range, per message)

	switch {
	default:
		numIterations = 1
		numExtents = 64                     // number of concurrent extents
		minMessages, maxMessages = 0, 1000  // messages to send on an extent
		minMsgSize, maxMsgSize = 1024, 1024 // size of message
		minRandWait, maxRandWait = 0, 0     // wait between messages published (in milliseconds)

	case s.RunShort():
		numIterations = 1
		numExtents = 5
		minMessages, maxMessages = 0, 100  // messages to send on an extent
		minMsgSize, maxMsgSize = 512, 2048 // size of message
		minRandWait, maxRandWait = 0, 0    // wait between messages published

	case s.RunLong():
		numIterations = 1
		numExtents = 256
		minMessages, maxMessages = 0, 10000 // messages to send on an extent
		minMsgSize, maxMsgSize = 512, 2048  // size of message
		minRandWait, maxRandWait = 0, 10    // wait between messages published
	}

	mode := AppendOnly // mode to open extents in

	storehost := s.storehost0

	var wgAll sync.WaitGroup

	for iter := 0; iter < numIterations; iter++ {

		var extent = make([]uuid.UUID, numExtents)
		var statsLatency = make([]*stats, numExtents)
		// FIXME: also compute read/write speeds

		for x := 0; x < numExtents; x++ {

			wgAll.Add(1)

			go func(i int) {

				defer wgAll.Done()

				extent[i] = uuid.NewRandom()

				log.Infof("extent[%d] =%v", i, extent[i])

				statsLatency[i] = &stats{}

				// randSeed := int64(binary.BigEndian.Uint64([]byte(extent[i])))
				numMessages := minMessages + rand.Intn(maxMessages-minMessages+1)

				// -- start the publisher -- //
				in, inErrC := storehost.OpenAppendStream(extent[i], mode)

				{
					var seqNum int

					for n := 0; n < numMessages; n++ {

						seqNum++

						rnd := rand.New(rand.NewSource(int64(seqNum)))

						dataSize := minMsgSize + rnd.Intn(maxMsgSize-minMsgSize+1)
						delay := 10 + rnd.Intn(10) // use a random delay between 10-20s (test it has no any effect)

						appMsg := newAppendMessage(seqNum, delay, dataSize, rnd)

						in.sendC <- appMsg

						wait := minRandWait + rnd.Intn(maxRandWait-minRandWait+1)

						log.Debugf("%v: sent msg: seq:%d enq:%x delay:%ds (wait:%dms)",
							extent[i], appMsg.GetSequenceNumber(), appMsg.GetEnqueueTimeUtc(),
							appMsg.GetPayload().GetDelayMessageInSeconds(), wait)

						time.Sleep(time.Duration(wait) * time.Millisecond)
					}

					// wait until we receive acks for all the messages
					s.True(waitFor(1000+maxMessages*100, func() bool { return in.acksRecv() == numMessages }), "acksRecv != numMessages")

					log.Infof("%v: sent %d msgs; recv %d acks (%d naks)", extent[i], in.msgsSent(), in.recvAcks, in.recvNaks)

					// now seal the extent at the seqNum of the last message sent
					waitFor(5000, func() bool {
						return storehost.SealExtent(extent[i], int64(seqNum)) == nil
					})

					// wait 3s for the OpenAppendStream to complete
					s.True(waitFor(3000, func() bool { return isDone(inErrC) }), "isDone(inErrC)")
				}

				{
					// -- start the reader -- //
					out, _ := storehost.OpenReadStreamBuffered(extent[i], mode, 1, numMessages+1)

					var wgReader sync.WaitGroup

					wgReader.Add(1)
					go func() {

						defer wgReader.Done()

						for msg := range out.recvC {

							if msg.GetType() != store.ReadMessageContentType_MESSAGE {
								continue
							}

							appMsg := msg.GetMessage().GetMessage()
							tNow := time.Now().UnixNano()

							// compute the delta from the actual ETA (from user perspective)
							tETA := appMsg.GetEnqueueTimeUtc() // assume the enqueue-time should be the ETA

							statsLatency[i].put(float64(tNow - tETA))

							seqNum := int(appMsg.GetSequenceNumber())

							rnd := rand.New(rand.NewSource(int64(seqNum)))

							dataSize := minMsgSize + rnd.Intn(maxMsgSize-minMsgSize+1)
							delay := 10 + rnd.Intn(10) // use a random delay between 10-20s (test it has no any effect)

							if !validateAppendMessage(appMsg, seqNum, delay, dataSize, rnd) {
								log.Errorf("%v: data corrupted; msg seqNum:%x", extent[i], appMsg.GetSequenceNumber())
								s.Fail("corrupted data")
							}

							log.Debugf("%v: recv msg: seq:%d tETA:%x delay:%d (latency:%v)",
								extent[i], appMsg.GetSequenceNumber(), tETA, appMsg.GetPayload().GetDelayMessageInSeconds(),
								time.Unix(0, tNow).Sub(time.Unix(0, tETA)))
						}

						s.True(waitFor(10000, func() bool { return out.msgsRecv() == numMessages }), "msgsRecv != numMessages")

						log.Infof("%v: recv %d msgs", extent[i], out.recvMsgs)
						log.Infof("%v: latency: %v", extent[i], statsLatency[i].timeString())
					}()

					// send enough credits to get all messages
					out.sendC <- newControlFlow(numMessages)

					log.Debugf("%v: waiting for OpenReadStream to complete", extent[i])
					wgReader.Wait()

					log.Infof("%v: done", extent[i])
				}
			}(x)
		}

		wgAll.Wait()

		for i := 0; i < numExtents; i++ {
			log.Debugf("[%d] %v: latency=%v", i, extent[i], statsLatency[i].timeString())
		}
	}
}

func (s *StoreHostSuite) TestStoreHostAppendOnlyWriteThenDoReadMessages() {

	var numIterations int // number of iterations of the whole test
	var numExtents int    // number of concurrent extents
	// number of messages to send (randomly picked from range, per extent)
	minMessages := 0
	maxMessages := 30
	var minMsgSize, maxMsgSize int   // size of message payload (randomly picked from range, per message)
	var minRandWait, maxRandWait int // wait in milliseconds, between publishing messages (randomly picked from range, per message)

	switch {
	default:
		numIterations = 1
		numExtents = 64                     // number of concurrent extents
		minMsgSize, maxMsgSize = 1024, 1024 // size of message
		minRandWait, maxRandWait = 0, 0     // wait between messages published (in milliseconds)

	case s.RunShort():
		numIterations = 1
		numExtents = 5
		minMsgSize, maxMsgSize = 512, 2048 // size of message
		minRandWait, maxRandWait = 0, 0    // wait between messages published

	case s.RunLong():
		numIterations = 1
		numExtents = 256
		minMsgSize, maxMsgSize = 512, 2048 // size of message
		minRandWait, maxRandWait = 0, 10   // wait between messages published
	}

	mode := AppendOnly // mode to open extents in

	storehost := s.storehost0

	var wgAll sync.WaitGroup

	for iter := 0; iter < numIterations; iter++ {

		var extent = make([]uuid.UUID, numExtents)
		var statsLatency = make([]*stats, numExtents)
		// FIXME: also compute read/write speeds

		for x := 0; x < numExtents; x++ {

			wgAll.Add(1)

			go func(i int) {

				defer wgAll.Done()

				extent[i] = uuid.NewRandom()

				log.Infof("extent[%d] =%v", i, extent[i])

				statsLatency[i] = &stats{}

				// randSeed := int64(binary.BigEndian.Uint64([]byte(extent[i])))
				numMessages := minMessages + rand.Intn(maxMessages-minMessages+1)

				// -- start the publisher -- //
				in, inErrC := storehost.OpenAppendStream(extent[i], mode)

				// remember addresses of messages; seqNum starts from 1
				sendAddrs := make([]int64, numMessages+1)

				go func() { // go routine that remembers addresses for each message
					for ack := range in.recvC {
						atomic.StoreInt64(&sendAddrs[ack.GetSequenceNumber()], ack.GetAddress())
					}
				}()

				var sealExtent bool

				{
					var seqNum int

					for n := 0; n < numMessages; n++ {

						seqNum++

						rnd := rand.New(rand.NewSource(int64(seqNum)))

						dataSize := minMsgSize + rnd.Intn(maxMsgSize-minMsgSize+1)
						delay := 10 + rnd.Intn(10) // use a random delay between 10-20s (test it has no any effect)

						appMsg := newAppendMessage(seqNum, delay, dataSize, rnd)

						in.sendC <- appMsg

						wait := minRandWait + rnd.Intn(maxRandWait-minRandWait+1)

						log.Debugf("%v: sent msg: seq:%d enq:%x delay:%ds (wait:%dms)",
							extent[i], appMsg.GetSequenceNumber(), appMsg.GetEnqueueTimeUtc(),
							appMsg.GetPayload().GetDelayMessageInSeconds(), wait)

						time.Sleep(time.Duration(wait) * time.Millisecond)
					}

					// wait until we receive acks for all the messages
					s.True(waitFor(1000+maxMessages*100, func() bool { return in.acksRecv() == numMessages }), "acksRecv != numMessages")

					log.Infof("%v: sent %d msgs; recv %d acks (%d naks)", extent[i], in.msgsSent(), in.recvAcks, in.recvNaks)

					// now seal the extent at the seqNum of the last message sent
					rnd := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
					sealExtent = rnd.Intn(100) > 50
					if sealExtent {
						// now seal the extent at the seqNum of the last message sent
						var err error

						waitFor(5000, func() bool {
							err = storehost.SealExtent(extent[i], int64(seqNum))
							return err == nil
						})

						if err != nil {
							sealExtent = false
						}
					} else {

						// return EOF and close stream
						close(in.sendC)
					}

					// wait 3s for the OpenAppendStream to complete
					waitFor(3000, func() bool { return isDone(inErrC) })
				}

				{
					// -- start the reader -- //

					msgs, _ := storehost.ReadMessages(
						extent[i],
						store.ADDR_BEGIN, /* startAddr */
						10,               /* numMessages */
						true,             /* startAddressInclusive */
					)

					countReturned := len(msgs)
					log.Infof("%v: numMessages: %d, returned: %d seal: %t", extent[i], numMessages, countReturned, sealExtent)
					verifyMsgs := func() {
						if numMessages < 10 {
							s.Equal(
								countReturned,
								numMessages+1,
								fmt.Sprintf("when request more than the extent (%v) has, should return one extra info/error message", extent[i]))

							if sealExtent {
								s.NotNil(
									msgs[numMessages].GetSealed(),
									fmt.Sprintf("last message should indicate extent sealed (%v)", extent[i]))
							} else {
								s.NotNil(
									msgs[numMessages].GetNoMoreMessage(),
									fmt.Sprintf("last message should indicate there is no more messages in extent (%v)", extent[i]))
							}
						} else {
							s.Equal(countReturned, 10, fmt.Sprintf("extent (%v) read starts from 0, msgsRecv != requested", extent[i]))
						}
					}
					verifyMsgs()

					if len(sendAddrs) > 1 { // there is a start address we can use for testing
						msgs, _ = storehost.ReadMessages(extent[i], atomic.LoadInt64(&sendAddrs[1]), 10, true)
						verifyMsgs()
					}

					log.Infof("%v: done", extent[i])
				}
			}(x)
		}

		wgAll.Wait()
	}
}

func (s *StoreHostSuite) TestStoreHostSealExtent() {

	mode := AppendOnly // AppendOnly
	// numExtents := 1
	numMessages := 20
	dataSize := 1024

	// setup test
	t := s.storehost0

	{
		extent := uuid.NewRandom()

		// -1: try sealing a non-existent extent
		// {
		// 	err := t.SealExtent(extent, math.MaxInt64 - 1)

		// 	// we should get a ExtentNotFoundError
		// 	_, ok := err.(*cherami.ExtentNotFoundError)
		// 	s.True( ok)
		// }

		// start the first consumer
		out0, out0ErrC := t.OpenReadStream(extent, mode)

		// start producer
		var in *inhostStreamMock
		var inErrC <-chan error

		// 0: start two produceers, one of them should fail!
		{
			log.Debugf("starting two producers:") // dbg
			in0, in0ErrC := t.OpenAppendStream(extent, mode)
			in1, in1ErrC := t.OpenAppendStream(extent, mode)

			// - OpenAppendStream should fail (second append stream)
			var err error
			select {
			case err = <-in0ErrC:
				in, inErrC = in1, in1ErrC // use the other one
			case err = <-in1ErrC:
				in, inErrC = in0, in0ErrC // use the other one
			case <-time.After(5 * time.Second):
			}

			log.Debugf("got error=%v", err) // dbg

			// - OpenAppendStream should have got an InternalServiceError
			_, ok := err.(*cherami.InternalServiceError)
			s.True(ok)
		}

		log.Debugf("sending credits") // dbg
		// now send some (extra) credits
		out0.sendC <- newControlFlow(numMessages + 5)

		log.Debugf("sending messages") // dbg
		// .. and messages
		var e error
	loop:
		for i := 0; i < numMessages; i++ {
			select {
			case in.sendC <- newAppendMessage(i+1, 0, dataSize, nil):
			case e = <-inErrC:
				break loop
			}
		}

		// no errors expected
		s.Equal(nil, e)

		// seqnum of last message sent
		lastSeqNum, sealSeqNum := int64(numMessages), int64(math.MaxInt64-1)

		log.Debugf("waiting until messages have been received") // dbg

		// wait until all messages have been received
		s.True(waitFor(5000, func() bool { return out0.msgsRecv() == numMessages }))

		// 1: send a SealExtent for a seqNum larger than lastSeqNum
		// {
		// 	err := t.SealExtent(extent, lastSeqNum+1)

		// 	// - we should get an ExtentFailedToSealError with lastSeqNum == numMessages
		// 	eftse, ok := err.(*cherami.ExtentFailedToSealError)
		// 	s.True( ok) // ensure we got an ExtentFailedToSealError
		// 	if ok {
		// 		s.Equal(lastSeqNum, eftse.GetLastSequenceNumber()) // validate lastSeqNum is as expected
		// 	}

		// 	// - the OpenAppendStream should not close
		// 	s.False(out0.isDone())
		// }

		// 2: send a SealExtent for a seqNum equal to lastSeqNum
		{
			log.Debugf("sending seal for seqnum=%x", lastSeqNum) // dbg

			err := t.SealExtent(extent, lastSeqNum)

			// - this should succeed
			s.Equal(nil, err)

			sealSeqNum = lastSeqNum // sealSeqNum should now be lastSeqNum

			// - OpenAppendStream should detect the seal-extent and close/fail
			select {
			case err = <-inErrC:
			case <-time.After(3 * time.Second):
			}

			log.Debugf("OpenAppendStream returned err=%v", err) // dbg

			// - OpenAppendStream should have got an ExtentSealedError
			ese, ok := err.(*store.ExtentSealedError)
			s.True(ok)
			if ok {
				// - ensure seqNum is equal to sealSeqNum
				s.Equal(sealSeqNum, ese.GetSequenceNumber())
			}

			// - OpenReadStream should detect seal-extent and complete
			select {
			case <-out0ErrC:
			case <-time.After(3 * time.Second):
			}

			// OpenReadStream should have got a "sealed" message
			s.True(out0.isDone())
			s.Equal(store.ReadMessageContentType_SEALED, out0.lastMsg().GetType())
			s.Equal(sealSeqNum, out0.lastMsg().GetSealed().GetSequenceNumber())
		}

		// 3: send a SealExtent for a seqNum less than oldSealSeqNum
		{
			err := t.SealExtent(extent, lastSeqNum-1)

			// - this should succeed
			s.Equal(nil, err)

			sealSeqNum = lastSeqNum - 1
		}

		// 4: send a SealExtent for a seqNum greater than new oldSealSeqNum
		// {
		// 	err := t.SealExtent(extent, lastSeqNum)

		// 	// - this should fail with an ExtentSealedError
		// 	ese, ok := err.(*cherami.ExtentSealedError)
		// 	s.True( ok) // ensure we got an ExtentSealedError
		// 	if ok {
		// 		// - ensure seqNum is equal to sealSeqNum
		// 		s.Equal(sealSeqNum, ese.GetSequenceNumber())
		// 	}
		// }

		// 5: start another consumer, read all messages until seal seqnum
		{
			out1, out1ErrC := t.OpenReadStream(extent, mode)

			out1.sendC <- newControlFlow(numMessages) // send credits

			// - OpenReadStream should detect seal-extent and complete
			select {
			case <-out1ErrC:
			case <-time.After(5 * time.Second):
			}

			// OpenReadStream should have got a "sealed" message
			s.True(out1.isDone())
			s.Equal(store.ReadMessageContentType_SEALED, out1.lastMsg().GetType())
			s.Equal(sealSeqNum, out1.lastMsg().GetSealed().GetSequenceNumber())

			// we should still receive _all_ the messages
			s.Equal(int32(lastSeqNum), out1.recvMsgs)
		}
	}

	// 6: behaviour when SealExtent comes in before read/write on an extent
	{
		extent := uuid.NewRandom()

		var sealSeqNum int64

		// - try sealing on an hitherto "unknown" extent; should succeed
		{
			err := t.SealExtent(extent, math.MaxInt64-1)

			// we should succeed!
			s.Equal(nil, err)

			sealSeqNum = math.MaxInt64 - 1
		}

		// - start a consumer; should fail
		{
			out2, out2ErrC := t.OpenReadStream(extent, mode)

			select {
			case <-out2ErrC:
			case <-time.After(5 * time.Second):
			}

			// OpenReadStream should have got a "sealed" message
			s.True(out2.isDone())
			s.Equal(store.ReadMessageContentType_SEALED, out2.lastMsg().GetType())
			s.Equal(sealSeqNum, out2.lastMsg().GetSealed().GetSequenceNumber())

			// no messages should have been returned
			s.Equal(int32(0), out2.recvMsgs)
		}

		// - start producer; should fail
		{
			_, inErrC := t.OpenAppendStream(extent, mode)
			err := <-inErrC

			// - OpenAppendStream should have got an InternalServiceError,
			// since we are attempting to open a sealed extent
			_, ok := err.(*cherami.InternalServiceError)
			s.True(ok) // ensure it is an InternalServiceError
		}
	}
}

func (s *StoreHostSuite) TestStoreHostSealExtentThrottling() {

	totalRequests := 1000

	t := s.storehost0

	{
		extent := uuid.NewRandom()

		var unthrottledRequests int32
		var throttledRequests int32

		t0 := time.Now()

		for i := 0; i < totalRequests; i++ {

			go func() {
				err := t.SealExtent(extent, math.MaxInt64-1)

				_, ok := err.(*cherami.InternalServiceError)

				if !ok {
					atomic.AddInt32(&unthrottledRequests, 1)
				} else {
					atomic.AddInt32(&throttledRequests, 1)
				}
			}()

			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
		}

		waitFor(5000, func() bool {
			return (int32(totalRequests) == atomic.LoadInt32(&unthrottledRequests)+atomic.LoadInt32(&throttledRequests))
		})

		dt := time.Since(t0)

		maxExpectedUnthrottledRequests := int32((2 + dt.Nanoseconds()/sealExtentThrottlePeriod.Nanoseconds()) * sealExtentThrottleRequests)

		log.Debugf("unthrottledRequests=%d (of total=%d in time %v) maxExpectedUnthrottledRequests=%d", unthrottledRequests, totalRequests, dt, maxExpectedUnthrottledRequests)
		s.True(unthrottledRequests <= maxExpectedUnthrottledRequests)
	}
}

func (s *StoreHostSuite) TestStoreHostPurgeMessages() {

	mode := AppendOnly // TimerQueue
	// numExtents := 1
	numMessages := 32
	dataSize := 1024

	// setup test
	t := s.storehost0

	extent := uuid.NewRandom()

	in, inErrC := t.OpenAppendStream(extent, mode)

	// remember addresses of messages
	sendAddrs := make([]int64, numMessages+1)
	go func() { // go routine that remembers addresses for each message
		for ack := range in.recvC {
			atomic.StoreInt64(&sendAddrs[ack.GetSequenceNumber()], ack.GetAddress())
		}
	}()

	var n int

	// send 10 messages
	for n = 0; n < 10; n++ {
		in.sendC <- newAppendMessage(n+1, 0, dataSize, nil)
	}

	// wait 3s until they have all been ack-ed
	s.True(waitFor(3000, func() bool { return in.acksRecv() == n }))

	// start a consumer
	out0, out0ErrC := t.OpenReadStream(extent, mode)
	{
		// receive addresses
		recv0Addrs := make([]int64, numMessages+1)
		go func() {
			for msg := range out0.recvC {
				if msg.GetType() == store.ReadMessageContentType_MESSAGE {
					atomic.StoreInt64(&recv0Addrs[msg.GetMessage().GetMessage().GetSequenceNumber()], msg.GetMessage().GetAddress())
				}
			}
		}()

		// send 3 credits
		out0.sendC <- newControlFlow(3)

		// wait until 3 msgs have been received (or 3 seconds)
		s.True(waitFor(3000, func() bool { return out0.msgsRecv() == 3 }))

		// delete the first 6 messages
		next, err := t.PurgeMessages(extent, atomic.LoadInt64(&sendAddrs[6]))

		s.Equal(nil, err)
		s.Equal(atomic.LoadInt64(&sendAddrs[7]), next)

		// send 3 more credits
		out0.sendC <- newControlFlow(3)

		// wait until 3 msgs have been received
		s.True(waitFor(3000, func() bool { return out0.msgsRecv() == 6 }))

		// 4, 5, 6 should be 0
		s.True(atomic.LoadInt64(&recv0Addrs[4]) == 0 && atomic.LoadInt64(&recv0Addrs[4]) != atomic.LoadInt64(&sendAddrs[4]))
		s.True(atomic.LoadInt64(&recv0Addrs[5]) == 0 && atomic.LoadInt64(&recv0Addrs[5]) != atomic.LoadInt64(&sendAddrs[5]))
		s.True(atomic.LoadInt64(&recv0Addrs[6]) == 0 && atomic.LoadInt64(&recv0Addrs[6]) != atomic.LoadInt64(&sendAddrs[6]))

		// 7, 8, 9 should be valid
		s.True(atomic.LoadInt64(&recv0Addrs[7]) != 0 && atomic.LoadInt64(&recv0Addrs[7]) == atomic.LoadInt64(&sendAddrs[7]))
		s.True(atomic.LoadInt64(&recv0Addrs[8]) != 0 && atomic.LoadInt64(&recv0Addrs[8]) == atomic.LoadInt64(&sendAddrs[8]))
		s.True(atomic.LoadInt64(&recv0Addrs[9]) != 0 && atomic.LoadInt64(&recv0Addrs[9]) == atomic.LoadInt64(&sendAddrs[9]))

		s.False(out0.isDone())
	}

	// send 5 more messages
	for ; n < 15; n++ {
		in.sendC <- newAppendMessage(n+1, 0, dataSize, nil)
	}

	// wait until they have all been ack-ed
	s.True(waitFor(3000, func() bool { return in.acksRecv() == n }))

	// start another consumer
	out1, out1ErrC := t.OpenReadStream(extent, mode)
	{
		// receive addresses
		recv1Addrs := make([]int64, numMessages+1)
		go func() {
			for msg := range out1.recvC {
				if msg.GetType() == store.ReadMessageContentType_MESSAGE {
					atomic.StoreInt64(&recv1Addrs[msg.GetMessage().GetMessage().GetSequenceNumber()], msg.GetMessage().GetAddress())
				}
			}
		}()

		// send 'n' credits
		out1.sendC <- newControlFlow(n)

		// we should get 15 - 6 = 9 messages
		// wait for them
		s.True(waitFor(3000, func() bool { return out1.msgsRecv() == 9 }))
		time.Sleep(time.Second) // wait a while more

		s.True(atomic.LoadInt32(&out1.recvMsgs) == 9)

		// 7, 8, 9 .. 15 should be valid
		s.True(atomic.LoadInt64(&recv1Addrs[6]) == 0 && atomic.LoadInt64(&recv1Addrs[6]) != atomic.LoadInt64(&sendAddrs[6]))
		s.True(atomic.LoadInt64(&recv1Addrs[7]) != 0 && atomic.LoadInt64(&recv1Addrs[7]) == atomic.LoadInt64(&sendAddrs[7]))
		s.True(atomic.LoadInt64(&recv1Addrs[15]) != 0 && atomic.LoadInt64(&recv1Addrs[15]) == atomic.LoadInt64(&sendAddrs[15]))

		s.False(out1.isDone())
	}

	// delete the first 15, ie, all messages
	next, err := t.PurgeMessages(extent, atomic.LoadInt64(&sendAddrs[15]))

	s.Equal(nil, err)
	s.Equal(int64(store.ADDR_END), next)

	// start another consumer
	out2, out2ErrC := t.OpenReadStream(extent, mode)

	{
		// receive addresses
		recv2Addrs := make([]int64, numMessages+1)
		go func() {
			for msg := range out2.recvC {
				if msg.GetType() == store.ReadMessageContentType_MESSAGE {
					atomic.StoreInt64(&recv2Addrs[msg.GetMessage().GetMessage().GetSequenceNumber()], msg.GetMessage().GetAddress())
				}
			}
		}()

		// send 'n' credits
		out2.sendC <- newControlFlow(n)

		// we should get no messages
		time.Sleep(time.Second)
		s.True(out2.msgsRecv() == 0)

		// 7, 8, 9 .. 15 should be valid
		s.True(atomic.LoadInt64(&recv2Addrs[15]) == 0 && atomic.LoadInt64(&recv2Addrs[15]) != atomic.LoadInt64(&sendAddrs[15]))

		s.False(out2.isDone())
	}

	// delete the entire extent
	next, err = t.PurgeMessages(extent, store.ADDR_SEAL)

	s.Equal(nil, err)
	s.Equal(int64(store.ADDR_BEGIN), next)

	{
		// we should see an "extent not found" error
		_, err = t.GetAddressFromTimestamp(extent, time.Now().UnixNano())
		_, ok := err.(*store.ExtentNotFoundError)
		s.True(ok)
	}

	s.True(waitFor(3000, func() bool { return isDone(inErrC) }))

	s.True(waitFor(3000, func() bool { return isDone(out0ErrC) }))

	s.True(waitFor(3000, func() bool { return isDone(out1ErrC) }))

	s.True(waitFor(3000, func() bool { return isDone(out2ErrC) }))
}

func (s *StoreHostSuite) TestStoreHostConcurrency() {

	mode := AppendOnly
	numExtents := 3
	numMessages := 200
	dataSize := 1024

	numRuns := 1000

	numParallel := 3

	// setup test
	t := s.storehost0
	// t := s.storehost0

	extents := make([]uuid.UUID, numExtents)

	for i := range extents {
		extents[i] = uuid.NewRandom()
	}

	{
		extent := extents[rand.Intn(numExtents)]

		log.Debugf("OpenAppendStream(%v)", extent)
		in, errC := t.OpenAppendStream(extent, mode)

	loop1:
		for i := 0; i < numMessages; i++ {
			select {
			case in.sendC <- newAppendMessage(i+1, 0, dataSize, nil):
			case err := <-errC:
				log.Debugf("[DONE} OpenAppendStream(%v): err=%v", extent, err)
				break loop1
			}
			// time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		}

		// wait until all messages have been received
	loop2:
		for atomic.LoadInt32(&in.recvAcks) < int32(numMessages) {
			select {
			case <-time.After(100 * time.Millisecond):
			case err := <-errC:
				log.Debugf("[DONE} OpenAppendStream(%v): err=%v", extent, err)
				break loop2
			}
		}

		t.SealExtent(extent, int64(numMessages))

		// wait for completion and print error, if any (on a separate go-routine)
		log.Debugf("[DONE} OpenAppendStream(%v): err=%v", extent, <-errC)
	}

	var wg sync.WaitGroup

	for p := 0; p < numParallel; p++ {
		wg.Add(1)
		go func(id int) {

			for j := 0; j < numRuns; j++ {

				extent := extents[rand.Intn(numExtents)]

				r := rand.Intn(6)

				switch r {
				case 0:
					timestamp := time.Now().UnixNano() - int64(rand.Int31())

					log.Debugf("[%d: %d] GetAddressFromTimestamp(%v, %x)", id, j, extent, timestamp)
					addr, err := t.GetAddressFromTimestamp(extent, timestamp)

					log.Debugf("[%d: %d DONE] GetAddressFromTimestamp(%v, %x): addr=%x err=%v", id, j, extent, timestamp, addr, err)

				case 1:
					sealSeqNum := rand.Int63n(int64(numMessages))

					log.Debugf("[%d: %d] SealExtent(%v, %x)", id, j, extent, sealSeqNum)
					err := t.SealExtent(extent, sealSeqNum)

					log.Debugf("[%d: %d DONE] SealExtent(%v, %x): err=%v", id, j, extent, sealSeqNum, err)

				case 2:
					sealSeqNum := int64(math.MaxInt64 - 1)

					log.Debugf("[%d: %d] SealExtent(%v, %x)", id, j, extent, sealSeqNum)
					err := t.SealExtent(extent, sealSeqNum)

					log.Debugf("[%d: %d DONE] SealExtent(%v, %x): err=%v", id, j, extent, sealSeqNum, err)

				case 3:
					purgeAddr := time.Now().UnixNano() - rand.Int63n(int64(10*time.Second))

					log.Debugf("[%d: %d] PurgeMessages(%v, %x)", id, j, extent, purgeAddr)
					err, addr := t.PurgeMessages(extent, purgeAddr)

					log.Debugf("[%d: %d DONE] PurgeMessages(%v, %x): addr=%x, err=%v", id, j, extent, purgeAddr, addr, err)

				case 4:
					log.Debugf("[%d: %d] OpenReadStream(%v)", id, j, extent)
					out, errC := t.OpenReadStream(extent, mode)

					// send some credits
					out.sendC <- newControlFlow(numMessages)

					// wait for completion and print error, if any (on a separate go-routine)
					wg.Add(1)
					go func(k int) {
						log.Debugf("[%d: %d DONE] OpenReadStream(%v): err=%v", id, k, extent, <-errC)
						wg.Done()
					}(j)

				case 5:
					log.Debugf("[%d: %d] OpenAppendStream(%v)", id, j, extent)
					in, errC := t.OpenAppendStream(extent, mode)

					var err error

				loop:
					for i := 0; i < numMessages; i++ {
						select {
						case in.sendC <- newAppendMessage(i+1, 0, dataSize, nil):
						case err = <-errC:
							break loop
						}
						// time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
					}

					if err == nil {
						log.Debugf("[%d: %d DONE]OpenAppendStream(%v): err=%v", id, j, extent, err)
					} else {
						// wait for completion and print error, if any (on a separate go-routine)
						wg.Add(1)
						go func(k int) {
							log.Debugf("[%d: %d DONE]OpenAppendStream(%v): err=%v", id, k, extent, <-errC)
							wg.Done()
						}(j)
					}
				}
			}

			wg.Done()
		}(p)
	}

	doneC := make(chan struct{})

	go func() {
		wg.Wait()
		close(doneC)
	}()

	// watchdog thread .. to help "deadlocks" (make readers/writers to complete)
	for j := 0; true; j++ {
		select {
		case <-doneC:
			return
		case <-time.After(100 * time.Millisecond):

			extent := extents[rand.Intn(numExtents)]
			sealSeqNum := int64(math.MaxInt64 - 1)

			log.Debugf("[watchdog: %d] SealExtent(%v, %x)", j, extent, sealSeqNum)
			err := t.SealExtent(extent, sealSeqNum)

			log.Debugf("[watchdog: %d DONE] SealExtent(%v, %x): err=%v", j, extent, sealSeqNum, err)
		}
	}
}

func (s *StoreHostSuite) TestStoreHostMaxSeqNum() {

	mode := AppendOnly // TimerQueue
	// numExtents := 1
	numMessages := 16
	dataSize := 1024

	// setup test
	t := s.storehost0

	extent := uuid.NewRandom()

	in, errC := t.OpenAppendStream(extent, mode)

	var seqNum = int(getAppendQueueCallbacks(appendQueueSeqNumBits).maxSeqNum()) - numMessages

	// send messages with seqNum that goes beyond 'maxSeqNum'
	for n := 0; n < numMessages+5 && !in.isDone(); n++ {
		in.sendC <- newAppendMessage(seqNum, 0, dataSize, nil)
		seqNum++
	}

	// wait 3s for the OpenAppendStream to complete
	s.True(waitFor(3000, func() bool { return isDone(errC) }))

	s.Equal(in.recvAcks, int32(numMessages))
}

func (s *StoreHostSuite) TestStoreHostManyManyExtents() {

	var numExtents int

	switch {
	default:
		numExtents = 512

	case s.RunShort():
		numExtents = 128

	case s.RunLong():
		numExtents = 100000 // NB: this will not work with '-race'; the go race detector is limited to 8192 go-routines
	}

	if numExtents*9 > 10000 {
		debug.SetMaxThreads(9 * numExtents)
	}

	storehost := s.storehost0

	extent := make([]uuid.UUID, numExtents)
	mode := make([]Mode, numExtents)

	for i := 0; i < numExtents; i++ {
		extent[i] = uuid.NewRandom()

		switch rand.Intn(2) {
		case 0:
			mode[i] = AppendOnly
		case 1:
			mode[i] = TimerQueue
		}
	}

	out := make([]*outhostStreamMock, numExtents)
	outErrC := make([]<-chan error, numExtents)

	in := make([]*inhostStreamMock, numExtents)
	inErrC := make([]<-chan error, numExtents)

	// open read/append streams
	for i := 0; i < numExtents; i++ {

		log.Debugf("\r%d: OpenReadStream(%v)      ", i, extent[i])
		out[i], outErrC[i] = storehost.OpenReadStream(extent[i], mode[i])

		log.Debugf("\r%d: OpenAppendStream(%v)", i, extent[i])
		in[i], inErrC[i] = storehost.OpenAppendStream(extent[i], mode[i])
	}

	/*
		// open append streams
		for i := 0; i < numExtents; i++ {

			log.Debugf("\r%d: OpenAppendStream(%v)", i, extent[i])
			in[i], inErrC[i] = storehost.OpenAppendStream(extent[i], mode[i])
		}

		fmt.Println()
	*/

	// seal extents
	for i := 0; i < numExtents; i++ {

		log.Debugf("\r%d: SealExtent(%v)       ", i, extent[i])

		for storehost.SealExtent(extent[i], 0) != nil {
			log.Debugf("\r%d: retrying SealExtent(%v)", i, extent[i])
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		}
	}

	// wait for streams to be done
	for i := 0; i < numExtents; i++ {

		log.Debugf("\r%d: waiting for OpenAppendStream(%v) to be done", i, extent[i])

		s.True(waitFor(5000, func() bool { return isDone(inErrC[i]) }),
			"OpenAppendStream (%d) %v not done after 5 secs", i, extent[i])
	}

	for i := 0; i < numExtents; i++ {

		log.Debugf("\r%d: waiting for OpenReadStream(%v) to be done    ", i, extent[i])

		s.True(waitFor(5000, func() bool { return isDone(outErrC[i]) }),
			"OpenReadStream (%d) %v not done after 5 secs", i, extent[i])
	}
}

func (s *StoreHostSuite) TestStoreHostReplicateExtent() {

	mode := AppendOnly // TimerQueue
	dataSize := 1024

	var numMessages int
	switch {
	default:
		numMessages = 256
	case s.RunShort():
		numMessages = 64
	case s.RunLong():
		numMessages = 4096
	}

	// start up two new stores
	store0, store1 := s.storehost0, s.storehost1

	log.Debugf("TestStoreHostReplicateExtent storehost0: uuid=%v hostPort=%v wsPort=%d", store0.hostID, store0.hostPort, store0.wsPort)
	log.Debugf("TestStoreHostReplicateExtent storehost1: uuid=%v hostPort=%v wsPort=%d", store1.hostID, store1.hostPort, store1.wsPort)

	extent := uuid.NewRandom() // random extent

	// == 1. start a consumer on the second store for the extent ==
	out, outErrC := store1.OpenReadStream(extent, mode)
	log.Debugf("[%v].OpenReadStream(%v) = %p", store1.hostID, extent, out)

	// == 2. start a producer on the first store for the extent ==
	in, inErrC := store0.OpenAppendStream(extent, mode)
	log.Debugf("[%v].OpenAppendStream(%v) = %p", store0.hostID, extent, in)

	// == 3. initiate replication of extent from first replica to the second ==

	// HACK: set $(CHERAMI_STOREHOST_WS_PORT) to the websocket port of source replica
	os.Setenv("CHERAMI_STOREHOST_WS_PORT", strconv.FormatInt(int64(store0.wsPort), 10))

	repl0ResC := make(chan error)
	go func() {

		// replicate extent from store0 to store1
		log.Debugf("[%v].ReplicateExtent(extent=%v, mode=%v, source=%v", store1.hostID, extent, mode, store0.hostID)

		repl0ResC <- store1.ReplicateExtent(extent, mode, store0.hostID)

		close(repl0ResC)
	}()

	// == 4. initiate a duplicate request -- one of these should fail!
	repl1ResC := make(chan error)
	go func() {

		// replicate extent from store0 to store1
		log.Debugf("[%v].ReplicateExtent(extent=%v, mode=%v, source=%v", store1.hostID, extent, mode, store0.hostID)

		repl1ResC <- store1.ReplicateExtent(extent, mode, store0.hostID)

		close(repl1ResC)
	}()

	var err0, err1 error
	var done0, done1 bool

	if waitFor(1000, func() bool {

		select {
		case err, ok := <-repl0ResC:
			if ok {
				err0 = err
			} else {
				done0 = true
			}
		case err, ok := <-repl1ResC:
			if ok {
				err1 = err
			} else {
				done1 = true
			}
		default: // don't block
		}

		return done0 && done1 // until both are done
	}) {
		// exactly one should succeed and exactly one should fail
		s.NotEqual(err0 != nil, err1 != nil, fmt.Sprintf("TestStoreHostReplicateExtent: only one should fail: err0=%v err1=%v", err0, err1))

		if err0 != nil {
			// we should have got an InternalServiceError
			_, ok := err0.(*cherami.InternalServiceError)
			s.True(ok, fmt.Sprintf("TestStoreHostReplicateExtent: expected InternalServiceError: err=%v", err0))
		} else {
			// we should have got an InternalServiceError
			_, ok := err1.(*cherami.InternalServiceError)
			s.True(ok, fmt.Sprintf("TestStoreHostReplicateExtent: expected InternalServiceError: err=%v", err1))
		}
	}

	{
		// == 4.5 initiate a (third) duplicate request -- which should always fail!
		repl2ResC := make(chan error)
		go func() {

			// replicate extent from store0 to store1
			log.Debugf("[%v].ReplicateExtent(extent=%v, mode=%v, source=%v", store1.hostID, extent, mode, store0.hostID)

			repl2ResC <- store1.ReplicateExtent(extent, mode, store0.hostID)

			close(repl2ResC)
		}()

		var err2 error

		// wait a sec to ensure one of the ReplicateExtent calls fails
		if waitFor(1000, func() bool {
			select {
			case err2 = <-repl2ResC:
				return true
			default:
				return false
			}
		}) {
			// exactly one should succeed and exactly one should fail
			s.True(err2 != nil, fmt.Sprintf("TestStoreHostReplicateExtent: duplicate replicate-extent should have failed: err=%v", err2))

			// we should have got an InternalServiceError
			_, ok := err2.(*cherami.InternalServiceError)
			s.True(ok, fmt.Sprintf("TestStoreHostReplicateExtent: expected InternalServiceError: err=%v", err2))
		}
	}

	// send 'numMessages' credits on read stream
	out.sendC <- newControlFlow(numMessages * 2)

	// send 'numMessages' messages on write stream of first store and validate reception on second store
	for seqnum := 1; ; seqnum++ {

		// get a random message
		sendMsg := newAppendMessage(seqnum, 0, dataSize, nil)

		// send it to the first store
		in.sendC <- sendMsg

		// wait for the mesasge to be ack-ed
		s.True(waitFor(3000, func() bool {
			return in.lastAck().GetStatus() == cherami.Status_OK &&
				in.lastAck().GetSequenceNumber() == int64(seqnum)
		}), "timed out waiting for ack")

		// wait until it gets replicated and shows up on the second store
		s.True(waitFor(3000, func() bool {
			return out.lastMsg().GetType() == store.ReadMessageContentType_MESSAGE &&
				out.lastMsg().GetMessage().GetAddress() == in.lastAck().GetAddress()
		}), "timed out waiting to receive message")

		recvMsg := out.lastMsg().GetMessage().GetMessage() // extract the message

		// validate message received from the second store matches that written to the first store
		s.Equal(sendMsg.GetSequenceNumber(), recvMsg.GetSequenceNumber())
		s.Equal(sendMsg.GetEnqueueTimeUtc(), recvMsg.GetEnqueueTimeUtc())
		s.Equal(sendMsg.GetPayload().GetID(), recvMsg.GetPayload().GetID())
		s.Equal(sendMsg.GetPayload().GetDelayMessageInSeconds(), recvMsg.GetPayload().GetDelayMessageInSeconds())
		s.Equal(sendMsg.GetPayload().GetData(), recvMsg.GetPayload().GetData())

		// if we have sent 'numMessages', seal the extent
		if seqnum == numMessages {

			// now seal the extent at the seqNum of the last message sent
			store0.SealExtent(extent, int64(seqnum))

			// wait until it gets replicated and the extent is sealed on the second store
			s.True(waitFor(3000, func() bool {
				return out.lastMsg().GetType() == store.ReadMessageContentType_SEALED
			}), "timed out waiting for sealed")

			break
		}
	}

	s.True(waitFor(3000, func() bool { return isDone(inErrC) }))  // ensure OpenAppendStream completes
	s.True(waitFor(3000, func() bool { return isDone(outErrC) })) // ensure OpenReadStream completes

	// == 4. initiate another ReplicateExtent request -- this one should be a no-op; ie,
	// complete immediately, because the extent already exists on the store and is sealed
	repl3ErrC := make(chan error)
	go func() {

		// replicate extent from store0 to store1
		log.Debugf("[%v].ReplicateExtent(extent=%v, mode=%v, source=%v", store1.hostID, extent, mode, store0.hostID)

		// HACK: set $(CHERAMI_STOREHOST_WS_PORT) to the websocket port of source replica
		os.Setenv("CHERAMI_STOREHOST_WS_PORT", strconv.FormatInt(int64(store0.wsPort), 10))

		repl3ErrC <- store1.ReplicateExtent(extent, mode, store0.hostID)

		close(repl3ErrC)
	}()

	var err error

	// wait a sec to ensure the ReplicateExtent completes (since the extent exists and is sealed)
	if waitFor(1000, func() bool {
		// if we see an error on repl3ErrC, save it
		select {
		case err = <-repl3ErrC:
			return true
		default:
			return false
		}
	}) {
		log.Infof("ReplicateExtent request (on a sealed extent) failed with error: %v", err)

		// we should not have received any error
		s.Equal(nil, err)
	}
}

func (s *StoreHostSuite) TestStoreHostReplicateExtentResume() {

	mode := AppendOnly // TimerQueue
	dataSize := 1024

	var numMessages int
	switch {
	default:
		numMessages = 256
	case s.RunShort():
		numMessages = 64
	case s.RunLong():
		numMessages = 4096
	}

	// stop and resume replication from half-way
	var resumeSeqNum = numMessages / 2

	// start up two stores
	store0, store1 := s.storehost0, s.storehost1

	log.Debugf("TestStoreHostReplicateExtentResume storehost0: uuid=%v hostPort=%v wsPort=%d", store0.hostID, store0.hostPort, store0.wsPort)
	log.Debugf("TestStoreHostReplicateExtentResume storehost1: uuid=%v hostPort=%v wsPort=%d", store1.hostID, store1.hostPort, store1.wsPort)

	extent := uuid.NewRandom() // random extent

	// == start producers on the first and second store for the extent ==
	in, inErrC := store0.OpenAppendStream(extent, mode)
	log.Debugf("[%v].OpenAppendStream(%v) = %p", store0.hostID, extent, in)

	in1, in1ErrC := store1.OpenAppendStream(extent, mode)
	log.Debugf("[%v].OpenAppendStream(%v) = %p", store1.hostID, extent, in)

	sendMsgs := make([]*store.AppendMessage, numMessages+1)

	for seqnum := 1; ; seqnum++ {

		// get a random message
		sendMsg := newAppendMessage(seqnum, 0, dataSize, nil)

		// send it to the first store
		in.sendC <- sendMsg

		// write messages until 'resumeSeqNum' to second store, as well
		if seqnum <= resumeSeqNum {
			in1.sendC <- sendMsg
		}

		// remember the message
		sendMsgs[seqnum] = sendMsg

		// if we have sent 'numMessages', seal the extent
		if seqnum == numMessages {

			// wait for all messages to be ack-ed, before sealing the extent
			s.True(waitFor(5*numMessages, func() bool {
				return in.lastAck().GetStatus() == cherami.Status_OK &&
					in.lastAck().GetSequenceNumber() == int64(seqnum)
			}), "timed out waiting for ack")

			// now seal the extent at the seqNum of the last message sent
			err := store0.SealExtent(extent, int64(seqnum))

			if err != nil {
				log.Errorf("TestStoreHostReplicateExtent: SealExtent failed with error: %v", err)
				s.Fail("SealExtent failed")
			}

			break
		}
	}

	// now close the stream to the second store (after writing 'resumeSeqNum' messages)
	// and wait until the OpenAppendStream to the second store completes
	s.True(waitFor(5*resumeSeqNum, func() bool {
		return in1.lastAck().GetStatus() == cherami.Status_OK &&
			in1.lastAck().GetSequenceNumber() == int64(resumeSeqNum)
	}), "timed out waiting for ack")
	close(in1.sendC)
	s.True(waitFor(3000, func() bool { return isDone(in1ErrC) }))

	// initiate replicate extent -- since store1 has only 'resumeSeqNum' messages, it should resume from there
	replErrC := make(chan error)
	go func() {

		// replicate extent from store0 to store1
		log.Debugf("[%v].ReplicateExtent(extent=%v, mode=%v, source=%v", store1.hostID, extent, mode, store0.hostID)

		// HACK: set $(CHERAMI_STOREHOST_WS_PORT) to the websocket port of source replica
		os.Setenv("CHERAMI_STOREHOST_WS_PORT", strconv.FormatInt(int64(store0.wsPort), 10))

		replErrC <- store1.ReplicateExtent(extent, mode, store0.hostID)

		close(replErrC)
	}()

	// wait 3 seconds until the first OpenAppendStream completes
	s.True(waitFor(3000, func() bool { return isDone(inErrC) }))

	// wait max '5 * numMessages' milliseconds for ReplicateExtent to complete
	s.True(waitFor(5*numMessages, func() bool { return isDone(replErrC) })) // ensure ReplicateExtent completes

	// == now read all messages from the second store and validate we have all messages
	out, outErrC := store1.OpenReadStream(extent, mode)
	log.Debugf("[%v].OpenReadStream(%v) = %p", store1.hostID, extent, out)

readMsgLoop:
	for seqnum := 1; ; seqnum++ {

		// send one credit at a time, to read message by message
		out.sendC <- newControlFlow(1)

		// wait until it gets replicated and shows up on the second store
		s.True(waitFor(10000, func() bool {

			switch out.lastMsg().GetType() {
			case store.ReadMessageContentType_MESSAGE:
				log.Debugf("[%d] lastMsg MESSAGE seqnum=%d addr=%x", seqnum, out.lastMsg().GetMessage().GetMessage().GetSequenceNumber(),
					out.lastMsg().GetMessage().GetAddress())

			case store.ReadMessageContentType_SEALED:
				log.Debugf("[%d] lastMsg SEALED seqnum=%d", seqnum, out.lastMsg().GetSealed().GetSequenceNumber())

			case store.ReadMessageContentType_ERROR:
				log.Debugf("[%d] lastMsg ERROR error=%v", seqnum, out.lastMsg().GetError())

			default:
				log.Debugf("[%d] lastMsg unknown type=%v", seqnum, out.lastMsg().GetType())
			}

			return out.lastMsg().GetType() == store.ReadMessageContentType_SEALED ||
				(out.lastMsg().GetType() == store.ReadMessageContentType_MESSAGE &&
					out.lastMsg().GetMessage().GetMessage().GetSequenceNumber() == int64(seqnum))

		}), "timed out waiting to receive message")

		switch out.lastMsg().GetType() {
		case store.ReadMessageContentType_MESSAGE:

			recvMsg := out.lastMsg().GetMessage().GetMessage() // extract the message

			// validate message received from the second store matches that written to the first store
			s.Equal(sendMsgs[seqnum].GetSequenceNumber(), recvMsg.GetSequenceNumber())
			s.Equal(sendMsgs[seqnum].GetEnqueueTimeUtc(), recvMsg.GetEnqueueTimeUtc())
			s.Equal(sendMsgs[seqnum].GetPayload().GetID(), recvMsg.GetPayload().GetID())
			s.Equal(sendMsgs[seqnum].GetPayload().GetDelayMessageInSeconds(), recvMsg.GetPayload().GetDelayMessageInSeconds())
			s.Equal(sendMsgs[seqnum].GetPayload().GetData(), recvMsg.GetPayload().GetData())

		case store.ReadMessageContentType_SEALED:

			s.Equal(numMessages, seqnum)
			s.Equal(int64(numMessages), out.lastMsg().GetSealed().GetSequenceNumber())
			break readMsgLoop

		case store.ReadMessageContentType_ERROR:

			log.Errorf("OpenReadStream failed with error: %v", out.lastMsg().GetError())
			s.Fail("OpenReadStream failed")
			break readMsgLoop
		}
	}

	s.True(waitFor(3000, func() bool { return isDone(outErrC) })) // ensure OpenReadStream completes
}
