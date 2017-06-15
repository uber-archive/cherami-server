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

package benchmark

import (
	"strconv"
	"sync"
	"time"

	_ "github.com/apache/thrift/lib/go/thrift"

	"testing"

	log "github.com/sirupsen/logrus"
)

func BenchmarkE2E(b *testing.B) {
	testbase := newBase()
	testbase.setupStore(3)
	testbase.setupInput()
	testbase.setupOutput()
	testbase.setupMetadata()

	b.SetBytes(int64(msgSize * int64(nMultiplier)))

	call, cancel, ackStreamClosed := testbase.setupInCall()
	if cancel != nil {
		defer cancel()
	}

	call2, cancel2, creditsCh, ackCh := testbase.setupOutCall()
	if cancel2 != nil {
		defer cancel2()
	}
	// set window to be 1024 messages (equal to output host flush size)
	creditsCh <- 1024
	outstandingCredits := 1024

	b.ResetTimer()
	wg := sync.WaitGroup{}
	wg.Add(2)
	b.ResetTimer()

	go func() {
		for i := 0; i < b.N*nMultiplier; i++ {
			msg := getPutMessage(i)
			if err := call.Write(msg); err != nil {
				log.Errorf("client: error writing messages to stream: %v", err)
				ackStreamClosed <- false
				break
			}

			if i%10000 == 0 {
				log.Infof("write %d", i)
			}
		}

		time.Sleep(5 * time.Second)
		if err := call.Done(); err != nil {
			log.Errorf("client: error closing message stream: %v", err)
		}

		<-ackStreamClosed
		call.Done()
		wg.Done()
	}()

	go func() {
		for i := 0; i < b.N*nMultiplier; i++ {
			msg, err := call2.Read()

			if err != nil {
				log.Errorf("client: error reading consumer stream: %v", err)
				break
			}

			rcvdID, _ := strconv.Atoi(msg.GetMessage().GetPayload().GetID())
			if i%10000 == 0 {
				log.Infof("read %d %d", i, rcvdID)
				// println("read", i, rcvdID)
			}

			outstandingCredits--
			if outstandingCredits == 0 {
				creditsCh <- 1024
				outstandingCredits = 1024
			}

			ackCh <- msg.GetMessage().GetAckId()
		}

		call2.Done()
		wg.Done()
		close(creditsCh)
		close(ackCh)
	}()

	wg.Wait()
	b.StopTimer()
	testbase.shutdownStore()
	testbase.shutdownInput()
	testbase.shutdownOutput()
}

func BenchmarkLatency(b *testing.B) {
	testbase := newBase()
	testbase.setupStore(1)
	testbase.setupInput()
	testbase.setupOutput()
	testbase.setupMetadata()

	divisor := 256
	count := b.N * nMultiplier / divisor

	b.SetBytes(int64(msgSize * int64(count)))

	call, cancel, ackStreamClosed := testbase.setupInCall()
	if cancel != nil {
		defer cancel()
	}

	call2, cancel2, creditsCh, ackCh := testbase.setupOutCall()
	if cancel2 != nil {
		defer cancel2()
	}
	// set window to be 1024 messages (equal to output host flush size)
	creditsCh <- 1024
	outstandingCredits := 1024

	var sum int64

	b.ResetTimer()
	for i := 0; i < count; i++ {
		begin := time.Now().UnixNano()
		if err := call.Write(getPutMessage(i)); err != nil {
			log.Errorf("client: error writing messages to stream: %v", err)
			ackStreamClosed <- false
			break
		}

		call.Flush()

		msg, err := call2.Read()
		if err != nil {
			log.Errorf("client: error reading consumer stream: %v", err)
			break
		}

		end := time.Now().UnixNano()
		sum = sum + end - begin

		outstandingCredits--
		if outstandingCredits == 0 {
			creditsCh <- 1024
			outstandingCredits = 1024
		}

		ackCh <- msg.GetMessage().GetAckId()
		if i%1024 == 0 {
			log.Infof("write %d", i)
		}
	}

	if err := call.Done(); err != nil {
		log.Errorf("client: error closing message stream: %v", err)
	}
	<-ackStreamClosed
	call.Done()
	call2.Done()

	b.StopTimer()
	delayMs := float64(sum) / float64(count) / 1e6
	println("average delay milliseconds", delayMs)

	testbase.shutdownStore()
	testbase.shutdownInput()
	testbase.shutdownOutput()
}
