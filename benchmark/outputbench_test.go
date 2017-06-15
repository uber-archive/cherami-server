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

func BenchmarkOutputRead(b *testing.B) {
	benchmarkOutputRead(b, 1)
}

func benchmarkOutputRead(b *testing.B, concurrency int) {
	testbase := newBase()
	testbase.setupStore(1)
	testbase.setupOutput()
	testbase.setupMetadata()

	go func() {
		call, cancel, ackStreamClosed := testbase.setupStoreCall(0, testbase.extentUUID)
		if cancel != nil {
			defer cancel()
		}
		for i := 0; i < b.N*nMultiplier; i++ {
			if err := call.Write(getAppendMessage(i)); err != nil {
				log.Errorf("client: error writing messages to stream: %v", err)
				ackStreamClosed <- false
				break
			}

			if i%10000 == 0 {
				log.Infof("write %d", i)
			}
		}

		time.Sleep(3 * time.Second)
		if err := call.Done(); err != nil {
			log.Errorf("client: error closing message stream: %v", err)
		}
		<-ackStreamClosed
		call.Done()
	}()

	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	b.SetBytes(int64(msgSize * int64(nMultiplier)))
	b.ResetTimer()

	for j := 0; j < concurrency; j++ {
		go func() {
			call2, cancel, creditsCh, ackCh := testbase.setupOutCall()
			if cancel != nil {
				defer cancel()
			}

			// set window to be 1024 messages (equal to output host flush size)
			creditsCh <- 1024
			outstandingCredits := 1024

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
	}

	wg.Wait()
	b.StopTimer()

	testbase.shutdownStore()
	testbase.shutdownOutput()
}
