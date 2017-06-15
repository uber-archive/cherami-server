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
	"io"
	"sync"
	"time"

	"github.com/pborman/uuid"

	_ "github.com/apache/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/cherami"

	"testing"
)

func benchmarkStorehostWrite(b *testing.B, concurrency int) {
	testbase := newBase()
	testbase.setupStore(1)

	wg := sync.WaitGroup{}
	stream := func(index int) {
		call, cancel, ackStreamClosed := testbase.setupStoreCall(0, uuid.New())
		if cancel != nil {
			defer cancel()
		}
		for i := index; i < b.N*nMultiplier; i = i + concurrency {
			if err := call.Write(getAppendMessage(i/concurrency + 1)); err != nil {
				log.Errorf("client: error writing messages to stream: %v", err)
				ackStreamClosed <- false
				break
			}
		}

		if err := call.Done(); err != nil {
			log.Errorf("client: error closing message stream: %v", err)
		}
		<-ackStreamClosed
		call.Done()
		wg.Done()
	}

	b.SetBytes(int64(msgSize * int64(nMultiplier)))
	b.ResetTimer()
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go stream(i)
	}

	wg.Wait()
	b.StopTimer()

	testbase.shutdownStore()
}

func BenchmarkStorehostWrite1(b *testing.B) {
	benchmarkStorehostWrite(b, 1)
}

func BenchmarkStorehostWrite2(b *testing.B) {
	benchmarkStorehostWrite(b, 2)
}

func BenchmarkStorehostWrite4(b *testing.B) {
	benchmarkStorehostWrite(b, 4)
}

func BenchmarkStorehostWrite128(b *testing.B) {
	benchmarkStorehostWrite(b, 128)
}

func benchmarkStorehostRead(b *testing.B, concurrency int) {
	testbase := newBase()
	testbase.setupStore(1)

	wg := sync.WaitGroup{}
	uuids := make([]string, concurrency)
	for i := 0; i < concurrency; i++ {
		uuids[i] = uuid.New()
	}

	writeStream := func(index int) {
		call, cancel, ackStreamClosed := testbase.setupStoreCall(0, uuids[index])
		if cancel != nil {
			defer cancel()
		}
		for i := index; i < b.N*nMultiplier; i = i + concurrency {
			if err := call.Write(getAppendMessage(i/concurrency + 1)); err != nil {
				log.Errorf("client: error writing messages to stream: %v", err)
				ackStreamClosed <- false
				break
			}

			if (i-index)%10000 == 0 {
				log.Infof("write %d", i)
			}
		}

		time.Sleep(time.Second)
		if err := call.Done(); err != nil {
			log.Errorf("client: error closing message stream: %v", err)
		}
		<-ackStreamClosed
		call.Done()
		wg.Done()
	}

	readStream := func(index int) {
		ackStreamClosed := make(chan bool, 1)
		call, cancel := testbase.setupStoreReadCall(0, uuids[index])
		if cancel != nil {
			defer cancel()
		}

		go func() {
			for i := 0; i < b.N*nMultiplier/concurrency; i++ {
				msg, err := call.Read()
				if err == io.EOF {
					log.Infof("client: read stream closed")
					ackStreamClosed <- true
					return
				}

				if err != nil {
					log.Errorf("client: error reading read stream: %v", err)
					ackStreamClosed <- false
					return
				}

				if i%10000 == 0 {
					log.Infof("read %d %d %s", i,
						msg.GetMessage().GetMessage().GetSequenceNumber(),
						msg.GetMessage().GetMessage().GetPayload().GetID())
				}
			}

			ackStreamClosed <- true
		}()

		cf := cherami.NewControlFlow()
		cf.Credits = common.Int32Ptr(int32(b.N * nMultiplier / concurrency))
		if err := call.Write(cf); err != nil {
			log.Errorf("client: error sending credits to stream: %v", err)
			ackStreamClosed <- false
		}

		if err := call.Flush(); err != nil {
			log.Errorf("client: error flushing stream: %v", err)
		}

		<-ackStreamClosed
		call.Done()
		wg.Done()
	}

	b.SetBytes(int64(msgSize * int64(nMultiplier)))
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go writeStream(i)
	}

	b.ResetTimer()
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go readStream(i)
	}

	wg.Wait()
	b.StopTimer()
	testbase.shutdownStore()
}

func BenchmarkStorehostRead1(b *testing.B) {
	benchmarkStorehostRead(b, 1)
}

func BenchmarkStorehostRead2(b *testing.B) {
	benchmarkStorehostRead(b, 2)
}

func BenchmarkStorehostRead4(b *testing.B) {
	benchmarkStorehostRead(b, 4)
}
