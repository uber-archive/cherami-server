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
	"testing"

	_ "github.com/apache/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"
)

func BenchmarkInputhostWrite(b *testing.B) {
	testbase := newBase()
	testbase.setupStore(3)
	testbase.setupInput()
	testbase.setupMetadata()

	b.SetBytes(int64(msgSize * int64(nMultiplier)))

	call, cancel, ackStreamClosed := testbase.setupInCall()
	if cancel != nil {
		defer cancel()
	}
	b.ResetTimer()
	for i := 0; i < b.N*nMultiplier; i++ {
		if err := call.Write(getPutMessage(i)); err != nil {
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
	b.StopTimer()

	testbase.shutdownStore()
	testbase.shutdownInput()
}
