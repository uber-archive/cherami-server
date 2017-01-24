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
	"math/rand"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

// -- internal helper methods -- //

// -- message serializer, deserializer functions -- //
func serializeMessage(msg *store.AppendMessage) ([]byte, error) {
	serializer := thrift.NewTSerializer()
	return serializer.Write(msg)
}

func deserializeMessage(data []byte) (*store.AppendMessage, error) {
	msg := &store.AppendMessage{}
	deserializer := thrift.NewTDeserializer()
	if err := deserializer.Read(msg, data); err != nil {
		return nil, err
	}

	return msg, nil
}

// collect stats real-time
type stats struct {
	num  int
	min  float64 // minimum
	max  float64 // maximum
	mean float64 // average
	vari float64 // variance
}

func (t *stats) put(v float64) {
	t.num++

	if (v < t.min) || (t.num == 1) {
		t.min = v
	}

	if (v > t.max) || (t.num == 1) {
		t.max = v
	}

	// compute (approx) variance realtime (Knuth/Welford)
	delta := v - t.mean

	t.mean += delta / float64(t.num)
	t.vari += delta * (v - t.mean)
}

func (t *stats) average() float64 {
	return t.mean
}

func (t *stats) variance() float64 {
	if t.num <= 1 {
		return 0.0
	}

	return (t.vari / float64(t.num-1))
}

func (t *stats) stdDev() float64 {
	return math.Sqrt(t.variance())
}

func (t *stats) maximum() float64 {
	return t.max
}

func (t *stats) minimum() float64 {
	return t.min
}

func (t stats) String() string {
	return fmt.Sprintf("[avg=%.3f σ:%.3f min:%.3f max:%.3f]", t.mean, t.stdDev(), t.min, t.max)
}

func (t stats) timeString() string {
	return fmt.Sprintf("[avg=%v σ:%v min:%v max:%v]", time.Duration(t.mean), time.Duration(t.stdDev()), time.Duration(t.min), time.Duration(t.max))
}

func shuffle(b []byte) {
	for i := len(b); i > 0; i-- {
		r := rand.Intn(i)
		b[i-1], b[r] = b[r], b[i-1]
	}
}
