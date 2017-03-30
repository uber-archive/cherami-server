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
	"errors"
	"strconv"

	s "github.com/Shopify/sarama"
	sc "github.com/bsm/sarama-cluster"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

// kafkaStream implements the same interface and similar-enough semantics to the Cherami store interface (BStoreOpenReadStreamOutCall)

type kafkaStream struct {
	creditSemaphore common.UnboundedSemaphore
	kafkaMsgsCh     <-chan *s.ConsumerMessage
}

var kafkaErrNilControlFlow = errors.New(`nil or non-positive controlFlow passed to Write()`)
var kafkaErrClosed = errors.New(`closed`)

/*
 * BStoreOpenReadStreamOutCall Interface
 */

// Write grants credits to the Read call
func (k *kafkaStream) Write(arg *cherami.ControlFlow) error {
	if arg == nil || arg.GetCredits() <= 0 {
		return kafkaErrNilControlFlow
	}
	k.creditSemaphore.Release(int(arg.GetCredits()))
	return nil
}

// Flush is a no-op
func (k *kafkaStream) Flush() error { return nil }

// Done is a no-op; consumer connection is handled at a higher level
func (k *kafkaStream) Done() error { return nil }

// Read returns the next message, if:
// * a message is available
// * enough credit has been granted
func (k *kafkaStream) Read() (*store.ReadMessageContent, error) {
	k.creditSemaphore.Acquire(1)
	m, ok := <-k.kafkaMsgsCh
	if !ok {
		k.creditSemaphore.Release(1) // TODO: size-based credits
		return nil, kafkaErrClosed
	}
	return convertKafkaMessageToCherami(m), nil
}

// ResponseHeaders returns the response headers sent from the server. This will block until server headers have been received.
func (k *kafkaStream) ResponseHeaders() (map[string]string, error) {
	return nil, errors.New(`unimplemented`)
}

/*
 * Setup & Utility
 */

func OpenKafkaStream(c *sc.Consumer) stream.BStoreOpenReadStreamOutCall {
	k := &kafkaStream{
		kafkaMsgsCh: c.Messages(),
	}
	return k
}

func convertKafkaMessageToCherami(k *s.ConsumerMessage) (c *store.ReadMessageContent) {
	c = &store.ReadMessageContent{
		Type: store.ReadMessageContentTypePtr(store.ReadMessageContentType_MESSAGE),
	}

	c.Message = &store.ReadMessage{
		Address: common.Int64Ptr(int64(4200000000000000000) + // arbitrary number to distinguish from offset
			int64(1000000000000000)*int64(k.Partition) + // Make addresses for different partitions disjoint
			k.Offset), // TODO: Topic mapping?
	}

	c.Message.Message = &store.AppendMessage{
		SequenceNumber: common.Int64Ptr(k.Offset),
		EnqueueTimeUtc: common.Int64Ptr(k.Timestamp.UnixNano()),
	}

	c.Message.Message.Payload = &cherami.PutMessage{
		Data: k.Value,
		UserContext: map[string]string{
			`key`:       string(k.Key),
			`topic`:     k.Topic,
			`partition`: strconv.Itoa(int(k.Partition)),
		},
		// TODO: Checksum?
	}
	return c
}
