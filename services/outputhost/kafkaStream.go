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
	"sync/atomic"

	s "github.com/Shopify/sarama"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

var (
	errKafkaNilControlFlow = errors.New(`nil or non-positive controlFlow passed to Write()`)
	errKafkaClosed         = errors.New(`closed`)
)

// kafkaStream implements the same interface and similar-enough semantics to the Cherami store interface (BStoreOpenReadStreamOutCall)

type kafkaStream struct {
	creditSemaphore common.UnboundedSemaphore
	kafkaMsgsCh     <-chan *s.ConsumerMessage
	kafkaConverter  KafkaMessageConverter
	logger          bark.Logger
	seqNo           int64
}

// KafkaMessageConverterConfig is used to config customized converter
type KafkaMessageConverterConfig struct {
	// Destination and ConsumerGroup are not needed currently, but may in the future
	// Destination *cherami.DestinationDescription
	// ConsumerGroup *cherami.ConsumerGroupDescription
	KafkaTopics  []string // optional: already contained in destination-description
	KafkaCluster string   // optional: already contained in destination-description
}

// KafkaMessageConverter defines interface to convert a Kafka message to Cherami message
type KafkaMessageConverter func(kMsg *s.ConsumerMessage) (cMsg *store.ReadMessageContent)

// KafkaMessageConverterFactory provides converter from Kafka message to Cherami message
// In the future, it may provide implementations for BStoreOpenReadStreamOutCall
type KafkaMessageConverterFactory interface {
	GetConverter(cfg *KafkaMessageConverterConfig, log bark.Logger) KafkaMessageConverter
}

/*
 * BStoreOpenReadStreamOutCall Interface
 */

// Write grants credits to the Read call
func (k *kafkaStream) Write(arg *cherami.ControlFlow) error {
	if arg == nil || arg.GetCredits() <= 0 {
		return errKafkaNilControlFlow
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
	m, ok := <-k.kafkaMsgsCh
	if !ok {
		return nil, errKafkaClosed
	}
	k.creditSemaphore.Acquire(1) // TODO: Size-based credits
	c := k.kafkaConverter(m)
	c.Message.Message.SequenceNumber = common.Int64Ptr(k.getNextSequenceNumber())
	return c, nil
}

// ResponseHeaders returns the response headers sent from the server. This will block until server headers have been received.
func (k *kafkaStream) ResponseHeaders() (map[string]string, error) {
	return nil, errors.New(`unimplemented`)
}

func (k *kafkaStream) getNextSequenceNumber() int64 {
	return atomic.AddInt64(&k.seqNo, 1)
}

/*
 * Setup & Utility
 */

// OpenKafkaStream opens a store call simulated from the given sarama message stream
func OpenKafkaStream(c <-chan *s.ConsumerMessage, kafkaMessageConverter KafkaMessageConverter, logger bark.Logger) stream.BStoreOpenReadStreamOutCall {
	k := &kafkaStream{
		kafkaMsgsCh:    c,
		logger:         logger,
		kafkaConverter: kafkaMessageConverter,
	}
	if k.kafkaConverter == nil {
		k.kafkaConverter = GetDefaultKafkaMessageConverter(k.logger)
	}
	return k
}

// GetDefaultKafkaMessageConverter returns the default kafka message converter
func GetDefaultKafkaMessageConverter(logger bark.Logger) KafkaMessageConverter {
	return func(m *s.ConsumerMessage) (c *store.ReadMessageContent) {
		c = &store.ReadMessageContent{
			Type: store.ReadMessageContentTypePtr(store.ReadMessageContentType_MESSAGE),
			Message: &store.ReadMessage{
				Address: common.Int64Ptr(
					int64(kafkaAddresser.GetStoreAddress(
						&TopicPartition{
							Topic:     m.Topic,
							Partition: m.Partition,
						},
						m.Offset,
						func() bark.Logger {
							return logger.WithFields(bark.Fields{
								`module`:    `kafkaStream`,
								`topic`:     m.Topic,
								`partition`: m.Partition,
							})
						},
					))),
				Message: &store.AppendMessage{
					Payload: &cherami.PutMessage{
						Data: m.Value,
						UserContext: map[string]string{
							`key`:       string(m.Key),
							`topic`:     m.Topic,
							`partition`: strconv.Itoa(int(m.Partition)),
							`offset`:    strconv.Itoa(int(m.Offset)),
						},
						// TODO: Checksum?
					},
				},
			},
		}

		if !m.Timestamp.IsZero() {
			// only set if kafka is version 0.10+
			c.Message.Message.EnqueueTimeUtc = common.Int64Ptr(m.Timestamp.UnixNano())
		}

		return c
	}
}
