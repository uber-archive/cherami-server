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
	"strconv"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

type KafkaStreamSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
}

func TestKafkaStreamSuite(t *testing.T) {
	suite.Run(t, new(KafkaStreamSuite))
}

func (s *KafkaStreamSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *KafkaStreamSuite) TearDownTest() {
}

func (s *KafkaStreamSuite) TestBasic() {
	s.WithTestMessages(1, func(c chan *sarama.ConsumerMessage) {
		s.WithKafkaStream(c, func(k stream.BStoreOpenReadStreamOutCall) {
			s.NoError(k.Write(&cherami.ControlFlow{Credits: common.Int32Ptr(1)}))
			msg, err := k.Read()
			s.NoError(err)
			s.NotNil(msg)
			s.ValidateMessage(msg)
		})
	})
}

func (s *KafkaStreamSuite) TestLong() {
	longCount := int(1e6)
	s.WithTestMessages(longCount, func(c chan *sarama.ConsumerMessage) {
		s.WithKafkaStream(c, func(k stream.BStoreOpenReadStreamOutCall) {
			close(c) // Just bail when we are done reading

			s.NoError(k.Write(&cherami.ControlFlow{Credits: common.Int32Ptr(1)}))
			for i := 0; i < longCount; i++ {
				msg, err := k.Read()
				s.NoError(err)
				s.NotNil(msg)
				s.ValidateMessage(msg)
				s.NoError(k.Write(&cherami.ControlFlow{Credits: common.Int32Ptr(1)}))
			}

			msg, err := k.Read()
			s.Error(err)
			s.Nil(msg)
		})
	})
}

func (s *KafkaStreamSuite) TestInsufficientCredits() {
	msgCount := 10
	creditCount := int32(msgCount - 1)

	s.WithTestMessages(msgCount, func(c chan *sarama.ConsumerMessage) {
		s.WithKafkaStream(c, func(k stream.BStoreOpenReadStreamOutCall) {

			readFn := func() {
				msg, err := k.Read()
				s.NoError(err)
				s.NotNil(msg)
				s.ValidateMessage(msg)
			}

			s.NoError(k.Write(&cherami.ControlFlow{Credits: common.Int32Ptr(creditCount)}))
			for i := int32(0); i < creditCount; i++ {
				readFn()
			}

			closeCh := make(chan struct{})
			go func() {
				readFn()
				close(closeCh)
			}()

			select {
			case <-closeCh:
				s.Fail(`Should block`)
			case <-time.After(time.Second / 10):
			}

			// One credit should unblock
			s.NoError(k.Write(&cherami.ControlFlow{Credits: common.Int32Ptr(1)}))
			<-closeCh
		})
	})
}

func (s *KafkaStreamSuite) TestChannelCloseReadFallthrough() {
	msgCount := 10
	creditCount := int32(msgCount)

	s.WithTestMessages(msgCount, func(c chan *sarama.ConsumerMessage) {
		s.WithKafkaStream(c, func(k stream.BStoreOpenReadStreamOutCall) {
			readFn := func(errExpected bool) {
				msg, err := k.Read()
				if !errExpected {
					s.NoError(err)
					s.NotNil(msg)
					s.ValidateMessage(msg)
				} else {
					s.Error(err)
					s.Nil(msg)
					s.Equal(errKafkaClosed.Error(), err.Error())
				}
			}

			s.NoError(k.Write(&cherami.ControlFlow{Credits: common.Int32Ptr(creditCount)}))
			for i := int32(0); i < creditCount; i++ {
				readFn(false)
			}

			closeCh := make(chan struct{})
			go func() {
				readFn(true) // Reading against an empty channel
				close(closeCh)
			}()

			close(c) // Closing the channel should unblock the reader
			select {
			case <-closeCh:
			case <-time.After(time.Second / 10):
				s.Fail(`Should not block`)
			}
		})
	})
}

func (s *KafkaStreamSuite) WithTestMessages(count int, fn func(chan *sarama.ConsumerMessage)) {
	c := make(chan *sarama.ConsumerMessage, count)
	for i := 0; i < count; i++ {
		sMsg := &sarama.ConsumerMessage{
			Key:       []byte(strconv.Itoa(10*i + 1)),
			Value:     []byte(strconv.Itoa(10*i + 5)),
			Topic:     strconv.Itoa((10*i + 2) % 100),
			Partition: int32(10*i+3) % 100,
			Offset:    int64(10*i + 4),
			Timestamp: time.Now(),
		}
		c <- sMsg
	}

	fn(c)

	defer func() { recover() }() // Test may have already closed channel, so just absorb the panic
	close(c)
}

func (s *KafkaStreamSuite) WithKafkaStream(c chan *sarama.ConsumerMessage, fn func(stream.BStoreOpenReadStreamOutCall)) {
	k := OpenKafkaStream(c, nil, common.GetDefaultLogger())
	s.NotNil(k)
	fn(k)
	s.NoError(k.Flush())
	s.NoError(k.Done())
}

func (s *KafkaStreamSuite) ValidateMessage(msg *store.ReadMessageContent) {
	s.NotNil(msg)
	s.Equal(store.ReadMessageContentType_MESSAGE, msg.GetType())
	s.Nil(msg.Error)
	s.Nil(msg.Sealed)
	s.Nil(msg.NoMoreMessage)
	s.NotNil(msg.Message)
	readMsg := msg.GetMessage()
	s.NotZero(readMsg.GetAddress())
	s.NotNil(readMsg.Message)
	appendMsg := readMsg.GetMessage()
	s.NotZero(appendMsg.GetSequenceNumber())
	s.InDelta(time.Now().Add(-1*time.Minute).UnixNano(), appendMsg.GetEnqueueTimeUtc(), float64(time.Minute), `EnqueueTime should not be older than 2 minutes`)
	s.Nil(appendMsg.FullyReplicatedWatermark)
	s.NotNil(appendMsg.Payload)

	i := int(appendMsg.GetSequenceNumber() - 1)

	putMsg := appendMsg.GetPayload()
	s.Nil(putMsg.Crc32IEEEDataChecksum)
	s.Nil(putMsg.Md5DataChecksum)
	s.Nil(putMsg.DelayMessageInSeconds)
	s.Nil(putMsg.SchemaVersion)
	s.Nil(putMsg.PreviousMessageId)
	s.Nil(putMsg.ID)
	s.NotNil(putMsg.UserContext)
	s.NotZero(len(putMsg.GetData()))

	userContext := putMsg.GetUserContext()
	s.Equal(strconv.Itoa(10*i+1), userContext[`key`])
	s.Equal(strconv.Itoa((10*i+2)%100), userContext[`topic`])
	s.Equal(strconv.Itoa((10*i+3)%100), userContext[`partition`])
	s.Equal(strconv.Itoa(10*i+4), userContext[`offset`])
	s.Equal(strconv.Itoa(10*i+5), string(putMsg.GetData()))
}
