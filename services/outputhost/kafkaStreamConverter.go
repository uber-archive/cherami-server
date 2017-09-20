package outputhost

import (
	"strconv"
	"sync/atomic"

	s "github.com/Shopify/sarama"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

// KafkaStreamConverter defines interface to convert a Kafka message to Cherami message
type KafkaStreamConverter interface {
	convertKafkaMessageToCherami(m *s.ConsumerMessage, logger bark.Logger) (c *store.ReadMessageContent)
}

type kafkaStreamConverter struct {
	seqNo int64
}

func (k *kafkaStreamConverter) convertKafkaMessageToCherami(m *s.ConsumerMessage, logger bark.Logger) (c *store.ReadMessageContent) {
	c = &store.ReadMessageContent{
		Type: store.ReadMessageContentTypePtr(store.ReadMessageContentType_MESSAGE),
	}

	c.Message = &store.ReadMessage{
		Address: common.Int64Ptr(
			int64(kafkaAddresser.GetStoreAddress(
				&topicPartition{
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
	}

	c.Message.Message = &store.AppendMessage{
		SequenceNumber: common.Int64Ptr(atomic.AddInt64(&k.seqNo, 1)),
	}

	if !m.Timestamp.IsZero() { // only set if kafka is version 0.10+
		c.Message.Message.EnqueueTimeUtc = common.Int64Ptr(m.Timestamp.UnixNano())
	}

	c.Message.Message.Payload = &cherami.PutMessage{
		Data: m.Value,
		UserContext: map[string]string{
			`key`:       string(m.Key),
			`topic`:     m.Topic,
			`partition`: strconv.Itoa(int(m.Partition)),
			`offset`:    strconv.Itoa(int(m.Offset)),
		},
		// TODO: Checksum?
	}
	return c
}