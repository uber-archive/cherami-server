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

package integration

import (
	"github.com/Shopify/sarama"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"log"
	"testing"
	"time"
)

// Note: you need to start ZooKeeper/Kafka on your local machine to run following tests.
// If running on Mac and java 1.7 for ZooKeeper/Kafka, run following command before starting Kafka:
// echo "127.0.0.1 $HOSTNAME" | sudo tee -a /etc/hosts

type KafkaLivenessIntegrationSuite struct {
	testBase
}

func TestKafkaLivenessSuite(t *testing.T) {
	s := new(KafkaLivenessIntegrationSuite)
	s.testBase.SetupSuite(t)
	suite.Run(t, s)
}

func (s *KafkaLivenessIntegrationSuite) TestKafkaLiveness() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	brokers := []string{"localhost:9092"}

	producer, err := sarama.NewSyncProducer(brokers, config)
	s.Assert().Nil(err)

	if err != nil {
		return
	}

	defer func() {
		err := producer.Close()
		s.Assert().Nil(err)
	}()

	topic := "test_topic_01"
	msgValue := "testing message " + uuid.New()

	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(msgValue)}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("Failed to send message: %s\n", err)
	} else {
		log.Printf("Sent message to partition %d at offset %d: %s\n", partition, offset, msgValue)
	}
	s.Assert().Nil(err)

	consumer, err := sarama.NewConsumer(brokers, config)
	s.Assert().Nil(err)

	if err != nil {
		return
	}

	defer func() {
		err := consumer.Close()
		s.Assert().Nil(err)
	}()

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	s.Assert().Nil(err)

	var receivedMessage = false
FOR:
	for {
		select {
		case err := <-partitionConsumer.Errors():
			log.Printf("Failed to receive message: %s\n", err)
		case msg := <-partitionConsumer.Messages():
			if string(msg.Value) == msgValue {
				receivedMessage = true
				log.Printf("Received message at partition %d at offset %d: %s\n", msg.Partition, msg.Offset, string(msg.Value))
				break FOR
			}
		case <-time.After(time.Second * 3):
			break FOR
		}
	}
	s.Assert().True(receivedMessage)
}
