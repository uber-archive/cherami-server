// Copyright (c) 2017 Uber Technologies, Inc.
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
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pborman/uuid"
	cheramiclient "github.com/uber/cherami-client-go/client/cherami"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
)

// Note: you need to start ZooKeeper/Kafka on your local machine to run following tests.
// If running on Mac and java 1.7 for ZooKeeper/Kafka, run following command before starting Kafka:
// echo "127.0.0.1 $HOSTNAME" | sudo tee -a /etc/hosts

const (
	numMsgs          = 1000      // number of messages to send/receive
	minSize, maxSize = 512, 8192 // range for message size

	nKafkaTopics = 5 // number of kafka topics
	kafkaBroker  = "localhost:9092"
	kafkaCluster = "local"
)

type kafkaMsg struct {
	topic string
	key   string
	val   []byte
	part  int32
	offs  int64
	seq   int
	count int
}

func (t *kafkaMsg) Equals(m *kafkaMsg) bool {

	if m.topic != t.topic || m.key != t.key ||
		len(m.val) != len(t.val) ||
		m.part != t.part || m.offs != t.offs {

		return false
	}

	for i, b := range m.val {

		if t.val[i] != b {
			return false
		}
	}

	return true
}

func (t *kafkaMsg) String() string {
	return fmt.Sprintf("[%d] (topic:%v key:%v val:%d bytes) => (part:%d, offs:%d) <count=%d>",
		t.seq, t.topic, t.key, len(t.val), t.part, t.offs, t.count)
}

func (s *NetIntegrationSuiteParallelG) TestKafkaForCherami() {

	destPath, cgPath := "/kafka_test_dest/kfc", "/kafka_test_cg/kfc"

	// initialize set of test kafka topics
	kafkaTopics := make([]string, nKafkaTopics)
	for i := range kafkaTopics {
		kafkaTopics[i] = uuid.New()
	}

	// create cherami client
	ipaddr, port, _ := net.SplitHostPort(s.GetFrontend().GetTChannel().PeerInfo().HostPort)
	portNum, _ := strconv.Atoi(port)
	cheramiClient := createCheramiClient("cherami-test-kfc-integration", ipaddr, portNum, nil)

	// create cherami kfc destination
	destDesc, err := cheramiClient.CreateDestination(&cherami.CreateDestinationRequest{
		Path: common.StringPtr(destPath),
		Type: cherami.DestinationTypePtr(cherami.DestinationType_KAFKA),
		ConsumedMessagesRetention:   common.Int32Ptr(60),
		UnconsumedMessagesRetention: common.Int32Ptr(120),
		ChecksumOption:              cherami.ChecksumOption_CRC32IEEE,
		OwnerEmail:                  common.StringPtr("cherami-test-kfc-integration@uber.com"),
		IsMultiZone:                 common.BoolPtr(false),
		KafkaCluster:                common.StringPtr(kafkaCluster),
		KafkaTopics:                 kafkaTopics,
	})
	s.NotNil(destDesc)
	s.NoError(err)

	// create cherami kfc consumer group
	cgDesc, err := cheramiClient.CreateConsumerGroup(&cherami.CreateConsumerGroupRequest{
		ConsumerGroupName:    common.StringPtr(cgPath),
		DestinationPath:      common.StringPtr(destPath),
		LockTimeoutInSeconds: common.Int32Ptr(30),
		MaxDeliveryCount:     common.Int32Ptr(1),
		OwnerEmail:           common.StringPtr("cherami-test-kfc-integration@uber.com"),
	})
	s.NoError(err)
	s.NotNil(cgDesc)

	// setup cherami consumer
	cheramiConsumer := cheramiClient.CreateConsumer(&cheramiclient.CreateConsumerRequest{
		Path:              destPath,
		ConsumerGroupName: cgPath,
		ConsumerName:      "KfCIntegration",
		PrefetchCount:     1,
		Options:           &cheramiclient.ClientOptions{Timeout: time.Second * 30}, // this is the thrift context timeout
	})
	s.NotNil(cheramiConsumer)
	defer cheramiConsumer.Close()

	cheramiMsgsCh, err := cheramiConsumer.Open(make(chan cheramiclient.Delivery, 1))
	s.NoError(err)

	// setup kafka producer
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	kafkaProducer, err := sarama.NewSyncProducer([]string{kafkaBroker}, config)
	if err != nil {
		fmt.Printf("sarama.NewSyncProducer error=%v\n", err)
	}
	s.NoError(err)
	defer kafkaProducer.Close()

	sentMsgs := make(map[string]*kafkaMsg)

	// publish messages to kafka
	for i := 0; i < numMsgs; i++ {

		var topic = kafkaTopics[rand.Intn(len(kafkaTopics))]       // pick one of the topics at random
		var key = uuid.New()                                       // random key
		var val = make([]byte, minSize+rand.Intn(maxSize-minSize)) // random buf
		rand.Read(val)                                             // fill 'val' with random bytes

		part, offs, err :=
			kafkaProducer.SendMessage(
				&sarama.ProducerMessage{
					Topic: topic,
					Key:   sarama.StringEncoder(key),
					Value: sarama.ByteEncoder(val),
				},
			)

		if err != nil {
			fmt.Printf("kafkaProducer.SendMessage (seq=%d topic=%v key=%v val=%d bytes) failed: %v\n", i, topic, key, len(val), err)
			time.Sleep(100 * time.Millisecond)
			i--
			continue
		}

		sentMsgs[key] = &kafkaMsg{
			topic: topic,
			key:   key,
			val:   val,
			part:  part,
			offs:  offs,
			seq:   i,
		}
	}

	recvMsgs := make(map[string]*kafkaMsg)

	// consume messages from cherami
loop:
	for i := 0; (len(recvMsgs) < numMsgs) && (i < 2*numMsgs); i++ {

		select {
		case cmsg := <-cheramiMsgsCh:
			payload := cmsg.GetMessage().Payload
			uc := payload.GetUserContext()

			key, topic := uc[`key`], uc[`topic`]
			part, _ := strconv.Atoi(uc[`partition`])
			offs, _ := strconv.Atoi(uc[`offset`])

			// check for duplicates
			if _, ok := recvMsgs[key]; ok {

				if recvMsgs[key].count++; recvMsgs[key].count > 3 {
					s.Fail("received message too many times")
				}

				continue loop
			}

			msg := &kafkaMsg{
				topic: topic,
				key:   key,
				val:   payload.GetData(),
				part:  int32(part),
				offs:  int64(offs),
				seq:   i,
				count: 1,
			}

			// validate that message is as expected
			if sentMsgs[key] == nil || !msg.Equals(sentMsgs[key]) {

				fmt.Printf("received[%d]=%v\n", i, msg)

				if sentMsgs[key] == nil {
					fmt.Printf("expected=<MISSING>\n")
				} else {
					fmt.Printf("expected=%v\n", sentMsgs[key])
				}

				fmt.Printf("\nreceived:\n")
				for _, m := range recvMsgs {
					fmt.Printf("%v\n", m)
				}

				fmt.Printf("\nsent (and not received):\n")
				for _, m := range sentMsgs {
					fmt.Printf("%v\n", m)
				}

				s.Fail("message validation failed")
			}

			// if we have seen all the messages, break out
			recvMsgs[key] = msg

			cmsg.Ack()

		case <-time.After(45 * time.Second):
			s.Fail("cherami-consumer: timed out")
			break loop
		}
	}

	s.Equal(numMsgs, len(recvMsgs)) // we should have received all the messages

	return
}
