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
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	client "github.com/uber/cherami-client-go/client/cherami"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/controllerhost"
	"github.com/uber/cherami-server/services/storehost"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"

	"testing"
)

type NetIntegrationSuiteParallelB struct {
	testBase
}
type NetIntegrationSuiteParallelA struct {
	testBase
}
type NetIntegrationSuiteParallelC struct {
	testBase
}
type NetIntegrationSuiteParallelD struct {
	testBase
}
type NetIntegrationSuiteParallelE struct {
	testBase
}
type NetIntegrationSuiteParallelF struct {
	testBase
}
type NetIntegrationSuiteParallelG struct {
	testBase
}

type NetIntegrationSuiteSerial struct {
	testBase
}

func TestNetIntegrationSuiteParallelA(t *testing.T) {
	s := new(NetIntegrationSuiteParallelA)
	s.testBase.SetupSuite(t)
	t.Parallel()
	suite.Run(t, s)
}
func TestNetIntegrationSuiteParallelB(t *testing.T) {
	s := new(NetIntegrationSuiteParallelB)
	s.testBase.SetupSuite(t)
	t.Parallel()
	suite.Run(t, s)
}
func TestNetIntegrationSuiteParallelC(t *testing.T) {
	s := new(NetIntegrationSuiteParallelC)
	s.testBase.SetupSuite(t)
	t.Parallel()
	suite.Run(t, s)
}
func TestNetIntegrationSuiteParallelD(t *testing.T) {
	s := new(NetIntegrationSuiteParallelD)
	s.testBase.SetupSuite(t)
	t.Parallel()
	suite.Run(t, s)
}
func TestNetIntegrationSuiteParallelE(t *testing.T) {
	s := new(NetIntegrationSuiteParallelE)
	s.testBase.SetupSuite(t)
	t.Parallel()
	suite.Run(t, s)
}
func TestNetIntegrationSuiteParallelF(t *testing.T) {
	s := new(NetIntegrationSuiteParallelF)
	s.testBase.SetupSuite(t)
	t.Parallel()
	suite.Run(t, s)
}
func TestNetIntegrationSuiteParallelG(t *testing.T) {
	s := new(NetIntegrationSuiteParallelG)
	s.testBase.SetupSuite(t)
	t.Parallel()
	suite.Run(t, s)
}

// Disabled, since it is apparently impossible to get this test to run without racing with the parallel tests
func XXXTestNetIntegrationSuiteSerial(t *testing.T) {
	if !testing.Short() {
		s := new(NetIntegrationSuiteSerial)
		s.testBase.SetupSuite(t)
		suite.Run(t, s)
	}
}

func createCheramiClient(svcName string, ipaddr string, port int, logger bark.Logger) client.Client {
	options := &client.ClientOptions{
		Timeout: time.Second * 30,
		ReconfigurationPollingInterval: time.Second,
	}
	if logger != nil {
		options.Logger = logger
	}
	cc, _ := client.NewClient(svcName, ipaddr, port, options)
	return cc
}

func (s *NetIntegrationSuiteParallelC) TestMsgCacheLimit() {
	destPath := "/dest/TestMsgCacheLimit"
	cgPath := "/cg/TestMsgCacheLimit"
	testMsgCount := 100
	testMsgCacheLimit := 50
	log := common.GetDefaultLogger()

	value := fmt.Sprintf("%v/%v=%v", destPath, cgPath, testMsgCacheLimit)

	// set the message cache limit for this cg on the outputhost to be a smaller number
	cItem := &metadata.ServiceConfigItem{
		ServiceName:    common.StringPtr("cherami-outputhost"),
		ServiceVersion: common.StringPtr("*"),
		Sku:            common.StringPtr("*"),
		Hostname:       common.StringPtr("*"),
		ConfigKey:      common.StringPtr("messagecachesize"),
		ConfigValue:    common.StringPtr(value),
	}

	req := &metadata.UpdateServiceConfigRequest{ConfigItem: cItem}
	err := s.mClient.UpdateServiceConfig(nil, req)
	s.NoError(err)

	time.Sleep(10 * time.Millisecond)

	// Create the client
	ipaddr, port, _ := net.SplitHostPort(s.GetFrontend().GetTChannel().PeerInfo().HostPort)
	portNum, _ := strconv.Atoi(port)
	cheramiClient := createCheramiClient("cherami-test-limit", ipaddr, portNum, nil)
	defer cheramiClient.Close()

	// Create the destination to publish message
	crReq := cherami.NewCreateDestinationRequest()
	crReq.Path = common.StringPtr(destPath)
	crReq.Type = cherami.DestinationTypePtr(0)
	crReq.ConsumedMessagesRetention = common.Int32Ptr(60)
	crReq.UnconsumedMessagesRetention = common.Int32Ptr(120)
	crReq.OwnerEmail = common.StringPtr("lhcIntegration@uber.com")

	desDesc, _ := cheramiClient.CreateDestination(crReq)
	s.NotNil(desDesc)

	// we should see the destination now in cassandra
	rReq := cherami.NewReadDestinationRequest()
	rReq.Path = common.StringPtr(destPath)
	readDesc, _ := cheramiClient.ReadDestination(rReq)
	s.NotNil(readDesc)

	// ==WRITE==

	log.Info("Write test beginning...")

	// Create the publisher
	cPublisherReq := &client.CreatePublisherRequest{
		Path: destPath,
	}

	publisherTest := cheramiClient.CreatePublisher(cPublisherReq)
	s.NotNil(publisherTest)

	err = publisherTest.Open()
	s.NoError(err)
	// Publish messages
	doneCh := make(chan *client.PublisherReceipt, testMsgCount)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 0; i < testMsgCount; i++ {
			receipt := <-doneCh
			log.Infof("client: acking id %s", receipt.Receipt)
			s.NoError(receipt.Error)
		}
		wg.Done()
	}()

	for i := 0; i < testMsgCount; i++ {
		data := []byte(fmt.Sprintf("msg_%d", i))
		id, err1 := publisherTest.PublishAsync(
			&client.PublisherMessage{
				Data: data,
			},
			doneCh,
		)
		log.Infof("client: publishing data: %s with id %s", data, id)
		s.NoError(err1)
	}

	wg.Wait()
	publisherTest.Close()

	// ==READ==

	// Create the consumer group
	cgReq := cherami.NewCreateConsumerGroupRequest()
	cgReq.ConsumerGroupName = common.StringPtr(cgPath)
	cgReq.DestinationPath = common.StringPtr(destPath)
	cgReq.LockTimeoutInSeconds = common.Int32Ptr(30) // this is the message redilvery timeout
	cgReq.MaxDeliveryCount = common.Int32Ptr(1)
	cgReq.OwnerEmail = common.StringPtr("consumer_integration_test@uber.com")

	cgDesc, err := cheramiClient.CreateConsumerGroup(cgReq)
	s.NoError(err)
	s.NotNil(cgDesc)
	s.Equal(cgReq.GetDestinationPath(), cgDesc.GetDestinationPath(), "Wrong destination path")
	s.Equal(cgReq.GetConsumerGroupName(), cgDesc.GetConsumerGroupName(), "Wrong consumer group name")
	s.Equal(cgReq.GetLockTimeoutInSeconds(), cgDesc.GetLockTimeoutInSeconds(), "Wrong LockTimeoutInSeconds")
	s.Equal(cgReq.GetMaxDeliveryCount(), cgDesc.GetMaxDeliveryCount(), "Wrong MaxDeliveryCount")
	s.Equal(cherami.ConsumerGroupStatus_ENABLED, cgDesc.GetStatus(), "Wrong Status")
	s.Equal(cgReq.GetOwnerEmail(), cgDesc.GetOwnerEmail(), "Wrong OwnerEmail")

	cConsumerReq := &client.CreateConsumerRequest{
		Path:              destPath,
		ConsumerGroupName: cgPath,
		ConsumerName:      "TestMsgCacheLimitConsumerName",
		PrefetchCount:     1,
		Options:           &client.ClientOptions{Timeout: time.Second * 30}, // this is the thrift context timeout
	}

	consumerTest := cheramiClient.CreateConsumer(cConsumerReq)
	s.NotNil(consumerTest)

	// Open the consumer channel
	delivery := make(chan client.Delivery, 1)
	delivery, err = consumerTest.Open(delivery)
	s.NoError(err)

	outstandingMsgs := []client.Delivery{}
	msgCount := 0
	// Read the messages in a loop. We will exit the loop via a timeout
ReadLoop:
	for i := 0; i < testMsgCount; i++ {
		timeout := time.NewTimer(time.Second * 10)
		log.Infof("waiting to get a message on del chan")
		select {
		case msg := <-delivery:
			msgCount++
			log.Infof("Read message: #%v (msg ID:%v) but not acking the message", msgCount, msg.GetMessage().Payload.GetID())
			outstandingMsgs = append(outstandingMsgs, msg)
		case <-timeout.C:
			// we expect the timeout here because we already have read all messages outstanding
			log.Errorf("consumer delivery channel timed out after %v messages", msgCount)
			break ReadLoop
		}
	}

	log.Infof("there are: %v outstanding messages", msgCount)
	s.Equal(msgCount, testMsgCacheLimit)

	// now ack all outstanding messages
	for _, msg := range outstandingMsgs {
		log.Infof("acking message with ID: %v", msg.GetMessage().Payload.GetID())
		msg.Ack()
	}

	// now read all the remaining messages
	log.Infof("now read additional messages")
ReadLoop2:
	for ; msgCount < testMsgCount; msgCount++ {
		timeout := time.NewTimer(time.Second * 10)
		log.Infof("waiting to get a message on del chan")
		select {
		case msg := <-delivery:
			log.Infof("Read message: #%v (msg ID:%v) acking the message", msgCount+1, msg.GetMessage().Payload.GetID())
			msg.Ack()
		case <-timeout.C:
			log.Errorf("consumer delivery channel timed out after %v messages", msgCount)
			s.Equal(msgCount, testMsgCount) // FAIL the test
			break ReadLoop2
		}
	}

	consumerTest.Close()
}

func (s *NetIntegrationSuiteParallelE) TestWriteEndToEndSuccessWithCassandra() {
	destPath := "/dest/testWriteEndToEndCassandra"
	cgPath := "/cg/testWriteEndToEndCassandra"
	testMsgCount := 100
	log := common.GetDefaultLogger()

	// Create the client
	ipaddr, port, _ := net.SplitHostPort(s.GetFrontend().GetTChannel().PeerInfo().HostPort)
	portNum, _ := strconv.Atoi(port)
	cheramiClient, _ := client.NewClient("cherami-test", ipaddr, portNum, nil)
	defer cheramiClient.Close()

	// Create the destination to publish message
	crReq := cherami.NewCreateDestinationRequest()
	crReq.Path = common.StringPtr(destPath)
	crReq.Type = cherami.DestinationTypePtr(0)
	crReq.ConsumedMessagesRetention = common.Int32Ptr(60)
	crReq.UnconsumedMessagesRetention = common.Int32Ptr(120)
	crReq.OwnerEmail = common.StringPtr("integration_test@uber.com")

	desDesc, _ := cheramiClient.CreateDestination(crReq)
	s.NotNil(desDesc)

	// we should see the destination now in cassandra
	rReq := cherami.NewReadDestinationRequest()
	rReq.Path = common.StringPtr(destPath)
	readDesc, _ := cheramiClient.ReadDestination(rReq)
	s.NotNil(readDesc)

	// ==WRITE==

	log.Info("Write test beginning...")

	// Create the publisher
	cPublisherReq := &client.CreatePublisherRequest{
		Path: destPath,
	}

	publisherTest := cheramiClient.CreatePublisher(cPublisherReq)
	s.NotNil(publisherTest)

	err := publisherTest.Open()
	s.NoError(err)

	// Publish messages
	doneCh := make(chan *client.PublisherReceipt, testMsgCount)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 0; i < testMsgCount; i++ {
			receipt := <-doneCh
			log.Infof("client: acking id %s", receipt.Receipt)
			s.NoError(receipt.Error)
		}
		wg.Done()
	}()

	for i := 0; i < testMsgCount; i++ {
		data := []byte(fmt.Sprintf("msg_%d", i))
		id, er := publisherTest.PublishAsync(
			&client.PublisherMessage{
				Data: data,
			},
			doneCh,
		)
		log.Infof("client: publishing data: %s with id %s", data, id)
		s.NoError(er)
	}

	wg.Wait()
	publisherTest.Close()

	// ==READ==

	// Create the consumer group
	cgReq := cherami.NewCreateConsumerGroupRequest()
	cgReq.ConsumerGroupName = common.StringPtr(cgPath)
	cgReq.DestinationPath = common.StringPtr(destPath)
	cgReq.LockTimeoutInSeconds = common.Int32Ptr(30) // this is the message redilvery timeout
	cgReq.MaxDeliveryCount = common.Int32Ptr(1)
	cgReq.OwnerEmail = common.StringPtr("consumer_integration_test@uber.com")

	cgDesc, err := cheramiClient.CreateConsumerGroup(cgReq)
	s.NoError(err)
	s.NotNil(cgDesc)
	s.Equal(cgReq.GetDestinationPath(), cgDesc.GetDestinationPath(), "Wrong destination path")
	s.Equal(cgReq.GetConsumerGroupName(), cgDesc.GetConsumerGroupName(), "Wrong consumer group name")
	s.Equal(cgReq.GetLockTimeoutInSeconds(), cgDesc.GetLockTimeoutInSeconds(), "Wrong LockTimeoutInSeconds")
	s.Equal(cgReq.GetMaxDeliveryCount(), cgDesc.GetMaxDeliveryCount(), "Wrong MaxDeliveryCount")
	s.Equal(cherami.ConsumerGroupStatus_ENABLED, cgDesc.GetStatus(), "Wrong Status")
	s.Equal(cgReq.GetOwnerEmail(), cgDesc.GetOwnerEmail(), "Wrong OwnerEmail")

	cConsumerReq := &client.CreateConsumerRequest{
		Path:              destPath,
		ConsumerGroupName: cgPath,
		ConsumerName:      "TestConsumerName",
		PrefetchCount:     1,
		Options:           &client.ClientOptions{Timeout: time.Second * 30}, // this is the thrift context timeout
	}

	consumerTest := cheramiClient.CreateConsumer(cConsumerReq)
	s.NotNil(consumerTest)

	// Open the consumer channel
	delivery := make(chan client.Delivery, 1)
	delivery, err = consumerTest.Open(delivery)
	s.NoError(err)

	// Read the messages in a loop. We will exit the loop via a timeout
ReadLoop:
	for msgCount := 0; msgCount < testMsgCount; msgCount++ {
		timeout := time.NewTimer(time.Second * 45)
		log.Infof("waiting to get a message on del chan")
		select {
		case msg := <-delivery:
			log.Infof("Read message: #%v (msg ID:%v)", msgCount+1, msg.GetMessage().Payload.GetID())
			msg.Ack()
		case <-timeout.C:
			log.Errorf("consumer delivery channel timed out after %v messages", msgCount)
			s.Equal(msgCount, testMsgCount) // FAIL the test
			break ReadLoop
		}
	}

	// Check that trying to read one more message would block
	log.Info("Checking for additional messages...")
	select {
	case msg := <-delivery:
		log.Infof("Read EXTRA message: %v", msg.GetMessage().Payload.GetID())
		msg.Ack()
		s.Nil(msg)
	default:
		log.Errorf("Good: No message available to consume.")
	}

	consumerTest.Close()

	err = cheramiClient.DeleteDestination(&cherami.DeleteDestinationRequest{Path: common.StringPtr(destPath)})
	s.Nil(err, "Failed to delete destination")
}

func (s *NetIntegrationSuiteParallelE) TestWriteWithDrain() { // Disabled pending fix for flakiness
	destPath := "/dest/testWriteDrain"
	cgPath := "/cg/testWriteDrain"
	testMsgCount := 1000
	log := common.GetDefaultLogger()

	// Create the client
	ipaddr, port, _ := net.SplitHostPort(s.GetFrontend().GetTChannel().PeerInfo().HostPort)
	portNum, _ := strconv.Atoi(port)
	cheramiClient, _ := client.NewClient("cherami-test-drain", ipaddr, portNum, nil)

	// Create the destination to publish message
	crReq := cherami.NewCreateDestinationRequest()
	crReq.Path = common.StringPtr(destPath)
	crReq.Type = cherami.DestinationTypePtr(0)
	crReq.ConsumedMessagesRetention = common.Int32Ptr(60)
	crReq.UnconsumedMessagesRetention = common.Int32Ptr(120)
	crReq.OwnerEmail = common.StringPtr("integration_test@uber.com")

	desDesc, _ := cheramiClient.CreateDestination(crReq)
	s.NotNil(desDesc)

	// we should see the destination now in cassandra
	rReq := cherami.NewReadDestinationRequest()
	rReq.Path = common.StringPtr(destPath)
	readDesc, _ := cheramiClient.ReadDestination(rReq)
	s.NotNil(readDesc)

	// ==WRITE==

	log.Info("Write test beginning...")

	// Create the publisher
	cPublisherReq := &client.CreatePublisherRequest{
		Path: destPath,
	}

	publisherTest := cheramiClient.CreatePublisher(cPublisherReq)
	s.NotNil(publisherTest)

	err := publisherTest.Open()
	s.NoError(err)

	// Publish messages
	// Make the doneCh to be a smaller size so that we don't
	// fill up immediately
	doneCh := make(chan *client.PublisherReceipt, 500)
	var wg sync.WaitGroup

	for i := 0; i < testMsgCount; i++ {
		data := []byte(fmt.Sprintf("msg_%d", i))
		id, er := publisherTest.PublishAsync(
			&client.PublisherMessage{
				Data: data,
			},
			doneCh,
		)
		log.Infof("client: publishing data: %s with id %s", data, id)
		s.NoError(er)
	}

	// now read one message to figure out the extent
	receipt := <-doneCh
	s.NoError(receipt.Error)

	// parse to get the extent
	receiptParts := strings.Split(receipt.Receipt, ":")

	// Now call SealExtent on the inputhost to seal this extent
	ih := s.testBase.GetInput()
	s.NotNil(ih, "no inputhosts found")

	dReq := admin.NewDrainExtentsRequest()
	dReq.UpdateUUID = common.StringPtr(uuid.New())

	drainReq := admin.NewDrainExtents()
	drainReq.DestinationUUID = common.StringPtr(readDesc.GetDestinationUUID())
	drainReq.ExtentUUID = common.StringPtr(receiptParts[0])
	dReq.Extents = append(dReq.Extents, drainReq)

	ctx, _ := thrift.NewContext(1 * time.Minute)
	err = ih.DrainExtent(ctx, dReq)
	s.Nil(err)

	consumeCount := testMsgCount
	// Now try to get all the other messages
	wg.Add(1)
	go func() {
		for i := 0; i < testMsgCount-1; i++ {
			receipt := <-doneCh
			log.Infof("client: acking id %s", receipt.Receipt)
			if receipt.Error != nil {
				s.InDelta(testMsgCount, i, 500)
				consumeCount = i + 2 // we read one message above and we start from 0 here as well
			} else {
				break
			}
			s.NoError(receipt.Error)
		}
		wg.Done()
	}()

	wg.Wait()
	publisherTest.Close()

	// ==READ==

	// Create the consumer group
	cgReq := cherami.NewCreateConsumerGroupRequest()
	cgReq.ConsumerGroupName = common.StringPtr(cgPath)
	cgReq.DestinationPath = common.StringPtr(destPath)
	cgReq.LockTimeoutInSeconds = common.Int32Ptr(30) // this is the message redilvery timeout
	cgReq.MaxDeliveryCount = common.Int32Ptr(1)
	cgReq.OwnerEmail = common.StringPtr("consumer_integration_test@uber.com")

	cgDesc, err := cheramiClient.CreateConsumerGroup(cgReq)
	s.NoError(err)
	s.NotNil(cgDesc)
	s.Equal(cgReq.GetDestinationPath(), cgDesc.GetDestinationPath(), "Wrong destination path")
	s.Equal(cgReq.GetConsumerGroupName(), cgDesc.GetConsumerGroupName(), "Wrong consumer group name")
	s.Equal(cgReq.GetLockTimeoutInSeconds(), cgDesc.GetLockTimeoutInSeconds(), "Wrong LockTimeoutInSeconds")
	s.Equal(cgReq.GetMaxDeliveryCount(), cgDesc.GetMaxDeliveryCount(), "Wrong MaxDeliveryCount")
	s.Equal(cherami.ConsumerGroupStatus_ENABLED, cgDesc.GetStatus(), "Wrong Status")
	s.Equal(cgReq.GetOwnerEmail(), cgDesc.GetOwnerEmail(), "Wrong OwnerEmail")

	cConsumerReq := &client.CreateConsumerRequest{
		Path:              destPath,
		ConsumerGroupName: cgPath,
		ConsumerName:      "TestConsumerName",
		PrefetchCount:     500,
		Options:           &client.ClientOptions{Timeout: time.Second * 30}, // this is the thrift context timeout
	}

	consumerTest := cheramiClient.CreateConsumer(cConsumerReq)
	s.NotNil(consumerTest)

	// Open the consumer channel
	delivery := make(chan client.Delivery, 500)
	delivery, err = consumerTest.Open(delivery)
	s.NoError(err)

	// Read the messages in a loop. We will exit the loop via a timeout
ReadLoop:
	for msgCount := 0; msgCount < consumeCount; msgCount++ {
		timeout := time.NewTimer(time.Second * 45)
		log.Infof("waiting to get a message on del chan")
		select {
		case msg := <-delivery:
			log.Infof("Read message: #%v (msg ID:%v)", msgCount+1, msg.GetMessage().Payload.GetID())
			msg.Ack()
		case <-timeout.C:
			log.Errorf("consumer delivery channel timed out after %v messages", msgCount)
			s.Equal(msgCount, consumeCount) // FAIL the test
			break ReadLoop
		}
	}

	// Check that trying to read one more message would block
	log.Info("Checking for additional messages...")
	select {
	case msg := <-delivery:
		log.Infof("Read EXTRA message: %v", msg.GetMessage().Payload.GetID())
		msg.Ack()
		s.Nil(msg)
	default:
		log.Errorf("Good: No message available to consume.")
	}

	consumerTest.Close()

	err = cheramiClient.DeleteDestination(&cherami.DeleteDestinationRequest{Path: common.StringPtr(destPath)})
	s.Nil(err, "Failed to delete destination")
}

func (s *NetIntegrationSuiteSerial) TestWriteEndToEndMultipleStore() {
	destPath := "/dest/testCassandraMultiple"
	cgPath := "/cg/testCassandraMultiple"
	testMsgCount := 10
	numReplicas := 2
	log := common.GetDefaultLogger()

	// This test starts 2 storehosts, publishes some messages, consumes half the messages from
	// one store and the other half from the other store
	s.testBase.SetUp(map[string]int{common.StoreServiceName: numReplicas}, numReplicas)

	// Create the client
	ipaddr, port, _ := net.SplitHostPort(s.GetFrontend().GetTChannel().PeerInfo().HostPort)
	portNum, _ := strconv.Atoi(port)
	cheramiClient := createCheramiClient("cherami-test-multiple", ipaddr, portNum, nil)
	defer cheramiClient.Close()

	// Create the destination to publish message
	crReq := cherami.NewCreateDestinationRequest()
	crReq.Path = common.StringPtr(destPath)
	crReq.Type = cherami.DestinationTypePtr(0)
	crReq.ConsumedMessagesRetention = common.Int32Ptr(60)
	crReq.UnconsumedMessagesRetention = common.Int32Ptr(120)
	crReq.OwnerEmail = common.StringPtr("lhcIntegration@uber.com")

	desDesc, _ := cheramiClient.CreateDestination(crReq)
	s.NotNil(desDesc)

	// we should see the destination now in cassandra
	rReq := cherami.NewReadDestinationRequest()
	rReq.Path = common.StringPtr(destPath)
	readDesc, _ := cheramiClient.ReadDestination(rReq)
	s.NotNil(readDesc)

	// ==WRITE==

	log.Info("Write test beginning...")

	// Create the publisher
	cPublisherReq := &client.CreatePublisherRequest{
		Path: destPath,
	}

	publisherTest := cheramiClient.CreatePublisher(cPublisherReq)
	s.NotNil(publisherTest)

	err := publisherTest.Open()
	s.NoError(err)

	// Publish messages
	doneCh := make(chan *client.PublisherReceipt, testMsgCount)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 0; i < testMsgCount; i++ {
			receipt := <-doneCh
			log.Infof("client: acking id %s", receipt.Receipt)
			s.NoError(receipt.Error)
		}
		wg.Done()
	}()

	for i := 0; i < testMsgCount; i++ {
		data := []byte(fmt.Sprintf("msg_%d", i))
		id, er := publisherTest.PublishAsync(
			&client.PublisherMessage{
				Data: data,
			},
			doneCh,
		)
		log.Infof("client: publishing data: %s with id %s", data, id)
		s.NoError(er)
	}

	wg.Wait()
	publisherTest.Close()

	// ==READ==

	// Create the consumer group
	cgReq := cherami.NewCreateConsumerGroupRequest()
	cgReq.ConsumerGroupName = common.StringPtr(cgPath)
	cgReq.DestinationPath = common.StringPtr(destPath)
	cgReq.LockTimeoutInSeconds = common.Int32Ptr(30) // this is the message redilvery timeout
	cgReq.MaxDeliveryCount = common.Int32Ptr(1)
	cgReq.OwnerEmail = common.StringPtr("consumer_integration_test@uber.com")

	cgDesc, err := cheramiClient.CreateConsumerGroup(cgReq)
	s.NoError(err)
	s.NotNil(cgDesc)
	s.Equal(cgReq.GetDestinationPath(), cgDesc.GetDestinationPath(), "Wrong destination path")
	s.Equal(cgReq.GetConsumerGroupName(), cgDesc.GetConsumerGroupName(), "Wrong consumer group name")
	s.Equal(cgReq.GetLockTimeoutInSeconds(), cgDesc.GetLockTimeoutInSeconds(), "Wrong LockTimeoutInSeconds")
	s.Equal(cgReq.GetMaxDeliveryCount(), cgDesc.GetMaxDeliveryCount(), "Wrong MaxDeliveryCount")
	s.Equal(cherami.ConsumerGroupStatus_ENABLED, cgDesc.GetStatus(), "Wrong Status")
	s.Equal(cgReq.GetOwnerEmail(), cgDesc.GetOwnerEmail(), "Wrong Owner Email")

	cConsumerReq := &client.CreateConsumerRequest{
		Path:              destPath,
		ConsumerGroupName: cgPath,
		ConsumerName:      "TestWriteEndToEndMultipleStoreConsumerName",
		PrefetchCount:     1,
		Options:           &client.ClientOptions{Timeout: time.Second * 30}, // this is the thrift context timeout
	}

	consumerTest := cheramiClient.CreateConsumer(cConsumerReq)
	s.NotNil(consumerTest)

	// Open the consumer channel
	delivery := make(chan client.Delivery, 1)
	delivery, err = consumerTest.Open(delivery)
	s.NoError(err)

	msgCount := 0
	// Read the messages in a loop. We will exit the loop via a timeout
ReadLoop:
	for msgCount = 0; msgCount < testMsgCount/2; msgCount++ {
		timeout := time.NewTimer(time.Second * 45)
		log.Infof("waiting to get a message on del chan")
		select {
		case msg := <-delivery:
			log.Infof("Read message: #%v (msg ID:%v)", msgCount+1, msg.GetMessage().Payload.GetID())
			msg.Ack()
		case <-timeout.C:
			log.Errorf("consumer delivery channel timed out after %v messages", msgCount)
			s.Equal(msgCount, testMsgCount) // FAIL the test
			break ReadLoop
		}
	}

	outputHost := s.testBase.GetOutput()
	s.NotNil(outputHost, "no outputhosts found")

	connStore := outputHost.UtilGetPickedStore(cgPath, destPath)
	if sHost, ok := s.testBase.StoreHosts[connStore]; ok {
		log.Infof("shutting down store.. %v", connStore)
		sHost.Shutdown()
		// remove this from the map
		delete(s.testBase.StoreHosts, connStore)
	}

	log.Infof("Start reading again (from another replica) ")
ReadLoop2:
	for ; msgCount < testMsgCount; msgCount++ {
		timeout := time.NewTimer(time.Second * 45)
		log.Infof("waiting to get a message on del chan")
		select {
		case msg := <-delivery:
			log.Infof("Read message: #%v (msg ID:%v)", msgCount+1, msg.GetMessage().Payload.GetID())
			msg.Ack()
		case <-timeout.C:
			log.Errorf("consumer delivery channel timed out after %v messages", msgCount)
			s.Equal(msgCount, testMsgCount) // FAIL the test
			break ReadLoop2
		}
	}

	consumerTest.Close()
}

// TestTimerQueue tests publish and consume on a timer-destination.
// The test first creates a destination and a consumer-group for the destination. It
// then starts the consumer and a publisher. The consumer reads half the messages
// that were published before waiting for the publisher to finish publishing all of the
// messages -- the consumer then reads the rest of the messages, from the "backlog".
func (s *NetIntegrationSuiteParallelB) _TestTimerQueue() {

	destPath, cg1Path, cg2Path := "/test.dest/TestTimerQueue", "/test.cg/TestTimerQueue", "/test.cg.backlog/TestTimerQueue"

	minMsgDelay, maxMsgDelay := 3, 5 // delay to use on messages

	// since the store uses the bottom 26-bits for seq-num, a message could
	// technically be delivered (1 << 26) nanoseconds too soon!
	var minExpectedDrift = -1 * ((1 << 26) * time.Nanosecond)
	var maxExpectedDrift = time.Second // fail if the message is seen too late!

	testMsgCount := 1000

	log := common.GetDefaultLogger()

	// Create the client
	ipaddr, port, _ := net.SplitHostPort(s.GetFrontend().GetTChannel().PeerInfo().HostPort)
	portNum, _ := strconv.Atoi(port)
	cheramiClient := createCheramiClient("cherami-test-timer", ipaddr, portNum, nil)
	defer cheramiClient.Close()

	// Create the destination to publish message
	crReq := cherami.NewCreateDestinationRequest()
	crReq.Path = common.StringPtr(destPath)
	crReq.Type = cherami.DestinationTypePtr(cherami.DestinationType_TIMER)
	crReq.ConsumedMessagesRetention = common.Int32Ptr(60)
	crReq.UnconsumedMessagesRetention = common.Int32Ptr(120)
	crReq.OwnerEmail = common.StringPtr("cherami-test+TestTimerQueue@uber.com")

	desDesc, _ := cheramiClient.CreateDestination(crReq)
	s.NotNil(desDesc)

	// we should see the destination now in cassandra
	rReq := cherami.NewReadDestinationRequest()
	rReq.Path = common.StringPtr(destPath)
	readDesc, _ := cheramiClient.ReadDestination(rReq)
	s.NotNil(readDesc)

	// Create the publisher
	cPublisherReq := &client.CreatePublisherRequest{
		Path: destPath,
	}

	publisherTest := cheramiClient.CreatePublisher(cPublisherReq)
	s.NotNil(publisherTest)

	// Create the (first) consumer group
	cgReq := cherami.NewCreateConsumerGroupRequest()
	cgReq.ConsumerGroupName = common.StringPtr(cg1Path)
	cgReq.DestinationPath = common.StringPtr(destPath)
	cgReq.LockTimeoutInSeconds = common.Int32Ptr(30) // this is the message redilvery timeout
	cgReq.MaxDeliveryCount = common.Int32Ptr(1)
	cgReq.OwnerEmail = common.StringPtr("cherami-test+TestTimerQueue@uber.com")

	_, err := cheramiClient.CreateConsumerGroup(cgReq)
	s.NoError(err)

	cConsumerReq := &client.CreateConsumerRequest{
		Path:              destPath,
		ConsumerGroupName: cg1Path,
		ConsumerName:      "TestTimerQueue",
		PrefetchCount:     1,
		Options:           &client.ClientOptions{Timeout: time.Second * 30}, // this is the thrift context timeout
	}

	consumerTest := cheramiClient.CreateConsumer(cConsumerReq)
	s.NotNil(consumerTest)

	// -- start consumer that is keeping up; this would read only half the messages
	delivery := make(chan client.Delivery, 1)
	delivery, err = consumerTest.Open(delivery)
	s.NoError(err)

	var wgConsumer sync.WaitGroup
	recvCount := 0

	var maxDrift time.Duration

	wgConsumer.Add(1)
	go func() {
		defer wgConsumer.Done()

	ReadLoop:
		for recvCount = 0; recvCount < testMsgCount; recvCount++ {

			timeout := time.NewTimer(time.Second * 45)

			select {
			case msg := <-delivery:
				var eta time.Time
				eta, err = time.Parse(time.RFC3339Nano, string(msg.GetMessage().Payload.GetData()))
				s.NoError(err)

				now := time.Now()
				drift := now.Sub(eta)

				log.Infof("consumer: recv msg id: %v [addr=%x drift=%v]",
					msg.GetMessage().Payload.GetID(), msg.GetMessage().GetAddress(), drift)

				// verify we did not get the message too soon .. or too late!

				if drift > maxDrift {
					maxDrift = drift
				}

				if drift < minExpectedDrift || drift >= maxExpectedDrift {
					log.Errorf("msg received too late or too soon! (eta=%v now=%v drift=%v)",
						eta.Format(time.RFC3339), now.Format(time.RFC3339), drift)
				}

				s.True(drift >= minExpectedDrift,
					fmt.Sprintf("msg received too soon! (eta=%v now=%v drift=%v)", eta.Format(time.RFC3339), now.Format(time.RFC3339), drift))

				s.True(drift < maxExpectedDrift,
					fmt.Sprintf("msg received too late! (eta=%v now=%v drift=%v)", eta.Format(time.RFC3339), now.Format(time.RFC3339), drift))

				msg.Ack()

			case <-timeout.C:
				log.Errorf("consumer: timed-out (count=%v)", recvCount)
				s.Equal(recvCount, testMsgCount) // FAIL the test
				break ReadLoop
			}
		}
	}()

	// -- start publisher and publish all messages
	err = publisherTest.Open()
	s.NoError(err)

	doneCh := make(chan *client.PublisherReceipt, testMsgCount)
	var wgPublisher sync.WaitGroup
	wgPublisher.Add(1)
	go func() {
		defer wgPublisher.Done()

		for i := 0; i < testMsgCount; i++ {
			receipt := <-doneCh
			log.Infof("publisher: recv ack id: %s [receipt=%v]", receipt.ID, receipt.Receipt)
			s.NoError(receipt.Error)
		}
	}()

	// NB: we wait 10 seconds here so that it gives a chance for the client to
	// do a reconfigure on the "slow path"; this ensures that all the cg-extents
	// have been loaded. this is necessary because the test measures and checks
	// for the max delay, and it would fail if all the cg-extents are not ready.
	time.Sleep(10 * time.Second)

	for i := 0; i < testMsgCount; i++ {

		delay := time.Duration(minMsgDelay+rand.Intn(maxMsgDelay-minMsgDelay+1)) * time.Second
		eta := time.Now().Add(delay)

		id, er := publisherTest.PublishAsync(
			&client.PublisherMessage{
				Data:  []byte(eta.Format(time.RFC3339Nano)), // put ETA in message payload
				Delay: delay,
			},
			doneCh,
		)

		log.Infof("publisher: sent msg id: %s (delay=%v eta=%v)", id, delay, eta)
		s.NoError(er)

		time.Sleep(25 * time.Millisecond) // simulate a slow publish
	}

	// -- wait for the publisher to be done and the consumer to be done (reading half the messages)
	wgPublisher.Wait()
	publisherTest.Close()
	wgConsumer.Wait()

	consumerTest.Close()

	log.Infof("max drift=%v", maxDrift)

	// Create the (second) consumer group -- this would read from the backlog
	cgReq = cherami.NewCreateConsumerGroupRequest()
	cgReq.ConsumerGroupName = common.StringPtr(cg2Path)
	cgReq.DestinationPath = common.StringPtr(destPath)
	cgReq.LockTimeoutInSeconds = common.Int32Ptr(30) // this is the message redilvery timeout
	cgReq.MaxDeliveryCount = common.Int32Ptr(1)
	cgReq.OwnerEmail = common.StringPtr("cherami-test+TestTimerQueue@uber.com")

	_, err = cheramiClient.CreateConsumerGroup(cgReq)
	s.NoError(err, fmt.Sprintf("CreateConsumerGroup failed: %v", err))

	cConsumerReq = &client.CreateConsumerRequest{
		Path:              destPath,
		ConsumerGroupName: cg2Path,
		ConsumerName:      "TestTimerQueue-Backlog",
		PrefetchCount:     1,
		Options:           &client.ClientOptions{Timeout: time.Second * 30}, // this is the thrift context timeout
	}

	consumerTest = cheramiClient.CreateConsumer(cConsumerReq)
	s.NotNil(consumerTest)

	// start consumer
	delivery, err = consumerTest.Open(delivery)
	s.NoError(err)

	log.Infof("consumer: reading from backlog")
ReadLoop2:
	for recvCount = 0; recvCount < testMsgCount; recvCount++ {

		timeout := time.NewTimer(time.Second * 45)

		select {
		case msg := <-delivery:
			var eta time.Time
			eta, err = time.Parse(time.RFC3339Nano, string(msg.GetMessage().Payload.GetData()))
			s.NoError(err)

			now := time.Now()
			drift := now.Sub(eta)

			log.Infof("consumer: recv msg id: %v [addr=%x drift=%v]",
				msg.GetMessage().Payload.GetID(), msg.GetMessage().GetAddress(), drift)

			// verify we did not get the message too soon. since we are currenlty reading
			// from the "backlog", we cannot put a threshold on the max drift.
			s.True(drift >= minExpectedDrift,
				fmt.Sprintf("msg received too soon! (eta=%v now=%v drift=%v", eta.Format(time.RFC3339), now.Format(time.RFC3339), drift))

			msg.Ack()

		case <-timeout.C:
			log.Errorf("consumer: timed-out (count=%v)", recvCount)
			s.Equal(recvCount, testMsgCount) // FAIL the test
			break ReadLoop2
		}
	}

	consumerTest.Close()
}

func (s *NetIntegrationSuiteParallelA) TestDLQWithCassandra() {
	const (
		destPath                = `/test.runner.SmartRetry/TestDLQWithCassandra` // This path ensures that throttling is limited for this test
		cgPath                  = `/test.runner.SmartRetry/TestDLQWithCassandraCG`
		cgMaxDeliveryCount      = 2
		cgLockTimeout           = 1
		cgExpectedDeliveryCount = cgMaxDeliveryCount + 1
		cgVerifyLoopTimeout     = time.Minute * 2
		cgVerifyLoopTicker      = (cgLockTimeout * time.Second) / 2
		cnsmrPrefetch           = 10
		publisherPubInterval    = 150

		DLQMergeMessageTargetCount = 10
		DLQPurgeMessageTargetCount = 10
		DLQMessageStart            = 5
		DLQMessageSpacing          = 2

		// DLQ Delivery map special values
		/* >0 = regular delivery count */
		merged = -1
		purged = -2
	)

	const (
		// Operation/Phase IDS
		noOp = iota
		purgeOp
		mergeOp
		done
		PhaseCount
	)

	dlqDeliveryMap := make(map[int]int) // Key is the message ID from GetID(), ok = scheduled to be not-acked; > 0 = count of deliveries, < 0 = merged or purged
	dlqAckIDMap := make(map[int]string) // Key is the message ID from GetID(), value is the ackID
	var dlqMutex sync.Mutex
	dlqProductionTargets := []int{DLQMergeMessageTargetCount, DLQPurgeMessageTargetCount, 0, 0}
	phase := noOp

	log := common.GetDefaultLogger()
	// ll - local log
	ll := func() bark.Logger {
		return (common.GetDefaultLogger()).WithFields(bark.Fields{`phase`: phase})
	}

	// lll - local log with locking
	lll := func() bark.Logger {
		dlqMutex.Lock()
		defer dlqMutex.Unlock()
		return ll()
	}

	// Create the client
	ipaddr, port, _ := net.SplitHostPort(s.GetFrontend().GetTChannel().PeerInfo().HostPort)
	portNum, _ := strconv.Atoi(port)
	cheramiClient := createCheramiClient("cherami-test-dlq", ipaddr, portNum, nil)
	defer cheramiClient.Close()

	// Create the destination to publish message
	crReq := cherami.NewCreateDestinationRequest()
	crReq.Path = common.StringPtr(destPath)
	crReq.Type = cherami.DestinationTypePtr(cherami.DestinationType_PLAIN)
	crReq.ConsumedMessagesRetention = common.Int32Ptr(60)
	crReq.UnconsumedMessagesRetention = common.Int32Ptr(120)
	crReq.OwnerEmail = common.StringPtr("lhcIntegration@uber.com")

	desDesc, _ := cheramiClient.CreateDestination(crReq)
	s.NotNil(desDesc)

	// we should see the destination now in cassandra
	rReq := cherami.NewReadDestinationRequest()
	rReq.Path = common.StringPtr(destPath)
	readDesc, _ := cheramiClient.ReadDestination(rReq)
	s.NotNil(readDesc)

	// ==WRITE==

	// Create the publisher
	cPublisherReq := &client.CreatePublisherRequest{
		Path: destPath,
	}

	publisherTest := cheramiClient.CreatePublisher(cPublisherReq)
	s.NotNil(publisherTest)

	err := publisherTest.Open()
	log.Errorf("error is: %v", err)
	s.NoError(err)

	// Publish messages continuously in a goroutine;
	// This ensures a steady supply of 'good' messages so that smart retry will not affect us
	closeCh := make(chan struct{})
	publisherCloseCh := make(chan struct{})
	defer close(closeCh)

	go func() {
		i := 0
		defer publisherTest.Close()
		defer log.WithField(`messageCount`, i).Info("DONE PUBLISHING MESSAGES")
		ticker := time.NewTicker(publisherPubInterval)
		for {
			select {
			case <-ticker.C:
				data := []byte(fmt.Sprintf("msg_%d", i+1))
				// log.Infof("Published: %v", string(data))
				receipt := publisherTest.Publish(&client.PublisherMessage{Data: data})
				s.NoError(receipt.Error)
				i++
			case <-closeCh:
				return
			case <-publisherCloseCh:
				return
			}
		}
	}()

	// ==READ==

	// Create the consumer group
	cgReq := cherami.NewCreateConsumerGroupRequest()
	cgReq.ConsumerGroupName = common.StringPtr(cgPath)
	cgReq.DestinationPath = common.StringPtr(destPath)
	cgReq.LockTimeoutInSeconds = common.Int32Ptr(1) // this is the message redelivery timeout
	cgReq.MaxDeliveryCount = common.Int32Ptr(1)
	cgReq.OwnerEmail = common.StringPtr("consumer_integration_test@uber.com")

	cgDesc, err := cheramiClient.CreateConsumerGroup(cgReq)
	s.NoError(err)
	s.NotNil(cgDesc)
	s.Equal(cgReq.GetDestinationPath(), cgDesc.GetDestinationPath(), "Wrong destination path")
	s.Equal(cgReq.GetConsumerGroupName(), cgDesc.GetConsumerGroupName(), "Wrong consumer group name")
	s.Equal(cgReq.GetLockTimeoutInSeconds(), cgDesc.GetLockTimeoutInSeconds(), "Wrong LockTimeoutInSeconds")
	s.Equal(cgReq.GetMaxDeliveryCount(), cgDesc.GetMaxDeliveryCount(), "Wrong MaxDeliveryCount")
	s.Equal(cherami.ConsumerGroupStatus_ENABLED, cgDesc.GetStatus(), "Wrong Status")
	s.Equal(cgReq.GetOwnerEmail(), cgDesc.GetOwnerEmail(), "Wrong Owner Email")

	// Update the consumer group (verifies that update doesn't destroy DLQ settings)
	cgUpdateReq := cherami.NewUpdateConsumerGroupRequest()
	cgUpdateReq.ConsumerGroupName = common.StringPtr(cgDesc.GetConsumerGroupName())
	cgUpdateReq.DestinationPath = common.StringPtr(cgDesc.GetDestinationPath())
	cgUpdateReq.LockTimeoutInSeconds = common.Int32Ptr(cgLockTimeout)  // this is the real message redelivery timeout
	cgUpdateReq.MaxDeliveryCount = common.Int32Ptr(cgMaxDeliveryCount) // this is the real message redelivery count
	cgUpdateReq.Status = cherami.ConsumerGroupStatusPtr(cgDesc.GetStatus())
	cgUpdateReq.SkipOlderMessagesInSeconds = common.Int32Ptr(cgDesc.GetSkipOlderMessagesInSeconds())
	cgUpdateReq.OwnerEmail = common.StringPtr("consumer_integration_test_update@uber.com")

	cgDesc2, err := cheramiClient.UpdateConsumerGroup(cgUpdateReq)
	s.NoError(err)
	s.NotNil(cgDesc2)

	s.Equal(cgUpdateReq.GetDestinationPath(), cgDesc2.GetDestinationPath(), "Wrong destination path")
	s.Equal(cgUpdateReq.GetConsumerGroupName(), cgDesc2.GetConsumerGroupName(), "Wrong consumer group name")
	s.Equal(cgUpdateReq.GetLockTimeoutInSeconds(), cgDesc2.GetLockTimeoutInSeconds(), "Wrong LockTimeoutInSeconds")
	s.Equal(cgUpdateReq.GetSkipOlderMessagesInSeconds(), cgDesc2.GetSkipOlderMessagesInSeconds(), "Wrong SkipOlderMessagesInSeconds")
	s.Equal(cgUpdateReq.GetMaxDeliveryCount(), cgDesc2.GetMaxDeliveryCount(), "Wrong MaxDeliveryCount")
	s.Equal(cherami.ConsumerGroupStatus_ENABLED, cgDesc2.GetStatus(), "Wrong Status")
	s.Equal(cgUpdateReq.GetOwnerEmail(), cgDesc2.GetOwnerEmail(), "Wrong Owner Email")

	// Verify that the DLQ Destination can be read
	dReq := shared.NewReadDestinationRequest()
	dReq.DestinationUUID = common.StringPtr(cgDesc2.GetDeadLetterQueueDestinationUUID())
	dlqDestDesc, err := s.mClient.ReadDestination(nil, dReq)
	s.Nil(err)
	s.NotNil(dlqDestDesc)

	cConsumerReq := &client.CreateConsumerRequest{
		Path:              destPath,
		ConsumerGroupName: cgPath,
		ConsumerName:      "TestDLQWithCassandraConsumerName",
		PrefetchCount:     cnsmrPrefetch,
	}

	consumerTest := cheramiClient.CreateConsumer(cConsumerReq)
	s.NotNil(consumerTest)

	// Open the consumer channel
	delivery := make(chan client.Delivery, 1)
	delivery, err = consumerTest.Open(delivery)
	s.NoError(err)

	// Check if we have received the not-acked messages the right number of times
	checkDeliveryMap := func() bool {
		ret := true
		for id, deliveryCount := range dlqDeliveryMap {
			// Pending T459454 //s.Assert().False(deliveryCount > cgExpectedDeliveryCount, "Msg %v was overdelivered (%v > %v), AckID: %v", id, deliveryCount, cgExpectedDeliveryCount, dlqAckIDMap[id])
			s.False(deliveryCount > 2*cgExpectedDeliveryCount, "Msg %v was greatly overdelivered (%v > %v), AckID: %v", id, deliveryCount, cgExpectedDeliveryCount, dlqAckIDMap[id])
			if deliveryCount >= 0 && deliveryCount < cgExpectedDeliveryCount {
				ret = false
			}
		}
		return ret
	}

	dlqConsumerTest := func(expected map[int]struct{}) {
		// Create DLQ consumer group
		cgReq.ConsumerGroupName = common.StringPtr(cgReq.GetConsumerGroupName() + `_DLQ`)
		cgReq.DestinationPath = common.StringPtr(cgDesc.GetDeadLetterQueueDestinationUUID())
		cgReq.StartFrom = common.Int64Ptr(1)
		cgDescDLQ, errdlq := cheramiClient.CreateConsumerGroup(cgReq)
		s.NoError(errdlq)
		s.NotNil(cgDescDLQ)

		cConsumerReq.Path = cgReq.GetDestinationPath()
		cConsumerReq.ConsumerGroupName = cgReq.GetConsumerGroupName()
		cConsumerReq.ConsumerName = "TestDLQWithCassandraDLQConsumerName"

		DLQConsumerTest := cheramiClient.CreateConsumer(cConsumerReq)
		s.NotNil(DLQConsumerTest)
		defer DLQConsumerTest.Close()

		// Open the consumer channel
		dlqDelivery := make(chan client.Delivery, 1)
		dlqDelivery, errdlq = DLQConsumerTest.Open(dlqDelivery)
		s.NoError(errdlq)

		for len(expected) > 0 {
			select {
			case msg := <-dlqDelivery:
				s.NotNil(msg)
				msg.Ack()
				msgID, _ := strconv.Atoi(string(msg.GetMessage().GetPayload().GetData()[4:]))
				delete(expected, msgID)
			case <-time.After(time.Minute):
				s.Fail(`DLQ Consumer Group timed out before receiving all messages`)
			}
		}

		// Verify that we can delete a DLQ consumer group
		cgDeleteReq := cherami.NewDeleteConsumerGroupRequest()
		cgDeleteReq.ConsumerGroupName = cgReq.ConsumerGroupName
		cgDeleteReq.DestinationPath = cgReq.DestinationPath
		errdlq = cheramiClient.DeleteConsumerGroup(cgDeleteReq)
		s.NoError(errdlq)
	}

	phaseCh := make(chan int, PhaseCount)

	go func() {
		// Read the messages in a loop.
		dlqProductionCounts := make([]int, PhaseCount)
		newPhase := phase
		for msgCount := 0; ; {
			select {
			case msg := <-delivery:
				dlqMutex.Lock()
				msgCount++
				msgID, _ := strconv.Atoi(string(msg.GetMessage().GetPayload().GetData()[4:]))

				v, ok := dlqDeliveryMap[msgID]

				if !ok &&
					msgID > DLQMessageStart &&
					msgID%DLQMessageSpacing == 0 &&
					dlqProductionCounts[phase] < dlqProductionTargets[phase] {
					dlqDeliveryMap[msgID] = 0
					dlqProductionCounts[phase]++
					dlqAckIDMap[msgID] = msg.GetDeliveryToken()
					v, ok = dlqDeliveryMap[msgID]
				}

				if ok {
					switch {
					case v >= 0:
						dlqDeliveryMap[msgID]++
					case v == purged:
						// Pending T459454 -- If a message wasn't actually delivered to DLQ, mark it as just DLQ delivered again
						if msg.GetDeliveryToken() == dlqAckIDMap[msgID] {
							ll().Infof(`Defect in purge, msg %d, ackID %v`, msgID, msg.GetDeliveryToken())
							dlqDeliveryMap[msgID] = cgExpectedDeliveryCount
						} else { // If the delivery token is new, that means that this message came from the DLQ
							ll().Errorf(`PURGE FAIL: msg %d, ackID orig/dlq %v/%v`, msgID, dlqAckIDMap[msgID], msg.GetDeliveryToken())
							s.Assert().NotEqual(v, purged, `Purged message was unexpectedly delivered, %v\%v\%v`, msgID, dlqAckIDMap[msgID], msg.GetDeliveryToken())
							msg.Ack()
						}
					case v == merged:
						// Pending T459454 -- If a message wasn't actually delivered to DLQ, just ack it and remove it from the dlqDeliveryMap
						if msg.GetDeliveryToken() == dlqAckIDMap[msgID] {
							ll().Infof(`Defect in merge, msg %d, ackID %v`, msgID, msg.GetDeliveryToken())
						}

						// If the delivery token is new, that means that this message was merged from the DLQ
						delete(dlqDeliveryMap, msgID)
						msg.Ack()
					}
				} else { // Normal messages just ack
					msg.Ack()
				}

				if phase == newPhase && phase != done && dlqProductionCounts[phase] >= dlqProductionTargets[phase] && checkDeliveryMap() {
					ll().Infof(`dlqProductionCount=%v`, dlqProductionCounts[phase])
					ll().Infof(`dlqProductionTarget=%v`, dlqProductionTargets[phase])
					newPhase = phase + 1
					phaseCh <- newPhase // Signal the operations loop to move to the next phase
					for id, v := range dlqDeliveryMap {
						ll().WithField(common.TagAckID, dlqAckIDMap[msgID]).Infof("DLQ Message %v status %v", id, v)
					}
				}

				dlqMutex.Unlock()
			case <-closeCh:
				log.Info(`READLOOP: closed normally`)
				return
			}
		}

	}()

	msgsExpectedInDLQ := func() map[int]struct{} {
		dlqMutex.Lock()
		defer dlqMutex.Unlock()
		result := make(map[int]struct{})
		for id, v := range dlqDeliveryMap {
			if v >= cgExpectedDeliveryCount {
				result[id] = struct{}{}
			}
		}
		return result
	}

operationsLoop:
	for {

		fe := s.GetFrontend()
		s.NotNil(fe)
		mergeReq := cherami.NewMergeDLQForConsumerGroupRequest()
		mergeReq.DestinationPath = common.StringPtr(destPath)
		mergeReq.ConsumerGroupName = common.StringPtr(cgPath)
		purgeReq := cherami.NewPurgeDLQForConsumerGroupRequest()
		purgeReq.DestinationPath = common.StringPtr(destPath)
		purgeReq.ConsumerGroupName = common.StringPtr(cgPath)

		p := <-phaseCh
		switch p {
		case purgeOp:

			dlqConsumerTest(msgsExpectedInDLQ())

			// Purge DLQ
			err = fe.PurgeDLQForConsumerGroup(nil, purgeReq)

			// Verify that repeating the request succeeds
			err = fe.PurgeDLQForConsumerGroup(nil, purgeReq)
			s.NoError(err)

			// Verify that immediately issuing a merge request fails
			// Note that this test could fail if the controller has somehow finished processing the above merge already (race condition)

			err = fe.MergeDLQForConsumerGroup(nil, mergeReq)
			s.Error(err)

			dlqMutex.Lock()
			ll().Info(`Performed purge`)

			// Mark all messages that should be in DLQ as purged
			for id, v := range dlqDeliveryMap {
				if v >= cgExpectedDeliveryCount {
					dlqDeliveryMap[id] = purged
				}
			}

			dlqMutex.Unlock()

			// Wait for operation to complete
			lll().Info(`Waiting for purge operation to complete...`)
			waitTime := time.Now()
			cond := func() bool {
				dlqDestDesc, err = s.mClient.ReadDestination(nil, dReq)
				s.Nil(err)
				s.NotNil(dlqDestDesc)
				if dlqDestDesc.DLQPurgeBefore == nil {
					panic(`foo`)
				}
				return dlqDestDesc.GetDLQPurgeBefore() == 0
			}

			succ := common.SpinWaitOnCondition(cond, time.Minute)
			s.True(succ, "dlq purge operation timed out")

			dlqMutex.Lock()
			ll().Infof(`Performed purge, waited %v for purge to clear`, time.Since(waitTime))
			dlqMutex.Unlock()

		case mergeOp:
			dlqConsumerTest(msgsExpectedInDLQ())
			// Merge DLQ
			err = fe.MergeDLQForConsumerGroup(nil, mergeReq)
			s.NoError(err)

			// Verify that repeating the merge request succeeds
			err = fe.MergeDLQForConsumerGroup(nil, mergeReq)
			s.NoError(err)

			// Verify that immediately issuing a purge request fails
			// Note that this test could fail if the controller has somehow finished processing the above merge already (race condition)

			err = fe.PurgeDLQForConsumerGroup(nil, purgeReq)
			s.Error(err)

			dlqMutex.Lock()
			ll().Infof(`Performed merge`)

			// Mark all messages that should be in DLQ as merged
			for id, v := range dlqDeliveryMap {
				if v >= cgExpectedDeliveryCount {
					dlqDeliveryMap[id] = merged
				}
			}

			dlqMutex.Unlock()

			// close the publisher, we no longer need it
			publisherCloseCh <- struct{}{}

		case done:

			verifyTimeout := time.NewTimer(cgVerifyLoopTimeout)
			verifyTicker := time.NewTicker(cgVerifyLoopTicker)
			unmergedMessages := DLQMergeMessageTargetCount * 1000
			for {

				select {
				case <-verifyTicker.C:
					dlqMutex.Lock()
					um := 0
					for _, v := range dlqDeliveryMap {
						if v == merged {
							um++
						}
					}

					// Keep waiting as long as progress is being made
					if um < unmergedMessages {
						verifyTimeout.Reset(cgVerifyLoopTimeout)
						unmergedMessages = um
					}

					if unmergedMessages == 0 {
						ll().Info(`All messages merged`)
						dlqMutex.Unlock()
						break operationsLoop
					} else {
						ll().Infof(`Waiting for %v messages to merge...`, unmergedMessages)
					}
					dlqMutex.Unlock()
				case <-verifyTimeout.C:
					s.Assert().Fail(`Verification loop timed out`)
					break operationsLoop
				}

			}
		}

		if p < done {
			dlqMutex.Lock()
			phase = p
			ll().Infof(`Changed phase`)
			dlqMutex.Unlock()
		}
	}

	// Output any unmerged messages for diagnostics
	for id, v := range dlqDeliveryMap {
		log.WithField(common.TagAckID, dlqAckIDMap[id]).Errorf("DLQ Message %v wasn't merged [%v]", id, v)
	}

}

func (s *NetIntegrationSuiteParallelD) _TestSmartRetryDisableDuringDLQMerge() {
	const (
		destPath                   = `/test.runner.SmartRetry/SRDDDM` // This path ensures that throttling is limited for this test
		cgPath                     = `/test.runner.SmartRetry/SRDDDMCG`
		metricsName                = `_test.runner.SmartRetry_SRDDDM`
		cgMaxDeliveryCount         = 1
		cgLockTimeout              = 1
		cnsmrPrefetch              = 10
		publisherPubInterval       = time.Second / 5
		publisherPubSlowInterval   = publisherPubInterval * 10 // This is 10x slower
		DLQPublishClearTime        = cgLockTimeout * time.Second * 2
		DLQMergeMessageTargetCount = 2
		DLQMessageStart            = 10
		DLQMessageSpacing          = 6
		mergeAssumedCompleteTime   = cgLockTimeout * (cgMaxDeliveryCount + 1) * 2 * time.Second * 2 // +1 for initial delivery, *2 for dlqInhibit, *2 for fudge
	)

	const (
		// Operation/Phase IDS
		produceDLQ = iota
		smartRetryProvoke1
		mergeOp
		smartRetryProvoke3
		smartRetryProvoke4
		done
		PhaseCount
	)

	const (
		stateStalled = iota
		stateIdle
		stateProgressing
	)

	var dlqMutex sync.RWMutex
	phase := produceDLQ
	var currentHealth = stateIdle
	var dlqDeliveryCount int
	var dlqDeliveryTime time.Time
	phaseOnce := make([]sync.Once, PhaseCount)

	// ll - local log
	ll := func(fmtS string, rest ...interface{}) {
		common.GetDefaultLogger().WithFields(bark.Fields{`phase`: phase}).Infof(fmtS, rest...)
	}

	// lll - local log with lock (for race on access to phase)
	lll := func(fmtS string, rest ...interface{}) {
		dlqMutex.Lock()
		ll(fmtS, rest...)
		dlqMutex.Unlock()
	}

	// == Metrics ===

	getCurrentHealth := func() int {
		dlqMutex.RLock()
		r := currentHealth
		dlqMutex.RUnlock()
		return r
	}

	getDLQDeliveryCount := func() int {
		dlqMutex.RLock()
		r := dlqDeliveryCount
		dlqMutex.RUnlock()
		return r
	}

	getDLQDeliveryTime := func() time.Time {
		dlqMutex.RLock()
		r := dlqDeliveryTime
		dlqMutex.RUnlock()
		return r
	}

	defer metrics.RegisterHandler(`outputhost.healthstate.cg`, `destination`, metricsName, nil)
	metrics.RegisterHandler(`outputhost.healthstate.cg`, `destination`, metricsName,
		func(metricName string, baseTags, tags map[string]string, value int64) {
			dlqMutex.Lock()
			last := currentHealth
			currentHealth = int(value)
			if last != currentHealth {
				ll("Metric %s: %d", metricName, value)
			}
			dlqMutex.Unlock()
		})

	defer metrics.RegisterHandler(`outputhost.message.sent-dlq.cg`, `destination`, metricsName, nil)
	metrics.RegisterHandler(`outputhost.message.sent-dlq.cg`, `destination`, metricsName,
		func(metricName string, baseTags, tags map[string]string, value int64) {
			dlqMutex.Lock()
			dlqDeliveryCount += int(value)
			ll("Metric %s: %d (+%d)", metricName, dlqDeliveryCount, value)
			if value > 0 {
				dlqDeliveryTime = time.Now()
			}
			dlqMutex.Unlock()
		})

	// == Merge ==

	merge := func() {
		fe := s.GetFrontend()
		s.NotNil(fe)
		mergeReq := cherami.NewMergeDLQForConsumerGroupRequest()
		mergeReq.DestinationPath = common.StringPtr(destPath)
		mergeReq.ConsumerGroupName = common.StringPtr(cgPath)

		time.Sleep(DLQPublishClearTime)
		// Merge DLQ
		err := fe.MergeDLQForConsumerGroup(nil, mergeReq)
		s.NoError(err)
	}

	// == Setup ==

	// Create the client
	ipaddr, port, _ := net.SplitHostPort(s.GetFrontend().GetTChannel().PeerInfo().HostPort)
	portNum, _ := strconv.Atoi(port)
	cheramiClient := createCheramiClient("cherami-test-smartretry-dlq", ipaddr, portNum, nil)
	defer cheramiClient.Close()

	// Create the destination to publish message
	crReq := cherami.NewCreateDestinationRequest()
	crReq.Path = common.StringPtr(destPath)
	crReq.Type = cherami.DestinationTypePtr(cherami.DestinationType_PLAIN)
	crReq.ConsumedMessagesRetention = common.Int32Ptr(60)
	crReq.UnconsumedMessagesRetention = common.Int32Ptr(120)
	crReq.OwnerEmail = common.StringPtr("lhcIntegration@uber.com")

	desDesc, _ := cheramiClient.CreateDestination(crReq)
	s.NotNil(desDesc)

	// we should see the destination now in cassandra
	rReq := cherami.NewReadDestinationRequest()
	rReq.Path = common.StringPtr(destPath)
	readDesc, _ := cheramiClient.ReadDestination(rReq)
	s.NotNil(readDesc)

	// ==WRITE==

	// Create the publisher
	cPublisherReq := &client.CreatePublisherRequest{
		Path: destPath,
	}

	publisherTest := cheramiClient.CreatePublisher(cPublisherReq)
	s.NotNil(publisherTest)

	err := publisherTest.Open()
	s.NoError(err)

	// Publish messages continuously in a goroutine to ensures a steady supply of 'good' messages so
	// that smart retry will not affect us.
	var curPubInterval = int64(publisherPubInterval)
	closeCh := make(chan struct{})
	defer close(closeCh)
	go func() {
		i := 0
		defer lll("DONE PUBLISHING %v MESSAGES", i)
		defer publisherTest.Close()
		myIntrvl := atomic.LoadInt64(&curPubInterval)
		ticker := time.NewTicker(time.Duration(myIntrvl))
		for {
			select {
			case <-ticker.C:
				data := []byte(fmt.Sprintf("msg_%d", i+1))
				receipt := publisherTest.Publish(&client.PublisherMessage{Data: data})
				s.NoError(receipt.Error)
				i++

				if atomic.LoadInt64(&curPubInterval) != myIntrvl { // Adjust publish speed as requested
					ticker.Stop()
					myIntrvl = atomic.LoadInt64(&curPubInterval)
					ticker = time.NewTicker(time.Duration(myIntrvl))
					lll("publisher changed interval to %v seconds", common.UnixNanoTime(myIntrvl).ToSeconds())
				}
			case <-closeCh:
				return
			}
		}
	}()

	// ==READ==

	// Create the consumer group
	cgReq := cherami.NewCreateConsumerGroupRequest()
	cgReq.ConsumerGroupName = common.StringPtr(cgPath)
	cgReq.DestinationPath = common.StringPtr(destPath)
	cgReq.LockTimeoutInSeconds = common.Int32Ptr(cgLockTimeout) // this is the message redelivery timeout
	cgReq.MaxDeliveryCount = common.Int32Ptr(cgMaxDeliveryCount)
	cgReq.OwnerEmail = common.StringPtr("consumer_integration_test@uber.com")

	cgDesc, err := cheramiClient.CreateConsumerGroup(cgReq)
	s.NoError(err)
	s.NotNil(cgDesc)

	cConsumerReq := &client.CreateConsumerRequest{
		Path:              destPath,
		ConsumerGroupName: cgPath,
		ConsumerName:      "consumerName",
		PrefetchCount:     cnsmrPrefetch,
		Options:           &client.ClientOptions{Timeout: time.Second * 30}, // this is the thrift context timeout
	}

	consumerTest := cheramiClient.CreateConsumer(cConsumerReq)
	s.NotNil(consumerTest)

	// Open the consumer channel
	delivery := make(chan client.Delivery, 1)
	delivery, err = consumerTest.Open(delivery)
	s.NoError(err)
	defer consumerTest.Close()

	beforeMergeDLQDeliveryCount := -1

	// Read the messages in a loop.
readLoop:
	for msgCount := 0; ; {
		select {
		case msg := <-delivery:
			msgCount++
			msgID, _ := strconv.Atoi(string(msg.GetMessage().GetPayload().GetData()[4:]))
			var ack, poison, merged bool

			msgDecorator := `  `
			if msgID > DLQMessageStart && msgID%DLQMessageSpacing == 0 {
				msgDecorator = `* ` // DLQ Message
				poison = true
				if msgID < DLQMessageStart+beforeMergeDLQDeliveryCount*DLQMessageSpacing {
					merged = true
					msgDecorator = `*M` // DLQ Message, and was likely merged
				}
			}

			lll("msgId %3d %s dlqDvlry=%3d (merged %2d, last %-12s), health = %d",
				msgID,
				msgDecorator,
				getDLQDeliveryCount(),
				beforeMergeDLQDeliveryCount,
				common.UnixNanoTime(time.Since(getDLQDeliveryTime())).ToSecondsFmt(),
				getCurrentHealth())

			switch phase {
			case produceDLQ: // Normal consumption with some selected 'poison' message. This is dilute poison going to DLQ
				if !poison {
					ack = true
				}
				if getDLQDeliveryCount() >= DLQMergeMessageTargetCount { // Produced enough DLQ, move on
					phase++
				}
			case smartRetryProvoke1: // Provoke smart retry by NACKing everything
				if getCurrentHealth() == stateStalled {
					phase++
				}
			case mergeOp: // Now in smart retry, perform the merge, wait for it to come into effect; transition to healthy state by acking
				beforeMergeDLQDeliveryCount = getDLQDeliveryCount()
				phaseOnce[phase].Do(func() { go merge() }) // Perform the merge once, asychronously

				if merged {
					s.True(poison) // This is just an assertion/sanity check
					if getCurrentHealth() == stateProgressing {
						phase++
					}
				} else {
					// ACK all messages until we see something get merged; this halts DLQ production
					ack = true
				}
			case smartRetryProvoke3:
				phaseOnce[phase].Do(func() { lll("smartRetryProvoke3") })

				// We will transition from healthy to stalled by NACKing in this phase
				s.False(getCurrentHealth() == stateIdle)
				if getCurrentHealth() == stateStalled {
					phase++
				}
			case smartRetryProvoke4:
				phaseOnce[phase].Do(func() {
					lll("smartRetryProvoke4")
					atomic.StoreInt64(&curPubInterval, int64(publisherPubSlowInterval)) // Slow down the publisher so that we can finish faster
				})

				// Since we are merging, the indicated state is stalled, but smart retry is disabled until the merge completes
				s.Equal(getCurrentHealth(), stateStalled, `While NACKING, indicated state should be stalled`)

				if getDLQDeliveryCount() > beforeMergeDLQDeliveryCount*2 && // Verify that we've published enough to DLQ since the merge
					time.Since(getDLQDeliveryTime()) > mergeAssumedCompleteTime { // Verify that we are done DLQ publishing (i.e. transitioned to normal smart retry behavior)
					phase++
				}
			case done:
				phaseOnce[phase].Do(func() { lll("done") })
				ack = true
				if getCurrentHealth() == stateProgressing { // Verify that we can get back to fully normal health
					break readLoop
				}
			}

			if ack {
				msg.Ack()
			} else {
				msg.Nack()
			}
		}
	}
}

func (s *NetIntegrationSuiteParallelF) _TestSmartRetry() {
	destPath := "/test.runner.SmartRetry/TestSmartRetry"
	cgPath := "/test.runner.SmartRetry/TestSmartRetryCG"
	testMsgCount := 100
	var ackedMsgID int

	log := common.GetDefaultLogger()
	// Create the client
	ipaddr, port, _ := net.SplitHostPort(s.GetFrontend().GetTChannel().PeerInfo().HostPort)
	portNum, _ := strconv.Atoi(port)
	cheramiClient := createCheramiClient("cherami-test-smartretry", ipaddr, portNum, nil)
	defer cheramiClient.Close()

	// Create the destination to publish message
	crReq := cherami.NewCreateDestinationRequest()
	crReq.Path = common.StringPtr(destPath)
	crReq.Type = cherami.DestinationTypePtr(cherami.DestinationType_PLAIN)
	crReq.ConsumedMessagesRetention = common.Int32Ptr(60)
	crReq.UnconsumedMessagesRetention = common.Int32Ptr(120)
	crReq.OwnerEmail = common.StringPtr("lhcIntegration@uber.com")

	desDesc, _ := cheramiClient.CreateDestination(crReq)
	s.NotNil(desDesc)

	// we should see the destination now in cassandra
	rReq := cherami.NewReadDestinationRequest()
	rReq.Path = common.StringPtr(destPath)
	readDesc, _ := cheramiClient.ReadDestination(rReq)
	s.NotNil(readDesc)

	// ==WRITE==

	// Create the publisher
	cPublisherReq := &client.CreatePublisherRequest{
		Path: destPath,
	}

	publisherTest := cheramiClient.CreatePublisher(cPublisherReq)
	s.NotNil(publisherTest)

	err := publisherTest.Open()
	s.NoError(err)

	// Publish messages
	for i := 0; i < testMsgCount; i++ {
		text := fmt.Sprintf("msg_%d", i+1)
		data := []byte(fmt.Sprintf("%s", text))

		// log.Infof("client: publishing data: %s", data)
		receipt := publisherTest.Publish(&client.PublisherMessage{Data: data})
		s.NoError(receipt.Error)
	}
	publisherTest.Close()

	log.Info("DONE PUBLISHING MESSAGES")

	// ==READ==

	// Create the consumer group
	cgReq := cherami.NewCreateConsumerGroupRequest()
	cgReq.ConsumerGroupName = common.StringPtr(cgPath)
	cgReq.DestinationPath = common.StringPtr(destPath)
	cgReq.LockTimeoutInSeconds = common.Int32Ptr(2) // this is the message redelivery timeout
	cgReq.MaxDeliveryCount = common.Int32Ptr(2)
	cgReq.OwnerEmail = common.StringPtr("consumer_integration_test@uber.com")

	cgDesc, err := cheramiClient.CreateConsumerGroup(cgReq)
	s.NoError(err)
	s.NotNil(cgDesc)
	s.Equal(cgReq.GetDestinationPath(), cgDesc.GetDestinationPath(), "Wrong destination path")
	s.Equal(cgReq.GetConsumerGroupName(), cgDesc.GetConsumerGroupName(), "Wrong consumer group name")
	s.Equal(cgReq.GetLockTimeoutInSeconds(), cgDesc.GetLockTimeoutInSeconds(), "Wrong LockTimeoutInSeconds")
	s.Equal(cgReq.GetMaxDeliveryCount(), cgDesc.GetMaxDeliveryCount(), "Wrong MaxDeliveryCount")
	s.Equal(cherami.ConsumerGroupStatus_ENABLED, cgDesc.GetStatus(), "Wrong Status")
	s.Equal(cgReq.GetOwnerEmail(), cgDesc.GetOwnerEmail(), "Wrong Owner Email")

	cConsumerReq := &client.CreateConsumerRequest{
		Path:              destPath,
		ConsumerGroupName: cgPath,
		ConsumerName:      "TestSmartRetryConsumerName",
		PrefetchCount:     10,
		Options:           &client.ClientOptions{Timeout: time.Second * 30}, // this is the thrift context timeout
	}

	consumerTest := cheramiClient.CreateConsumer(cConsumerReq)
	s.NotNil(consumerTest)

	// Open the consumer channel
	delivery := make(chan client.Delivery, 1)
	delivery, err = consumerTest.Open(delivery)
	s.NoError(err)

	dlqDeliveryMap := make(map[int]bool) // Key is the message ID from GetID(), true = was not acked or was nacked, false = normal

	// Read the messages in a loop. We will exit the loop via a timeout
	timeout := time.Second * time.Duration(cgDesc.GetLockTimeoutInSeconds())
	timeoutTimer := time.NewTimer(10 * timeout)
	expectedMessageCount := testMsgCount * 5
	log.Info(`Verifying that not-acked/NACKed messages spin endlessly...`)
ReadLoop:
	for msgCount := 0; msgCount < expectedMessageCount; {
		select {
		case msg := <-delivery:
			msgCount++
			timeoutTimer.Reset(timeout)
			msgID, _ := strconv.Atoi(string(msg.GetMessage().GetPayload().GetData()[4:]))

			if msgCount == expectedMessageCount {
				log.Warnf("Acking one message to allow the others to go to DLQ...: (msg ID:%v) (AckID:%q)", msgID, msg.GetMessage().GetAckId())
				ackedMsgID = msgID
				dlqDeliveryMap[msgID] = false
				msg.Ack()
			}

			if rand.Intn(1) == 0 {
				// _, ok := dlqDeliveryMap[msgID]
				// log.Infof("Not acking message (first pass: %v): (msg ID:%v) (AckID:%q)", !ok, msgID, msg.GetMessage().GetAckId())
				dlqDeliveryMap[msgID] = true
				continue
			} else {
				// s.Assert().False(ok, "Message was NACKed, so it should not have been redelivered")
				// log.Infof("NACKing message: (msg ID:%v) (AckID:%q)", msgID, msg.GetMessage().GetAckId())
				dlqDeliveryMap[msgID] = true
				msg.Nack()
			}

		case <-timeoutTimer.C:
			log.Errorf("consumer delivery channel timed out after %v messages", msgCount)
			s.Assert().Equal(expectedMessageCount, msgCount) // FAIL the test
			break ReadLoop
		}
	}

	timeoutTimer = time.NewTimer(10 * timeout)
	expectedMessageCount = testMsgCount * 100
	log.Info(`Verifying that a single acked message unblocks everything...`)
ReadLoopB:
	for msgCount := 0; msgCount < expectedMessageCount; {
		select {
		case msg := <-delivery:
			msgCount++
			timeoutTimer.Reset(timeout)
			msgID, _ := strconv.Atoi(string(msg.GetMessage().GetPayload().GetData()[4:]))

			if msgID == ackedMsgID {
				log.Warnf("ACKing the one good message: (msg ID:%v) (AckID:%q)", msgID, msg.GetMessage().GetAckId())
				dlqDeliveryMap[msgID] = false
				msg.Ack()
			}

			if rand.Intn(1) == 0 {
				//_, ok := dlqDeliveryMap[msgID]
				//log.Infof("Not acking message (first pass: %v): (msg ID:%v) (AckID:%q)", !ok, msgID, msg.GetMessage().GetAckId())
				dlqDeliveryMap[msgID] = true
				continue
			} else {
				// s.Assert().False(ok, "Message was NACKed, so it should not have been redelivered")
				//log.Infof("NACKing message: (msg ID:%v) (AckID:%q)", msgID, msg.GetMessage().GetAckId())
				dlqDeliveryMap[msgID] = true
				msg.Nack()
			}
		case <-timeoutTimer.C:
			log.Errorf("consumer delivery channel timed out after %v messages", msgCount)
			s.True(msgCount < expectedMessageCount)
			break ReadLoopB
		}
	}

	time.Sleep(time.Second * time.Duration(cgDesc.GetLockTimeoutInSeconds())) // Need to sleep for the full lock timeout, or there's a race against the DLQ consumption below
	consumerTest.Close()

	log.Infof(`DLQ Consumption Test starting...`)

	// Create a consumer group to read the DLQ

	// Create the consumer group
	dlqDest := cgDesc.GetDeadLetterQueueDestinationUUID()
	cgDLQReq := cherami.NewCreateConsumerGroupRequest()
	cgDLQReq.ConsumerGroupName = common.StringPtr(cgPath)
	cgDLQReq.DestinationPath = common.StringPtr(dlqDest)
	cgDLQReq.LockTimeoutInSeconds = common.Int32Ptr(30) // this is the message redelivery timeout
	cgDLQReq.MaxDeliveryCount = common.Int32Ptr(2)
	cgDLQReq.OwnerEmail = common.StringPtr("consumer_integration_test@uber.com")

	cgDLQDesc, err := cheramiClient.CreateConsumerGroup(cgDLQReq)
	s.NoError(err)
	s.NotNil(cgDLQDesc)
	s.NotEqual(cgDLQReq.GetDestinationPath(), cgDLQDesc.GetDestinationPath(), "Wrong destination path")
	s.Equal(cgDLQReq.GetConsumerGroupName(), cgDLQDesc.GetConsumerGroupName(), "Wrong consumer group name")
	s.Equal(cgDLQReq.GetLockTimeoutInSeconds(), cgDLQDesc.GetLockTimeoutInSeconds(), "Wrong LockTimeoutInSeconds")
	s.Equal(cgDLQReq.GetMaxDeliveryCount(), cgDLQDesc.GetMaxDeliveryCount(), "Wrong MaxDeliveryCount")
	s.Equal(cherami.ConsumerGroupStatus_ENABLED, cgDLQDesc.GetStatus(), "Wrong Status")
	s.Equal(cgDLQReq.GetOwnerEmail(), cgDLQDesc.GetOwnerEmail(), "Wrong Owner Email")

	cDLQConsumerReq := &client.CreateConsumerRequest{
		Path:              dlqDest,
		ConsumerGroupName: cgPath,
		ConsumerName:      "TestConsumerName-DLQ",
		PrefetchCount:     10,
		Options:           &client.ClientOptions{Timeout: time.Second * 30}, // this is the thrift context timeout
	}

	dlqConsumerTest := cheramiClient.CreateConsumer(cDLQConsumerReq)
	s.NotNil(dlqConsumerTest)

	// Open the consumer channel
	delivery, err = dlqConsumerTest.Open(delivery)
	s.NoError(err)

	// Read the messages in a loop. We will exit the loop via a timeout
	timeoutTimer.Reset(timeout * 100)
ReadLoop2_TheReloopening:
	for msgCount := 0; msgCount < testMsgCount; {
		select {
		case msg := <-delivery:
			msgCount++
			timeoutTimer.Reset(timeout * 10)

			// IDs have been reassigned when these messages were sent to DLQ, so we need to reconstruct from the payload
			msgID, _ := strconv.Atoi(string(msg.GetMessage().GetPayload().GetData()[4:]))
			log.Infof("DLQ, got message original ID %d, new ID %d, payload %#q", msgID, msgCount+1, msg.GetMessage().GetPayload().GetData())
			isNew, expected := dlqDeliveryMap[msgID]

			if !isNew && expected {
				// Disable pending investigation. This hits often in Jenkins
				log.Errorf("DLQ, got DUPLICATE message original ID %d, new ID %d, payload %#q", msgID, msgCount+1, msg.GetMessage().GetPayload().GetData())
				// s.Assert().False(!isNew && expected, "We shouldn't get duplicate deliveries on the DLQ, though we will get unexpected ones")
			}

			if !expected {
				dlqDeliveryMap[msgID] = false // not new; check against duplications of the unexpected messages
				log.Infof("Received EXTRA (unnecessary, unexpected) DLQ message #%v, payload %#q", msgCount+1, msg.GetMessage().GetPayload().GetData())
			}

			dlqDeliveryMap[msgID] = false // Not "isNew" anymore

			log.Infof("DLQ: ACKing message: #%v (msg ID:%v)", msgCount+1, msgID)
			msg.Ack()

		case <-timeoutTimer.C:
			log.Infof("DLQ: consumer delivery channel timed out after %v messages", msgCount)
			log.Infof("Expected: between %d messages in DLQ. Actual: %d", testMsgCount-1, msgCount)
			s.Assert().Equal(msgCount, testMsgCount-1, "We should get all of the original messages, less one")

			break ReadLoop2_TheReloopening
		}
	}

	for i, v := range dlqDeliveryMap {
		log.Debugf("dlqDeliveryMap[%v] = %v (false = delivered to DLQ CG, true = not delivered to DLQ CG but should have been)", i, v)
		// XXX: Enable this after fixing the test
		//s.Assert().False(v, "All known bad messages should have been delivered by the DLQ group")
	}

	dlqConsumerTest.Close()

}

func (s *NetIntegrationSuiteParallelE) TestStartFromWithCassandra() {
	destPath := "/dest/TestStartFromWithCassandra"
	cgPathEverything := "/cg/TestStartFromWithCassandraEverything"
	cgPathStartFrom := "/cg/TestStartFromWithCassandra"
	testMsgCount := 100
	log := common.GetDefaultLogger()

	// Create the client
	ipaddr, port, _ := net.SplitHostPort(s.GetFrontend().GetTChannel().PeerInfo().HostPort)
	portNum, _ := strconv.Atoi(port)
	cheramiClient, _ := client.NewClient("cherami-test", ipaddr, portNum, nil)

	// Create the destination to publish message
	crReq := cherami.NewCreateDestinationRequest()
	crReq.Path = common.StringPtr(destPath)
	crReq.Type = cherami.DestinationTypePtr(0)
	crReq.ConsumedMessagesRetention = common.Int32Ptr(60)
	crReq.UnconsumedMessagesRetention = common.Int32Ptr(120)
	crReq.OwnerEmail = common.StringPtr("lhcIntegration@uber.com")

	desDesc, _ := cheramiClient.CreateDestination(crReq)
	s.NotNil(desDesc)

	// we should see the destination now in cassandra
	rReq := cherami.NewReadDestinationRequest()
	rReq.Path = common.StringPtr(destPath)
	readDesc, _ := cheramiClient.ReadDestination(rReq)
	s.NotNil(readDesc)

	// ==WRITE==

	log.Info("Write test beginning...")

	// Create the publisher
	cPublisherReq := &client.CreatePublisherRequest{
		Path: destPath,
	}

	publisherTest := cheramiClient.CreatePublisher(cPublisherReq)
	s.NotNil(publisherTest)

	err := publisherTest.Open()
	s.NoError(err)

	// Publish messages
	doneCh := make(chan *client.PublisherReceipt, testMsgCount)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 0; i < testMsgCount; i++ {
			receipt := <-doneCh
			log.Infof("client: acking id %s", receipt.Receipt)
			s.NoError(receipt.Error)
		}
		wg.Done()
	}()

	// first publish messages
	for i := 0; i < testMsgCount; i++ {
		data := []byte(fmt.Sprintf("msg_%d", i))
		id, er := publisherTest.PublishAsync(
			&client.PublisherMessage{
				Data: data,
			},
			doneCh,
		)
		log.Infof("client: publishing data: %s with id %s", data, id)
		s.NoError(er)

		// After publishing half of the total number of messages,
		// sleep for a second so that we can start from that point
		// Note: id is already one greater than i and so we sleep
		// at i = half-2
		if i == (testMsgCount/2 - 2) {
			log.Infof("Sleeping in publish for msg with id: %v", id)
			time.Sleep(1 * time.Second)
		}
	}

	wg.Wait()
	publisherTest.Close()

	// ==READ==

	// First create a consumer group which reads from the start
	// Create the consumer group
	cgReq := cherami.NewCreateConsumerGroupRequest()
	cgReq.ConsumerGroupName = common.StringPtr(cgPathEverything)
	cgReq.DestinationPath = common.StringPtr(destPath)
	cgReq.LockTimeoutInSeconds = common.Int32Ptr(30) // this is the message redilvery timeout
	cgReq.MaxDeliveryCount = common.Int32Ptr(1)
	cgReq.OwnerEmail = common.StringPtr("consumer_integration_test@uber.com")

	cgDesc, err := cheramiClient.CreateConsumerGroup(cgReq)
	s.NoError(err)
	s.NotNil(cgDesc)
	s.Equal(cgReq.GetDestinationPath(), cgDesc.GetDestinationPath(), "Wrong destination path")
	s.Equal(cgReq.GetConsumerGroupName(), cgDesc.GetConsumerGroupName(), "Wrong consumer group name")
	s.Equal(cgReq.GetLockTimeoutInSeconds(), cgDesc.GetLockTimeoutInSeconds(), "Wrong LockTimeoutInSeconds")
	s.Equal(cgReq.GetMaxDeliveryCount(), cgDesc.GetMaxDeliveryCount(), "Wrong MaxDeliveryCount")
	s.Equal(cherami.ConsumerGroupStatus_ENABLED, cgDesc.GetStatus(), "Wrong Status")
	s.Equal(cgReq.GetOwnerEmail(), cgDesc.GetOwnerEmail(), "Wrong OwnerEmail")

	cConsumerReq := &client.CreateConsumerRequest{
		Path:              destPath,
		ConsumerGroupName: cgPathEverything,
		ConsumerName:      "TestStartFromWithCassandraConsumerName",
		PrefetchCount:     1,
		Options:           &client.ClientOptions{Timeout: time.Second * 30}, // this is the thrift context timeout
	}

	consumerTest := cheramiClient.CreateConsumer(cConsumerReq)
	s.NotNil(consumerTest)

	// Open the consumer channel
	delivery := make(chan client.Delivery, 1)
	delivery, err = consumerTest.Open(delivery)
	s.NoError(err)

	tStartFrom := time.Now().UnixNano()
	startFromID := 0

	// Read the messages in a loop. We will exit the loop via a timeout
ReadLoop:
	for msgCount := 0; msgCount < testMsgCount; msgCount++ {
		timeout := time.NewTimer(time.Second * 45)
		log.Infof("waiting to get a message on del chan")
		select {
		case msg := <-delivery:
			log.Infof("Read message: #%v (msg ID:%v)", msgCount+1, msg.GetMessage().Payload.GetID())
			msg.Ack()
			// record the timestamp to start from for the next consumer
			if msgCount == (testMsgCount/2)-1 {
				tStartFrom = msg.GetMessage().GetEnqueueTimeUtc()
				startFromID, _ = strconv.Atoi(msg.GetMessage().Payload.GetID())
			}
		case <-timeout.C:
			log.Errorf("consumer delivery channel timed out after %v messages", msgCount)
			s.Equal(msgCount, testMsgCount) // FAIL the test
			break ReadLoop
		}
	}

	// Check that trying to read one more message would block
	log.Info("Checking for additional messages...")
	select {
	case msg := <-delivery:
		log.Infof("Read EXTRA message: %v", msg.GetMessage().Payload.GetID())
		msg.Ack()
		s.Nil(msg)
	default:
		log.Errorf("Good: No message available to consume.")
	}

	consumerTest.Close()

	// Now create another consumer group which starts from msgCount/2
	// ==READ==

	// Create the consumer group
	cgReq2 := cherami.NewCreateConsumerGroupRequest()
	cgReq2.ConsumerGroupName = common.StringPtr(cgPathStartFrom)
	cgReq2.DestinationPath = common.StringPtr(destPath)
	cgReq2.StartFrom = common.Int64Ptr(tStartFrom)
	cgReq2.LockTimeoutInSeconds = common.Int32Ptr(30) // this is the message redilvery timeout
	cgReq2.MaxDeliveryCount = common.Int32Ptr(1)
	cgReq2.OwnerEmail = common.StringPtr("consumer_integration_test@uber.com")

	cgDesc, err = cheramiClient.CreateConsumerGroup(cgReq2)
	s.NoError(err)
	s.NotNil(cgDesc)
	s.Equal(cgReq2.GetDestinationPath(), cgDesc.GetDestinationPath(), "Wrong destination path")
	s.Equal(cgReq2.GetConsumerGroupName(), cgDesc.GetConsumerGroupName(), "Wrong consumer group name")
	s.Equal(cgReq2.GetLockTimeoutInSeconds(), cgDesc.GetLockTimeoutInSeconds(), "Wrong LockTimeoutInSeconds")
	s.Equal(cgReq2.GetMaxDeliveryCount(), cgDesc.GetMaxDeliveryCount(), "Wrong MaxDeliveryCount")
	s.Equal(cherami.ConsumerGroupStatus_ENABLED, cgDesc.GetStatus(), "Wrong Status")
	s.Equal(cgReq2.GetOwnerEmail(), cgDesc.GetOwnerEmail(), "Wrong OwnerEmail")
	s.Equal(cgReq2.GetStartFrom(), cgDesc.GetStartFrom(), "Wrong StartFrom time")

	cConsumerReq2 := &client.CreateConsumerRequest{
		Path:              destPath,
		ConsumerGroupName: cgPathStartFrom,
		ConsumerName:      "TestConsumerNameStartFrom",
		PrefetchCount:     1,
		Options:           &client.ClientOptions{Timeout: time.Second * 30}, // this is the thrift context timeout
	}

	consumerTest2 := cheramiClient.CreateConsumer(cConsumerReq2)
	s.NotNil(consumerTest2)

	// Open the consumer channel
	delivery = make(chan client.Delivery, 1)
	delivery, err = consumerTest2.Open(delivery)
	s.NoError(err)

	// Read the messages in a loop. We will exit the loop via a timeout
ReadLoop2:
	for msgCountStartFrom := 0; msgCountStartFrom < testMsgCount/2; msgCountStartFrom++ {
		timeout := time.NewTimer(time.Second * 45)
		log.Infof("waiting to get a message on del chan")
		select {
		case msg := <-delivery:
			log.Infof("Read message: #%v (msg ID:%v)", msgCountStartFrom+1, msg.GetMessage().Payload.GetID())
			msg.Ack()
			// make sure we start from the correct ID
			actualID, _ := strconv.Atoi(msg.GetMessage().Payload.GetID())
			s.Equal(startFromID+msgCountStartFrom, actualID)
		case <-timeout.C:
			log.Errorf("consumer delivery channel timed out after %v messages", msgCountStartFrom)
			s.Equal(msgCountStartFrom, testMsgCount/2) // FAIL the test
			break ReadLoop2
		}
	}

	consumerTest2.Close()

	err = cheramiClient.DeleteDestination(&cherami.DeleteDestinationRequest{Path: common.StringPtr(destPath)})
	s.Nil(err, "Failed to delete destination")
}

func (s *NetIntegrationSuiteParallelB) _TestQueueDepth() { // Disable pending fix for flakiness
	const (
		destPath                = `/test.runner.SmartRetry/TestQueueDepth` // This path ensures that throttling is limited for this test
		cgPath                  = `/test.runner.SmartRetry/TestQueueDepthCG`
		cgMaxDeliveryCount      = 2
		cgLockTimeout           = 5000 // No redeliveries
		cgReadLoopTimeout       = time.Minute / 2
		cgExpectedDeliveryCount = cgMaxDeliveryCount + 1
		cgVerifyLoopTimeout     = time.Minute * 2
		cgVerifyLoopTicker      = cgLockTimeout * time.Second
		cnsmrPrefetch           = 13
		publisherPubInterval    = time.Second / 5
		DLQPublishClearTime     = cgLockTimeout * time.Second * 2

		futureTSOffset = 45 * time.Second
		phaseCount     = 200
	)

	// How long to wait to be sure that the storehost reporter has committed its pending writes.
	// Note that because store tries to spread its writes over the entire interval, we need to wait
	// for a full interval to guarantee that the reporter is captured
	ReporterPauseEffectiveTime := controllerhost.IntervalBtwnScans

	var phase int
	var msgCount int
	var reporterPaused bool

	// Consumer group definitions
	const (
		startFrom = iota
		dangling
		dlq
	)

	cgNames := []string{
		`StartFrom`,
		`Dangling`,
		`DLQ`,
	}

	cgStartFrom := []int64{
		int64(common.Now()) + int64(futureTSOffset),
		0,
		0,
	}

	cgDescs := make([]*cherami.ConsumerGroupDescription, len(cgNames))
	deletedCGDescs := make([]*cherami.ConsumerGroupDescription, len(cgNames))
	deliveryChans := make([]chan client.Delivery, len(cgNames))
	consumers := make([]client.Consumer, len(cgNames))
	testStart := int64(common.Now())

	ll := func() bark.Logger {
		return common.GetDefaultLogger().WithFields(bark.Fields{`phase`: phase, `t`: `qDepth`})
	}

	// Enable the tabulation feature for verbose logging only
	ownerEmail := `gbailey@uber.com`
	if testing.Verbose() {
		ownerEmail = `gbailey+queueDepthTabulation@uber.com`
	}

	// Create the client
	ipaddr, port, _ := net.SplitHostPort(s.GetFrontend().GetTChannel().PeerInfo().HostPort)
	portNum, _ := strconv.Atoi(port)
	cheramiClient := createCheramiClient("TestQueueDepth", ipaddr, portNum, ll())
	defer cheramiClient.Close()

	// Create the destination to publish message
	crReq := cherami.NewCreateDestinationRequest()
	crReq.Path = common.StringPtr(destPath)
	crReq.Type = cherami.DestinationTypePtr(cherami.DestinationType_PLAIN)
	crReq.ConsumedMessagesRetention = common.Int32Ptr(600)
	crReq.UnconsumedMessagesRetention = common.Int32Ptr(1200)
	crReq.OwnerEmail = common.StringPtr(ownerEmail)

	desDesc, _ := cheramiClient.CreateDestination(crReq)
	s.NotNil(desDesc)

	// Create the consumer groups

	createCG := func(cg int) {
		cgReq := cherami.NewCreateConsumerGroupRequest()
		cgReq.ConsumerGroupName = common.StringPtr(cgPath + cgNames[cg])
		cgReq.DestinationPath = common.StringPtr(destPath)
		cgReq.LockTimeoutInSeconds = common.Int32Ptr(cgLockTimeout) // this is the message redelivery timeout
		cgReq.MaxDeliveryCount = common.Int32Ptr(cgMaxDeliveryCount)
		cgReq.OwnerEmail = common.StringPtr(ownerEmail)
		cgReq.StartFrom = common.Int64Ptr(cgStartFrom[cg])
		cgDesc, err := cheramiClient.CreateConsumerGroup(cgReq)
		s.NoError(err)
		s.NotNil(cgDesc)
		cgDescs[cg] = cgDesc
	}

	for i := range cgNames {
		createCG(i)
	}

	// Create the publisher
	cPublisherReq := &client.CreatePublisherRequest{
		Path: destPath,
	}

	publisherTest := cheramiClient.CreatePublisher(cPublisherReq)
	s.NotNil(publisherTest)

	errPTO := publisherTest.Open()
	s.NoError(errPTO)
	defer publisherTest.Close()

	pauseReporter := func() {
		if !reporterPaused {
			reporterPaused = true
			storehost.ExtStatsReporterPause()
			ll().Info(`Reporter paused`)
			time.Sleep(ReporterPauseEffectiveTime)
		}
	}

	unpauseReporter := func() {
		if reporterPaused {
			reporterPaused = false
			ll().Info(`Reporter unpaused`)
			storehost.ExtStatsReporterResume()
		}
	}
	defer unpauseReporter()

	produceN := func(n int) {
		unpauseReporter()
		for i := msgCount; i < msgCount+n; i++ {
			data := []byte(fmt.Sprintf("msg_%d", i+1))
			receipt := publisherTest.Publish(&client.PublisherMessage{Data: data})
			s.NoError(receipt.Error)
		}
		msgCount += n
	}

	dlqPublishers := make([]client.Publisher, len(cgNames))

	produceNdlq := func(cg int, n int) {
		if cg >= len(cgNames) {
			return
		}

		if dlqPublishers[cg] == nil {
			// Create the publisher
			cPublisherReq = &client.CreatePublisherRequest{
				Path: cgDescs[cg].GetDeadLetterQueueDestinationUUID(),
			}

			p := cheramiClient.CreatePublisher(cPublisherReq)
			s.NotNil(p)

			err := p.Open()
			s.NoError(err)
			dlqPublishers[cg] = p
		}

		p := dlqPublishers[cg]

		unpauseReporter()
		for n > 0 {
			data := []byte(fmt.Sprintf("DLQ_msg_%d_%d", phase, n))
			receipt := p.Publish(&client.PublisherMessage{Data: data})
			s.NoError(receipt.Error)
			n--
		}
	}

	consumeN := func(cg int, n int) {
		if cg >= len(cgNames) {
			return
		}
		if deliveryChans[cg] == nil {
			cConsumerReq := &client.CreateConsumerRequest{
				Path:              destPath,
				ConsumerGroupName: cgPath + cgNames[cg],
				ConsumerName:      "TestQueueDepthConsumerName" + cgNames[cg],
				PrefetchCount:     cnsmrPrefetch,
			}

			consumer := cheramiClient.CreateConsumer(cConsumerReq)
			s.NotNil(consumer)

			// Open the consumer channel
			d := make(chan client.Delivery, 0)
			d, errCO := consumer.Open(d)
			s.NoError(errCO)
			deliveryChans[cg] = d
			consumers[cg] = consumer
			//defer consumer.Close() // Test doesn't work properly if we close; ack level doesn't reach the right level. T475365
		}

		delivery := deliveryChans[cg]

		ll().Infof(`Consuming %v from %v`, n, cgNames[cg])
		for n > 0 {
			msg := <-delivery
			ll().Infof(`RECV: %v`, string(msg.GetMessage().GetPayload().GetData()))
			msg.Ack()
			n--
		}
	}

	checkBacklog := func(cg int, ba, dlq int64) {
		if cg >= len(cgNames) {
			return
		}
		for {
			qReq := &controller.GetQueueDepthInfoRequest{Key: common.StringPtr(cgDescs[cg].GetConsumerGroupUUID())}
			r, err := s.GetController().GetQueueDepthInfo(nil, qReq)
			if err != nil {
				if _, ok := err.(*controller.QueueCacheMissError); !ok {
					ll().WithField(`error`, err).Error("GetQueueDepthInfo error")
				}
				continue
			}

			var queueDepthInfo controllerhost.QueueDepthCacheJSONFields
			err = json.Unmarshal([]byte(r.GetValue()), &queueDepthInfo)
			if err != nil {
				ll().WithField(`error`, err).Error("json.Unmarshal(queueDepthJSONFields) error")
				continue
			}

			ll().WithFields(bark.Fields{
				`cg`:                   cgNames[cg],
				`BacklogAvailable`:     queueDepthInfo.BacklogAvailable,
				`WantBacklogAvailable`: ba,
				`BacklogInflight`:      queueDepthInfo.BacklogInflight,
				`BacklogDLQ`:           queueDepthInfo.BacklogDLQ, `WantBacklogDLQ`: dlq,
			}).Info(`waiting on backlog result...`)

			if queueDepthInfo.BacklogAvailable == ba && queueDepthInfo.BacklogDLQ == dlq {
				return
			}
			time.Sleep(controllerhost.IntervalBtwnScans)
		}
	}

	purgeDLQ := func(cg int) {
		if cg >= len(cgNames) {
			return
		}
		fe := s.GetFrontend()
		s.NotNil(fe)
		purgeReq := cherami.NewPurgeDLQForConsumerGroupRequest()
		purgeReq.DestinationPath = common.StringPtr(destPath)
		purgeReq.ConsumerGroupName = common.StringPtr(cgPath + cgNames[cg])
		err := fe.PurgeDLQForConsumerGroup(nil, purgeReq)
		s.NoError(err)
		ll().Infof(`Purged %v`, cgNames[cg])
	}

	mergeDLQ := func(cg int) {
		if cg >= len(cgNames) {
			return
		}
		fe := s.GetFrontend()
		s.NotNil(fe)
		mergeReq := cherami.NewMergeDLQForConsumerGroupRequest()
		mergeReq.DestinationPath = common.StringPtr(destPath)
		mergeReq.ConsumerGroupName = common.StringPtr(cgPath + cgNames[cg])
		err := fe.MergeDLQForConsumerGroup(nil, mergeReq)
		s.NoError(err)
		ll().Infof(`Merged %v`, cgNames[cg])
	}

	changeStartFrom := func(cg int, startFrom int64) {
		if cg >= len(cgNames) {
			return
		}

		// UpdateConsumerGroup doesn't support changing startFrom. We will delete and recreate
		fe := s.GetFrontend()
		s.NotNil(fe)

		dReq := cherami.NewDeleteConsumerGroupRequest()
		dReq.DestinationPath = common.StringPtr(destPath)
		dReq.ConsumerGroupName = common.StringPtr(cgPath + cgNames[cg])

		if deliveryChans[cg] != nil {
			deliveryChans[cg] = nil // Force the consumer to be re-opened
			consumers[cg].Close()
		}

		ctx, _ := thrift.NewContext(30 * time.Second)
		delErr := fe.DeleteConsumerGroup(ctx, dReq)
		s.NoError(delErr)

		deletedCGDescs = append(deletedCGDescs, cgDescs[cg])
		cgStartFrom[cg] = startFrom

		createCG(cg)
	}

	// NOTE: there is a race in this function with produceN. If the backlog for the production isn't checked, then the availableSequence may still be
	// zero here for all extents and no update will occur, failing the test
	retention := func(retentionAmount int64) {
		rq := shared.NewListExtentsStatsRequest()
		rq.DestinationUUID = common.StringPtr(desDesc.GetDestinationUUID())
		rq.Limit = common.Int64Ptr(100)
		rs, lesErr := s.mClient.ListExtentsStats(nil, rq)
		s.NoError(lesErr)
		s.NotNil(rs)

		reset := false
		if retentionAmount == 0 {
			reset = true
		}

		pauseReporter()

		var update bool
		for _, e := range rs.GetExtentStatsList() {
			ll().Infof(`e=%v`, e.GetExtent().GetExtentUUID())
			for _, rps := range e.GetReplicaStats() {
				rqse := metadata.NewReadStoreExtentReplicaStatsRequest()
				rqse.ExtentUUID = common.StringPtr(e.GetExtent().GetExtentUUID())
				rqse.StoreUUID = common.StringPtr(rps.GetStoreUUID())
				rps = nil // This copy of the replica stats has almost nothing set, and it shouldn't be used below
				srs, err := s.mClient.ReadStoreExtentReplicaStats(nil, rqse)
				s.NoError(err)
				s.NotNil(srs)

				as := srs.GetExtent().GetReplicaStats()[0].GetAvailableSequence()
				bs := srs.GetExtent().GetReplicaStats()[0].GetBeginSequence()
				ll().Infof(`srs=%v/%v`, as, bs)

				update = false
				stats := srs.GetExtent().GetReplicaStats()[0] // Modify the existing stats. We would otherwise write nils to most fields

				if reset { // clearing
					if bs != math.MaxInt64 {
						update = true
						// reset BeginSequence to original starting seq (of "1")
						stats.BeginSequence = common.Int64Ptr(1)
					}
				} else { // Setting retention
					// check if there are messages in the extent, and if there
					// are 'retention' as many as we can out of this extent
					if bs != math.MaxInt64 {
						update = true
						// move the begin-sequence as much as we can (within 'retentionAmount')
						stats.BeginSequence = common.Int64Ptr(bs + common.MinInt64(retentionAmount, as-bs))
						retentionAmount -= common.MaxInt64(0, common.MinInt64(retentionAmount, as-bs))
					}
				}

				if update {
					updateStatsRequest := &metadata.UpdateStoreExtentReplicaStatsRequest{
						ExtentUUID:   common.StringPtr(e.GetExtent().GetExtentUUID()),
						ReplicaStats: []*shared.ExtentReplicaStats{stats},
					}

					ll().Infof(`Retention S`)
					s.NoError(s.mClient.UpdateStoreExtentReplicaStats(nil, updateStatsRequest))
					ll().Infof(`Retention E`)
				}
			}

			if !reset {
				if retentionAmount == 0 { // touched enough extents to achieve target
					break
				}
			}
		}

		s.True(retentionAmount == 0)
	}

	var newStartFrom int64
	for ; phase < phaseCount; phase++ {
		ll().WithField(`phase`, phase).Error(`Starting...`)
		// Producer actions
		switch phase {
		case 0:
			// Verify that all backlogs are zero before producing anything

			checkBacklog(startFrom, 0, 0)
			checkBacklog(dlq, 0, 0)
			checkBacklog(dangling, 0, 0)
		case 10:
			produceN(20)
			checkBacklog(dangling, 20, 0)
			checkBacklog(dlq, 20, 0)
			checkBacklog(startFrom, 0, 0) // Startfrom has a time in the future, so zero backlog
		case 20:
			// Adjust retention and see that the backlog adjusts
			retention(4) // Need to produce at least 5 messages per extent * 4 extents (=20), or there may be no single extent with greater than 4 messages to apply retention to
			checkBacklog(dangling, 20-4, 0)
			checkBacklog(dlq, 20-4, 0)
			checkBacklog(startFrom, 0, 0)
		case 30:
			// Put retention back, check that everything bounced back
			retention(0)
			checkBacklog(dangling, 20, 0)
			checkBacklog(dlq, 20, 0)
			checkBacklog(startFrom, 0, 0)
		case 40:
			newStartFrom = int64(common.Now())
			changeStartFrom(startFrom, newStartFrom) // Switch from a startFrom in the future to startFrom = now; new CG
			checkBacklog(startFrom, 0, 0)            // Should be nothing, since we haven't produced any messages after 'now'
		case 50:
			produceN(13)
			checkBacklog(dangling, 20+13, 0)
			checkBacklog(dlq, 20+13, 0)
			checkBacklog(startFrom, 13, 0) // New production should show up
		case 60:
			// Just opening the consumer (switching from dangling to assigned extents), shouldn't affect the backlog
			// NOTE: since controller doesn't call GetAddressFromTimestamp, it always sets an ackLevel of zero, so we need to consume at least one message so
			// outputhost will set the ackLevel
			consumeN(dangling, 0)
			consumeN(startFrom, 0)
			produceNdlq(dlq, 0)
			produceNdlq(startFrom, 4)
			checkBacklog(dangling, 20+13, 0)
			checkBacklog(dlq, 20+13, 0)
			checkBacklog(startFrom, 13, 4)
		case 70:
			consumeN(dangling, 5)
			consumeN(dlq, 5)
			consumeN(startFrom, 5)
			produceNdlq(dlq, 11)
			checkBacklog(dangling, 20+13-5, 0)
			checkBacklog(dlq, 20+13-5, 11)
			checkBacklog(startFrom, 13-5, 4)
		case 80:
			purgeDLQ(dlq)
			consumeN(dangling, 20+13-5)
			consumeN(startFrom, 13-5)
			checkBacklog(dangling, 0, 0)
			checkBacklog(dlq, 20+13-5, 0)
			checkBacklog(startFrom, 0, 4)
		case 90:
			produceNdlq(dlq, 23)
			checkBacklog(dlq, 20+13-5, 23)
			checkBacklog(startFrom, 0, 4)
			checkBacklog(dangling, 0, 0)
		case 100:
			changeStartFrom(startFrom, testStart) // Reset the startFrom CG to a non-zero but old value, verify that DLQ went away, and all messages are available
			checkBacklog(dlq, 20+13-5, 23)
			checkBacklog(dangling, 0, 0)
			checkBacklog(startFrom, 20+13, 0)
		case 110:
			mergeDLQ(dlq)
			checkBacklog(dlq, (20+13-5)+23, 0)
			checkBacklog(dangling, 0, 0)
			checkBacklog(startFrom, 20+13, 0)
		case 120:
			// verify that switching from dangling to assigned doesn't affect the startfrom group
			//consumeN(startFrom, 1) // this depends on outputhost unloading/reloading extents (since cgUUID changed)
			//checkBacklog(startFrom, 20+13-1, 0)
			//checkBacklog(dlq, (20+13-5)+23, 0) // T471438, sometimes fails because store wrote a bad value here
			checkBacklog(dangling, 0, 0)
		}
		ll().Info(`Ending...`)
	}

	ll().Info(`END`)
}

func (s *NetIntegrationSuiteParallelC) doPublishAndReadTest(
	cheramiClient client.Client,
	destPath, cgPath string,
	testMsgCount int,
	verifyFunc func(msg *cherami.ConsumerMessage)) {

	log := common.GetDefaultLogger()
	// ==WRITE==

	log.Info("Write test beginning...")

	// Create the publisher
	cPublisherReq := &client.CreatePublisherRequest{
		Path: destPath,
	}

	publisherTest := cheramiClient.CreatePublisher(cPublisherReq)
	s.NotNil(publisherTest)

	err := publisherTest.Open()
	s.NoError(err)

	// Publish messages
	doneCh := make(chan *client.PublisherReceipt, testMsgCount)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 0; i < testMsgCount; i++ {
			receipt := <-doneCh
			log.Infof("client: acking id %s", receipt.Receipt)
			s.NoError(receipt.Error)
		}
		wg.Done()
	}()

	for i := 0; i < testMsgCount; i++ {
		data := []byte(fmt.Sprintf("msg_%d", i))
		id, er := publisherTest.PublishAsync(
			&client.PublisherMessage{
				Data: data,
			},
			doneCh,
		)
		log.Infof("client: publishing data: %s with id %s", data, id)
		s.NoError(er)
	}

	wg.Wait()
	publisherTest.Close()

	// ==READ==

	cConsumerReq := &client.CreateConsumerRequest{
		Path:              destPath,
		ConsumerGroupName: cgPath,
		ConsumerName:      "doPublishAndReadTestConsumerName",
		PrefetchCount:     1,
		Options:           &client.ClientOptions{Timeout: time.Second * 30}, // this is the thrift context timeout
	}

	consumerTest := cheramiClient.CreateConsumer(cConsumerReq)
	s.NotNil(consumerTest)

	// Open the consumer channel
	delivery := make(chan client.Delivery, 1)
	delivery, err = consumerTest.Open(delivery)
	s.NoError(err)

	// Read the messages in a loop. We will exit the loop via a timeout
ReadLoop:
	for msgCount := 0; msgCount < testMsgCount; msgCount++ {
		timeout := time.NewTimer(time.Second * 45)
		log.Infof("waiting to get a message on del chan")
		select {
		case msg := <-delivery:
			log.Infof("Read message: #%v (msg ID:%v)", msgCount+1, msg.GetMessage().Payload.GetID())
			//Wait until client side integrated before enable this test
			//verifyFunc(msg.GetMessage())
			//s.True(delivery.VerifyChecksum(), "verify checksum failed")
			msg.Ack()
		case <-timeout.C:
			log.Errorf("consumer delivery channel timed out after %v messages", msgCount)
			s.Equal(msgCount, testMsgCount) // FAIL the test
			break ReadLoop
		}
	}

	consumerTest.Close()
}

func (s *NetIntegrationSuiteParallelC) TestEndToEndChecksum() {
	destPath := "/dest/testChecksum"
	cgPath := "/cg/testChecksum"
	testMsgCount := 10

	// Create the client
	ipaddr, port, _ := net.SplitHostPort(s.GetFrontend().GetTChannel().PeerInfo().HostPort)
	portNum, _ := strconv.Atoi(port)
	cheramiClient := createCheramiClient("cherami-test-e2echecksum", ipaddr, portNum, nil)

	// Create the destination to publish message
	crReq := cherami.NewCreateDestinationRequest()
	crReq.Path = common.StringPtr(destPath)
	crReq.Type = cherami.DestinationTypePtr(0)
	crReq.ConsumedMessagesRetention = common.Int32Ptr(60)
	crReq.UnconsumedMessagesRetention = common.Int32Ptr(120)
	crReq.ChecksumOption = cherami.ChecksumOption_CRC32IEEE
	crReq.OwnerEmail = common.StringPtr("lhcIntegration@uber.com")

	desDesc, _ := cheramiClient.CreateDestination(crReq)
	s.NotNil(desDesc)

	// Create the consumer group
	cgReq := cherami.NewCreateConsumerGroupRequest()
	cgReq.ConsumerGroupName = common.StringPtr(cgPath)
	cgReq.DestinationPath = common.StringPtr(destPath)
	cgReq.LockTimeoutInSeconds = common.Int32Ptr(30) // this is the message redilvery timeout
	cgReq.MaxDeliveryCount = common.Int32Ptr(1)
	cgReq.OwnerEmail = common.StringPtr("consumer_integration_test@uber.com")

	cgDesc, err := cheramiClient.CreateConsumerGroup(cgReq)
	s.NoError(err)
	s.NotNil(cgDesc)
	s.Equal(cgReq.GetDestinationPath(), cgDesc.GetDestinationPath(), "Wrong destination path")
	s.Equal(cgReq.GetConsumerGroupName(), cgDesc.GetConsumerGroupName(), "Wrong consumer group name")
	s.Equal(cgReq.GetLockTimeoutInSeconds(), cgDesc.GetLockTimeoutInSeconds(), "Wrong LockTimeoutInSeconds")
	s.Equal(cgReq.GetMaxDeliveryCount(), cgDesc.GetMaxDeliveryCount(), "Wrong MaxDeliveryCount")
	s.Equal(cherami.ConsumerGroupStatus_ENABLED, cgDesc.GetStatus(), "Wrong Status")
	s.Equal(cgReq.GetOwnerEmail(), cgDesc.GetOwnerEmail(), "Wrong OwnerEmail")

	s.doPublishAndReadTest(
		cheramiClient, destPath, cgPath, testMsgCount,
		func(msg *cherami.ConsumerMessage) {
			s.True(msg.GetPayload().IsSetCrc32IEEEDataChecksum(), "crc32IEEE checksum field not set")
		})

	// Update the checksum option
	upReq := cherami.NewUpdateDestinationRequest()
	upReq.Path = common.StringPtr(destPath)
	newChecksumOption := cherami.ChecksumOption_MD5
	upReq.ChecksumOption = &newChecksumOption
	upReq.OwnerEmail = common.StringPtr("lhcIntegration@uber.com")

	desDesc, _ = cheramiClient.UpdateDestination(upReq)
	s.NotNil(desDesc)

	s.doPublishAndReadTest(
		cheramiClient, destPath, cgPath, testMsgCount,
		func(msg *cherami.ConsumerMessage) {
			s.True(msg.GetPayload().IsSetMd5DataChecksum(), "md5 checksum field not set")
		})

	err = cheramiClient.DeleteDestination(&cherami.DeleteDestinationRequest{Path: common.StringPtr(destPath)})
	s.Nil(err, "Failed to delete destination")
}
