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
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/apache/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"

	"testing"

	client "github.com/uber/cherami-client-go/client/cherami"
	"github.com/uber/cherami-server/common"
	localMetrics "github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/controllerhost"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

type NodeFailureTestSuite struct {
	testBase
	msgSeq            int32
	ackedIDs          map[string]bool
	receiptIDToMsgSeq map[string]int32
}

func TestNodeFailureTestSuite(t *testing.T) {
	log.Infof("Skipping NodeFailureTestSuite in short mode")
	return
}

func (s *NodeFailureTestSuite) SetupTest() {
	s.msgSeq = 0
	s.ackedIDs = make(map[string]bool)
	s.receiptIDToMsgSeq = make(map[string]int32)
}

func (s *NodeFailureTestSuite) TearDownTest() {
	s.testBase.TearDown()
}

func (s *NodeFailureTestSuite) createDestination(cClient client.Client, path string) (*cherami.DestinationDescription, error) {
	crReq := cherami.NewCreateDestinationRequest()
	crReq.Path = common.StringPtr(path)
	crReq.Type = cherami.DestinationTypePtr(0)
	crReq.ConsumedMessagesRetention = common.Int32Ptr(0)
	crReq.UnconsumedMessagesRetention = common.Int32Ptr(120)
	return cClient.CreateDestination(crReq)
}

func (s *NodeFailureTestSuite) createConsumerGroup(cClient client.Client, destPath string, cgPath string) (*cherami.ConsumerGroupDescription, error) {
	cgReq := cherami.NewCreateConsumerGroupRequest()
	cgReq.ConsumerGroupName = common.StringPtr(cgPath)
	cgReq.DestinationPath = common.StringPtr(destPath)
	cgReq.LockTimeoutInSeconds = common.Int32Ptr(30) // this is the message redelivery timeout
	cgReq.MaxDeliveryCount = common.Int32Ptr(1)
	return cClient.CreateConsumerGroup(cgReq)
}

func (s *NodeFailureTestSuite) publish(cClient client.Client, publisher client.Publisher, maxMessages int, doneCh chan *client.PublisherReceipt, wg *sync.WaitGroup) {
	defer wg.Done()

	for i := 0; i < maxMessages; i++ {
		s.msgSeq++
		data := []byte(fmt.Sprintf("%d", s.msgSeq))
		id, err := publisher.PublishAsync(
			&client.PublisherMessage{
				Data: data,
			},
			doneCh,
		)
		s.NoError(err)
		s.receiptIDToMsgSeq[id] = s.msgSeq
	}
}

func (s *NodeFailureTestSuite) recvReceipts(doneCh chan *client.PublisherReceipt, maxMessages int, idleTimeout time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()

	timeoutCh := time.After(idleTimeout)

	for i := 0; i < maxMessages; i++ {
		select {
		case receipt := <-doneCh:
			if receipt.Error == nil {
				s.ackedIDs[receipt.ID] = true
				log.Infof("Publish receipt success, id=%v, where=%v", receipt.ID, receipt.Receipt)
			} else {
				log.Infof("Publish receipt failure, id=%v, where=%v", receipt.ID, receipt.Receipt)
			}
		case <-timeoutCh:
			log.Infof("Publish receipt timed out")
			return
		}
	}
}

func (s *NodeFailureTestSuite) listOpenExtents(dstUUID string) ([]*shared.ExtentStats, error) {
	mm := controllerhost.NewMetadataMgr(s.mClient, &mockM3Metrics{}, common.GetDefaultLogger())
	return mm.ListExtentsByDstIDStatus(dstUUID, []shared.ExtentStatus{shared.ExtentStatus_OPEN})
}

func (s *NodeFailureTestSuite) TestInputFailure() {
	s.testNodeFailure(common.InputServiceName, 1)
}

func (s *NodeFailureTestSuite) TestOneStoreFailure() {
	s.testNodeFailure(common.StoreServiceName, 1)
}

func (s *NodeFailureTestSuite) TestTwoStoreFailure() {
	s.testNodeFailure(common.StoreServiceName, 2)
}

func (s *NodeFailureTestSuite) testNodeFailure(serviceName string, nKill int) {

	destPath := "/integration.test/nodefailure"
	cgPath := "/integration.test/nodefailure"
	testMsgCount := 2000
	numReplicas := 3

	clusterSzMap := map[string]int{
		common.StoreServiceName:  2 * numReplicas,
		common.InputServiceName:  3,
		common.OutputServiceName: 2,
	}

	s.testBase.SetUp(clusterSzMap, numReplicas)

	// Create the client
	ipaddr, port, _ := net.SplitHostPort(s.GetFrontend().GetTChannel().PeerInfo().HostPort)
	portNum, _ := strconv.Atoi(port)
	cheramiClient, _ := client.NewClient("cherami-test", ipaddr, portNum, nil)

	_, err := s.createDestination(cheramiClient, destPath)
	s.Nil(err)

	mReq := &shared.ReadDestinationRequest{Path: common.StringPtr(destPath)}
	dstDesc, err := s.mClient.ReadDestination(nil, mReq)
	s.Nil(err)

	cPublisherReq := &client.CreatePublisherRequest{
		Path: destPath,
	}

	publisher := cheramiClient.CreatePublisher(cPublisherReq)
	s.NotNil(publisher)

	err = publisher.Open()
	s.NoError(err)
	defer publisher.Close()

	// Publish messages
	doneCh := make(chan *client.PublisherReceipt, testMsgCount)
	var wg sync.WaitGroup

	wg.Add(2)
	go s.recvReceipts(doneCh, testMsgCount, 30*time.Second, &wg)
	go s.publish(cheramiClient, publisher, testMsgCount, doneCh, &wg)

	const warmupMsgCount = 99

	cond := func() bool {
		return len(s.ackedIDs) > warmupMsgCount
	}

	succ := common.SpinWaitOnCondition(cond, time.Minute)
	s.True(succ, "Failed to publish %v async messages within a minute", warmupMsgCount)

	stats, err := s.listOpenExtents(dstDesc.GetDestinationUUID())
	s.Nil(err)
	s.Equal(1, len(stats), "Wrong number of extents for destination")

	var killHosts = make([]string, nKill)

	if strings.Compare(serviceName, common.InputServiceName) == 0 {
		killHosts[0] = stats[0].GetExtent().GetInputHostUUID()
		_, ok := s.InputHosts[killHosts[0]]
		s.True(ok, "Failed to find input host mapped to extent")
		s.InputHosts[killHosts[0]].Stop()
		log.Infof("Killed input host, uuid=%v", killHosts[0])
	} else {
		for i := 0; i < nKill; i++ {
			killHosts[i] = stats[0].GetExtent().GetStoreUUIDs()[i]
			_, ok := s.StoreHosts[killHosts[i]]
			s.True(ok, "Failed to find store host mapped to extent")
			s.StoreHosts[killHosts[i]].Stop() // kill the store host while the msgs are published
			log.Infof("Killed store host, uuid=%v", killHosts[i])
		}
	}

	var ch *controllerhost.Mcp
	for _, ch = range s.Controllers {
		break
	}

	cond = func() bool {
		nInput, _ := clusterSzMap[common.InputServiceName]
		nStores, _ := clusterSzMap[common.StoreServiceName]
		if strings.Compare(serviceName, common.InputServiceName) == 0 {
			var hosts []*common.HostInfo
			hosts, err = ch.GetRingpopMonitor().GetHosts(common.InputServiceName)
			return err == nil && len(hosts) == nInput-1
		}
		var storeHosts []*common.HostInfo
		storeHosts, err = ch.GetRingpopMonitor().GetHosts(common.StoreServiceName)
		return err == nil && len(storeHosts) == nStores-nKill
	}

	success := common.SpinWaitOnCondition(cond, time.Minute)
	s.True(success, "Timed out waiting for Ringpop to detect host failure")

	succ = common.AwaitWaitGroup(&wg, time.Minute*2)
	s.True(succ, "Timed out waiting for publisher to publish all messages")

	wg.Add(2)
	go s.recvReceipts(doneCh, testMsgCount, time.Second*30, &wg)
	go s.publish(cheramiClient, publisher, testMsgCount, doneCh, &wg)
	succ = common.AwaitWaitGroup(&wg, time.Minute*2)
	s.True(succ, "Timed out waiting for publisher to publish all messages")

	log.Infof("Publish complete, starting consumer")

	_, err = s.createConsumerGroup(cheramiClient, destPath, cgPath)
	s.NoError(err)

	cConsumerReq := &client.CreateConsumerRequest{
		Path:              destPath,
		ConsumerGroupName: cgPath,
		ConsumerName:      "TestConsumer",
		PrefetchCount:     1,
		Options:           &client.ClientOptions{Timeout: time.Second * 30}, // this is the thrift context timeout
	}

	consumer := cheramiClient.CreateConsumer(cConsumerReq)
	s.NotNil(consumer)

	// Open the consumer channel
	delivery := make(chan client.Delivery, 100)
	delivery, err = consumer.Open(delivery)
	s.NoError(err)
	defer consumer.Close()

	rcvdMsgs := make(map[int32]bool)

	mm := controllerhost.NewMetadataMgr(s.mClient, &mockM3Metrics{}, common.GetDefaultLogger())

	msgCount := 0
	// Read the messages in a loop. We will exit the loop via a timeout
ReadLoop:
	for {
		timeout := time.NewTimer(time.Second * 30)
		select {
		case msg := <-delivery:
			var data int
			data, err = strconv.Atoi(string(msg.GetMessage().GetPayload().GetData()))
			s.Nil(err)
			rcvdMsgs[int32(data)] = true
			log.Infof("Read message, msg:%v", data)
			msg.Ack()
			msgCount++
		case <-timeout.C:
			log.Infof("Consumer delivery channel timed out after %v messages", msgCount)
			break ReadLoop
		}
	}

	for id := range s.ackedIDs {
		msg := s.receiptIDToMsgSeq[id]
		_, ok := rcvdMsgs[msg]
		s.True(ok, "Failed to receive acked message %v", msg)
	}

	if serviceName == common.InputServiceName {
		stats, err = mm.ListExtentsByInputIDStatus(killHosts[0], common.MetadataExtentStatusPtr(shared.ExtentStatus_SEALED))
		s.Nil(err)
		s.True(len(stats) > 0, "Incorrect number of sealed extents")

		stats, err = mm.ListExtentsByInputIDStatus(killHosts[0], common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN))
		s.Nil(err)
		s.Equal(0, len(stats), "Incorrect number of open extents")
		return
	}

	for _, h := range killHosts {
		stats, err = mm.ListExtentsByStoreIDStatus(h, common.MetadataExtentStatusPtr(shared.ExtentStatus_SEALED))
		s.Nil(err)
		s.True(len(stats) > 0, "Incorrect number of sealed extents")

		stats, err = mm.ListExtentsByStoreIDStatus(h, common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN))
		s.Nil(err)
		s.Equal(0, len(stats), "Incorrect number of open extents")
	}
}

type mockM3Metrics struct{}
type mockStopWatch struct{}

func (s *mockStopWatch) Stop() time.Duration                              { return time.Second }
func (m *mockM3Metrics) IncCounter(scope int, counter int)                {}
func (m *mockM3Metrics) AddCounter(scope int, counter int, delta int64)   {}
func (m *mockM3Metrics) UpdateGauge(scope int, counter int, delta int64)  {}
func (m *mockM3Metrics) RecordTimer(op int, idx int, delta time.Duration) {}
func (m *mockM3Metrics) StartTimer(scope int, timer int) localMetrics.Stopwatch {
	return &mockStopWatch{}
}
func (m *mockM3Metrics) GetParentReporter() localMetrics.Reporter { return nil }
