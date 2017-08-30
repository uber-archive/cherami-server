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
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/outputhost/load"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

type MessageCacheSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
	msgCache        *cgMsgCache
	msgRedeliveryCh chan *cherami.ConsumerMessage
}

func TestMessageCacheTestSuite(t *testing.T) {
	suite.Run(t, new(MessageCacheSuite))
}

func (s *MessageCacheSuite) SetupTest() {

	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil

	cgStatus := shared.ConsumerGroupStatus_ENABLED

	options := make(map[string]string)
	options[common.FlagEnableSmartRetry] = "false"

	cgDesc := &shared.ConsumerGroupDescription{
		ConsumerGroupUUID:  common.StringPtr(uuid.New()),
		DestinationUUID:    common.StringPtr(uuid.New()),
		ConsumerGroupName:  common.StringPtr("/unittest/msgCache_cg"),
		Status:             &cgStatus,
		MaxDeliveryCount:   common.Int32Ptr(10),
		LockTimeoutSeconds: common.Int32Ptr(1),
		OwnerEmail:         common.StringPtr("foo@uber.com"),
		Options:            options,
	}

	s.msgRedeliveryCh = make(chan *cherami.ConsumerMessage, 128)
	cgCache := &consumerGroupCache{
		cachedCGDesc:          *cgDesc,
		cgMetrics:             load.NewCGMetrics(),
		msgsRedeliveryCh:      s.msgRedeliveryCh,
		msgsCh:                make(chan *cherami.ConsumerMessage, 32),
		msgCacheCh:            make(chan cacheMsg, 32),
		msgCacheRedeliveredCh: make(chan cacheMsg, 32),
		ackMsgCh:              make(chan timestampedAckID, 32),
		nackMsgCh:             make(chan timestampedAckID, 32),
		logger:                bark.NewLoggerFromLogrus(log.New()),
		m3Client:              &mockM3Client{},
		consumerM3Client:      &mockM3Client{},
		notifier:              &mockNotifier{},
		creditNotifyCh:        make(chan int32, 8),
		creditRequestCh:       make(chan string, 8),
		cfgMgr:                &mockConfigMgr{},
	}

	s.msgCache = newMessageDeliveryCache(
		nil,
		128,
		cgCache)
}

func (s *MessageCacheSuite) TearDownTest() {
}

func (s *MessageCacheSuite) TestTimerQueueCleanupAfterRedelivery() {

	ackID := 101
	ackIDMap := make(map[string]struct{})

	for i := 0; i < 100; i++ {

		ackIDStr := strconv.Itoa(ackID)

		cMsg := cacheMsg{
			connID: 99,
			msg: &cherami.ConsumerMessage{
				EnqueueTimeUtc: common.Int64Ptr(int64(time.Now().UnixNano())),
				AckId:          &ackIDStr,
				Lsn:            common.Int64Ptr(87666),
				Address:        common.Int64Ptr(88888),
				Payload: &cherami.PutMessage{
					ID:   common.StringPtr("64"),
					Data: []byte("abc"),
				},
			},
		}

		cMsg.msg.AckId = common.StringPtr(ackIDStr)
		s.msgCache.utilHandleDeliveredMsg(cMsg)
		ackIDMap[ackIDStr] = struct{}{}
		ackID++
	}

	time.Sleep(time.Second)

	s.msgCache.utilHandleRedeliveryTicker()
	s.Equal(100, len(s.msgRedeliveryCh), "Unexpected message cache redelivery")

	for i := 0; i < 100; i++ {
		m := <-s.msgRedeliveryCh
		_, ok := ackIDMap[m.GetAckId()]
		s.True(ok, "message cache redelivery for unknown ackID %v", m.GetAckId())
		delete(ackIDMap, m.GetAckId())
	}

	s.msgCache.utilHandleRedeliveryTicker()
	s.Equal(0, len(s.msgRedeliveryCh), "Unexpected message cache redelivery")
}

// mocks go here

type mockNotifier struct{}

func (m *mockNotifier) Register(id int, notifyCh chan NotifyMsg) {}
func (m *mockNotifier) Unregister(id int)                        {}
func (m *mockNotifier) Notify(id int, notifyMsg NotifyMsg)       {}

type mockM3Client struct{}
type mockStopwatch struct{}

func (m *mockM3Client) IncCounter(op int, idx int)                       {}
func (m *mockM3Client) AddCounter(op int, idx int, delta int64)          {}
func (m *mockM3Client) UpdateGauge(op int, idx int, delta int64)         {}
func (m *mockM3Client) RecordTimer(op int, idx int, delta time.Duration) {}
func (m *mockM3Client) StartTimer(scopeIdx int, timerIdx int) metrics.Stopwatch {
	watch := &mockStopwatch{}
	return watch
}
func (m *mockM3Client) GetParentReporter() metrics.Reporter { return nil }
func (m *mockStopwatch) Stop() time.Duration {
	return time.Second
}

type mockConfigMgr struct{}

func (m *mockConfigMgr) Start() {}
func (m *mockConfigMgr) Stop()  {}
func (m *mockConfigMgr) Get(svc string, version string, sku string, host string) (interface{}, error) {
	return nil, errors.New("mock cfg")
}
