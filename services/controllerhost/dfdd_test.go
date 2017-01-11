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

package controllerhost

import (
	"sync"
	"testing"
	"time"

	"github.com/uber/cherami-server/common"
	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
)

type (
	DfddTestSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
		rpm           *testRpmImpl
		eventPipeline *testEventPipelineImpl
		context       *Context
		dfdd          Dfdd
	}
)

func TestDfddSuite(t *testing.T) {
	suite.Run(t, new(DfddTestSuite))
}

func (s *DfddTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.rpm = newTestRpm()
	s.eventPipeline = newTestEventPipeline()

	s.context = &Context{
		log:           bark.NewLoggerFromLogrus(log.New()).WithField("testName", "DfddTest"),
		rpm:           s.rpm,
		eventPipeline: s.eventPipeline,
	}
	s.dfdd = NewDfdd(s.context)
}

func (s *DfddTestSuite) TestFailureDetection() {
	s.dfdd.OverrideHostDownPeriodForStage2(time.Duration(1 * time.Second))
	s.dfdd.OverrideHealthCheckInterval(time.Duration(1 * time.Second))
	s.dfdd.Start()
	inHostIDs := []string{uuid.New(), uuid.New(), uuid.New()}
	storeIDs := []string{uuid.New(), uuid.New(), uuid.New()}

	for _, h := range inHostIDs {
		s.rpm.NotifyListeners(common.InputServiceName, h, common.HostAddedEvent)
		s.rpm.NotifyListeners(common.InputServiceName, h, common.HostRemovedEvent)
	}
	for _, h := range storeIDs {
		s.rpm.NotifyListeners(common.StoreServiceName, h, common.HostAddedEvent)
		s.rpm.NotifyListeners(common.StoreServiceName, h, common.HostRemovedEvent)
	}

	cond := func() bool {
		return (s.eventPipeline.inHostFailureCount() == len(inHostIDs) &&
			s.eventPipeline.storeHostFailureStage1Count() == len(storeIDs) &&
			s.eventPipeline.storeHostFailureStage2Count() == len(storeIDs))
	}

	succ := common.SpinWaitOnCondition(cond, 10*time.Second)
	s.True(succ, "Dfdd failed to detect failure within timeout")

	for _, h := range inHostIDs {
		s.True(s.eventPipeline.isHostFailed(h), "Dfdd failed to detect in host failure")
	}

	for _, h := range storeIDs {
		s.True(s.eventPipeline.isHostFailed(h), "Dfdd failed to detect store host failure")
		s.True(s.eventPipeline.isStoreFailedStage2(h), "Dfdd failed to detect store host stage 2 failure")
	}

	s.dfdd.Stop()
}

type testEventPipelineImpl struct {
	inHostFailures          int
	storeHostStage1Failures int
	storeHostStage2Failures int
	failedHosts             map[string]bool
	failedStage2Stores      map[string]bool
	mutex                   sync.Mutex
}

func newTestEventPipeline() *testEventPipelineImpl {
	return &testEventPipelineImpl{
		failedHosts:        make(map[string]bool),
		failedStage2Stores: make(map[string]bool),
	}
}

func (ep *testEventPipelineImpl) Start() {}
func (ep *testEventPipelineImpl) Stop()  {}
func (ep *testEventPipelineImpl) Add(event Event) bool {
	ep.mutex.Lock()
	switch event.(type) {
	case *InputHostFailedEvent:
		ep.inHostFailures++
		e, _ := event.(*InputHostFailedEvent)
		ep.failedHosts[e.hostUUID] = true
	case *StoreHostFailedEvent:
		e, _ := event.(*StoreHostFailedEvent)
		if e.stage == hostDownStage1 {
			ep.storeHostStage1Failures++
			ep.failedHosts[e.hostUUID] = true
		} else if e.stage == hostDownStage2 {
			ep.storeHostStage2Failures++
			ep.failedStage2Stores[e.hostUUID] = true
		}
	}
	ep.mutex.Unlock()
	return true
}

func (ep *testEventPipelineImpl) GetRetryableEventExecutor() RetryableEventExecutor {
	return nil
}

func (ep *testEventPipelineImpl) isHostFailed(uuid string) bool {
	ok := false
	ep.mutex.Lock()
	_, ok = ep.failedHosts[uuid]
	ep.mutex.Unlock()
	return ok
}

func (ep *testEventPipelineImpl) isStoreFailedStage2(uuid string) bool {
	ok := false
	ep.mutex.Lock()
	_, ok = ep.failedStage2Stores[uuid]
	ep.mutex.Unlock()
	return ok
}

func (ep *testEventPipelineImpl) storeHostFailureStage1Count() int {
	count := 0
	ep.mutex.Lock()
	count = ep.storeHostStage1Failures
	ep.mutex.Unlock()
	return count
}

func (ep *testEventPipelineImpl) storeHostFailureStage2Count() int {
	count := 0
	ep.mutex.Lock()
	count = ep.storeHostStage2Failures
	ep.mutex.Unlock()
	return count
}

func (ep *testEventPipelineImpl) inHostFailureCount() int {
	count := 0
	ep.mutex.Lock()
	count = ep.inHostFailures
	ep.mutex.Unlock()
	return count
}

type testRpmImpl struct {
	listeners map[string][]chan<- *common.RingpopListenerEvent
}

func newTestRpm() *testRpmImpl {
	return &testRpmImpl{
		listeners: make(map[string][]chan<- *common.RingpopListenerEvent),
	}
}

func (rpm *testRpmImpl) Start() {}
func (rpm *testRpmImpl) Stop()  {}
func (rpm *testRpmImpl) GetBootstrappedChannel() chan struct{} {
	return nil
}

func (rpm *testRpmImpl) GetHosts(service string) ([]*common.HostInfo, error) {
	return nil, common.ErrUUIDLookupFailed
}

// FindHostForAddr finds and returns the host for the given address
func (rpm *testRpmImpl) FindHostForAddr(service string, addr string) (*common.HostInfo, error) {
	return nil, common.ErrUUIDLookupFailed
}

// FindHostForKey finds and returns the host responsible for handling the given (service, key)
func (rpm *testRpmImpl) FindHostForKey(service string, key string) (*common.HostInfo, error) {
	return nil, common.ErrUUIDLookupFailed
}

// IsHostHealthy returns true if the given (service, host) is healthy
func (rpm *testRpmImpl) IsHostHealthy(service string, uuid string) bool { return false }

// ResolveUUID resovles a host UUID to an IP address, if the host is found
func (rpm *testRpmImpl) ResolveUUID(service string, uuid string) (string, error) {
	return "", common.ErrUUIDLookupFailed
}

// AddListener adds a listener for this service.
// The listener will get notified on the given
// channel, whenever there is host joining/leaving
// the ring.
// @service: The service to be listened on
// @name: The name for identifying the listener
// @notifyChannel: The channel on which the caller receives notifications
func (rpm *testRpmImpl) AddListener(service string, name string, notifyChannel chan<- *common.RingpopListenerEvent) error {
	list, ok := rpm.listeners[service]
	if !ok {
		list = make([]chan<- *common.RingpopListenerEvent, 0, 2)
		rpm.listeners[service] = list
	}
	rpm.listeners[service] = append(list, notifyChannel)
	return nil
}

// RemoveListener removes a listener for this service.
func (rpm *testRpmImpl) RemoveListener(service string, name string) error { return nil }

// SetMetadata sets the metadata on this rp instance
func (rpm *testRpmImpl) SetMetadata(key string, data string) error { return nil }

func (rpm *testRpmImpl) NotifyListeners(service string, key string, eventType common.RingpopEventType) {
	event := &common.RingpopListenerEvent{
		Key:  key,
		Type: eventType,
	}
	for _, l := range rpm.listeners[service] {
		select {
		case l <- event:
			break
		default:
			log.Errorf("Listener Queue full")
		}
	}
}
