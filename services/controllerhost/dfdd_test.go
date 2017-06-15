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

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
)

type (
	DfddTestSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
		rpm           *testRpmImpl
		eventPipeline *testEventPipelineImpl
		context       *Context
		dfdd          Dfdd
		timeSource    *common.MockTimeSource
	}
)

func TestDfddSuite(t *testing.T) {
	suite.Run(t, new(DfddTestSuite))
}

func (s *DfddTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.rpm = newTestRpm()
	s.eventPipeline = newTestEventPipeline()
	s.timeSource = common.NewMockTimeSource()

	s.context = &Context{
		log:           bark.NewLoggerFromLogrus(log.New()).WithField("testName", "DfddTest"),
		rpm:           s.rpm,
		eventPipeline: s.eventPipeline,
	}
	s.dfdd = NewDfdd(s.context, s.timeSource)
}

func (s *DfddTestSuite) TestFailureDetection() {
	oldStateMachineInterval := stateMachineTickerInterval
	stateMachineTickerInterval = time.Second
	s.dfdd.Start()
	inHostIDs := []string{uuid.New(), uuid.New(), uuid.New()}
	storeIDs := []string{uuid.New(), uuid.New(), uuid.New()}

	for _, h := range inHostIDs {
		s.rpm.NotifyListeners(common.InputServiceName, h, common.HostAddedEvent)
		s.rpm.NotifyListeners(common.InputServiceName, h, common.HostRemovedEvent)
	}
	for i, h := range storeIDs {
		s.rpm.NotifyListeners(common.StoreServiceName, h, common.HostAddedEvent)
		if i == len(storeIDs)-1 {
			s.dfdd.ReportHostGoingDown(common.StoreServiceName, h)
		} else {
			s.rpm.NotifyListeners(common.StoreServiceName, h, common.HostRemovedEvent)
		}
	}

	cond := func() bool {
		return s.eventPipeline.numInputHostFailedEvents() == len(inHostIDs) &&
			s.eventPipeline.numStoreHostFailedEvents() == len(storeIDs)
	}

	succ := common.SpinWaitOnCondition(cond, 10*time.Second)
	s.True(succ, "Dfdd failed to detect failure within timeout")
	s.Equal(0, s.eventPipeline.numStoreRemoteExtentReplicatorDownEvents(), "unexpected events generated")

	// async, not guaranteed to change state immediately
	s.rpm.NotifyListeners(common.StoreServiceName, storeIDs[2], common.HostRemovedEvent)
	cond = func() bool {
		s, _ := s.dfdd.GetHostState(common.StoreServiceName, storeIDs[2])
		return s == dfddHostStateDown
	}
	succ = common.SpinWaitOnCondition(cond, 10*time.Second)
	s.True(succ, "dfdd failed to mark host as down")

	for _, h := range inHostIDs {
		state, _ := s.dfdd.GetHostState(common.InputServiceName, h)
		s.True(s.eventPipeline.isInputHostFailed(h), "Dfdd failed to detect in host failure")
		s.Equal(dfddHostStateUnknown, state, "dfdd failed to delete failed input host entry")
	}

	s.timeSource.Advance(time.Duration(downToForgottenDuration) - time.Millisecond)
	for _, h := range storeIDs {
		state, d := s.dfdd.GetHostState(common.StoreServiceName, h)
		s.True(s.eventPipeline.isStoreHostFailed(h), "Dfdd failed to detect store host failure")
		s.Equal(dfddHostStateDown, state, "dfdd forgot about a storehost pre-maturely")
		s.Equal(time.Duration(downToForgottenDuration)-time.Millisecond, d, "getHostState() returned wrong duration")
	}

	s.timeSource.Advance(time.Millisecond)
	for _, h := range storeIDs {
		cond := func() bool { s, _ := s.dfdd.GetHostState(common.StoreServiceName, h); return s == dfddHostStateUnknown }
		succ := common.SpinWaitOnCondition(cond, time.Second*10)
		s.True(succ, "dfdd failed to remove store host entry after downToForgottenDuration")
	}
	// now test hostGoingDown state
	s.rpm.NotifyListeners(common.InputServiceName, inHostIDs[0], common.HostAddedEvent)
	s.rpm.NotifyListeners(common.StoreServiceName, storeIDs[0], common.HostAddedEvent)
	succ = common.SpinWaitOnCondition(func() bool {
		h1, _ := s.dfdd.GetHostState(common.InputServiceName, inHostIDs[0])
		h2, _ := s.dfdd.GetHostState(common.StoreServiceName, storeIDs[0])
		return h1 == dfddHostStateUP && h2 == dfddHostStateUP
	}, 10*time.Second)
	s.True(succ, "dfdd failed to discover new hosts")

	s.dfdd.ReportHostGoingDown(common.InputServiceName, inHostIDs[0])
	s.dfdd.ReportHostGoingDown(common.StoreServiceName, storeIDs[0])
	succ = common.SpinWaitOnCondition(func() bool {
		h1, _ := s.dfdd.GetHostState(common.InputServiceName, inHostIDs[0])
		h2, _ := s.dfdd.GetHostState(common.StoreServiceName, storeIDs[0])
		return h1 == dfddHostStateGoingDown && h2 == dfddHostStateGoingDown
	}, 10*time.Second)
	s.True(succ, "dfdd failed to discover new hosts")

	s.context.failureDetector = s.dfdd
	succ = isDfddHostStatusGoingDown(s.context.failureDetector, common.InputServiceName, inHostIDs[0])
	s.True(succ, "isInputGoingDown() failed")

	succ = isDfddHostStatusGoingDown(s.context.failureDetector, common.StoreServiceName, storeIDs[0])
	s.True(succ, "isStoreGoingDown() failed")

	dstExtent := &m.DestinationExtent{
		ExtentUUID: common.StringPtr(uuid.New()),
		StoreUUIDs: []string{storeIDs[0]},
	}

	succ = areExtentStoresHealthy(s.context, dstExtent)
	s.False(succ, "areExtentStoresHealthy check failed")

	dstExtent.StoreUUIDs = []string{storeIDs[1]}
	succ = areExtentStoresHealthy(s.context, dstExtent)
	s.False(succ, "areExtentStoresHealthy check failed")

	s.dfdd.Stop()
	stateMachineTickerInterval = oldStateMachineInterval
}

func (s *DfddTestSuite) TestDfddStateToString() {
	states := []dfddHostState{
		dfddHostStateUnknown,
		dfddHostStateUP,
		dfddHostStateGoingDown,
		dfddHostStateDown,
		dfddHostStateForgotten,
		dfddHostState(-1),
		dfddHostState(128),
	}
	for _, st := range states {
		switch st {
		case dfddHostStateUnknown:
			s.Equal("unknown", st.String())
		case dfddHostStateUP:
			s.Equal("up", st.String())
		case dfddHostStateGoingDown:
			s.Equal("goingDown", st.String())
		case dfddHostStateDown:
			s.Equal("down", st.String())
		case dfddHostStateForgotten:
			s.Equal("forgotten", st.String())
		default:
			s.Equal("invalid", st.String())
		}
	}
}

type testEventPipelineImpl struct {
	sync.Mutex
	nInputHostFailedEvents                 int
	nStoreHostFailedEvents                 int
	nStoreRemoteExtentReplicatorDownEvents int
	failedInputs                           map[string]struct{}
	failedStores                           map[string]struct{}
}

func newTestEventPipeline() *testEventPipelineImpl {
	return &testEventPipelineImpl{
		failedInputs: make(map[string]struct{}, 4),
		failedStores: make(map[string]struct{}, 4),
	}
}

func (ep *testEventPipelineImpl) Start() {}
func (ep *testEventPipelineImpl) Stop()  {}
func (ep *testEventPipelineImpl) Add(event Event) bool {
	ep.Lock()
	switch event.(type) {
	case *InputHostFailedEvent:
		ep.nInputHostFailedEvents++
		e, _ := event.(*InputHostFailedEvent)
		ep.failedInputs[e.hostUUID] = struct{}{}
	case *StoreHostFailedEvent:
		e, _ := event.(*StoreHostFailedEvent)
		ep.nStoreHostFailedEvents++
		ep.failedStores[e.hostUUID] = struct{}{}
	}
	ep.Unlock()
	return true
}

func (ep *testEventPipelineImpl) GetRetryableEventExecutor() RetryableEventExecutor {
	return nil
}

func (ep *testEventPipelineImpl) isInputHostFailed(uuid string) bool {
	ok := false
	ep.Lock()
	_, ok = ep.failedInputs[uuid]
	ep.Unlock()
	return ok
}

func (ep *testEventPipelineImpl) isStoreHostFailed(uuid string) bool {
	ok := false
	ep.Lock()
	_, ok = ep.failedStores[uuid]
	ep.Unlock()
	return ok
}

func (ep *testEventPipelineImpl) numInputHostFailedEvents() int {
	count := 0
	ep.Lock()
	count = ep.nInputHostFailedEvents
	ep.Unlock()
	return count
}

func (ep *testEventPipelineImpl) numStoreHostFailedEvents() int {
	count := 0
	ep.Lock()
	count = ep.nStoreHostFailedEvents
	ep.Unlock()
	return count
}

func (ep *testEventPipelineImpl) numStoreRemoteExtentReplicatorDownEvents() int {
	count := 0
	ep.Lock()
	count = ep.nStoreRemoteExtentReplicatorDownEvents
	ep.Unlock()
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

// FindRandomHost finds and returns a random host responsible for handling the given service
func (rpm *testRpmImpl) FindRandomHost(service string) (*common.HostInfo, error) {
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
