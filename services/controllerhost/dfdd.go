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
	"sync/atomic"
	"time"

	"errors"
	"github.com/uber/cherami-server/common"
)

type (
	// Dfdd Discovery and Failure Detection Daemon
	// is a background task that keeps track of the
	// healthy members for all cherami services. It
	// is also the place where any custom failure
	// detection logic (on top of Ringpop) must go.
	Dfdd interface {
		common.Daemon
		// ReportHostGoingDown reports a host as going down
		// for planned deployment or maintenance
		ReportHostGoingDown(service string, hostID string)
		// GetHostState returns a tuple representing the current
		// dfdd host state and the duration for which the host
		// has been in that state
		GetHostState(service string, hostID string) (dfddHostState, time.Duration)
	}

	dfddHost struct {
		state               dfddHostState
		lastStateChangeTime int64
	}

	dfddImpl struct {
		started    int32
		shutdownC  chan struct{}
		shutdownWG sync.WaitGroup
		context    *Context
		// Channels subscribed to RingpopMonitor
		// RingpopMonitor will enqueue Join/Leave
		// events to this channel
		inputListenerCh chan *common.RingpopListenerEvent
		storeListenerCh chan *common.RingpopListenerEvent

		inputHosts atomic.Value
		storeHosts atomic.Value

		timeSource common.TimeSource
	}
)

var errUnknownService = errors.New("dfdd: unknown service")

const (
	listenerChannelSize = 32
)

type dfddHostState int

/*
 * State Transitions
 *
 *          Unknown  -- rp.HostAddedEvent           --> UP
 *               UP  -- loadReporter.HostGoingDown  --> GoingDown
 *               UP  -- rp.HostRemovedEvent         --> Down
 *        GoingDown  -- rp.HostRemovedEvent         --> Down
 *  Down (storeHost) -- 2 hours                     --> Forgotten/Removed
 *  Down (inputHost) -- 0 hours                     --> Forgotten/Removed
 */
const (
	dfddHostStateUnknown dfddHostState = iota
	dfddHostStateUP
	dfddHostStateGoingDown
	dfddHostStateDown
	dfddHostStateForgotten
	numDfddStates // must be the last member
)

var dfddStateNames = [numDfddStates]string{
	"unknown",
	"up",
	"goingDown",
	"down",
	"forgotten",
}

// state that represents that the host is about to
// go down for planned deployment or maintenance
const hostGoingDownEvent common.RingpopEventType = 99

// how long remains in down state before its forgotten forever
const downToForgottenDuration = int64(time.Hour * 2)

// max time host can be down during deployments / restarts etc
const maxHostRestartDuration = 5 * time.Minute

// initial capacity of the hosts map
const hostMapInitialCapacity = 8

// periodic ticker interval for the dfdd state machine
var stateMachineTickerInterval = time.Minute * 5

// NewDfdd creates and returns an instance of discovery
// and failure detection daemon. Dfdd will monitor
// the health of Input/Output/Store hosts and trigger
// Input/Output/StoreHostFailedEvent for every host
// that failed. It currently does not maintain a list
// of healthy hosts for every service, thats a WIP.
func NewDfdd(context *Context, timeSource common.TimeSource) Dfdd {
	dfdd := &dfddImpl{
		context:         context,
		timeSource:      timeSource,
		shutdownC:       make(chan struct{}),
		inputListenerCh: make(chan *common.RingpopListenerEvent, listenerChannelSize),
		storeListenerCh: make(chan *common.RingpopListenerEvent, listenerChannelSize),
	}
	dfdd.inputHosts.Store(make(map[string]dfddHost, 8))
	dfdd.storeHosts.Store(make(map[string]dfddHost, 8))
	return dfdd
}

func (dfdd *dfddImpl) Start() {

	if !atomic.CompareAndSwapInt32(&dfdd.started, 0, 1) {
		dfdd.context.log.Fatal("dfdd daemon already started")
	}

	rpm := dfdd.context.rpm

	err := rpm.AddListener(common.InputServiceName, buildListenerName(common.InputServiceName), dfdd.inputListenerCh)
	if err != nil {
		dfdd.context.log.WithField(common.TagErr, err).Fatal(`rpm.addListener(inputhost) failed`)
		return
	}

	err = rpm.AddListener(common.StoreServiceName, buildListenerName(common.StoreServiceName), dfdd.storeListenerCh)
	if err != nil {
		dfdd.context.log.WithField(common.TagErr, err).Fatal(`rpm.addListener(storehost) failed`)
		return
	}

	dfdd.shutdownWG.Add(1)
	go dfdd.run()
	dfdd.context.log.Info("dfdd daemon started")
}

func (dfdd *dfddImpl) Stop() {
	close(dfdd.shutdownC)
	if !common.AwaitWaitGroup(&dfdd.shutdownWG, time.Second) {
		dfdd.context.log.Error("timed out waiting for dfdd daemon to stop")
	}
	dfdd.context.log.Info("dfdd daemon stopped")
}

// GetHostState returns a tuple representing the current
// dfdd host state and the duration for which the host
// has been in that state
func (dfdd *dfddImpl) GetHostState(service string, hostID string) (dfddHostState, time.Duration) {
	hosts, err := dfdd.getHosts(service)
	if err != nil {
		return dfddHostStateUnknown, time.Duration(0)
	}
	if curr, ok := hosts[hostID]; ok {
		now := dfdd.timeSource.Now().UnixNano()
		return curr.state, time.Duration(now - curr.lastStateChangeTime)
	}
	return dfddHostStateUnknown, time.Duration(0)
}

// ReportHostGoingDown reports a host as going down
// for planned deployment or maintenance
func (dfdd *dfddImpl) ReportHostGoingDown(service string, hostID string) {
	event := &common.RingpopListenerEvent{
		Key:  hostID,
		Type: hostGoingDownEvent,
	}
	switch service {
	case common.InputServiceName:
		dfdd.inputListenerCh <- event
		dfdd.context.log.WithField(common.TagIn, hostID).
			Info("input host reported as going down for planned maintenance")
	case common.StoreServiceName:
		dfdd.storeListenerCh <- event
		dfdd.context.log.WithField(common.TagStor, hostID).
			Info("store host reported as going down for planned maintenance")
	default:
		return
	}
}

// run is the main event loop that receives
// events from RingpopMonitor and triggers
// node failed events to the event pipeline
func (dfdd *dfddImpl) run() {

	ticker := common.NewTimer(stateMachineTickerInterval)

	for {
		select {
		case e := <-dfdd.inputListenerCh:
			dfdd.handleListenerEvent(common.InputServiceName, e)
		case e := <-dfdd.storeListenerCh:
			dfdd.handleListenerEvent(common.StoreServiceName, e)
		case <-ticker.C:
			dfdd.handleTicker()
			ticker.Reset(stateMachineTickerInterval)
		case <-dfdd.shutdownC:
			dfdd.shutdownWG.Done()
			return
		}
	}
}

func (dfdd *dfddImpl) handleTicker() {
	now := dfdd.timeSource.Now().UnixNano()
	hosts, _ := dfdd.getHosts(common.StoreServiceName)
	forgotten := make(map[string]struct{}, 4)
	for k, v := range hosts {
		if v.state == dfddHostStateDown {
			diff := now - v.lastStateChangeTime
			if diff >= int64(downToForgottenDuration) {
				forgotten[k] = struct{}{}
				continue
			}
		}
	}

	if len(forgotten) == 0 {
		return
	}

	copy := deepCopyMap(hosts)
	for k := range forgotten {
		delete(copy, k)
	}
	dfdd.putHosts(common.StoreServiceName, copy)
}

func (dfdd *dfddImpl) handleHostAddedEvent(service string, event *common.RingpopListenerEvent) {
	hosts, err := dfdd.getHosts(service)
	if err != nil {
		return
	}
	if curr, ok := hosts[event.Key]; ok {
		if curr.state == dfddHostStateUP {
			return
		}
	}
	copy := deepCopyMap(hosts)
	copy[event.Key] = newDFDDHost(dfddHostStateUP, dfdd.timeSource)
	dfdd.putHosts(service, copy)
}

func (dfdd *dfddImpl) handleHostRemovedEvent(service string, event *common.RingpopListenerEvent) {

	hosts, err := dfdd.getHosts(service)
	if err != nil {
		return
	}

	curr, ok := hosts[event.Key]
	if !ok || curr.state >= dfddHostStateDown {
		return
	}

	var failedEvent Event
	copy := deepCopyMap(hosts)
	switch service {
	case common.InputServiceName:
		delete(copy, event.Key)
		failedEvent = NewInputHostFailedEvent(event.Key)
	case common.StoreServiceName:
		dfddHost := newDFDDHost(dfddHostStateDown, dfdd.timeSource)
		copy[event.Key] = dfddHost
		failedEvent = NewStoreHostFailedEvent(event.Key)
	}

	dfdd.putHosts(service, copy)
	if !dfdd.context.eventPipeline.Add(failedEvent) {
		dfdd.context.log.WithField(common.TagEvent, event).Error("failed to enqueue event")
	}
}

func (dfdd *dfddImpl) handleHostGoingDownEvent(service string, event *common.RingpopListenerEvent) {

	hosts, err := dfdd.getHosts(service)
	if err != nil {
		return
	}

	curr, ok := hosts[event.Key]
	if !ok || curr.state != dfddHostStateUP {
		return
	}

	copy := deepCopyMap(hosts)
	copy[event.Key] = newDFDDHost(dfddHostStateGoingDown, dfdd.timeSource)
	dfdd.putHosts(service, copy)

	if service == common.StoreServiceName {
		// When a store host is about to go down for deployment,
		// we need to trigger draining of every OPEN extent. However,
		// the store is still not *considered* down until its down
		// in ringpop. Ideally, we need a StoreGoingDownEvent, but
		// since the behavior is going to be the same as StoreFailedEvent,
		// we simply enqueue a storeFailedEvent
		if !dfdd.context.eventPipeline.Add(NewStoreHostFailedEvent(event.Key)) {
			dfdd.context.log.WithField(common.TagEvent, event).Error("failed to enqueue event after store reported as GoingDown")
		}
	}
}

func (dfdd *dfddImpl) handleListenerEvent(service string, event *common.RingpopListenerEvent) {
	switch event.Type {
	case common.HostAddedEvent:
		dfdd.handleHostAddedEvent(service, event)
	case common.HostRemovedEvent:
		dfdd.handleHostRemovedEvent(service, event)
	case hostGoingDownEvent:
		dfdd.handleHostGoingDownEvent(service, event)
	}
}

func (dfdd *dfddImpl) getHosts(service string) (map[string]dfddHost, error) {
	switch service {
	case common.InputServiceName:
		return dfdd.inputHosts.Load().(map[string]dfddHost), nil
	case common.StoreServiceName:
		return dfdd.storeHosts.Load().(map[string]dfddHost), nil
	default:
		return nil, errUnknownService
	}
}

func (dfdd *dfddImpl) putHosts(service string, hosts map[string]dfddHost) {
	switch service {
	case common.InputServiceName:
		dfdd.inputHosts.Store(hosts)
	case common.StoreServiceName:
		dfdd.storeHosts.Store(hosts)
	default:
		return
	}
}

func isDfddHostStatusGoingDown(dfdd Dfdd, service string, hostID string) bool {
	state, _ := dfdd.GetHostState(service, hostID)
	return state == dfddHostStateGoingDown
}

func newDFDDHost(state dfddHostState, timeSource common.TimeSource) dfddHost {
	return dfddHost{
		state:               state,
		lastStateChangeTime: timeSource.Now().UnixNano(),
	}
}

func (state dfddHostState) String() string {
	if state < 0 || state >= numDfddStates {
		return "invalid"
	}
	return dfddStateNames[state]
}

// creates a new map and copies the key/values from the
// given map into the new map. The capacity of the new
// map will the max(hostMapInitialCapacity, len(src))
// This is to avoid map reallocations later
func deepCopyMap(src map[string]dfddHost) map[string]dfddHost {
	copy := make(map[string]dfddHost, common.MaxInt(len(src), hostMapInitialCapacity))
	for k, v := range src {
		copy[k] = v
	}
	return copy
}

func buildListenerName(prefix string) string {
	return prefix + "-fail-detector-listener"
}
