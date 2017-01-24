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

	"github.com/uber-common/bark"
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

		// override the durations, used for testing
		OverrideHostDownPeriodForStage2(period time.Duration)
		OverrideHealthCheckInterval(period time.Duration)
	}

	// serviceID is an enum for identifying
	// cherami service [input/output/store]
	serviceID int

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

		healthCheckTicker   *time.Ticker
		healthCheckInterval time.Duration

		unhealthyStores     map[string]time.Time
		unhealthyStoresLock sync.RWMutex

		unhealthyInputs     map[string]time.Time
		unhealthyInputsLock sync.RWMutex

		hostDownPeriodForStage2 time.Duration
	}
)

const (
	inputServiceID serviceID = iota
	outputServiceID
	storeServiceID
)

const (
	listenerChannelSize = 32

	healthCheckInterval = time.Duration(1 * time.Minute)
)

const (
	hostDownPeriodForStage2 = time.Duration(3 * time.Minute)
)

// different stages of host being down
// stage 1: host is just removed from ringpop. Any service restart or deployment can trigger it
// stage 2: host is removed from ringpop for hostDownPeriodForStage2. For example a machine reboot can trigger it
// stage 3: host is removed form ringpop for hostDownPeriodForStage3(for example: 24 hrs). Most likely the machine is down and needs manual repair.
// Note currently stage 3 is not being handled yet
type hostDownStage int

const (
	hostDownStage1 hostDownStage = iota
	hostDownStage2
)

// NewDfdd creates and returns an instance of discovery
// and failure detection daemon. Dfdd will monitor
// the health of Input/Output/Store hosts and trigger
// Input/Output/StoreHostFailedEvent for every host
// that failed. It currently does not maintain a list
// of healthy hosts for every service, thats a WIP.
func NewDfdd(context *Context) Dfdd {
	return &dfddImpl{
		context:                 context,
		shutdownC:               make(chan struct{}),
		inputListenerCh:         make(chan *common.RingpopListenerEvent, listenerChannelSize),
		storeListenerCh:         make(chan *common.RingpopListenerEvent, listenerChannelSize),
		unhealthyStores:         make(map[string]time.Time),
		unhealthyInputs:         make(map[string]time.Time),
		hostDownPeriodForStage2: hostDownPeriodForStage2,
		healthCheckInterval:     healthCheckInterval,
	}
}

func (dfdd *dfddImpl) Start() {
	if !atomic.CompareAndSwapInt32(&dfdd.started, 0, 1) {
		dfdd.context.log.Fatal("Attempt to start failure detector twice")
	}

	dfdd.healthCheckTicker = time.NewTicker(dfdd.healthCheckInterval)

	rpm := dfdd.context.rpm

	err := rpm.AddListener(common.InputServiceName, buildListenerName(common.InputServiceName), dfdd.inputListenerCh)
	if err != nil {
		dfdd.context.log.WithField(common.TagErr, err).Fatal(`AddListener(inputhost) failed`)
		return
	}

	err = rpm.AddListener(common.StoreServiceName, buildListenerName(common.StoreServiceName), dfdd.storeListenerCh)
	if err != nil {
		dfdd.context.log.WithField(common.TagErr, err).Fatal(`AddListener(storehost) failed`)
		return
	}

	dfdd.shutdownWG.Add(1)

	go dfdd.run()

	go dfdd.healthCheck()

	dfdd.context.log.Info("Failure Detector Daemon started")
}

func (dfdd *dfddImpl) Stop() {
	close(dfdd.shutdownC)
	if !common.AwaitWaitGroup(&dfdd.shutdownWG, time.Second) {
		dfdd.context.log.Error("Timeoud out waiting for failure detector to stop")
	}

	dfdd.context.log.Info("Failure Detector Daemon stopped")
}

// run is the main event loop that receives
// events from RingpopMonitor and triggers
// node failed events to the event pipeline
func (dfdd *dfddImpl) run() {
	for {
		select {
		case e := <-dfdd.inputListenerCh:
			dfdd.handleListenerEvent(inputServiceID, e)
		case e := <-dfdd.storeListenerCh:
			dfdd.handleListenerEvent(storeServiceID, e)
		case <-dfdd.shutdownC:
			dfdd.shutdownWG.Done()
			return
		}
	}
}

func (dfdd *dfddImpl) OverrideHostDownPeriodForStage2(period time.Duration) {
	dfdd.hostDownPeriodForStage2 = period
}

func (dfdd *dfddImpl) OverrideHealthCheckInterval(period time.Duration) {
	dfdd.healthCheckInterval = period
}

func (dfdd *dfddImpl) healthCheckRoutine() {
	var unhealthyInputList []string
	{
		dfdd.unhealthyInputsLock.RLock()
		currentTime := time.Now().UTC()
		for host, lastSeenTime := range dfdd.unhealthyInputs {
			if currentTime.Sub(lastSeenTime) > dfdd.hostDownPeriodForStage2 {
				dfdd.context.log.WithFields(bark.Fields{
					common.TagIn:     common.FmtIn(host),
					`last seen time`: lastSeenTime,
				}).Info("Input host is down(stage 2)")
				unhealthyInputList = append(unhealthyInputList, host)
			}
		}
		dfdd.unhealthyInputsLock.RUnlock()
	}
	for _, host := range unhealthyInputList {
		// report unhealthy here(which will reset the timestamp to now) so that the actions can be triggered again
		// in next cycle(after hostDownPeriodForStage2) if the host still doesn't come back
		dfdd.reportHostUnhealthy(inputServiceID, host)
	}

	var unhealthyStoreList []string
	{
		dfdd.unhealthyStoresLock.RLock()
		currentTime := time.Now().UTC()
		for host, lastSeenTime := range dfdd.unhealthyStores {
			if currentTime.Sub(lastSeenTime) > dfdd.hostDownPeriodForStage2 {
				dfdd.context.log.WithFields(bark.Fields{
					common.TagStor:   common.FmtStor(host),
					`last seen time`: lastSeenTime,
				}).Info("Store host is down(stage2)")

				event := NewStoreHostFailedEvent(host, hostDownStage2)
				if !dfdd.context.eventPipeline.Add(event) {
					dfdd.context.log.WithField(common.TagStor, common.FmtStor(host)).Error("Failed to enqueue StoreHostFailedEvent(stage 2)")
				}
				unhealthyStoreList = append(unhealthyStoreList, host)
			}
		}
		dfdd.unhealthyStoresLock.RUnlock()
	}
	for _, host := range unhealthyStoreList {
		// report unhealthy here(which will reset the timestamp to now) so that the actions can be triggered again
		// in next cycle(after hostDownPeriodForStage2) if the host still doesn't come back
		dfdd.reportHostUnhealthy(storeServiceID, host)
	}

	return
}

func (dfdd *dfddImpl) healthCheck() {
	for {
		select {
		case <-dfdd.healthCheckTicker.C:
			dfdd.healthCheckRoutine()
		case <-dfdd.shutdownC:
			return
		}
	}
}

func (dfdd *dfddImpl) reportHostUnhealthy(id serviceID, hostUUID string) {
	switch id {
	case inputServiceID:
		dfdd.context.log.WithField(common.TagIn, common.FmtIn(hostUUID)).Info("report input unhealthy")
		dfdd.unhealthyInputsLock.Lock()
		defer dfdd.unhealthyInputsLock.Unlock()
		dfdd.unhealthyInputs[hostUUID] = time.Now().UTC()
	case storeServiceID:
		dfdd.context.log.WithField(common.TagStor, common.FmtStor(hostUUID)).Info("report store unhealthy")
		dfdd.unhealthyStoresLock.Lock()
		defer dfdd.unhealthyStoresLock.Unlock()
		dfdd.unhealthyStores[hostUUID] = time.Now().UTC()
	}
}

func (dfdd *dfddImpl) reportHostHealthy(id serviceID, hostUUID string) {
	switch id {
	case inputServiceID:
		dfdd.unhealthyInputsLock.Lock()
		defer dfdd.unhealthyInputsLock.Unlock()
		delete(dfdd.unhealthyInputs, hostUUID)
	case storeServiceID:
		dfdd.unhealthyStoresLock.Lock()
		defer dfdd.unhealthyStoresLock.Unlock()
		delete(dfdd.unhealthyStores, hostUUID)
	}
}

func (dfdd *dfddImpl) handleListenerEvent(id serviceID, listenerEvent *common.RingpopListenerEvent) {

	if listenerEvent.Type == common.HostAddedEvent {
		dfdd.reportHostHealthy(id, listenerEvent.Key)
		return
	}

	dfdd.reportHostUnhealthy(id, listenerEvent.Key)

	var event Event

	switch id {
	case inputServiceID:
		dfdd.context.log.WithField(common.TagIn, common.FmtIn(listenerEvent.Key)).Info("InputHostFailed")
		event = NewInputHostFailedEvent(listenerEvent.Key)
	case storeServiceID:
		dfdd.context.log.WithField(common.TagStor, common.FmtStor(listenerEvent.Key)).Info("StoreHostFailed")
		event = NewStoreHostFailedEvent(listenerEvent.Key, hostDownStage1)
	default:
		dfdd.context.log.Error("ListenerEvent for unknown service")
		return
	}

	if !dfdd.context.eventPipeline.Add(event) {
		dfdd.context.log.WithField(common.TagEvent, event).Error("Failed to enqueue event")
	}
}

func buildListenerName(prefix string) string {
	return prefix + "-fail-detector-listener"
}
