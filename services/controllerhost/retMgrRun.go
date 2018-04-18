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

// retMgrRunner: ensures that retention-manager actually runs on only one of
// the controllers. it does so by using ringpop hashing to pick a "chosen"
// node.

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/retentionmgr"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
)

// config args for the retentionMgr
const (
	// TODO: read these in from "config"
	retentionMgrInterval             = 10 * time.Minute
	dlqRetentionMgrInterval          = 1 * time.Hour
	singleCGVisibleExtentGracePeriod = 7 * 24 * time.Hour
	extentDeleteDeferPeriod          = 3 * time.Hour
	retentionMgrWorkers              = 8
)

type (
	// retMgrRunnerContext are args passed into newRetMgr
	retMgrRunnerContext struct {
		hostID         string
		ringpop        common.RingpopMonitor
		metadataClient metadata.TChanMetadataService
		clientFactory  common.ClientFactory
		log            bark.Logger
		m3Client       metrics.Client
		localZone      string
	}

	// retMgrRunner holds the instance context
	retMgrRunner struct {
		*retMgrRunnerContext

		retentionMgr   *retentionmgr.RetentionManager
		running        uint32
		listenC        chan *common.RingpopListenerEvent
		stopC          chan struct{}
		sync.WaitGroup // to wait on go-routines to complete
	}
)

func newRetMgrRunner(ctx *retMgrRunnerContext) *retMgrRunner {
	return &retMgrRunner{
		retMgrRunnerContext: ctx,
		listenC:             make(chan *common.RingpopListenerEvent, 32),
		stopC:               make(chan struct{}),
	}
}

// Start: start
func (t *retMgrRunner) Start() {

	t.log.Debug("retMgrRunner: Starting")

	t.Add(1)
	go t.worker()
}

// Stop: stop
func (t *retMgrRunner) Stop() {

	close(t.stopC)
	t.Wait()
	t.log.Debug("retMgrRunner: Stopped")
}

// worker: go-routine that listens for ringpop notifications and start
// retention-manager, if appropriate
func (t *retMgrRunner) worker() {

	defer t.Done()

	// register with ringpop to get notified on 'controller' events
	err := t.ringpop.AddListener(common.ControllerServiceName, common.ControllerServiceName+"-retention", t.listenC)

	if err != nil {
		t.log.WithField(common.TagErr, err).Error(`retMgrRunner: AddListener('controller', 'controller-retention') failed`)
		// NB: we ignore any failures here and continue
	}

	if t.isPrimary() {
		t.startRetentionMgr()
	} else {
		t.log.Debug("retMgrRunner: not primary; not running here")
	}

forever:
	for {
		select {
		case <-t.listenC: // ringpop change notification

			if t.isPrimary() {
				t.startRetentionMgr()
			} else {
				t.stopRetentionMgr()
			}

		case <-t.stopC: // stopped
			t.log.Debug("retMgrRunner: Stop notification")
			t.stopRetentionMgr()
			break forever
		}
	}
}

// runHere: checks if retention manager should run on this host
func (t *retMgrRunner) isPrimary() bool {

	// do a findhost on a controller, with key "retention-manager"
	hostInfo, err := t.ringpop.FindHostForKey(common.ControllerServiceName, "retention-manager")

	if err != nil {
		t.log.WithField(common.TagErr, err).Error(`retMgrRunner: FindHostForKey failed`)
		return true // NB: on failure, we default to 'true'
	}

	// if the host-id matches ours, run here!
	return t.hostID == hostInfo.UUID
}

// startRetentionMgr: starts retention manager, if it were not already running
func (t *retMgrRunner) startRetentionMgr() {

	if atomic.CompareAndSwapUint32(&t.running, 0, 1) {

		// if uninitialized, initialize an instance (will happen only the first time)
		if t.retentionMgr == nil {

			opts := &retentionmgr.Options{
				RetentionInterval: retentionMgrInterval,
				// DLQRetentionInterval: dlqRetentionMgrInterval,
				SingleCGVisibleExtentGracePeriod: singleCGVisibleExtentGracePeriod,
				ExtentDeleteDeferPeriod:          extentDeleteDeferPeriod,
				NumWorkers:                       retentionMgrWorkers,
				LocalZone:                        t.retMgrRunnerContext.localZone,
			}

			t.retentionMgr = retentionmgr.New(opts, t.metadataClient, t.clientFactory, t.m3Client, t.log)
		}

		t.log.Debug("retMgrRunner: Starting retention manager")
		t.retentionMgr.Start()
	}
}

// stopRetentionMgr: stops retention manager, if it were running
func (t *retMgrRunner) stopRetentionMgr() {

	if atomic.CompareAndSwapUint32(&t.running, 1, 0) {

		t.retentionMgr.Stop()
		t.log.Debug("retMgrRunner: Stopped retention manager")
	}
}

// IsRunning is a test-hook to enable tests to check if retention-manager
// is running on this host
func (t *retMgrRunner) IsRunning() bool {

	return atomic.LoadUint32(&t.running) == 1
}
