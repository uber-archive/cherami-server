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

package common

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"

	"github.com/uber/cherami-thrift/.generated/go/metadata"
)

type (
	// HostIDHeartbeater keeps the host uuid to ip
	// address mapping alive by periodically heartbeating
	// to the registry. Currently, the uuid to addr
	// mapping is stored within a cassandra table.
	HostIDHeartbeater interface {
		Daemon
	}
	// uuidHeartbeater is an implementation of
	// HostIDHeartbeater that heartbeats to
	// the underlying cassandra tables.
	uuidHeartbeater struct {
		hostID     string
		hostName   string
		hostAddr   string
		started    int32
		shutdownC  chan struct{}
		shutdownWG sync.WaitGroup
		mClient    metadata.TChanMetadataService
		logger     bark.Logger
	}
)

const (
	hbInterval = time.Minute
	ttlSeconds = int64((time.Hour * 24 * 7) / time.Second)
)

// NewHostIDHeartbeater creates and returns a new instance of HostIDHeartbeater
func NewHostIDHeartbeater(mClient metadata.TChanMetadataService, hostID string, hostAddr string, hostName string, log bark.Logger) HostIDHeartbeater {
	return &uuidHeartbeater{
		hostID:    hostID,
		hostName:  hostName,
		hostAddr:  hostAddr,
		shutdownC: make(chan struct{}),
		mClient:   mClient,
		logger:    log,
	}
}

// Start starts the heartbeater
func (uhb *uuidHeartbeater) Start() {
	if !atomic.CompareAndSwapInt32(&uhb.started, 0, 1) {
		return
	}
	uhb.shutdownWG.Add(1)
	go uhb.heartbeat()
	uhb.logger.WithFields(bark.Fields{`hostID`: uhb.hostID, `hostName`: uhb.hostName, `hostAddr`: uhb.hostAddr}).Info("HostIDHeartbeater started")
}

// Stop stops the heartbeater
func (uhb *uuidHeartbeater) Stop() {
	if uhb.isStopped() {
		return
	}
	close(uhb.shutdownC)
	succ := AwaitWaitGroup(&uhb.shutdownWG, time.Minute)
	if !succ {
		uhb.logger.Error("Timed out waiting for HostIDHeartbeater to stop")
	}
}

func (uhb *uuidHeartbeater) isStopped() bool {
	select {
	case <-uhb.shutdownC:
		return true
	default:
		return false
	}
}

func (uhb *uuidHeartbeater) heartbeat() {

	refreshTimeout := time.After(0)

	req := &metadata.RegisterHostUUIDRequest{
		HostUUID:   StringPtr(uhb.hostID),
		HostAddr:   StringPtr(uhb.hostAddr),
		HostName:   StringPtr(uhb.hostName),
		TtlSeconds: Int64Ptr(ttlSeconds),
	}

	for {
		select {
		case <-refreshTimeout:
			err := uhb.mClient.RegisterHostUUID(nil, req)
			if err != nil {
				uhb.logger.WithFields(bark.Fields{`hostID`: uhb.hostID, `hostName`: uhb.hostName, `hostAddr`: uhb.hostAddr, TagErr: err}).Error("HostIDHeartbeater: RegisterHostUUID failed")
			}
			refreshTimeout = time.After(hbInterval)
		case <-uhb.shutdownC:
			uhb.shutdownWG.Done()
			return
		}
	}
}
