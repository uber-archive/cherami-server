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

package storehost

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

type (
	// ReplicationJobRunner periodically queries metadata and
	// start replication job for extents if there's no existing job running
	// The runner is needed to handle error cases like store restart, etc
	// Eventually it should be replaced with a more general purpose job framework
	ReplicationJobRunner interface {
		common.Daemon
	}

	// replicationJobRunner is an implementation of ReplicationJobRunner.
	replicationJobRunner struct {
		storeHost   *StoreHost
		storeID     string
		currentZone string
		failedJobs  map[string]int

		mClient  metadata.TChanMetadataService
		logger   bark.Logger
		m3Client metrics.Client

		closeChannel chan struct{}

		ticker  *time.Ticker
		running int64
	}
)

const (
	// runInterval determines how often the runner will run
	runInterval = time.Duration(10 * time.Minute)

	// timeout to wait for rpm bootstrap
	rpmBootstrapTimeout = time.Duration(2 * time.Minute)
)

// NewReplicationJobRunner returns an instance of ReplicationJobRunner
func NewReplicationJobRunner(mClient metadata.TChanMetadataService, store *StoreHost, logger bark.Logger, m3Client metrics.Client) ReplicationJobRunner {
	return &replicationJobRunner{
		storeHost:  store,
		storeID:    store.GetHostUUID(),
		failedJobs: make(map[string]int),
		mClient:    mClient,
		logger:     logger,
		m3Client:   m3Client,
		ticker:     time.NewTicker(runInterval),
		running:    0,
	}
}

func (runner *replicationJobRunner) Start() {
	runner.logger.Info("ReplicationJobRunner: started")

	runner.currentZone, _ = common.GetLocalClusterInfo(strings.ToLower(runner.storeHost.SCommon.GetConfig().GetDeploymentName()))
	runner.closeChannel = make(chan struct{})

	// replication job needs rpm to be bootstrapped first (in order to resolve other store host or replicator)
	select {
	case <-runner.closeChannel:
		runner.logger.Error("ReplicationJobRunner: runner stopped before rpm is bootstrapped")
		return
	case <-runner.storeHost.SCommon.GetRingpopMonitor().GetBootstrappedChannel():
	case <-time.After(rpmBootstrapTimeout):
		// rpm still not bootstrapped after time out. Start the jobs anyway (won't hurt)
		runner.logger.Warn("ReplicationJobRunner: rpm not bootstrapped after timeout")
	}

	go runner.run()
	go runner.houseKeep()
}

func (runner *replicationJobRunner) Stop() {
	close(runner.closeChannel)

	runner.logger.Info("ReplicationJobRunner: stopped")
}

func (runner *replicationJobRunner) run() {
	if !atomic.CompareAndSwapInt64(&runner.running, 0, 1) {
		runner.logger.Warn("Prev run is still ongoing...")
		return
	}

	defer atomic.StoreInt64(&runner.running, 0)

	runner.logger.Info("replication run started")

	listReq := &metadata.ListStoreExtentsStatsRequest{
		StoreUUID:         common.StringPtr(runner.storeID),
		ReplicationStatus: common.InternalExtentReplicaReplicationStatusTypePtr(shared.ExtentReplicaReplicationStatus_PENDING),
	}

	res, err := runner.mClient.ListStoreExtentsStats(nil, listReq)
	if err != nil {
		runner.logger.WithFields(bark.Fields{
			common.TagStor: common.FmtStor(runner.storeID),
		}).Error(`Query metadata failed`)
		return
	}

	totalExtents := len(res.GetExtentStatsList())
	totalRemoteExtents := 0
	openedForReplication := 0
	primaryExtents := 0
	secondaryExtents := 0
	jobsStarted := 0
	currentFailedJobs := make(map[string]struct{})
	for _, extentStats := range res.GetExtentStatsList() {
		extentID := extentStats.GetExtent().GetExtentUUID()
		destID := extentStats.GetExtent().GetDestinationUUID()
		zone := extentStats.GetExtent().GetOriginZone()

		if len(zone) == 0 || zone == runner.currentZone {
			continue
		}

		totalRemoteExtents++

		if runner.storeHost.xMgr.IsExtentOpenedForReplication(extentID) {
			openedForReplication++
			continue
		}

		storeIds := extentStats.GetExtent().GetStoreUUIDs()
		if len(storeIds) == 0 {
			runner.logger.WithFields(bark.Fields{
				common.TagDst:  common.FmtDst(destID),
				common.TagExt:  common.FmtExt(extentID),
				common.TagStor: common.FmtStor(runner.storeID),
			}).Error(`No store Ids for extent from metadata`)
			currentFailedJobs[extentID] = struct{}{}
			continue
		}

		// If the primary store field doesn't exist, the first store will be treated as primary store
		primaryStore := extentStats.GetExtent().GetRemoteExtentPrimaryStore()
		if len(primaryStore) == 0 {
			sort.Strings(storeIds)
			primaryStore = storeIds[0]
		}

		if primaryStore == runner.storeID {
			primaryExtents++

			req := store.NewRemoteReplicateExtentRequest()
			req.DestinationUUID = common.StringPtr(destID)
			req.ExtentUUID = common.StringPtr(extentID)

			err = runner.storeHost.RemoteReplicateExtent(nil, req)
			if err != nil {
				runner.logger.WithFields(bark.Fields{
					common.TagDst:  common.FmtDst(destID),
					common.TagExt:  common.FmtExt(extentID),
					common.TagStor: common.FmtStor(runner.storeID),
					common.TagErr:  err,
				}).Error(`Remote replication for extent failed`)
				currentFailedJobs[extentID] = struct{}{}
				continue
			}
		} else {
			idFound := false
			for _, storeID := range storeIds {
				if storeID == runner.storeID {
					idFound = true
					break
				}
			}

			if !idFound {
				runner.logger.WithFields(bark.Fields{
					common.TagDst:  common.FmtDst(destID),
					common.TagExt:  common.FmtExt(extentID),
					common.TagStor: common.FmtStor(runner.storeID),
				}).Error(`No matching store id found in metadata`)
				currentFailedJobs[extentID] = struct{}{}
				continue
			}

			secondaryExtents++

			req := store.NewReplicateExtentRequest()
			req.DestinationUUID = common.StringPtr(destID)
			req.ExtentUUID = common.StringPtr(extentID)
			req.StoreUUID = common.StringPtr(primaryStore)
			err = runner.storeHost.ReplicateExtent(nil, req)
			if err != nil {
				runner.logger.WithFields(bark.Fields{
					common.TagDst:  common.FmtDst(destID),
					common.TagExt:  common.FmtExt(extentID),
					common.TagStor: common.FmtStor(runner.storeID),
					common.TagErr:  err,
				}).Error(`Rereplication for extent failed`)
				currentFailedJobs[extentID] = struct{}{}
				continue
			}
		}

		runner.logger.WithFields(bark.Fields{
			common.TagDst:  common.FmtDst(destID),
			common.TagExt:  common.FmtExt(extentID),
			common.TagStor: common.FmtStor(runner.storeID),
		}).Info(`replication for extent started`)

		jobsStarted++
	}

	var maxConsecutiveFailures int
	for existingFailedJob, failedTimes := range runner.failedJobs {
		if _, ok := currentFailedJobs[existingFailedJob]; !ok {
			delete(runner.failedJobs, existingFailedJob)
		} else {
			newFailedTimes := failedTimes + 1
			runner.failedJobs[existingFailedJob] = newFailedTimes
			if newFailedTimes > maxConsecutiveFailures {
				maxConsecutiveFailures = newFailedTimes
			}

			runner.logger.WithFields(bark.Fields{
				common.TagExt:  common.FmtExt(existingFailedJob),
				`failed times`: newFailedTimes,
			}).Info(`replication job failed for at least twice`)
		}
	}
	for currentFailedJob := range currentFailedJobs {
		if _, ok := runner.failedJobs[currentFailedJob]; !ok {
			runner.failedJobs[currentFailedJob] = 1
		}
	}

	runner.m3Client.UpdateGauge(metrics.ReplicateExtentScope, metrics.StorageReplicationJobCurrentFailures, int64(len(currentFailedJobs)))
	runner.m3Client.UpdateGauge(metrics.ReplicateExtentScope, metrics.StorageReplicationJobMaxConsecutiveFailures, int64(maxConsecutiveFailures))
	runner.m3Client.UpdateGauge(metrics.ReplicateExtentScope, metrics.StorageReplicationJobCurrentSuccess, int64(jobsStarted))
	runner.m3Client.UpdateGauge(metrics.ReplicateExtentScope, metrics.StorageReplicationJobRun, 1)

	runner.logger.WithFields(bark.Fields{
		`stats`: fmt.Sprintf(`total extents: %v, remote extents:%v, opened for replication: %v, primary: %v, secondary: %v, failed: %v, success: %v`,
			totalExtents, totalRemoteExtents, openedForReplication, primaryExtents, secondaryExtents, len(currentFailedJobs), jobsStarted),
		common.TagStor: common.FmtStor(runner.storeID),
	}).Info(`replication run finished`)
}

func (runner *replicationJobRunner) houseKeep() {
	for {
		select {
		case <-runner.ticker.C:
			go runner.run()
		case <-runner.closeChannel:
			return
		}
	}
}
