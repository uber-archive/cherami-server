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

package retentionmgr

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

const numWorkersDefault = 1                      // default number of workers to use
const bufferedJobs = 65536                       // capacity of the jobs-channel
const defaultExtentDeleteDeferPeriod = time.Hour // wait an hour before deleting extent (after it is 'consumed')

// TODO:
// -- on failure, post a job that retries the failure condition
// -- use a separate worker pool that does the synchronous calls into storage

// exported types
type (
	// Options are the options used to initialize RetentionManager
	Options struct {
		RetentionInterval                time.Duration
		DLQRetentionInterval             time.Duration
		SingleCGVisibleExtentGracePeriod time.Duration
		ExtentDeleteDeferPeriod          time.Duration
		NumWorkers                       int
		LocalZone                        string
	}

	// RetentionManager context
	RetentionManager struct {
		*Options

		logger   bark.Logger
		m3Client metrics.Client

		metadata  metadataDep
		storehost storehostDep

		wg sync.WaitGroup

		jobsC chan *retentionJob
		stopC chan struct{}

		sync.Mutex
		running bool

		lastDLQRetentionRun time.Time
	}
)

type (
	// metadataDep interface that encapsulates the dependencies on metadata
	metadataDep interface {
		// calls to metadata
		GetDestinations() (destinations []*destinationInfo, err error)
		GetExtents(destID destinationID) (extents []*extentInfo, err error)
		GetConsumerGroups(destID destinationID) (consumerGroups []*consumerGroupInfo, err error)
		DeleteExtent(destID destinationID, extID extentID) (err error)
		GetExtentsForConsumerGroup(dstID destinationID, cgID consumerGroupID) (extIDs []extentID, err error)
		MarkExtentConsumed(destID destinationID, extID extentID) (err error)
		DeleteConsumerGroupUUID(destID destinationID, cgID consumerGroupID) (err error)
		DeleteDestination(destID destinationID) (err error)
		DeleteConsumerGroupExtent(destID destinationID, cgID consumerGroupID, extID extentID) (err error)
		GetAckLevel(destID destinationID, extID extentID, cgID consumerGroupID) (ackLevel int64, err error)
		GetExtentInfo(destID destinationID, extID extentID) (extInfo *extentInfo, err error)
	}

	// storehostDep interface that encapsulates the dependencies on storehost
	storehostDep interface {
		// calls to storehost(s)
		GetAddressFromTimestamp(storeID storehostID, extID extentID, timestamp int64) (addr int64, sealed bool, err error)
		PurgeMessages(storeID storehostID, extID extentID, retentionAddr int64) (doneAddr int64, err error)
	}
)

// local types
type (
	// define various types
	extentID        string
	consumerGroupID string
	destinationID   string
	storehostID     string

	destinationInfo struct {
		id            destinationID
		destType      shared.DestinationType
		status        shared.DestinationStatus
		extents       []*extentInfo
		softRetention int32 // in seconds
		hardRetention int32 // in seconds
		path          string
		isMultiZone   bool
	}

	extentInfo struct {
		id                 extentID
		status             shared.ExtentStatus
		statusUpdatedTime  time.Time
		storehosts         []storehostID
		singleCGVisibility consumerGroupID
		originZone         string
		kafkaPhantomExtent bool
		// destID  destinationID
		// dest    *destinationInfo
	}

	consumerGroupInfo struct {
		id     consumerGroupID
		status shared.ConsumerGroupStatus
		// destID destinationID
		// dest   *destinationInfo
	}

	retentionJob struct {
		runAt         time.Time
		dest          *destinationInfo
		ext           *extentInfo
		consumers     []*consumerGroupInfo
		err           error
		minAckAddr    int64 // min-ack for all consumers
		retentionAddr int64 // retention-address
		deleteExtent  bool  // extent should be deleted
	}
)

// New initializes context for a new instance of retention manager
func New(opts *Options, metadata metadata.TChanMetadataService, clientFactory common.ClientFactory, m3Client metrics.Client, logger bark.Logger) *RetentionManager {

	if opts.NumWorkers <= 0 {
		opts.NumWorkers = numWorkersDefault
	}

	if opts.ExtentDeleteDeferPeriod <= 0 {
		opts.ExtentDeleteDeferPeriod = defaultExtentDeleteDeferPeriod
	}

	logger = logger.WithField(common.TagModule, `retMgr`)

	return &RetentionManager{
		Options:             opts,
		logger:              logger,
		m3Client:            m3Client,
		metadata:            newMetadataDep(metadata, logger),
		storehost:           newStorehostDep(clientFactory, logger),
		lastDLQRetentionRun: time.Now().AddDate(0, 0, -1),
	}
}

// tNew takes in the metadata and storehost dependencies (that could potentially be mocked for testing)
func tNew(opts *Options, metadata metadataDep, storehost storehostDep, m3Client metrics.Client, logger bark.Logger) *RetentionManager {

	if opts.NumWorkers <= 0 {
		opts.NumWorkers = numWorkersDefault
	}

	logger = logger.WithField(common.TagModule, `retMgr`)

	return &RetentionManager{
		Options:             opts,
		logger:              logger,
		m3Client:            m3Client,
		metadata:            metadata,
		storehost:           storehost,
		lastDLQRetentionRun: time.Now().AddDate(0, 0, -1),
	}
}

// Start starts the various go-routines asynchronously
func (t *RetentionManager) Start() {

	t.Lock()
	defer t.Unlock()

	if !t.running {
		t.logger.WithFields(bark.Fields{
			`interval`:    t.RetentionInterval,
			`dlqinterval`: t.DLQRetentionInterval,
			`workers`:     t.NumWorkers,
		}).Info(`RetentionMgr starting`)

		t.jobsC = make(chan *retentionJob, bufferedJobs) // compute and buffer up to 64k jobs
		t.stopC = make(chan struct{})

		// start the workers that would listen on the 'jobsC' channel.
		for i := 0; i < t.NumWorkers; i++ {
			t.wg.Add(1)
			go t.retentionWorker(i, t.jobsC)
		}

		// go-routine #1: that schedules retention on a regular basis, that
		// posts 'jobs' to the jobs channel (jobsC) for the workers to process
		if t.RetentionInterval > 0 {
			t.wg.Add(1)
			go func() {
				defer t.wg.Done()
				defer close(t.jobsC) // close 'jobs' channel

				// wait a minute, before doing the first run
				timer := time.NewTimer(time.Minute)
				defer timer.Stop()

				for {
					// wait until 'RetentionInterval' or if we get a "stop"
					select {
					case <-timer.C: // wait for timer to fire

					case <-t.stopC:
						return
					}

					if !t.runRetention(t.jobsC) {
						return
					}

					// assert( t.RetentionInterval != 0 )
					timer.Reset(t.RetentionInterval)
				}
			}()
		}

		t.running = true
	}
}

// Stop stops the retention manager
func (t *RetentionManager) Stop() {

	t.Lock()
	defer t.Unlock()

	if t.running {

		close(t.stopC) // request stoppage
		t.wg.Wait()    // wait until it stops

		t.running = false

		t.logger.Info("RetentionMgr stopped")
	}
}

// Run does a 'one-shot' run of retention manager
func (t *RetentionManager) Run() {

	t.logger.WithFields(bark.Fields{
		`workers`: t.NumWorkers,
	}).Info(`RetentionMgr starting`)

	t.RetentionInterval = 0
	t.Start()
	t.runRetention(t.jobsC) // run retention (once)
	close(t.jobsC)          // there will be no more jobs
	t.wg.Wait()             // wait for completion

	t.logger.Info("RetentionMgr stopped")
}

func (t *RetentionManager) wait() {
	t.wg.Wait()
}

// runRetention finds the list of destinations, extents and consumer-groups and
// schedules a job, one per extent, to compute and enforce retention on storage.
func (t *RetentionManager) runRetention(jobsC chan<- *retentionJob) bool {

	t.logger.Info("runRetention: start")

	destList, err := t.metadata.GetDestinations()

	if err != nil {

		t.logger.WithField(common.TagErr, err).Error("GetDestinations failed")
		return true
	}

	// shuffle list of destinations (ie, randomize order each time)
	for i := len(destList); i > 0; i-- {
		r := rand.Intn(i)
		destList[i-1], destList[r] = destList[r], destList[i-1]
	}

	skipDLQ := t.Options.DLQRetentionInterval > 0 &&
		time.Now().Sub(t.lastDLQRetentionRun) < t.Options.DLQRetentionInterval

	// estimate the number of retention jobs, we'll be scheduling
	var totalJobs int64

	// populate each destination-info with extents and consumer-groups information
	for _, dest := range destList {

		if dest.status == shared.DestinationStatus_DELETED {
			continue
		}

		log := t.logger.WithField(common.TagDst, dest.id)

		var numCGs int

		// query consumer groups for the destination
		cgs, err := t.metadata.GetConsumerGroups(dest.id)

		if err != nil {

			log.WithField(common.TagErr, err).Error("GetConsumerGroups failed")
			numCGs = -1 // some non-zero value

		} else {

			// for each extent, compute retention cursor and convey to storage
			for _, cg := range cgs {

				log.WithFields(bark.Fields{
					common.TagCnsm: cg.id,
					`status`:       cg.status,
				}).Info("processing cg")

				if cg.status == shared.ConsumerGroupStatus_DELETED {
					continue // ignore deleted CGs
				}

				numCGs++ // count 'active' CGs

				if cg.status != shared.ConsumerGroupStatus_DELETING {
					continue
				}

				log.WithFields(bark.Fields{
					common.TagCnsm: cg.id,
					`status`:       cg.status,
				}).Info("cg in deleting state")

				// CG is in DELETING state; delete all cg-extents:

				var numCGExtents int

				extIDs, err := t.metadata.GetExtentsForConsumerGroup(dest.id, cg.id)

				if err != nil {
					log.WithFields(bark.Fields{
						common.TagCnsm: cg.id,
						common.TagErr:  err,
					}).Error("GetExtentsForConsumerGroup failed")
					continue
				}

				numCGExtents = len(extIDs)

				log.WithFields(bark.Fields{
					common.TagCnsm: cg.id,
					`numCGExtents`: numCGExtents,
				}).Info("deleting all consumer-group extents for CG")

				for _, extID := range extIDs {

					e := t.metadata.DeleteConsumerGroupExtent(dest.id, cg.id, extID)

					if e != nil {
						// 'EntityNotExistsError' indicates the CG-extent was already deleted
						if _, ok := e.(*shared.EntityNotExistsError); ok {
							e = nil
						}

						log.WithFields(bark.Fields{
							common.TagCnsm: cg.id,
							common.TagExt:  extID,
							common.TagErr:  e,
						}).Error("error deleting consumer-group extent")
					}

					if e == nil {
						numCGExtents--
					}
				}

				if numCGExtents == 0 {

					log.WithFields(bark.Fields{
						common.TagCnsm: cg.id,
					}).Info("deleting consumer-group (all cg-extents deleted)")

					if e := t.metadata.DeleteConsumerGroupUUID(dest.id, cg.id); e != nil {

						log.WithFields(bark.Fields{
							common.TagCnsm: cg.id,
							common.TagErr:  e,
						}).Error("error deleting consumer-group")
					}
				}
			}
		}

		// don't process extents for DLQ destinations every time
		if common.IsDLQDestinationPath(dest.path) && skipDLQ {
			continue
		}

		// query extents for the destination
		dest.extents, err = t.metadata.GetExtents(dest.id)

		var numExtents int

		if err != nil {

			log.WithField(common.TagErr, err).Error("GetExtents failed")
			numExtents = -1 // some non-zero value

		} else {

			// iterate through all extents and compute number of 'jobs' that would need to be created
			for j := range dest.extents {

				if dest.extents[j].status == shared.ExtentStatus_DELETED {
					continue // skip deleted extent
				}

				numExtents++ // count active extents

				if dest.extents[j].status == shared.ExtentStatus_CONSUMED &&
					time.Since(dest.extents[j].statusUpdatedTime) < t.ExtentDeleteDeferPeriod {

					continue // skip consumed extent within 'delete-defer-period'
				}

				totalJobs++
			}
		}

		if dest.status == shared.DestinationStatus_DELETING && numCGs == 0 && numExtents == 0 {

			log.Info("deleting destination: all CGs and extents deleted")

			// All extents have been deleted for this destination. Since the
			// deletion of an extent requires all consumer groups to have
			// either consumed or deleted, all state associated with this
			// destination has been cleaned at this point. Mark the destination
			// as DELETED. There will be no cleanup after this.
			t.metadata.DeleteDestination(dest.id)
		}
	}

	if t.Options.DLQRetentionInterval > 0 && !skipDLQ {
		t.lastDLQRetentionRun = time.Now()
	}

	// compute the time until when retention would run next to help pre-schedule the jobs
	// so that they are distributed more or less evenly during the retention-interval

	if totalJobs == 0 {
		t.logger.Info("runRetention: done (nothing to do)")
		return true
	}

	// spread out the jobs over the time until when retention would run next

	durationPerJob := time.Duration(int64(t.RetentionInterval) / totalJobs)

	scheduleAt := time.Now() // schedule first job 'now'

	// for every destination, for every extent, compute and enforce retention
	for _, dest := range destList {

		// check if we have been asked to stop
		select {
		case <-t.stopC:
			t.logger.Info("runRetention: stopped!")
			return false
		default:
			// continue to schedule next job
		}

		// skip deleted destinations
		if dest.status == shared.DestinationStatus_DELETED {
			t.logger.WithField(`destID`, dest.id).Info(`skipping deleted destination`)
			continue
		}

		t.logger.WithField(common.TagDst, dest.id).Info("scheduling retention jobs for dest")

		// for each extent, compute retention cursor and convey to storage
		for j := range dest.extents {

			if dest.extents[j].status == shared.ExtentStatus_DELETED {

				t.logger.WithFields(bark.Fields{
					common.TagDst: dest.id,
					common.TagExt: dest.extents[j].id,
				}).Info("skipping retention on deleted extent")
				continue
			}

			if dest.extents[j].status == shared.ExtentStatus_CONSUMED &&
				time.Since(dest.extents[j].statusUpdatedTime) < t.ExtentDeleteDeferPeriod {

				t.logger.WithFields(bark.Fields{
					common.TagDst: dest.id,
					common.TagExt: dest.extents[j].id,
				}).Info("skipping retention on consumed extent within 'delete-defer-period'")
				continue
			}

			t.logger.WithFields(bark.Fields{
				common.TagDst: dest.id,
				common.TagExt: dest.extents[j].id,
				`runAt`:       scheduleAt,
			}).Info("scheduling retention job")

			// create and post a job to process this extent on the destination
			jobsC <- &retentionJob{
				runAt:         scheduleAt,
				dest:          dest,
				ext:           dest.extents[j],
				retentionAddr: int64(store.ADDR_BEGIN),
			}

			// schedule next job after 'durationPerJob'
			scheduleAt = scheduleAt.Add(durationPerJob)
		}
	}

	t.logger.WithField(`totalJobs`, totalJobs).Info("runRetention: done")

	return true
}

func (t *RetentionManager) computeRetention(job *retentionJob, log bark.Logger) {

	dest := job.dest
	ext := job.ext

	if ext.status == shared.ExtentStatus_CONSUMED {

		// keep extent in "consumed" state until 'ExtentDeleteDeferPeriod' has
		// elapsed, only then "delete" the extent. this extra time helps ensure
		// that any consumer-group extents that were just placed, will also
		// move into "consumed" state, when they read from store.
		if time.Since(ext.statusUpdatedTime) >= t.ExtentDeleteDeferPeriod {
			job.retentionAddr = store.ADDR_SEAL // delete extent from the stores
			job.deleteExtent = true             // delete extent from metadata
		}
		return
	}

	if dest.status == shared.DestinationStatus_DELETING &&
		ext.status == shared.ExtentStatus_SEALED {

		// when the destination is being deleted and the extent is SEALED, then we
		// can short-circuit and decide to delete the whole extent immediately.

		job.retentionAddr = store.ADDR_SEAL
		job.deleteExtent = true
		return
	}

	// -- step 1: take a snapshot of the current time and compute retention timestamps -- //

	tNow := time.Now().UnixNano()

	hardRetentionTime := tNow - int64(dest.hardRetention)*int64(time.Second)
	softRetentionTime := tNow - int64(dest.softRetention)*int64(time.Second)

	if len(ext.singleCGVisibility) > 0 {
		hardRetentionTime -= int64(t.Options.SingleCGVisibleExtentGracePeriod) // Provide additional time for DLQ merged extents
		softRetentionTime = tNow                                               // DLQ merged extents are deleted as soon as they are consumed
		log.WithFields(bark.Fields{
			`hardRetentionTime`: hardRetentionTime,
			`softRetentionTime`: softRetentionTime,
		}).Info(`computeRetention: overriding retention times for DLQ merged extent`)
	}

	// -- step 2: compute hard-retention address, by querying storehosts -- //

	// find the 'hardRetentionAddr' that corresponds to the 'hardRetentionTime'

	// NB: when computing the hardRetentionAddr and softRetentionAddr, we find
	// the maximum address returned from querying the storehost for the timestamp.
	// this is because GetAddressFromTimestamp returns the address of the message
	// whose timestamp is _less than or equal_ to the given timestamp, so we can
	// safely use the 'max' of the addresses we got from querying the various
	// storehosts, it just means the other storehosts did not receive the particular
	// message. in case, even one of the storehosts
	// reports that the extent is sealed at this timestamp (ie, returns 'ADDR_SEAL'),
	// we just use that and stop querying, since that is, technically, the "furthest"
	// address possible.

	log.WithFields(bark.Fields{
		`hardRetentionTime_unixnano`: hardRetentionTime,
		`hardRetentionTime`:          time.Unix(0, hardRetentionTime),
	}).Info("computing hardRetentionAddr")

	var hardRetentionAddr = int64(store.ADDR_BEGIN)
	var hardRetentionConsumed bool

	for _, storeID := range ext.storehosts {

		getAddressStartTime := time.Now()
		addr, consumed, err := t.storehost.GetAddressFromTimestamp(storeID, ext.id, hardRetentionTime)
		t.m3Client.RecordTimer(metrics.RetentionMgrScope, metrics.ControllerGetAddressLatency, time.Since(getAddressStartTime))

		if err != nil {
			t.m3Client.IncCounter(metrics.RetentionMgrScope, metrics.ControllerGetAddressFailedCounter)

			log.WithFields(bark.Fields{
				common.TagStor:      storeID,
				`hardRetentionTime`: hardRetentionTime,
				common.TagErr:       err,
			}).Error(`computeRetention: hardRetention GetAddressFromTimestamp error`)
			continue
		}
		t.m3Client.IncCounter(metrics.RetentionMgrScope, metrics.ControllerGetAddressCompletedCounter)

		// find the max address and use that as the hardRetentionAddr
		if addr > hardRetentionAddr {
			hardRetentionAddr = addr
		}

		if consumed {
			hardRetentionConsumed = true
		}
	}

	log.WithFields(bark.Fields{
		`hardRetentionAddr`:      hardRetentionAddr,
		`hardRetentionAddr_time`: time.Unix(0, hardRetentionAddr),
		`hardRetentionConsumed`:  hardRetentionConsumed,
	}).Info("computed hardRetentionAddr")

	// -- step 3: compute soft-retention address -- //

	// find the 'softRetentionAddr' that corresponds to the 'softRetentionTime, by querying each
	// of the storehosts and finding the max address we can get. (see comments above on why)

	log.WithFields(bark.Fields{
		`softRetentionTime_unixnano`: softRetentionTime,
		`softRetentionTime`:          time.Unix(0, softRetentionTime),
	}).Info("computing softRetentionAddr")

	var softRetentionAddr = int64(store.ADDR_BEGIN)
	var softRetentionConsumed bool

	for _, storeID := range ext.storehosts {

		getAddressStartTime := time.Now()
		addr, consumed, err := t.storehost.GetAddressFromTimestamp(storeID, ext.id, softRetentionTime)
		t.m3Client.RecordTimer(metrics.RetentionMgrScope, metrics.ControllerGetAddressLatency, time.Since(getAddressStartTime))

		if err != nil {
			t.m3Client.IncCounter(metrics.RetentionMgrScope, metrics.ControllerGetAddressFailedCounter)

			log.WithFields(bark.Fields{
				common.TagStor:      storeID,
				`softRetentionTime`: softRetentionTime,
				common.TagErr:       err,
			}).Error(`computeRetention: softRetention GetAddressFromTimestamp error`)
			continue
		}
		t.m3Client.IncCounter(metrics.RetentionMgrScope, metrics.ControllerGetAddressCompletedCounter)

		// find the max address and use that as the softRetentionAddr
		if addr > softRetentionAddr {
			softRetentionAddr = addr
		}

		if consumed {
			softRetentionConsumed = true
		}
	}

	// If this is a multi_zone destination and local extent, disable soft retention -- and fall
	// back to using only hard-retention. We also move the extent to the "consumed" state only
	// based on hard-retention.  The reason is if soft retention is very short, we may
	// delete messages before remote zone has a chance to replicate the messages.
	// Long term solution should create a fake consumer for the remote zone.
	if dest.isMultiZone && !common.IsRemoteZoneExtent(ext.originZone, t.Options.LocalZone) {
		log.Info(`overridden: soft retention overridden for multi_zone extent`)
		softRetentionAddr = int64(store.ADDR_BEGIN)
		softRetentionConsumed = false
	}

	log.WithFields(bark.Fields{
		`softRetentionAddr`:      softRetentionAddr,
		`softRetentionAddr_time`: time.Unix(0, softRetentionAddr),
		`softRetentionConsumed`:  softRetentionConsumed,
	}).Info("computed softRetentionAddr")

	// -- step 4: compute minimum ack cursor by querying metadata -- //

	log.Info("computing minAckAddr")

	var minAckAddr = int64(store.ADDR_END)
	var allHaveConsumed = true // start by assuming this is all-consumed

	// skip 'minAckAddr' queries for kafka phantom extents
	if !ext.kafkaPhantomExtent {

		for _, cgInfo := range job.consumers {

			if cgInfo.status == shared.ConsumerGroupStatus_DELETED {
				// don't include deleted consumer groups in the
				// soft retention based delete calculation
				continue
			}

			// Skip non-matching CGs for single CG visible extents
			if len(ext.singleCGVisibility) > 0 {
				if cgInfo.id != ext.singleCGVisibility {
					continue
				}

				log.Info("calculating minAckAddr for DLQ merged extent")
			}

			ackAddr, err := t.metadata.GetAckLevel(dest.id, ext.id, cgInfo.id)

			if err != nil {
				// if we got an error, go ahead with 'ADDR_BEGIN'

				log.WithFields(bark.Fields{
					common.TagCnsmID: cgInfo.id,
					common.TagErr:    err,
				}).Error(`computeRetention: minAckAddr GetAckLevel failed`)

				minAckAddr = store.ADDR_BEGIN
				allHaveConsumed = false
				break
			}

			// check if all CGs have consumed this extent
			if ackAddr != store.ADDR_SEAL {
				allHaveConsumed = false
			}

			// update minAckAddr, if ackAddr is less than the current value
			if (minAckAddr == store.ADDR_END) ||
				(minAckAddr == store.ADDR_SEAL) || // -> consumers we have seen so far have completely consumed this extent
				(ackAddr != store.ADDR_SEAL && ackAddr < minAckAddr) {

				minAckAddr = ackAddr
			}
		}
	}

	// if we were unable to find any consumer groups, set minAckAddr to
	// ADDR_BEGIN, effectively disabling soft-retention. that said, hard
	// retention could still be enforced.
	if minAckAddr == store.ADDR_END {

		log.Info("could not compute ackLevel, using 'ADDR_BEGIN'")
		minAckAddr = store.ADDR_BEGIN
		allHaveConsumed = false
	}

	job.minAckAddr = minAckAddr // remember the minAckAddr for doing checks later

	log.WithFields(bark.Fields{
		`minAckAddr`:      minAckAddr,
		`minAckAddr_time`: time.Unix(0, minAckAddr),
	}).Info("computed minAckAddr")

	// -- step 5: compute retention address -- //

	//** retentionAddr = max( hardRetentionAddr, min( softRetentionAddr, minAckAddr ) ) **//

	if softRetentionAddr == store.ADDR_SEAL || (minAckAddr != store.ADDR_SEAL && minAckAddr < softRetentionAddr) {
		softRetentionAddr = minAckAddr
	}

	if softRetentionAddr == store.ADDR_SEAL || (hardRetentionAddr != store.ADDR_SEAL && softRetentionAddr > hardRetentionAddr) {
		job.retentionAddr = softRetentionAddr
	} else {
		job.retentionAddr = hardRetentionAddr
	}

	log.WithFields(bark.Fields{
		`hardRetentionAddr`: hardRetentionAddr,
		`softRetentionAddr`: softRetentionAddr,
		`minAckAddr`:        minAckAddr,
		`retentionAddr`:     job.retentionAddr,
	}).Info("computed retentionAddr")

	// -- step 6: check to see if the extent status can be updated to 'consumed' -- //

	// move the extent to 'consumed' if the extent is "sealed" _and_ either:
	// A.
	// 	1. the extent was fully consumed by all of the consumer groups
	// 	2. and, a period of 'soft retention period' has passed (in other
	//         words, a consumer that is consuming along the soft retention
	//         time has "consumed" the extent)
	// B. or, the hard-retention has reached the end of the sealed extent,
	// 	in which case we will force the extent to be "consumed"
	// C. or, this is a kafka 'phantom' extent, which is sealed only when
	//      the destination is deleted.
	// NB: if there was an error querying either the hard/soft retention addresses,
	// {soft,hard}RetentionConsumed would be set to 'false'; if there was an error
	// querying ack-addr, then allHaveConsumed will be false. therefore errors in
	// either of the conditions would cause the extent to *not* be moved to the
	// CONSUMED state, and would cause it to be retried on the next iteration.
	if (ext.status == shared.ExtentStatus_SEALED) &&
		((allHaveConsumed && softRetentionConsumed) ||
			hardRetentionConsumed ||
			ext.kafkaPhantomExtent) {

		log.WithFields(bark.Fields{
			`retentionAddr`:         job.retentionAddr,
			`extent-status`:         ext.status,
			`minAckAddr`:            minAckAddr,
			`softRetentionConsumed`: softRetentionConsumed,
			`hardRetentionConsumed`: hardRetentionConsumed,
			`kafkaPhantomExtent`:    ext.kafkaPhantomExtent,
		}).Info("computeRetention: marking extent consumed")

		e := t.metadata.MarkExtentConsumed(dest.id, ext.id)

		if e != nil {
			log.WithField(common.TagErr, job.err).Error("computeRetention: error marking extent consumed")
		}
	}
}

// retentionWorker picks up a 'retention job' for an extent and computes the 'retention address' (before which
// all messages need to be purged) and calls into all the storehosts to actually do the deletion.
func (t *RetentionManager) retentionWorker(id int, jobsC <-chan *retentionJob) {

	defer t.wg.Done()

	t.logger.WithField(`worker`, id).Info("retentionWorker: started")

workerLoop:
	for job := range jobsC {

		dest := job.dest
		ext := job.ext

		log := t.logger.WithFields(bark.Fields{
			common.TagDst: string(dest.id),
			common.TagExt: string(ext.id),
			`worker`:      id,
		})

		log.WithField(`runAt`, job.runAt).Info("retentionWorker: picked up job; waiting until runAt")

		// run a timer to wake us up when this job is ready to be scheduled
		// NB: if the timer duration is negative, the timer fires immediately
		timer := time.NewTimer(job.runAt.Sub(time.Now()))

		select {
		case <-timer.C: // job-scheduler timer fired
			// the job is ready to be run; continue ..

		case <-t.stopC: // we have been asked to stop
			log.Info("retentionWorker: retention worker stop signal")
			break workerLoop
		}

		timer.Stop()

		log.WithFields(bark.Fields{
			`softRetention`: dest.softRetention,
			`hardRetention`: dest.hardRetention,
		}).Info("retentionWorker: starting job")

		t.m3Client.IncCounter(metrics.RetentionMgrScope, metrics.ControllerRetentionJobStartCounter)
		jobStartTime := time.Now()

		// refresh the extent information, since it could potentially be stale
		var err error
		job.ext, err = t.metadata.GetExtentInfo(job.dest.id, job.ext.id)

		if err != nil {
			job.retentionAddr = store.ADDR_BEGIN // skip this extent
			job.err = err
			log.WithField(common.TagErr, err).Error(`retentionWorker: GetExtentInfo failed; skipping extent`)
			continue
		}

		// get consumer groups for the destination
		job.consumers, _ = t.metadata.GetConsumerGroups(dest.id)

		if t.computeRetention(job, log); job.retentionAddr != store.ADDR_BEGIN {

			log.WithFields(bark.Fields{
				`retentionAddr`: job.retentionAddr,
				`deleteExtent`:  job.deleteExtent,
			}).Info(`retentionWorker: computeRetention`)

			// -- step 7: persist computed address into metadata --
			// TODO: do this so storehosts can asynchronously query retention address and purge //

			// -- step 8: send out command to storage nodes to purge messages until the retention address -- //

			for _, storehost := range ext.storehosts {

				// TODO: create a separate worker pool to do this, since this is potentially the
				// part of the processing that might take the most time.
				t.m3Client.IncCounter(metrics.RetentionMgrScope, metrics.ControllerPurgeMessagesRequestCounter)

				purgeStartTime := time.Now()
				addr, e := t.storehost.PurgeMessages(storehost, ext.id, job.retentionAddr)
				t.m3Client.RecordTimer(metrics.RetentionMgrScope, metrics.ControllerPurgeMessagesLatency, time.Since(purgeStartTime))

				log.WithFields(bark.Fields{
					common.TagStor: storehost,
					`purgeAddr`:    job.retentionAddr,
					`doneAddr`:     addr,
					common.TagErr:  e,
				}).Info(`retentionWorker: PurgeMessages output`)

				if e != nil && job.deleteExtent {
					// FIXME: if the failure was because the extent was already deleted,
					// treat it as a "success". Or, change storehost to return success in
					// if it could not find the extent. For now, assume these errors are because
					// the extent was missing.
					// job.deleteExtent = false
					job.err = e
				}
			}

			// -- step 9: check and move the extent to "deleted" state -- //

			// if we were able to successfully purge from all storehosts, move extent to "deleted" state;
			// this would prevent retention manager from picking this extent up again.
			if job.deleteExtent {

				var e error

			iterateConsumers:
				for _, cgInfo := range job.consumers {

					log.WithField(common.TagCnsm, cgInfo.id).Info("retentionWorker: mark consumer-group extent deleted")

					e = t.metadata.DeleteConsumerGroupExtent(dest.id, cgInfo.id, ext.id)

					if e != nil {

						// if we got an 'EntityNotExistsError' here, it is very likely because
						// a consumer never connected to this particular extent, which is not
						// abnormal -- so just ignore the error and continue
						if _, ok := e.(*shared.EntityNotExistsError); ok {
							e = nil
							continue iterateConsumers
						}

						job.err = e

						log.WithFields(bark.Fields{
							common.TagCnsm: cgInfo.id,
							common.TagErr:  e,
						}).Error("retentionWorker: error marking consumer-group extent deleted")
					}
				}

				if e == nil {

					// TODO: in the current model, where retention manager talks to the
					// storehosts synchronously, we need to get successful acks from all
					// storehosts before being able to move the extent to "deleted" state,
					// thereby avoiding picking this up for retention next time, etc. when
					// we move to the model, where retention manager only marks the metadata
					// on the retention status (and storehost polls the metadata to query
					// it, etc), we could actually move the extent to "deleted" state in the
					// metadata even if we don't hear back from the storehosts.

					log.Info("retentionWorker: marking extent deleted")
					e = t.metadata.DeleteExtent(dest.id, ext.id)

					if e != nil {
						job.err = e
						log.WithField(common.TagErr, job.err).Error("retentionWorker: error marking extent deleted")
					}
				}
			}

			// check and log the case when a consumer is potentially lagging behind and we may be deleting unconsumed messages
			if job.minAckAddr != store.ADDR_END && job.minAckAddr != store.ADDR_SEAL &&
				(job.retentionAddr == store.ADDR_END || job.retentionAddr == store.ADDR_SEAL ||
					job.deleteExtent || job.minAckAddr < job.retentionAddr) {

				log.WithFields(bark.Fields{
					`minAckAddr`:    job.minAckAddr,
					`retentionAddr`: job.retentionAddr,
					`deleteExtent`:  job.deleteExtent,
				}).Info(`retentionWorker: potentially purged unconsumed messages! (minAckAddr < retentionAddr)`)
			}

		} else {

			log.Info("retentionWorker: retentionAddr == ADDR_BEGIN; nothing to do")
		}

		if job.err != nil {
			log.WithField(common.TagErr, job.err).Error("retentionWorker: retention job failed")
			t.m3Client.IncCounter(metrics.RetentionMgrScope, metrics.ControllerRetentionJobFailedCounter)
			// FIXME: retry failed jobs?
		} else {
			log.Info("retentionWorker: retention job completed")
			t.m3Client.IncCounter(metrics.RetentionMgrScope, metrics.ControllerRetentionJobCompletedCounter)
		}

		t.m3Client.RecordTimer(metrics.RetentionMgrScope, metrics.ControllerRetentionJobDuration, time.Since(jobStartTime))
	}

	// we were stopped; cancel all jobs
	countCancelled := int64(0)
	for job := range jobsC {
		job.err = fmt.Errorf("cancelled")

		t.logger.WithFields(bark.Fields{
			`worker`:      id,
			common.TagDst: string(job.dest.id),
			common.TagExt: string(job.ext.id),
		}).Info("retention job cancelled")

		countCancelled++
	}

	t.m3Client.AddCounter(metrics.RetentionMgrScope, metrics.ControllerRetentionJobCancelledCounter, countCancelled)

	t.logger.WithField(`wworker`, id).Info("retentionWorker: done")
}
