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
	"math"
	"sync"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/storage"
	storeStream "github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

const replicateDebug = false

type (
	// ReplicationManager holds the context for replication manager
	ReplicationManager struct {
		xMgr     *ExtentManager
		m3Client metrics.Client
		mClient  metadata.TChanMetadataService
		log      bark.Logger
		hostID   string
		wsConn   common.WSConnector
		credMgr  map[string]CreditMgr // credit manager per replica
		sync.RWMutex
	}

	// replicationArgs are the arguments passed to replicateExtent
	replicationArgs struct {
		extentID       uuid.UUID
		destID         uuid.UUID
		destType       cherami.DestinationType
		mode           Mode      // based on destType
		sourceHostID   uuid.UUID // could be another store or replicator
		sourceHostPort string    // source host:port
	}

	// ReplicationJob holds the context for a replication job
	ReplicationJob struct {
		jobType          ReplicationJobType
		*replicationArgs // args passed to replicateExtent

		ext     *ExtentObj // handle to extent store
		stream  storeStream.BStoreOpenReadStreamOutCall
		replMgr *ReplicationManager

		cred     CreditLine
		credC    chan int32
		log      bark.Logger
		m3Client metrics.Client
		wg       sync.WaitGroup
		stopC    chan struct{}
		doneC    chan struct{}
		err      error
	}

	// ReplicationJobType is the job type
	ReplicationJobType int
)

// thresholds that determine when credits should be sent
const (
	creditsUpdateInterval  = 5 * time.Second
	creditsUpdateThreshold = 1000
	defaultCreditsPerHost  = 100000
)

const (
	_ = iota

	// JobTypeReReplication is the job type for re-replication. The source of re-replication is another store
	JobTypeReReplication

	// JobTypeRemoteReplication is the job type for remote replication. The source of remote replication is
	// replicator(which will receive message from remote replicator)
	JobTypeRemoteReplication
)

// NewReplicationManager initializes and returns a new 'replication manager'
func NewReplicationManager(
	xMgr *ExtentManager,
	m3Client metrics.Client,
	mClient metadata.TChanMetadataService,
	log bark.Logger,
	hostID string,
	wsConn common.WSConnector,
) *ReplicationManager {

	return &ReplicationManager{
		xMgr:     xMgr,
		log:      log,
		m3Client: m3Client,
		mClient:  mClient,
		hostID:   hostID,
		wsConn:   wsConn,
		credMgr:  make(map[string]CreditMgr),
	}
}

// NewJob schedules starts a new replication job
func (t *ReplicationManager) NewJob(jobType ReplicationJobType, args *replicationArgs, log bark.Logger) *ReplicationJob {

	// share the credit-manager across clients to the same 'sourceReplica'
	t.Lock()
	credMgr, ok := t.credMgr[string(args.sourceHostID)]
	if !ok {
		credMgr = NewCreditMgr(defaultCreditsPerHost)
		t.credMgr[string(args.sourceHostID)] = credMgr
	}
	t.Unlock()

	return &ReplicationJob{
		jobType:         jobType,
		replicationArgs: args,
		doneC:           make(chan struct{}),
		stopC:           make(chan struct{}),
		log:             log,
		m3Client:        t.m3Client,
		replMgr:         t,
		cred:            credMgr.NewCreditLine(),
		credC:           make(chan int32, 1),
	} // create a new replication job
}

func (t *ReplicationJob) cleanup() {

	if t.cred != nil {
		t.cred.Close()
		t.cred = nil
	}

	if t.ext != nil {
		t.ext.storeSync() // sync store messages
		t.ext.Close()     // cleanup/close extent (unlistens automatically)
		t.ext = nil
	}

	if t.doneC != nil {
		close(t.doneC)
	}
}

// Start starts the replication job
func (t *ReplicationJob) Start() error {

	log := t.log // get "local" logger (that already contains extent info)

	var err error

	t.ext, err = t.replMgr.xMgr.OpenExtent(t.extentID, t.mode, OpenIntentReplicateExtent)

	if err != nil {

		log.WithFields(bark.Fields{
			`context`:     "OpenExtent failed",
			common.TagErr: err,
			`replMsgs`:    0,
		}).Error(`ReplicationJob done`)

		t.err = fmt.Errorf("OpenExtent failed: %v", err)
		t.cleanup()
		return t.err
	}

	// if we already have this extent and it is sealed, then no-op this request
	if t.ext.getSealSeqNum() != math.MaxInt64 {

		log.WithFields(bark.Fields{
			`context`:  "extent already sealed",
			`replMsgs`: 0,
		}).Info(`ReplicationJob done`)

		// now try to mark the replication status as 'done' so that the job won't be scheduled again
		t.updateReplicationStatus(t.extentID.String(), shared.ExtentReplicaReplicationStatus_DONE)

		t.err = nil
		t.cleanup()
		return t.err
	}

	var beginKey storage.Key // key to start replicating from

	if t.ext.getLastSeqNum() != SeqnumInvalid {

		// if we already have the extent, it's likely because we failed during
		// replication. in order to resume replication, find the address of the
		// last message in the extent and resume reading/replicating from there.
		_, beginKey, err = t.ext.storeSeekLast()

		if err != nil {
			log.WithFields(bark.Fields{
				`context`:     "SeekLast failed",
				common.TagErr: err,
				`replMsgs`:    0,
			}).Error(`ReplicationJob done`)

			t.err = fmt.Errorf("SeekLast error: %v", err)
			t.cleanup()
			return t.err
		}
	}

	req := &store.OpenReadStreamRequest{
		DestinationUUID:   common.StringPtr(t.destID.String()),
		DestinationType:   cherami.DestinationTypePtr(t.destType),
		ExtentUUID:        common.StringPtr(t.extentID.String()),
		ConsumerGroupUUID: common.StringPtr(t.sourceHostID.String()), // using host-id as consumer group id
		Address:           common.Int64Ptr(int64(beginKey)),
		Inclusive:         common.BoolPtr(false), // inclusive == false -> start _after_ beginKey
	}

	if t.jobType == JobTypeReReplication {
		t.stream, err = t.replMgr.wsConn.OpenReadStream(t.sourceHostPort, common.GetOpenReadStreamRequestHTTPHeaders(req))
		if err != nil {
			log.WithFields(bark.Fields{
				`context`:     "OpenReadStream failed",
				common.TagErr: err,
				`replMsgs`:    0,
			}).Error(`ReplicationJob done`)

			t.err = fmt.Errorf("openReadStream err: %v", err)
			t.cleanup()
			return t.err
		}
	} else {
		request := &common.OpenReplicationRemoteReadStreamRequest{OpenReadStreamRequest: *req}
		t.stream, err = t.replMgr.wsConn.OpenReplicationRemoteReadStream(t.sourceHostPort, common.GetOpenReplicationRemoteReadStreamRequestHTTPHeaders(request))
		if err != nil {
			log.WithFields(bark.Fields{
				`context`:     "OpenReplicationRemoteReadStream failed",
				common.TagErr: err,
				`replMsgs`:    0,
			}).Error(`ReplicationJob done`)

			t.err = fmt.Errorf("Find host for replicator err: %v", err)
			t.cleanup()
			return t.err
		}
	}

	t.wg.Add(2) // for the go-routines below

	go t.sendPump()
	go t.replicationPump()

	return nil
}

// Stop stops the replication job
func (t *ReplicationJob) Stop() {
	close(t.stopC) // should initiate the chain of closures
	t.wg.Wait()    // wait for completion
}

// Done returns a channel that is signaled when the replication job completes
func (t *ReplicationJob) Done() <-chan struct{} {
	return t.doneC
}

// Error returns the error from the replication job
func (t *ReplicationJob) Error() error {
	return t.err
}

func (t *ReplicationJob) replicationPump() {

	log := t.log // get "local" logger (that already contains extent info)
	x := t.ext   // get a local reference to the extent-object
	stream := t.stream
	credLine := t.cred

	log.Debug("replicationPump: started")

	var replMsgs int64             // total messages written
	var lastMessageKey storage.Key // track the previous message's key to ensure that it strictly increasing

	var credits int32
	lastCreditUpdate := time.Now()

	var done bool
	var firstMsg = (t.ext.getLastSeqNum() == SeqnumInvalid)

pump:
	for {
		// see if we can get some some credits
		credits += credLine.Borrow()

		// check if we need to update credits
		if credits >= creditsUpdateThreshold ||
			(time.Since(lastCreditUpdate) >= creditsUpdateInterval && credits > 0) {

			// try updating credits non-blockingly
			select {
			case t.credC <- credits:
				credits = 0
				lastCreditUpdate = time.Now()
			default:
				// channel full; so just continue ..
			}
		}

		// read (blocking) from websocket-stream
		readMsg, err := stream.Read()

		if err != nil {

			log.WithFields(bark.Fields{
				`reason`:      `stream closed`,
				`replMsgs`:    replMsgs,
				common.TagErr: err,
			}).Info(`replicationPump done`)
			t.err = fmt.Errorf(`stopped`)
			break pump

		} else if done {
			// we came back here to give a chance for the connection to gracefully
			// shutdown, with a read-error; if it did not, its likely we saw a local
			// error (as opposed to a server-side error), so get out anyway.
			break pump
		}

		switch readMsg.GetType() {
		case store.ReadMessageContentType_MESSAGE:

			// continue below to write the message
			break

		case store.ReadMessageContentType_SEALED:
			// write a new SealExtentKey
			sealSeqNum := readMsg.GetSealed().GetSequenceNumber()

			sealExtentKey := x.constructSealExtentKey(sealSeqNum)

			_, err = x.storePut(sealExtentKey, []byte{})

			if err != nil {
				log.WithFields(bark.Fields{
					`reason`:      `error writing seal-key`,
					common.TagErr: err,
					"replMsgs":    replMsgs,
				}).Error(`replicationPump done`)

				t.m3Client.IncCounter(metrics.ReplicateExtentScope, metrics.StorageFailures)
				t.err = fmt.Errorf("%v failed writing seal-key: %v", t.extentID, err)
				done = true
				continue pump
			}

			x.setSealSeqNum(sealSeqNum) // save sealSeqNum

			log.WithFields(bark.Fields{
				`seal-seqnum`: sealSeqNum,
			}).Info(`replicationPump: extent sealed`)

			// now try to mark the replication status as 'done' so that the job won't be scheduled again
			t.updateReplicationStatus(t.extentID.String(), shared.ExtentReplicaReplicationStatus_DONE)

			t.err = nil // no error
			done = true
			continue pump

		case store.ReadMessageContentType_ERROR:
			t.err = readMsg.GetError()
			log.WithFields(bark.Fields{
				`reason`:      `error message (ReadMessageContentType_ERROR)`,
				common.TagErr: t.err,
				`replMsgs`:    replMsgs,
			}).Error(`replicationPump done`)
			done = true
			continue pump

		default:
			t.err = fmt.Errorf("unknown ReadMessageContentType: %v", readMsg.GetType())
			log.WithFields(bark.Fields{
				common.TagErr:  t.err,
				`readmsg-type`: readMsg.GetType(),
				`replMsgs`:     replMsgs,
			}).Error(`unknown ReadMessageContentType`)
			done = true
			continue pump
		}

		msg := readMsg.GetMessage()
		msgSeqNum := msg.GetMessage().GetSequenceNumber()
		visibilityTime := x.messageVisibilityTime(msg.GetMessage())

		if replicateDebug {
			log.WithFields(bark.Fields{ // #perfdisable
				common.TagSeq: msgSeqNum,                                                // #perfdisable
				`addr`:        msg.GetAddress(),                                         // #perfdisable
				`enq`:         msg.GetMessage().GetEnqueueTimeUtc(),                     // #perfdisable
				`delay`:       msg.GetMessage().GetPayload().GetDelayMessageInSeconds(), // #perfdisable
				`len`:         len(msg.GetMessage().GetPayload().GetData()),             // #perfdisable
			}).Debug("replicationPump: recv msg") // #perfdisable
		}

		// FIXME: watermark messages require special handling? (if msg.FullyReplicatedWatermark != nil && msg.Payload == nil)

		var key storage.Key   // key to write this messages against
		var val storage.Value // message converted to byte-array

		key = storage.Key(msg.GetAddress()) // the address of the message is the 'key' (that was computed on the source replica)

		// ensure that the keys are strictly increasing; this also
		// ensures that we never overwrite an existing message
		if key <= lastMessageKey {

			// FIXME: add metric to help alert this case
			log.WithFields(bark.Fields{
				`reason`:   `msg key less than previous`,
				`key`:      key,
				`prev-key`: lastMessageKey,
				`replMsgs`: replMsgs,
			}).Error("replicationPump done")

			t.m3Client.IncCounter(metrics.ReplicateExtentScope, metrics.StorageFailures)
			t.err = fmt.Errorf("%v msg key=%x less than last-key=%x", t.extentID, key, lastMessageKey)
			done = true
			continue pump
		}

		lastMessageKey = key

		// serialize message into byte-array for storage
		val, err = serializeMessage(msg.GetMessage())

		if err != nil { // we should never really see this error!

			// log.WithFields(log.Fields{
			// 	common.TagErr:                                        err,
			// 	`key`:                                                key, `msg`: msg}).
			// 	Fatal(`replicationPump: serializeMessage msg failed`) // FIXME: Don't Panic.

			log.WithFields(bark.Fields{
				`reason`:      "serialize-message error",
				common.TagErr: err,
				"replMsgs":    replMsgs,
			}).Error("replicationPump done")

			t.m3Client.IncCounter(metrics.ReplicateExtentScope, metrics.StorageFailures)
			t.err = fmt.Errorf("%v error serializing message (seqnum=%x key=%x): %v", t.extentID, msgSeqNum, key, err)
			done = true
			continue pump
		}

		if replicateDebug {
			log.WithFields(bark.Fields{ // #perfdisable
				common.TagSeq: msgSeqNum, // #perfdisable
				`key`:         key,       // #perfdisable
				`val-len`:     len(val),  // #perfdisable
			}).Debug("replicationPump: put msg") // #perfdisable
		}

		// tWrite := time.Now() // #perfdisable

		// write message to storage
		_, err = x.storePut(key, val)

		// msg.m3Client.RecordTimer(metrics.ReplicateExtentScope, metrics.StorageWriteStoreLatency, time.Since(tWrite)) // #perfdisable

		if err != nil {

			log.WithFields(bark.Fields{
				"error":       "extent.Put error",
				common.TagErr: err,
				"key":         key,
				"len":         len(val),
				"replMsgs":    replMsgs,
			}).Error("replicationPump done")

			t.m3Client.IncCounter(metrics.ReplicateExtentScope, metrics.StorageFailures)
			t.m3Client.IncCounter(metrics.ReplicateExtentScope, metrics.StorageStoreFailures)
			t.err = fmt.Errorf("%v error writing message to store (key=%x, val=%d bytes): %v", t.extentID, key, len(val), err)
			done = true
			continue pump
		}

		replMsgs++ // keep a count of the messages written during this "session"

		// if first msg written to this extent, update first-seqnum, etc
		if firstMsg {
			x.setFirstMsg(int64(key), msgSeqNum, visibilityTime)
			firstMsg = false
		}

		credLine.Return(1) // TODO: make credits proportional to payload size

		// update last-seqnum, etc (reported by extStats)
		x.extentLock()
		x.setLastMsg(int64(key), msgSeqNum, visibilityTime)
		x.extentUnlock()
	}

	close(t.credC) // this should close the sendPump

	t.wg.Done()
	t.wg.Wait() // wait until the sendPump is done
	t.cleanup() // do any necessary cleanup
}

// sendPump sends out credits, when appropriate
func (t *ReplicationJob) sendPump() {

	defer t.wg.Done()     // drop ref on waitgroup
	defer t.stream.Done() // close stream

	stream := t.stream
	credC := t.credC

	log := t.log // get "local" logger (that already contains extent info)

pump:
	for {
		select {
		case cred, ok := <-credC: // read from the go-channel

			if !ok {
				log.Debug(`replicate.sendPump: credC closed`)
				break pump
			}

			// write out (blocking) to stream
			if err := stream.Write(&cherami.ControlFlow{Credits: common.Int32Ptr(cred)}); err != nil {
				log.WithField(common.TagErr, err).Error(`replicate.sendPump: stream.Write error`)
				break pump
			}

			// flush immediately
			if err := stream.Flush(); err != nil {
				log.WithField(common.TagErr, err).Error(`replicate.sendPump: stream.Flush error`)
				break pump
			}

		case <-t.stopC: // we were asked to stop
			log.Error(`replicate.sendPump: stop triggered`)
			break pump
		}
	}
}

func (t *ReplicationJob) updateReplicationStatus(extentUUID string, status shared.ExtentReplicaReplicationStatus) error {
	updateRequest := &metadata.UpdateStoreExtentReplicaStatsRequest{
		ExtentUUID:        common.StringPtr(extentUUID),
		StoreUUID:         common.StringPtr(t.replMgr.hostID),
		ReplicationStatus: common.InternalExtentReplicaReplicationStatusTypePtr(status),
	}
	err := t.replMgr.mClient.UpdateStoreExtentReplicaStats(nil, updateRequest)
	if err != nil {
		t.log.WithFields(bark.Fields{
			common.TagExt: common.FmtExt(extentUUID),
		}).Error(`Failed to update replication status`)
	}
	return err
}
