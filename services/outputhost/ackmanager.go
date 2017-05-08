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
	"sync"
	"time"

	"github.com/uber-common/bark"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

const ackLevelInterval = 5 * time.Second
const replicatorCallTimeout = 20 * time.Second

type storeHostAddress int64

type (
	ackIndex uint32

	// msgCtx is the message context which is stored locally on the ackMgr
	msgCtx struct {
		addr   storeHostAddress
		seqnum common.SequenceNumber
		acked  bool
	}

	levels struct {
		asOf         common.UnixNanoTime   // Timestamp when this level was calculated
		readLevel    ackIndex              // index upto which msgs have been read
		readLevelSeq common.SequenceNumber // msg seqnum corresponding to readLevel
		ackLevel     ackIndex              // index upto which msgs have been acked
		ackLevelSeq  common.SequenceNumber // msg seqnum corresponding to the ackLevel
		lastAckedSeq common.SequenceNumber // the latest sequence which is acked
	}

	// ackManager is held per CG extent and it holds the addresses that we get from the store.
	ackManager struct {
		addrs              map[ackIndex]*msgCtx // ‡
		sealed             bool                 // ‡
		outputHostUUID     string
		cgUUID             string
		extUUID            string
		connectedStoreUUID *string
		*levels            // ‡ the current levels
		ackLevelTicker     *time.Ticker
		closeChannel       chan struct{}
		waitConsumed       chan<- bool // waitConsumed is the channel which will signal if the extent is completely consumed given by extentCache
		committer          Committer   // ‡
		doneWG             sync.WaitGroup
		logger             bark.Logger
		sessionID          uint16
		ackMgrID           uint16              // ID of this ackManager; unique on this host
		cgCache            *consumerGroupCache // back pointer to the consumer group cache
		lk                 sync.RWMutex        // ‡ = guarded by this mutex
	}
)

func newAckManager(
	cgCache *consumerGroupCache,
	ackMgrID uint32,
	outputHostUUID string,
	cgUUID string,
	extUUID string,
	connectedStoreUUID *string,
	waitConsumedCh chan<- bool,
	cge *shared.ConsumerGroupExtent,
	committer Committer,
	logger bark.Logger) *ackManager {
	ackMgr := &ackManager{
		addrs:              make(map[ackIndex]*msgCtx),
		cgCache:            cgCache,
		outputHostUUID:     outputHostUUID,
		cgUUID:             cgUUID,
		extUUID:            extUUID,
		connectedStoreUUID: connectedStoreUUID,
		sessionID:          cgCache.sessionID, //sessionID,
		ackMgrID:           uint16(ackMgrID),  //ackMgrID,
		committer:          committer,
		ackLevelTicker:     time.NewTicker(ackLevelInterval),
		waitConsumed:       waitConsumedCh,
		logger:             logger.WithField(common.TagModule, `ackMgr`),
	}

	ackMgr.levels = &levels{
		readLevel:    ackIndex(cge.GetAckLevelSeqNo()),
		readLevelSeq: common.SequenceNumber(cge.GetAckLevelSeqNo()),
		ackLevel:     ackIndex(cge.GetAckLevelSeqNo()),
		ackLevelSeq:  common.SequenceNumber(cge.GetAckLevelSeqNo()),
	}

	return ackMgr
}

// ackID is a string which is a base64 encoded string
// First we get the ackID and store the address locally in our data structure
// for maintaining the ack level
func (ackMgr *ackManager) getNextAckID(address storeHostAddress, sequence common.SequenceNumber) (ackID string) {
	ackMgr.lk.Lock()
	defer ackMgr.lk.Unlock()

	ackMgr.readLevel++ // This means that the first ID is '1'
	ackMgr.readLevelSeq++

	if sequence != ackMgr.readLevelSeq {
		if sequence > ackMgr.readLevelSeq {
			ackMgr.logger.WithFields(bark.Fields{
				`old-readLevelSeq`: ackMgr.readLevelSeq,
				`new-readLevelSeq`: sequence,
			}).Warn(`adjusting read-level sequence forward`)

			// update gauge here to indicate we skipped messages (potentially due to retention?)
			ackMgr.cgCache.consumerM3Client.UpdateGauge(metrics.ConsConnectionScope,
				metrics.OutputhostCGSkippedMessages, int64(sequence-ackMgr.readLevelSeq))
		} else {
			ackMgr.logger.WithFields(bark.Fields{
				`old-readLevelSeq`: ackMgr.readLevelSeq,
				`new-readLevelSeq`: sequence,
			}).Error(`adjusting read-level sequence backwards`)
		}

		ackMgr.readLevelSeq = sequence
	}

	ackID = common.ConstructAckID(ackMgr.sessionID, ackMgr.ackMgrID, uint32(ackMgr.readLevel), int64(address))

	// now store the message in the data structure internally
	ackMgr.addrs[ackMgr.readLevel] = &msgCtx{
		addr:   address,
		seqnum: sequence,
	}

	// Let the committer know about the new read level
	ackMgr.committer.SetReadLevel(CommitterLevel{
		seqNo:   sequence,
		address: address,
	})

	return
}

func (ackMgr *ackManager) stop() {
	close(ackMgr.closeChannel)
	ackMgr.doneWG.Wait()
}

func (ackMgr *ackManager) start() {
	ackMgr.closeChannel = make(chan struct{})
	ackMgr.doneWG.Add(1)
	go ackMgr.manageAckLevel()
}

// getCurrentReadLevel returns the current read-level address and seqnum. this is called
// by extcache when it connects to a new replica, when one stream is disconnected.
func (ackMgr *ackManager) getCurrentReadLevel() (addr int64, seqnum common.SequenceNumber) {

	ackMgr.lk.RLock()
	defer ackMgr.lk.RUnlock()

	// the 'readLevel' may not exist in the 'addrs' map if this instance
	// of ackMgr has not seen a message yet (ie, getNextAckID has not been
	// called), in which case we would return '0' addr.
	if msg, ok := ackMgr.addrs[ackMgr.readLevel]; ok {
		addr = int64(msg.addr)
	}

	seqnum = ackMgr.readLevelSeq

	return
}

// resetMsg is a utility routine which is used to reset the readLevel because
// we couldn't even write this to the client msgsCh
func (ackMgr *ackManager) resetMsg(offset int64) {
	ackMgr.lk.Lock()
	// make sure the address of the current readLevel matches with this offset
	if addrs, ok := ackMgr.addrs[ackMgr.readLevel]; ok {
		expectedOffset := int64(addrs.addr)
		if expectedOffset != offset {
			// this should *never* happen and we should panic here because the only way to
			// get here is only because of memory corruption.
			ackMgr.cgCache.consumerM3Client.UpdateGauge(metrics.ConsConnectionScope, metrics.OutputhostCGAckMgrResetMsgError, 1)
			ackMgr.logger.WithFields(bark.Fields{
				`expectedOffset`: expectedOffset,
				`offset`:         offset,
			}).Panic(`resetMsg: offsets don't match!`)
		} else {
			// report that we removed a read level
			ackMgr.cgCache.consumerM3Client.UpdateGauge(metrics.ConsConnectionScope, metrics.OutputhostCGAckMgrResetMsg, 1)
			delete(ackMgr.addrs, ackMgr.readLevel)
			// move the readLevel one below, since this message is not here anymore
			ackMgr.readLevel--
		}
	}
	ackMgr.lk.Unlock()
}

// notifySealed just makes a note that this extent is sealed
func (ackMgr *ackManager) notifySealed() {
	ackMgr.lk.Lock()
	ackMgr.sealed = true
	ackMgr.lk.Unlock()
	return
}

func (ackMgr *ackManager) updateAckLevel() {
	var update, consumed bool
	var ackLevelAddress int64
	ackMgr.lk.Lock()

	count := 0
	stop := ackMgr.ackLevel + ackIndex(len(ackMgr.addrs))

	// We go through the map here and see if the messages are acked,
	// moving the acklevel as we go forward.
	for curr := ackMgr.ackLevel + 1; curr <= stop; curr++ {
		if addrs, ok := ackMgr.addrs[curr]; ok {
			if !addrs.acked {
				break
			}

			update = true
			ackMgr.ackLevel = curr
			ackLevelAddress = int64(addrs.addr)

			// We need to commit every message we see here, since we may have an interleved stream,
			// and only the committer knows how to report the level(s). This is true, e.g. for Kafka.
			ackMgr.committer.SetCommitLevel(CommitterLevel{
				seqNo:   addrs.seqnum,
				address: addrs.addr,
			})

			// getCurrentAckLevelOffset needs addr[ackMgr.ackLevel], so delete the previous one if it exists
			count++
			delete(ackMgr.addrs, curr-1)
		}
	}

	// check if the extent can be marked as consumed
	// We can mark an extent as consumed, if we have both these conditions:
	// 1. The extent is sealed (which means we have it marked after receiving the last message)
	// 2. The ackLevel has reached the end (which means that the ackLevel equals the readLevel)
	if ackMgr.sealed {
		ackMgr.committer.SetFinalLevel(CommitterLevel{
			seqNo:   ackMgr.committer.GetReadLevel().seqNo,
			address: ackMgr.committer.GetReadLevel().address,
		})
	}

	if ackMgr.sealed && ackMgr.ackLevel == ackMgr.readLevel {
		ackMgr.logger.Debug("extent sealed and consumed")
		consumed = true
		update = true
	}

	updatedSize := len(ackMgr.addrs)

	if update {
		ackMgr.asOf = common.Now()
		if err := ackMgr.committer.UnlockAndFlush(&ackMgr.lk); err != nil { // implicit ackMgr.lk.Unlock()
			ackMgr.logger.WithFields(bark.Fields{
				common.TagErr:     err,
				`ackLevelAddress`: ackLevelAddress,
			}).Error(`error updating ackLevel`)
		} else {
			// Updating metadata succeeded; report some metrics and mark the extent as consumed if necessary
			// report the count of updates we did this round
			ackMgr.cgCache.consumerM3Client.UpdateGauge(metrics.ConsConnectionScope, metrics.OutputhostCGAckMgrLevelUpdate, int64(count))
			if consumed {
				// report that the extent is consumed
				ackMgr.cgCache.consumerM3Client.UpdateGauge(metrics.ConsConnectionScope, metrics.OutputhostCGAckMgrConsumed, 1)
				// notify extentCache that this extent is consumed
				// We can do this in a non-blocking way because, if we fail to
				// notify this time, we will anyway notify it the next round.
				// We do it in a non-blocking way because we could potentially
				// deadlock during unload
				select {
				case ackMgr.waitConsumed <- true:
					ackMgr.logger.WithField(`ackLevelAddress`, ackLevelAddress).Info(`extent consumed`)
				default:
				}
			}
		}
	} else {
		ackMgr.lk.Unlock()
	}

	// Report the size of the ackMgr map, if greater than 0
	if updatedSize > 0 {
		ackMgr.cgCache.consumerM3Client.UpdateGauge(metrics.ConsConnectionScope, metrics.OutputhostCGAckMgrSize, int64(updatedSize))
	}
}

func (ackMgr *ackManager) acknowledgeMessage(ackID AckID, index ackIndex, address int64, isNack bool) error {
	var err error
	notifyCg := true
	ackMgr.lk.Lock() // Read lock would be OK in this case (except for a benign race with two simultaneous acks for the same ackID), see below
	// check if this id is present
	if msg, ok := ackMgr.addrs[index]; ok {
		// validate the address from the ackID
		if msg.addr != storeHostAddress(address) {
			ackMgr.logger.WithFields(bark.Fields{
				`address`:  address,
				`expected`: msg.addr,
			}).Error(`ack address does not match!`)
			err = errors.New("address of the ackID doesn't match with ackMgr")
			notifyCg = false
		} else {
			if ackMgr.cgCache.cachedCGDesc.GetOwnerEmail() == SmartRetryDisableString {
				ackMgr.logger.WithFields(bark.Fields{
					`Address`: address,
					`addr`:    msg.addr,
					`index`:   index,
					`isNack`:  isNack,
				}).Info(`msg ack`)
			}
			if !isNack {
				msg.acked = true // This is the only place that this field of msg is changed. It was initially set under a write lock elsewhere, hence we can have a read lock
				// update the last acked sequence, if this is the most recent ack
				if ackMgr.lastAckedSeq < msg.seqnum {
					ackMgr.lastAckedSeq = msg.seqnum
				}
			}
		}
	} else {
		// Update metric to reflect that the sequence number is not found
		ackMgr.cgCache.consumerM3Client.IncCounter(metrics.ConsConnectionScope, metrics.OutputhostCGAckMgrSeqNotFound)
	}
	ackMgr.lk.Unlock()

	// Now notify the message cache so that it can update it's state
	// Note: We explicitly do this outside the lock above to prevent us from
	// blocking with a lock held
	// send the ack to the ack channel for the msg cache to cleanup
	if notifyCg {
		if isNack {
			ackMgr.cgCache.nackMsgCh <- timestampedAckID{AckID: ackID, ts: common.Now()}
		} else {
			ackMgr.cgCache.ackMsgCh <- timestampedAckID{AckID: ackID, ts: common.Now()}
		}
	}
	return err
}

func (ackMgr *ackManager) manageAckLevel() {
	defer ackMgr.doneWG.Done()
	// this needs to look at all the acked messages and update the ackLevel
	// accordingly.
	for {
		select {
		case <-ackMgr.ackLevelTicker.C:
			ackMgr.updateAckLevel()
		case <-ackMgr.closeChannel:
			// before returning make sure we try to set the ack offset
			ackMgr.updateAckLevel()
			return
		}
	}
}

// get the number of acked and unacked messages from the last ack level
func (ackMgr *ackManager) getNumAckedAndUnackedMessages() (*int64, *int64) {
	stop := ackMgr.ackLevel + ackIndex(len(ackMgr.addrs))

	var acked int64
	var unacked int64
	// We go through the map here and see if the messages are acked,
	for curr := ackMgr.ackLevel + 1; curr <= stop; curr++ {
		if msg, ok := ackMgr.addrs[curr]; ok {
			if msg.acked {
				acked++
			} else {
				unacked++
			}
		} else {
			ackMgr.logger.WithFields(bark.Fields{
				common.TagSeq: curr,
			}).Error(`sequence number not found in the ack mgr`)
		}
	}

	return common.Int64Ptr(acked), common.Int64Ptr(unacked)
}

func (ackMgr *ackManager) getAckMgrState() *admin.AckMgrState {
	ackMgrState := admin.NewAckMgrState()
	ackMgr.lk.RLock()
	defer ackMgr.lk.RUnlock()

	c := ackMgr.committer.GetCommitLevel()
	r := ackMgr.committer.GetReadLevel()

	ackMgrState.AckMgrID = common.Int16Ptr(int16(ackMgr.ackMgrID))
	ackMgrState.IsSealed = common.BoolPtr(ackMgr.sealed)
	ackMgrState.ReadLevelSeq = common.Int64Ptr(int64(ackMgr.readLevel))
	ackMgrState.AckLevelSeq = common.Int64Ptr(int64(ackMgr.ackLevel))
	ackMgrState.ReadLevelOffset = common.Int64Ptr(int64(r.address))
	ackMgrState.AckLevelOffset = common.Int64Ptr(int64(c.address))
	ackMgrState.LastAckLevelUpdateTime = common.Int64Ptr(int64(ackMgr.asOf))
	ackMgrState.LastAckedSeq = common.Int64Ptr(int64(ackMgr.lastAckedSeq))
	ackMgrState.NumAckedMsgs, ackMgrState.NumUnackedMsgs = ackMgr.getNumAckedAndUnackedMessages()

	return ackMgrState
}
