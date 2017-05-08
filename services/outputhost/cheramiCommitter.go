// Copyright (c) 2017 Uber Technologies, Inc.
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
	"fmt"
	"sync"
	"time"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/tchannel-go/thrift"
)

const metaContextTimeout = 10 * time.Second

// cheramiCommitter is commits ackLevels to Cassandra through the TChanMetadataClient interface
type cheramiCommitter struct {
	outputHostUUID     string
	cgUUID             string
	extUUID            string
	connectedStoreUUID *string
	commitLevel        CommitterLevel
	readLevel          CommitterLevel
	finalLevel         CommitterLevel
	finalSet           bool
	metaclient         metadata.TChanMetadataService
	isMultiZone        bool
	tClients           common.ClientFactory
}

/*
 * Committer interface
 */

// SetCommitLevel just updates the next Cherami ack level that will be flushed
func (c *cheramiCommitter) SetCommitLevel(l CommitterLevel) {
	c.commitLevel = l
}

// SetReadLevel just updates the next Cherami read level that will be flushed
func (c *cheramiCommitter) SetReadLevel(l CommitterLevel) {
	c.readLevel = l
}

// SetFinalLevel just updates the last possible read level
func (c *cheramiCommitter) SetFinalLevel(l CommitterLevel) {
	c.finalSet = true
	c.finalLevel = l
}

// UnlockAndFlush pushes our commit and read levels to Cherami metadata, using SetAckOffset
func (c *cheramiCommitter) UnlockAndFlush(l sync.Locker) error {
	ctx, cancel := thrift.NewContext(metaContextTimeout)
	defer cancel()

	oReq := &shared.SetAckOffsetRequest{
		Status:             common.CheramiConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_OPEN),
		OutputHostUUID:     common.StringPtr(c.outputHostUUID),
		ConsumerGroupUUID:  common.StringPtr(c.cgUUID),
		ExtentUUID:         common.StringPtr(c.extUUID),
		ConnectedStoreUUID: common.StringPtr(*c.connectedStoreUUID),
		AckLevelAddress:    common.Int64Ptr(int64(c.commitLevel.address)),
		AckLevelSeqNo:      common.Int64Ptr(int64(c.commitLevel.seqNo)),
		ReadLevelAddress:   common.Int64Ptr(int64(c.readLevel.address)),
		ReadLevelSeqNo:     common.Int64Ptr(int64(c.readLevel.seqNo)),
	}

	if c.finalSet { // If the final level has been set
		if c.finalLevel.address == c.readLevel.address && c.readLevel.address == c.commitLevel.address { // And final==read==commit
			oReq.Status = common.CheramiConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_CONSUMED)
		}
	}

	// At this point we have the final state to be committed, just need to commit it, which may take some time
	l.Unlock()

	// Store the local-zone metadata
	errLocal := c.metaclient.SetAckOffset(ctx, oReq)

	// update the ack offset in remote zone.
	// Failure is fine as a reconciliation process between replicators(slow path) will fix the inconsistency eventually
	// DEVNOTE: SetAckOffsetInRemote modifies oReq
	var errRemote error
	if c.isMultiZone {
		errRemote = c.SetAckOffsetInRemote(oReq)
	}

	// Prefer the local error
	if errLocal != nil {
		return errLocal
	}
	return errRemote
}

// GetReadLevel returns the next readlevel that will be flushed
func (c *cheramiCommitter) GetReadLevel() (l CommitterLevel) {
	l = c.readLevel
	return
}

// GetCommitLevel returns the next commit level that will be flushed
func (c *cheramiCommitter) GetCommitLevel() (l CommitterLevel) {
	l = c.commitLevel
	return
}

/*
 * Setup & Utility
 */

// newCheramiCommitter instantiates a cheramiCommitter
func newCheramiCommitter(metaclient metadata.TChanMetadataService,
	outputHostUUID string,
	cgUUID string,
	extUUID string,
	connectedStoreUUID *string,
	isMultiZone bool,
	tClients common.ClientFactory) *cheramiCommitter {
	return &cheramiCommitter{
		metaclient:         metaclient,
		outputHostUUID:     outputHostUUID,
		cgUUID:             cgUUID,
		extUUID:            extUUID,
		connectedStoreUUID: connectedStoreUUID,
		isMultiZone:        isMultiZone,
		tClients:           tClients,
	}
}

// SetAckOffsetInRemote attempts to flush ack level to a remote zone
// DEVNOTE: SetAckOffsetInRemote modifies setAckLevelRequest
func (c *cheramiCommitter) SetAckOffsetInRemote(setAckLevelRequest *shared.SetAckOffsetRequest) error {
	// send to local replicator to fan out
	localReplicator, err := c.tClients.GetReplicatorClient()
	if err != nil {
		return fmt.Errorf("SetAckOffsetInRemote: GetReplicatorClient failed: %#v", err)
	}

	// empty the output host and store uuid as these apply to local zone only
	setAckLevelRequest.OutputHostUUID = nil
	setAckLevelRequest.ConnectedStoreUUID = nil

	ctx, cancel := thrift.NewContext(replicatorCallTimeout)
	defer cancel()
	err = localReplicator.SetAckOffsetInRemote(ctx, setAckLevelRequest)
	if err != nil {
		return fmt.Errorf("SetAckOffsetInRemote: SetAckOffsetInRemote failed from replicator: %#v", err)
	}
	return nil
}
