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

package retentionMgr

import (
	"time"

	"github.com/uber/cherami-server/.generated/go/metadata"
	"github.com/uber/cherami-server/.generated/go/shared"
	"github.com/uber/cherami-server/.generated/go/store"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"

	"github.com/uber-common/bark"
)

const defaultPageSize = 1000

type metadataDepImpl struct {
	metadata common.MetadataMgr
	logger   bark.Logger
}

func newMetadataDep(metadata metadata.TChanMetadataService, m3Client metrics.Client, logger bark.Logger) *metadataDepImpl {
	return &metadataDepImpl{
		metadata: common.NewMetadataMgr(metadata, m3Client, logger),
		logger:   logger,
	}
}

// -- the following are various helper routines, that talk to metadata, storehosts, etc -- //
func (t *metadataDepImpl) GetDestinations() (destinations []*destinationInfo) {

	log := t.logger

	log.Debug("GetDestinations: calling ListDestinations")

	list, err := t.metadata.ListDestinations()
	if err != nil {
		log.WithField(common.TagErr, err).Error(`GetDestinations: ListDestinations failed`)
		return
	}

	for _, destDesc := range list {

		dest := &destinationInfo{
			id:            destinationID(destDesc.GetDestinationUUID()),
			status:        destDesc.GetStatus(),
			softRetention: destDesc.GetConsumedMessagesRetention(),
			hardRetention: destDesc.GetUnconsumedMessagesRetention(),
			// type: mDestDesc.GetType(),
			path: destDesc.GetPath(),
		}

		destinations = append(destinations, dest)

		log.WithFields(bark.Fields{
			common.TagDst:   dest.id,
			`status`:        dest.status,
			`hardRetention`: dest.hardRetention,
			`softRetention`: dest.softRetention,
		}).Debug("GetDestinations: ListDestinations output")
	}

	log.WithField(`numDestinations`, len(destinations)).Debug("GetDestinations done")
	return
}

func (t *metadataDepImpl) GetExtents(destID destinationID) (extents []*extentInfo) {

	log := t.logger.WithField(common.TagDst, string(destID))

	extents = make([]*extentInfo, 0, 8)

	log.Debug("GetExtents: calling ListExtentsByDstIDStatus")

	list, err := t.metadata.ListExtentsByDstIDStatus(string(destID), nil)
	if err != nil {
		log.WithField(common.TagErr, err).Error(`GetExtents: ListExtentsByDstIDStatus failed`)
		return
	}

	for _, extStats := range list {

		extent := extStats.GetExtent()
		storeUUIDs := extent.GetStoreUUIDs()

		extInfo := &extentInfo{
			id:                 extentID(extent.GetExtentUUID()),
			status:             extStats.GetStatus(),
			statusUpdatedTime:  time.Unix(0, extStats.GetStatusUpdatedTimeMillis()*int64(time.Millisecond)),
			storehosts:         make([]storehostID, 0, len(storeUUIDs)),
			singleCGVisibility: consumerGroupID(extStats.GetConsumerGroupVisibility()),
		}

		for j := range storeUUIDs {
			extInfo.storehosts = append(extInfo.storehosts, storehostID(storeUUIDs[j]))
		}

		extents = append(extents, extInfo)

		log.WithFields(bark.Fields{
			common.TagExt: string(extInfo.id),
			`status`:      extInfo.status,
			`replicas`:    extInfo.storehosts,
		}).Debug(`GetExtents: ListExtentsByDstIDStatus output`)

	}

	log.WithField(`numExtents`, len(extents)).Debug("GetExtents done")
	return
}

func (t *metadataDepImpl) GetExtentInfo(destID destinationID, extID extentID) (extInfo *extentInfo, err error) {

	log := t.logger.WithFields(bark.Fields{
		common.TagDst: string(destID),
		common.TagExt: string(extID),
	})

	log.Debug("GetExtentInfo: ReadExtentStats on metadata")

	extStats, err := t.metadata.ReadExtentStats(string(destID), string(destID))
	if err != nil {
		log.WithField(common.TagErr, err).Error("GetExtentInfo: ReadExtentStats failed")
		return
	}

	extInfo = &extentInfo{
		id:                extID,
		status:            extStats.GetStatus(),
		statusUpdatedTime: time.Unix(0, extStats.GetStatusUpdatedTimeMillis()*int64(time.Millisecond)),
		storehosts:        make([]storehostID, 0, len(extStats.GetReplicaStats())),
	}

	for _, replicaStat := range extStats.GetReplicaStats() {
		extInfo.storehosts = append(extInfo.storehosts, storehostID(replicaStat.GetStoreUUID()))
	}

	log.WithFields(bark.Fields{
		`extInfo.status`:            extInfo.status,
		`extInfo.statusUpdatedTime`: extInfo.statusUpdatedTime,
		`extInfo.storehosts`:        extInfo.storehosts,
	}).Debug(`GetExtentInfo done`)

	return
}

func (t *metadataDepImpl) GetConsumerGroups(destID destinationID) (consumerGroups []*consumerGroupInfo) {

	log := t.logger.WithField(common.TagDst, string(destID))

	log.Debug("GetConsumerGroups: ListConsumerGroupsByDstID on metadata")

	list, err := t.metadata.ListConsumerGroupsByDstID(string(destID))
	if err != nil {
		log.WithField(common.TagErr, err).Error("GetConsumerGroups: ListConsumerGroupsByDstID failed")
		return
	}

	for _, cgDesc := range list {
		// assert(destId == cgDesc.GetDestinationUUID()) //

		cg := &consumerGroupInfo{
			id:     consumerGroupID(cgDesc.GetConsumerGroupUUID()),
			status: cgDesc.GetStatus(),
		}

		consumerGroups = append(consumerGroups, cg)

		log.WithFields(bark.Fields{
			common.TagCnsm: string(cg.id),
			`status`:       cg.status,
		}).Debug(`GetConsumerGroups: ListConsumerGroupsByDstID output`)
	}

	log.WithField(`numConsumerGroups`, len(consumerGroups)).Debug("GetConsumerGroups done")
	return
}

func (t *metadataDepImpl) DeleteExtent(destID destinationID, extID extentID) (err error) {

	log := t.logger.WithFields(bark.Fields{
		common.TagDst: string(destID),
		common.TagExt: string(extID),
	})

	log.Debug("DeleteExtent: UpdateExtentStats on metadata")

	err = t.metadata.UpdateExtentStatus(string(destID), string(destID), shared.ExtentStatus_DELETED)
	if err != nil {
		log.WithField(common.TagErr, err).Error(`DeleteExtent: UpdateExtentStats failed`)
		return
	}

	log.Debug(`DeleteExtent done`)
	return
}

func (t *metadataDepImpl) MarkExtentConsumed(destID destinationID, extID extentID) (err error) {

	log := t.logger.WithFields(bark.Fields{
		common.TagDst: string(destID),
		common.TagExt: string(extID),
	})

	log.Debug("MarkExtentConsumed: UpdateExtentStats on metadata")

	err = t.metadata.UpdateExtentStatus(string(destID), string(destID), shared.ExtentStatus_CONSUMED)
	if err != nil {
		log.WithField(common.TagErr, err).Error(`MarkExtentConsumed: UpdateExtentStats failed`)
		return
	}

	log.Debug(`MarkExtentConsumed done`)
	return
}

func (t *metadataDepImpl) DeleteConsumerGroupExtent(cgID consumerGroupID, extID extentID) (err error) {

	log := t.logger.WithFields(bark.Fields{
		common.TagCnsmID: string(cgID),
		common.TagExt:    string(extID),
	})

	log.Debug("DeleteConsumerGroupExtent: UpdateConsumerGroupExtentStatus on metadata")

	err = t.metadata.UpdateConsumerGroupExtentStatus(string(cgID), string(extID), metadata.ConsumerGroupExtentStatus_DELETED)

	if err != nil {
		log.WithField(common.TagErr, err).Error("DeleteConsumerGroupExtent: UpdateConsumerGroupExtentStatus failed")
		return err
	}

	log.Debug("DeleteConsumerGroupExtent done")
	return nil
}

func (t *metadataDepImpl) DeleteDestination(destID destinationID) (err error) {

	log := t.logger.WithField(common.TagDst, string(destID))

	log.Debug("DeleteDestination: DeleteDestination on metadata")

	err = t.metadata.DeleteDestination(string(destID))

	if err != nil {
		log.WithField(common.TagErr, err).Error("DeleteDestination: DeleteDestination failed")
		return err
	}

	log.Debug("DeleteDestination done")
	return nil
}

func (t *metadataDepImpl) GetAckLevel(destID destinationID, extID extentID, cgID consumerGroupID) (ackLevel int64, err error) {

	log := t.logger.WithFields(bark.Fields{
		common.TagDst:    string(destID),
		common.TagExt:    string(extID),
		common.TagCnsmID: string(cgID),
	})

	log.Debug("GetAckLevel: ReadConsumerGroupExtent on metadata")

	resp, err := t.metadata.ReadConsumerGroupExtent(string(destID), string(destID), string(destID))
	if err != nil {
		log.WithField(common.TagErr, err).Error("GetAckLevel: ReadConsumerGroupExtent failed")
		return store.ADDR_BEGIN, err
	}

	// assert(resp.GetExtentUUID() == extID)
	// assert(resp.GetConsumerGroupUUID() == cgID)

	// check if the consumer-group has read to "sealed" point
	if resp.GetStatus() != metadata.ConsumerGroupExtentStatus_OPEN {
		ackLevel = store.ADDR_SEAL
	} else {
		ackLevel = resp.GetAckLevelOffset()
		// assert(ackLevel != cherami.ADDR_END
	}

	log.WithField(`ackLevel`, ackLevel).Debug("GetAckLevel done")
	return ackLevel, nil
}
