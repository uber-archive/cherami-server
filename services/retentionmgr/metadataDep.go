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

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"
)

const defaultPageSize = 1000

type metadataDepImpl struct {
	metadata metadata.TChanMetadataService
	logger   bark.Logger
}

func newMetadataDep(metadata metadata.TChanMetadataService, log bark.Logger) *metadataDepImpl {
	return &metadataDepImpl{
		metadata: metadata,
		logger:   log,
	}
}

// -- the following are various helper routines, that talk to metadata, storehosts, etc -- //
func (t *metadataDepImpl) GetDestinations() (destinations []*destinationInfo) {

	req := shared.NewListDestinationsByUUIDRequest()
	req.Limit = common.Int64Ptr(defaultPageSize)

	ctx, cancel := thrift.NewContext(5 * time.Second)
	defer cancel()

	log := t.logger

	i := 0
	for {

		log.Info("GetDestinations: ListDestinationsByUUID on metadata")

		resp, err := t.metadata.ListDestinationsByUUID(ctx, req)
		if err != nil {
			log.WithField(common.TagErr, err).Error(`GetDestinations: ListDestinationsByUUID failed`)
			return
		}

		for _, destDesc := range resp.GetDestinations() {

			dest := &destinationInfo{
				id:            destinationID(destDesc.GetDestinationUUID()),
				destType:      destDesc.GetType(),
				status:        destDesc.GetStatus(),
				softRetention: destDesc.GetConsumedMessagesRetention(),
				hardRetention: destDesc.GetUnconsumedMessagesRetention(),
				path:          destDesc.GetPath(),
				isMultiZone:   destDesc.GetIsMultiZone(),
			}

			destinations = append(destinations, dest)
			i++

			log.WithFields(bark.Fields{
				common.TagDst:   dest.id,
				`status`:        dest.status,
				`hardRetention`: dest.hardRetention,
				`softRetention`: dest.softRetention,
			}).Info("GetDestinations: ListDestinationsByUUID output")
		}

		if len(resp.GetNextPageToken()) == 0 {
			break
		}

		req.PageToken = resp.GetNextPageToken()

		log.Info("GetDestinations: fetching next page of ListDestinationsByUUID")
	}

	log.WithField(`numDestinations`, len(destinations)).Info("GetDestinations done")
	return
}

func (t *metadataDepImpl) GetExtents(destID destinationID) (extents []*extentInfo) {

	req := shared.NewListExtentsStatsRequest()
	req.DestinationUUID = common.StringPtr(string(destID))
	req.Limit = common.Int64Ptr(defaultPageSize)

	ctx, cancel := thrift.NewContext(5 * time.Second)
	defer cancel()

	log := t.logger.WithField(common.TagDst, string(destID))

	extents = make([]*extentInfo, 0, 8)
	i := 0

	for {
		log.Info("GetExtents: ListExtentStats on metadata")

		resp, err := t.metadata.ListExtentsStats(ctx, req)
		if err != nil {
			log.WithField(common.TagErr, err).Error(`GetExtents: ListExtentsStats failed`)
			return
		}

		for _, extStats := range resp.GetExtentStatsList() {

			extent := extStats.GetExtent()

			storeUUIDs := extent.GetStoreUUIDs()
			storehosts := make([]storehostID, 0, len(storeUUIDs))

			for _, storeUUID := range storeUUIDs {
				storehosts = append(storehosts, storehostID(storeUUID))
			}

			extInfo := &extentInfo{
				id:                 extentID(extent.GetExtentUUID()),
				status:             extStats.GetStatus(),
				statusUpdatedTime:  time.Unix(0, extStats.GetStatusUpdatedTimeMillis()*int64(time.Millisecond)),
				storehosts:         storehosts,
				singleCGVisibility: consumerGroupID(extStats.GetConsumerGroupVisibility()),
				originZone:         extStats.GetExtent().GetOriginZone(),
				kafkaPhantomExtent: common.AreKafkaPhantomStores(storeUUIDs),
			}

			extents = append(extents, extInfo)
			i++

			log.WithFields(bark.Fields{
				common.TagExt: string(extInfo.id),
				`status`:      extInfo.status,
				`replicas`:    extInfo.storehosts,
			}).Info(`GetExtents: ListExtentStats output`)
		}

		if len(resp.GetNextPageToken()) == 0 {
			break
		}

		req.PageToken = resp.GetNextPageToken()

		log.Info("GetExtents: fetching next page of ListExtentStats")
	}

	log.WithField(`numExtents`, len(extents)).Info("GetExtents done")
	return
}

func (t *metadataDepImpl) GetExtentInfo(destID destinationID, extID extentID) (extInfo *extentInfo, err error) {

	req := metadata.NewReadExtentStatsRequest()
	req.DestinationUUID = common.StringPtr(string(destID))
	req.ExtentUUID = common.StringPtr(string(extID))

	ctx, cancel := thrift.NewContext(5 * time.Second)
	defer cancel()

	log := t.logger.WithFields(bark.Fields{
		common.TagDst: string(destID),
		common.TagExt: string(extID),
	})

	log.Info("GetExtentInfo: ReadExtentStats on metadata")

	resp, err := t.metadata.ReadExtentStats(ctx, req)
	if err != nil {
		log.WithField(common.TagErr, err).Error("GetExtentInfo: ReadExtentStats failed")
		return
	}

	extStats := resp.GetExtentStats()

	storeUUIDs := make([]string, 0, len(extStats.GetReplicaStats()))
	storehosts := make([]storehostID, 0, len(extStats.GetReplicaStats()))

	for _, replicaStat := range extStats.GetReplicaStats() {
		storehosts = append(storehosts, storehostID(replicaStat.GetStoreUUID()))
		storeUUIDs = append(storeUUIDs, replicaStat.GetStoreUUID())
	}

	extInfo = &extentInfo{
		id:                 extID,
		status:             extStats.GetStatus(),
		storehosts:         storehosts,
		kafkaPhantomExtent: common.AreKafkaPhantomStores(storeUUIDs),
		statusUpdatedTime:  time.Unix(0, extStats.GetStatusUpdatedTimeMillis()*int64(time.Millisecond)),
	}

	log.WithFields(bark.Fields{
		`extInfo.status`:            extInfo.status,
		`extInfo.statusUpdatedTime`: extInfo.statusUpdatedTime,
		`extInfo.storehosts`:        extInfo.storehosts,
	}).Info(`GetExtentInfo done`)

	return
}

func (t *metadataDepImpl) GetConsumerGroups(destID destinationID) (consumerGroups []*consumerGroupInfo) {

	req := shared.NewListConsumerGroupsUUIDRequest()
	req.DestinationUUID = common.StringPtr(string(destID))
	req.Limit = common.Int64Ptr(defaultPageSize)
	ctx, _ := thrift.NewContext(time.Second)

	ctx, cancel := thrift.NewContext(30 * time.Second)
	defer cancel()

	log := t.logger.WithField(common.TagDst, string(destID))

	for {
		log.Info("GetConsumerGroups: ListConsumerGroupsUUID on metadata")

		res, err := t.metadata.ListConsumerGroupsUUID(ctx, req)
		if err != nil {
			log.WithField(common.TagErr, err).Error("GetConsumerGroups: ListConsumerGroupsUUID failed")
			break
		}

		for _, cgDesc := range res.GetConsumerGroups() {
			// assert(destId == cgDesc.GetDestinationUUID()) //

			cg := &consumerGroupInfo{
				id:     consumerGroupID(cgDesc.GetConsumerGroupUUID()),
				status: cgDesc.GetStatus(),
			}

			consumerGroups = append(consumerGroups, cg)

			log.WithFields(bark.Fields{
				common.TagCnsm: string(cg.id),
				`status`:       cg.status,
			}).Info(`GetConsumerGroups: ListConsumerGroupsUUID output`)
		}

		if len(res.GetNextPageToken()) == 0 {
			break
		}

		req.PageToken = res.GetNextPageToken()

		log.Info("GetConsumerGroups: fetching next page of ListConsumerGroupsUUID")
	}

	log.WithField(`numConsumerGroups`, len(consumerGroups)).Info("GetConsumerGroups done")
	return
}

func (t *metadataDepImpl) DeleteExtent(destID destinationID, extID extentID) (err error) {

	req := metadata.NewUpdateExtentStatsRequest()
	req.DestinationUUID = common.StringPtr(string(destID))
	req.ExtentUUID = common.StringPtr(string(extID))
	req.Status = shared.ExtentStatusPtr(shared.ExtentStatus_DELETED)

	ctx, cancel := thrift.NewContext(2 * time.Second)
	defer cancel()

	log := t.logger.WithFields(bark.Fields{
		common.TagDst: string(destID),
		common.TagExt: string(extID),
	})

	log.Info("DeleteExtent: UpdateExtentStats on metadata")

	resp, err := t.metadata.UpdateExtentStats(ctx, req)
	if err != nil {
		log.WithField(common.TagErr, err).Error(`DeleteExtent: UpdateExtentStats failed`)
		return
	}

	log.WithField(`resp.status`, resp.GetExtentStats().GetStatus()).Info(`DeleteExtent done`)
	return
}

func (t *metadataDepImpl) MarkExtentConsumed(destID destinationID, extID extentID) (err error) {

	req := metadata.NewUpdateExtentStatsRequest()
	req.DestinationUUID = common.StringPtr(string(destID))
	req.ExtentUUID = common.StringPtr(string(extID))
	req.Status = shared.ExtentStatusPtr(shared.ExtentStatus_CONSUMED)

	ctx, cancel := thrift.NewContext(2 * time.Second)
	defer cancel()

	log := t.logger.WithFields(bark.Fields{
		common.TagDst: string(destID),
		common.TagExt: string(extID),
	})

	log.Info("MarkExtentConsumed: UpdateExtentStats on metadata")

	resp, err := t.metadata.UpdateExtentStats(ctx, req)
	if err != nil {
		log.WithField(common.TagErr, err).Error(`MarkExtentConsumed: UpdateExtentStats failed`)
		return
	}

	log.WithField(`resp.status`, resp.GetExtentStats().GetStatus()).Info(`MarkExtentConsumed done`)
	return
}

func (t *metadataDepImpl) GetExtentsForConsumerGroup(destID destinationID, cgID consumerGroupID) (extIDs []extentID, err error) {

	req := metadata.NewReadConsumerGroupExtentsLiteRequest()
	req.ConsumerGroupUUID = common.StringPtr(string(cgID))
	req.MaxResults = common.Int32Ptr(defaultPageSize)

	ctx, cancel := thrift.NewContext(30 * time.Second)
	defer cancel()

	log := t.logger.WithFields(bark.Fields{
		common.TagDst:  string(destID),
		common.TagCnsm: string(cgID),
	})

	for {
		log.Info("GetExtentsForConsumerGroup: ReadConsumerGroupExtentsLite on metadata")

		res, e := t.metadata.ReadConsumerGroupExtentsLite(ctx, req)
		if e != nil {
			err = e
			log.WithField(common.TagErr, err).Error("GetExtentsForConsumerGroup: ReadConsumerGroupExtentsLite failed")
			break
		}

		for _, cgx := range res.GetExtents() {
			extIDs = append(extIDs, extentID(cgx.GetExtentUUID()))
		}

		if len(res.GetNextPageToken()) == 0 {
			break
		}

		req.PageToken = res.GetNextPageToken()

		log.Info("GetExtentsForConsumerGroup: fetching next page of consumer-group extents")
	}

	log.WithField(`numExtents`, len(extIDs)).Info("GetExtentsForConsumerGroup done")
	return
}

func (t *metadataDepImpl) DeleteConsumerGroupExtent(destID destinationID, cgID consumerGroupID, extID extentID) error {

	req := shared.NewUpdateConsumerGroupExtentStatusRequest()
	req.ExtentUUID = common.StringPtr(string(extID))
	req.ConsumerGroupUUID = common.StringPtr(string(cgID))
	req.Status = common.MetadataConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_DELETED)

	ctx, cancel := thrift.NewContext(2 * time.Second)
	defer cancel()

	log := t.logger.WithFields(bark.Fields{
		common.TagDst:    string(destID),
		common.TagCnsmID: string(cgID),
		common.TagExt:    string(extID),
	})

	log.Info("DeleteConsumerGroupExtent: UpdateConsumerGroupExtentStatus on metadata")

	err := t.metadata.UpdateConsumerGroupExtentStatus(ctx, req)

	if err != nil {
		log.WithField(common.TagErr, err).Error("DeleteConsumerGroupExtent: UpdateConsumerGroupExtentStatus failed")
		return err
	}

	log.Info("DeleteConsumerGroupExtent done")
	return nil
}

func (t *metadataDepImpl) DeleteConsumerGroup(destID destinationID, cgID consumerGroupID) error {

	req := metadata.NewDeleteConsumerGroupUUIDRequest()
	req.UUID = common.StringPtr(string(destID))

	ctx, cancel := thrift.NewContext(2 * time.Second)
	defer cancel()

	log := t.logger.WithFields(bark.Fields{
		common.TagDst:    string(destID),
		common.TagCnsmID: string(cgID),
	})

	log.Info("DeleteConsumerGroup: DeleteConsumerGroupUUID on metadata")

	err := t.metadata.DeleteConsumerGroupUUID(ctx, req)

	if err != nil {
		log.WithField(common.TagErr, err).Error("DeleteConsumerGroup: DeleteConsumerGroupUUID failed")
		return err
	}

	log.Info("DeleteConsumerGroup done")
	return nil
}

func (t *metadataDepImpl) DeleteDestination(destID destinationID) error {

	req := metadata.NewDeleteDestinationUUIDRequest()
	req.UUID = common.StringPtr(string(destID))

	ctx, cancel := thrift.NewContext(2 * time.Second)
	defer cancel()

	log := t.logger.WithField(common.TagDst, string(destID))

	log.Info("DeleteDestination: DeleteDestinationUUID on metadata")

	err := t.metadata.DeleteDestinationUUID(ctx, req)

	if err != nil {
		log.WithField(common.TagErr, err).Error("DeleteDestination: DeleteDestinationUUID failed")
		return err
	}

	log.Info("DeleteDestination done")
	return nil
}

func (t *metadataDepImpl) GetAckLevel(destID destinationID, extID extentID, cgID consumerGroupID) (ackLevel int64, err error) {

	req := metadata.NewReadConsumerGroupExtentRequest()
	req.DestinationUUID = common.StringPtr(string(destID))
	req.ExtentUUID = common.StringPtr(string(extID))
	req.ConsumerGroupUUID = common.StringPtr(string(cgID))

	ctx, cancel := thrift.NewContext(2 * time.Second)
	defer cancel()

	log := t.logger.WithFields(bark.Fields{
		common.TagDst:    string(destID),
		common.TagExt:    string(extID),
		common.TagCnsmID: string(cgID),
	})

	log.Info("GetAckLevel: ReadConsumerGroupExtent on metadata")

	resp, err := t.metadata.ReadConsumerGroupExtent(ctx, req)
	if err != nil {
		log.WithField(common.TagErr, err).Error("GetAckLevel: ReadConsumerGroupExtent failed")
		return store.ADDR_BEGIN, err
	}

	// assert(resp.GetExtent().GetExtentUUID() == extID)
	// assert(resp.GetExtent().GetConsumerGroupUUID() == cgID)

	switch resp.GetExtent().GetStatus() {

	case shared.ConsumerGroupExtentStatus_OPEN:
		// return the ack-level from metadata
		ackLevel = resp.GetExtent().GetAckLevelOffset()

	case shared.ConsumerGroupExtentStatus_CONSUMED:
		// 'ADDR_SEAL' indicates to the caller that this CG has fully consumed the extent
		ackLevel = store.ADDR_SEAL

	case shared.ConsumerGroupExtentStatus_DELETED:
		// set to 'ADDR_BEGIN' if cg-extent is deleted
		ackLevel = store.ADDR_BEGIN

	default:
		ackLevel = store.ADDR_BEGIN
		log.WithField(`ConsumerGroupExtentStatus`, resp.GetExtent().GetStatus()).
			Error("GetAckLevel: Unknown ConsumerGroupExtentStatus")
	}

	log.WithField(`ackLevel`, ackLevel).Info("GetAckLevel done")
	return ackLevel, nil
}
