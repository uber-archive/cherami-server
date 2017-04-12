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
	"time"

	"github.com/uber-common/bark"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"

	m "github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

// Some of our records like ExtentStats are
// huge. Larger records along with large
// pageSize will result in bigger response
// payloads. This can impact latency for all
// queries multiplexed on a tcp conn. So,
// keep the pageSize small.
const defaultPageSize = 512

type (
	// MetadataMgr manages extents. It exposes easy
	// to use CRUD API on top of the cassandra
	// metadata client API
	MetadataMgr interface {
		// ReadDestination returns the destination desc for the given uuid
		ReadDestination(dstID string, dstPath string) (*shared.DestinationDescription, error)
		// ReadExtentStats returns the extent stats for the given extent; it is not scalable, and therefore is to be used for debugging only
		ReadExtentStats(dstID string, extentID string) (*shared.ExtentStats, error)
		// ReadStoreExtentStats returns the extent stats for the given extent and store
		ReadStoreExtentStats(extentID string, storeID string) (*shared.ExtentStats, error)
		// ReadConsumerGroupExtent returns the consumer group extent stats corresponding to the given dst/cg/ext.
		ReadConsumerGroupExtent(dstID string, cgID string, extentID string) (*shared.ConsumerGroupExtent, error)
		// ListDestinations returns an list of adestinations
		ListDestinations() ([]*shared.DestinationDescription, error)
		// ListDestinationsPage returns an list of adestinations
		ListDestinationsPage(mReq *shared.ListDestinationsRequest) (*shared.ListDestinationsResult_, error)
		// ListDestinationsByUUID returns an iterator to the destinations
		ListDestinationsByUUID() ([]*shared.DestinationDescription, error)
		// ListDestinationExtentsByStatus lists extents by dstID/status
		// The returned type is a list of DestinationExtent objects as
		// opposed to list of Extent objects
		ListDestinationExtentsByStatus(dstID string, filterByStatus []shared.ExtentStatus) ([]*m.DestinationExtent, error)
		// ListExtentsByDstIDStatus lists extents dstID/Status
		ListExtentsByDstIDStatus(dstID string, filterByStatus []shared.ExtentStatus) ([]*shared.ExtentStats, error)
		// ListExtentsByInputIDStatus lists extents for dstID/InputUUID/Status
		ListExtentsByInputIDStatus(inputID string, status *shared.ExtentStatus) ([]*shared.ExtentStats, error)
		// ListExtentsByStoreIDStatus lists extents by storeID/Status
		ListExtentsByStoreIDStatus(storeID string, status *shared.ExtentStatus) ([]*shared.ExtentStats, error)
		// ListExtentsByReplicationStatus lists extents by storeID/ReplicationStatus
		ListExtentsByReplicationStatus(storeID string, status *shared.ExtentReplicaReplicationStatus) ([]*shared.ExtentStats, error)
		// ListExtentsByConsumerGroup lists all extents for the given destination / consumer group
		ListExtentsByConsumerGroup(dstID string, cgID string, filterByStatus []shared.ConsumerGroupExtentStatus) ([]*shared.ConsumerGroupExtent, error)
		// ListExtentsByConsumerGroupLite lists all extents for the given destination / consumer group
		// this api only returns a few interesting columns for each consumer group extent in the
		// result. For detailed info, see ListExtentsByConsumerGroup
		ListExtentsByConsumerGroupLite(dstID string, cgID string, filterByStatus []shared.ConsumerGroupExtentStatus) ([]*m.ConsumerGroupExtentLite, error)
		// CreateExtent creates a new extent for the given destination and marks the status as OPEN
		CreateExtent(dstID string, extentID string, inhostID string, storeIDs []string) (*shared.CreateExtentResult_, error)
		// CreateRemoteZoneExtent creates a new remote zone extent for the given destination and marks the status as OPEN
		CreateRemoteZoneExtent(dstID string, extentID string, inhostID string, storeIDs []string, originZone string, remoteExtentPrimaryStore string, consumerGroupVisibility string) (*shared.CreateExtentResult_, error)
		// AddExtentToConsumerGroup adds an open extent to consumer group for consumption
		AddExtentToConsumerGroup(dstID string, cgID string, extentID string, outHostID string, storeIDs []string) error
		// ListConsumerGroupsByDstID lists all consumer groups for a given destination uuid
		ListConsumerGroupsByDstID(dstID string) ([]*shared.ConsumerGroupDescription, error)
		// ListConsumerGroupsPage lists all consumer groups for a given destination uuid
		ListConsumerGroupsPage(mReq *shared.ListConsumerGroupRequest) (*shared.ListConsumerGroupResult_, error)
		// UpdateOutHost changes the out host for the consumer group extent
		UpdateOutHost(dstID string, cgID string, extentID string, outHostID string) error
		// DeleteConsumerGroup deletes the consumer group status
		DeleteConsumerGroup(dstID string, cgName string) error
		// SealExtent seals the extent if it was previously sealed
		SealExtent(dstID string, extentID string) error
		// UpdateDestinationDLQCursors updates the DLQCursor on a destination
		UpdateDestinationDLQCursors(dstID string, mergeBefore common.UnixNanoTime, purgeBefore common.UnixNanoTime) error
		// MoveExtent moves an extent
		MoveExtent(fromDstID, toDstID, extentID, cgUUID string) error
		// ReadConsumerGroup reads a consumer group
		ReadConsumerGroup(dstID, dstPath, cgUUID, cgName string) (*shared.ConsumerGroupDescription, error)
		// ReadConsumerGroupByUUID reads a consumer group by UUID
		ReadConsumerGroupByUUID(cgUUID string) (*shared.ConsumerGroupDescription, error)
		// UpdateExtentStatus updates the status of an extent
		UpdateExtentStatus(dstID, extID string, status shared.ExtentStatus) error
		// UpdateRemoteExtentPrimaryStore updates remoteExtentPrimaryStore
		UpdateRemoteExtentPrimaryStore(dstID string, extentID string, remoteExtentPrimaryStore string) (*m.UpdateExtentStatsResult_, error)
		// UpdateConsumerGroupExtentStatus updates the status of a consumer group extent
		UpdateConsumerGroupExtentStatus(cgID, extID string, status shared.ConsumerGroupExtentStatus) error
		// DeleteDestination marks a destination to be deleted
		DeleteDestination(dstID string) error
	}

	metadataMgrImpl struct {
		mClient m.TChanMetadataService
		logger  bark.Logger
	}
)

// NewMetadataMgr creates and returns a new instance of MetadataMgr
func NewMetadataMgr(mClient m.TChanMetadataService, m3Client metrics.Client, logger bark.Logger) MetadataMgr {
	return &metadataMgrImpl{
		mClient: mClient,
		logger:  logger,
	}
}

func (mm *metadataMgrImpl) ListDestinations() ([]*shared.DestinationDescription, error) {

	mReq := &shared.ListDestinationsRequest{
		Prefix: common.StringPtr("/"),
		Limit:  common.Int64Ptr(defaultPageSize),
	}

	var result []*shared.DestinationDescription

	for {
		resp, err := mm.mClient.ListDestinations(nil, mReq)
		if err != nil {
			return nil, err
		}

		result = append(result, resp.GetDestinations()...)

		if len(resp.GetNextPageToken()) == 0 {
			break
		} else {
			mReq.PageToken = resp.GetNextPageToken()
		}
	}

	return result, nil
}

func (mm *metadataMgrImpl) ListDestinationsPage(mReq *shared.ListDestinationsRequest) (*shared.ListDestinationsResult_, error) {
	return mm.mClient.ListDestinations(nil, mReq)
}

func (mm *metadataMgrImpl) ReadDestination(dstID string, dstPath string) (*shared.DestinationDescription, error) {

	mReq := &shared.ReadDestinationRequest{}

	if len(dstID) > 0 {
		mReq.DestinationUUID = common.StringPtr(dstID)
	}

	if len(dstPath) > 0 {
		mReq.Path = common.StringPtr(dstPath)
	}

	return mm.mClient.ReadDestination(nil, mReq)
}

func (mm *metadataMgrImpl) ListDestinationsByUUID() ([]*shared.DestinationDescription, error) {
	mReq := &shared.ListDestinationsByUUIDRequest{
		Limit: common.Int64Ptr(defaultPageSize),
	}

	var result []*shared.DestinationDescription

	for {
		resp, err := mm.mClient.ListDestinationsByUUID(nil, mReq)
		if err != nil {
			return nil, err
		}

		result = append(result, resp.GetDestinations()...)

		if len(resp.GetNextPageToken()) == 0 {
			break
		} else {
			mReq.PageToken = resp.GetNextPageToken()
		}
	}

	return result, nil
}

func (mm *metadataMgrImpl) ListDestinationExtentsByStatus(dstID string, filterByStatus []shared.ExtentStatus) ([]*m.DestinationExtent, error) {

	listReq := &m.ListDestinationExtentsRequest{
		DestinationUUID: common.StringPtr(dstID),
		Limit:           common.Int64Ptr(defaultPageSize),
	}

	filterLocally := len(filterByStatus) > 1
	if len(filterByStatus) == 1 {
		listReq.Status = common.MetadataExtentStatusPtr(filterByStatus[0])
	}

	startTime := time.Now()
	defer func() {

		elapsed := time.Since(startTime)

		if elapsed >= time.Second {
			mm.logger.WithFields(bark.Fields{
				common.TagDst:   dstID,
				`filter`:        filterByStatus,
				`latencyMillis`: float64(elapsed) / float64(time.Millisecond),
			}).Info("listDestinationExtentsByStatus high latency")
		}
	}()

	var result []*m.DestinationExtent
	for {
		listResp, err := mm.mClient.ListDestinationExtents(nil, listReq)
		if err != nil {
			return nil, err
		}

		if filterLocally {
			for _, ext := range listResp.GetExtents() {
			statusLoop:
				for _, status := range filterByStatus {
					if ext.GetStatus() == status {
						result = append(result, ext)
						break statusLoop
					}
				}
			}
		} else {
			result = append(result, listResp.GetExtents()...)
		}

		if len(listResp.GetNextPageToken()) == 0 {
			break
		} else {
			listReq.PageToken = listResp.GetNextPageToken()
		}
	}

	return result, nil
}

func (mm *metadataMgrImpl) ListExtentsByDstIDStatus(dstID string, filterByStatus []shared.ExtentStatus) ([]*shared.ExtentStats, error) {

	listReq := &shared.ListExtentsStatsRequest{
		DestinationUUID: common.StringPtr(dstID),
		Limit:           common.Int64Ptr(defaultPageSize),
	}

	filterLocally := len(filterByStatus) > 1
	if len(filterByStatus) == 1 {
		listReq.Status = common.MetadataExtentStatusPtr(filterByStatus[0])
	}

	startTime := time.Now()
	defer func() {

		elapsed := time.Since(startTime)

		if elapsed >= time.Second {
			mm.logger.WithFields(bark.Fields{
				common.TagDst:   dstID,
				`filter`:        filterByStatus,
				`latencyMillis`: float64(elapsed) / float64(time.Millisecond),
			}).Info("listExtentsByDstID high latency")
		}
	}()

	var result []*shared.ExtentStats
	for {
		listResp, err := mm.mClient.ListExtentsStats(nil, listReq)
		if err != nil {
			return nil, err
		}

		if filterLocally {
			for _, ext := range listResp.GetExtentStatsList() {
			statusLoop:
				for _, status := range filterByStatus {
					if ext.GetStatus() == status {
						result = append(result, ext)
						break statusLoop
					}
				}
			}
		} else {
			result = append(result, listResp.GetExtentStatsList()...)
		}

		if len(listResp.GetNextPageToken()) == 0 {
			break
		} else {
			listReq.PageToken = listResp.GetNextPageToken()
		}
	}

	return result, nil
}

func (mm *metadataMgrImpl) ListExtentsByInputIDStatus(inputID string, status *shared.ExtentStatus) ([]*shared.ExtentStats, error) {

	listReq := &m.ListInputHostExtentsStatsRequest{
		InputHostUUID: common.StringPtr(inputID),
		Status:        status,
	}

	resp, err := mm.mClient.ListInputHostExtentsStats(nil, listReq)
	if err != nil {
		return nil, err
	}

	return resp.GetExtentStatsList(), nil
}

func (mm *metadataMgrImpl) ListExtentsByStoreIDStatus(storeID string, status *shared.ExtentStatus) ([]*shared.ExtentStats, error) {

	listReq := &m.ListStoreExtentsStatsRequest{
		StoreUUID: common.StringPtr(storeID),
		Status:    status,
	}

	resp, err := mm.mClient.ListStoreExtentsStats(nil, listReq)
	if err != nil {
		return nil, err
	}

	return resp.GetExtentStatsList(), nil
}

func (mm *metadataMgrImpl) ListExtentsByReplicationStatus(storeID string, status *shared.ExtentReplicaReplicationStatus) ([]*shared.ExtentStats, error) {

	listReq := &m.ListStoreExtentsStatsRequest{
		StoreUUID:         common.StringPtr(storeID),
		ReplicationStatus: status,
	}

	resp, err := mm.mClient.ListStoreExtentsStats(nil, listReq)
	if err != nil {
		return nil, err
	}

	return resp.GetExtentStatsList(), nil
}

func (mm *metadataMgrImpl) ListExtentsByConsumerGroupLite(dstID string, cgID string, filterByStatus []shared.ConsumerGroupExtentStatus) ([]*m.ConsumerGroupExtentLite, error) {

	mReq := &m.ReadConsumerGroupExtentsLiteRequest{
		DestinationUUID:   common.StringPtr(dstID),
		ConsumerGroupUUID: common.StringPtr(cgID),
		MaxResults:        common.Int32Ptr(defaultPageSize),
	}

	filterLocally := len(filterByStatus) > 1
	if len(filterByStatus) == 1 {
		mReq.Status = common.MetadataConsumerGroupExtentStatusPtr(filterByStatus[0])
	}

	startTime := time.Now()
	defer func() {
		elapsed := time.Since(startTime)
		if elapsed >= time.Second {
			mm.logger.WithFields(bark.Fields{
				common.TagDst:   dstID,
				common.TagCnsm:  cgID,
				`filter`:        filterByStatus,
				`latencyMillis`: float64(elapsed) / float64(time.Millisecond),
			}).Info("listExtentsByConsumerGroupLite high latency")
		}
	}()

	var result []*m.ConsumerGroupExtentLite
	for {
		mResp, err := mm.mClient.ReadConsumerGroupExtentsLite(nil, mReq)
		if err != nil {
			return nil, err
		}

		if filterLocally {
			for _, ext := range mResp.GetExtents() {
			statusLoop:
				for _, status := range filterByStatus {
					if ext.GetStatus() == status {
						result = append(result, ext)
						break statusLoop
					}
				}
			}
		} else {
			result = append(result, mResp.GetExtents()...)
		}

		if len(mResp.GetNextPageToken()) == 0 {
			break
		} else {
			mReq.PageToken = mResp.GetNextPageToken()
		}
	}

	return result, nil
}

func (mm *metadataMgrImpl) ListExtentsByConsumerGroup(dstID string, cgID string, filterByStatus []shared.ConsumerGroupExtentStatus) ([]*shared.ConsumerGroupExtent, error) {

	mReq := &shared.ReadConsumerGroupExtentsRequest{
		DestinationUUID:   common.StringPtr(dstID),
		ConsumerGroupUUID: common.StringPtr(cgID),
		MaxResults:        common.Int32Ptr(defaultPageSize),
	}

	filterLocally := len(filterByStatus) > 1
	if len(filterByStatus) == 1 {
		mReq.Status = common.MetadataConsumerGroupExtentStatusPtr(filterByStatus[0])
	}

	startTime := time.Now()
	defer func() {
		elapsed := time.Since(startTime)
		if elapsed >= time.Second {
			mm.logger.WithFields(bark.Fields{
				common.TagDst:   dstID,
				common.TagCnsm:  cgID,
				`filter`:        filterByStatus,
				`latencyMillis`: float64(elapsed) / float64(time.Millisecond),
			}).Info("listExtentsByConsumerGroup high latency")
		}
	}()

	var result []*shared.ConsumerGroupExtent
	for {
		mResp, err := mm.mClient.ReadConsumerGroupExtents(nil, mReq)
		if err != nil {
			return nil, err
		}

		if filterLocally {
			for _, ext := range mResp.GetExtents() {
			statusLoop:
				for _, status := range filterByStatus {
					if ext.GetStatus() == status {
						result = append(result, ext)
						break statusLoop
					}
				}
			}
		} else {
			result = append(result, mResp.GetExtents()...)
		}

		if len(mResp.GetNextPageToken()) == 0 {
			break
		} else {
			mReq.PageToken = mResp.GetNextPageToken()
		}
	}

	return result, nil
}

// CreateExtent creates a new extent for the given destination and marks the status as OPEN
func (mm *metadataMgrImpl) CreateExtent(dstID string, extentID string, inhostID string, storeIDs []string) (*shared.CreateExtentResult_, error) {
	return mm.createExtentInternal(dstID, extentID, inhostID, storeIDs, ``, ``, ``)
}

// CreateRemoteZoneExtent creates a new remote zone extent for the given destination and marks the status as OPEN
func (mm *metadataMgrImpl) CreateRemoteZoneExtent(dstID string, extentID string, inhostID string, storeIDs []string, originZone string, remoteExtentPrimaryStore string, consumerGroupVisibility string) (*shared.CreateExtentResult_, error) {
	return mm.createExtentInternal(dstID, extentID, inhostID, storeIDs, originZone, remoteExtentPrimaryStore, consumerGroupVisibility)
}

func (mm *metadataMgrImpl) createExtentInternal(dstID string, extentID string, inhostID string, storeIDs []string, originZone string, remoteExtentPrimaryStore string, consumerGroupVisibility string) (*shared.CreateExtentResult_, error) {

	extent := &shared.Extent{
		ExtentUUID:               common.StringPtr(extentID),
		DestinationUUID:          common.StringPtr(dstID),
		InputHostUUID:            common.StringPtr(inhostID),
		StoreUUIDs:               storeIDs,
		OriginZone:               common.StringPtr(originZone),
		RemoteExtentPrimaryStore: common.StringPtr(remoteExtentPrimaryStore),
	}
	mReq := &shared.CreateExtentRequest{Extent: extent}
	if len(consumerGroupVisibility) > 0 {
		mReq.ConsumerGroupVisibility = common.StringPtr(consumerGroupVisibility)
	}

	res, err := mm.mClient.CreateExtent(nil, mReq)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (mm *metadataMgrImpl) AddExtentToConsumerGroup(dstID string, cgID string, extentID string, outHostID string, storeIDs []string) error {

	mReq := &shared.CreateConsumerGroupExtentRequest{
		DestinationUUID:   common.StringPtr(dstID),
		ExtentUUID:        common.StringPtr(extentID),
		ConsumerGroupUUID: common.StringPtr(cgID),
		OutputHostUUID:    common.StringPtr(outHostID),
		StoreUUIDs:        storeIDs,
	}

	return mm.mClient.CreateConsumerGroupExtent(nil, mReq)
}

func (mm *metadataMgrImpl) ListConsumerGroupsByDstID(dstID string) ([]*shared.ConsumerGroupDescription, error) {

	mReq := &shared.ListConsumerGroupRequest{
		DestinationUUID: common.StringPtr(dstID),
		Limit:           common.Int64Ptr(defaultPageSize),
	}

	var result []*shared.ConsumerGroupDescription

	for {
		resp, err := mm.mClient.ListConsumerGroups(nil, mReq)
		if err != nil {
			return nil, err
		}

		result = append(result, resp.GetConsumerGroups()...)

		if len(resp.GetNextPageToken()) == 0 {
			break
		} else {
			mReq.PageToken = resp.GetNextPageToken()
		}
	}

	return result, nil
}

func (mm *metadataMgrImpl) ListConsumerGroupsPage(mReq *shared.ListConsumerGroupRequest) (*shared.ListConsumerGroupResult_, error) {
	return mm.mClient.ListConsumerGroups(nil, mReq)
}

func (mm *metadataMgrImpl) UpdateOutHost(dstID string, cgID string, extentID string, outHostID string) error {

	mReq := &m.SetOutputHostRequest{
		DestinationUUID:   common.StringPtr(dstID),
		ExtentUUID:        common.StringPtr(extentID),
		ConsumerGroupUUID: common.StringPtr(cgID),
		OutputHostUUID:    common.StringPtr(outHostID),
	}

	return mm.mClient.SetOutputHost(nil, mReq)
}

func (mm *metadataMgrImpl) ReadExtentStats(dstID string, extentID string) (*shared.ExtentStats, error) {

	mReq := &m.ReadExtentStatsRequest{
		DestinationUUID: common.StringPtr(dstID),
		ExtentUUID:      common.StringPtr(extentID),
	}

	result, err := mm.mClient.ReadExtentStats(nil, mReq)
	if err != nil {
		return nil, err
	}

	return result.GetExtentStats(), nil
}

func (mm *metadataMgrImpl) ReadConsumerGroupExtent(dstID string, cgID string, extentID string) (*shared.ConsumerGroupExtent, error) {

	mReq := &m.ReadConsumerGroupExtentRequest{
		DestinationUUID:   common.StringPtr(dstID),
		ConsumerGroupUUID: common.StringPtr(cgID),
		ExtentUUID:        common.StringPtr(extentID),
	}

	result, err := mm.mClient.ReadConsumerGroupExtent(nil, mReq)
	if err != nil {
		return nil, err
	}

	return result.GetExtent(), nil
}

func (mm *metadataMgrImpl) ReadStoreExtentStats(extentID string, storeID string) (*shared.ExtentStats, error) {
	mReq := &m.ReadStoreExtentReplicaStatsRequest{
		StoreUUID:  common.StringPtr(storeID),
		ExtentUUID: common.StringPtr(extentID),
	}

	result, err := mm.mClient.ReadStoreExtentReplicaStats(nil, mReq)
	if err != nil {
		return nil, err
	}
	return result.GetExtent(), nil
}

func (mm *metadataMgrImpl) SealExtent(dstID string, extentID string) error {

	mReq := &m.SealExtentRequest{
		DestinationUUID: common.StringPtr(dstID),
		ExtentUUID:      common.StringPtr(extentID),
	}

	return mm.mClient.SealExtent(nil, mReq)
}

func (mm *metadataMgrImpl) DeleteConsumerGroup(dstID string, cgName string) error {

	mReq := &shared.DeleteConsumerGroupRequest{
		DestinationUUID:   common.StringPtr(dstID),
		ConsumerGroupName: common.StringPtr(cgName),
	}
	return mm.mClient.DeleteConsumerGroup(nil, mReq)
}

func (mm *metadataMgrImpl) ReadConsumerGroup(dstID, dstPath, cgUUID, cgName string) (cgDesc *shared.ConsumerGroupDescription, err error) {

	mReq := &shared.ReadConsumerGroupRequest{}

	if len(dstID) > 0 {
		mReq.DestinationUUID = common.StringPtr(dstID)
	}

	if len(dstPath) > 0 {
		mReq.DestinationPath = common.StringPtr(dstPath)
	}

	if len(cgUUID) > 0 {
		mReq.ConsumerGroupUUID = common.StringPtr(cgUUID)
	}

	if len(cgName) > 0 {
		mReq.ConsumerGroupName = common.StringPtr(cgName)
	}

	cgDesc, err = mm.mClient.ReadConsumerGroup(nil, mReq)
	if err != nil {
		return nil, err
	}

	return cgDesc, nil
}

func (mm *metadataMgrImpl) ReadConsumerGroupByUUID(cgUUID string) (cgDesc *shared.ConsumerGroupDescription, err error) {

	mReq := &shared.ReadConsumerGroupRequest{
		ConsumerGroupUUID: common.StringPtr(cgUUID),
	}

	cgDesc, err = mm.mClient.ReadConsumerGroupByUUID(nil, mReq)
	if err != nil {
		return nil, err
	}

	return cgDesc, nil
}

func (mm *metadataMgrImpl) UpdateDestinationDLQCursors(dstID string, mergeBefore common.UnixNanoTime, purgeBefore common.UnixNanoTime) error {

	mReq := m.NewUpdateDestinationDLQCursorsRequest()
	mReq.DestinationUUID = common.StringPtr(dstID)

	if mergeBefore >= 0 {
		mReq.DLQMergeBefore = common.Int64Ptr(int64(mergeBefore))
	}

	if purgeBefore >= 0 {
		mReq.DLQPurgeBefore = common.Int64Ptr(int64(purgeBefore))
	}

	_, err := mm.mClient.UpdateDestinationDLQCursors(nil, mReq)
	if err != nil {
		return err
	}

	return nil
}

func (mm *metadataMgrImpl) MoveExtent(fromDstID, toDstID, extentID, cgUUID string) error {

	mReq := &m.MoveExtentRequest{
		DestinationUUID:             common.StringPtr(fromDstID),
		NewDestinationUUID_:         common.StringPtr(toDstID),
		ExtentUUID:                  common.StringPtr(extentID),
		ConsumerGroupVisibilityUUID: common.StringPtr(cgUUID),
	}

	return mm.mClient.MoveExtent(nil, mReq)
}

func (mm *metadataMgrImpl) UpdateExtentStatus(dstID, extID string, status shared.ExtentStatus) error {

	mReq := &m.UpdateExtentStatsRequest{
		DestinationUUID: common.StringPtr(dstID),
		ExtentUUID:      common.StringPtr(extID),
		Status:          shared.ExtentStatusPtr(status),
	}

	_, err := mm.mClient.UpdateExtentStats(nil, mReq)
	if err != nil {
		return err
	}
	return nil
}

func (mm *metadataMgrImpl) UpdateRemoteExtentPrimaryStore(dstID string, extentID string, remoteExtentPrimaryStore string) (*m.UpdateExtentStatsResult_, error) {
	mReq := &m.UpdateExtentStatsRequest{
		DestinationUUID:          common.StringPtr(dstID),
		ExtentUUID:               common.StringPtr(extentID),
		RemoteExtentPrimaryStore: common.StringPtr(remoteExtentPrimaryStore),
	}

	if len(remoteExtentPrimaryStore) == 0 {
		return nil, &shared.BadRequestError{
			Message: "remoteExtentPrimaryStore is empty",
		}
	}

	return mm.mClient.UpdateExtentStats(nil, mReq)
}

func (mm *metadataMgrImpl) UpdateConsumerGroupExtentStatus(cgID, extID string, status shared.ConsumerGroupExtentStatus) error {

	mReq := &shared.UpdateConsumerGroupExtentStatusRequest{
		ConsumerGroupUUID: common.StringPtr(cgID),
		ExtentUUID:        common.StringPtr(extID),
		Status:            shared.ConsumerGroupExtentStatusPtr(status),
	}

	return mm.mClient.UpdateConsumerGroupExtentStatus(nil, mReq)
}

func (mm *metadataMgrImpl) DeleteDestination(dstID string) error {
	mReq := &m.DeleteDestinationUUIDRequest{
		UUID: common.StringPtr(dstID),
	}
	return mm.mClient.DeleteDestinationUUID(nil, mReq)
}
