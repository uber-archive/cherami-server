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
	"time"

	"github.com/uber-common/bark"

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
		ReadConsumerGroupExtent(dstID string, cgID string, extentID string) (*m.ConsumerGroupExtent, error)
		// ListDestinations returns an list of adestinations
		ListDestinations() ([]*shared.DestinationDescription, error)
		// ListDestinationsPage returns an list of adestinations
		ListDestinationsPage(mReq *shared.ListDestinationsRequest) (*shared.ListDestinationsResult_, error)
		// ListDestinationsByUUID returns an iterator to the destinations
		ListDestinationsByUUID() ([]*shared.DestinationDescription, error)
		// ListExtentsByDstIDStatus lists extents dstID/Status
		ListExtentsByDstIDStatus(dstID string, filterByStatus []shared.ExtentStatus) ([]*shared.ExtentStats, error)
		// ListExtentsByInputIDStatus lists extents for dstID/InputUUID/Status
		ListExtentsByInputIDStatus(inputID string, status *shared.ExtentStatus) ([]*shared.ExtentStats, error)
		// ListExtentsByStoreIDStatus lists extents by storeID/Status
		ListExtentsByStoreIDStatus(storeID string, status *shared.ExtentStatus) ([]*shared.ExtentStats, error)
		// ListExtentsByReplicationStatus lists extents by storeID/ReplicationStatus
		ListExtentsByReplicationStatus(storeID string, status *shared.ExtentReplicaReplicationStatus) ([]*shared.ExtentStats, error)
		// ListExtentsByConsumerGroup lists all extents for the given destination / consumer group
		ListExtentsByConsumerGroup(dstID string, cgID string, filterByStatus []m.ConsumerGroupExtentStatus) ([]*m.ConsumerGroupExtent, error)
		// CreateExtent creates a new extent for the given destination and marks the status as OPEN
		CreateExtent(dstID string, extentID string, inhostID string, storeIDs []string) (*shared.CreateExtentResult_, error)
		// CreateRemoteZoneExtent creates a new remote zone extent for the given destination and marks the status as OPEN
		CreateRemoteZoneExtent(dstID string, extentID string, inhostID string, storeIDs []string, originZone string, remoteExtentPrimaryStore string) (*shared.CreateExtentResult_, error)
		// AddExtentToConsumerGroup adds an open extent to consumer group for consumption
		AddExtentToConsumerGroup(dstID string, cgID string, extentID string, outHostID string, storeIDs []string) error
		// ListConsumerGroupsByDstID lists all consumer groups for a given destination uuid
		ListConsumerGroupsByDstID(dstID string) ([]*shared.ConsumerGroupDescription, error)
		// ListConsumerGroupsPage lists all consumer groups for a given destination uuid
		ListConsumerGroupsPage(mReq *m.ListConsumerGroupRequest) (*m.ListConsumerGroupResult_, error)
		// UpdateOutHost changes the out host for the consumer group extent
		UpdateOutHost(dstID string, cgID string, extentID string, outHostID string) error
		// DeleteConsumerGroup deletes the consumer group status
		DeleteConsumerGroup(dstID string, cgName string) error
		// SealExtent seals the extent if it was previously sealed
		SealExtent(dstID string, extentID string) error
		// UpdateDestinationDLQCursors updates the DLQCursor on a destination
		UpdateDestinationDLQCursors(dstID string, mergeBefore UnixNanoTime, purgeBefore UnixNanoTime) error
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
		UpdateConsumerGroupExtentStatus(cgID, extID string, status m.ConsumerGroupExtentStatus) error
		// DeleteDestination marks a destination to be deleted
		DeleteDestination(dstID string) error
	}

	metadataMgrImpl struct {
		m3Client metrics.Client
		mClient  m.TChanMetadataService
		logger   bark.Logger
	}
)

// NewMetadataMgr creates and returns a new instance of MetadataMgr
func NewMetadataMgr(mClient m.TChanMetadataService, m3Client metrics.Client, logger bark.Logger) MetadataMgr {
	return &metadataMgrImpl{
		mClient:  mClient,
		m3Client: m3Client,
		logger:   logger,
	}
}

func (mm *metadataMgrImpl) ListDestinations() ([]*shared.DestinationDescription, error) {

	mm.m3Client.IncCounter(metrics.MetadataListDestinationsScope, metrics.MetadataRequests)

	sw := mm.m3Client.StartTimer(metrics.MetadataListDestinationsScope, metrics.MetadataLatency)
	defer sw.Stop()

	mReq := &shared.ListDestinationsRequest{
		Prefix: StringPtr("/"),
		Limit:  Int64Ptr(defaultPageSize),
	}

	var result []*shared.DestinationDescription

	for {
		resp, err := mm.mClient.ListDestinations(nil, mReq)
		if err != nil {
			mm.m3Client.IncCounter(metrics.MetadataListDestinationsScope, metrics.MetadataFailures)
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

	mm.m3Client.IncCounter(metrics.MetadataListDestinationsScope, metrics.MetadataRequests)

	sw := mm.m3Client.StartTimer(metrics.MetadataListDestinationsScope, metrics.MetadataLatency)
	defer sw.Stop()

	resp, err := mm.mClient.ListDestinations(nil, mReq)
	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataListDestinationsScope, metrics.MetadataFailures)
		return nil, err
	}

	return resp, nil
}

func (mm *metadataMgrImpl) ReadDestination(dstID string, dstPath string) (*shared.DestinationDescription, error) {

	mm.m3Client.IncCounter(metrics.MetadataReadDestinationScope, metrics.MetadataRequests)

	mReq := &m.ReadDestinationRequest{}

	if len(dstID) > 0 {
		mReq.DestinationUUID = StringPtr(dstID)
	}

	if len(dstPath) > 0 {
		mReq.Path = StringPtr(dstPath)
	}

	sw := mm.m3Client.StartTimer(metrics.MetadataReadDestinationScope, metrics.MetadataLatency)
	desc, err := mm.mClient.ReadDestination(nil, mReq)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*shared.EntityNotExistsError); !ok {
			mm.m3Client.IncCounter(metrics.MetadataReadDestinationScope, metrics.MetadataFailures)
		}
	}

	return desc, err
}

func (mm *metadataMgrImpl) ListDestinationsByUUID() ([]*shared.DestinationDescription, error) {
	mReq := &shared.ListDestinationsByUUIDRequest{
		Limit: Int64Ptr(defaultPageSize),
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

func (mm *metadataMgrImpl) ListExtentsByDstIDStatus(dstID string, filterByStatus []shared.ExtentStatus) ([]*shared.ExtentStats, error) {

	mm.m3Client.IncCounter(metrics.MetadataListExtentsStatsScope, metrics.MetadataRequests)

	listReq := &shared.ListExtentsStatsRequest{
		DestinationUUID: StringPtr(dstID),
		Limit:           Int64Ptr(defaultPageSize),
	}

	filterLocally := len(filterByStatus) > 1
	if len(filterByStatus) == 1 {
		listReq.Status = MetadataExtentStatusPtr(filterByStatus[0])
	}

	startTime := time.Now()
	defer func() {

		elapsed := time.Since(startTime)

		if elapsed >= time.Second {
			mm.logger.WithFields(bark.Fields{
				TagDst:          dstID,
				`filter`:        filterByStatus,
				`latencyMillis`: float64(elapsed) / float64(time.Millisecond),
			}).Info("listExtentsByDstID high latency")
		}

		mm.m3Client.RecordTimer(metrics.MetadataListExtentsStatsScope, metrics.MetadataLatency, elapsed)
	}()

	var result []*shared.ExtentStats
	for {
		listResp, err := mm.mClient.ListExtentsStats(nil, listReq)
		if err != nil {
			mm.m3Client.IncCounter(metrics.MetadataListExtentsStatsScope, metrics.MetadataFailures)
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

	mm.m3Client.IncCounter(metrics.MetadataListInputHostExtentsStatsScope, metrics.MetadataRequests)

	listReq := &m.ListInputHostExtentsStatsRequest{
		InputHostUUID: StringPtr(inputID),
		Status:        status,
	}

	sw := mm.m3Client.StartTimer(metrics.MetadataListInputHostExtentsStatsScope, metrics.MetadataLatency)
	resp, err := mm.mClient.ListInputHostExtentsStats(nil, listReq)
	sw.Stop()
	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataListInputHostExtentsStatsScope, metrics.MetadataFailures)
		return nil, err
	}

	return resp.GetExtentStatsList(), nil
}

func (mm *metadataMgrImpl) ListExtentsByStoreIDStatus(storeID string, status *shared.ExtentStatus) ([]*shared.ExtentStats, error) {

	mm.m3Client.IncCounter(metrics.MetadataListStoreExtentsStatsScope, metrics.MetadataRequests)

	listReq := &m.ListStoreExtentsStatsRequest{
		StoreUUID: StringPtr(storeID),
		Status:    status,
	}

	sw := mm.m3Client.StartTimer(metrics.MetadataListStoreExtentsStatsScope, metrics.MetadataLatency)
	resp, err := mm.mClient.ListStoreExtentsStats(nil, listReq)
	sw.Stop()
	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataListStoreExtentsStatsScope, metrics.MetadataFailures)
		return nil, err
	}

	return resp.GetExtentStatsList(), nil
}

func (mm *metadataMgrImpl) ListExtentsByReplicationStatus(storeID string, status *shared.ExtentReplicaReplicationStatus) ([]*shared.ExtentStats, error) {

	mm.m3Client.IncCounter(metrics.MetadataListStoreExtentsStatsScope, metrics.MetadataRequests)

	listReq := &m.ListStoreExtentsStatsRequest{
		StoreUUID:         StringPtr(storeID),
		ReplicationStatus: status,
	}

	sw := mm.m3Client.StartTimer(metrics.MetadataListStoreExtentsStatsScope, metrics.MetadataLatency)
	resp, err := mm.mClient.ListStoreExtentsStats(nil, listReq)
	sw.Stop()
	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataListStoreExtentsStatsScope, metrics.MetadataFailures)
		return nil, err
	}

	return resp.GetExtentStatsList(), nil
}

func (mm *metadataMgrImpl) ListExtentsByConsumerGroup(dstID string, cgID string, filterByStatus []m.ConsumerGroupExtentStatus) ([]*m.ConsumerGroupExtent, error) {

	mm.m3Client.IncCounter(metrics.MetadataReadConsumerGroupExtentScope, metrics.MetadataRequests)

	mReq := &m.ReadConsumerGroupExtentsRequest{
		DestinationUUID:   StringPtr(dstID),
		ConsumerGroupUUID: StringPtr(cgID),
		MaxResults:        Int32Ptr(defaultPageSize),
	}

	filterLocally := len(filterByStatus) > 1
	if len(filterByStatus) == 1 {
		mReq.Status = MetadataConsumerGroupExtentStatusPtr(filterByStatus[0])
	}

	startTime := time.Now()
	defer func() {
		elapsed := time.Since(startTime)
		mm.m3Client.RecordTimer(metrics.MetadataReadConsumerGroupExtentScope, metrics.MetadataLatency, elapsed)
		if elapsed >= time.Second {
			mm.logger.WithFields(bark.Fields{
				TagDst:          dstID,
				TagCnsm:         cgID,
				`filter`:        filterByStatus,
				`latencyMillis`: float64(elapsed) / float64(time.Millisecond),
			}).Info("listExtentsByConsumerGroup high latency")
		}
	}()

	var result []*m.ConsumerGroupExtent
	for {
		mResp, err := mm.mClient.ReadConsumerGroupExtents(nil, mReq)
		if err != nil {
			mm.m3Client.IncCounter(metrics.MetadataReadConsumerGroupExtentScope, metrics.MetadataFailures)
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
	return mm.createExtentInternal(dstID, extentID, inhostID, storeIDs, ``, ``)
}

// CreateRemoteZoneExtent creates a new remote zone extent for the given destination and marks the status as OPEN
func (mm *metadataMgrImpl) CreateRemoteZoneExtent(dstID string, extentID string, inhostID string, storeIDs []string, originZone string, remoteExtentPrimaryStore string) (*shared.CreateExtentResult_, error) {
	return mm.createExtentInternal(dstID, extentID, inhostID, storeIDs, originZone, remoteExtentPrimaryStore)
}

func (mm *metadataMgrImpl) createExtentInternal(dstID string, extentID string, inhostID string, storeIDs []string, originZone string, remoteExtentPrimaryStore string) (*shared.CreateExtentResult_, error) {

	mm.m3Client.IncCounter(metrics.MetadataCreateExtentScope, metrics.MetadataRequests)

	extent := &shared.Extent{
		ExtentUUID:               StringPtr(extentID),
		DestinationUUID:          StringPtr(dstID),
		InputHostUUID:            StringPtr(inhostID),
		StoreUUIDs:               storeIDs,
		OriginZone:               StringPtr(originZone),
		RemoteExtentPrimaryStore: StringPtr(remoteExtentPrimaryStore),
	}
	mReq := &shared.CreateExtentRequest{Extent: extent}

	sw := mm.m3Client.StartTimer(metrics.MetadataCreateExtentScope, metrics.MetadataLatency)
	res, err := mm.mClient.CreateExtent(nil, mReq)
	sw.Stop()
	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataCreateExtentScope, metrics.MetadataFailures)
		return nil, err
	}

	return res, nil
}

func (mm *metadataMgrImpl) AddExtentToConsumerGroup(dstID string, cgID string, extentID string, outHostID string, storeIDs []string) error {

	mm.m3Client.IncCounter(metrics.MetadataCreateConsumerGroupExtentScope, metrics.MetadataRequests)

	mReq := &m.CreateConsumerGroupExtentRequest{
		DestinationUUID:   StringPtr(dstID),
		ExtentUUID:        StringPtr(extentID),
		ConsumerGroupUUID: StringPtr(cgID),
		OutputHostUUID:    StringPtr(outHostID),
		StoreUUIDs:        storeIDs,
	}

	sw := mm.m3Client.StartTimer(metrics.MetadataCreateConsumerGroupExtentScope, metrics.MetadataLatency)
	err := mm.mClient.CreateConsumerGroupExtent(nil, mReq)
	sw.Stop()
	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataCreateConsumerGroupExtentScope, metrics.MetadataFailures)
		return err
	}

	return err
}

func (mm *metadataMgrImpl) ListConsumerGroupsByDstID(dstID string) ([]*shared.ConsumerGroupDescription, error) {

	mm.m3Client.IncCounter(metrics.MetadataListConsumerGroupsScope, metrics.MetadataRequests)

	mReq := &m.ListConsumerGroupRequest{
		DestinationUUID: StringPtr(dstID),
		Limit:           Int64Ptr(defaultPageSize),
	}

	var result []*shared.ConsumerGroupDescription

	sw := mm.m3Client.StartTimer(metrics.MetadataListConsumerGroupsScope, metrics.MetadataLatency)
	defer sw.Stop()
	for {
		resp, err := mm.mClient.ListConsumerGroups(nil, mReq)
		if err != nil {
			mm.m3Client.IncCounter(metrics.MetadataListConsumerGroupsScope, metrics.MetadataFailures)
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

func (mm *metadataMgrImpl) ListConsumerGroupsPage(mReq *m.ListConsumerGroupRequest) (*m.ListConsumerGroupResult_, error) {

	mm.m3Client.IncCounter(metrics.MetadataListConsumerGroupsScope, metrics.MetadataRequests)

	sw := mm.m3Client.StartTimer(metrics.MetadataListConsumerGroupsScope, metrics.MetadataLatency)
	defer sw.Stop()

	resp, err := mm.mClient.ListConsumerGroups(nil, mReq)
	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataListConsumerGroupsScope, metrics.MetadataFailures)
		return nil, err
	}

	return resp, nil
}

func (mm *metadataMgrImpl) UpdateOutHost(dstID string, cgID string, extentID string, outHostID string) error {

	mm.m3Client.IncCounter(metrics.MetadataSetOutputHostScope, metrics.MetadataRequests)

	mReq := &m.SetOutputHostRequest{
		DestinationUUID:   StringPtr(dstID),
		ExtentUUID:        StringPtr(extentID),
		ConsumerGroupUUID: StringPtr(cgID),
		OutputHostUUID:    StringPtr(outHostID),
	}

	sw := mm.m3Client.StartTimer(metrics.MetadataSetOutputHostScope, metrics.MetadataLatency)
	err := mm.mClient.SetOutputHost(nil, mReq)
	sw.Stop()
	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataSetOutputHostScope, metrics.MetadataFailures)
		return err
	}

	return err
}

func (mm *metadataMgrImpl) ReadExtentStats(dstID string, extentID string) (*shared.ExtentStats, error) {

	mm.m3Client.IncCounter(metrics.MetadataReadExtentStatsScope, metrics.MetadataRequests)

	mReq := &m.ReadExtentStatsRequest{
		DestinationUUID: StringPtr(dstID),
		ExtentUUID:      StringPtr(extentID),
	}

	sw := mm.m3Client.StartTimer(metrics.MetadataReadExtentStatsScope, metrics.MetadataLatency)
	result, err := mm.mClient.ReadExtentStats(nil, mReq)
	sw.Stop()
	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataReadExtentStatsScope, metrics.MetadataFailures)
		return nil, err
	}

	return result.GetExtentStats(), nil
}

func (mm *metadataMgrImpl) ReadConsumerGroupExtent(dstID string, cgID string, extentID string) (*m.ConsumerGroupExtent, error) {

	mm.m3Client.IncCounter(metrics.MetadataReadConsumerGroupExtentScope, metrics.MetadataRequests)

	mReq := &m.ReadConsumerGroupExtentRequest{
		DestinationUUID:   StringPtr(dstID),
		ConsumerGroupUUID: StringPtr(cgID),
		ExtentUUID:        StringPtr(extentID),
	}

	sw := mm.m3Client.StartTimer(metrics.MetadataReadConsumerGroupExtentScope, metrics.MetadataLatency)
	result, err := mm.mClient.ReadConsumerGroupExtent(nil, mReq)
	sw.Stop()

	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataReadConsumerGroupExtentScope, metrics.MetadataFailures)
		return nil, err
	}

	return result.GetExtent(), nil
}

func (mm *metadataMgrImpl) ReadStoreExtentStats(extentID string, storeID string) (*shared.ExtentStats, error) {
	mm.m3Client.IncCounter(metrics.MetadataReadStoreExtentReplicaStatsScope, metrics.MetadataRequests)
	mReq := &m.ReadStoreExtentReplicaStatsRequest{
		StoreUUID:  StringPtr(storeID),
		ExtentUUID: StringPtr(extentID),
	}

	sw := mm.m3Client.StartTimer(metrics.MetadataReadStoreExtentReplicaStatsScope, metrics.MetadataLatency)
	result, err := mm.mClient.ReadStoreExtentReplicaStats(nil, mReq)
	sw.Stop()

	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataReadStoreExtentReplicaStatsScope, metrics.MetadataFailures)
		return nil, err
	}
	return result.GetExtent(), nil
}

func (mm *metadataMgrImpl) SealExtent(dstID string, extentID string) error {

	mm.m3Client.IncCounter(metrics.MetadataSealExtentScope, metrics.MetadataRequests)

	mReq := &m.SealExtentRequest{
		DestinationUUID: StringPtr(dstID),
		ExtentUUID:      StringPtr(extentID),
	}

	sw := mm.m3Client.StartTimer(metrics.MetadataSealExtentScope, metrics.MetadataLatency)
	err := mm.mClient.SealExtent(nil, mReq)
	sw.Stop()

	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataSealExtentScope, metrics.MetadataFailures)
		return err
	}

	return nil
}

func (mm *metadataMgrImpl) DeleteConsumerGroup(dstID string, cgName string) error {

	mm.m3Client.IncCounter(metrics.MetadataDeleteConsumerGroupScope, metrics.MetadataRequests)

	mReq := &shared.DeleteConsumerGroupRequest{
		DestinationUUID:   StringPtr(dstID),
		ConsumerGroupName: StringPtr(cgName),
	}
	sw := mm.m3Client.StartTimer(metrics.MetadataDeleteConsumerGroupScope, metrics.MetadataLatency)
	err := mm.mClient.DeleteConsumerGroup(nil, mReq)
	sw.Stop()

	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataDeleteConsumerGroupScope, metrics.MetadataFailures)
		return err
	}

	return nil
}

func (mm *metadataMgrImpl) ReadConsumerGroup(dstID, dstPath, cgUUID, cgName string) (cgDesc *shared.ConsumerGroupDescription, err error) {

	mm.m3Client.IncCounter(metrics.MetadataReadConsumerGroupScope, metrics.MetadataRequests)

	mReq := &m.ReadConsumerGroupRequest{}

	if len(dstID) > 0 {
		mReq.DestinationUUID = StringPtr(dstID)
	}

	if len(dstPath) > 0 {
		mReq.DestinationPath = StringPtr(dstPath)
	}

	if len(cgUUID) > 0 {
		mReq.ConsumerGroupUUID = StringPtr(cgUUID)
	}

	if len(cgName) > 0 {
		mReq.ConsumerGroupName = StringPtr(cgName)
	}

	sw := mm.m3Client.StartTimer(metrics.MetadataReadConsumerGroupScope, metrics.MetadataLatency)
	cgDesc, err = mm.mClient.ReadConsumerGroup(nil, mReq)
	sw.Stop()

	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataReadConsumerGroupScope, metrics.MetadataFailures)
		return nil, err
	}

	return cgDesc, nil
}

func (mm *metadataMgrImpl) ReadConsumerGroupByUUID(cgUUID string) (cgDesc *shared.ConsumerGroupDescription, err error) {

	mm.m3Client.IncCounter(metrics.MetadataReadConsumerGroupByUUIDScope, metrics.MetadataRequests)

	mReq := &m.ReadConsumerGroupRequest{
		ConsumerGroupUUID: StringPtr(cgUUID),
	}

	sw := mm.m3Client.StartTimer(metrics.MetadataReadConsumerGroupByUUIDScope, metrics.MetadataLatency)
	cgDesc, err = mm.mClient.ReadConsumerGroupByUUID(nil, mReq)
	sw.Stop()

	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataReadConsumerGroupByUUIDScope, metrics.MetadataFailures)
		return nil, err
	}

	return cgDesc, nil
}

func (mm *metadataMgrImpl) UpdateDestinationDLQCursors(dstID string, mergeBefore UnixNanoTime, purgeBefore UnixNanoTime) error {

	mm.m3Client.IncCounter(metrics.MetadataUpdateDestinationDLQCursorsScope, metrics.MetadataRequests)

	mReq := m.NewUpdateDestinationDLQCursorsRequest()
	mReq.DestinationUUID = StringPtr(dstID)

	if mergeBefore >= 0 {
		mReq.DLQMergeBefore = Int64Ptr(int64(mergeBefore))
	}

	if purgeBefore >= 0 {
		mReq.DLQPurgeBefore = Int64Ptr(int64(purgeBefore))
	}

	sw := mm.m3Client.StartTimer(metrics.MetadataUpdateDestinationDLQCursorsScope, metrics.MetadataLatency)
	_, err := mm.mClient.UpdateDestinationDLQCursors(nil, mReq)
	sw.Stop()

	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataUpdateDestinationDLQCursorsScope, metrics.MetadataFailures)
		return err
	}

	return nil
}

func (mm *metadataMgrImpl) MoveExtent(fromDstID, toDstID, extentID, cgUUID string) error {

	mm.m3Client.IncCounter(metrics.MetadataMoveExtentScope, metrics.MetadataRequests)

	mReq := &m.MoveExtentRequest{
		DestinationUUID:             StringPtr(fromDstID),
		NewDestinationUUID_:         StringPtr(toDstID),
		ExtentUUID:                  StringPtr(extentID),
		ConsumerGroupVisibilityUUID: StringPtr(cgUUID),
	}

	sw := mm.m3Client.StartTimer(metrics.MetadataMoveExtentScope, metrics.MetadataLatency)
	err := mm.mClient.MoveExtent(nil, mReq)
	sw.Stop()

	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataMoveExtentScope, metrics.MetadataFailures)
		return err
	}

	return nil
}

func (mm *metadataMgrImpl) UpdateExtentStatus(dstID, extID string, status shared.ExtentStatus) error {

	mm.m3Client.IncCounter(metrics.MetadataUpdateExtentStatsScope, metrics.MetadataRequests)

	mReq := &m.UpdateExtentStatsRequest{
		DestinationUUID: StringPtr(dstID),
		ExtentUUID:      StringPtr(extID),
		Status:          shared.ExtentStatusPtr(status),
	}

	sw := mm.m3Client.StartTimer(metrics.MetadataUpdateExtentStatsScope, metrics.MetadataLatency)
	_, err := mm.mClient.UpdateExtentStats(nil, mReq)
	sw.Stop()

	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataUpdateExtentStatsScope, metrics.MetadataFailures)
		return err
	}

	return nil
}

func (mm *metadataMgrImpl) UpdateRemoteExtentPrimaryStore(dstID string, extentID string, remoteExtentPrimaryStore string) (*m.UpdateExtentStatsResult_, error) {
	mm.m3Client.IncCounter(metrics.MetadataUpdateExtentStatsScope, metrics.MetadataRequests)

	mReq := &m.UpdateExtentStatsRequest{
		DestinationUUID:          StringPtr(dstID),
		ExtentUUID:               StringPtr(extentID),
		RemoteExtentPrimaryStore: StringPtr(remoteExtentPrimaryStore),
	}

	if len(remoteExtentPrimaryStore) == 0 {
		mm.m3Client.IncCounter(metrics.MetadataUpdateExtentStatsScope, metrics.MetadataFailures)
		return nil, &shared.BadRequestError{
			Message: "remoteExtentPrimaryStore is empty",
		}
	}

	sw := mm.m3Client.StartTimer(metrics.MetadataUpdateExtentStatsScope, metrics.MetadataLatency)
	res, err := mm.mClient.UpdateExtentStats(nil, mReq)
	sw.Stop()
	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataUpdateExtentStatsScope, metrics.MetadataFailures)
		return nil, err
	}

	return res, err
}

func (mm *metadataMgrImpl) UpdateConsumerGroupExtentStatus(cgID, extID string, status m.ConsumerGroupExtentStatus) error {

	mm.m3Client.IncCounter(metrics.MetadataUpdateConsumerGroupExtentStatusScope, metrics.MetadataRequests)

	mReq := &m.UpdateConsumerGroupExtentStatusRequest{
		ConsumerGroupUUID: StringPtr(cgID),
		ExtentUUID:        StringPtr(extID),
		Status:            m.ConsumerGroupExtentStatusPtr(status),
	}

	sw := mm.m3Client.StartTimer(metrics.MetadataUpdateConsumerGroupExtentStatusScope, metrics.MetadataLatency)
	err := mm.mClient.UpdateConsumerGroupExtentStatus(nil, mReq)
	sw.Stop()

	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataUpdateConsumerGroupExtentStatusScope, metrics.MetadataFailures)
		return err
	}

	return nil
}

func (mm *metadataMgrImpl) DeleteDestination(dstID string) error {

	mm.m3Client.IncCounter(metrics.MetadataDeleteDestinationUUIDScope, metrics.MetadataRequests)

	mReq := &m.DeleteDestinationUUIDRequest{
		UUID: StringPtr(dstID),
	}

	sw := mm.m3Client.StartTimer(metrics.MetadataDeleteDestinationUUIDScope, metrics.MetadataLatency)
	err := mm.mClient.DeleteDestinationUUID(nil, mReq)
	sw.Stop()

	if err != nil {
		mm.m3Client.IncCounter(metrics.MetadataDeleteDestinationUUIDScope, metrics.MetadataFailures)
		return err
	}

	return nil
}
