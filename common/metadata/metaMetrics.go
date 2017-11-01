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

package metadata

import (
	"time"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"
)

const (
	pointQueryHighLatencyThreshold = 3 * time.Second
	listQueryHighLatencyThreshold  = 8 * time.Second
)

// metadataMetricsMgr Implements TChanMetadataServiceClient interface
type metadataMetricsMgr struct {
	meta m.TChanMetadataService
	m3   metrics.Client
	log  bark.Logger
}

// NewMetadataMetricsMgr creates an instance of metadataMetricsMgr that collects/emits metrics
func NewMetadataMetricsMgr(metaClient m.TChanMetadataService, m3Client metrics.Client, logger bark.Logger) m.TChanMetadataService {

	return &metadataMetricsMgr{
		meta: metaClient,
		m3:   m3Client,
		log:  logger.WithField(common.TagModule, `metametrics`),
	}
}

func (m *metadataMetricsMgr) ListEntityOps(ctx thrift.Context, request *m.ListEntityOpsRequest) (result *m.ListEntityOpsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListEntityOpsScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ListEntityOps(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListEntityOpsScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > listQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			`EntityUUID`: request.GetEntityUUID(),
			`EntityName`: request.GetEntityName(),
			`EntityType`: request.GetEntityType(),
		}).Warn("ListEntityOps: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataListEntityOpsScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) HostAddrToUUID(ctx thrift.Context, request string) (result string, err error) {

	m.m3.IncCounter(metrics.MetadataHostAddrToUUIDScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.HostAddrToUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataHostAddrToUUIDScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			`HostAddr`: request,
		}).Warn("HostAddrToUUID: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataHostAddrToUUIDScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ListAllConsumerGroups(ctx thrift.Context, request *shared.ListConsumerGroupRequest) (result *shared.ListConsumerGroupResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListAllConsumerGroupsScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ListAllConsumerGroups(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListAllConsumerGroupsScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > listQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDstPth: request.GetDestinationPath(),
			common.TagCnsPth: request.GetConsumerGroupName(),
			common.TagDst:    request.GetDestinationUUID(),
		}).Warn("ListAllConsumerGroups: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataListAllConsumerGroupsScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ListConsumerGroups(ctx thrift.Context, request *shared.ListConsumerGroupRequest) (result *shared.ListConsumerGroupResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListConsumerGroupsScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ListConsumerGroups(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListConsumerGroupsScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > listQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDstPth: request.GetDestinationPath(),
			common.TagCnsPth: request.GetConsumerGroupName(),
			common.TagDst:    request.GetDestinationUUID(),
		}).Warn("ListConsumerGroups: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataListConsumerGroupsScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ListConsumerGroupsUUID(ctx thrift.Context, request *shared.ListConsumerGroupsUUIDRequest) (result *shared.ListConsumerGroupsUUIDResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListConsumerGroupsUUIDScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ListConsumerGroupsUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListConsumerGroupsUUIDScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > listQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst: request.GetDestinationUUID(),
		}).Warn("ListConsumerGroupsUUID: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataListConsumerGroupsUUIDScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ListDestinations(ctx thrift.Context, request *shared.ListDestinationsRequest) (result *shared.ListDestinationsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListDestinationsScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ListDestinations(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListDestinationsScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > listQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			`Prefix`:        request.GetPrefix(),
			`MultiZoneOnly`: request.GetMultiZoneOnly(),
		}).Warn("ListDestinations: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataListDestinationsScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ListDestinationsByUUID(ctx thrift.Context, request *shared.ListDestinationsByUUIDRequest) (result *shared.ListDestinationsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListDestinationsByUUIDScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ListDestinationsByUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListDestinationsByUUIDScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > listQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			`MultiZoneOnly`:            request.GetMultiZoneOnly(),
			`ValidateAgainstPathTable`: request.GetValidateAgainstPathTable(),
		}).Warn("ListDestinationsByUUID: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataListDestinationsByUUIDScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ListDestinationExtents(ctx thrift.Context, request *m.ListDestinationExtentsRequest) (result *m.ListDestinationExtentsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListDestinationExtentsScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ListDestinationExtents(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListDestinationExtentsScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > listQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst:    request.GetDestinationUUID(),
			common.TagStatus: request.GetStatus(),
		}).Warn("ListDestinationExtents: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataListDestinationExtentsScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ListExtentsStats(ctx thrift.Context, request *shared.ListExtentsStatsRequest) (result *shared.ListExtentsStatsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListExtentsStatsScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ListExtentsStats(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListExtentsStatsScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > listQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst:      request.GetDestinationUUID(),
			common.TagStatus:   request.GetStatus(),
			`LocalExtentsOnly`: request.GetLocalExtentsOnly(),
		}).Warn("ListExtentsStats: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataListExtentsStatsScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ListHosts(ctx thrift.Context, request *m.ListHostsRequest) (result *m.ListHostsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListHostsScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ListHosts(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListHostsScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > listQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			`HostType`: request.GetHostType(),
		}).Warn("ListHosts: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataListHostsScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ListInputHostExtentsStats(ctx thrift.Context, request *m.ListInputHostExtentsStatsRequest) (result *m.ListInputHostExtentsStatsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListInputHostExtentsStatsScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ListInputHostExtentsStats(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListInputHostExtentsStatsScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > listQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst:    request.GetDestinationUUID(),
			common.TagIn:     request.GetInputHostUUID(),
			common.TagStatus: request.GetStatus(),
		}).Warn("ListInputHostExtentsStats: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataListInputHostExtentsStatsScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ListStoreExtentsStats(ctx thrift.Context, request *m.ListStoreExtentsStatsRequest) (result *m.ListStoreExtentsStatsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListStoreExtentsStatsScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ListStoreExtentsStats(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListStoreExtentsStatsScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > listQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagStor:      request.GetStoreUUID(),
			common.TagStatus:    request.GetStatus(),
			`ReplicationStatus`: request.GetReplicationStatus(),
		}).Warn("ListStoreExtentsStats: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataListStoreExtentsStatsScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ReadConsumerGroup(ctx thrift.Context, request *shared.ReadConsumerGroupRequest) (result *shared.ConsumerGroupDescription, err error) {

	m.m3.IncCounter(metrics.MetadataReadConsumerGroupScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ReadConsumerGroup(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadConsumerGroupScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDstPth: request.GetDestinationPath(),
			common.TagCnsPth: request.GetConsumerGroupName(),
		}).Warn("ReadConsumerGroup: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataReadConsumerGroupScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ReadConsumerGroupByUUID(ctx thrift.Context, request *shared.ReadConsumerGroupRequest) (result *shared.ConsumerGroupDescription, err error) {

	m.m3.IncCounter(metrics.MetadataReadConsumerGroupByUUIDScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ReadConsumerGroupByUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadConsumerGroupByUUIDScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagCnsPth: request.GetConsumerGroupName(),
		}).Warn("ReadConsumerGroupByUUID: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataReadConsumerGroupByUUIDScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ReadConsumerGroupExtent(ctx thrift.Context, request *m.ReadConsumerGroupExtentRequest) (result *m.ReadConsumerGroupExtentResult_, err error) {

	m.m3.IncCounter(metrics.MetadataReadConsumerGroupExtentScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ReadConsumerGroupExtent(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadConsumerGroupExtentScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst:  request.GetDestinationUUID(),
			common.TagCnsm: request.GetConsumerGroupUUID(),
			common.TagExt:  request.GetExtentUUID(),
		}).Warn("ReadConsumerGroupExtent: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataReadConsumerGroupExtentScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ReadConsumerGroupExtents(ctx thrift.Context, request *shared.ReadConsumerGroupExtentsRequest) (result *shared.ReadConsumerGroupExtentsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataReadConsumerGroupExtentsScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ReadConsumerGroupExtents(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadConsumerGroupExtentsScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > listQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst:    request.GetDestinationUUID(),
			common.TagCnsm:   request.GetConsumerGroupUUID(),
			common.TagOut:    request.GetOutputHostUUID(),
			common.TagStatus: request.GetStatus(),
		}).Warn("ReadConsumerGroupExtents: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataReadConsumerGroupExtentsScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ReadConsumerGroupExtentsLite(ctx thrift.Context, request *m.ReadConsumerGroupExtentsLiteRequest) (result *m.ReadConsumerGroupExtentsLiteResult_, err error) {

	m.m3.IncCounter(metrics.MetadataReadConsumerGroupExtentsLiteScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ReadConsumerGroupExtentsLite(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadConsumerGroupExtentsLiteScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > listQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst:    request.GetDestinationUUID(),
			common.TagCnsm:   request.GetConsumerGroupUUID(),
			common.TagOut:    request.GetOutputHostUUID(),
			common.TagStatus: request.GetStatus(),
		}).Warn("ReadConsumerGroupExtentsLite: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataReadConsumerGroupExtentsLiteScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ReadConsumerGroupExtentsByExtUUID(ctx thrift.Context, request *m.ReadConsumerGroupExtentsByExtUUIDRequest) (result *m.ReadConsumerGroupExtentsByExtUUIDResult_, err error) {

	m.m3.IncCounter(metrics.MetadataReadConsumerGroupExtentsByExtUUIDScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ReadConsumerGroupExtentsByExtUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadConsumerGroupExtentsByExtUUIDScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > listQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagExt: request.GetExtentUUID(),
		}).Warn("ReadConsumerGroupExtents: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataReadConsumerGroupExtentsByExtUUIDScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ReadDestination(ctx thrift.Context, request *shared.ReadDestinationRequest) (result *shared.DestinationDescription, err error) {

	m.m3.IncCounter(metrics.MetadataReadDestinationScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ReadDestination(ctx, request)

	if err != nil {
		if _, ok := err.(*shared.EntityNotExistsError); !ok {
			m.m3.IncCounter(metrics.MetadataReadDestinationScope, metrics.MetadataFailures)
		}
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDstPth: request.GetPath(),
			common.TagDst:    request.GetDestinationUUID(),
		}).Warn("ReadDestination: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataReadDestinationScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ReadExtentStats(ctx thrift.Context, request *m.ReadExtentStatsRequest) (result *m.ReadExtentStatsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataReadExtentStatsScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ReadExtentStats(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadExtentStatsScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst: request.GetDestinationUUID(),
			common.TagExt: request.GetExtentUUID(),
		}).Warn("ReadExtentStats: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataReadExtentStatsScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) UUIDToHostAddr(ctx thrift.Context, request string) (result string, err error) {

	m.m3.IncCounter(metrics.MetadataUUIDToHostAddrScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.UUIDToHostAddr(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUUIDToHostAddrScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			`UUID`: request,
		}).Warn("UUIDToHostAddr: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataUUIDToHostAddrScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) UpdateServiceConfig(ctx thrift.Context, request *m.UpdateServiceConfigRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataUpdateServiceConfigScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.UpdateServiceConfig(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUpdateServiceConfigScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.Warn("UpdateServiceConfig: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataUpdateServiceConfigScope, metrics.MetadataLatency, latency)

	return err
}

func (m *metadataMetricsMgr) CreateConsumerGroup(ctx thrift.Context, request *shared.CreateConsumerGroupRequest) (result *shared.ConsumerGroupDescription, err error) {

	m.m3.IncCounter(metrics.MetadataCreateConsumerGroupScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.CreateConsumerGroup(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataCreateConsumerGroupScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDstPth: request.GetDestinationPath(),
			common.TagCnsPth: request.GetConsumerGroupName(),
		}).Warn("CreateConsumerGroup: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataCreateConsumerGroupScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) CreateConsumerGroupUUID(ctx thrift.Context, request *shared.CreateConsumerGroupUUIDRequest) (result *shared.ConsumerGroupDescription, err error) {

	m.m3.IncCounter(metrics.MetadataCreateConsumerGroupUUIDScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.CreateConsumerGroupUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataCreateConsumerGroupUUIDScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagCnsm: request.GetConsumerGroupUUID(),
		}).Warn("CreateConsumerGroupUUID: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataCreateConsumerGroupUUIDScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) CreateConsumerGroupExtent(ctx thrift.Context, request *shared.CreateConsumerGroupExtentRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataCreateConsumerGroupExtentScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.CreateConsumerGroupExtent(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataCreateConsumerGroupExtentScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst:  request.GetDestinationUUID(),
			common.TagExt:  request.GetExtentUUID(),
			common.TagCnsm: request.GetConsumerGroupUUID(),
		}).Warn("CreateConsumerGroupExtent: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataCreateConsumerGroupExtentScope, metrics.MetadataLatency, latency)

	return err
}

func (m *metadataMetricsMgr) CreateDestination(ctx thrift.Context, request *shared.CreateDestinationRequest) (result *shared.DestinationDescription, err error) {

	m.m3.IncCounter(metrics.MetadataCreateDestinationScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.CreateDestination(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataCreateDestinationScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDstPth: request.GetPath(),
		}).Warn("CreateDestination: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataCreateDestinationScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) CreateDestinationUUID(ctx thrift.Context, request *shared.CreateDestinationUUIDRequest) (result *shared.DestinationDescription, err error) {

	m.m3.IncCounter(metrics.MetadataCreateDestinationUUIDScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.CreateDestinationUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataCreateDestinationUUIDScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst: request.GetDestinationUUID(),
		}).Warn("CreateDestinationUUID: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataCreateDestinationUUIDScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) CreateExtent(ctx thrift.Context, request *shared.CreateExtentRequest) (result *shared.CreateExtentResult_, err error) {

	m.m3.IncCounter(metrics.MetadataCreateExtentScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.CreateExtent(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataCreateExtentScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagExt: request.GetExtent().GetExtentUUID(),
		}).Warn("CreateExtent: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataCreateExtentScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) CreateHostInfo(ctx thrift.Context, request *m.CreateHostInfoRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataCreateHostInfoScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.CreateHostInfo(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataCreateHostInfoScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			`Hostname`: request.GetHostname(),
		}).Warn("CreateHostInfo: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataCreateHostInfoScope, metrics.MetadataLatency, latency)

	return err
}

func (m *metadataMetricsMgr) CreateServiceConfig(ctx thrift.Context, request *m.CreateServiceConfigRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataCreateServiceConfigScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.CreateServiceConfig(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataCreateServiceConfigScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.Warn("CreateServiceConfig: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataCreateServiceConfigScope, metrics.MetadataLatency, latency)

	return err
}

func (m *metadataMetricsMgr) DeleteConsumerGroup(ctx thrift.Context, request *shared.DeleteConsumerGroupRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataDeleteConsumerGroupScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.DeleteConsumerGroup(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataDeleteConsumerGroupScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst:    request.GetDestinationUUID(),
			common.TagDstPth: request.GetDestinationPath(),
			common.TagCnsPth: request.GetConsumerGroupName(),
		}).Warn("DeleteConsumerGroup: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataDeleteConsumerGroupScope, metrics.MetadataLatency, latency)

	return err
}

func (m *metadataMetricsMgr) DeleteConsumerGroupUUID(ctx thrift.Context, request *m.DeleteConsumerGroupUUIDRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataDeleteConsumerGroupUUIDScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.DeleteConsumerGroupUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataDeleteConsumerGroupUUIDScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagCnsm: request.GetUUID(),
		}).Warn("DeleteConsumerGroupUUID: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataDeleteConsumerGroupUUIDScope, metrics.MetadataLatency, latency)

	return err
}

func (m *metadataMetricsMgr) DeleteDestination(ctx thrift.Context, request *shared.DeleteDestinationRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataDeleteDestinationScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.DeleteDestination(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataDeleteDestinationScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDstPth: request.GetPath(),
		}).Warn("DeleteDestination: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataDeleteDestinationScope, metrics.MetadataLatency, latency)

	return err
}

func (m *metadataMetricsMgr) DeleteDestinationUUID(ctx thrift.Context, request *m.DeleteDestinationUUIDRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataDeleteDestinationUUIDScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.DeleteDestinationUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataDeleteDestinationUUIDScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst: request.GetUUID(),
		}).Warn("DeleteDestinationUUID: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataDeleteDestinationUUIDScope, metrics.MetadataLatency, latency)

	return err
}

func (m *metadataMetricsMgr) DeleteHostInfo(ctx thrift.Context, request *m.DeleteHostInfoRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataDeleteHostInfoScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.DeleteHostInfo(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataDeleteHostInfoScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			`Hostname`: request.GetHostname(),
		}).Warn("DeleteHostInfo: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataDeleteHostInfoScope, metrics.MetadataLatency, latency)

	return err
}

func (m *metadataMetricsMgr) DeleteServiceConfig(ctx thrift.Context, request *m.DeleteServiceConfigRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataDeleteServiceConfigScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.DeleteServiceConfig(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataDeleteServiceConfigScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.Warn("DeleteServiceConfig: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataDeleteServiceConfigScope, metrics.MetadataLatency, latency)

	return err
}

func (m *metadataMetricsMgr) MoveExtent(ctx thrift.Context, request *m.MoveExtentRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataMoveExtentScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.MoveExtent(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataMoveExtentScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst:        request.GetDestinationUUID(),
			common.TagExt:        request.GetExtentUUID(),
			`NewDestinationUUID`: request.GetNewDestinationUUID_(),
		}).Warn("MoveExtent: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataMoveExtentScope, metrics.MetadataLatency, latency)

	return err
}

func (m *metadataMetricsMgr) ReadHostInfo(ctx thrift.Context, request *m.ReadHostInfoRequest) (result *m.ReadHostInfoResult_, err error) {

	m.m3.IncCounter(metrics.MetadataReadHostInfoScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ReadHostInfo(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadHostInfoScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			`Hostname`: request.GetHostname(),
		}).Warn("ReadHostInfo: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataReadHostInfoScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ReadServiceConfig(ctx thrift.Context, request *m.ReadServiceConfigRequest) (result *m.ReadServiceConfigResult_, err error) {

	m.m3.IncCounter(metrics.MetadataReadServiceConfigScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ReadServiceConfig(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadServiceConfigScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.Warn("ReadServiceConfig: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataReadServiceConfigScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) ReadStoreExtentReplicaStats(ctx thrift.Context, request *m.ReadStoreExtentReplicaStatsRequest) (result *m.ReadStoreExtentReplicaStatsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataReadStoreExtentReplicaStatsScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.ReadStoreExtentReplicaStats(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadStoreExtentReplicaStatsScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagStor: request.GetStoreUUID(),
			common.TagExt:  request.GetExtentUUID(),
		}).Warn("ReadStoreExtentReplicaStats: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataReadStoreExtentReplicaStatsScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) RegisterHostUUID(ctx thrift.Context, request *m.RegisterHostUUIDRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataRegisterHostUUIDScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.RegisterHostUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataRegisterHostUUIDScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			`HostUUID`: request.GetHostUUID(),
			`HostAddr`: request.GetHostAddr(),
			`HostName`: request.GetHostName(),
		}).Warn("RegisterHostUUID: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataRegisterHostUUIDScope, metrics.MetadataLatency, latency)

	return err
}

func (m *metadataMetricsMgr) SealExtent(ctx thrift.Context, request *m.SealExtentRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataSealExtentScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.SealExtent(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataSealExtentScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst: request.GetDestinationUUID(),
			common.TagExt: request.GetExtentUUID(),
		}).Warn("SealExtent: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataSealExtentScope, metrics.MetadataLatency, latency)

	return err
}

func (m *metadataMetricsMgr) SetAckOffset(ctx thrift.Context, request *shared.SetAckOffsetRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataSetAckOffsetScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.SetAckOffset(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataSetAckOffsetScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagExt:    request.GetExtentUUID(),
			common.TagCnsm:   request.GetConsumerGroupUUID(),
			common.TagStatus: request.GetStatus(),
		}).Warn("SetAckOffset: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataSetAckOffsetScope, metrics.MetadataLatency, latency)

	return err
}

func (m *metadataMetricsMgr) SetOutputHost(ctx thrift.Context, request *m.SetOutputHostRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataSetOutputHostScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.SetOutputHost(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataSetOutputHostScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst:  request.GetDestinationUUID(),
			common.TagExt:  request.GetExtentUUID(),
			common.TagCnsm: request.GetConsumerGroupUUID(),
			common.TagOut:  request.GetOutputHostUUID(),
		}).Warn("SetOutputHost: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataSetOutputHostScope, metrics.MetadataLatency, latency)

	return err
}

func (m *metadataMetricsMgr) UpdateConsumerGroup(ctx thrift.Context, request *shared.UpdateConsumerGroupRequest) (result *shared.ConsumerGroupDescription, err error) {

	m.m3.IncCounter(metrics.MetadataUpdateConsumerGroupScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.UpdateConsumerGroup(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUpdateConsumerGroupScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDstPth: request.GetDestinationPath(),
			common.TagCnsPth: request.GetConsumerGroupName(),
		}).Warn("UpdateConsumerGroup: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataUpdateConsumerGroupScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) UpdateConsumerGroupExtentStatus(ctx thrift.Context, request *shared.UpdateConsumerGroupExtentStatusRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataUpdateConsumerGroupExtentStatusScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.UpdateConsumerGroupExtentStatus(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUpdateConsumerGroupExtentStatusScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagCnsm:   request.GetConsumerGroupUUID(),
			common.TagExt:    request.GetExtentUUID(),
			common.TagStatus: request.GetStatus(),
		}).Warn("UpdateConsumerGroupExtentStatus: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataUpdateConsumerGroupExtentStatusScope, metrics.MetadataLatency, latency)

	return err
}

func (m *metadataMetricsMgr) UpdateDestination(ctx thrift.Context, request *shared.UpdateDestinationRequest) (result *shared.DestinationDescription, err error) {

	m.m3.IncCounter(metrics.MetadataUpdateDestinationScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.UpdateDestination(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUpdateDestinationScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst: request.GetDestinationUUID(),
		}).Warn("UpdateDestination: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataUpdateDestinationScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) UpdateDestinationDLQCursors(ctx thrift.Context, request *m.UpdateDestinationDLQCursorsRequest) (result *shared.DestinationDescription, err error) {

	m.m3.IncCounter(metrics.MetadataUpdateDestinationDLQCursorsScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.UpdateDestinationDLQCursors(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUpdateDestinationDLQCursorsScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst: request.GetDestinationUUID(),
		}).Warn("UpdateDestinationDLQCursors: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataUpdateDestinationDLQCursorsScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) UpdateExtentReplicaStats(ctx thrift.Context, request *m.UpdateExtentReplicaStatsRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataUpdateExtentReplicaStatsScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.UpdateExtentReplicaStats(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUpdateExtentReplicaStatsScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst: request.GetDestinationUUID(),
			common.TagExt: request.GetExtentUUID(),
			common.TagIn:  request.GetInputHostUUID(),
		}).Warn("UpdateExtentReplicaStats: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataUpdateExtentReplicaStatsScope, metrics.MetadataLatency, latency)

	return err
}

func (m *metadataMetricsMgr) UpdateExtentStats(ctx thrift.Context, request *m.UpdateExtentStatsRequest) (result *m.UpdateExtentStatsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataUpdateExtentStatsScope, metrics.MetadataRequests)

	t0 := time.Now()

	result, err = m.meta.UpdateExtentStats(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUpdateExtentStatsScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagDst: request.GetDestinationUUID(),
			common.TagExt: request.GetExtentUUID(),
		}).Warn("UpdateExtentStats: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataUpdateExtentStatsScope, metrics.MetadataLatency, latency)

	return result, err
}

func (m *metadataMetricsMgr) UpdateHostInfo(ctx thrift.Context, request *m.UpdateHostInfoRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataUpdateHostInfoScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.UpdateHostInfo(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUpdateHostInfoScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			`Hostname`: request.GetHostname(),
		}).Warn("UpdateHostInfo: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataUpdateHostInfoScope, metrics.MetadataLatency, latency)

	return err
}

func (m *metadataMetricsMgr) UpdateStoreExtentReplicaStats(ctx thrift.Context, request *m.UpdateStoreExtentReplicaStatsRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataUpdateStoreExtentReplicaStatsScope, metrics.MetadataRequests)

	t0 := time.Now()

	err = m.meta.UpdateStoreExtentReplicaStats(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUpdateStoreExtentReplicaStatsScope, metrics.MetadataFailures)
	}

	latency := time.Since(t0)

	if latency > pointQueryHighLatencyThreshold {

		m.log.WithFields(bark.Fields{
			common.TagExt:  request.GetExtentUUID(),
			common.TagStor: request.GetStoreUUID(),
		}).Warn("UpdateStoreExtentReplicaStats: high latency")
	}

	m.m3.RecordTimer(metrics.MetadataUpdateStoreExtentReplicaStatsScope, metrics.MetadataLatency, latency)

	return err
}
