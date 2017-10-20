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
	"github.com/uber/cherami-server/common/metrics"
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"
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
		log:  logger,
	}
}

func (m *metadataMetricsMgr) ListEntityOps(ctx thrift.Context, request *m.ListEntityOpsRequest) (result *m.ListEntityOpsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListEntityOpsScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataListEntityOpsScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ListEntityOps(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListEntityOpsScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) HostAddrToUUID(ctx thrift.Context, request string) (result string, err error) {

	m.m3.IncCounter(metrics.MetadataHostAddrToUUIDScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataHostAddrToUUIDScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.HostAddrToUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataHostAddrToUUIDScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ListAllConsumerGroups(ctx thrift.Context, request *shared.ListConsumerGroupRequest) (result *shared.ListConsumerGroupResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListAllConsumerGroupsScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataListAllConsumerGroupsScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ListAllConsumerGroups(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListAllConsumerGroupsScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ListConsumerGroups(ctx thrift.Context, request *shared.ListConsumerGroupRequest) (result *shared.ListConsumerGroupResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListConsumerGroupsScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataListConsumerGroupsScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ListConsumerGroups(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListConsumerGroupsScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ListConsumerGroupsUUID(ctx thrift.Context, request *shared.ListConsumerGroupsUUIDRequest) (result *shared.ListConsumerGroupsUUIDResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListConsumerGroupsUUIDScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataListConsumerGroupsUUIDScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ListConsumerGroupsUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListConsumerGroupsUUIDScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ListDestinations(ctx thrift.Context, request *shared.ListDestinationsRequest) (result *shared.ListDestinationsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListDestinationsScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataListDestinationsScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ListDestinations(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListDestinationsScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ListDestinationsByUUID(ctx thrift.Context, request *shared.ListDestinationsByUUIDRequest) (result *shared.ListDestinationsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListDestinationsByUUIDScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataListDestinationsByUUIDScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ListDestinationsByUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListDestinationsByUUIDScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ListDestinationExtents(ctx thrift.Context, request *m.ListDestinationExtentsRequest) (result *m.ListDestinationExtentsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListDestinationExtentsScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataListDestinationExtentsScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ListDestinationExtents(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListDestinationExtentsScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ListExtentsStats(ctx thrift.Context, request *shared.ListExtentsStatsRequest) (result *shared.ListExtentsStatsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListExtentsStatsScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataListExtentsStatsScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ListExtentsStats(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListExtentsStatsScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ListHosts(ctx thrift.Context, request *m.ListHostsRequest) (result *m.ListHostsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListHostsScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataListHostsScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ListHosts(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListHostsScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ListInputHostExtentsStats(ctx thrift.Context, request *m.ListInputHostExtentsStatsRequest) (result *m.ListInputHostExtentsStatsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListInputHostExtentsStatsScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataListInputHostExtentsStatsScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ListInputHostExtentsStats(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListInputHostExtentsStatsScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ListStoreExtentsStats(ctx thrift.Context, request *m.ListStoreExtentsStatsRequest) (result *m.ListStoreExtentsStatsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataListStoreExtentsStatsScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataListStoreExtentsStatsScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ListStoreExtentsStats(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataListStoreExtentsStatsScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ReadConsumerGroup(ctx thrift.Context, request *shared.ReadConsumerGroupRequest) (result *shared.ConsumerGroupDescription, err error) {

	m.m3.IncCounter(metrics.MetadataReadConsumerGroupScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataReadConsumerGroupScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ReadConsumerGroup(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadConsumerGroupScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ReadConsumerGroupByUUID(ctx thrift.Context, request *shared.ReadConsumerGroupRequest) (result *shared.ConsumerGroupDescription, err error) {

	m.m3.IncCounter(metrics.MetadataReadConsumerGroupByUUIDScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataReadConsumerGroupByUUIDScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ReadConsumerGroupByUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadConsumerGroupByUUIDScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ReadConsumerGroupExtent(ctx thrift.Context, request *m.ReadConsumerGroupExtentRequest) (result *m.ReadConsumerGroupExtentResult_, err error) {

	m.m3.IncCounter(metrics.MetadataReadConsumerGroupExtentScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataReadConsumerGroupExtentScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ReadConsumerGroupExtent(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadConsumerGroupExtentScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ReadConsumerGroupExtents(ctx thrift.Context, request *shared.ReadConsumerGroupExtentsRequest) (result *shared.ReadConsumerGroupExtentsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataReadConsumerGroupExtentsScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataReadConsumerGroupExtentsScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ReadConsumerGroupExtents(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadConsumerGroupExtentsScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ReadConsumerGroupExtentsLite(ctx thrift.Context, request *m.ReadConsumerGroupExtentsLiteRequest) (result *m.ReadConsumerGroupExtentsLiteResult_, err error) {

	m.m3.IncCounter(metrics.MetadataReadConsumerGroupExtentsLiteScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataReadConsumerGroupExtentsLiteScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ReadConsumerGroupExtentsLite(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadConsumerGroupExtentsLiteScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ReadConsumerGroupExtentsByExtUUID(ctx thrift.Context, request *m.ReadConsumerGroupExtentsByExtUUIDRequest) (result *m.ReadConsumerGroupExtentsByExtUUIDResult_, err error) {

	m.m3.IncCounter(metrics.MetadataReadConsumerGroupExtentsByExtUUIDScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataReadConsumerGroupExtentsByExtUUIDScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ReadConsumerGroupExtentsByExtUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadConsumerGroupExtentsByExtUUIDScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ReadDestination(ctx thrift.Context, request *shared.ReadDestinationRequest) (result *shared.DestinationDescription, err error) {

	m.m3.IncCounter(metrics.MetadataReadDestinationScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataReadDestinationScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ReadDestination(ctx, request)

	if err != nil {
		if _, ok := err.(*shared.EntityNotExistsError); !ok {
			m.m3.IncCounter(metrics.MetadataReadDestinationScope, metrics.MetadataFailures)
		}
	}

	return result, err
}

func (m *metadataMetricsMgr) ReadExtentStats(ctx thrift.Context, request *m.ReadExtentStatsRequest) (result *m.ReadExtentStatsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataReadExtentStatsScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataReadExtentStatsScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ReadExtentStats(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadExtentStatsScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) UUIDToHostAddr(ctx thrift.Context, request string) (result string, err error) {

	m.m3.IncCounter(metrics.MetadataUUIDToHostAddrScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataUUIDToHostAddrScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.UUIDToHostAddr(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUUIDToHostAddrScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) UpdateServiceConfig(ctx thrift.Context, request *m.UpdateServiceConfigRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataUpdateServiceConfigScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataUpdateServiceConfigScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.UpdateServiceConfig(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUpdateServiceConfigScope, metrics.MetadataFailures)
	}

	return err
}

func (m *metadataMetricsMgr) CreateConsumerGroup(ctx thrift.Context, request *shared.CreateConsumerGroupRequest) (result *shared.ConsumerGroupDescription, err error) {

	m.m3.IncCounter(metrics.MetadataCreateConsumerGroupScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataCreateConsumerGroupScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.CreateConsumerGroup(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataCreateConsumerGroupScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) CreateConsumerGroupUUID(ctx thrift.Context, request *shared.CreateConsumerGroupUUIDRequest) (result *shared.ConsumerGroupDescription, err error) {

	m.m3.IncCounter(metrics.MetadataCreateConsumerGroupUUIDScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataCreateConsumerGroupUUIDScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.CreateConsumerGroupUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataCreateConsumerGroupUUIDScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) CreateConsumerGroupExtent(ctx thrift.Context, request *shared.CreateConsumerGroupExtentRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataCreateConsumerGroupExtentScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataCreateConsumerGroupExtentScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.CreateConsumerGroupExtent(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataCreateConsumerGroupExtentScope, metrics.MetadataFailures)
	}

	return err
}

func (m *metadataMetricsMgr) CreateDestination(ctx thrift.Context, request *shared.CreateDestinationRequest) (result *shared.DestinationDescription, err error) {

	m.m3.IncCounter(metrics.MetadataCreateDestinationScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataCreateDestinationScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.CreateDestination(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataCreateDestinationScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) CreateDestinationUUID(ctx thrift.Context, request *shared.CreateDestinationUUIDRequest) (result *shared.DestinationDescription, err error) {

	m.m3.IncCounter(metrics.MetadataCreateDestinationUUIDScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataCreateDestinationUUIDScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.CreateDestinationUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataCreateDestinationUUIDScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) CreateExtent(ctx thrift.Context, request *shared.CreateExtentRequest) (result *shared.CreateExtentResult_, err error) {

	m.m3.IncCounter(metrics.MetadataCreateExtentScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataCreateExtentScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.CreateExtent(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataCreateExtentScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) CreateHostInfo(ctx thrift.Context, request *m.CreateHostInfoRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataCreateHostInfoScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataCreateHostInfoScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.CreateHostInfo(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataCreateHostInfoScope, metrics.MetadataFailures)
	}

	return err
}

func (m *metadataMetricsMgr) CreateServiceConfig(ctx thrift.Context, request *m.CreateServiceConfigRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataCreateServiceConfigScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataCreateServiceConfigScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.CreateServiceConfig(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataCreateServiceConfigScope, metrics.MetadataFailures)
	}

	return err
}

func (m *metadataMetricsMgr) DeleteConsumerGroup(ctx thrift.Context, request *shared.DeleteConsumerGroupRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataDeleteConsumerGroupScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataDeleteConsumerGroupScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.DeleteConsumerGroup(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataDeleteConsumerGroupScope, metrics.MetadataFailures)
	}

	return err
}

func (m *metadataMetricsMgr) DeleteConsumerGroupUUID(ctx thrift.Context, request *m.DeleteConsumerGroupUUIDRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataDeleteConsumerGroupUUIDScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataDeleteConsumerGroupUUIDScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.DeleteConsumerGroupUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataDeleteConsumerGroupUUIDScope, metrics.MetadataFailures)
	}

	return err
}

func (m *metadataMetricsMgr) DeleteDestination(ctx thrift.Context, request *shared.DeleteDestinationRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataDeleteDestinationScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataDeleteDestinationScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.DeleteDestination(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataDeleteDestinationScope, metrics.MetadataFailures)
	}

	return err
}

func (m *metadataMetricsMgr) DeleteDestinationUUID(ctx thrift.Context, request *m.DeleteDestinationUUIDRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataDeleteDestinationUUIDScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataDeleteDestinationUUIDScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.DeleteDestinationUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataDeleteDestinationUUIDScope, metrics.MetadataFailures)
	}

	return err
}

func (m *metadataMetricsMgr) DeleteHostInfo(ctx thrift.Context, request *m.DeleteHostInfoRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataDeleteHostInfoScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataDeleteHostInfoScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.DeleteHostInfo(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataDeleteHostInfoScope, metrics.MetadataFailures)
	}

	return err
}

func (m *metadataMetricsMgr) DeleteServiceConfig(ctx thrift.Context, request *m.DeleteServiceConfigRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataDeleteServiceConfigScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataDeleteServiceConfigScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.DeleteServiceConfig(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataDeleteServiceConfigScope, metrics.MetadataFailures)
	}

	return err
}

func (m *metadataMetricsMgr) MoveExtent(ctx thrift.Context, request *m.MoveExtentRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataMoveExtentScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataMoveExtentScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.MoveExtent(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataMoveExtentScope, metrics.MetadataFailures)
	}

	return err
}

func (m *metadataMetricsMgr) ReadHostInfo(ctx thrift.Context, request *m.ReadHostInfoRequest) (result *m.ReadHostInfoResult_, err error) {

	m.m3.IncCounter(metrics.MetadataReadHostInfoScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataReadHostInfoScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ReadHostInfo(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadHostInfoScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ReadServiceConfig(ctx thrift.Context, request *m.ReadServiceConfigRequest) (result *m.ReadServiceConfigResult_, err error) {

	m.m3.IncCounter(metrics.MetadataReadServiceConfigScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataReadServiceConfigScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ReadServiceConfig(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadServiceConfigScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) ReadStoreExtentReplicaStats(ctx thrift.Context, request *m.ReadStoreExtentReplicaStatsRequest) (result *m.ReadStoreExtentReplicaStatsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataReadStoreExtentReplicaStatsScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataReadStoreExtentReplicaStatsScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.ReadStoreExtentReplicaStats(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataReadStoreExtentReplicaStatsScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) RegisterHostUUID(ctx thrift.Context, request *m.RegisterHostUUIDRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataRegisterHostUUIDScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataRegisterHostUUIDScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.RegisterHostUUID(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataRegisterHostUUIDScope, metrics.MetadataFailures)
	}

	return err
}

func (m *metadataMetricsMgr) SealExtent(ctx thrift.Context, request *m.SealExtentRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataSealExtentScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataSealExtentScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.SealExtent(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataSealExtentScope, metrics.MetadataFailures)
	}

	return err
}

func (m *metadataMetricsMgr) SetAckOffset(ctx thrift.Context, request *shared.SetAckOffsetRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataSetAckOffsetScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataSetAckOffsetScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.SetAckOffset(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataSetAckOffsetScope, metrics.MetadataFailures)
	}

	return err
}

func (m *metadataMetricsMgr) SetOutputHost(ctx thrift.Context, request *m.SetOutputHostRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataSetOutputHostScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataSetOutputHostScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.SetOutputHost(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataSetOutputHostScope, metrics.MetadataFailures)
	}

	return err
}

func (m *metadataMetricsMgr) UpdateConsumerGroup(ctx thrift.Context, request *shared.UpdateConsumerGroupRequest) (result *shared.ConsumerGroupDescription, err error) {

	m.m3.IncCounter(metrics.MetadataUpdateConsumerGroupScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataUpdateConsumerGroupScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.UpdateConsumerGroup(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUpdateConsumerGroupScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) UpdateConsumerGroupExtentStatus(ctx thrift.Context, request *shared.UpdateConsumerGroupExtentStatusRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataUpdateConsumerGroupExtentStatusScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataUpdateConsumerGroupExtentStatusScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.UpdateConsumerGroupExtentStatus(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUpdateConsumerGroupExtentStatusScope, metrics.MetadataFailures)
	}

	return err
}

func (m *metadataMetricsMgr) UpdateDestination(ctx thrift.Context, request *shared.UpdateDestinationRequest) (result *shared.DestinationDescription, err error) {

	m.m3.IncCounter(metrics.MetadataUpdateDestinationScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataUpdateDestinationScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.UpdateDestination(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUpdateDestinationScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) UpdateDestinationDLQCursors(ctx thrift.Context, request *m.UpdateDestinationDLQCursorsRequest) (result *shared.DestinationDescription, err error) {

	m.m3.IncCounter(metrics.MetadataUpdateDestinationDLQCursorsScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataUpdateDestinationDLQCursorsScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.UpdateDestinationDLQCursors(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUpdateDestinationDLQCursorsScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) UpdateExtentReplicaStats(ctx thrift.Context, request *m.UpdateExtentReplicaStatsRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataUpdateExtentReplicaStatsScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataUpdateExtentReplicaStatsScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.UpdateExtentReplicaStats(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUpdateExtentReplicaStatsScope, metrics.MetadataFailures)
	}

	return err
}

func (m *metadataMetricsMgr) UpdateExtentStats(ctx thrift.Context, request *m.UpdateExtentStatsRequest) (result *m.UpdateExtentStatsResult_, err error) {

	m.m3.IncCounter(metrics.MetadataUpdateExtentStatsScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataUpdateExtentStatsScope, metrics.MetadataLatency)
	defer sw.Stop()

	result, err = m.meta.UpdateExtentStats(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUpdateExtentStatsScope, metrics.MetadataFailures)
	}

	return result, err
}

func (m *metadataMetricsMgr) UpdateHostInfo(ctx thrift.Context, request *m.UpdateHostInfoRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataUpdateHostInfoScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataUpdateHostInfoScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.UpdateHostInfo(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUpdateHostInfoScope, metrics.MetadataFailures)
	}

	return err
}

func (m *metadataMetricsMgr) UpdateStoreExtentReplicaStats(ctx thrift.Context, request *m.UpdateStoreExtentReplicaStatsRequest) (err error) {

	m.m3.IncCounter(metrics.MetadataUpdateStoreExtentReplicaStatsScope, metrics.MetadataRequests)
	sw := m.m3.StartTimer(metrics.MetadataUpdateStoreExtentReplicaStatsScope, metrics.MetadataLatency)
	defer sw.Stop()

	err = m.meta.UpdateStoreExtentReplicaStats(ctx, request)

	if err != nil {
		m.m3.IncCounter(metrics.MetadataUpdateStoreExtentReplicaStatsScope, metrics.MetadataFailures)
	}

	return err
}
