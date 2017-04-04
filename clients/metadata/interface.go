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
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

type (
	// Client exposes API for metadata access
	Client interface {
		UUIDToHostAddr(uuid string) (string, error)
		HostAddrToUUID(hostAddr string) (string, error)
		ReadExtentStats(request *m.ReadExtentStatsRequest) (*m.ReadExtentStatsResult_, error)
		ListExtentsStats(request *shared.ListExtentsStatsRequest) (*shared.ListExtentsStatsResult_, error)
		ListStoreExtentsStats(request *m.ListStoreExtentsStatsRequest) (*m.ListStoreExtentsStatsResult_, error)
		ReadDestination(request *shared.ReadDestinationRequest) (*shared.DestinationDescription, error)
		ListDestinations(request *shared.ListDestinationsRequest) (*shared.ListDestinationsResult_, error)
		ListDestinationsByUUID(request *shared.ListDestinationsByUUIDRequest) (*shared.ListDestinationsResult_, error)
		ReadConsumerGroupExtents(request *shared.ReadConsumerGroupExtentsRequest) (*shared.ReadConsumerGroupExtentsResult_, error)
		ReadConsumerGroupExtent(request *m.ReadConsumerGroupExtentRequest) (*m.ReadConsumerGroupExtentResult_, error)
		ReadConsumerGroupExtentsByExtUUID(request *m.ReadConsumerGroupExtentsByExtUUIDRequest) (*m.ReadConsumerGroupExtentsByExtUUIDResult_, error)
		ReadConsumerGroupByUUID(request *shared.ReadConsumerGroupRequest) (*shared.ConsumerGroupDescription, error)
		ReadConsumerGroup(request *shared.ReadConsumerGroupRequest) (*shared.ConsumerGroupDescription, error)
		ListHosts(request *m.ListHostsRequest) (*m.ListHostsResult_, error)
		ListConsumerGroups(request *shared.ListConsumerGroupRequest) (*shared.ListConsumerGroupResult_, error)
		ListAllConsumerGroups(request *shared.ListConsumerGroupRequest) (*shared.ListConsumerGroupResult_, error)
		ReadServiceConfig(request *m.ReadServiceConfigRequest) (*m.ReadServiceConfigResult_, error)
		UpdateServiceConfig(request *m.UpdateServiceConfigRequest) error
		DeleteServiceConfig(request *m.DeleteServiceConfigRequest) error
		ListEntityOps(request *m.ListEntityOpsRequest) (*m.ListEntityOpsResult_, error)
	}
)
