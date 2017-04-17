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

	"github.com/pborman/uuid"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

const (
	maxExtentsToConsumeForDstPlain = 8
	extentsToConsumePerRemoteZone  = 4
)

func (s *McpSuite) TestCGExtentSelectorWithNoExtents() {
	dstPath := s.generateName("/test/selector")
	dstDesc, err := s.createDestination(dstPath, shared.DestinationType_PLAIN)
	s.Nil(err, "Failed to create destination")

	cgPath := s.generateName("/test/selector-cg")
	cgDesc, err := s.createConsumerGroup(dstPath, cgPath)
	s.Nil(err, "Failed to create consumer group")

	context := s.mcp.context
	cgExtents := newCGExtentsByCategory()
	extents, avail, err := selectNextExtentsToConsume(context, dstDesc, cgDesc, cgExtents, metrics.GetOutputHostsScope)
	s.Nil(err, "selectNextExtentsToConsume() error")
	s.Equal(1, len(extents), "Extent not created when no consummable extent")
	s.Equal(1, avail, "Extent not created when no consummable extent")
}

func (s *McpSuite) TestCGExtentSelectorWithNoExtentsKafka() {
	dstPath := s.generateName("/test/selector")
	dstDesc, err := s.createDestination(dstPath, shared.DestinationType_KAFKA)
	s.Nil(err, "Failed to create destination")

	cgPath := s.generateName("/test/selector-cg")
	cgDesc, err := s.createConsumerGroup(dstPath, cgPath)
	s.Nil(err, "Failed to create consumer group")

	context := s.mcp.context
	cgExtents := newCGExtentsByCategory()
	extents, avail, err := selectNextExtentsToConsumeKafka(context, dstDesc, cgDesc, cgExtents, metrics.GetOutputHostsScope)
	s.Nil(err, "selectNextExtentsToConsumeKafka() error")
	s.Equal(numKafkaExtentsForDstKafka, len(extents), "Extent not created when no consummable extent")
	s.Equal(numKafkaExtentsForDstKafka, avail, "Extent not created when no consummable extent")
}

func (s *McpSuite) TestCGExtentSelectorWithNoConsumableExtents() {
	dstPath := s.generateName("/test/selector")
	dstDesc, err := s.createDestination(dstPath, shared.DestinationType_PLAIN)
	s.Nil(err, "Failed to create destination")

	cgPath := s.generateName("/test/selector-cg")
	cgDesc, err := s.createConsumerGroup(dstPath, cgPath)
	s.Nil(err, "Failed to create consumer group")

	stores := []string{uuid.New(), uuid.New(), uuid.New()}
	for _, id := range stores {
		s.mockrpm.Add(common.StoreServiceName, id, "127.2.2.2:0")
	}

	nExtents := 0
	context := s.mcp.context
	dstID := dstDesc.GetDestinationUUID()
	inhosts, _ := s.mockrpm.GetHosts(common.InputServiceName)

	// add some extents with unhealthy store
	for i := 0; i < 15; i++ {
		extID := uuid.New()
		storeIDs := []string{uuid.New(), uuid.New(), uuid.New()}
		context.mm.CreateExtent(dstID, extID, inhosts[0].UUID, storeIDs)
		if i%3 == 0 {
			context.mm.SealExtent(dstID, extID)
		}
		nExtents++
	}

	cgExtents := newCGExtentsByCategory()

	// add some healthy consumed extents
	for i := 0; i < 2; i++ {
		extID := uuid.New()
		storeIDs := []string{stores[0], stores[1], stores[2]}
		context.mm.CreateExtent(dstID, extID, inhosts[0].UUID, storeIDs)
		cgExtents.consumed[extID] = struct{}{}
	}

	extents, avail, err := selectNextExtentsToConsume(context, dstDesc, cgDesc, cgExtents, metrics.GetOutputHostsScope)
	s.Nil(err, "selectNextExtentsToConsume() error")
	s.Equal(1, len(extents), "Extent not created when no consummable extent")
	s.Equal(1, avail, "Extent not created when no consummable extent")
}

func (s *McpSuite) TestCGExtentSelectorWithNoConsumableExtentsKafka() {
	dstPath := s.generateName("/test/selector")
	dstDesc, err := s.createDestination(dstPath, shared.DestinationType_KAFKA)
	s.Nil(err, "Failed to create destination")

	cgPath := s.generateName("/test/selector-cg")
	cgDesc, err := s.createConsumerGroup(dstPath, cgPath)
	s.Nil(err, "Failed to create consumer group")

	stores := []string{uuid.New(), uuid.New(), uuid.New()}
	for _, id := range stores {
		s.mockrpm.Add(common.StoreServiceName, id, "127.2.2.2:0")
	}

	nExtents := 0
	context := s.mcp.context
	dstID := dstDesc.GetDestinationUUID()
	inhosts, _ := s.mockrpm.GetHosts(common.InputServiceName)

	// add some DLQ extents with unhealthy store
	for i := 0; i < 5; i++ {
		extID := uuid.New()
		storeIDs := []string{uuid.New(), uuid.New(), uuid.New()}
		context.mm.CreateExtent(dstID, extID, inhosts[0].UUID, storeIDs)
		context.mm.SealExtent(dstID, extID)
		context.mm.MoveExtent(dstID, dstID, extID, cgDesc.GetConsumerGroupUUID())
		nExtents++
	}

	cgExtents := newCGExtentsByCategory()

	// add some healthy consumed DLQ extents
	for i := 0; i < 2; i++ {
		extID := uuid.New()
		storeIDs := []string{stores[0], stores[1], stores[2]}
		context.mm.CreateExtent(dstID, extID, inhosts[0].UUID, storeIDs)
		context.mm.SealExtent(dstID, extID)
		context.mm.MoveExtent(dstID, dstID, extID, cgDesc.GetConsumerGroupUUID())
		cgExtents.consumed[extID] = struct{}{}
	}

	extents, avail, err := selectNextExtentsToConsumeKafka(context, dstDesc, cgDesc, cgExtents, metrics.GetOutputHostsScope)
	s.Nil(err, "selectNextExtentsToConsume() error")
	s.Equal(numKafkaExtentsForDstKafka, len(extents), "Extent not created when no consummable DLQ extent")
	s.Equal(numKafkaExtentsForDstKafka, avail, "Extent not created when no consummable DLQ extent")
}

func (s *McpSuite) TestCGExtentSelectorHonorsCreatedTime() {
	dstPath := s.generateName("/test/selector")
	dstDesc, err := s.createDestination(dstPath, shared.DestinationType_PLAIN)
	s.Nil(err, "Failed to create destination")

	cgPath := s.generateName("/test/selector-cg")
	cgDesc, err := s.createConsumerGroup(dstPath, cgPath)
	s.Nil(err, "Failed to create consumer group")

	hosts, _ := s.mockrpm.GetHosts(common.StoreServiceName)
	stores := []string{hosts[0].UUID, hosts[1].UUID, hosts[2].UUID}
	inhosts, _ := s.mockrpm.GetHosts(common.InputServiceName)

	nExtents := 0
	context := s.mcp.context
	dstID := dstDesc.GetDestinationUUID()
	extents := make([]string, 0, 10)

	for i := 0; i < 10; i++ {
		extID := uuid.New()
		context.mm.CreateExtent(dstID, extID, inhosts[0].UUID, stores)
		if i%3 == 0 {
			context.mm.SealExtent(dstID, extID)
		}
		extents = append(extents, extID)
		// sleep to advance createdTime for next extent
		time.Sleep(2 * time.Millisecond)
		nExtents++
	}

	cgExtents := newCGExtentsByCategory()

	gotExtents, avail, err := selectNextExtentsToConsume(context, dstDesc, cgDesc, cgExtents, metrics.GetOutputHostsScope)
	s.Nil(err, "selectNextExtentsToConsume() error")

	s.Equal(maxExtentsToConsumeForDstPlain, len(gotExtents), "Wrong number of next extents to consume")
	s.Equal(nExtents, avail, "Wrong number of available extents")

	for i := 0; i < len(gotExtents); i++ {
		s.Equal(extents[i], gotExtents[i].GetExtentUUID(), "Extents not served in time order")
	}
}

func (s *McpSuite) TestCGExtentSelectorHonorsCreatedTimeKafka() {
	dstPath := s.generateName("/test/selector")
	dstDesc, err := s.createDestination(dstPath, shared.DestinationType_KAFKA)
	s.Nil(err, "Failed to create destination")

	cgPath := s.generateName("/test/selector-cg")
	cgDesc, err := s.createConsumerGroup(dstPath, cgPath)
	s.Nil(err, "Failed to create consumer group")

	hosts, _ := s.mockrpm.GetHosts(common.StoreServiceName)
	stores := []string{hosts[0].UUID, hosts[1].UUID, hosts[2].UUID}
	inhosts, _ := s.mockrpm.GetHosts(common.InputServiceName)

	nExtents := 0
	context := s.mcp.context
	dstID := dstDesc.GetDestinationUUID()
	extents := make([]string, 0, 10)

	for i := 0; i < 5; i++ {
		extID := uuid.New()
		context.mm.CreateExtent(dstID, extID, inhosts[0].UUID, stores)
		context.mm.SealExtent(dstID, extID)
		context.mm.MoveExtent(dstID, dstID, extID, cgDesc.GetConsumerGroupUUID())
		extents = append(extents, extID)
		// sleep to advance createdTime for next extent
		time.Sleep(2 * time.Millisecond)
		nExtents++
	}

	cgExtents := newCGExtentsByCategory()

	gotExtents, avail, err := selectNextExtentsToConsumeKafka(context, dstDesc, cgDesc, cgExtents, metrics.GetOutputHostsScope)
	s.Nil(err, "selectNextExtentsToConsume() error")

	s.Equal(maxExtentsToConsumeForDstKafka, len(gotExtents), "Wrong number of next extents to consume")
	s.Equal(numKafkaExtentsForDstKafka+nExtents, avail, "Wrong number of available extents")

	var nPhantom, nDlq int
	for _, x := range gotExtents {
		if common.AreKafkaPhantomStores(x.GetStoreUUIDs()) {
			nPhantom++
		} else {
			s.Equal(extents[nDlq], x.GetExtentUUID(), "Extents not served in time order")
			nDlq++
		}
	}

	// ensure that the number of extents picked are within bounds
	s.Equal(numKafkaExtentsForDstKafka, nPhantom, "Wrong number of phantom extents")
	s.Equal(maxDlqExtentsForDstKafka, nDlq, "Wrong number of phantom extents")
}

func (s *McpSuite) TestCGExtentSelectorHonorsDlqQuota() {
	dstPath := s.generateName("/test/selector")
	dstDesc, err := s.createDestination(dstPath, shared.DestinationType_PLAIN)
	s.Nil(err, "Failed to create destination")

	cgPath := s.generateName("/test/selector-cg")
	cgDesc, err := s.createConsumerGroup(dstPath, cgPath)
	s.Nil(err, "Failed to create consumer group")

	hosts, _ := s.mockrpm.GetHosts(common.StoreServiceName)
	stores := []string{hosts[0].UUID, hosts[1].UUID, hosts[2].UUID}
	inhosts, _ := s.mockrpm.GetHosts(common.InputServiceName)

	context := s.mcp.context
	dstID := dstDesc.GetDestinationUUID()

	dlqExtID := ""
	cgExtents := newCGExtentsByCategory()

	for i := 0; i <= maxExtentsToConsumeForDstPlain; i++ {
		extID := uuid.New()
		context.mm.CreateExtent(dstID, extID, inhosts[0].UUID, stores)
		if i == maxExtentsToConsumeForDstPlain {
			// make the last extent a DLQExtent
			// don't add it to the list of open CGExtents
			dlqExtID = extID
			context.mm.SealExtent(dstID, extID)
			context.mm.MoveExtent(dstID, dstID, extID, cgDesc.GetConsumerGroupUUID())
		} else {
			cgExtents.open[extID] = struct{}{}
		}
	}

	gotExtents, avail, err := selectNextExtentsToConsume(context, dstDesc, cgDesc, cgExtents, metrics.GetOutputHostsScope)
	s.Nil(err, "selectNextExtentsToConsume() error")
	s.Equal(1, len(gotExtents), "Wrong number of next extents to consume")
	s.Equal(1, avail, "Wrong number of available extents")
	s.Equal(dlqExtID, gotExtents[0].GetExtentUUID(), "DLQ quota not honored")

	cgExtents.open[dlqExtID] = struct{}{}
	gotExtents, avail, err = selectNextExtentsToConsume(context, dstDesc, cgDesc, cgExtents, metrics.GetOutputHostsScope)
	s.Nil(err, "selectNextExtentsToConsume() error")
	s.Equal(0, len(gotExtents), "Wrong number of next extents to consume")
	s.Equal(0, avail, "Wrong number of available extents")

	// make all currently open CGExtents consumed and start fresh
	cgExtents.consumed, cgExtents.open = cgExtents.open, cgExtents.consumed

	// create a bunch of dlq and regular extents
	dstExtents := make(map[string]struct{})
	dlqExtents := make(map[string]struct{})
	for i := 0; i < 15; i++ {
		extID := uuid.New()
		context.mm.CreateExtent(dstID, extID, inhosts[0].UUID, stores)
		if i < 5 {
			dlqExtents[extID] = struct{}{}
			context.mm.SealExtent(dstID, extID)
			context.mm.MoveExtent(dstID, dstID, extID, cgDesc.GetConsumerGroupUUID())
			if i == 0 {
				// add the first one to the CGE table
				cgExtents.open[extID] = struct{}{}
			}
			continue
		}
		dstExtents[extID] = struct{}{}
	}

	gotExtents, avail, err = selectNextExtentsToConsume(context, dstDesc, cgDesc, cgExtents, metrics.GetOutputHostsScope)
	s.Nil(err, "selectNextExtentsToConsume() error")
	s.Equal(maxExtentsToConsumeForDstPlain, len(gotExtents), "Wrong number of next extents to consume")
	s.Equal(14, avail, "Wrong number of available extents")

	nDlq := 0
	for _, ext := range gotExtents {
		if _, ok := dlqExtents[ext.GetExtentUUID()]; ok {
			nDlq++
			cgExtents.open[ext.GetExtentUUID()] = struct{}{}
		}
	}

	dlqQuota := maxExtentsToConsumeForDstPlain/4 - 1 // 25% minus the already added one
	s.Equal(dlqQuota, nDlq, "Wrong number of dlq extents, reservation not honored")

	// now make all the regular extents as consumed
	for k := range dstExtents {
		delete(cgExtents.open, k)
		cgExtents.consumed[k] = struct{}{}
	}

	gotExtents, avail, err = selectNextExtentsToConsume(context, dstDesc, cgDesc, cgExtents, metrics.GetOutputHostsScope)
	s.Nil(err, "selectNextExtentsToConsume() error")
	s.Equal(1, len(gotExtents), "Extent not created when no consummable extent")
}

func (s *McpSuite) TestCGExtentSelectorHonorsRemoteExtent() {
	dstPath := s.generateName("/test/selector")
	dstDesc, err := s.createMultiZoneDestination(dstPath, shared.DestinationType_PLAIN, []string{`sjc1`, `zone1`})
	s.Nil(err, "Failed to create destination")

	cgPath := s.generateName("/test/selector-cg")
	cgDesc, err := s.createConsumerGroup(dstPath, cgPath)
	s.Nil(err, "Failed to create consumer group")

	hosts, _ := s.mockrpm.GetHosts(common.StoreServiceName)
	stores := []string{hosts[0].UUID, hosts[1].UUID, hosts[2].UUID}
	inhosts, _ := s.mockrpm.GetHosts(common.InputServiceName)

	nExtents := 0
	context := s.mcp.context
	dstID := dstDesc.GetDestinationUUID()
	extents := make([]string, 0, 10)

	for i := 0; i < 20; i++ {
		extID := uuid.New()
		zone := ``
		if i%2 == 0 {
			zone = `zone1`
		}
		context.mm.CreateRemoteZoneExtent(dstID, extID, inhosts[0].UUID, stores, zone, stores[0], ``)
		extents = append(extents, extID)
		nExtents++
	}

	cgExtents := newCGExtentsByCategory()

	gotExtents, avail, err := selectNextExtentsToConsume(context, dstDesc, cgDesc, cgExtents, metrics.GetOutputHostsScope)
	s.Nil(err, "selectNextExtentsToConsume() error")

	s.Equal(maxExtentsToConsumeForDstPlain+extentsToConsumePerRemoteZone, len(gotExtents), "Wrong number of next extents to consume")
	s.Equal(nExtents, avail, "Wrong number of available extents")

	var curZoneExtent int
	var remoteZoneExtent int
	for i := 0; i < len(gotExtents); i++ {
		if len(gotExtents[i].GetOriginZone()) == 0 {
			curZoneExtent++
		} else {
			remoteZoneExtent++
		}
	}
	s.Equal(curZoneExtent, len(gotExtents)/2)
	s.Equal(remoteZoneExtent, len(gotExtents)/2)
}

func (s *McpSuite) updateCGExtentStatus(cgID string, extID string, status shared.ConsumerGroupExtentStatus) error {
	return s.mcp.mClient.UpdateConsumerGroupExtentStatus(nil, &shared.UpdateConsumerGroupExtentStatusRequest{
		ConsumerGroupUUID: &cgID,
		ExtentUUID:        &extID,
		Status:            &status,
	})
}

func (s *McpSuite) TestCGExtentSelectorWithBacklog() {
	dstPath := s.generateName("/test/selector")
	dstDesc, err := s.createDestination(dstPath, shared.DestinationType_PLAIN)
	s.Nil(err, "Failed to create destination")

	cgPath := s.generateName("/test/selector-cg")
	cgDesc, err := s.createConsumerGroup(dstPath, cgPath)
	s.Nil(err, "Failed to create consumer group")

	hosts, _ := s.mockrpm.GetHosts(common.StoreServiceName)
	stores := []string{hosts[0].UUID, hosts[1].UUID, hosts[2].UUID}
	inhosts, _ := s.mockrpm.GetHosts(common.InputServiceName)

	nExtents := 0
	context := s.mcp.context
	dstID := dstDesc.GetDestinationUUID()
	var openExtents []string
	var dlqExtents []string

	for i := 0; i < 100; i++ {
		extID := uuid.New()
		// create 96 DLQ dstExtents and 4 regular dstExtents
		// with dlq extents preceding in creation time
		context.mm.CreateExtent(dstID, extID, inhosts[0].UUID, stores)
		if i < 96 {
			context.mm.SealExtent(dstID, extID)
			context.mm.MoveExtent(dstID, dstID, extID, cgDesc.GetConsumerGroupUUID())
			dlqExtents = append(dlqExtents, extID)
		} else {
			openExtents = append(openExtents, extID)
		}
		// sleep to advance createdTime for next extent
		time.Sleep(2 * time.Millisecond)
		nExtents++
	}

	cgExtents := newCGExtentsByCategory()
	openDLQExtents := make(map[string]struct{})
	dlqExtsAvail, dlqExtsAssigned := len(dlqExtents), 0
	openExtsAvail, openExtsAssigned := len(openExtents), 0

	for dlqExtsAvail > 0 || openExtsAvail > 0 {

		totalAvail := openExtsAvail + dlqExtsAvail

		gotExtents, avail, err1 := selectNextExtentsToConsume(context, dstDesc, cgDesc, cgExtents, metrics.GetOutputHostsScope)
		s.Nil(err1, "selectNextExtentsToConsume() error")

		dlqQuota := common.MinInt(dlqExtsAvail, maxExtentsToConsumeForDstPlain/4-len(openDLQExtents))
		dlqQuota = common.MaxInt(0, dlqQuota)

		expectedCount := common.MinInt(maxExtentsToConsumeForDstPlain, (openExtsAvail + dlqQuota))
		s.Equal(expectedCount, len(gotExtents), "Wrong number of next extents to consume")
		s.Equal(totalAvail, avail, "Wrong number of available extents")

		for _, ext := range gotExtents {

			extID := ext.GetExtentUUID()

			if dlqQuota > 0 {
				// if there are both dlq and regular extents, selectNextExtents()
				// is supposed to reserve 25% of total capacity (maxExtentsToConsumeForDstPlain)
				// for DLQ extents. So, this will appear first
				s.Equal(dlqExtents[dlqExtsAssigned], extID, "Incorrect extent serving order")
				cgExtents.open[extID] = struct{}{}
				openDLQExtents[extID] = struct{}{}
				dlqExtsAssigned++
				dlqExtsAvail--
				dlqQuota--
			} else if openExtsAvail > 0 {
				// Once the dlqQuota is satisfied, we expect all of our regular
				// extents to be added to CG, because we have only (4) extents
				s.Equal(openExtents[openExtsAssigned], extID, "Incorrect extent serving order")
				cgExtents.open[extID] = struct{}{}
				openExtsAssigned++
				openExtsAvail--
			} else {
				// If there are still dlq extents left, then should show up next
				s.Equal(dlqExtents[dlqExtsAssigned], extID, "Incorrect extent serving order")
				cgExtents.open[extID] = struct{}{}
				openDLQExtents[extID] = struct{}{}
				dlqExtsAssigned++
				dlqExtsAvail--
			}
		}

		target := gotExtents[0].GetExtentUUID()
		s.updateCGExtentStatus(cgDesc.GetConsumerGroupUUID(), target, shared.ConsumerGroupExtentStatus_CONSUMED)
		delete(cgExtents.open, target)
		delete(openDLQExtents, target)
		cgExtents.consumed[target] = struct{}{}
	}
}
