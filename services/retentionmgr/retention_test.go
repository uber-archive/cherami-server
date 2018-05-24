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

package retentionmgr

// TESTS:
// - ack-level less than softRetentionAddr (slow consumer)
// - ack-level less that hardRetentionAddr (very slow consumer), racing with hardRetentionAddr
// - empty extent (not sealed)
// - empty extent that is seaeld (so has a 'seal-extent-key')
// - if one/more storehosts are unreachable
// - if one/more storehosts returns error on GetAddressFromTimestamp and/or PurgeMessages calls
// -

import (
	"testing"
	"time"

	// "github.com/stretchr/testify/assert"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type RetentionMgrSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite

	metadata  *mockMetadataDep
	storehost *mockStorehostDep
}

func TestRetentionMgrSuite(t *testing.T) {
	suite.Run(t, new(RetentionMgrSuite))
}

func (s *RetentionMgrSuite) SetupCommonMock() {
	s.metadata = new(mockMetadataDep)
	s.storehost = new(mockStorehostDep)
}

func (s *RetentionMgrSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.SetupCommonMock()
}

func (s *RetentionMgrSuite) TearDownTest() {
}

func (s *RetentionMgrSuite) TestRetentionManager() {

	// Test cases
	// DEST1,EXT1: minAckAddr < hardRetentionAddr -> retentionAddr = hardRetentionAddr
	// DEST1,EXT2: softRetentionAddr < minAckAddr -> retentionAddr = softRetentionAddr
	// DEST1,EXT3: hardRetentionAddr < minAckAddr < softRetentionAddr -> retentionAddr = minAckAddr
	// DEST1,EXT31: hardRetentionAddr < minAckAddr < softRetentionAddr but is multi_zone(extent in source zone) -> retentionAddr = hardRetentionAddr
	// DEST1,EXT32: hardRetentionAddr < minAckAddr < softRetentionAddr but is multi_zone(extent in remote zone) -> retentionAddr = minAckAddr
	// DEST1,EXT4: minAckAddr < softRetentionAddr; softRetentionAddr == seal -> retentionAddr = minAckAddr
	// DEST1,EXT5: minAckAddr == seal; softRetentionAddr == seal -> retentionAddr = seal (and delete)
	// DEST1,EXT6: minAckAddr == seal; softRetentionAddr != seal -> retentionAddr = softRetentionAddr
	// DEST1,EXT7: deleted extent -> don't process
	// DEST1,EXT8:
	// DEST1,EXT9:
	// DEST1,EXTA: test DeleteConsumerGroupExtent returns EntityNotExistsError
	// DEST1,EXTE0: test extent that is 'active', but hardRetentionConsumed = true (should not move to 'consumed')
	// DEST1,EXTE1: test extent that is 'sealed', but hardRetentionConsumed = true (should move to 'consumed')
	// DEST1,EXTE2: test extent that is 'sealed', but softRetentionConsumed = true (should move to 'consumed')

	softRetSecs, hardRetSecs := int32(10), int32(20)

	seconds := int64(time.Second)
	softRet, hardRet := int64(softRetSecs)*seconds, int64(hardRetSecs)*seconds

	tNow := time.Now().Truncate(time.Second).UnixNano()
	tSoft := tNow - softRet
	tHard := tNow - hardRet

	addrSoft, addrHard := tSoft, tHard
	addrPreSoft, addrPostSoft := addrSoft-2*seconds, addrSoft+2*seconds
	addrPreHard, addrPostHard := addrHard-2*seconds, addrHard+2*seconds
	addrBegin, addrSeal := int64(store.ADDR_BEGIN), int64(store.ADDR_SEAL)

	// fmt.Printf("tNow=%v tSoft=%v tHard=%v\n", tNow, tSoft, tHard)
	// fmt.Printf("addrPreSoft=%v addrePostSoft=%v\n", addrPreSoft, addrPostSoft)
	// fmt.Printf("addrPreHard=%v addrePostHard=%v\n", addrPreHard, addrPostHard)
	// fmt.Printf("addrSeal=%v", addrSeal)

	destinations := []*destinationInfo{{
		id:            "DEST1",
		status:        shared.DestinationStatus_ENABLED,
		softRetention: softRetSecs,
		hardRetention: hardRetSecs,
		isMultiZone:   true,
	}}

	extInfoMap := map[extentID]extentInfo{
		"EXT1":  {status: shared.ExtentStatus_OPEN, storehosts: []storehostID{"STOR1", "STOR2", "STOR3"}, originZone: "zone2"},
		"EXT2":  {status: shared.ExtentStatus_OPEN, storehosts: []storehostID{"STOR4", "STOR5", "STOR6"}, originZone: "zone2"},
		"EXT3":  {status: shared.ExtentStatus_OPEN, storehosts: []storehostID{"STOR1", "STOR3", "STOR5"}, originZone: "zone2"},
		"EXT31": {status: shared.ExtentStatus_OPEN, storehosts: []storehostID{"STOR1", "STOR3", "STOR5"}}, // no origin zone
		"EXT32": {status: shared.ExtentStatus_OPEN, storehosts: []storehostID{"STOR1", "STOR3", "STOR5"}, originZone: "zone2"},
		"EXT4":  {status: shared.ExtentStatus_OPEN, storehosts: []storehostID{"STOR2", "STOR4", "STOR6"}, originZone: "zone2"},
		"EXT5":  {status: shared.ExtentStatus_SEALED, storehosts: []storehostID{"STOR2", "STOR3", "STOR4"}, originZone: "zone2"},
		"EXT6":  {status: shared.ExtentStatus_CONSUMED, storehosts: []storehostID{"STOR3", "STOR5", "STOR4"}, originZone: "zone2"},
		// 'EXT61' (below) is like EXT6, except the statusUpdatedTime is beyond the ExtentDeleteDeferPeriod, causing it to be deleted.
		"EXT61": {status: shared.ExtentStatus_CONSUMED, statusUpdatedTime: time.Now().Add(-2 * time.Hour), storehosts: []storehostID{"STOR3", "STOR5", "STOR4"}, originZone: "zone2"},
		"EXT7":  {status: shared.ExtentStatus_DELETED, storehosts: []storehostID{"STOR7", "STOR6", "STOR5"}, originZone: "zone2"},
		"EXT8":  {status: shared.ExtentStatus_OPEN, storehosts: []storehostID{"STOR3", "STOR4", "STOR6"}, originZone: "zone2"},
		"EXT9":  {status: shared.ExtentStatus_SEALED, storehosts: []storehostID{"STOR1", "STOR2", "STOR5"}, originZone: "zone2"},
		"EXTA":  {status: shared.ExtentStatus_SEALED, storehosts: []storehostID{"STOR2", "STOR3", "STOR4"}, originZone: "zone2"},
		"EXTB":  {status: shared.ExtentStatus_SEALED, storehosts: []storehostID{"STOR2", "STOR3", "STOR4"}, originZone: "zone2"},
		"EXTC":  {status: shared.ExtentStatus_OPEN, storehosts: []storehostID{"STOR2", "STOR4", "STOR6"}, originZone: "zone2"},
		"EXTD1": {status: shared.ExtentStatus_SEALED, storehosts: []storehostID{"STOR2", "STOR4", "STOR6"}, originZone: "zone2"},
		"EXTE0": {status: shared.ExtentStatus_OPEN, storehosts: []storehostID{"STOR2", "STOR3", "STOR4"}, originZone: "zone2"},
		"EXTE1": {status: shared.ExtentStatus_SEALED, storehosts: []storehostID{"STOR2", "STOR3", "STOR4"}, originZone: "zone2"},
		"EXTE2": {status: shared.ExtentStatus_SEALED, storehosts: []storehostID{"STOR2", "STOR3", "STOR4"}, originZone: "zone2"},
		"EXTE3": {status: shared.ExtentStatus_SEALED, storehosts: []storehostID{"STOR2", "STOR3", "STOR4"}, originZone: "zone2"},
		"EXTE4": {status: shared.ExtentStatus_SEALED, storehosts: []storehostID{"STOR2", "STOR3", "STOR4"}, originZone: "zone2"},
		"EXTk1": {status: shared.ExtentStatus_OPEN, storehosts: []storehostID{common.KafkaPhantomExtentStorehost}, originZone: "zone2", kafkaPhantomExtent: true},   // kafka phantom extent
		"EXTk2": {status: shared.ExtentStatus_SEALED, storehosts: []storehostID{common.KafkaPhantomExtentStorehost}, originZone: "zone2", kafkaPhantomExtent: true}, // kafka phantom extent
		"EXTm":  {status: shared.ExtentStatus_SEALED, singleCGVisibility: "CGm", storehosts: []storehostID{"STOR3"}, originZone: "zone2"},                           // Merged DLQ extent should always be sealed
		"EXTn":  {status: shared.ExtentStatus_SEALED, singleCGVisibility: "CGm", storehosts: []storehostID{"STOR7"}, originZone: "zone2"},
	}

	var extents []*extentInfo

	for ext, xi := range extInfoMap {

		extInfo := xi
		extInfo.id = ext
		// if statusUpdatedTime not set, then default to  55 mins before now
		if extInfo.statusUpdatedTime.IsZero() {
			extInfo.statusUpdatedTime = time.Unix(0, tNow).Add(-55 * time.Minute)
		}

		extents = append(extents, &extInfo)
		s.metadata.On("GetExtentInfo", destinationID("DEST1"), ext).Return(&extInfo, nil).Once()
	}

	s.metadata.On("GetDestinations").Return(destinations, nil).Once()
	s.metadata.On("GetExtents", destinationID("DEST1")).Return(extents, nil).Once()

	consumerGroups := []*consumerGroupInfo{
		{id: "CG1", status: shared.ConsumerGroupStatus_ENABLED},
		{id: "CG2", status: shared.ConsumerGroupStatus_DELETED},
		{id: "CG3", status: shared.ConsumerGroupStatus_DISABLED},
		{id: "CGm", status: shared.ConsumerGroupStatus_ENABLED}, // Single CG Visible consumer group
	}

	s.metadata.On("GetConsumerGroups", destinationID("DEST1")).Return(consumerGroups, nil)

	type gaftRet struct {
		addr   int64
		sealed bool
	}

	// hard retention addr
	gaftHard := map[extentID]map[storehostID]gaftRet{
		"EXT1":  {"STOR1": {addrHard, false}, "STOR2": {addrHard - 5, false}, "STOR3": {addrHard - 10, false}},
		"EXT2":  {"STOR4": {addrHard - 10, false}, "STOR5": {addrHard - 8, false}, "STOR6": {addrHard, false}},
		"EXT3":  {"STOR1": {addrHard - 30, false}, "STOR3": {addrHard, false}, "STOR5": {addrHard - 7, false}},
		"EXT31": {"STOR1": {addrHard - 30, false}, "STOR3": {addrHard, false}, "STOR5": {addrHard - 7, false}},
		"EXT32": {"STOR1": {addrHard - 30, false}, "STOR3": {addrHard, false}, "STOR5": {addrHard - 7, false}},
		"EXT4":  {"STOR2": {addrHard, false}, "STOR4": {addrHard - 1, false}, "STOR6": {addrHard, false}},
		"EXT5":  {"STOR2": {addrHard - 10, false}, "STOR3": {addrHard, false}, "STOR4": {addrHard - 20, false}},
		// "EXT6": {}, // should not get called
		// "EXT61": {}, // should not get called
		// "EXT7": {}, // should not get called
		"EXT8":  {"STOR3": {addrHard, false}, "STOR4": {addrHard - 5, false}, "STOR6": {addrHard - 20, false}},
		"EXT9":  {"STOR1": {addrHard, false}, "STOR2": {addrHard - 5, false}, "STOR5": {addrHard - 20, false}},
		"EXTA":  {"STOR2": {addrHard - 10, false}, "STOR3": {addrHard, false}, "STOR4": {addrHard - 20, false}},
		"EXTB":  {"STOR2": {addrHard - 10, false}, "STOR3": {addrHard, false}, "STOR4": {addrHard - 20, false}},
		"EXTC":  {"STOR2": {addrHard, false}, "STOR4": {addrHard, false}, "STOR6": {addrHard, false}},
		"EXTD":  {"STOR2": {addrHard, false}, "STOR4": {addrHard, false}, "STOR6": {addrHard, false}},
		"EXTD1": {"STOR2": {addrHard, true}, "STOR4": {addrHard, false}, "STOR6": {addrHard, false}},
		"EXTE0": {"STOR2": {addrBegin, false}, "STOR3": {addrBegin, true}, "STOR4": {addrBegin, false}},
		"EXTE1": {"STOR2": {addrBegin, false}, "STOR3": {addrBegin, true}, "STOR4": {addrBegin, false}},
		"EXTE2": {"STOR2": {addrBegin, false}, "STOR3": {addrBegin, false}, "STOR4": {addrBegin, false}},
		"EXTE3": {"STOR2": {addrBegin, false}, "STOR3": {addrBegin, false}, "STOR4": {addrBegin, false}},
		"EXTE4": {"STOR2": {addrBegin, false}, "STOR3": {addrBegin, false}, "STOR4": {addrBegin, false}},
		"EXTk1": {common.KafkaPhantomExtentStorehost: {addrBegin, false}},
		"EXTk2": {common.KafkaPhantomExtentStorehost: {addrBegin, false}},
		"EXTm":  {"STOR3": {addrHard - 100, true}},
		"EXTn":  {"STOR7": {addrHard - 100, true}},
	}

	// soft retention addr
	gaftSoft := map[extentID]map[storehostID]gaftRet{
		"EXT1":  {"STOR1": {addrSoft - 3, false}, "STOR2": {addrSoft - 1, false}, "STOR3": {addrSoft, false}},
		"EXT2":  {"STOR4": {addrSoft, false}, "STOR5": {addrSoft - 5, false}, "STOR6": {addrSoft - 42, false}},
		"EXT3":  {"STOR1": {addrSoft - 9, false}, "STOR3": {addrSoft, false}, "STOR5": {addrSoft - 12, false}},
		"EXT31": {"STOR1": {addrSoft - 9, false}, "STOR3": {addrSoft, false}, "STOR5": {addrSoft - 12, false}},
		"EXT32": {"STOR1": {addrSoft - 9, false}, "STOR3": {addrSoft, false}, "STOR5": {addrSoft - 12, false}},
		"EXT4":  {"STOR2": {addrSoft, false}, "STOR4": {addrSoft + 51, true}, "STOR6": {addrSoft + 50, false}},
		"EXT5":  {"STOR2": {addrSoft + 10, false}, "STOR3": {addrSoft - 100, false}, "STOR4": {addrSoft + 11, true}},
		// "EXT6": {}, // should not get called
		// "EXT61": {}, // should not get called
		// "EXT7": {}, // should not get called
		"EXT8":  {"STOR3": {addrSoft + 51, true}, "STOR4": {addrSoft + 50, false}, "STOR6": {addrSoft - 20, false}},
		"EXT9":  {"STOR1": {addrSoft + 51, true}, "STOR2": {addrSoft + 50, false}, "STOR5": {addrSoft - 20, false}},
		"EXTA":  {"STOR2": {addrSoft + 10, false}, "STOR3": {addrSoft - 100, false}, "STOR4": {addrSoft + 11, true}},
		"EXTB":  {"STOR2": {addrSoft + 10, false}, "STOR3": {addrSoft - 100, false}, "STOR4": {addrSoft + 11, true}},
		"EXTC":  {"STOR2": {addrSoft, true}, "STOR4": {addrSoft, true}, "STOR6": {addrSoft, true}},
		"EXTD":  {"STOR2": {addrSoft, true}, "STOR4": {addrSoft, true}, "STOR6": {addrSoft, true}},
		"EXTD1": {"STOR2": {addrBegin, false}, "STOR4": {addrBegin, false}, "STOR6": {addrBegin, false}},
		"EXTE0": {"STOR2": {addrBegin, false}, "STOR3": {addrBegin, false}, "STOR4": {addrBegin, false}},
		"EXTE1": {"STOR2": {addrBegin, false}, "STOR3": {addrBegin, false}, "STOR4": {addrBegin, false}},
		"EXTE2": {"STOR2": {addrBegin, false}, "STOR3": {addrBegin, false}, "STOR4": {addrBegin, true}},
		"EXTE3": {"STOR2": {addrBegin, false}, "STOR3": {addrBegin, false}, "STOR4": {addrBegin, false}},
		"EXTE4": {"STOR2": {addrBegin, false}, "STOR3": {addrBegin, false}, "STOR4": {addrBegin, true}},
		"EXTk1": {common.KafkaPhantomExtentStorehost: {addrBegin, false}},
		"EXTk2": {common.KafkaPhantomExtentStorehost: {addrBegin, false}},
		"EXTm":  {"STOR3": {addrSoft, true}},
		"EXTn":  {"STOR7": {addrSoft, true}},
	}

	// get ack level
	gal := map[extentID]map[consumerGroupID]int64{
		"EXT1":  {"CGm": addrSeal, "CG1": addrPreHard, "CG2": addrPostHard, "CG3": addrSoft},
		"EXT2":  {"CGm": addrSeal, "CG1": addrPostSoft - 10, "CG2": addrSoft + 100, "CG3": addrPostSoft - 20},
		"EXT3":  {"CGm": addrSeal, "CG1": addrPreSoft, "CG2": addrPreSoft - 20, "CG3": addrPreSoft + 50},
		"EXT31": {"CGm": addrSeal, "CG1": addrPreSoft, "CG2": addrPreSoft - 20, "CG3": addrPreSoft + 50},
		"EXT32": {"CGm": addrSeal, "CG1": addrPreSoft, "CG2": addrPreSoft - 20, "CG3": addrPreSoft + 50},
		"EXT4":  {"CGm": addrSeal, "CG1": addrPreSoft, "CG2": addrSoft, "CG3": addrPostSoft},
		"EXT5":  {"CGm": addrSeal, "CG1": addrSeal, "CG2": addrSeal, "CG3": addrSeal},
		// "EXT6": {}, // should not get called
		// "EXT7": {}, // should not get called
		"EXT8":  {"CGm": addrSeal, "CG1": addrSeal, "CG2": addrSeal, "CG3": addrSeal},
		"EXT9":  {"CGm": addrSeal, "CG1": addrSeal, "CG2": addrSeal, "CG3": addrSeal},
		"EXTA":  {"CGm": addrSeal, "CG1": addrSeal, "CG2": addrSeal, "CG3": addrSeal},
		"EXTB":  {"CGm": addrSeal, "CG1": addrSeal, "CG2": addrSeal, "CG3": addrSeal},
		"EXTC":  {"CGm": addrSeal, "CG1": addrSeal, "CG2": addrSeal, "CG3": addrSeal},
		"EXTD":  {"CGm": addrSeal, "CG1": addrSeal, "CG2": addrSeal, "CG3": addrSeal},
		"EXTD1": {"CGm": addrBegin, "CG1": addrBegin, "CG2": addrBegin, "CG3": addrBegin},
		"EXTE0": {"CGm": addrBegin, "CG1": addrBegin, "CG2": addrBegin, "CG3": addrBegin},
		"EXTE1": {"CGm": addrBegin, "CG1": addrBegin, "CG2": addrBegin, "CG3": addrBegin},
		"EXTE2": {"CGm": addrBegin, "CG1": addrBegin, "CG2": addrBegin, "CG3": addrBegin},
		"EXTE3": {"CGm": addrBegin, "CG1": addrBegin, "CG2": addrSeal, "CG3": addrBegin},
		"EXTE4": {"CGm": addrSeal, "CG1": addrBegin, "CG2": addrBegin, "CG3": addrBegin},
		// "EXTk1": // kafka phantom extents should not see a GetAckLevel call
		// "EXTk2":
		"EXTm": {"CGm": addrPostSoft},
		"EXTn": {"CGm": addrPreSoft},
	}

	for ext, retMap := range gaftHard {
		for stor, ret := range retMap {
			s.storehost.On("GetAddressFromTimestamp", storehostID(stor), extentID(ext), mock.AnythingOfType("int64")).Return(ret.addr, ret.sealed, nil).Once()
		}
	}

	for ext, retMap := range gaftSoft {
		for stor, ret := range retMap {
			s.storehost.On("GetAddressFromTimestamp", storehostID(stor), extentID(ext), mock.AnythingOfType("int64")).Return(ret.addr, ret.sealed, nil).Once()
		}
	}

	for ext, ackMap := range gal {
		for cg, addr := range ackMap {
			s.metadata.On("GetAckLevel", destinationID("DEST1"), extentID(ext), consumerGroupID(cg)).Return(addr, nil).Once()
		}
	}

	s.metadata.On("MarkExtentConsumed", destinationID("DEST1"), extentID("EXT5")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST1"), extentID("EXT9")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST1"), extentID("EXTA")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST1"), extentID("EXTB")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST1"), extentID("EXTD")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST1"), extentID("EXTD1")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST1"), extentID("EXTE1")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST1"), extentID("EXTE4")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST1"), extentID("EXTm")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST1"), extentID("EXTn")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST1"), extentID("EXTk2")).Return(nil).Once()

	// expected retention addresses
	purgeAddrMap := map[extentID]int64{
		"EXT1":  addrHard,
		"EXT2":  addrSoft,
		"EXT3":  addrPreSoft,
		"EXT31": addrHard,
		"EXT32": addrPreSoft,
		"EXT4":  addrPreSoft,
		"EXT5":  addrSoft + 11,
		// "EXT6": addrSeal, // EXT6 should not see a delete extent, unlike EXT6 below
		"EXT61": addrSeal,
		// "EXT7": ,
		"EXT8":  addrSoft + 51,
		"EXT9":  addrSoft + 51,
		"EXTA":  addrSoft + 11,
		"EXTB":  addrSoft + 11,
		"EXTC":  addrSoft,
		"EXTD":  addrSoft,
		"EXTD1": addrHard,
		// "EXTk": , // kafka phantom extents that are OPEN should not see a PurgeAddress
		"EXTm": addrSoft,
		"EXTn": addrPreSoft,
	}

	for ext, addr := range purgeAddrMap {
		for _, storeID := range extInfoMap[ext].storehosts {
			s.storehost.On("PurgeMessages", storeID, ext, addr).Return(addr+1, nil).Once()
		}
	}

	for _, cg := range consumerGroups {
		s.metadata.On("DeleteConsumerGroupExtent", destinationID("DEST1"), consumerGroupID(cg.id), extentID("EXT5")).Return(nil).Once()
		s.metadata.On("DeleteConsumerGroupExtent", destinationID("DEST1"), consumerGroupID(cg.id), extentID("EXT61")).Return(nil).Once()
		s.metadata.On("DeleteConsumerGroupExtent", destinationID("DEST1"), consumerGroupID(cg.id), extentID("EXT9")).Return(shared.NewEntityNotExistsError()).Once()
		s.metadata.On("DeleteConsumerGroupExtent", destinationID("DEST1"), consumerGroupID(cg.id), extentID("EXTA")).Return(shared.NewEntityNotExistsError()).Once()
		s.metadata.On("DeleteConsumerGroupExtent", destinationID("DEST1"), consumerGroupID(cg.id), extentID("EXTB")).Return(shared.NewBadRequestError()).Once()
		s.metadata.On("DeleteConsumerGroupExtent", destinationID("DEST1"), consumerGroupID(cg.id), extentID("EXTD")).Return(nil).Once()
	}

	// // DeleteConsumerGroupExtent on EXTA got an EntityNotExistsError, but it should still be deleted; while EXTB shouldn't
	// s.metadata.On("DeleteExtent", destinationID("DEST1"), extentID("EXTA")).Return(nil).Once()

	s.metadata.On("DeleteExtent", destinationID("DEST1"), extentID("EXT61")).Return(nil).Once()

	var retMgr *RetentionManager

	opts := &Options{NumWorkers: 3, RetentionInterval: 5 * time.Second, ExtentDeleteDeferPeriod: 1 * time.Hour, LocalZone: `zone1`}

	metricsClient := metrics.NewClient(common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig()), metrics.Controller)
	retMgr = tNew(opts, s.metadata, s.storehost, metricsClient, common.GetDefaultLogger())
	retMgr.Run()
	// retMgr.Start()
	// retMgr.wait()

	// s.metadata.AssertExpectations(s.T())
}

func (s *RetentionMgrSuite) TestRetentionManagerOnDeletedDestinations() {

	// Test cases
	// DEST1,EXT1: deleted, EXT1 also deleted -> skip
	// DEST1,EXT2: deleted, EXT2 open, do delete

	destinations := []*destinationInfo{
		{id: "DEST1", status: shared.DestinationStatus_DELETING},
		{id: "DEST2", status: shared.DestinationStatus_DELETING},
	}

	extInfoMapDEST1 := map[extentID]extentInfo{
		"EXT1": {status: shared.ExtentStatus_DELETED, storehosts: []storehostID{"STOR1", "STOR2", "STOR3"}},
		"EXT2": {status: shared.ExtentStatus_SEALED, storehosts: []storehostID{"STOR1", "STOR2", "STOR3"}},
		"EXT3": {status: shared.ExtentStatus_CONSUMED, kafkaPhantomExtent: true, storehosts: []storehostID{common.KafkaPhantomExtentStorehost}},
		"EXT4": {status: shared.ExtentStatus_SEALED, kafkaPhantomExtent: true, storehosts: []storehostID{common.KafkaPhantomExtentStorehost}},
	}

	extInfoMapDEST2 := map[extentID]extentInfo{
		"EXT5": {status: shared.ExtentStatus_DELETED, storehosts: []storehostID{common.KafkaPhantomExtentStorehost}},
		"EXT6": {status: shared.ExtentStatus_CONSUMED, kafkaPhantomExtent: true, storehosts: []storehostID{common.KafkaPhantomExtentStorehost}},
		"EXT7": {status: shared.ExtentStatus_SEALED, kafkaPhantomExtent: true, storehosts: []storehostID{common.KafkaPhantomExtentStorehost}},
	}

	consumerGroupsDEST1 := []*consumerGroupInfo{
		{id: "CG1", status: shared.ConsumerGroupStatus_DELETED},
		{id: "CG2", status: shared.ConsumerGroupStatus_DELETED},
		{id: "CG3", status: shared.ConsumerGroupStatus_DELETED},
	}

	consumerGroupsDEST2 := []*consumerGroupInfo{
		{id: "CG4", status: shared.ConsumerGroupStatus_DELETED},
		{id: "CG5", status: shared.ConsumerGroupStatus_ENABLED},
		{id: "CG6", status: shared.ConsumerGroupStatus_DELETED},
	}

	var extentsDEST1 []*extentInfo
	for ext, xi := range extInfoMapDEST1 {
		extInfo := xi
		extInfo.id = ext
		extentsDEST1 = append(extentsDEST1, &extInfo)
		if xi.status != shared.ExtentStatus_DELETED {
			s.metadata.On("GetExtentInfo", destinationID("DEST1"), ext).Return(&extInfo, nil).Once()
		}
	}

	var extentsDEST2 []*extentInfo
	for ext, xi := range extInfoMapDEST2 {
		extInfo := xi
		extInfo.id = ext
		extentsDEST2 = append(extentsDEST2, &extInfo)
		if xi.status != shared.ExtentStatus_DELETED {
			s.metadata.On("GetExtentInfo", destinationID("DEST2"), ext).Return(&extInfo, nil).Once()
		}
	}

	s.metadata.On("GetDestinations").Return(destinations, nil).Once()
	s.metadata.On("GetExtents", destinationID("DEST1")).Return(extentsDEST1, nil).Once()
	s.metadata.On("GetConsumerGroups", destinationID("DEST1")).Return(consumerGroupsDEST1, nil)

	s.metadata.On("GetExtents", destinationID("DEST2")).Return(extentsDEST2, nil).Once()
	s.metadata.On("GetConsumerGroups", destinationID("DEST2")).Return(consumerGroupsDEST2, nil)

	s.storehost.On("GetAddressFromTimestamp", storehostID(common.KafkaPhantomExtentStorehost), mock.AnythingOfType("extentID"), mock.AnythingOfType("int64")).Return(int64(store.ADDR_BEGIN), false, nil)
	s.storehost.On("PurgeMessages", storehostID(common.KafkaPhantomExtentStorehost), mock.AnythingOfType("extentID"), mock.AnythingOfType("int64")).Return(int64(store.ADDR_BEGIN), nil)

	// expected retention addresses
	purgeAddrMap := map[extentID]int64{
		// "EXT1": int64(store.ADDR_SEAL),
		"EXT2": int64(store.ADDR_SEAL),
		"EXT3": int64(store.ADDR_SEAL),
		"EXT4": int64(store.ADDR_SEAL),
	}

	for ext, addr := range purgeAddrMap {
		for _, storeID := range extInfoMapDEST1[ext].storehosts {
			s.storehost.On("PurgeMessages", storeID, ext, addr).Return(addr+1, nil).Once()
		}
	}

	for _, cg := range consumerGroupsDEST1 {
		s.metadata.On("DeleteConsumerGroupExtent", destinationID("DEST1"), consumerGroupID(cg.id), extentID("EXT1")).Return(nil).Once()
		s.metadata.On("DeleteConsumerGroupExtent", destinationID("DEST1"), consumerGroupID(cg.id), extentID("EXT2")).Return(nil).Once()
		s.metadata.On("DeleteConsumerGroupExtent", destinationID("DEST1"), consumerGroupID(cg.id), extentID("EXT3")).Return(nil).Once()
		s.metadata.On("DeleteConsumerGroupExtent", destinationID("DEST1"), consumerGroupID(cg.id), extentID("EXT4")).Return(nil).Once()
	}

	for _, cg := range consumerGroupsDEST2 {
		s.metadata.On("DeleteConsumerGroupExtent", destinationID("DEST2"), consumerGroupID(cg.id), extentID("EXT5")).Return(nil).Once()
		s.metadata.On("DeleteConsumerGroupExtent", destinationID("DEST2"), consumerGroupID(cg.id), extentID("EXT6")).Return(nil).Once()
		s.metadata.On("DeleteConsumerGroupExtent", destinationID("DEST2"), consumerGroupID(cg.id), extentID("EXT7")).Return(nil).Once()
	}

	// s.metadata.On("DeleteExtent", destinationID("DEST1"), extentID("EXT1")).Return(nil).Once()
	s.metadata.On("DeleteExtent", destinationID("DEST1"), extentID("EXT2")).Return(nil).Once()
	s.metadata.On("DeleteExtent", destinationID("DEST1"), extentID("EXT3")).Return(nil).Once()
	s.metadata.On("DeleteExtent", destinationID("DEST1"), extentID("EXT4")).Return(nil).Once()

	// DEST2/EXT7 (which was SEALED) should be moved to CONSUMED
	s.metadata.On("MarkExtentConsumed", destinationID("DEST2"), extentID("EXT7")).Return(nil).Once()

	// DEST2/EXT6&EXT7 (which are CONSUMED) should be DELETED
	s.metadata.On("DeleteExtent", destinationID("DEST2"), extentID("EXT6")).Return(nil).Once()
	s.metadata.On("DeleteExtent", destinationID("DEST2"), extentID("EXT7")).Return(nil).Once()

	var retMgr *RetentionManager

	opts := &Options{NumWorkers: 3, RetentionInterval: 5 * time.Second, LocalZone: `zone1`}

	metricsClient := metrics.NewClient(common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig()), metrics.Controller)
	retMgr = tNew(opts, s.metadata, s.storehost, metricsClient, common.GetDefaultLogger())
	retMgr.Run()
}
