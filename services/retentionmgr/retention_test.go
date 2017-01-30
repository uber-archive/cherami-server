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
	// DEST2,EXT1: minAckAddr < hardRetentionAddr -> retentionAddr = hardRetentionAddr
	// DEST2,EXT2: softRetentionAddr < minAckAddr -> retentionAddr = softRetentionAddr
	// DEST2,EXT3: hardRetentionAddr < minAckAddr < softRetentionAddr -> retentionAddr = minAckAddr
	// DEST2,EXT31: hardRetentionAddr < minAckAddr < softRetentionAddr but is multi_zone(extent in source zone) -> retentionAddr = hardRetentionAddr
	// DEST2,EXT32: hardRetentionAddr < minAckAddr < softRetentionAddr but is multi_zone(extent in remote zone) -> retentionAddr = minAckAddr
	// DEST2,EXT4: minAckAddr < softRetentionAddr; softRetentionAddr == seal -> retentionAddr = minAckAddr
	// DEST2,EXT5: minAckAddr == seal; softRetentionAddr == seal -> retentionAddr = seal (and delete)
	// DEST2,EXT6: minAckAddr == seal; softRetentionAddr != seal -> retentionAddr = softRetentionAddr
	// DEST2,EXT7: deleted extent -> don't process
	// DEST2,EXT8:
	// DEST2,EXT9:
	// DEST2,EXTA: test DeleteConsumerGroupExtent returns EntityNotExistsError
	// DEST2,EXTE0: test extent that is 'active', but hardRetentionConsumed = true (should not move to 'consumed')
	// DEST2,EXTE1: test extent that is 'sealed', but hardRetentionConsumed = true (should move to 'consumed')
	// DEST2,EXTE2: test extent that is 'sealed', but softRetentionConsumed = true (should move to 'consumed')

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

	destinations := []*destinationInfo{
		{id: "DEST2", status: shared.DestinationStatus_ENABLED, softRetention: softRetSecs, hardRetention: hardRetSecs, isMultiZone: true},
	}

	extStoreMap := map[extentID][]storehostID{
		"EXT1":  {"STOR1", "STOR2", "STOR3"},
		"EXT2":  {"STOR4", "STOR5", "STOR6"},
		"EXT3":  {"STOR1", "STOR3", "STOR5"},
		"EXT31": {"STOR1", "STOR3", "STOR5"},
		"EXT32": {"STOR1", "STOR3", "STOR5"},
		"EXT4":  {"STOR2", "STOR4", "STOR6"},
		"EXT5":  {"STOR2", "STOR3", "STOR4"},
		"EXT6":  {"STOR3", "STOR5", "STOR4"},
		"EXT61": {"STOR3", "STOR5", "STOR4"},
		"EXT7":  {"STOR7", "STOR6", "STOR5"},
		"EXT8":  {"STOR3", "STOR4", "STOR6"},
		"EXT9":  {"STOR1", "STOR2", "STOR5"},
		"EXTA":  {"STOR2", "STOR3", "STOR4"},
		"EXTB":  {"STOR2", "STOR3", "STOR4"},
		"EXTC":  {"STOR2", "STOR4", "STOR6"},
		"EXTD1": {"STOR2", "STOR4", "STOR6"},
		"EXTE0": {"STOR2", "STOR3", "STOR4"},
		"EXTE1": {"STOR2", "STOR3", "STOR4"},
		"EXTE2": {"STOR2", "STOR3", "STOR4"},
		"EXTE3": {"STOR2", "STOR3", "STOR4"},
		"EXTE4": {"STOR2", "STOR3", "STOR4"},
		"EXTm":  {"STOR3"}, // Single CG Visible
		"EXTn":  {"STOR7"}, // Single CG Visible
	}

	extStatusMap := map[extentID]shared.ExtentStatus{
		"EXT1":  shared.ExtentStatus_OPEN,
		"EXT2":  shared.ExtentStatus_OPEN,
		"EXT3":  shared.ExtentStatus_OPEN,
		"EXT31": shared.ExtentStatus_OPEN,
		"EXT32": shared.ExtentStatus_OPEN,
		"EXT4":  shared.ExtentStatus_OPEN,
		"EXT5":  shared.ExtentStatus_SEALED,
		"EXT6":  shared.ExtentStatus_CONSUMED,
		"EXT61": shared.ExtentStatus_CONSUMED,
		"EXT7":  shared.ExtentStatus_DELETED,
		"EXT8":  shared.ExtentStatus_OPEN,
		"EXT9":  shared.ExtentStatus_SEALED,
		"EXTA":  shared.ExtentStatus_SEALED,
		"EXTB":  shared.ExtentStatus_SEALED,
		"EXTC":  shared.ExtentStatus_OPEN,
		"EXTD":  shared.ExtentStatus_SEALED,
		"EXTD1": shared.ExtentStatus_SEALED,
		"EXTE0": shared.ExtentStatus_OPEN,
		"EXTE1": shared.ExtentStatus_SEALED,
		"EXTE2": shared.ExtentStatus_SEALED,
		"EXTE3": shared.ExtentStatus_SEALED,
		"EXTE4": shared.ExtentStatus_SEALED,
		"EXTm":  shared.ExtentStatus_SEALED, // Merged DLQ extents should always be sealed
		"EXTn":  shared.ExtentStatus_SEALED, //
	}

	extSingleCGVisibilityMap := map[extentID]consumerGroupID{
		"EXTm": consumerGroupID("CGm"),
		"EXTn": consumerGroupID("CGm"),
	}

	var extents []*extentInfo

	for ext, storehosts := range extStoreMap {

		statusUpdatedTime := time.Unix(0, tNow).Add(-55 * time.Minute)

		// 'EXT61' is like EXT6, except the statusUpdatedTime is beyond the ExtentDeleteDeferPeriod,
		// causing it to be deleted.
		if ext == "EXT61" {
			statusUpdatedTime = time.Unix(0, tNow).Add(-2 * time.Hour)
		}

		extents = append(extents, &extentInfo{
			id:                 extentID(ext),
			status:             extStatusMap[ext],
			statusUpdatedTime:  statusUpdatedTime,
			storehosts:         storehosts,
			singleCGVisibility: extSingleCGVisibilityMap[ext],
		})
	}

	s.metadata.On("GetDestinations").Return(destinations).Once()
	s.metadata.On("GetExtents", destinationID("DEST2")).Return(extents).Once()

	for ext, storehosts := range extStoreMap {
		originZone := `zone2`
		if ext == "EXT31" {
			originZone = ``
		}

		extInfo := &extentInfo{
			id:                 extentID(ext),
			status:             extStatusMap[ext],
			storehosts:         storehosts,
			singleCGVisibility: extSingleCGVisibilityMap[ext],
			originZone:         originZone,
		}
		s.metadata.On("GetExtentInfo", destinationID("DEST2"), extentID(ext)).Return(extInfo, nil).Once()
	}

	consumerGroups := []*consumerGroupInfo{
		{id: "CG1", status: shared.ConsumerGroupStatus_ENABLED},
		{id: "CG2", status: shared.ConsumerGroupStatus_DELETED},
		{id: "CG3", status: shared.ConsumerGroupStatus_DISABLED},
		{id: "CGm", status: shared.ConsumerGroupStatus_ENABLED}, // Single CG Visible consumer group
	}

	s.metadata.On("GetConsumerGroups", destinationID("DEST2")).Return(consumerGroups)

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
		"EXTm":  {"CGm": addrPostSoft},
		"EXTn":  {"CGm": addrPreSoft},
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
			s.metadata.On("GetAckLevel", destinationID("DEST2"), extentID(ext), consumerGroupID(cg)).Return(addr, nil).Once()
		}
	}

	s.metadata.On("MarkExtentConsumed", destinationID("DEST2"), extentID("EXT5")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST2"), extentID("EXT9")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST2"), extentID("EXTA")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST2"), extentID("EXTB")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST2"), extentID("EXTD")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST2"), extentID("EXTD1")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST2"), extentID("EXTE1")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST2"), extentID("EXTE4")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST2"), extentID("EXTm")).Return(nil).Once()
	s.metadata.On("MarkExtentConsumed", destinationID("DEST2"), extentID("EXTn")).Return(nil).Once()

	// expected retention addresses
	purgeAddrMap := map[extentID]int64{
		"EXT1":  addrHard,
		"EXT2":  addrSoft,
		"EXT3":  addrPreSoft,
		"EXT31": addrHard,
		"EXT32": addrPreSoft,
		"EXT4":  addrPreSoft,
		"EXT5":  addrSoft + 11,
		// "EXT6": addrSeal,
		"EXT61": addrSeal,
		// "EXT7": ,
		"EXT8":  addrSoft + 51,
		"EXT9":  addrSoft + 51,
		"EXTA":  addrSoft + 11,
		"EXTB":  addrSoft + 11,
		"EXTC":  addrSoft,
		"EXTD":  addrSoft,
		"EXTD1": addrHard,
		"EXTm":  addrSoft,
		"EXTn":  addrPreSoft,
	}

	for ext, addr := range purgeAddrMap {
		for i := range extStoreMap[ext] {
			s.storehost.On("PurgeMessages", extStoreMap[ext][i], ext, addr).Return(addr+1, nil).Once()
		}
	}

	for _, cg := range consumerGroups {
		s.metadata.On("DeleteConsumerGroupExtent", consumerGroupID(cg.id), extentID("EXT5")).Return(nil).Once()
		s.metadata.On("DeleteConsumerGroupExtent", consumerGroupID(cg.id), extentID("EXT61")).Return(nil).Once()
		s.metadata.On("DeleteConsumerGroupExtent", consumerGroupID(cg.id), extentID("EXT9")).Return(shared.NewEntityNotExistsError()).Once()
		s.metadata.On("DeleteConsumerGroupExtent", consumerGroupID(cg.id), extentID("EXTA")).Return(shared.NewEntityNotExistsError()).Once()
		s.metadata.On("DeleteConsumerGroupExtent", consumerGroupID(cg.id), extentID("EXTB")).Return(shared.NewBadRequestError()).Once()
		s.metadata.On("DeleteConsumerGroupExtent", consumerGroupID(cg.id), extentID("EXTD")).Return(nil).Once()
	}

	// // DeleteConsumerGroupExtent on EXTA got an EntityNotExistsError, but it should still be deleted; while EXTB shouldn't
	// s.metadata.On("DeleteExtent", destinationID("DEST2"), extentID("EXTA")).Return(nil).Once()

	s.metadata.On("DeleteExtent", destinationID("DEST2"), extentID("EXT61")).Return(nil).Once()

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
	}

	consumerGroups := []*consumerGroupInfo{
		{id: "CG1", status: shared.ConsumerGroupStatus_DELETED},
		{id: "CG2", status: shared.ConsumerGroupStatus_DELETED},
		{id: "CG3", status: shared.ConsumerGroupStatus_DELETED},
	}

	extStoreMap := map[extentID][]storehostID{
		"EXT1": {"STOR1", "STOR2", "STOR3"},
		"EXT2": {"STOR1", "STOR2", "STOR3"},
	}

	extents := []*extentInfo{
		{
			id:         "EXT1",
			status:     shared.ExtentStatus_DELETED,
			storehosts: []storehostID{"STOR1", "STOR2", "STOR3"},
		},
		{
			id:         "EXT2",
			status:     shared.ExtentStatus_SEALED,
			storehosts: []storehostID{"STOR1", "STOR2", "STOR3"},
		},
	}

	s.metadata.On("GetDestinations").Return(destinations).Once()
	s.metadata.On("GetExtents", destinationID("DEST1")).Return(extents).Once()

	for _, extInfo := range extents {
		s.metadata.On("GetExtentInfo", destinationID("DEST1"), extentID(extInfo.id)).Return(extInfo, nil).Once()
	}

	s.metadata.On("GetConsumerGroups", destinationID("DEST1")).Return(consumerGroups)

	// expected retention addresses
	purgeAddrMap := map[extentID]int64{
		"EXT1": int64(store.ADDR_SEAL),
		"EXT2": int64(store.ADDR_SEAL),
	}

	for ext, addr := range purgeAddrMap {
		for i := range extStoreMap[ext] {
			s.storehost.On("PurgeMessages", extStoreMap[ext][i], ext, addr).Return(addr, nil).Once()
		}
	}

	for _, cg := range consumerGroups {
		s.metadata.On("DeleteConsumerGroupExtent", consumerGroupID(cg.id), extentID("EXT1")).Return(nil).Once()
		s.metadata.On("DeleteConsumerGroupExtent", consumerGroupID(cg.id), extentID("EXT2")).Return(nil).Once()
	}

	s.metadata.On("DeleteExtent", destinationID("DEST1"), extentID("EXT1")).Return(nil).Once()
	s.metadata.On("DeleteExtent", destinationID("DEST1"), extentID("EXT2")).Return(nil).Once()

	var retMgr *RetentionManager

	opts := &Options{NumWorkers: 3, RetentionInterval: 5 * time.Second, LocalZone: `zone1`}

	metricsClient := metrics.NewClient(common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig()), metrics.Controller)
	retMgr = tNew(opts, s.metadata, s.storehost, metricsClient, common.GetDefaultLogger())
	retMgr.Run()
}
