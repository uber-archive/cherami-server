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

package admin

import (
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/codegangsta/cli"
	mcli "github.com/uber/cherami-server/clients/metadata"
	"github.com/uber/cherami-server/common"
	toolscommon "github.com/uber/cherami-server/tools/common"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

const (
	adminToolService = "cherami-admin"
)

// UnloadConsumerGroup unloads the CG on the given outputhost
func UnloadConsumerGroup(c *cli.Context) {
	mClient := toolscommon.GetMClient(c, adminToolService)
	toolscommon.UnloadConsumerGroup(c, mClient)
}

// ListAllCgs unloads the CG on the given outputhost
func ListAllCgs(c *cli.Context) {
	toolscommon.ListAllConsumerGroups(c)
}

// GetCgState gets the cg state on the given outputhost
func GetCgState(c *cli.Context) {
	toolscommon.GetConsumerGroupState(c)
}

type cgAckIDJSONOutputFields struct {
	Address        int64  `json:"address"`
	SessionID      uint16 `json:"sessioni_id"`
	AckMgID        uint16 `json:"ack_manager_id"`
	SequenceNumber uint32 `json:"seq_num"`
}

// ReadCgAckID parses cg ack id
func ReadCgAckID(c *cli.Context) {
	if len(c.Args()) < 1 {
		toolscommon.ExitIfError(errors.New("not enough arguments, using show dlq dlqUUID"))
	}

	cgAckID := c.Args().First()
	ackID, err := common.AckIDFromString(cgAckID)
	toolscommon.ExitIfError(err)

	sessionID, ackMgID, seqNum := ackID.MutatedID.DeconstructCombinedID()
	output := &cgAckIDJSONOutputFields{
		Address:        ackID.Address,
		SessionID:      sessionID,
		AckMgID:        ackMgID,
		SequenceNumber: seqNum,
	}
	outputStr, _ := json.Marshal(output)
	fmt.Fprintln(os.Stdout, string(outputStr))
}

// UnloadDestination unloads the destination on the given inputhost
func UnloadDestination(c *cli.Context) {
	mClient := toolscommon.GetMClient(c, adminToolService)
	toolscommon.UnloadDestination(c, mClient)
}

// ListAllLoadedDestinations unloads the destination on the given inputhost
func ListAllLoadedDestinations(c *cli.Context) {
	toolscommon.ListAllLoadedDestinations(c)
}

// GetDestinationState gets the destination state on the given inputhost
func GetDestinationState(c *cli.Context) {
	toolscommon.GetDestinationState(c)
}

type storeExtJSONOutputFields struct {
	StoreAddr      string `json:"storehost_addr"`
	StoreUUID      string `json:"storehost_uuid"`
	TotalExtent    int    `json:"total_ext"`
	OpenExtent     int    `json:"open"`
	SealedExtent   int    `json:"sealed"`
	ConsumedExtent int    `json:"consumed"`
	DeletedExtent  int    `json:"deleted"`
}

type topKExtJSONOUtputFields struct {
	ExtentUUID          string                     `json:"extent_uuid"`
	ExtentStatus        shared.ExtentStatus        `json:"extent_status"`
	ExtentReplicaStatus shared.ExtentReplicaStatus `json:"extent_replica_status"`
	QueueDepth          int64                      `json:"queue_depth"`
	BeginSequence       int64                      `json:"begin_sequence"`
	LastSequence        int64                      `json:"last_sequence"`
}

type extentAllJSONOutputFields struct {
	DestinationUUID   string                           `json:"destination_uuid"`
	ExtentUUID        string                           `json:"extent_uuid"`
	Status            shared.ExtentStatus              `json:"status"`
	InputHost         string                           `json:"inputhost,omitempty"`
	StoreHosts        []string                         `json:"storehosts,omitempty"`
	CreatedTime       time.Time                        `json:"created_time,omitempty"`
	ReplicaExtents    []*replicaExtentJSONOutputFields `json:"replica_extents"`
	StatusUpdatedTime time.Time                        `json:"status_updated_time"`
}

type replicaExtentJSONOutputFields struct {
	StoreHost         string  `json:"store_host"`
	AvailableAddress  int64   `json:"available_address"`
	AvailableSequence int64   `json:"available_sequence"`
	BeginAddress      int64   `json:"begin_address"`
	BeginSequence     int64   `json:"begin_sequence"`
	LastAddress       int64   `json:"last_address"`
	LastSequence      int64   `json:"last_sequence"`
	SizeInBytes       int64   `json:"size_in_byes"`
	SizeInBytesRate   float64 `json:"size_in_bytes_rate"`
}

// ReadStoreHost reads the store host metadata from metastore
func ReadStoreHost(c *cli.Context) {

	if len(c.Args()) < 1 {
		toolscommon.ExitIfError(errors.New("not enough arguments"))
	}
	hostAddr := c.Args()[0]
	topK := c.Int("top")

	minHeapExt := make(common.MinHeap, 0)
	heap.Init(&minHeapExt)

	mClient := toolscommon.GetMClient(c, adminToolService)
	storeHostUUID, err := mClient.HostAddrToUUID(hostAddr)
	toolscommon.ExitIfError(err)

	extRes, err1 := mClient.ListStoreExtentsStats(&metadata.ListStoreExtentsStatsRequest{
		StoreUUID: common.StringPtr(storeHostUUID),
	})
	toolscommon.ExitIfError(err1)

	nTotal := len(extRes.GetExtentStatsList())
	nOpen := 0
	nSealed := 0
	nConsumed := 0
	nDeleted := 0
	for _, stats := range extRes.GetExtentStatsList() {
		//extent := stats.GetExtent()
		if stats.GetStatus() == shared.ExtentStatus_OPEN {
			nOpen++
		} else if stats.GetStatus() == shared.ExtentStatus_SEALED {
			nSealed++
		} else if stats.GetStatus() == shared.ExtentStatus_CONSUMED {
			nConsumed++
		} else if stats.GetStatus() == shared.ExtentStatus_DELETED {
			nDeleted++
		}
		extReplicas := stats.GetReplicaStats()
		// only pushed the stats into the heap when have replica
		if len(extReplicas) > 0 {
			extReplica := extReplicas[0]
			extItem := &common.Item{
				Value: stats,
				Key:   extReplica.GetLastSequence() - extReplica.GetBeginSequence(),
			}
			heap.Push(&minHeapExt, extItem)
			if minHeapExt.Len() > topK {
				_ = heap.Pop(&minHeapExt).(*common.Item)
			}

		}
	}
	output := &storeExtJSONOutputFields{
		StoreAddr:      hostAddr,
		StoreUUID:      storeHostUUID,
		TotalExtent:    nTotal,
		OpenExtent:     nOpen,
		SealedExtent:   nSealed,
		ConsumedExtent: nConsumed,
		DeletedExtent:  nDeleted,
	}
	outputStr, _ := json.Marshal(output)
	fmt.Fprintln(os.Stdout, string(outputStr))

	var orderRes []*topKExtJSONOUtputFields
	for minHeapExt.Len() > 0 {
		extItem := heap.Pop(&minHeapExt).(*common.Item)
		stats := extItem.Value.(*shared.ExtentStats)
		ext := stats.GetExtent()
		extReplica := stats.GetReplicaStats()[0] // only pushed the stats into the heap when have replica
		output := &topKExtJSONOUtputFields{
			ExtentUUID:          ext.GetExtentUUID(),
			ExtentStatus:        stats.GetStatus(),
			ExtentReplicaStatus: extReplica.GetStatus(),
			BeginSequence:       extReplica.GetBeginSequence(),
			LastSequence:        extReplica.GetLastSequence(),
			QueueDepth:          extItem.Key,
		}
		orderRes = append([]*topKExtJSONOUtputFields{output}, orderRes...)
	}
	for _, output := range orderRes {
		outStr, _ := json.Marshal(output)
		fmt.Fprintln(os.Stdout, string(outStr))
	}
}

func printCgExtent(cgExtent *shared.ConsumerGroupExtent, mClient mcli.Client) {

	outputHostAddr, err := mClient.UUIDToHostAddr(cgExtent.GetOutputHostUUID())
	if err != nil {
		outputHostAddr = cgExtent.GetOutputHostUUID() + toolscommon.UnknownUUID
	}
	cgDesc, err1 :=
		mClient.ReadConsumerGroupByUUID(&shared.ReadConsumerGroupRequest{
			ConsumerGroupUUID: common.StringPtr(cgExtent.GetConsumerGroupUUID())})
	toolscommon.ExitIfError(err1)

	outputCgExt := &cgExtentJSONOutputFields{
		ExtentUUID:         cgExtent.GetExtentUUID(),
		CGName:             cgDesc.GetConsumerGroupName(),
		CGUUID:             cgDesc.GetConsumerGroupUUID(),
		CGDlq:              cgDesc.GetDeadLetterQueueDestinationUUID(),
		CGEmail:            cgDesc.GetOwnerEmail(),
		OutputHostAddr:     outputHostAddr,
		OutputHostUUID:     cgExtent.GetOutputHostUUID(),
		Status:             cgExtent.GetStatus(),
		AckLevelOffset:     cgExtent.GetAckLevelOffset(),
		AckLevelSeqNo:      cgExtent.GetAckLevelSeqNo(),
		AckLeverSeqNoRate:  cgExtent.GetAckLevelSeqNoRate(),
		ReadLevelOffset:    cgExtent.GetReadLevelOffset(),
		ReadLevelSeqNo:     cgExtent.GetReadLevelSeqNo(),
		ReadLevelSeqNoRate: cgExtent.GetReadLevelSeqNoRate(),
	}

	outputCgStr, _ := json.Marshal(outputCgExt)
	fmt.Fprintln(os.Stdout, string(outputCgStr))
}

// ReadExtent shows the detailed information of an extent
func ReadExtent(c *cli.Context) {
	if len(c.Args()) < 1 {
		toolscommon.ExitIfError(errors.New("not enough arguments"))
	}

	uuidStr := c.Args()[0]
	mClient := toolscommon.GetMClient(c, adminToolService)
	showCG := string(c.String("showcg"))

	descExtent, err1 := mClient.ReadExtentStats(&metadata.ReadExtentStatsRequest{
		ExtentUUID: &uuidStr,
	})

	toolscommon.ExitIfError(err1)
	extentStats := descExtent.GetExtentStats()
	extent := extentStats.GetExtent()
	extReplicas := extentStats.GetReplicaStats()

	storeHosts := []string{}
	storeHostUUIDs := extent.GetStoreUUIDs()
	for _, storeUUID := range storeHostUUIDs {
		storeHostAddr, err2 := mClient.UUIDToHostAddr(storeUUID)
		if err2 != nil {
			storeHostAddr = storeUUID + toolscommon.UnknownUUID
		}
		storeHosts = append(storeHosts, storeHostAddr)
	}

	inputHostAddr, err3 := mClient.UUIDToHostAddr(extent.GetInputHostUUID())
	if err3 != nil {
		inputHostAddr = extent.GetInputHostUUID() + toolscommon.UnknownUUID
	}
	replicaExtents := []*replicaExtentJSONOutputFields{}
	if len(extReplicas) > 0 {
		// updata the begin seq, last seq in extent replicas
		for _, extReplica := range extReplicas {
			storeHostAddr, err := mClient.UUIDToHostAddr(extReplica.GetStoreUUID())
			if err != nil {
				storeHostAddr = extReplica.GetStoreUUID() + toolscommon.UnknownUUID
			}
			replicaJSON := &replicaExtentJSONOutputFields{
				StoreHost:         storeHostAddr,
				AvailableAddress:  extReplica.GetAvailableAddress(),
				AvailableSequence: extReplica.GetAvailableSequence(),
				BeginAddress:      extReplica.GetBeginAddress(),
				LastAddress:       extReplica.GetLastAddress(),
				BeginSequence:     extReplica.GetBeginSequence(),
				LastSequence:      extReplica.GetLastSequence(),
				SizeInBytes:       extReplica.GetSizeInBytes(),
				SizeInBytesRate:   extReplica.GetSizeInBytesRate(),
			}
			replicaExtents = append(replicaExtents, replicaJSON)
		}
	}
	output := &extentAllJSONOutputFields{
		DestinationUUID:   extent.GetDestinationUUID(),
		ExtentUUID:        uuidStr,
		Status:            extentStats.GetStatus(),
		CreatedTime:       time.Unix(0, *(extentStats.CreatedTimeMillis)*1000000),
		StatusUpdatedTime: time.Unix(0, *(extentStats.StatusUpdatedTimeMillis)*1000000),
		InputHost:         inputHostAddr,
		StoreHosts:        storeHosts,
		ReplicaExtents:    replicaExtents,
	}

	outputStr, _ := json.Marshal(output)
	fmt.Fprintln(os.Stdout, string(outputStr))

	// only show cg infor if showCG flag is true
	if showCG == "true" {
		// based on extent uuid ,get the related consumer group info
		req := &metadata.ReadConsumerGroupExtentsByExtUUIDRequest{
			ExtentUUID: common.StringPtr(uuidStr),
			Limit:      common.Int64Ptr(toolscommon.DefaultPageSize),
		}

		mResp, err4 := mClient.ReadConsumerGroupExtentsByExtUUID(req)
		toolscommon.ExitIfError(err4)
		for _, cgExtent := range mResp.GetCgExtents() {
			// get the consumer group name and storehost addr based on cgUUID and storeUUID and print out
			printCgExtent(cgExtent, mClient)
		}
	}
}

func matchExtentStatus(status string, wantStatus shared.ConsumerGroupExtentStatus) bool {

	switch status {
	case "open":
		return wantStatus == shared.ConsumerGroupExtentStatus_OPEN
	case "consumed":
		return wantStatus == shared.ConsumerGroupExtentStatus_CONSUMED
	case "deleted":
		return wantStatus == shared.ConsumerGroupExtentStatus_DELETED
	default:
		toolscommon.ExitIfError(errors.New("please use right status: open | consumed | deleted"))
	}
	return false
}

// ReadCgQueue shows the detailed information of a cg
func ReadCgQueue(c *cli.Context) {
	if len(c.Args()) < 1 {
		toolscommon.ExitIfError(errors.New("not enough arguments, using show cgq cg_uuid or show cgq cg_uuid ext_uuid"))
	}

	extentStatus := string(c.String("status"))

	cgUUID := c.Args()[0]
	mClient := toolscommon.GetMClient(c, adminToolService)
	if len(c.Args()) >= 2 {
		extUUID := c.Args()[1]

		req := &metadata.ReadConsumerGroupExtentRequest{
			ConsumerGroupUUID: common.StringPtr(cgUUID),
			ExtentUUID:        common.StringPtr(extUUID),
		}
		result, err1 := mClient.ReadConsumerGroupExtent(req)

		toolscommon.ExitIfError(err1)
		printCgExtent(result.GetExtent(), mClient)
		return
	}

	if len(c.Args()) == 1 {
		req := &shared.ReadConsumerGroupExtentsRequest{
			ConsumerGroupUUID: common.StringPtr(cgUUID),
			MaxResults:        common.Int32Ptr(int32(toolscommon.DefaultPageSize)),
		}

		for {
			mResp, err := mClient.ReadConsumerGroupExtents(req)
			toolscommon.ExitIfError(err)

			for _, cgExtent := range mResp.GetExtents() {
				// if -status option not provide, show all the extent queue
				status := cgExtent.GetStatus()
				if len(extentStatus) == 0 || matchExtentStatus(extentStatus, status) {
					printCgExtent(cgExtent, mClient)
				}
			}

			if len(mResp.GetExtents()) < toolscommon.DefaultPageSize {
				break
			} else {
				req.PageToken = mResp.NextPageToken
			}
		}
	}
}

func getExtentStatusFromString(status string) shared.ExtentStatus {

	switch status {
	case "open":
		return shared.ExtentStatus_OPEN
	case "sealed":
		return shared.ExtentStatus_SEALED
	case "consumed":
		return shared.ExtentStatus_CONSUMED
	case "archived":
		return shared.ExtentStatus_ARCHIVED
	case "deleted":
		return shared.ExtentStatus_DELETED
	default:
		toolscommon.ExitIfError(errors.New("please use right status: open | sealed | consumed | archived | deleted"))
	}
	return shared.ExtentStatus_OPEN
}

// ReadDestQueue shows the detailed information of destination queue
func ReadDestQueue(c *cli.Context) {
	if len(c.Args()) < 1 {
		toolscommon.ExitIfError(errors.New("not enough arguments, using show destq dest_uuid or show destq dest_path ext_uuid"))
	}

	destPath := c.Args()[0]
	status := string(c.String("status"))

	extentStatus := getExtentStatusFromString(status)
	mClient := toolscommon.GetMClient(c, adminToolService)
	cClient := toolscommon.GetCClient(c, adminToolService)

	desc, err := cClient.ReadDestination(&cherami.ReadDestinationRequest{
		Path: &destPath,
	})

	toolscommon.ExitIfError(err)

	listExtentsStats := &shared.ListExtentsStatsRequest{
		DestinationUUID: desc.DestinationUUID,
		Status:          &extentStatus,
		Limit:           common.Int64Ptr(toolscommon.DefaultPageSize),
	}

	for {
		listExtentStatsResult, err1 := mClient.ListExtentsStats(listExtentsStats)
		toolscommon.ExitIfError(err1)

		for _, stats := range listExtentStatsResult.GetExtentStatsList() {
			extent := stats.GetExtent()
			extReplicas := stats.GetReplicaStats()
			replicaExtents := []*replicaExtentJSONOutputFields{}
			if len(extReplicas) > 0 {
				// updata the begin seq, last seq in extent replicas
				for _, extReplica := range extReplicas {
					storeHostAddr, err4 := mClient.UUIDToHostAddr(extReplica.GetStoreUUID())
					if err4 != nil {
						storeHostAddr = extReplica.GetStoreUUID() + toolscommon.UnknownUUID
					}
					replicaJSON := &replicaExtentJSONOutputFields{
						StoreHost:         storeHostAddr,
						AvailableAddress:  extReplica.GetAvailableAddress(),
						AvailableSequence: extReplica.GetAvailableSequence(),
						BeginAddress:      extReplica.GetBeginAddress(),
						LastAddress:       extReplica.GetLastAddress(),
						BeginSequence:     extReplica.GetBeginSequence(),
						LastSequence:      extReplica.GetLastSequence(),
						SizeInBytes:       extReplica.GetSizeInBytes(),
						SizeInBytesRate:   extReplica.GetSizeInBytesRate(),
					}
					replicaExtents = append(replicaExtents, replicaJSON)
				}
			}
			output := &extentAllJSONOutputFields{
				DestinationUUID:   extent.GetDestinationUUID(),
				ExtentUUID:        extent.GetExtentUUID(),
				Status:            stats.GetStatus(),
				CreatedTime:       time.Unix(0, *(stats.CreatedTimeMillis)*1000000),
				StatusUpdatedTime: time.Unix(0, *(stats.StatusUpdatedTimeMillis)*1000000),
				ReplicaExtents:    replicaExtents,
			}

			outputStr, _ := json.Marshal(output)
			fmt.Fprintln(os.Stdout, string(outputStr))
		}

		if len(listExtentStatsResult.GetNextPageToken()) == 0 {
			break
		} else {
			listExtentsStats.PageToken = listExtentStatsResult.GetNextPageToken()
		}
	}
}

func matchDestAllStatus(status string, wantStatus shared.DestinationStatus) bool {

	switch status {
	case "enabled":
		return wantStatus == shared.DestinationStatus_ENABLED
	case "disabled":
		return wantStatus == shared.DestinationStatus_DISABLED
	case "sendonly":
		return wantStatus == shared.DestinationStatus_SENDONLY
	case "recvonly":
		return wantStatus == shared.DestinationStatus_RECEIVEONLY
	case "deleting":
		return wantStatus == shared.DestinationStatus_DELETING
	case "deleted":
		return wantStatus == shared.DestinationStatus_DELETED
	default:
		toolscommon.ExitIfError(errors.New("please use right status: enabled | disabled | sendonly | recvonly"))
	}
	return false
}

type destAllJSONOutputFields struct {
	DestinationName             string                   `json:"destination_name"`
	DestinationUUID             string                   `json:"destination_uuid"`
	Status                      shared.DestinationStatus `json:"status"`
	Type                        shared.DestinationType   `json:"type"`
	OwnerEmail                  string                   `json:"owner_email"`
	TotalExts                   int                      `json:"total_ext"`
	OpenExts                    int                      `json:"open"`
	SealedExts                  int                      `json:"sealed"`
	ConsumedExts                int                      `json:"consumed"`
	DeletedExts                 int                      `json:"deleted"`
	ConsumedMessagesRetention   int32                    `json:"consumed_messages_retention"`
	UnconsumedMessagesRetention int32                    `json:"unconsumed_messages_retention"`
}

// ListAllDestinations lists all the destinations, user can choose deleted status too
func ListAllDestinations(c *cli.Context) {

	included := string(c.String("include"))
	excluded := string(c.String("exclude"))
	destStatus := string(c.String("status"))

	req := &shared.ListDestinationsByUUIDRequest{
		Limit: common.Int64Ptr(toolscommon.DefaultPageSize),
	}

	mClient := toolscommon.GetMClient(c, adminToolService)

	inReg, errI := regexp.Compile(included)
	toolscommon.ExitIfError(errI)
	exReg, errE := regexp.Compile(excluded)
	toolscommon.ExitIfError(errE)
	for {
		resp, err := mClient.ListDestinationsByUUID(req)
		toolscommon.ExitIfError(err)

		for _, desc := range resp.GetDestinations() {
			listExtentsStats := &shared.ListExtentsStatsRequest{
				DestinationUUID: desc.DestinationUUID,
				Limit:           common.Int64Ptr(toolscommon.DefaultPageSize),
			}
			if len(included) > 0 && !inReg.MatchString(desc.GetPath()) {
				continue
			}
			if len(excluded) > 0 && exReg.MatchString(desc.GetPath()) {
				continue
			}
			listExtentStatsResult, err1 := mClient.ListExtentsStats(listExtentsStats)
			toolscommon.ExitIfError(err1)

			nOpen := 0
			nSealed := 0
			nConsumed := 0
			nDeleted := 0
			for _, stats := range listExtentStatsResult.ExtentStatsList {
				if stats.GetStatus() == shared.ExtentStatus_OPEN {
					nOpen++
				} else if stats.GetStatus() == shared.ExtentStatus_SEALED {
					nSealed++
				} else if stats.GetStatus() == shared.ExtentStatus_CONSUMED {
					nConsumed++
				} else if stats.GetStatus() == shared.ExtentStatus_DELETED {
					nDeleted++
				}
			}

			// if -status option not provide, show all the destination path
			status := desc.GetStatus()
			if len(destStatus) == 0 || matchDestAllStatus(destStatus, status) {
				outputDest := &destAllJSONOutputFields{
					desc.GetPath(),
					desc.GetDestinationUUID(),
					status,
					desc.GetType(),
					desc.GetOwnerEmail(),
					len(listExtentStatsResult.ExtentStatsList),
					nOpen,
					nSealed,
					nConsumed,
					nDeleted,
					desc.GetConsumedMessagesRetention(),
					desc.GetUnconsumedMessagesRetention(),
				}
				outputStr, _ := json.Marshal(outputDest)
				fmt.Fprintln(os.Stdout, string(outputStr))
			}
		}

		if len(resp.GetDestinations()) < toolscommon.DefaultPageSize {
			break
		} else {
			req.PageToken = resp.NextPageToken
		}
	}
}

type extentJSONOutputFields struct {
	DestinationUUID   *string             `json:"destination_uuid"`
	ExtentUUID        string              `json:"extent_uuid"`
	Status            shared.ExtentStatus `json:"status"`
	InputHost         string              `json:"inputhost"`
	StoreHosts        []string            `json:"storehosts"`
	CreatedTime       time.Time           `json:"created_time,omitempty"`
	StatusUpdatedTime time.Time           `json:"status_updated_time"`
}

type cgExtentJSONOutputFields struct {
	ExtentUUID         string                           `json:"extent_uuid"`
	CGName             string                           `json:"consumer_group_name"`
	CGUUID             string                           `json:"consumer_group_uuid"`
	CGEmail            string                           `json:"owner_email"`
	CGDlq              string                           `json:"dlq_uuid"`
	OutputHostAddr     string                           `json:"outputhost_addr"`
	OutputHostUUID     string                           `json:"outputhost_uuid"`
	Status             shared.ConsumerGroupExtentStatus `json:"status"`
	AckLevelOffset     int64                            `json:"ack_level_offset"`
	AckLevelSeqNo      int64                            `json:"ack_level_seq_no"`
	AckLeverSeqNoRate  float64                          `json:"ack_level_seq_no_rate"`
	ReadLevelOffset    int64                            `json:"read_level_offset"`
	ReadLevelSeqNo     int64                            `json:"read_level_seq_no"`
	ReadLevelSeqNoRate float64                          `json:"read_level_seq_no_rate"`
}

// ListExtents lists all the extents of a destination
func ListExtents(c *cli.Context) {
	if len(c.Args()) < 1 {
		toolscommon.ExitIfError(errors.New("not enough arguments"))
	}

	path := c.Args().First()
	prefix := string(c.String("prefix"))

	cClient := toolscommon.GetCClient(c, adminToolService)
	desc, err := cClient.ReadDestination(&cherami.ReadDestinationRequest{
		Path: &path,
	})

	toolscommon.ExitIfError(err)

	// if we come here, there should no error and desc != nil
	mClient := toolscommon.GetMClient(c, adminToolService)

	listExtentsStats := &shared.ListExtentsStatsRequest{
		DestinationUUID: desc.DestinationUUID,
		Limit:           common.Int64Ptr(toolscommon.DefaultPageSize),
	}

	for {
		listExtentStatsResult, err1 := mClient.ListExtentsStats(listExtentsStats)
		toolscommon.ExitIfError(err1)

		for _, stats := range listExtentStatsResult.ExtentStatsList {
			extent := stats.GetExtent()
			extentUUID := extent.GetExtentUUID()
			if len(prefix) > 0 && !strings.HasPrefix(extentUUID, prefix) {
				continue
			}

			inputHostAddr, err2 := mClient.UUIDToHostAddr(extent.GetInputHostUUID())
			if err2 != nil {
				inputHostAddr = extent.GetInputHostUUID() + toolscommon.UnknownUUID
			}

			storeHosts := []string{}
			storeHostUUIDs := extent.GetStoreUUIDs()
			for _, storeUUID := range storeHostUUIDs {
				storeHostAddr, err3 := mClient.UUIDToHostAddr(storeUUID)
				if err3 != nil {
					storeHostAddr = storeUUID + toolscommon.UnknownUUID
				}
				storeHosts = append(storeHosts, storeHostAddr)
			}

			output := &extentJSONOutputFields{
				DestinationUUID:   desc.DestinationUUID,
				ExtentUUID:        extentUUID,
				Status:            stats.GetStatus(),
				InputHost:         inputHostAddr,
				StoreHosts:        storeHosts,
				CreatedTime:       time.Unix(0, *(stats.CreatedTimeMillis)*1000000),
				StatusUpdatedTime: time.Unix(0, *(stats.StatusUpdatedTimeMillis)*1000000),
			}

			outputStr, _ := json.Marshal(output)
			fmt.Fprintln(os.Stdout, string(outputStr))
		}

		if len(listExtentStatsResult.GetNextPageToken()) == 0 {
			break
		} else {
			listExtentsStats.PageToken = listExtentStatsResult.GetNextPageToken()
		}
	}
}

type hostJSONOutputFields struct {
	HostRole string `json:"role"`
	HostName string `json:"host_name"`
	HostAddr string `json:"host_Addr"`
	HostUUID string `json:"host_uuid"`
}

// ListHosts lists all the hosts and roles in the current deployment
func ListHosts(c *cli.Context) {

	name := string(c.String("service"))
	hostType := string(c.String("type"))
	adminToolService := fmt.Sprintf("cherami-%vhost", name)

	reqType := metadata.HostType_HOST
	if hostType == "history" {
		reqType = metadata.HostType_UUID
	}

	var hostsInfo = make(map[string][]*metadata.HostDescription, 0)

	mClient := toolscommon.GetMClient(c, adminToolService)

	req := &metadata.ListHostsRequest{
		HostType: &reqType,
		Limit:    common.Int64Ptr(toolscommon.DefaultPageSize),
	}

	for {
		resp, err := mClient.ListHosts(req)
		toolscommon.ExitIfError(err)

		for _, desc := range resp.GetHosts() {
			hostAddr := desc.GetHostAddr()
			ipAndPort := strings.Split(hostAddr, ":")
			portNum := ipAndPort[1]
			for service, realPort := range common.ServiceToPort {
				if portNum == realPort {
					hostsInfo[service] = append(hostsInfo[service], desc)
					break
				}
			}
		}

		if len(resp.GetHosts()) < toolscommon.DefaultPageSize {
			break
		} else {
			req.PageToken = resp.NextPageToken
		}
	}
	if len(name) > 0 {
		printHosts(adminToolService, hostsInfo[adminToolService])
	} else {
		for service, hosts := range hostsInfo {
			printHosts(service, hosts)
		}
	}
}

func printHosts(service string, hosts []*metadata.HostDescription) {
	for _, desc := range hosts {
		hostAddr := desc.GetHostAddr()
		output := &hostJSONOutputFields{
			HostRole: service,
			HostName: desc.GetHostName(),
			HostAddr: hostAddr,
			HostUUID: desc.GetHostUUID()}
		outputStr, _ := json.Marshal(output)
		fmt.Fprintln(os.Stdout, string(outputStr))
	}
	fmt.Println()
}

type consumerGroupExtentJSONOutputFields struct {
	ExtentUUID     string   `json:"extent_uuid"`
	AckLevelOffset int64    `json:"ack_level_offset"`
	StoreHosts     []string `json:"storehosts"`
	OutputHost     string   `json:"outputhost"`
}

//ListEntityOps lists all CRUD ops related with the destination
func ListEntityOps(c *cli.Context) {
	mClient := toolscommon.GetMClient(c, adminToolService)
	name := c.String("name")
	UUID := c.String("uuid")
	maxResult := c.Int("limit")
	req := &metadata.ListEntityOpsRequest{
		EntityUUID: common.StringPtr(UUID),
		EntityName: common.StringPtr(name),
		EntityType: common.StringPtr(""),
		Limit:      common.Int64Ptr(toolscommon.DefaultPageSize),
	}
	count := 0
	for {
		mResp, err := mClient.ListEntityOps(req)
		toolscommon.ExitIfError(err)

		for _, opsEntity := range mResp.GetEntityOps() {
			if count >= maxResult {
				break
			}
			count++

			outputStr, _ := json.Marshal(opsEntity)
			fmt.Fprintln(os.Stdout, string(outputStr))
		}

		if len(mResp.GetNextPageToken()) == 0 {
			break
		} else {
			req.PageToken = mResp.GetNextPageToken()
		}
	}
}

// ListConsumerGroupExtents lists all the consumer group extents
func ListConsumerGroupExtents(c *cli.Context) {
	destinationPath := c.Args().First()
	consumerGroupPath := c.Args()[1]

	destUUID := destinationPath
	cgUUID := consumerGroupPath

	cClient := toolscommon.GetCClient(c, adminToolService)

	if common.PathRegex.MatchString(destinationPath) && common.ConsumerGroupRegex.MatchString(consumerGroupPath) {
		readRequest := cherami.NewReadConsumerGroupRequest()
		readRequest.DestinationPath = &destinationPath
		readRequest.ConsumerGroupName = &consumerGroupPath

		cgDesc, err := cClient.ReadConsumerGroup(readRequest)
		toolscommon.ExitIfError(err)

		if cgDesc != nil {
			destUUID = cgDesc.GetDestinationUUID()
			cgUUID = cgDesc.GetConsumerGroupUUID()
		}
	}

	mClient := toolscommon.GetMClient(c, adminToolService)

	maxResults := c.Int("limit")
	req := &shared.ReadConsumerGroupExtentsRequest{
		DestinationUUID:   common.StringPtr(destUUID),
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		MaxResults:        common.Int32Ptr(int32(toolscommon.DefaultPageSize)),
	}

	count := 0
	for {
		mResp, err := mClient.ReadConsumerGroupExtents(req)
		toolscommon.ExitIfError(err)

		for _, cgExtent := range mResp.GetExtents() {
			if count >= maxResults {
				break
			}
			count++

			storeHosts := []string{}
			storeHostUUIDs := cgExtent.GetStoreUUIDs()
			for _, storeUUID := range storeHostUUIDs {
				storeHostAddr, err1 := mClient.UUIDToHostAddr(storeUUID)
				if err1 != nil {
					storeHostAddr = storeUUID + toolscommon.UnknownUUID
				}
				storeHosts = append(storeHosts, storeHostAddr)
			}

			outputHostAddr, err := mClient.UUIDToHostAddr(*cgExtent.OutputHostUUID)
			if err != nil {
				outputHostAddr = *cgExtent.OutputHostUUID + toolscommon.UnknownUUID
			}

			output := &consumerGroupExtentJSONOutputFields{
				ExtentUUID:     cgExtent.GetExtentUUID(),
				AckLevelOffset: cgExtent.GetAckLevelOffset(),
				StoreHosts:     storeHosts,
				OutputHost:     outputHostAddr,
			}
			outputStr, _ := json.Marshal(output)
			fmt.Fprintln(os.Stdout, string(outputStr))
		}

		if len(mResp.GetNextPageToken()) == 0 {
			break
		} else {
			req.PageToken = mResp.GetNextPageToken()
		}
	}
}

// UUID2hostAddr returns the address port given a uuid
func UUID2hostAddr(c *cli.Context) {
	if len(c.Args()) < 1 {
		toolscommon.ExitIfError(errors.New("not enough arguments"))
	}
	uuid := c.Args().First()

	mClient := toolscommon.GetMClient(c, adminToolService)
	hostAddr, err := mClient.UUIDToHostAddr(uuid)
	toolscommon.ExitIfError(err)

	fmt.Fprintf(os.Stdout, "%v\n", hostAddr)
}

// HostAddr2uuid returns the uuid based on a host port information
func HostAddr2uuid(c *cli.Context) {
	if len(c.Args()) < 1 {
		toolscommon.ExitIfError(errors.New("not enough arguments"))
	}
	hostAddr := c.Args().First()

	mClient := toolscommon.GetMClient(c, adminToolService)
	uuid, err := mClient.HostAddrToUUID(hostAddr)
	toolscommon.ExitIfError(err)

	fmt.Fprintf(os.Stdout, "%v\n", uuid)
}

const nServiceConfigTreeLevels = 5

var errInvalidServiceName = errors.New("ServiceName is invalid")
var errInvalidConfigKey = errors.New("configKey must be of the form serviceName.version.sku.hostname.key")

func splitServiceConfigKey(arg string) ([]string, error) {
	tokens := strings.Split(arg, ".")
	if len(tokens) != nServiceConfigTreeLevels {
		return nil, errInvalidConfigKey
	}
	for i := 0; i < len(tokens); i++ {
		if len(tokens[i]) == 0 {
			return nil, errInvalidConfigKey
		}
	}
	if !common.IsValidServiceName(tokens[0]) {
		return nil, errInvalidServiceName
	}
	// make sure configKey is not a wildcard
	if tokens[4] == "*" {
		return nil, errInvalidConfigKey
	}
	return tokens, nil
}

// GetServiceConfig prints the config items matching
// the given input criteria.
func GetServiceConfig(c *cli.Context) {
	if len(c.Args()) < 1 {
		toolscommon.ExitIfError(errors.New("not enough arguments"))
	}

	configKey := ""
	serviceName := c.Args().First()

	if !common.IsValidServiceName(serviceName) {
		toolscommon.ExitIfError(errInvalidServiceName)
	}

	isKeySet := c.IsSet("key")
	if isKeySet {
		configKey = strings.ToLower(c.String("key"))
	}

	mClient := toolscommon.GetMClient(c, adminToolService)
	request := &metadata.ReadServiceConfigRequest{
		ServiceName: common.StringPtr(serviceName),
	}

	result, err := mClient.ReadServiceConfig(request)
	toolscommon.ExitIfError(err)

	for _, cItem := range result.GetConfigItems() {

		if isKeySet && cItem.GetConfigKey() != configKey {
			continue
		}

		keyParams := []string{
			serviceName,
			cItem.GetServiceVersion(),
			cItem.GetSku(),
			cItem.GetHostname(),
			cItem.GetConfigKey(),
		}

		fmt.Fprintf(os.Stdout, "%v=%v\n", strings.Join(keyParams, "."), cItem.GetConfigValue())
	}
}

// SetServiceConfig persists the given key-value
// config mapping onto the config store.
func SetServiceConfig(c *cli.Context) {
	if len(c.Args()) < 2 {
		toolscommon.ExitIfError(errors.New("not enough arguments"))
	}

	tokens, err := splitServiceConfigKey(c.Args()[0])
	toolscommon.ExitIfError(err)

	configValue := c.Args()[1]

	mClient := toolscommon.GetMClient(c, adminToolService)

	cItem := &metadata.ServiceConfigItem{
		ServiceName:    common.StringPtr(strings.ToLower(tokens[0])),
		ServiceVersion: common.StringPtr(strings.ToLower(tokens[1])),
		Sku:            common.StringPtr(strings.ToLower(tokens[2])),
		Hostname:       common.StringPtr(strings.ToLower(tokens[3])),
		ConfigKey:      common.StringPtr(strings.ToLower(tokens[4])),
		ConfigValue:    common.StringPtr(configValue),
	}

	req := &metadata.UpdateServiceConfigRequest{ConfigItem: cItem}
	err = mClient.UpdateServiceConfig(req)
	toolscommon.ExitIfError(err)
}

// DeleteServiceConfig deletes the service config items
// matching the given criteria
func DeleteServiceConfig(c *cli.Context) {
	if len(c.Args()) < 1 {
		toolscommon.ExitIfError(errors.New("not enough arguments"))
	}

	tokens, err := splitServiceConfigKey(c.Args().First())
	toolscommon.ExitIfError(err)

	mClient := toolscommon.GetMClient(c, adminToolService)

	req := &metadata.DeleteServiceConfigRequest{
		ServiceName:    common.StringPtr(strings.ToLower(tokens[0])),
		ServiceVersion: common.StringPtr(strings.ToLower(tokens[1])),
		Sku:            common.StringPtr(strings.ToLower(tokens[2])),
		Hostname:       common.StringPtr(strings.ToLower(tokens[3])),
		ConfigKey:      common.StringPtr(strings.ToLower(tokens[4])),
	}

	err = mClient.DeleteServiceConfig(req)
	toolscommon.ExitIfError(err)
}

// SealConsistencyCheck iterates through every sealed extent for every destination
// and checks to see if the corresponding replicas have been sealed.
func SealConsistencyCheck(c *cli.Context) {

	mClient := toolscommon.GetMClient(c, adminToolService)
	toolscommon.SealConsistencyCheck(c, mClient)
}

// StoreSealExtent sends a SealExtent command to the specified store.
func StoreSealExtent(c *cli.Context) {

	mClient := toolscommon.GetMClient(c, adminToolService)
	toolscommon.StoreSealExtent(c, mClient)
}

// StoreIsExtentSealed checks if an extent is sealed on the specified store
func StoreIsExtentSealed(c *cli.Context) {

	mClient := toolscommon.GetMClient(c, adminToolService)
	toolscommon.StoreIsExtentSealed(c, mClient)
}

// StoreGetAddressFromTimestamp sends a GetAddressFromTimestamp command to the specified store.
func StoreGetAddressFromTimestamp(c *cli.Context) {

	mClient := toolscommon.GetMClient(c, adminToolService)
	toolscommon.StoreGetAddressFromTimestamp(c, mClient)
}

// StorePurgeMessages sends a purge command for an extent to the specified store.
func StorePurgeMessages(c *cli.Context) {

	mClient := toolscommon.GetMClient(c, adminToolService)
	toolscommon.StorePurgeMessages(c, mClient)
}

// StoreListExtents sends a list-extents request to the specified store.
func StoreListExtents(c *cli.Context) {

	mClient := toolscommon.GetMClient(c, adminToolService)
	toolscommon.StoreListExtents(c, mClient)
}
