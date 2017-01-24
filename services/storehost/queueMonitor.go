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

package storehost

import (
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

type (
	// QueueMonitor keeps record of extents progress, and flush to metastore periodically
	QueueMonitor interface {
		common.Daemon
	}

	// queueMonitor is an implementation of QueueMonitor.
	queueMonitor struct {
		storeHost *StoreHost

		mClient metadata.TChanMetadataService
		logger  bark.Logger

		closeChannel chan struct{}

		queueInfoReportTicker *time.Ticker
		inReportingSession    int64

		// Store the snapshot of the extentInfoMap
		prevExtentInfoMap map[string]*seqNumSnapshot

		reportSpanDuration time.Duration
	}
)

// ReportInterval determines how often reporting housekeeping will happen. It is exported to allow test to modify this value
var ReportInterval = time.Duration(1 * time.Minute)

// ReportPause is used for testing to allow the reporter to be paused at will
var ReportPause *int32

// Target to evenly distribute the reporting of all extents to the first 50 seconds

// NewQueueMonitor returns an instance of NewQueueMonitor that can be
// used to monitor and report all the extents progress on a store host.
func (host *StoreHost) NewQueueMonitor(mClient metadata.TChanMetadataService, store *StoreHost, logger bark.Logger) QueueMonitor {
	return &queueMonitor{
		storeHost:             store,
		mClient:               mClient,
		logger:                logger,
		queueInfoReportTicker: time.NewTicker(ReportInterval),
		inReportingSession:    0,
		reportSpanDuration:    ReportInterval * 5 / 6, // 50 seconds in the default case
	}
}

func (monitor *queueMonitor) Start() {
	monitor.logger.Info("queueMonitor: started")

	monitor.closeChannel = make(chan struct{})

	go monitor.doHouseKeeping()
}

func (monitor *queueMonitor) Stop() {
	close(monitor.closeChannel)

	monitor.logger.Info("queueMonitor: stopped")
}

func (monitor *queueMonitor) doReport(extentID string, prevQueueInfo *seqNumSnapshot, extentInfoMap map[string]*seqNumSnapshot) {

	queueInfo, ok := extentInfoMap[extentID]

	stats := &shared.ExtentReplicaStats{
		ExtentUUID:       common.StringPtr(extentID),
		StoreUUID:        common.StringPtr(monitor.storeHost.GetHostUUID()),
		LastSequenceRate: common.Float64Ptr(0),
	}

	// The extent is gone, fill in most info from saved in previous round
	if !ok {
		stats.BeginSequence = common.Int64Ptr(prevQueueInfo.beginSeqNum)
		stats.LastSequence = common.Int64Ptr(prevQueueInfo.lastSeqNum)
	} else {
		stats.BeginSequence = common.Int64Ptr(queueInfo.beginSeqNum)
		stats.LastSequence = common.Int64Ptr(queueInfo.lastSeqNum)
		stats.LastSequenceRate = common.Float64Ptr(common.CalculateRate(
			common.SequenceNumber(prevQueueInfo.lastSeqNum),
			common.SequenceNumber(queueInfo.lastSeqNum),
			common.UnixNanoTime(prevQueueInfo.snapshotTime),
			common.UnixNanoTime(queueInfo.snapshotTime),
		))
	}

	//	if monitor.storeHost.mode == AppendOnly {
	stats.AvailableSequence = stats.LastSequence
	stats.AvailableSequenceRate = stats.LastSequenceRate
	//	}

	// logger.WithFields(bark.Fields{
	// 	common.TagExt, extentID,
	// 	`begin-seq`:          stats.GetBeginSequence(),         // #perfdisable
	// 	`last-seq`:           stats.GetLastSequence(),          // #perfdisable
	// 	`last-seq-rate`:      stats.GetLastSequenceRate(),      // #perfdisable
	// 	`available-seq`:      stats.GetAvailableSequence(),     // #perfdisable
	// 	`available-seq-rate`: stats.GetAvailableSequenceRate(), // #perfdisable
	// }).Info("queueMonitor: report") // #perfdisable

	updateStatsRequest := &metadata.UpdateStoreExtentReplicaStatsRequest{
		ExtentUUID:   common.StringPtr(extentID),
		ReplicaStats: []*shared.ExtentReplicaStats{stats},
	}
	monitor.mClient.UpdateStoreExtentReplicaStats(nil, updateStatsRequest)
}

func (monitor *queueMonitor) flushToMetaStore() {
	if !atomic.CompareAndSwapInt64(&monitor.inReportingSession, 0, 1) {
		// DATA RACE: Don't read monitor.prevExtentInfoMap here
		monitor.logger.Warn("Prev flushToMetaStore is still ongoing...")
		return
	}

	currentExtentInfoMap := monitor.storeHost.xMgr.getAllExtentsSeqNumSnapshot()

	total := int64(len(monitor.prevExtentInfoMap))

	// Loop through all previouly saved extents
	// For those new, will be reported next round
	// For those gone, report a 0 rate to wrap it up
	if total > 0 {

		// logger.WithField(`total`, total).Info(`Reporting started for extents`) // #perfdisable

		durationPerWorkItem := time.Duration(monitor.reportSpanDuration.Nanoseconds()/total) * time.Nanosecond
		reportStartTime := time.Now()
		for extentID, prevExtentInfo := range monitor.prevExtentInfoMap {
			monitor.doReport(extentID, prevExtentInfo, currentExtentInfoMap)

			reportStartTime = reportStartTime.Add(durationPerWorkItem)
			time.Sleep(reportStartTime.Sub(time.Now()))
		}
	}

	// Save for rate calculation
	monitor.prevExtentInfoMap = currentExtentInfoMap

	atomic.StoreInt64(&monitor.inReportingSession, 0)
}

func (monitor *queueMonitor) doHouseKeeping() {
	for {
		select {
		case <-monitor.queueInfoReportTicker.C:
			queueMonitorWait() // For testing purposes, pause the reporter to allow the test to modify values
			go monitor.flushToMetaStore()
		case <-monitor.closeChannel:
			return
		}
	}
}

// queueMonitorWait is a replacement for sync.WaitGroup that allows benign races. The Golang race detector is picky about
// the order of calling WaitGroup.Add() and WaitGroup.Wait(), and races between those two routines
func queueMonitorWait() {
	if ReportPause != nil {
		for atomic.LoadInt32(ReportPause) != 0 { // Low-CPU spinlock
			time.Sleep(time.Second / 10)
		}
	}
}

// QueueMonitorPause is for testing use. It causes the queueMonitor to wait at the next tick, instead of doing its normal processing
func QueueMonitorPause() {
	atomic.StoreInt32(ReportPause, 1)
}

// QueueMonitorUnpause is for testing use. It causes the queueMonitor to operate normally.
func QueueMonitorUnpause() {
	atomic.StoreInt32(ReportPause, 0)
}
