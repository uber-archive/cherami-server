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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

const (
	reportChanBufLen      = 1024
	defaultReportInterval = time.Duration(time.Minute)
)

type (
	// ExtStatsReporter reports extent stats to metadata
	ExtStatsReporter struct {
		hostID  string
		xMgr    *ExtentManager
		mClient metadata.TChanMetadataService
		logger  bark.Logger

		extents    extStatsContextMap
		reportPool sync.Pool
		reportC    chan *report

		wg    sync.WaitGroup
		stopC chan struct{} // internal channel to stop
	}

	extStatsContextMap struct {
		sync.RWMutex
		xmap map[string]*extStatsContext
	}

	extStatsContext struct {
		ext        *extentContext
		ref        int64
		lastReport *report
	}

	report struct {
		ctx       *extStatsContext
		extentID  uuid.UUID
		timestamp int64

		firstSeqNum, lastSeqNum       int64
		firstAddress, lastAddress     int64
		firstTimestamp, lastTimestamp int64
		sealed                        bool
	}
)

// the following globals and their methods are
var (
	reportInterval         atomic.Value
	extStatsReporterPaused int32
)

func init() {
	reportInterval.Store(defaultReportInterval)
}

// ExtStatsReporterPause pauses the reporting (intended for tests)
func ExtStatsReporterPause() {

	swapped := atomic.CompareAndSwapInt32(&extStatsReporterPaused, 0, 1)
	common.GetDefaultLogger().WithField(`was-running`, swapped).Info("extStatsReporter: paused")
}

// ExtStatsReporterResume resumes the reporting (intended for tests)
func ExtStatsReporterResume() {

	swapped := atomic.CompareAndSwapInt32(&extStatsReporterPaused, 1, 0)
	common.GetDefaultLogger().WithField(`was-running`, !swapped).Info("extStatsReporter: resumed")
}

// ExtStatsReporterSetReportInterval updates the report interval (intended for tests)
func ExtStatsReporterSetReportInterval(interval time.Duration) {

	reportInterval.Store(interval)
	common.GetDefaultLogger().WithField(`interval`, interval).Info("extStatsReporter: updated report interval")
}

func (t *report) String() string {
	return fmt.Sprintf("{extent=%v first{seq=%d addr=%x ts=%d} last{seq=%d addr=%x ts=%d}",
		t.extentID, t.firstSeqNum, t.firstAddress, t.firstTimestamp, t.lastSeqNum, t.lastAddress, t.lastTimestamp)
}

// NewExtStatsReporter is the constructor for ExtStatsReporter
func NewExtStatsReporter(hostID string, xMgr *ExtentManager, mClient metadata.TChanMetadataService, logger bark.Logger) *ExtStatsReporter {

	return &ExtStatsReporter{
		hostID:  hostID,
		xMgr:    xMgr,
		mClient: mClient,
		logger:  logger,

		extents: extStatsContextMap{
			xmap: make(map[string]*extStatsContext),
		},

		reportPool: sync.Pool{New: func() interface{} { return &report{} }},
		reportC:    make(chan *report, reportChanBufLen),
		stopC:      make(chan struct{}),
	}
}

// Start registers callbacks, and kicks off reporter pump and scheduler pump
func (t *ExtStatsReporter) Start() {

	t.logger.Info("extStatsReporter: started")

	// register callbacks to get notified every time an extent is opened/closed
	t.xMgr.RegisterCallbacks(t)

	t.wg.Add(1)
	go t.reporterPump()

	t.wg.Add(1)
	go t.schedulerPump()
}

// Stop stops periodic pumps for ExtStatsReporter
func (t *ExtStatsReporter) Stop() {

	close(t.stopC)
	t.wg.Wait()

	t.logger.Info("extStatsReporter: stopped")
}

func (t *ExtStatsReporter) trySendReport(extentID uuid.UUID, ext *extentContext, ctx *extStatsContext) bool {

	// if paused, do nothing
	if atomic.LoadInt32(&extStatsReporterPaused) == 1 {
		return false
	}

	r := t.reportPool.Get().(*report)

	r.ctx = ctx
	r.extentID = extentID
	r.timestamp = time.Now().UnixNano()

	// get a snapshot of various extent stats
	r.firstAddress, r.firstSeqNum, r.firstTimestamp = ext.getFirstMsg()
	r.lastAddress, r.lastSeqNum, r.lastTimestamp = ext.getLastMsg()
	r.sealed, _ = ext.isSealed()

	// create and send report non-blockingly
	select {
	case t.reportC <- r:

		return true // sent

	default:
		// drop the report, if we are running behind
		if ctx.lastReport != nil {
			t.reportPool.Put(ctx.lastReport) // return old 'lastReport' to pool
		}
		ctx.lastReport = r // update lastReport to this one

		// t.logger.WithFields(bark.Fields{ // #perfdisable
		// 	common.TagExt:  extentID,      // #perfdisable
		// 	`first-seqnum`: r.firstSeqNum, // #perfdisable
		// 	`last-seqnum`:  r.lastSeqNum,  // #perfdisable
		// }).Info("extStatsReporter: report dropped") // #perfdisable

		return false // not sent
	}
}

func (t *ExtStatsReporter) schedulerPump() {

	defer t.wg.Done()

	interval := reportInterval.Load().(time.Duration)
	ticker := time.NewTicker(interval)

	t.logger.WithFields(bark.Fields{
		`report-interval`: interval,
	}).Info("extStatsReporter: schedulerPump started")

pump:
	for {
		select {
		case <-ticker.C:

			// list of extents that can be deleted
			deleteExtents := make([]string, 8)

			// get lock shared, while iterating through map
			t.extents.RLock()

			// pause between extents to spread out the calls
			pause := time.Duration(interval.Nanoseconds() / int64(1+len(t.extents.xmap)))

			// iterate through the extents and prepare/send report
			for id, ctx := range t.extents.xmap {

				t.extents.RUnlock()

				if atomic.LoadInt64(&ctx.ref) > 0 {

					ext := ctx.ext

					// trySendReport will be accessing 'ext' members,
					// some of which require holding 'ext' shared.
					ext.RLock()
					t.trySendReport(uuid.UUID(id), ext, ctx)
					ext.RUnlock()

				} else {

					// collect extents to delete; and delete them in
					// one go with the lock held exclusive.
					deleteExtents = append(deleteExtents, id)
				}

				time.Sleep(pause) // take a short nap

				t.extents.RLock()
			}

			t.extents.RUnlock()

			// get lock exclusive, and delete extents from the map whose ref has dropped to zero
			t.extents.Lock()
			for _, id := range deleteExtents {
				if ctx, ok := t.extents.xmap[string(id)]; ok && atomic.LoadInt64(&ctx.ref) == 0 {
					delete(t.extents.xmap, id)
				}
			}
			t.extents.Unlock()

		case <-t.stopC:
			t.logger.Info("extStatsReporter: schedulerPump stopped")
			break pump
		}

		if newInterval := reportInterval.Load().(time.Duration); interval != newInterval {
			ticker.Stop()
			interval = newInterval
			ticker = time.NewTicker(interval)

			t.logger.WithFields(bark.Fields{
				`report-interval`: interval,
			}).Info("extStatsReporter: schedulerPump: report-interval changed")
		}
	}
}

func (t *ExtStatsReporter) reporterPump() {

	defer t.wg.Done()

	t.logger.Info("extStatsReporter: reporterPump started")

pump:
	for {
		select {
		case report := <-t.reportC:

			var extentID, lastReport = report.extentID, report.ctx.lastReport

			var lastSeqRate float64
			if lastReport != nil {
				lastSeqRate = common.CalculateRate(
					common.SequenceNumber(lastReport.lastSeqNum),
					common.SequenceNumber(report.lastSeqNum),
					common.UnixNanoTime(lastReport.timestamp),
					common.UnixNanoTime(report.timestamp),
				)
			}

			var extReplStatus = shared.ExtentReplicaStatus_OPEN
			if report.sealed {
				extReplStatus = shared.ExtentReplicaStatus_SEALED
			}

			extReplStats := &shared.ExtentReplicaStats{
				StoreUUID:             common.StringPtr(t.hostID),
				ExtentUUID:            common.StringPtr(extentID.String()),
				BeginAddress:          common.Int64Ptr(report.firstAddress),
				LastAddress:           common.Int64Ptr(report.lastAddress),
				AvailableAddress:      common.Int64Ptr(report.lastAddress),
				BeginSequence:         common.Int64Ptr(report.firstSeqNum),
				LastSequence:          common.Int64Ptr(report.lastSeqNum),
				LastSequenceRate:      common.Float64Ptr(lastSeqRate),
				AvailableSequence:     common.Int64Ptr(report.lastSeqNum),
				AvailableSequenceRate: common.Float64Ptr(lastSeqRate),
				BeginEnqueueTimeUtc:   common.Int64Ptr(report.firstTimestamp),
				LastEnqueueTimeUtc:    common.Int64Ptr(report.lastTimestamp),
				Status:                shared.ExtentReplicaStatusPtr(extReplStatus),
				// BeginTime:             common.Int64Ptr(report.firstTimestamp), // only for timer-queue
				// EndTime:               common.Int64Ptr(report.lastTimestamp),
				// SizeInBytes: common.Int64Ptr(report.size),
				// SizeInBytesRate
				// WriteTime
			}

			req := &metadata.UpdateStoreExtentReplicaStatsRequest{
				ExtentUUID:   common.StringPtr(extentID.String()),
				ReplicaStats: []*shared.ExtentReplicaStats{extReplStats},
			}

			err := t.mClient.UpdateStoreExtentReplicaStats(nil, req)

			if err != nil {
				t.logger.WithFields(bark.Fields{
					common.TagExt:  extentID,
					common.TagStor: t.hostID,
				}).Error(`UpdateStoreExtentReplicaStats failed`)
			}

			t.logger.WithFields(bark.Fields{ // #perfdisable
				common.TagExt:    extentID,                                // #perfdisable
				`first-seq`:      extReplStats.GetBeginSequence(),         // #perfdisable
				`last-seq`:       extReplStats.GetLastSequence(),          // #perfdisable
				`last-seq-rate`:  extReplStats.GetLastSequenceRate(),      // #perfdisable
				`avail-seq`:      extReplStats.GetAvailableSequence(),     // #perfdisable
				`avail-seq-rate`: extReplStats.GetAvailableSequenceRate(), // #perfdisable
			}).Info("extStatsReporter: report") // #perfdisable

			report.ctx.lastReport = report // update last-report

			if lastReport != nil {
				t.reportPool.Put(lastReport) // return old one to pool
			}

		case <-t.stopC:
			t.logger.Info("extStatsReporter: reporterPump stopped")
			break pump
		}
	}
}

// ignoreExtentEvent is called by the ExtentOpen and ExtentClose callbacks to
// check to see if the particular open/close extent can be ignored; we ignore
// all extent open calls that will not mutate the extent.
func ignoreExtentEvent(intent OpenIntent) bool {

	return intent != OpenIntentAppendStream &&
		intent != OpenIntentSealExtent &&
		intent != OpenIntentPurgeMessages &&
		intent != OpenIntentReplicateExtent
}

// ExtentInit is the callback from extent-manager when a new extent is initialized
func (t *ExtStatsReporter) ExtentInit(id uuid.UUID, ext *extentContext) {
	return // no-op
}

// ExtentOpen is the callback from extent-manager when an extent is opened/referenced
func (t *ExtStatsReporter) ExtentOpen(id uuid.UUID, ext *extentContext, intent OpenIntent) {

	if ignoreExtentEvent(intent) {

		// t.logger.WithFields(bark.Fields{ // #perfdisable
		// 	common.TagExt: id,     // #perfdisable
		// 	`intent`:      intent, // #perfdisable
		// }).Info("extStatsReporter: ignoring extent opened event") // #perfdisable

		return // ignore, if not any of the interesting 'intents'
	}

	// get exclusive lock, since we could be adding to the map
	t.extents.Lock()
	defer t.extents.Unlock()

	ctx, ok := t.extents.xmap[string(id)]

	if ok && ctx.ext == ext {

		atomic.AddInt64(&ctx.ref, 1) // add ref

	} else {

		// assert(ctx.ref == 0) //

		// create a new context
		ctx = &extStatsContext{ext: ext, ref: 1}
		t.extents.xmap[string(id)] = ctx
	}

	// t.logger.WithFields(bark.Fields{ // #perfdisable
	// 	common.TagExt: id,                         // #perfdisable
	// 	`intent`:      intent,                     // #perfdisable
	// 	`ctx.ref`:     atomic.LoadInt64(&ctx.ref), // #perfdisable
	// }).Info("extStatsReporter: extent opened") // #perfdisable

	return
}

// ExtentClose is the callback from extent-manager when an extent is closed/dereferenced
func (t *ExtStatsReporter) ExtentClose(id uuid.UUID, ext *extentContext, intent OpenIntent) (done bool) {

	if ignoreExtentEvent(intent) {

		// t.logger.WithFields(bark.Fields{ // #perfdisable
		// 	common.TagExt: id,     // #perfdisable
		// 	`intent`:      intent, // #perfdisable
		// }).Info("extStatsReporter: ignoring extent closed event") // #perfdisable

		return true // ignore, if not any of the interesting 'intents'
	}

	// get lock shared, when reading through the map
	t.extents.RLock()
	defer t.extents.RUnlock()

	ctx, ok := t.extents.xmap[string(id)]

	if ok && ctx.ext == ext {

		// remove ref, if it drops to zero, then send out one last report
		if atomic.AddInt64(&ctx.ref, -1) == 0 {
			t.trySendReport(id, ext, ctx)
		}

		// t.logger.WithFields(bark.Fields{ // #perfdisable
		// 	common.TagExt: id,                         // #perfdisable
		// 	`intent`:      intent,                     // #perfdisable
		// 	`ctx.ref`:     atomic.LoadInt64(&ctx.ref), // #perfdisable
		// }).Info("extStatsReporter: extent closed") // #perfdisable

	} else {

		// assert(ok) //
		t.logger.WithFields(bark.Fields{
			common.TagExt: id,
			`intent`:      intent,
			`old-ext`:     ctx.ext,
			`new-ext`:     ext,
		}).Error("extStatsReporter: extent-context changed unexpectedly")
	}

	return true // go ahead with the cleanup
}

// ExtentCleanUp is the callback from extent-manager when an extent is cleaned-up/torn down
func (t *ExtStatsReporter) ExtentCleanUp(id uuid.UUID, ext *extentContext) bool {
	return true // no-op; go ahead with cleanup
}
