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
	"fmt"

	"github.com/uber-common/bark"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

type (
	dlqMonitor struct {
		*Context
		dlqMinMergeTimestamp common.UnixNanoTime
		destinationFlags
	}

	op uint8

	destinationFlags struct {
		dirty bool // Detects when an extent seal or move was performed during a sweep
		op
		operationTime common.UnixNanoTime
		dstID         string
	}
)

const (
	mergeOp = op(iota + 77)
	purgeOp

	purgedExtentUUID = `42000000-dead-0420-dead-000000000042`
)

func (o op) String() string {
	switch o {
	case mergeOp:
		return `merge`
	case purgeOp:
		return `purge`
	case 0:
		return `nil`
	default:
		return `INVALID` + fmt.Sprintf(`%d`, o)
	}
}

func newDlqMonitor(c *Context) *dlqMonitor {
	return &dlqMonitor{Context: c}
}

/*

DLQ Monitor Phases

Phase 0: Rest. Operation timestamp is really old or zero. Force it to zero. We have the notion of an 'old' timestamp
         So that ineffective operations don't surprise the user when/if they become effective later.

Phase 1: Extent Seals & Moves. We have an active operation timestamp. We scan all DLQ destination extents looking for
         extents created before that timestamp. If they are not sealed, we seal them. If they are sealed, we move them.

Phase 2: Reset & notify. We have an active operation timestamp. We scan as above. There are no extents that qualify for
         sealing or merging, so we send a reconfigure notification to the outputhosts for this consumer group. We reset
		 the timestamp to zero.

Since the interval between scans is one minute (see intervalBtwnScans), it will take on average about 2.5 minutes for a
DLQ operation to complete, assuming that controller is able to perform a scan once per minute.

*/

func (m *dlqMonitor) handleEvent(e *mIteratorEvent) {
	// Lazy evaluation of logger;
	ll := func() bark.Logger {
		l := m.Context.log.WithField(common.TagModule, `dlqMonitor`)
		if e.dest != nil {
			l = l.WithFields(bark.Fields{
				common.TagDstPth: common.FmtDstPth(e.dest.GetPath()),
				common.TagDst:    common.FmtDst(e.dest.GetDestinationUUID()),
			})
		}
		if e.extent != nil {
			l = l.WithFields(bark.Fields{
				common.TagExt: common.FmtExt(e.extent.GetExtentUUID()),
				common.TagDst: common.FmtDst(m.dstID),
			})
		}
		l = l.WithField(`event`, fmt.Sprintf(`%v`, e.t))
		return l
	}

	now := common.Now()

	switch e.t {
	case eIterStart:
		m.dlqMinMergeTimestamp = common.Now() - common.UnixNanoTime(common.DLQMaxMergeAge)
	case eDestStart:
		// Reset all destination flags to their original values
		m.destinationFlags = destinationFlags{}

		if !common.IsDLQDestination(e.dest) {
			//ll().Info(`Skipping non-DLQ dest`)
			return
		}

		mergeTime := common.UnixNanoTime(e.dest.GetDLQMergeBefore())
		purgeTime := common.UnixNanoTime(e.dest.GetDLQPurgeBefore())
		if mergeTime > 0 {
			m.op = mergeOp
			m.operationTime = mergeTime
		} else if purgeTime > 0 {
			m.op = purgeOp
			m.operationTime = purgeTime
		} else {
			//ll().Info(`Skipping destination, no operation pending`)
			return
		}

		ll().Info(`Processing DLQ destination operation`)

		//  If we aren't interested in extents for this destination,
		//  because outstanding merge operations are too old,† just return
		//
		//  † We assume that partitions will have healed within this time period,
		//    and also that it is long enough that someone might forget that they
		// 	  pushed merge on an inactive consumer group
		if m.operationTime < common.UnixNanoTime(m.dlqMinMergeTimestamp) {
			ll().WithFields(bark.Fields{
				`mergeTime`:            (now - common.UnixNanoTime(mergeTime)).ToSeconds(),
				`purgeTime`:            (now - common.UnixNanoTime(purgeTime)),
				`maxTime`:              m.operationTime,
				`dlqMinMergeTimestamp`: m.dlqMinMergeTimestamp,
			}).Info(`Resetting DLQ operation, timestamps are too old`)
			m.resetTimestamp(e, m.op, m.operationTime)
			m.destinationFlags = destinationFlags{}
			return
		}

		m.dstID = e.dest.GetDestinationUUID()
	case eExtent:
		if m.operationTime == 0 {
			return
		}

		// We only care about consumed, open or sealed extents; others we ignore
		switch e.extent.GetStatus() {
		case shared.ExtentStatus_CONSUMED:
			fallthrough
		case shared.ExtentStatus_OPEN:
			fallthrough
		case shared.ExtentStatus_SEALED:
			// continue
		default:
			//ll().WithField(`extentStatus`, e.extent.GetStatus()).Infof(`Skipping extent`)
			return
		}

		//ll().Info(`Processing extent`)
		createTime := common.UnixNanoTime(e.extent.GetCreatedTimeMillis() * 1000 * 1000)
		if createTime == 0 || createTime > common.Now() {
			ll().WithField(`createTime`, createTime).Error(`createTime should be valid`)
		}

		// Check for purged extents, which will have ConsumerGroupVisibility set to purgedExtentUUID
		if e.extent.GetConsumerGroupVisibility() != `` {
			return
		}

		if common.UnixNanoTime(createTime) < common.UnixNanoTime(m.operationTime) {
			m.destinationFlags.dirty = true
			switch m.op {
			case mergeOp:
				m.merge(e)
			case purgeOp:
				m.purge(e)
			default:
				panic(`bad op`)
			}
		} else {
			ll().WithFields(bark.Fields{
				`operationTime`: (now - m.operationTime).ToSecondsFmt(),
				`createTime`:    (now - createTime).ToSecondsFmt(),
			}).Info(`createTime is newer than operation time`)
		}
	case eDestEnd:
		if m.operationTime == 0 {
			return
		}

		ll().Info(`End of destination`)

		if !m.destinationFlags.dirty { // End of multiple iterations for this operation, no seals/merges/purges occurred on this latest pass
			ll().Info(`Notifying output hosts`)
			err := notifyOutputHostsForConsumerGroup(m.Context, e.dest.GetDestinationUUID(), e.dest.GetDLQConsumerGroupUUID(),
				notifyDLQMergedExtents, e.dest.GetDestinationUUID(), metrics.DLQOperationScope)
			if err != nil {
				ll().WithField(common.TagErr, err).Error(`Not able to notify output hosts`)
				return
			}
			m.resetTimestamp(e, m.op, m.operationTime)
			ll().Info(`End of operation`)
		}
	}
}

func (m *dlqMonitor) merge(e *mIteratorEvent) (err error) {
	defer func() {
		if err != nil {
			m.Context.log.WithFields(bark.Fields{
				common.TagExt:    common.FmtExt(e.extent.GetExtentUUID()),
				common.TagDst:    common.FmtDst(m.dstID),
				common.TagErr:    err,
				`op`:             fmt.Sprintf("%v", mergeOp),
				common.TagModule: `dlqMonitor`,
			}).Error(`Error merging extent during DLQ Operation`)
			m.Context.m3Client.IncCounter(metrics.DLQOperationScope, metrics.ControllerErrMetadataUpdateCounter)
		}
	}()

	if e.extent.GetStatus() == shared.ExtentStatus_OPEN {
		m.seal(e, mergeOp)
		return
	}

	ll := m.Context.log.WithFields(bark.Fields{
		common.TagExt:    common.FmtExt(e.extent.GetExtentUUID()),
		common.TagDst:    common.FmtDst(m.dstID),
		`op`:             fmt.Sprintf("%v", mergeOp),
		common.TagModule: `dlqMonitor`,
	})

	cnsm := e.dest.GetDLQConsumerGroupUUID()
	ext := e.extent.GetExtentUUID()

	if len(cnsm) == 0 {
		ll.Warn(`DLQ Destination metadata not pointing to CG; needs fixing`)
		return
	}

	// Presently, this is not being read. Based on the metadata structure,
	// it need never be read. We give the DLQ ID here so that a reasonable
	// destination UUID is logged, but the DLQ ID isn't the destination that
	// the consumer consumes from
	dst := e.dest.GetDestinationUUID()

	cgDesc, err := m.Context.mm.ReadConsumerGroup(dst, "", cnsm, "")
	if err != nil {
		return
	}

	newDst := cgDesc.GetDestinationUUID()

	ll.WithFields(bark.Fields{
		common.TagDst + `New`: common.FmtDst(newDst),
		common.TagCnsm:        common.FmtCnsm(cnsm),
	}).Info(`moving extent`)
	err = m.Context.mm.MoveExtent(dst, newDst, ext, cnsm)
	return
}

func (m *dlqMonitor) purge(e *mIteratorEvent) (err error) {
	defer func() {
		if err != nil {
			m.Context.log.WithFields(bark.Fields{
				common.TagExt:    common.FmtExt(e.extent.GetExtentUUID()),
				common.TagDst:    common.FmtDst(m.dstID),
				common.TagErr:    err,
				`op`:             fmt.Sprintf("%v", purgeOp),
				common.TagModule: `dlqMonitor`,
			}).Error(`Error purging extent during DLQ Operation`)
			m.Context.m3Client.IncCounter(metrics.DLQOperationScope, metrics.ControllerErrMetadataUpdateCounter)
		}
	}()

	if e.extent.GetStatus() == shared.ExtentStatus_OPEN {
		m.seal(e, mergeOp)
		return
	}

	ll := m.Context.log.WithFields(bark.Fields{
		common.TagExt:    common.FmtExt(e.extent.GetExtentUUID()),
		common.TagDst:    common.FmtDst(m.dstID),
		`op`:             fmt.Sprintf("%v", purgeOp),
		common.TagModule: `dlqMonitor`,
	})

	ext := e.extent.GetExtentUUID()
	dst := e.dest.GetDestinationUUID()

	ll.Info(`purging extent`)

	// What this does is set the consumer group visibility to an arbitrary value so that it is not visible to consumers of the DLQ destination
	// and so that it is conspicuously marked so that future merge operations will not move it
	err = m.Context.mm.MoveExtent(dst, dst, ext, purgedExtentUUID)

	// TODO: call the storehost's purgemessages API with ADDR_SEAL, which will instantly delete the extent. For now,
	// we can preserve the extent, which allows a manual undelete operation if the customer needs it.
	return
}

func (m *dlqMonitor) seal(e *mIteratorEvent, op op) {
	// no need to seal remote zone extent
	if common.IsRemoteZoneExtent(e.extent.GetOriginZone(), m.Context.localZone) {
		return
	}

	if e.extent.GetStatus() == shared.ExtentStatus_OPEN && !isExtentBeingSealed(m.Context, e.extent.GetExtentUUID()) {
		m.Context.log.WithFields(bark.Fields{
			common.TagExt:    common.FmtExt(e.extent.GetExtentUUID()),
			common.TagDst:    common.FmtDst(m.dstID),
			`op`:             fmt.Sprintf("%v", op),
			common.TagModule: `dlqMonitor`,
		}).Info(`Sealing DLQ extent for operation`)

		addExtentDownEvent(m.Context, 0, m.dstID, e.extent.GetExtentUUID())
	}
}

func (m *dlqMonitor) resetTimestamp(e *mIteratorEvent, op op, curTime common.UnixNanoTime) (err error) {
	defer func() {
		if err != nil {
			m.Context.log.WithFields(bark.Fields{
				common.TagErr:    err,
				`op`:             fmt.Sprintf("%v", mergeOp),
				common.TagModule: `dlqMonitor`,
			}).Error(`Error resetting timestamp during DLQ operation`)
			m.Context.m3Client.IncCounter(metrics.DLQOperationScope, metrics.ControllerErrMetadataUpdateCounter)
		}
	}()

	m.Context.log.WithFields(bark.Fields{
		`op`:             fmt.Sprintf("%v", mergeOp),
		common.TagModule: `dlqMonitor`,
	}).Info(`Resetting timestamp`)

	err = m.updateDLQCursor(e.dest.GetDestinationUUID(), op, curTime, 0)
	return
}

func (m *dlqMonitor) updateDLQCursor(dstID string, op op, curTime, newTime common.UnixNanoTime) (err error) {

	destDesc, err := m.Context.mm.ReadDestination(dstID, "")
	if err != nil {
		return
	}

	// If the current time shown in Cassandra is newer than what we are trying to set, make this a no-op
	// Otherwise, set the appropriate time in the request. Leaving the other one nil is fine.
	switch op {
	case mergeOp:
		if destDesc.GetDLQMergeBefore() <= int64(curTime) {
			return m.Context.mm.UpdateDestinationDLQCursors(dstID, newTime, -1)
		}
	case purgeOp:
		if destDesc.GetDLQPurgeBefore() <= int64(curTime) {
			return m.Context.mm.UpdateDestinationDLQCursors(dstID, -1, newTime)
		}
	default:
		panic(`bad op: ` + fmt.Sprintf(`%v`, op))
	}

	return nil
}
