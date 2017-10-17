package main

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/gocql/gocql"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/urfave/cli"
)

// TODO:
// - sort extents appropriately for "consuming", "unconsumed" and "consuming" extents -- by 'created-time', 'status', name, etc.
// - refresh rate throttle
// - look at extents for DLQ destination for CG
// - show total consume/publish rates
// - show special status if possibly 'stuck'

func monitor(c *cli.Context, mc *MetadataClient) error {

	if c.NArg() < 2 {
		fmt.Printf("dest-uuid and cg-uuid not specified\n")
		return nil
	}

	destUUID := c.Args()[0]
	cgUUID := c.Args()[1]

	ticker := time.NewTicker(time.Second) // don't query more than once every second // TODO: make configurable

	// print("\033[H\033[2J") // clear screen

	cgMon := newCGMonitor(mc, destUUID, cgUUID)

	output, _, _ := cgMon.refresh()

	print("\033[H\033[2J") // clear screen and move cursor to (0,0)
	print(output)

	for range ticker.C {

		output, _, _ := cgMon.refresh()

		// print("\033[H\033[2J") // clear screen and move cursor to (0,0)
		print("\033[H") // move cursor to (0,0)
		print(output)
		fmt.Printf(" publish: %.1f msgs/sec [%d msgs]    \n", cgMon.ratePublish, cgMon.deltaPublish)
		fmt.Printf(" replicate: %.1f msgs/sec [%d msgs]   \n", cgMon.rateReplicate, cgMon.deltaPublish)
		fmt.Printf(" consume: %.1f msgs/sec [%d msgs]    \n", cgMon.rateConsume, cgMon.deltaConsume)
		fmt.Printf(" backlog: %d    \n", cgMon.totalBacklog)
	}

	return nil
}

type extStatus int

const (
	extOpen extStatus = iota
	extSealed
	extConsumed
	extDeleted
	extMissing
	extError
)

func (t extStatus) String() string {

	switch t {
	case extOpen:
		return "open"
	case extSealed:
		return "sealed"
	case extConsumed:
		return "consumed"
	case extDeleted:
		return "deleted"
	case extMissing:
		return "MISSING"
	case extError:
		return "ERROR"
	}

	return "unknown"
}

type cgxStatus int

const (
	cgxUnconsumed cgxStatus = iota
	cgxConsuming
	cgxConsumed
	cgxError
)

func (t cgxStatus) String() string {

	switch t {
	case cgxUnconsumed:
		return "unconsumed"
	case cgxConsuming:
		return "consuming"
	case cgxConsumed:
		return "consumed"
	case cgxError:
		return "ERROR"
	}

	return "unknown"
}

type extentInfo struct {
	uuid            string
	remote          bool
	createdµs       int64
	extStatus       extStatus
	statusUpdatedµs int64
	cgxStatus       cgxStatus
	dlq             bool

	ackSeq          int64
	ackSeqUpdatedµs int64
	ackSeqDelta     int64
	ackSeqRate      float32

	readSeq          int64
	readSeqUpdatedµs int64
	readSeqDelta     int64

	beginSeq          int64
	beginSeqUpdatedµs int64
	beginSeqDelta     int64

	lastSeq          int64
	lastSeqUpdatedµs int64
	lastSeqDelta     int64
	lastSeqRate      float32

	backlog int64

	outputUUID string
	storeUUID  string
}

// units in micro-seconds
const (
	Secondµs int64 = 10e6
	Minuteµs int64 = 60 * Secondµs
	Hourµs   int64 = 60 * Minuteµs
	Dayµs    int64 = 24 * Hourµs
)

func timeSince(tMicros int64) string {

	d := time.Now().UnixNano()/1000 - tMicros

	switch {
	case d > Dayµs:
		return fmt.Sprintf("%02dd", d/Dayµs)

	case d > Hourµs:
		return fmt.Sprintf("%02dh", d/Hourµs)

	case d > Minuteµs:
		return fmt.Sprintf("%02dm", d/Minuteµs)

	// case d > Secondµs:
	// 	fallthrough

	default:
		return fmt.Sprintf("%02ds", d/Secondµs)
	}
}

type cgMonitor struct {
	mc *MetadataClient

	cgUUID   string
	cgName   string
	destUUID string
	destPath string

	extentMap      map[string]*extentInfo
	deletedExtents int

	deltaPublish, deltaReplicate, deltaConsume int64
	ratePublish, rateReplicate, rateConsume    float32
	totalBacklog                               int64
}

func newCGMonitor(mc *MetadataClient, dest, cg string) *cgMonitor {

	return &cgMonitor{
		mc:        mc,
		destUUID:  dest, // TODO: check if dest/cg are UUIDs or paths
		cgUUID:    cg,
		extentMap: make(map[string]*extentInfo),
	}
}

func (t *cgMonitor) iter(cql string) (iter *gocql.Iter, close func() error) {

	iter = t.mc.session.Query(cql).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: t.mc.retries}).Iter()

	close = func() (err error) {
		if err = iter.Close(); err != nil {
			// FIXME: handle error!
			fmt.Printf("ERROR from query '%v': %v\n", cql, err)
		}
		return
	}

	return
}

func (t *cgMonitor) scan(cql string, vals ...interface{}) error {
	return t.mc.session.Query(cql).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: t.mc.retries}).Scan(vals...)
}

const remoteExtentInputHostUUID = "88888888-8888-8888-8888-888888888888"

func (t *cgMonitor) refreshMetadata() error {

	{
		cql := "SELECT extent_uuid, consumer_group_visibility, created_time, status, status_updated_time, extent.input_host_uuid " +
			"FROM destination_extents WHERE destination_uuid=" + t.destUUID

		iter, close := t.iter(cql)

		var extentUUID string
		var createdMs int64
		var extStatus shared.ExtentStatus
		var statusUpdatedMs int64
		var cgVisibility string
		var inputUUID string

		t.deletedExtents = 0

		for iter.Scan(&extentUUID, &cgVisibility, &createdMs, &extStatus, &statusUpdatedMs, &inputUUID) {

			if cgVisibility == "" || cgVisibility == t.cgUUID {

				x, ok := t.extentMap[extentUUID]

				if !ok {

					if extStatus == shared.ExtentStatus_DELETED {
						t.deletedExtents++
						continue
					}

					x = &extentInfo{
						uuid:            extentUUID,
						createdµs:       createdMs * 1000,
						statusUpdatedµs: statusUpdatedMs * 1000,
						cgxStatus:       cgxUnconsumed,
						remote:          inputUUID == remoteExtentInputHostUUID,
					}
				}

				switch extStatus {
				case shared.ExtentStatus_OPEN:
					x.extStatus = extOpen

				case shared.ExtentStatus_SEALED:
					x.extStatus = extSealed

				case shared.ExtentStatus_CONSUMED:
					x.extStatus = extConsumed
				}

				t.extentMap[extentUUID] = x

				// if x.extStatus == extOpen {
				// 	fmt.Printf("%v [%d]: %v [%d]\n", extentUUID, createdMs, extStatus, statusUpdatedMs)
				// }
			}
		}

		close()
	}

	{
		cql := "SELECT extent_uuid, status, ack_level_sequence, WRITETIME(ack_level_sequence), received_level_sequence, " +
			"WRITETIME(received_level_sequence), connected_store, output_host_uuid " +
			"FROM consumer_group_extents WHERE consumer_group_uuid=" + t.cgUUID

		iter, close := t.iter(cql)

		var extentUUID, storeUUID, outputUUID string
		var status shared.ConsumerGroupExtentStatus
		var ackSeq, readSeq int64
		var ackSeqUpdatedµs, readSeqUpdatedµs int64

		for iter.Scan(&extentUUID, &status, &ackSeq, &ackSeqUpdatedµs, &readSeq, &readSeqUpdatedµs, &storeUUID, &outputUUID) {

			x, ok := t.extentMap[extentUUID]

			if ok {

				switch status {
				case shared.ConsumerGroupExtentStatus_OPEN:

					if x.extStatus == extConsumed {
						x.cgxStatus = cgxError // metadata inconsistency!
						x.extStatus = extError
					} else {
						x.cgxStatus = cgxConsuming
					}

				case shared.ConsumerGroupExtentStatus_DELETED:
					fallthrough

				case shared.ConsumerGroupExtentStatus_CONSUMED:
					x.cgxStatus = cgxConsumed
					// x.extStatus = extSealed
				}

			} else {

				switch status {
				case shared.ConsumerGroupExtentStatus_OPEN:

					x = &extentInfo{
						uuid:      extentUUID,
						extStatus: extMissing,
						cgxStatus: cgxConsuming,
					}

				case shared.ConsumerGroupExtentStatus_CONSUMED:

					x = &extentInfo{
						uuid:      extentUUID,
						extStatus: extMissing,
						cgxStatus: cgxConsumed,
					}

				case shared.ConsumerGroupExtentStatus_DELETED:
					continue // ignore, if deleted from extent and cg-extent
				}

				t.extentMap[extentUUID] = x
			}

			if ackSeqUpdatedµs > x.ackSeqUpdatedµs {

				if x.ackSeqUpdatedµs > 0 {

					x.ackSeqDelta = ackSeq - x.ackSeq
					x.ackSeqRate = float32(x.ackSeqDelta) * 1e6 / float32(ackSeqUpdatedµs-x.ackSeqUpdatedµs)
				}

				x.ackSeq, x.ackSeqUpdatedµs = ackSeq, ackSeqUpdatedµs
			}

			if readSeqUpdatedµs > x.readSeqUpdatedµs {

				if x.readSeqUpdatedµs > 0 {
					x.readSeqDelta = readSeq - x.readSeq
				}

				x.readSeq, x.readSeqUpdatedµs = readSeq, readSeqUpdatedµs
			}

			x.outputUUID, x.storeUUID = outputUUID, storeUUID
			// fmt.Printf("%v: ack[%v]\n", x.uuid, x.ackSeqUpdatedµs)

			// backlog < 0 typically happens if the last-seq was updated before the ack-seq
			if x.backlog = x.lastSeq - x.ackSeq; x.backlog < 0 && x.lastSeqUpdatedµs < x.ackSeqUpdatedµs {
				x.backlog = 0
			}
		}

		close()
	}

	{
		for _, x := range t.extentMap {

			var cql string
			var beginSeq, lastSeq int64
			var beginSeqUpdatedµs, lastSeqUpdatedµs int64

			beginSeq, lastSeq = -1, -1

			if x.cgxStatus == cgxConsuming && x.storeUUID != "" {

				// if the CG is consuming the extent from a particular replica, then query metadata for that

				// if extent is sealed, then don't query store_extents unless necessary
				if x.extStatus == extOpen || x.lastSeqUpdatedµs < x.statusUpdatedµs {

					cql = "SELECT replica_stats.begin_sequence, replica_stats.last_sequence, WRITETIME(replica_stats) " +
						"FROM store_extents WHERE store_uuid=" + x.storeUUID + " AND extent_uuid=" + x.uuid

					err := t.scan(cql, &beginSeq, &lastSeq, &beginSeqUpdatedµs)

					if err != nil {
						// FIXME: handle error
						fmt.Printf("ERROR from query '%v': %v\n", cql, err)
						continue
					}

					if beginSeq == math.MaxInt64 {
						beginSeq = 0
					}

					if lastSeq == math.MaxInt64 {
						lastSeq = 0
					}

					lastSeqUpdatedµs = beginSeqUpdatedµs
				}

			} else {

				// no specific replica -> use the data from all three replicas

				// if extent is sealed, then don't query store_extents unless necessary
				if x.extStatus == extOpen || x.lastSeqUpdatedµs < x.statusUpdatedµs {

					cql = "SELECT replica_stats.begin_sequence, replica_stats.last_sequence, WRITETIME(replica_stats) " +
						"FROM store_extents WHERE extent_uuid=" + x.uuid + " ALLOW FILTERING"

					iter, close := t.iter(cql)

					var tBeginSeq, tLastSeq int64
					var tSeqUpdatedµs int64

					for iter.Scan(&tBeginSeq, &tLastSeq, &tSeqUpdatedµs) {

						if tBeginSeq == math.MaxInt64 {
							tBeginSeq = 0
						}

						if tLastSeq == math.MaxInt64 {
							tLastSeq = 0
						}

						if beginSeq == -1 || beginSeq < tBeginSeq {
							beginSeq = tBeginSeq
							beginSeqUpdatedµs = tSeqUpdatedµs
						}

						if lastSeq == -1 || lastSeq < tLastSeq {
							lastSeq = tLastSeq
							lastSeqUpdatedµs = tSeqUpdatedµs
						}
					}

					if err := close(); err != nil {
						continue
					}
				}
			}

			if beginSeqUpdatedµs > x.beginSeqUpdatedµs {

				if x.beginSeqUpdatedµs > 0 {
					x.beginSeqDelta = beginSeq - x.beginSeq
				}

				x.beginSeq, x.beginSeqUpdatedµs = beginSeq, beginSeqUpdatedµs
			}

			if lastSeqUpdatedµs > x.lastSeqUpdatedµs {

				if x.lastSeqUpdatedµs > 0 {

					x.lastSeqDelta = lastSeq - x.lastSeq
					x.lastSeqRate = float32(x.lastSeqDelta) * 1e6 / float32(lastSeqUpdatedµs-x.lastSeqUpdatedµs)
				}

				x.lastSeq, x.lastSeqUpdatedµs = lastSeq, lastSeqUpdatedµs

			} else {

				if x.lastSeqUpdatedµs > 0 {

					now := time.Now().UTC().UnixNano() / 1000
					x.lastSeqRate = float32(x.lastSeqDelta) * 1e6 / float32(now-x.lastSeqUpdatedµs)
				}
			}
		}
	}

	return nil
}

func (t *cgMonitor) refresh() (output string, maxRows int, maxCols int) {

	t.refreshMetadata() // FIXME: check errors

	var consuming sortedExtents
	var consumed sortedExtents
	var unconsumed sortedExtents
	var others sortedExtents

	t.ratePublish, t.rateReplicate, t.rateConsume = 0.0, 0.0, 0.0
	t.totalBacklog = 0

	t.deltaPublish, t.deltaConsume = 0, 0

	for _, x := range t.extentMap {

		if x.extStatus == extOpen || x.lastSeqUpdatedµs < x.statusUpdatedµs {

			if !x.remote {
				t.ratePublish += x.lastSeqRate
				t.deltaPublish += x.lastSeqDelta
			} else {
				t.rateReplicate += x.lastSeqRate
				t.deltaReplicate += x.lastSeqDelta
			}
		}

		switch x.cgxStatus {
		case cgxConsuming:
			consuming = append(consuming, x)
			t.deltaConsume += x.ackSeqDelta
			t.rateConsume += x.ackSeqRate
			t.totalBacklog += x.backlog

		case cgxUnconsumed:
			unconsumed = append(unconsumed, x)
			t.totalBacklog += x.backlog

		case cgxConsumed:
			consumed = append(consumed, x)

		default:
			others = append(others, x)
		}
	}

	// sort extents
	sort.Sort(consuming)
	sort.Sort(unconsumed)
	sort.Sort(consumed)
	sort.Sort(others)

	var out = new(bytes.Buffer)

	fmt.Fprintf(out, "=============================================================================================================================================\n")
	fmt.Fprintf(out, " %44s | %14s | %14s | %14s | %8s | %8s | %8s | %8s\n", "extent", "status", "msgs", "ack", "backlog", "read", "output", "store")
	// TODO: show extent-created time; last-ack updated time; local/remote extent; output/store hostname

	// fmt.Fprintf(out, "----------------------------------------|----------|----------|----------|----------|----------|--------------------------------------|-------------------------------------\n")
	fmt.Fprintf(out, "--consuming-----------------------------------+----------------+----------------+----------------+----------+----------+----------+----------\n")

	for _, x := range consuming {

		if x.cgxStatus != cgxConsuming {
			continue
		}

		var remote rune
		if x.remote {
			remote = 'R'
		} else {
			remote = ' '
		}

		fmt.Fprintf(out, " %36s [%3s] %c | %8s [%3s] | %8d [%3s] | %8d [%3s] | %8d | %8d | %8s | %8s\n", x.uuid, timeSince(x.createdµs), remote, x.extStatus, timeSince(x.statusUpdatedµs),
			x.lastSeq, timeSince(x.lastSeqUpdatedµs), x.ackSeq, timeSince(x.ackSeqUpdatedµs), x.backlog, x.readSeq, trunc(x.outputUUID), trunc(x.storeUUID))
	}

	fmt.Fprintf(out, "--unconsumed----------------------------------+----------------+----------------+----------------+----------+----------+----------+----------\n")

	for _, x := range unconsumed {

		if x.cgxStatus != cgxUnconsumed {
			continue
		}

		var remote rune
		if x.remote {
			remote = 'R'
		} else {
			remote = ' '
		}

		fmt.Fprintf(out, " %36s [%3s] %c | %8s [%3s] | %8d [%3s] |               | %8d |     | %8s | %8s\n", x.uuid, timeSince(x.createdµs), remote, x.extStatus, timeSince(x.statusUpdatedµs),
			x.lastSeq, timeSince(x.lastSeqUpdatedµs), x.lastSeq, trunc(x.outputUUID), trunc(x.storeUUID))

		t.totalBacklog += x.backlog
	}

	fmt.Fprintf(out, "--consumed------------------------------------|----------------|----------------|----------------|----------|----------|----------|----------\n")

	var num int
	for _, x := range consumed {

		if x.cgxStatus != cgxConsumed {
			continue
		}

		if num++; num < 20 { // TODO: make configurable

			var remote rune
			if x.remote {
				remote = 'R'
			} else {
				remote = ' '
			}

			// fmt.Fprintf(out, " %36s | %8s | %8d | %8d | %8d | %8d | %36s | %36s\n", x.uuid, x.extStatus,
			// 	x.lastSeq, x.ackSeq, x.lastSeq-x.ackSeq, x.readSeq, x.outputUUID, x.storeUUID)
			fmt.Fprintf(out, " %36s [%3s] %c | %8s [%3s] | %8d [%3s] | %8d [%3s] |          | %8d | %8s | %8s\n", x.uuid, timeSince(x.createdµs), remote,
				x.extStatus, timeSince(x.statusUpdatedµs), x.lastSeq, timeSince(x.lastSeqUpdatedµs), x.ackSeq, timeSince(x.ackSeqUpdatedµs), x.readSeq,
				trunc(x.outputUUID), trunc(x.storeUUID))
		}
	}

	if num >= 20 {
		fmt.Fprintf(out, " %44s | %14s | %14s | %14s | %8s | %8s | %8s | %8s\n", fmt.Sprintf("... %d others .. ", num-20), "", "", "", "", "", "", "")
	}

	if len(others) > 0 {
		fmt.Fprintf(out, "--others--------------------------------------|----------------|----------------|----------------|----------|----------|----------|----------\n")

		var num int
		for _, x := range consumed {

			if x.cgxStatus != cgxConsumed {
				continue
			}

			if num++; num < 20 { // TODO: make configurable

				var remote rune
				if x.remote {
					remote = 'R'
				} else {
					remote = ' '
				}

				// fmt.Fprintf(out, " %36s | %8s | %8d | %8d | %8d | %8d | %36s | %36s\n", x.uuid, x.extStatus,
				// 	x.lastSeq, x.ackSeq, x.lastSeq-x.ackSeq, x.readSeq, x.outputUUID, x.storeUUID)
				fmt.Fprintf(out, " %36s [%3s] %c | %8s [%3s] | %8d [%3s] | %8d [%3s] |          | %8d | %8s | %8s\n", x.uuid, timeSince(x.createdµs), remote,
					x.extStatus, timeSince(x.statusUpdatedµs), x.lastSeq, timeSince(x.lastSeqUpdatedµs), x.ackSeq, timeSince(x.ackSeqUpdatedµs),
					x.readSeq, trunc(x.outputUUID), trunc(x.storeUUID))
			}
		}

		if num >= 20 {
			fmt.Fprintf(out, " %44s | %14s | %14s | %14s | %8s | %8s | %8s | %8s\n", fmt.Sprintf("... %d others .. ", num-20), "", "", "", "", "", "", "")
		}
	}

	fmt.Fprintf(out, "=============================================================================================================================================\n")

	return out.String(), 0, 0
}

func trunc(uuid string) string {

	if len(uuid) >= 8 {
		return uuid[:8]
	}

	return uuid
}

type sortedExtents []*extentInfo

func (t sortedExtents) Less(i, j int) bool {

	// sort by:
	// 1. status
	// 2. status-updated time
	// 3. extent created-time
	// 4. extent-uuid

	if t[i].extStatus < t[j].extStatus {
		return true
	}

	if t[i].extStatus > t[j].extStatus {
		return false
	}

	if t[i].statusUpdatedµs > t[j].statusUpdatedµs {
		return true
	}

	if t[i].statusUpdatedµs < t[j].statusUpdatedµs {
		return false
	}

	if t[i].createdµs > t[j].createdµs {
		return true
	}

	if t[i].createdµs < t[j].createdµs {
		return false
	}

	// fallback to sorting by extent-uuid
	return t[i].uuid < t[j].uuid
}

func (t sortedExtents) Len() int {
	return len(t)
}

func (t sortedExtents) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}
