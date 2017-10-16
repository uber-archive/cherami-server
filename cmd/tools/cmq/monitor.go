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
	createdMs       int64
	extStatus       extStatus
	statusUpdatedMs int64
	cgxStatus       cgxStatus
	dlq             bool

	ackSeq          int64
	ackSeqUpdatedMs int64
	ackSeqRate      float32

	readSeq          int64
	readSeqUpdatedMs int64
	readSeqRate      float32

	beginSeq          int64
	beginSeqUpdatedMs int64
	beginSeqRate      float32

	lastSeq          int64
	lastSeqUpdatedMs int64
	lastSeqRate      float32

	backlog     int64
	backlogRate float32

	outputUUID string
	storeUUID  string
}

const (
	Second int64 = 10e6
	Minute int64 = 60 * Second
	Hour   int64 = 60 * Minute
	Day    int64 = 24 * Hour
)

func timeSinceMsUTC(tMillis int64) string {

	return timeSince(tMillis * 1000)
}

func timeSince(tMicros int64) string {

	d := time.Now().UnixNano()/1000 - tMicros

	switch {
	case d > Day:
		return fmt.Sprintf("%02dd", d/Day)

	case d > Hour:
		return fmt.Sprintf("%02dh", d/Hour)

	case d > Minute:
		return fmt.Sprintf("%02dm", d/Minute)

	// case d > Second:
	// 	fallthrough

	default:
		return fmt.Sprintf("%02ds", d/Second)
	}
}

func monitor(c *cli.Context, mc *MetadataClient) error {

	if c.NArg() < 2 {
		fmt.Printf("dest-uuid and cg-uuid not specified\n")
		return nil
	}

	destUUID := c.Args()[0]
	cgUUID := c.Args()[1]

	iter := func(cql string) (iter *gocql.Iter, close func() error) {

		iter = mc.session.Query(cql).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: mc.retries}).Iter()

		close = func() (err error) {
			if err = iter.Close(); err != nil {
				// FIXME: handle error!
				fmt.Printf("ERROR from query '%v': %v\n", cql, err)
			}
			return
		}

		return
	}

	scan := func(cql string, vals ...interface{}) error {
		return mc.session.Query(cql).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: mc.retries}).Scan(vals...)
	}

	ticker := time.NewTicker(time.Second) // don't query more than once every second // TODO: make configurable

	print("\033[H\033[2J") // clear screen

	var extentMap = make(map[string]*extentInfo)

	for range ticker.C {

		var deletedExtents int

		{
			cql := "SELECT extent_uuid, consumer_group_visibility, created_time, status, status_updated_time FROM destination_extents WHERE destination_uuid=" + destUUID

			iter, close := iter(cql)

			var extentUUID string
			var createdMs int64
			var extStatus shared.ExtentStatus
			var statusUpdatedMs int64
			var cgVisibility string

			for iter.Scan(&extentUUID, &cgVisibility, &createdMs, &extStatus, &statusUpdatedMs) {

				if cgVisibility == "" || cgVisibility == cgUUID {

					x, ok := extentMap[extentUUID]

					if !ok {

						if extStatus == shared.ExtentStatus_DELETED {
							deletedExtents++
							continue
						}

						x = &extentInfo{
							uuid:            extentUUID,
							createdMs:       createdMs,
							statusUpdatedMs: statusUpdatedMs,
							cgxStatus:       cgxUnconsumed,
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

					extentMap[extentUUID] = x

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
				"FROM consumer_group_extents WHERE consumer_group_uuid=" + cgUUID

			iter, close := iter(cql)

			var extentUUID, storeUUID, outputUUID string
			var status shared.ConsumerGroupExtentStatus
			var ackSeq, readSeq int64
			var ackSeqUpdatedMs, readSeqUpdatedMs int64

			for iter.Scan(&extentUUID, &status, &ackSeq, &ackSeqUpdatedMs, &readSeq, &readSeqUpdatedMs, &storeUUID, &outputUUID) {

				x, ok := extentMap[extentUUID]

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

					extentMap[extentUUID] = x
				}

				if ackSeqUpdatedMs > x.ackSeqUpdatedMs {

					if x.ackSeqUpdatedMs != 0 {
						x.ackSeqRate = float32(ackSeq-x.ackSeq) * 1000.0 / float32(ackSeqUpdatedMs-x.ackSeqUpdatedMs)
					}

					x.ackSeq, x.ackSeqUpdatedMs = ackSeq, ackSeqUpdatedMs
				}

				if readSeqUpdatedMs > x.readSeqUpdatedMs {

					if x.readSeqUpdatedMs != 0 {
						x.readSeqRate = float32(readSeq-x.readSeq) * 1000.0 / float32(readSeqUpdatedMs-x.readSeqUpdatedMs)
					}

					x.readSeq, x.readSeqUpdatedMs = readSeq, readSeqUpdatedMs
				}

				x.outputUUID, x.storeUUID = outputUUID, storeUUID
				// fmt.Printf("%v: ack[%v]\n", x.uuid, x.ackSeqUpdatedMs)

				// backlog < 0 typically happens if the last-seq was updated before the ack-seq
				if x.backlog = x.lastSeq - x.ackSeq; x.backlog < 0 && x.lastSeqUpdatedMs < x.ackSeqUpdatedMs {
					x.backlog = 0
				}
			}

			close()
		}

		{
			for _, x := range extentMap {

				var cql string
				var beginSeq, lastSeq int64
				var beginSeqUpdatedMs, lastSeqUpdatedMs int64

				beginSeq, lastSeq = -1, -1

				if x.cgxStatus == cgxConsuming && x.storeUUID != "" {

					cql = "SELECT replica_stats.begin_sequence, replica_stats.last_sequence, WRITETIME(replica_stats) " +
						"FROM store_extents WHERE store_uuid=" + x.storeUUID + " AND extent_uuid=" + x.uuid

					err := scan(cql, &beginSeq, &lastSeq, &beginSeqUpdatedMs)

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

					lastSeqUpdatedMs = beginSeqUpdatedMs

				} else {

					cql = "SELECT replica_stats.begin_sequence, replica_stats.last_sequence, WRITETIME(replica_stats) " +
						"FROM store_extents WHERE extent_uuid=" + x.uuid + " ALLOW FILTERING"

					iter, close := iter(cql)

					var tBeginSeq, tLastSeq int64
					var tSeqUpdatedMs int64

					for iter.Scan(&tBeginSeq, &tLastSeq, &tSeqUpdatedMs) {

						if tBeginSeq == math.MaxInt64 {
							tBeginSeq = 0
						}

						if tLastSeq == math.MaxInt64 {
							tLastSeq = 0
						}

						if beginSeq == -1 || beginSeq < tBeginSeq {
							beginSeq = tBeginSeq
							beginSeqUpdatedMs = tSeqUpdatedMs
						}

						if lastSeq == -1 || lastSeq < tLastSeq {
							lastSeq = tLastSeq
							lastSeqUpdatedMs = tSeqUpdatedMs
						}
					}

					if err := close(); err != nil {
						continue
					}
				}

				if beginSeqUpdatedMs > x.beginSeqUpdatedMs {

					if x.beginSeqUpdatedMs != 0 {
						x.beginSeqRate = float32(beginSeq-x.beginSeq) * 1000.0 / float32(beginSeqUpdatedMs-x.beginSeqUpdatedMs)
					}

					x.beginSeq, x.beginSeqUpdatedMs = beginSeq, beginSeqUpdatedMs
				}

				if lastSeqUpdatedMs > x.lastSeqUpdatedMs {

					if x.lastSeqUpdatedMs != 0 {
						x.lastSeqRate = float32(lastSeq-x.lastSeq) * 1000.0 / float32(lastSeqUpdatedMs-x.lastSeqUpdatedMs)
					}

					x.lastSeq, x.lastSeqUpdatedMs = lastSeq, lastSeqUpdatedMs
				}
			}
		}

		var extents extentsByCreatedTime

		for _, x := range extentMap {
			extents = append(extents, x)
		}

		sort.Sort(extents) // FIXME: sort by extent-created time

		var out = new(bytes.Buffer)

		var rateConsume float32
		var ratePublish float32

		fmt.Fprintf(out, "-------------------------------------------------------------------------------------------------------------------------------------------\n")
		fmt.Fprintf(out, " %42s | %14s | %14s | %14s | %8s | %8s | %8s | %8s\n", "extent-uuid", "status", "msgs", "ack", "backlog", "read", "output", "store")
		// TODO: show extent-created time; last-ack updated time; local/remote extent; output/store hostname

		// fmt.Fprintf(out, "--------------------------------------|----------|----------|----------|----------|----------|--------------------------------------|-------------------------------------\n")
		fmt.Fprintf(out, "--consuming---------------------------------+----------------+----------------+----------------+----------+----------+----------+----------\n")

		for _, x := range extents {

			ratePublish += x.lastSeqRate
			rateConsume += x.ackSeqRate

			if x.cgxStatus != cgxConsuming {
				continue
			}

			fmt.Fprintf(out, " %36s [%3s] | %8s [%3s] | %8d [%3s] | %8d [%3s] | %8d | %8d | %8s | %8s\n", x.uuid, timeSinceMsUTC(x.createdMs), x.extStatus, timeSinceMsUTC(x.statusUpdatedMs),
				x.lastSeq, timeSince(x.lastSeqUpdatedMs), x.ackSeq, timeSince(x.ackSeqUpdatedMs), x.backlog, x.readSeq, shortenUUID(x.outputUUID), shortenUUID(x.storeUUID))
		}

		fmt.Fprintf(out, "--unconsumed--------------------------------+----------------+----------------+----------------+----------+----------+----------+----------\n")

		for _, x := range extents {

			if x.cgxStatus != cgxUnconsumed {
				continue
			}

			fmt.Fprintf(out, " %36s [%3s] | %8s [%3s] | %8d [%3s] |               | %8d |     | %8s | %8s\n", x.uuid, timeSinceMsUTC(x.createdMs), x.extStatus, timeSinceMsUTC(x.statusUpdatedMs),
				x.lastSeq, timeSince(x.lastSeqUpdatedMs), x.lastSeq, shortenUUID(x.outputUUID), shortenUUID(x.storeUUID))
		}

		fmt.Fprintf(out, "--consumed----------------------------------|----------------|----------------|----------------|----------|----------|----------|----------\n")

		var num int
		for _, x := range extents {

			if x.cgxStatus != cgxConsumed {
				continue
			}

			// fmt.Fprintf(out, " %36s | %8s | %8d | %8d | %8d | %8d | %36s | %36s\n", x.uuid, x.extStatus,
			// 	x.lastSeq, x.ackSeq, x.lastSeq-x.ackSeq, x.readSeq, x.outputUUID, x.storeUUID)
			fmt.Fprintf(out, " %36s [%3s] | %8s [%3s] | %8d [%3s] | %8d [%3s] | %8d | %8d | %8s | %8s\n", x.uuid, timeSinceMsUTC(x.createdMs), x.extStatus, timeSinceMsUTC(x.statusUpdatedMs),
				x.lastSeq, timeSince(x.lastSeqUpdatedMs), x.ackSeq, timeSince(x.ackSeqUpdatedMs), x.backlog, x.readSeq, shortenUUID(x.outputUUID), shortenUUID(x.storeUUID))

			if num++; num > 10 {
				fmt.Fprintf(out, "...\n")
				break
			}
		}

		fmt.Fprintf(out, "-------------------------------------------------------------------------------------------------------------------------------------------\n")

		print("\033[H\033[2J") // clear screen and move cursor to (0,0)
		// print("\033[H") // move cursor to (0,0)
		print(out.String()) // write new values
	}

	return nil
}

func shortenUUID(uuid string) string {

	if len(uuid) >= 8 {
		return uuid[:8]
	}

	return uuid
}

type extentsByCreatedTime []*extentInfo

func (t extentsByCreatedTime) Less(i, j int) bool {

	if t[i].createdMs > t[j].createdMs {
		return true
	}

	if t[i].createdMs < t[j].createdMs {
		return false
	}

	if t[i].extStatus < t[j].extStatus {
		return true
	}

	if t[i].extStatus > t[j].extStatus {
		return false
	}

	if t[i].statusUpdatedMs > t[j].statusUpdatedMs {
		return true
	}

	if t[i].statusUpdatedMs < t[j].statusUpdatedMs {
		return false
	}

	// fallback to sorting by extent-uuid
	return t[i].uuid < t[j].uuid
}

func (t extentsByCreatedTime) Len() int {
	return len(t)
}

func (t extentsByCreatedTime) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}
