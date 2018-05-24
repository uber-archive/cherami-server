package main

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/uber/cherami-server/common/set"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/urfave/cli"
)

func gc(c *cli.Context) error {

	var gcDestinations, gcConsumerGroups, gcDestExtents,
		gcStoreExtents, gcInputExtents, gcCGExtents bool

	if c.Bool("all") {
		gcDestinations = true
		gcConsumerGroups = true
		gcDestExtents = true
		gcStoreExtents = true
		gcInputExtents = true
		gcCGExtents = true
	}

	if c.Bool("destinations") {
		gcDestinations = true
	}

	if c.Bool("consumer_groups") {
		gcConsumerGroups = true
	}

	if c.Bool("destination_extents") {
		gcDestExtents = true
	}

	if c.Bool("store_extents") {
		gcStoreExtents = true
	}

	if c.Bool("input_host_extents") {
		gcInputExtents = true
	}

	if c.Bool("consumer_group_extents") {
		gcCGExtents = true
	}

	if !gcDestinations && !gcConsumerGroups && !gcDestExtents &&
		!gcStoreExtents && !gcInputExtents && !gcCGExtents {

		fmt.Printf("must specify at least one table to GC\n")
		return nil
	}

	mc, err := newMetadataClient()

	if err != nil {
		fmt.Errorf("newMetadataClient error: %v", err)
		return nil
	}

	defer mc.Close()

	// lookup tables

	type (
		cgExtentRow struct {
			cgUUID, extUUID string
		}

		inputExtentRow struct {
			destUUID, inputUUID string
		}

		storeExtentRow struct {
			storeUUID, extUUID string
		}
	)

	var (
		destPaths   = make(map[string]string)   // dest-uuid to path
		cgPaths     = make(map[string]string)   // cg-uuid to path
		extDest     = make(map[string]string)   // ext-uuid to dest-uuid
		cgDest      = make(map[string]string)   // cg-uuid to dest-uuid
		destCGs     = make(map[string][]string) // dest-uuid to list of CGs
		destDLQs    = make(map[string][]string) // dest-uuid to list of DLQs (for its CGs)
		destExtents = make(map[string][]string) // destination to extents

		destActive = make(map[string]string) // destinations active
		destNoTTL  = set.New(0)

		cgActive = set.New(0) // consumer_groups
		cgNoTTL  = set.New(0)

		extActive = set.New(0) // destination_extents
		extNoTTL  = set.New(0)

		cgExtentsActive = set.New(0) // consumer_group_extents: extents to consumer-groups
		cgExtentsNoTTL  = set.New(0)

		inputExtentsActive = set.New(0) // input_host_extents: extent-uuid to {dest-uuid,input-uuid} (for active extents in input_host_extents)
		inputExtentsNoTTL  = set.New(0)

		storeExtentsActive = set.New(0) // store_extents: extent-uuid to store-uuid (for active extents in store_extents)
		storeExtentsNoTTL  = set.New(0)

		destDeleting = make(map[string]string) // destinations in 'deleting' state
	)

	var consistency = gocql.All // use 'all' consistency

	if cliContext.IsSet("consistency") { // override consistency, if specified
		consistency = gocql.ParseConsistency(cliContext.String("consistency"))
	}

	// get query iterator
	getIterator := func(cql string) (iter *gocql.Iter, close func() error) {

		iter = mc.session.Query(cql).Consistency(consistency).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: mc.retries}).Iter()
		close = func() (err error) {
			if err = iter.Close(); err != nil {
				fmt.Printf("ERROR from query '%v': %v\n", cql, err)
			}
			return
		}

		return
	}

	// 'store_extents' table:
	if gcStoreExtents {

		fmt.Printf("store_extents: ")

		cql := `SELECT extent_uuid, store_uuid, status, TTL(status) FROM store_extents`
		iter, close := getIterator(cql)

		var extUUID, storeUUID string
		var status, ttl int
		var nRows int
		for iter.Scan(&extUUID, &storeUUID, &status, &ttl) {

			if status != int(shared.ExtentStatus_DELETED) { // FIXME: BUG in metadata-client (should use ExtentReplicaStatus)

				storeExtentsActive.Insert(storeUUID + `:` + extUUID)

			} else if ttl == 0 {

				storeExtentsNoTTL.Insert(storeUUID + `:` + extUUID)
			}

			nRows++
			//fmt.Printf("\rstore_extents: %d rows", nRows)
		}

		fmt.Printf("\rstore_extents: %d rows: %d active\n", nRows, storeExtentsActive.Count())
		if close() != nil {
			return nil
		}

		if !storeExtentsNoTTL.Empty() {
			fmt.Printf("\tWARNING: %d deleted store_extents have no TTL!\n", storeExtentsNoTTL.Count())
		}
	}

	// 'input_host_extents' table:
	if gcInputExtents {

		fmt.Printf("input_host_extents: ")

		cql := `SELECT input_host_uuid, destination_uuid, extent_uuid, status, TTL(status) FROM input_host_extents`
		iter, close := getIterator(cql)

		var inputUUID, destUUID, extUUID string
		var status, ttl int
		var nRows int
		for iter.Scan(&inputUUID, &destUUID, &extUUID, &status, &ttl) {

			if status != int(shared.ExtentStatus_DELETED) {

				inputExtentsActive.Insert(inputUUID + `:` + extUUID)

			} else if ttl == 0 {

				inputExtentsNoTTL.Insert(inputUUID + `:` + extUUID)
			}

			nRows++
			//fmt.Printf("\rinput_host_extents: %d rows", nRows)
		}

		fmt.Printf("\rinput_host_extents: %d rows: %d active\n", nRows, inputExtentsActive.Count())

		if close() != nil {
			return nil
		}

		if !inputExtentsNoTTL.Empty() {
			fmt.Printf("\tWARNING: %d deleted input_host_extents have no TTL!\n", inputExtentsNoTTL.Count())
		}
	}

	// 'consumer_group_extents' table:
	if gcCGExtents {

		fmt.Printf("consumer_group_extents: ")

		cql := `SELECT extent_uuid, consumer_group_uuid, status, TTL(status) FROM consumer_group_extents`
		iter, close := getIterator(cql)

		var extUUID, cgUUID string
		var status, ttl int
		var nRows int
		var cgxExtents = set.New(0) // consumer_group_extents: extents to consumer-groups
		var cgxCGs = set.New(0)     // consumer_group_extents: consumer-groups to extents
		for iter.Scan(&extUUID, &cgUUID, &status, &ttl) {

			if status != int(shared.ConsumerGroupExtentStatus_DELETED) {

				cgExtentsActive.Insert(cgUUID + `:` + extUUID)

				cgxExtents.Insert(extUUID)
				cgxCGs.Insert(cgUUID)

			} else if ttl == 0 {

				cgExtentsNoTTL.Insert(cgUUID + `:` + extUUID)
			}

			nRows++
			//fmt.Printf("\rconsumer_group_extents: %d rows", nRows)
		}

		fmt.Printf("\rconsumer_group_extents: %d rows: %d active (for %d extents, %d cgs)\n", nRows, cgExtentsActive.Count(), cgxExtents.Count(), cgxCGs.Count())
		if close() != nil {
			return nil
		}

		if !cgExtentsNoTTL.Empty() {
			fmt.Printf("\tWARNING: %d deleted consumer-group extents have no TTL!\n", cgExtentsNoTTL.Count())
		}
	}

	// 'destination_extents' table:
	if gcDestExtents || gcStoreExtents || gcInputExtents || gcCGExtents {

		fmt.Printf("destination_extents: ")

		cql := `SELECT extent_uuid, extent.status, TTL(extent), destination_uuid FROM destination_extents`
		iter, close := getIterator(cql)

		var extUUID, destUUID string
		var status, ttl int
		var nRows int
		for iter.Scan(&extUUID, &status, &ttl, &destUUID) {

			extDest[extUUID] = destUUID

			if status != int(shared.ExtentStatus_DELETED) {

				extActive.Insert(extUUID)
				destExtents[destUUID] = append(destExtents[destUUID], extUUID)

			} else if ttl == 0 {

				extNoTTL.Insert(extUUID)
				destExtents[destUUID] = append(destExtents[destUUID], extUUID)
			}

			nRows++
			//fmt.Printf("\rdestination_extents: %d rows", nRows)
		}

		fmt.Printf("\rdestination_extents: %d rows: %d active (for %d destinations)\n", nRows, extActive.Count(), len(destExtents))
		if close() != nil {
			return nil
		}

		if !extNoTTL.Empty() {
			fmt.Printf("\tWARNING: %d deleted extents have no TTL!\n", extNoTTL.Count())
		}
	}

	// consumer_groups table:
	{
		fmt.Printf("consumer_groups: ")

		cql := `SELECT uuid, consumer_group.name, consumer_group.status, TTL(consumer_group), consumer_group.destination_uuid, consumer_group.dead_letter_queue_destination_uuid FROM consumer_groups`
		iter, close := getIterator(cql)

		var cgUUID, destUUID, cgName, dlqDestUUID string
		var status, ttl int
		var nRows, nDLQ, nCGDeleting, nCGDeleted int
		for iter.Scan(&cgUUID, &cgName, &status, &ttl, &destUUID, &dlqDestUUID) {

			cgPaths[cgUUID] = cgName
			cgDest[cgUUID] = destUUID

			if status != int(shared.ConsumerGroupStatus_DELETED) {

				if status == int(shared.ConsumerGroupStatus_DELETING) {
					nCGDeleting++
				} else {
					cgActive.Insert(cgUUID)
				}

				destCGs[destUUID] = append(destCGs[destUUID], cgUUID)

				if dlqDestUUID != "" {
					destDLQs[destUUID] = append(destDLQs[destUUID], dlqDestUUID)
					nDLQ++
				}

			} else {

				nCGDeleted++

				if ttl == 0 {
					cgNoTTL.Insert(cgUUID)
				}
			}

			nRows++
			//fmt.Printf("\rconsumer_groups: %d rows", nRows)
		}

		fmt.Printf("\rconsumer_groups: %d rows: %d active, %d deleting, %d deleted (found %d DLQs for %d destinations)\n",
			nRows, cgActive.Count(), nCGDeleting, nCGDeleted, nDLQ, len(destDLQs))

		if close() != nil {
			return nil
		}

		if !cgNoTTL.Empty() {
			fmt.Printf("\tWARNING: %d deleted consumer-groups have no TTL!\n", cgNoTTL.Count())
		}
	}

	// 'destinations' table: find all destinations
	{
		fmt.Printf("destinations: ")

		cql := `SELECT uuid, destination.path, destination.status, TTL(destination) FROM destinations`
		iter, close := getIterator(cql)
		var destUUID, destPath string
		var status, ttl int
		var nRows, nDestDeleted int
		for iter.Scan(&destUUID, &destPath, &status, &ttl) {

			destPaths[destUUID] = destPath

			if status != int(shared.DestinationStatus_DELETED) {

				if status == int(shared.DestinationStatus_DELETING) {
					destDeleting[destUUID] = destPath
				} else {
					destActive[destUUID] = destPath
				}

			} else {

				nDestDeleted++

				if ttl == 0 {
					destNoTTL.Insert(destUUID)
				}
			}

			nRows++
			//fmt.Printf("\rdestinations: %d rows", nRows)
		}

		fmt.Printf("\rdestinations: %d rows: %d active, %d deleting, %d deleted\n", nRows, len(destActive), len(destDeleting), nDestDeleted)
		if close() != nil {
			return nil
		}

		if !destNoTTL.Empty() {
			fmt.Printf("\tWARNING: %d deleted destinations have no TTL!\n", destNoTTL.Count())
		}
	}

	// 'destinations_by_path' table: find all valid top-level destinations
	var destByPathActive = make(map[string]string)
	{
		fmt.Printf("destinations_by_path: ")
		cql := `SELECT path, destination.uuid FROM destinations_by_path`
		iter, close := getIterator(cql)

		var path, uuid string
		var nRows int
		for iter.Scan(&path, &uuid) {
			destByPathActive[uuid] = path
			nRows++
			//fmt.Printf("\rdestinations_by_path: %d rows", nRows)
		}

		fmt.Printf("\rdestinations_by_path: %d rows\n", len(destByPathActive))
		if close() != nil {
			return nil
		}

	}

	// -- compute set of valid destination, cg and extent UUIDs -- //

	var destValid = set.New(0) // set of valid destination-uuids
	var cgValid = set.New(0)   // set of valid consumergroup-uuids
	var extValid = set.New(0)  // set of valid extent-uuids

	// include all entries from destinations_by_path table
	var nDestNoUUID, nDestPathMismatch int
	for d, p := range destByPathActive {

		destValid.Insert(d)

		// check if exists in the 'destinations' table
		if dp, ok := destActive[d]; !ok {

			nDestNoUUID++

		} else if p != dp { // validate that paths match

			nDestPathMismatch++
		}
	}

	if nDestNoUUID > 0 {
		fmt.Printf("\tWARNING: %d destinations in destinations_by_path have no entry in destinations table!\n", nDestNoUUID)
	}

	if nDestPathMismatch > 0 {
		fmt.Printf("\tWARNING: %d destinations in destinations_by_path do not match path in destinations table!\n", nDestPathMismatch)
	}

	// all 'deleting' destinations as valid
	for d := range destDeleting {
		destValid.Insert(d)
	}

	// find valid DLQ destinations and include them
	for d, dlqs := range destDLQs {

		// only if the destination is valid (ie, exists in the destinations_by_path table),
		if _, ok := destByPathActive[d]; ok {

			for _, dlq := range dlqs {
				destValid.Insert(dlq) // get path from 'destinations' table entry
			}
		}
	}

	// find all valid CGs
	if gcConsumerGroups || gcCGExtents {
		for d, cgs := range destCGs {

			// only include CGs for destinations that are valid
			if destValid.Contains(d) {
				for _, cg := range cgs {
					cgValid.Insert(cg)
				}
			}
		}
	}

	// find all valid Extents
	if gcDestExtents || gcStoreExtents || gcInputExtents || gcCGExtents {
		for d, extents := range destExtents {

			// only include extents for destinations that are valid
			if destValid.Contains(d) {
				for _, x := range extents {
					extValid.Insert(x)
				}
			}
		}
	}

	fmt.Printf("--\n")

	out := cmqOutputWriter([]string{"deletecql", "undocql"})
	defer out.close()

	// -- find leaks -- //

	// * destinations:
	if gcDestinations {
		fmt.Printf("destinations: ")

		var nInvalidRows int

		iter, close := getIterator("SELECT * from destinations")
		defer close()

		var row = make(map[string]interface{})

		for iter.MapScan(row) {

			d := row["uuid"].(gocql.UUID).String()

			if _, ok := destActive[d]; ok {

				if !destValid.Contains(d) {

					nInvalidRows++
					out.Destination(row, destPaths[d])
				}

			} else if destNoTTL.Contains(d) {

				nInvalidRows++
				out.Destination(row, destPaths[d])
			}

			row = make(map[string]interface{})
		}

		fmt.Printf("%d (of %d) to be deleted\n", nInvalidRows, len(destActive)+destNoTTL.Count())
	}

	// * consumer_groups:
	if gcConsumerGroups {
		fmt.Printf("consumer_groups: ")

		var nInvalidRows int

		iter, close := getIterator("SELECT * from consumer_groups")
		defer close()

		var row = make(map[string]interface{})

		for iter.MapScan(row) {

			cg := row["uuid"].(gocql.UUID).String()

			if cgActive.Contains(cg) {

				if !cgValid.Contains(cg) {

					nInvalidRows++
					out.ConsumerGroup(row, cgPaths[cg])
				}

			} else if cgNoTTL.Contains(cg) {

				nInvalidRows++
				out.ConsumerGroup(row, cgPaths[cg])
			}

			row = make(map[string]interface{})
		}

		fmt.Printf("%d (of %d) to be deleted\n", nInvalidRows, cgActive.Count()+cgNoTTL.Count())
	}

	// * destination_extents:
	if gcDestExtents {
		fmt.Printf("destination_extents: ")

		var nInvalidRows int

		iter, close := getIterator("SELECT * from destination_extents")
		defer close()

		var row = make(map[string]interface{})

		for iter.MapScan(row) {

			x := row["extent_uuid"].(gocql.UUID).String()

			if extActive.Contains(x) {

				if !extValid.Contains(x) {

					nInvalidRows++
					destUUID := row["destination_uuid"].(gocql.UUID).String()
					out.Extent(row, destPaths[destUUID])
				}

			} else if extNoTTL.Contains(x) {

				nInvalidRows++
				destUUID := row["destination_uuid"].(gocql.UUID).String()
				out.Extent(row, destPaths[destUUID])
			}

			row = make(map[string]interface{})
		}

		fmt.Printf("%d (of %d) to be deleted\n", nInvalidRows, extActive.Count()+extNoTTL.Count())
	}

	// * consumer_group_extents:
	if gcCGExtents {
		fmt.Printf("consumer_group_extents: ")

		var nInvalidRows int

		iter, close := getIterator("SELECT * from consumer_group_extents")
		defer close()

		var row = make(map[string]interface{})

		for iter.MapScan(row) {

			extUUID := row["extent_uuid"].(gocql.UUID).String()
			cgUUID := row["consumer_group_uuid"].(gocql.UUID).String()
			key := cgUUID + `:` + extUUID

			// -- validate that row was in our first query --
			if !cgExtentsActive.Contains(key) {

				row = make(map[string]interface{})
				continue

			} else if cgExtentsNoTTL.Contains(key) { // check if no-ttl case

				nInvalidRows++
				out.ConsumerGroupExtent(row, fmt.Sprintf("%s, %s", destPaths[extDest[extUUID]], cgPaths[cgUUID]))
				row = make(map[string]interface{})
				continue
			}

			if !extValid.Contains(extUUID) { // valid extent?
				nInvalidRows++
				out.ConsumerGroupExtent(row, fmt.Sprintf("%s, %s", destPaths[extDest[extUUID]], cgPaths[cgUUID]))
				row = make(map[string]interface{})
				continue
			}

			if !cgValid.Contains(cgUUID) { // valid CG?
				nInvalidRows++
				out.ConsumerGroupExtent(row, fmt.Sprintf("%s, %s", destPaths[extDest[extUUID]], cgPaths[cgUUID]))
				row = make(map[string]interface{})
				continue
			}

			row = make(map[string]interface{})
		}

		/* // logic for additional validation //
		for x, cgs := range cgExtents {
			if !extValid.Contains(x) { // check if valid extent
				for _, cg := range cgs {
					fmt.Printf("DELETE FROM consumer_group_extents WHERE consumer_group_uuid=%v AND extent_uuid=%v;\n", cg, x)
				}
				continue
			}
			for _, cg := range cgs { // check if valid CG
				if _, ok := cgValid[cg]; !ok {
					fmt.Printf("DELETE FROM consumer_group_extents WHERE consumer_group_uuid=%v AND extent_uuid=%v;\n", cg, x)
				}
			}
		}
		*/

		fmt.Printf("%d (of %d) to be deleted\n", nInvalidRows, cgExtentsActive.Count()+cgExtentsNoTTL.Count())
	}

	// * input_host_extents:
	if gcInputExtents {
		fmt.Printf("input_host_extents: ")

		var nInvalidRows int

		iter, close := getIterator("SELECT * from input_host_extents")
		defer close()

		var row = make(map[string]interface{})

		for iter.MapScan(row) {

			input := row["input_host_uuid"].(gocql.UUID).String()
			x := row["extent_uuid"].(gocql.UUID).String()
			key := input + `:` + x

			if inputExtentsActive.Contains(key) {

				if !extValid.Contains(x) {
					nInvalidRows++
					out.InputExtent(row, "") // FIXME: look for dest info?
				}

			} else if inputExtentsNoTTL.Contains(key) {

				nInvalidRows++
				out.InputExtent(row, "") // FIXME: look for dest info?
			}

			row = make(map[string]interface{})
		}

		fmt.Printf("%d (of %d) to be deleted\n", nInvalidRows, inputExtentsActive.Count()+inputExtentsNoTTL.Count())
	}

	// * store_extents:
	if gcStoreExtents {
		fmt.Printf("store_extents: ")

		var nInvalidRows int

		iter, close := getIterator("SELECT * from store_extents")
		defer close()

		var row = make(map[string]interface{})

		for iter.MapScan(row) {

			x := row["extent_uuid"].(gocql.UUID).String()
			s := row["store_uuid"].(gocql.UUID).String()
			key := s + `:` + x

			if storeExtentsActive.Contains(key) {

				if !extValid.Contains(x) {
					nInvalidRows++
					out.StoreExtent(row, "") // TOOD: annotate with dest-info?
				}

			} else if storeExtentsNoTTL.Contains(key) {

				nInvalidRows++
				out.StoreExtent(row, "") // TODO: annotate with dest-info?
			}

			row = make(map[string]interface{})
		}

		fmt.Printf("%d (of %d) to be deleted\n", nInvalidRows, storeExtentsActive.Count()+storeExtentsNoTTL.Count())
	}

	return nil
}
