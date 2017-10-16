package main

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/urfave/cli"
)

func gc(c *cli.Context, mc *MetadataClient) error {

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

	// lookup tables
	var (
		destPaths   = make(map[string]string)   // dest-uuid to path
		cgPaths     = make(map[string]string)   // cg-uuid to path
		extDest     = make(map[string]string)   // ext-uuid to dest-uuid
		cgDest      = make(map[string]string)   // cg-uuid to dest-uuid
		destCGs     = make(map[string][]string) // dest-uuid to list of CGs
		destDLQs    = make(map[string][]string) // dest-uuid to list of DLQs (for its CGs)
		destExtents = make(map[string][]string) // destination to extents
	)

	type inputExtentRow struct {
		destUUID  string
		inputUUID string
	}

	var (
		storeExtents = make(map[string]string)         // store_extents: extent-uuid to store-uuid (for active extents in store_extents)
		inputExtents = make(map[string]inputExtentRow) // input_host_extents: extent-uuid to {dest-uuid,input-uuid} (for active extents in input_host_extents)

		cgxExtents = make(map[string][]string) // consumer_group_extents: extents to consumer-groups
		cgxCGs     = make(map[string][]string) // consumer_group_extents: consumer-groups to extents

		extActive = make(map[string]string) // destination_extents
		cgActive  = make(map[string]string) // consumer_groups
	)

	// get query iterator
	getIterator := func(cql string) (iter *gocql.Iter, close func() error) {

		iter = mc.session.Query(cql).Consistency(gocql.All).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: mc.retries}).Iter()
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
		var nDeletedNoTTL int
		for iter.Scan(&extUUID, &storeUUID, &status, &ttl) {

			if status != int(shared.ExtentStatus_DELETED) { // FIXME: BUG in metadata-client (should use ExtentReplicaStatus)

				storeExtents[extUUID] = storeUUID

			} else if ttl == 0 {

				nDeletedNoTTL++
			}

			nRows++
			//fmt.Printf("\rstore_extents: %d rows", nRows)
		}

		fmt.Printf("\rstore_extents: %d rows: %d active\n", nRows, len(storeExtents))
		if close() != nil {
			return nil
		}

		if nDeletedNoTTL > 0 {
			fmt.Printf("\tWARNING: %d deleted store_extents have no TTL!\n", nDeletedNoTTL)
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
		var nDeletedNoTTL int
		for iter.Scan(&inputUUID, &destUUID, &extUUID, &status, &ttl) {

			if status != int(shared.ExtentStatus_DELETED) {

				inputExtents[extUUID] = inputExtentRow{destUUID: destUUID, inputUUID: inputUUID}

			} else if ttl == 0 {

				nDeletedNoTTL++
			}

			nRows++
			//fmt.Printf("\rinput_host_extents: %d rows", nRows)
		}

		fmt.Printf("\rinput_host_extents: %d rows: %d active\n", nRows, len(inputExtents))
		if close() != nil {
			return nil
		}

		if nDeletedNoTTL > 0 {
			fmt.Printf("\tWARNING: %d deleted input_host_extents have no TTL!\n", nDeletedNoTTL)
		}
	}

	// 'consumer_group_extents' table:
	var nCGExtents int
	if gcCGExtents {

		fmt.Printf("consumer_group_extents: ")

		cql := `SELECT extent_uuid, consumer_group_uuid, status, TTL(status) FROM consumer_group_extents`
		iter, close := getIterator(cql)

		var extUUID, cgUUID string
		var status, ttl int
		var nRows int
		var nDeletedNoTTL int
		for iter.Scan(&extUUID, &cgUUID, &status, &ttl) {

			if status != int(shared.ConsumerGroupExtentStatus_DELETED) {

				cgxExtents[extUUID] = append(cgxExtents[extUUID], cgUUID)
				cgxCGs[cgUUID] = append(cgxCGs[cgUUID], extUUID)
				nCGExtents++

			} else if ttl == 0 {

				nDeletedNoTTL++
			}

			nRows++
			//fmt.Printf("\rconsumer_group_extents: %d rows", nRows)
		}

		fmt.Printf("\rconsumer_group_extents: %d rows: %d active (for %d extents, %d cgs)\n", nRows, nCGExtents, len(cgxExtents), len(cgxCGs))
		if close() != nil {
			return nil
		}

		if nDeletedNoTTL > 0 {
			fmt.Printf("\tWARNING: %d deleted consumer-group extents have no TTL!\n", nDeletedNoTTL)
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
		var nDeletedNoTTL int
		for iter.Scan(&extUUID, &status, &ttl, &destUUID) {

			extDest[extUUID] = destUUID

			if status != int(shared.ExtentStatus_DELETED) {

				extActive[extUUID] = destUUID
				destExtents[destUUID] = append(destExtents[destUUID], extUUID)

			} else if ttl == 0 {

				nDeletedNoTTL++
			}

			nRows++
			//fmt.Printf("\rdestination_extents: %d rows", nRows)
		}

		fmt.Printf("\rdestination_extents: %d rows: %d active (for %d destinations)\n", nRows, len(extActive), len(destExtents))
		if close() != nil {
			return nil
		}

		if nDeletedNoTTL > 0 {
			fmt.Printf("\tWARNING: %d deleted extents have no TTL!\n", nDeletedNoTTL)
		}
	}

	// consumer_groups table:
	{
		fmt.Printf("consumer_groups: ")

		cql := `SELECT uuid, consumer_group.name, consumer_group.status, TTL(consumer_group), consumer_group.destination_uuid, consumer_group.dead_letter_queue_destination_uuid FROM consumer_groups`
		iter, close := getIterator(cql)

		var cgUUID, destUUID, cgName, dlqDestUUID string
		var status, ttl int
		var nRows, nDLQ int
		var nDeletedNoTTL int
		for iter.Scan(&cgUUID, &cgName, &status, &ttl, &destUUID, &dlqDestUUID) {

			cgPaths[cgUUID] = cgName
			cgDest[cgUUID] = destUUID

			if status != int(shared.ConsumerGroupStatus_DELETED) {

				cgActive[cgUUID] = destUUID
				destCGs[destUUID] = append(destCGs[destUUID], cgUUID)

				if dlqDestUUID != "" {
					destDLQs[destUUID] = append(destDLQs[destUUID], dlqDestUUID)
					nDLQ++
				}

			} else if ttl == 0 {

				nDeletedNoTTL++
			}

			nRows++
			//fmt.Printf("\rconsumer_groups: %d rows", nRows)
		}

		fmt.Printf("\rconsumer_groups: %d rows: %d active (found %d DLQs for %d destinations)\n", nRows, len(cgActive), nDLQ, len(destDLQs))
		if close() != nil {
			return nil
		}

		if nDeletedNoTTL > 0 {
			fmt.Printf("\tWARNING: %d deleted consumer-groups have no TTL!\n", nDeletedNoTTL)
		}
	}

	// 'destinations' table: find all destinations
	var destActive = make(map[string]string)
	{
		fmt.Printf("destinations: ")
		cql := `SELECT uuid, destination.path, destination.status, TTL(destination) FROM destinations`
		iter, close := getIterator(cql)
		var destUUID, destPath string
		var status, ttl int
		var nRows, nDestDeleted, nDestDeleting int
		var nDeletedNoTTL int
		for iter.Scan(&destUUID, &destPath, &status, &ttl) {

			destPaths[destUUID] = destPath

			if status != int(shared.DestinationStatus_DELETED) {

				if status != int(shared.DestinationStatus_DELETING) {
					destActive[destUUID] = destPath
				} else {
					nDestDeleting++
				}

			} else {

				nDestDeleted++

				if ttl == 0 {
					nDeletedNoTTL++
				}
			}

			nRows++
			//fmt.Printf("\rdestinations: %d rows", nRows)
		}

		fmt.Printf("\rdestinations: %d rows: %d active, %d deleting, %d deleted destinations\n", nRows, len(destActive), nDestDeleting, nDestDeleted)
		if close() != nil {
			return nil
		}

		if nDeletedNoTTL > 0 {
			fmt.Printf("\tWARNING: %d deleted destinations have no TTL!\n", nDeletedNoTTL)
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

	var destUUIDs = make(map[string]string) // set of valid destination-uuids
	var cgUUIDs = make(map[string]string)   // set of valid consumergroup-uuids
	var extUUIDs = make(map[string]string)  // set of valid extent-uuids

	// include all entries from destinations_by_path table
	var nDestNoUUID, nDestPathMismatch int
	for d, p := range destByPathActive {

		destUUIDs[d] = p

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

	// find valid DLQ destinations and include them
	for d, dlqs := range destDLQs {

		// only if the destinations is valid (ie, exists in the destinations_by_path table),
		if _, ok := destByPathActive[d]; ok {

			for _, dlq := range dlqs {
				destUUIDs[dlq] = destActive[dlq] // get path from 'destinations' table entry
			}
		}
	}

	// find all valid CGs
	if gcConsumerGroups || gcCGExtents {
		for d, cgs := range destCGs {

			// only include CGs for destinations that are valid
			if _, ok := destUUIDs[d]; ok {
				for _, cg := range cgs {
					cgUUIDs[cg] = d
				}
			}
		}
	}

	// find all valid Extents
	if gcDestExtents || gcStoreExtents || gcInputExtents || gcCGExtents {
		for d, extents := range destExtents {

			// only include CGs for destinations that are valid
			if _, ok := destUUIDs[d]; ok {
				for _, x := range extents {
					extUUIDs[x] = d
				}
			}
		}
	}

	fmt.Printf("--\n")

	out := getCmqWriter(&outContext{destPaths: destPaths, cgPaths: cgPaths, cgDest: cgDest, extDest: extDest},
		outputCqlDelete, outputCqlDeleteUndo /*, outputNull, outputText */)
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

				if _, ok = destUUIDs[d]; !ok {
					// destInvalid = append(destInvalid, d)
					nInvalidRows++
					out.destination(row)
				}
			}

			row = make(map[string]interface{})
		}

		fmt.Printf("%d (of %d) to be deleted\n", nInvalidRows, len(destActive))
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

			if _, ok := cgActive[cg]; ok {

				if _, ok = cgUUIDs[cg]; !ok {
					// cgInvalid = append(cgInvalid, cg)
					nInvalidRows++
					out.consumer_group(row)
				}
			}

			row = make(map[string]interface{})
		}

		fmt.Printf("%d (of %d) to be deleted\n", nInvalidRows, len(cgActive))
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

			if _, ok := extActive[x]; ok {

				if _, ok = extUUIDs[x]; !ok {
					// extInvalid = append(extInvalid, x)
					nInvalidRows++
					out.extent(row)
				}
			}

			row = make(map[string]interface{})
		}

		fmt.Printf("%d (of %d) to be deleted\n", nInvalidRows, len(extActive))
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

			cgs, ok := cgxExtents[extUUID]

			// -- validate that row was in our first query --
			if !ok {
				row = make(map[string]interface{})
				continue
			}

			ok = false
			for _, cg := range cgs {
				if cgUUID == cg {
					ok = true
					break
				}
			}

			if !ok {
				row = make(map[string]interface{})
				continue
			}
			// -- //

			if _, ok := extUUIDs[extUUID]; !ok { // valid extent?
				nInvalidRows++
				out.consumer_group_extent(row)
				row = make(map[string]interface{})
				continue
			}

			if _, ok := cgUUIDs[cgUUID]; !ok { // valid CG?
				nInvalidRows++
				out.consumer_group_extent(row)
				row = make(map[string]interface{})
				continue
			}

			row = make(map[string]interface{})
		}

		/* // logic for additional validation //
		for x, cgs := range cgxExtents {
			if _, ok := extUUIDs[x]; !ok { // check if valid extent
				for _, cg := range cgs {
					fmt.Printf("DELETE FROM consumer_group_extents WHERE consumer_group_uuid=%v AND extent_uuid=%v;\n", cg, x)
				}
				continue
			}
			for _, cg := range cgs { // check if valid CG
				if _, ok := cgUUIDs[cg]; !ok {
					fmt.Printf("DELETE FROM consumer_group_extents WHERE consumer_group_uuid=%v AND extent_uuid=%v;\n", cg, x)
				}
			}
		}
		*/

		fmt.Printf("%d (of %d) to be deleted\n", nInvalidRows, nCGExtents)
	}

	// * input_host_extents:
	if gcInputExtents {
		fmt.Printf("input_host_extents: ")

		var nInvalidRows int

		iter, close := getIterator("SELECT * from input_host_extents")
		defer close()

		var row = make(map[string]interface{})

		for iter.MapScan(row) {

			x := row["extent_uuid"].(gocql.UUID).String()

			if _, ok := inputExtents[x]; ok {

				if _, ok = extUUIDs[x]; !ok {
					nInvalidRows++
					out.input_extent(row)
				}
			}

			row = make(map[string]interface{})
		}

		fmt.Printf("%d (of %d) to be deleted\n", nInvalidRows, len(inputExtents))
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

			if store, ok := storeExtents[x]; ok && s == store {

				if _, ok := extUUIDs[x]; !ok {
					nInvalidRows++
					out.store_extent(row)
				}
			}

			row = make(map[string]interface{})
		}

		fmt.Printf("%d (of %d) to be deleted\n", nInvalidRows, len(storeExtents))
	}

	return nil
}
