package main

import (
	"fmt"
	"regexp"

	"github.com/gocql/gocql"
	"github.com/urfave/cli"
)

func listDestinations(c *cli.Context) error {

	mc, err := newMetadataClient()

	if err != nil {
		fmt.Errorf("newMetadataClient error: %v", err)
		return nil
	}

	defer mc.Close()

	out := cmqOutputWriter(cliContext.StringSlice(`output`))
	defer out.close()

	cql := "SELECT * FROM destinations"

	var where []string

	if destMatch := multiMatchWhereClause("uuid", append(c.Args(), c.StringSlice("destination")...)); len(destMatch) > 0 {
		where = append(where, destMatch)
	}

	if c.IsSet("multizone") {
		where = append(where, "is_multi_zone=true")
	}

	cql = appendWhere(cql, where)

	var typeFilters []int
	for _, destType := range c.StringSlice("type") {
		if t := getDestType(destType); t >= 0 {
			typeFilters = append(typeFilters, t)
		}
	}

	var statusFilters []int
	for _, status := range c.StringSlice("status") {
		if s := getDestStatus(status); s >= 0 {
			statusFilters = append(statusFilters, s)
		}
	}

	var pathFilters []*regexp.Regexp
	for _, path := range c.StringSlice("path") {
		if r, e := regexp.Compile(path); e == nil {
			pathFilters = append(pathFilters, r)
		}
	}

	iter := mc.session.Query(cql).Iter()

	out.DestinationStart()

	for {
		var row = make(map[string]interface{})

		if !iter.MapScan(row) {
			break
		}

		if len(typeFilters) > 0 || len(statusFilters) > 0 || len(pathFilters) > 0 {

			dest, ok := row[`destination`].(map[string]interface{})

			if !ok || len(dest) == 0 {
				continue
			}

			if !matchIntFilters(dest["type"].(int), typeFilters) {
				continue
			}

			if !matchIntFilters(dest["status"].(int), statusFilters) {
				continue
			}

			if len(pathFilters) > 0 && !matchRegexpFilters(dest["path"].(string), pathFilters) {
				continue
			}
		}

		out.Destination(row, "")
	}

	out.DestinationEnd()

	if err := iter.Close(); err != nil {
		fmt.Printf("listDestinations: iterator error: %v\n", err)
	}

	return nil
}

func listDestinationsByPath(c *cli.Context) error {

	return fmt.Errorf("listDestinationsByPath: not implemented")
}

func listConsumerGroups(c *cli.Context) error {

	mc, err := newMetadataClient()

	if err != nil {
		fmt.Errorf("newMetadataClient error: %v", err)
		return nil
	}

	defer mc.Close()

	out := cmqOutputWriter(cliContext.StringSlice(`output`))
	defer out.close()

	cql := "SELECT * FROM consumer_groups"

	var where []string
	if cgMatch := multiMatchWhereClause("uuid", append(c.Args(), c.StringSlice("consumergroup")...)); len(cgMatch) > 0 {
		where = append(where, cgMatch)
	}

	cql = appendWhere(cql, where)

	/*
		var filters = map[string]Filter{
			"destUUID": makeUUIDFilter(c.StringSlice("destination")),
			"status": makeIntFilter(getStatusSlice(c.StringSlice("status")),
		}
	*/

	var destFilters = getUUIDFilters(c.StringSlice("destination"))

	var statusFilters []int
	for _, status := range c.StringSlice("status") {
		if s := getCGStatus(status); s >= 0 {
			statusFilters = append(statusFilters, s)
		}
	}

	var nameFilters []*regexp.Regexp
	for _, name := range c.StringSlice("name") {
		if r, e := regexp.Compile(name); e == nil {
			nameFilters = append(nameFilters, r)
		}
	}

	iter := mc.session.Query(cql).Iter()

	out.ConsumerGroupStart()

	for {
		var row = make(map[string]interface{})

		if !iter.MapScan(row) {
			break
		}

		if len(destFilters) > 0 {

			destUUID, ok := row["destination_uuid"].(gocql.UUID)

			if !ok || !matchUUIDFilters(destUUID, destFilters) {
				continue
			}
		}

		if len(statusFilters) > 0 || len(nameFilters) > 0 {

			cg, ok := row["consumer_group"].(map[string]interface{})

			if !ok || len(cg) == 0 {
				continue
			}

			if !matchIntFilters(cg["status"].(int), statusFilters) {
				continue
			}

			if !matchRegexpFilters(cg["name"].(string), nameFilters) {
				continue
			}
		}

		out.ConsumerGroup(row, "")
	}

	out.ConsumerGroupEnd()

	if err := iter.Close(); err != nil {
		fmt.Printf("listConsumerGroups: iterator error: %v\n", err)
	}

	return nil
}

func listDestinationExtents(c *cli.Context) error {

	mc, err := newMetadataClient()

	if err != nil {
		fmt.Errorf("newMetadataClient error: %v", err)
		return nil
	}

	defer mc.Close()

	out := cmqOutputWriter(cliContext.StringSlice(`output`))
	defer out.close()

	cql := "SELECT * FROM destination_extents"

	var where []string
	var allowFiltering = false

	if destMatch := multiMatchWhereClause("destination_uuid", c.StringSlice("destination")); len(destMatch) > 0 {
		where = append(where, destMatch)
	}

	if extMatch := multiMatchWhereClause("extent_uuid", append(c.Args(), c.StringSlice("extent")...)); len(extMatch) > 0 {
		where = append(where, extMatch)
		allowFiltering = true
	}

	cql = appendWhere(cql, where)

	var statusFilters []int
	for _, status := range c.StringSlice("status") {
		if s := getExtStatus(status); s >= 0 {
			statusFilters = append(statusFilters, s)
		}
	}

	if allowFiltering {
		cql += "ALLOW FILTERING"
	}

	iter := mc.session.Query(cql).Iter()

	out.ExtentStart()

	for {
		var row = make(map[string]interface{})

		if !iter.MapScan(row) {
			break
		}

		if len(statusFilters) > 0 {

			status, ok := row["status"].(int)

			if !ok || !matchIntFilters(status, statusFilters) {
				continue
			}
		}

		out.Extent(row, "")
	}

	out.ExtentEnd()

	if err := iter.Close(); err != nil {
		fmt.Printf("listDestinationExtents: iterator error: %v\n", err)
	}

	return nil
}

func listConsumerGroupExtents(c *cli.Context) error {

	mc, err := newMetadataClient()

	if err != nil {
		fmt.Errorf("newMetadataClient error: %v", err)
		return nil
	}

	defer mc.Close()

	out := cmqOutputWriter(cliContext.StringSlice(`output`))
	defer out.close()

	cql := "SELECT * FROM consumer_group_extents"

	var where []string
	var allowFiltering = false

	if cgMatch := multiMatchWhereClause("consumer_group_uuid", append(c.Args(), c.StringSlice("consumergroup")...)); len(cgMatch) > 0 {
		where = append(where, cgMatch)
	}

	if extMatch := multiMatchWhereClause("extent_uuid", c.StringSlice("extent")); len(extMatch) > 0 {
		where = append(where, extMatch)
		allowFiltering = true
	}

	cql = appendWhere(cql, where)

	var statusFilters []int
	for _, status := range c.StringSlice("status") {
		if s := getCgxStatus(status); s >= 0 {
			statusFilters = append(statusFilters, s)
		}
	}

	if allowFiltering {
		cql += "ALLOW FILTERING"
	}

	iter := mc.session.Query(cql).Iter()

	out.ConsumerGroupExtentStart()

	for {
		var row = make(map[string]interface{})

		if !iter.MapScan(row) {
			break
		}

		if len(statusFilters) > 0 {

			status, ok := row["status"].(int)

			if !ok || !matchIntFilters(status, statusFilters) {
				continue
			}
		}

		out.ConsumerGroupExtent(row, "")
	}

	out.ConsumerGroupExtentEnd()

	if err := iter.Close(); err != nil {
		fmt.Printf("listConsumerGroups: iterator error: %v\n", err)
	}

	return nil
}

func listStoreExtents(c *cli.Context) error {

	mc, err := newMetadataClient()

	if err != nil {
		fmt.Errorf("newMetadataClient error: %v", err)
		return nil
	}

	defer mc.Close()

	out := cmqOutputWriter(cliContext.StringSlice(`output`))
	defer out.close()

	cql := "SELECT * FROM store_extents"

	var where []string
	var allowFiltering = false

	if extMatch := multiMatchWhereClause("extent_uuid", append(c.Args(), c.StringSlice("extent")...)); len(extMatch) > 0 {
		where = append(where, extMatch)
		allowFiltering = true
	}

	cql = appendWhere(cql, where)

	var statusFilters []int
	for _, status := range c.StringSlice("status") {
		if s := getSxStatus(status); s >= 0 {
			statusFilters = append(statusFilters, s)
		}
	}

	if allowFiltering {
		cql += "ALLOW FILTERING"
	}

	iter := mc.session.Query(cql).Iter()

	out.StoreExtentStart()

	for {
		var row = make(map[string]interface{})

		if !iter.MapScan(row) {
			break
		}

		// storeUUID := row["store_uuid"]
		// extUUID := row["extent_uuid"]

		if matchIntFilters(row["status"].(int), statusFilters) {
			continue
		}

		// fmt.Printf("store=%v ext=%v\n", storeUUID, extUUID)
		// printRow("\t", row)
		// fmt.Printf("\n")
		out.StoreExtent(row, "")
	}

	out.StoreExtentEnd()

	if err := iter.Close(); err != nil {
		fmt.Printf("listStoreExtents: iterator error: %v\n", err)
	}

	return nil
}

func listInputHostExtents(c *cli.Context) error {

	// TODO: this is currenty just copy-pasted from 'storeExtents'; needs review/work

	mc, err := newMetadataClient()

	if err != nil {
		fmt.Errorf("newMetadataClient error: %v", err)
		return nil
	}

	defer mc.Close()

	out := cmqOutputWriter(cliContext.StringSlice(`output`))
	defer out.close()

	cql := "SELECT * FROM input_extents"

	var where []string
	var allowFiltering = false

	if extMatch := multiMatchWhereClause("extent_uuid", append(c.Args(), c.StringSlice("extent")...)); len(extMatch) > 0 {
		where = append(where, extMatch)
		allowFiltering = true
	}

	cql = appendWhere(cql, where)

	var statusFilters []int
	for _, status := range c.StringSlice("status") {
		if s := getSxStatus(status); s >= 0 {
			statusFilters = append(statusFilters, s)
		}
	}

	if allowFiltering {
		cql += "ALLOW FILTERING"
	}

	iter := mc.session.Query(cql).Iter()

	out.StoreExtentStart()

	for {
		var row = make(map[string]interface{})

		if !iter.MapScan(row) {
			break
		}

		// storeUUID := row["store_uuid"]
		// extUUID := row["extent_uuid"]

		if !matchIntFilters(row["status"].(int), statusFilters) {
			continue
		}

		// fmt.Printf("store=%v ext=%v\n", storeUUID, extUUID)
		// printRow("\t", row)
		// fmt.Printf("\n")
		out.StoreExtent(row, "")
	}

	out.StoreExtentEnd()

	if err := iter.Close(); err != nil {
		fmt.Printf("listStoreExtents: iterator error: %v\n", err)
	}

	return nil
}

func listOperations(c *cli.Context) error {

	if c.NArg() == 0 {
		return fmt.Errorf("specify UUIDs to search for")
	}

	mc, err := newMetadataClient()

	if err != nil {
		fmt.Errorf("newMetadataClient error: %v", err)
		return nil
	}

	defer mc.Close()

	out := cmqOutputWriter(cliContext.StringSlice(`output`))
	defer out.close()

	cql := "SELECT * FROM user_operations_by_entity_uuid"

	var where []string

	if uuidMatch := multiMatchWhereClause("entity_uuid", c.Args()); len(uuidMatch) > 0 {
		where = append(where, uuidMatch)
	}

	cql = appendWhere(cql, where)

	iter := mc.session.Query(cql).Iter()

	for {
		var row = make(map[string]interface{})

		if !iter.MapScan(row) {
			break
		}

		uuid := row["uuid"]

		fmt.Printf("%v\n", uuid)
		printRow("\t", row)
		fmt.Printf("\n")
	}

	if err := iter.Close(); err != nil {
		fmt.Printf("listOperations: iterator error: %v\n", err)
	}

	return nil
}
