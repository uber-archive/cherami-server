package main

import (
	"fmt"
	"regexp"

	"github.com/gocql/gocql"
	"github.com/urfave/cli"
)

func list_destinations(c *cli.Context, mc *MetadataClient) error {

	cql := "SELECT * FROM destinations"

	var where []string

	if c.IsSet("multizone") {
		where = append(where, "is_multi_zone=true")
	}

	if destMatch := multiMatchWhereClause("uuid", c.StringSlice("destination")); len(destMatch) > 0 {
		where = append(where, destMatch)
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

	for {
		var row = make(map[string]interface{})

		if !iter.MapScan(row) {
			break
		}

		destUUID := row["uuid"]

		if len(typeFilters) > 0 &&
			!matchIntFilters(row["destination"].(map[string]interface{})["type"].(int), typeFilters) {
			continue
		}

		if len(statusFilters) > 0 &&
			!matchIntFilters(row["destination"].(map[string]interface{})["status"].(int), statusFilters) {
			continue
		}

		if len(pathFilters) > 0 &&
			!matchRegexpFilters(row["destination"].(map[string]interface{})["path"].(string), pathFilters) {
			continue
		}

		fmt.Printf("%v\n", destUUID)
		printRow("\t", row)
		fmt.Printf("\n")
	}

	if err := iter.Close(); err != nil {
		fmt.Printf("list_destinations: iterator error: %v\n", err)
	}

	return nil
}

func list_destinations_by_path(c *cli.Context, mc *MetadataClient) error {

	return fmt.Errorf("list_destinations_by_path: not implemented")
}

func list_consumer_groups(c *cli.Context, mc *MetadataClient) error {

	cql := "SELECT * FROM consumer_groups"

	var where []string
	if cgMatch := multiMatchWhereClause("uuid", c.StringSlice("consumergroup")); len(cgMatch) > 0 {
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

	for {
		var row = make(map[string]interface{})

		if !iter.MapScan(row) {
			break
		}

		cgUUID := row["uuid"]

		if len(destFilters) > 0 &&
			!matchUUIDFilters(row["consumer_group"].(map[string]interface{})["destination_uuid"].(gocql.UUID), destFilters) {
			continue
		}

		if len(statusFilters) > 0 &&
			!matchIntFilters(row["consumer_group"].(map[string]interface{})["status"].(int), statusFilters) {
			continue
		}

		if len(nameFilters) > 0 &&
			!matchRegexpFilters(row["consumer_group"].(map[string]interface{})["name"].(string), nameFilters) {
			continue
		}

		fmt.Printf("%v\n", cgUUID)
		printRow("\t", row)
		fmt.Printf("\n")
	}

	if err := iter.Close(); err != nil {
		fmt.Printf("list_consumer_groups: iterator error: %v\n", err)
	}

	return nil
}

func list_destination_extents(c *cli.Context, mc *MetadataClient) error {

	cql := "SELECT * FROM destination_extents"

	var where []string
	var allowFiltering = false

	if destMatch := multiMatchWhereClause("destination_uuid", c.StringSlice("destination")); len(destMatch) > 0 {
		where = append(where, destMatch)
	}

	if extMatch := multiMatchWhereClause("extent_uuid", c.StringSlice("extent")); len(extMatch) > 0 {
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

	for {
		var row = make(map[string]interface{})

		if !iter.MapScan(row) {
			break
		}

		extUUID := row["extent_uuid"]

		if len(statusFilters) > 0 && !matchIntFilters(row["status"].(int), statusFilters) {
			continue
		}

		fmt.Printf("%v\n", extUUID)
		printRow("\t", row)
		fmt.Printf("\n")
	}

	if err := iter.Close(); err != nil {
		fmt.Printf("list_consumer_groups: iterator error: %v\n", err)
	}

	return nil
}

func list_consumer_group_extents(c *cli.Context, mc *MetadataClient) error {

	cql := "SELECT * FROM consumer_group_extents"

	var where []string
	var allowFiltering = false

	if cgMatch := multiMatchWhereClause("consumer_group_uuid", c.StringSlice("consumergroup")); len(cgMatch) > 0 {
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

	for {
		var row = make(map[string]interface{})

		if !iter.MapScan(row) {
			break
		}

		cgUUID := row["consumer_group_uuid"]
		extUUID := row["extent_uuid"]

		if len(statusFilters) > 0 && !matchIntFilters(row["status"].(int), statusFilters) {
			continue
		}

		fmt.Printf("cg=%v ext=%v\n", cgUUID, extUUID)
		printRow("\t", row)
		fmt.Printf("\n")
	}

	if err := iter.Close(); err != nil {
		fmt.Printf("list_consumer_groups: iterator error: %v\n", err)
	}

	return nil
}

func list_store_extents(c *cli.Context, mc *MetadataClient) error {

	cql := "SELECT * FROM store_extents"

	var where []string
	var allowFiltering = false

	if extMatch := multiMatchWhereClause("extent_uuid", c.StringSlice("extent")); len(extMatch) > 0 {
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

	for {
		var row = make(map[string]interface{})

		if !iter.MapScan(row) {
			break
		}

		storeUUID := row["store_uuid"]
		extUUID := row["extent_uuid"]

		if len(statusFilters) > 0 && !matchIntFilters(row["status"].(int), statusFilters) {
			continue
		}

		fmt.Printf("store=%v ext=%v\n", storeUUID, extUUID)
		printRow("\t", row)
		fmt.Printf("\n")
	}

	if err := iter.Close(); err != nil {
		fmt.Printf("list_store_extents: iterator error: %v\n", err)
	}

	return nil
}

func list_operations(c *cli.Context, mc *MetadataClient) error {

	if c.NArg() == 0 {
		return fmt.Errorf("specify UUIDs to search for")
	}

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
		fmt.Printf("list_operations: iterator error: %v\n", err)
	}

	return nil
}
