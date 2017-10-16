package main

import (
	"fmt"
	"sort"

	"github.com/urfave/cli"
)

func printRow(prefix string, row map[string]interface{}) {

	var keys []string
	for k := range row {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		fmt.Printf("%s%v:", prefix, k)
		switch row[k].(type) {
		case map[string]interface{}:
			fmt.Printf("\n")
			printRow(prefix+"\t", row[k].(map[string]interface{}))
		case map[string]map[string]interface{}:
			fmt.Printf("\n")
			printRow(prefix+"\t", row[k].(map[string]interface{}))
		default:
			fmt.Printf(" %v\n", row[k])
		}
	}
}

func show_destination_by_path(c *cli.Context, mc *MetadataClient) error {

	if c.NArg() == 0 {
		return fmt.Errorf("destination path not specified")
	}

	path := c.Args()[0]
	cql := fmt.Sprintf("SELECT * FROM destinations_by_path WHERE path='%v' ALLOW FILTERING", path)

	row, err := mc.QueryRow(cql)

	if err != nil {
		fmt.Printf("show_destination_by_path: '%v': %v\n", cql, err)
		return nil
	}

	fmt.Printf("destinations_by_path[%v]:\n", path)
	printRow("\t", row)

	return nil
}

func show_destination(c *cli.Context, mc *MetadataClient) error {

	if c.NArg() == 0 {
		return fmt.Errorf("destination uuid not specified")
	}

	uuid := c.Args()[0]
	cql := fmt.Sprintf("SELECT * FROM destinations WHERE uuid=%v", uuid)

	row, err := mc.QueryRow(cql)

	if err != nil {
		fmt.Printf("show_destination: '%v': %v\n", cql, err)
		return nil
	}

	fmt.Printf("destinations[%v]:\n", uuid)
	printRow("\t", row)

	return nil
}

func show_consumergroup(c *cli.Context, mc *MetadataClient) error {

	if c.NArg() == 0 {
		return fmt.Errorf("consumer-group uuid not specified")
	}

	uuid := c.Args()[0]
	cql := fmt.Sprintf("SELECT * FROM consumer_groups WHERE uuid=%v", uuid)

	row, err := mc.QueryRow(cql)

	if err != nil {
		fmt.Printf("show_consumergroup: '%v': %v\n", cql, err)
		return nil
	}

	fmt.Printf("consumergroups[%v]:\n", uuid)
	printRow("\t", row)

	return nil
}

func show_extent(c *cli.Context, mc *MetadataClient) error {

	if c.NArg() == 0 {
		return fmt.Errorf("extent uuid not specified")
	}

	uuid := c.Args()[0]
	cql := fmt.Sprintf("SELECT * FROM destination_extents WHERE extent_uuid=%v ALLOW FILTERING", uuid)

	row, err := mc.QueryRow(cql)

	if err != nil {
		fmt.Printf("show_extent: '%v': %v\n", cql, err)
		return nil
	}

	fmt.Printf("destination_extents[%v]:\n", uuid)
	printRow("\t", row)

	return nil
}

func show_cgextent(c *cli.Context, mc *MetadataClient) error {

	if c.NArg() < 2 {
		return fmt.Errorf("cg/extent uuid not specified")
	}

	cgUUID := c.Args()[0]
	extUUID := c.Args()[1]
	cql := fmt.Sprintf("SELECT * FROM consumer_group_extents WHERE consumer_group_uuid=%v AND extent_uuid=%v", cgUUID, extUUID)

	row, err := mc.QueryRow(cql)

	if err != nil {
		fmt.Printf("show_cgextent: '%v': %v\n", cql, err)
		return nil
	}

	fmt.Printf("consumer_group_extent[cg=%v, ext=%v]:\n", cgUUID, extUUID)
	printRow("\t", row)

	return nil
}
