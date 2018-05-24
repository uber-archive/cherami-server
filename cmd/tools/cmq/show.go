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

func showDestinationByPath(c *cli.Context) error {

	if c.NArg() == 0 {
		return fmt.Errorf("destination path not specified")
	}

	mc, err := newMetadataClient()

	if err != nil {
		fmt.Errorf("newMetadataClient error: %v", err)
		return nil
	}

	out := cmqOutputWriter(cliContext.StringSlice(`output`))
	defer out.close()

	for _, path := range c.Args() {

		cql := fmt.Sprintf("SELECT * FROM destinations_by_path WHERE path='%v' ALLOW FILTERING", path)

		row, err := mc.QueryRow(cql)

		if err != nil {
			fmt.Printf("showDestinationByPath: '%v': %v\n", cql, err)
			return nil
		}

		fmt.Printf("destinations_by_path[%v]:\n", path)
		printRow("\t", row)
	}

	return nil
}

func showDestination(c *cli.Context) error {

	if c.NArg() == 0 {
		return fmt.Errorf("destination uuid not specified")
	}

	mc, err := newMetadataClient()

	if err != nil {
		fmt.Errorf("newMetadataClient error: %v", err)
		return nil
	}

	out := cmqOutputWriter(cliContext.StringSlice(`output`))
	defer out.close()

	for _, uuid := range c.Args() {

		cql := fmt.Sprintf("SELECT * FROM destinations WHERE uuid=%v", uuid)

		row, err := mc.QueryRow(cql)

		if err != nil {
			fmt.Printf("showDestination: '%v': %v\n", cql, err)
			return nil
		}

		// fmt.Printf("destinations[%v]:\n", uuid)
		// printRow("\t", row)
		out.Destination(row, "")
	}

	return nil
}

func showConsumerGroup(c *cli.Context) error {

	if c.NArg() == 0 {
		return fmt.Errorf("consumer-group uuid not specified")
	}

	mc, err := newMetadataClient()

	if err != nil {
		fmt.Errorf("newMetadataClient error: %v", err)
		return nil
	}

	out := cmqOutputWriter(cliContext.StringSlice(`output`))
	defer out.close()

	for _, uuid := range c.Args() {

		cql := fmt.Sprintf("SELECT * FROM consumer_groups WHERE uuid=%v", uuid)

		row, err := mc.QueryRow(cql)

		if err != nil {
			fmt.Printf("showConsumerGroup: '%v': %v\n", cql, err)
			return nil
		}

		// fmt.Printf("consumergroups[%v]:\n", uuid)
		// printRow("\t", row)
		out.ConsumerGroup(row, "")
	}

	return nil
}

func showExtent(c *cli.Context) error {

	if c.NArg() == 0 {
		return fmt.Errorf("extent uuid not specified")
	}

	mc, err := newMetadataClient()

	if err != nil {
		fmt.Errorf("newMetadataClient error: %v", err)
		return nil
	}

	out := cmqOutputWriter(cliContext.StringSlice(`output`))
	defer out.close()

	for _, uuid := range c.Args() {

		cql := fmt.Sprintf("SELECT * FROM destination_extents WHERE extent_uuid=%v ALLOW FILTERING", uuid)

		row, err := mc.QueryRow(cql)

		if err != nil {
			fmt.Printf("showExtent: '%v': %v\n", cql, err)
			return nil
		}

		// fmt.Printf("destination_extents[%v]:\n", uuid)
		// printRow("\t", row)
		out.Extent(row, "")
	}

	return nil
}

func showCGExtent(c *cli.Context) error {

	if c.NArg() < 2 {
		return fmt.Errorf("cg/extent uuid not specified")
	}
	mc, err := newMetadataClient()

	if err != nil {
		fmt.Errorf("newMetadataClient error: %v", err)
		return nil
	}

	out := cmqOutputWriter(cliContext.StringSlice(`output`))
	defer out.close()

	cgUUID := c.Args()[0]
	extUUID := c.Args()[1]
	cql := fmt.Sprintf("SELECT * FROM consumer_group_extents WHERE consumer_group_uuid=%v AND extent_uuid=%v", cgUUID, extUUID)

	row, err := mc.QueryRow(cql)

	if err != nil {
		fmt.Printf("showCGExtent: '%v': %v\n", cql, err)
		return nil
	}

	// fmt.Printf("consumer_group_extent[cg=%v, ext=%v]:\n", cgUUID, extUUID)
	// printRow("\t", row)
	out.ConsumerGroupExtent(row, "")

	return nil
}
