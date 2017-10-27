package main

import (
	"fmt"
	// "reflect"
	// "sort"
	// "time"
	"encoding/json"

	"github.com/gocql/gocql"
	"github.com/urfave/cli"
)

/*
func test(c *cli.Context, mc *metadataClient) error {

	if c.NArg() < 3 {
		return fmt.Errorf("specify table and column")
	}

	// 'store_extents' table:
	var writeTimeMs int64

	tbl := c.Args()[0]
	row := c.Args()[1]
	col := c.Args()[2]

	cql := fmt.Sprintf(`SELECT WRITETIME(%s) FROM %s WHERE %s`, col, tbl, row)

	err := mc.session.Query(cql).Scan(&writeTimeMs)

	if err != nil {
		fmt.Printf("ERROR running query '%s': %v\n", cql, err)
	}

	fmt.Printf("tbl:%s row:%s col:%s WRITETIME=%v\n", tbl, row, col, time.Unix(0, writeTimeMs*1000))
	return nil
}
*/

/*
func test(c *cli.Context, mc *metadataClient) error {
	if c.NArg() == 0 {
		fmt.Printf("uuid not specified")
		return nil
	}

	uuid := c.Args()[0]
	cql := fmt.Sprintf("SELECT * FROM destinations WHERE uuid=%v ALLOW FILTERING", uuid)

	row, err := mc.QueryRow(cql)

	if err != nil {
		fmt.Printf("test: '%v': %v\n", cql, err)
		return nil
	}

	fmt.Printf("destinations[%v]:\n", uuid)

	prefix := "\t"

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
			fmt.Printf("(%v) %v\n", reflect.TypeOf(row[k]), row[k])
		}
	}

	return nil
}
*/

/*
func rowString(row map[string]interface{}) string {

	for k, v := range row {
		switch v.(type) {
		}
	}
}
*/

func test(c *cli.Context, mc *metadataClient) error {

	cql := "SELECT * FROM store_extents"

	getIterator := func(cql string) (iter *gocql.Iter, close func() error) {

		iter = mc.session.Query(cql).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: 16 /*mc.retries*/}).Iter()
		close = func() (err error) {
			if err = iter.Close(); err != nil {
				fmt.Printf("ERROR from query '%v': %v\n", cql, err)
			}
			return
		}

		return
	}

	iter, close := getIterator(cql)
	defer close()

	for {
		row := make(map[string]interface{})

		if !iter.MapScan(row) {
			break
		}

		// extUUID := row["extent_uuid"]
		j, _ := json.Marshal(row)
		// fmt.Printf("%v, %v\n", string(j), e)

		fmt.Printf("INSERT INTO store_extents VALUES %v\n", string(j))
		break
	}

	fmt.Printf("\n")

	return nil
}
