package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/urfave/cli"
	"os"
	"strings"
	"time"
)

func repair(c *cli.Context) error {
	if c.NArg() < 2 {
		fmt.Printf("dest-uuid and cg-uuid not specified\n")
		return nil
	}

	mc, err := newMetadataClient()
	if err != nil {
		fmt.Errorf("newMetadataClient error: %v", err)
		return nil
	}
	consistency := gocql.All
	if consistencyStr := cliContext.String("consistency"); consistencyStr != "" {
		consistency = gocql.ParseConsistency(consistencyStr)
	}
	mc.consistency = consistency

	defer mc.Close()

	destUUID := c.Args()[0]
	cgUUID := c.Args()[1]

	cgMon := newCGWatch(mc, destUUID, cgUUID)
	cgMon.refreshMetadata()

	plan := generatePlan(cgMon.extentMap, cgUUID)
	for _, p := range plan {
		fmt.Println(p)
	}
	fmt.Printf("Please confirm the deletion plan above.\n" +
		"You should confirm that the consumer_group_uuid match ONLY the consumer_group_uuid provided")
	if askForConfirmation("Delete now") == false {
		return nil
	}

	run := c.Bool("run")
	timeout := c.Duration("timeout")
	deleteMISSING(mc, plan, run, timeout)
	return nil
}

// askForConfirmation asks the user for confirmation. A user must type in "yes" or "no" and
// then press enter. It has fuzzy matching, so "y", "Y", "yes", "YES", and "Yes" all count as
// confirmations. If the input is not recognized, it will ask again. The function does not return
// until it gets a valid response from the user.
// Reference: https://gist.github.com/r0l1/3dcbb0c8f6cfe9c66ab8008f55f8f28b
func askForConfirmation(s string) bool {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("%s [y/n]: ", s)

		response, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Error reading from stdin", err.Error())
			os.Exit(1)
		}

		response = strings.ToLower(strings.TrimSpace(response))

		if response == "y" || response == "yes" {
			return true
		} else if response == "n" || response == "no" {
			return false
		}
	}
}

// generatePlan creates the cql commands for deletion.
func generatePlan(extentMap map[string]*extentInfo, consumergroupUUID string) []string {
	var output []string
	for extentUUID, extentInfo := range extentMap {
		if extentInfo.extStatus == extMissing {
			output = append(output, fmt.Sprintf("DELETE FROM consumer_group_extents WHERE consumer_group_uuid=%s AND extent_uuid=%s;", consumergroupUUID, extentUUID))
		}
	}
	return output
}

// deleteMISSING deletes the MISSING extents from cassandra.
func deleteMISSING(mc *metadataClient, cqls []string, run bool, timeout time.Duration) {
	if !run {
		fmt.Printf("dry run only so no deleting performed. set -run to execute plan.")
		return
	}

	deleted := make([]string, 0, len(cqls))
	defer func() {
		fmt.Printf("Deleted %d MISSING extents", len(deleted))
	}()
	for _, cql := range cqls {
		ctx, _ := context.WithTimeout(context.Background(), timeout)
		if err := mc.session.Query(cql).WithContext(ctx).Exec(); err != nil {
			fmt.Printf("Error deleting consumer_group_extents: %s\n", err)
			return
		}
		deleted = append(deleted, cql)
	}
}
