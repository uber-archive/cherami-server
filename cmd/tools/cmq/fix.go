package main

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/urfave/cli"
)

func fixsmartretry(c *cli.Context, mc *MetadataClient) error {

	if !c.IsSet("all") && c.NArg() == 0 {
		fmt.Printf("fixsmartretry: specify consumer-group or '-all'\n")
		return nil
	}

	verbose := c.Bool("verbose")

	cqlReadCG := "SELECT " +
		"consumer_group.uuid, " +
		"consumer_group.destination_uuid, " +
		"consumer_group.name, " +
		"consumer_group.status, " +
		"consumer_group.lock_timeout_seconds, " +
		"consumer_group.max_delivery_count, " +
		"consumer_group.skip_older_messages_seconds, " +
		"consumer_group.dead_letter_queue_destination_uuid, " +
		"consumer_group.owner_email, " +
		"consumer_group.start_from, " +
		"consumer_group.is_multi_zone, " +
		"consumer_group.active_zone, " +
		"consumer_group.zone_configs, " +
		"consumer_group.delay_seconds, " +
		"consumer_group.options " +
		"FROM consumer_groups"

	if c.NArg() > 0 {
		cqlReadCG += fmt.Sprintf(" WHERE uuid=%s;", c.Args()[0])
	}

	cqlTypeCG := "{" +
		"uuid: ?," +
		"destination_uuid: ?," +
		"name: ?," +
		"status: ?," +
		"lock_timeout_seconds: ?," +
		"max_delivery_count: ?," +
		"skip_older_messages_seconds: ?," +
		"dead_letter_queue_destination_uuid: ?," +
		"owner_email: ?," +
		"start_from: ?," +
		"is_multi_zone: ?," +
		"active_zone: ?," +
		"zone_configs: ?," +
		"delay_seconds: ?," +
		"options: ?" +
		"}"

	cqlUpdateCG := "UPDATE consumer_groups SET consumer_group = " + cqlTypeCG + " WHERE uuid = ?"
	cqlUpdateCGByName := "UPDATE consumer_groups_by_name SET consumer_group = " + cqlTypeCG + " WHERE destination_uuid = ? AND name = ?"

	var uuid gocql.UUID                               // uuid
	var destination_uuid gocql.UUID                   // uuid
	var name string                                   // text
	var status int                                    // int
	var lock_timeout_seconds int                      // int
	var max_delivery_count int                        // int
	var skip_older_messages_seconds int               // int
	var dead_letter_queue_destination_uuid gocql.UUID // uuid
	var owner_email string                            // text
	var start_from int64                              // bigint
	var is_multi_zone bool                            // boolean
	var active_zone string                            // text
	var zone_configs []map[string]interface{}         // frozen<list<frozen<consumer_group_zone_config>>>
	var delay_seconds int                             // int
	var options = make(map[string]string)             // options map<text, text>

	var nUpdated, nErrors, nDeleted int

	iter := mc.session.Query(cqlReadCG).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: 16 /*mc.retries*/}).Iter()

	for iter.Scan(&uuid, &destination_uuid, &name, &status, &lock_timeout_seconds, &max_delivery_count, &skip_older_messages_seconds,
		&dead_letter_queue_destination_uuid, &owner_email, &start_from, &is_multi_zone, &active_zone, &zone_configs, &delay_seconds, &options) {

		if status == int(shared.ConsumerGroupStatus_DELETED) {
			nDeleted++
			continue
		}

		if options == nil {
			options = make(map[string]string) // options map<text, text>
		}

		options["enable_smart_retry"] = "true" // update 'smartretry' option to 'true'

		batch := mc.session.NewBatch(gocql.LoggedBatch)

		batch.Query(cqlUpdateCG,
			uuid, destination_uuid, name, status, lock_timeout_seconds, max_delivery_count, skip_older_messages_seconds,
			dead_letter_queue_destination_uuid, owner_email, start_from, is_multi_zone, active_zone, zone_configs, delay_seconds, options,
			uuid)

		batch.Query(cqlUpdateCGByName,
			uuid, destination_uuid, name, status, lock_timeout_seconds, max_delivery_count, skip_older_messages_seconds,
			dead_letter_queue_destination_uuid, owner_email, start_from, is_multi_zone, active_zone, zone_configs, delay_seconds, options,
			destination_uuid, name)

		err := mc.session.ExecuteBatch(batch)

		if err != nil {
			fmt.Printf("%v: error=%v\n", uuid, err)
			nErrors++
			continue
		}

		if verbose {
			fmt.Printf("%v: done\n", uuid)
		}

		nUpdated++
	}

	if err := iter.Close(); err != nil {
		fmt.Printf("error from query '%s' error: %v\n", cqlReadCG, err)
	}

	fmt.Printf("updated smartretry for %d rows (%d errors, %d deleted)\n", nUpdated, nErrors, nDeleted)
	return nil
}
