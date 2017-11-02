package main

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/urfave/cli"
)

func fixsmartretry(c *cli.Context) error {

	mc, err := newMetadataClient()

	if err != nil {
		fmt.Errorf("newMetadataClient error: %v", err)
		return nil
	}

	defer mc.Close()

	if !c.IsSet("all") && c.NArg() == 0 {
		fmt.Printf("fix smartretry: specify consumer-group or '-all'\n")
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

	var uuid gocql.UUID                           // uuid
	var destinationUUID gocql.UUID                // uuid
	var name string                               // text
	var status int                                // int
	var lockTimeoutSeconds int                    // int
	var maxDeliveryCount int                      // int
	var skipOlderMessagesSeconds int              // int
	var deadLetterQueueDestinationUUID gocql.UUID // uuid
	var ownerEmail string                         // text
	var startFrom int64                           // bigint
	var isMultiZone bool                          // boolean
	var activeZone string                         // text
	var zoneConfigs []map[string]interface{}      // frozen<list<frozen<consumer_group_zone_config>>>
	var delaySeconds int                          // int
	var options = make(map[string]string)         // options map<text, text>

	var nUpdated, nErrors, nDeleted int

	iter := mc.session.Query(cqlReadCG).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: 16 /*mc.retries*/}).Iter()

	for iter.Scan(&uuid, &destinationUUID, &name, &status, &lockTimeoutSeconds, &maxDeliveryCount, &skipOlderMessagesSeconds,
		&deadLetterQueueDestinationUUID, &ownerEmail, &startFrom, &isMultiZone, &activeZone, &zoneConfigs, &delaySeconds, &options) {

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
			uuid, destinationUUID, name, status, lockTimeoutSeconds, maxDeliveryCount, skipOlderMessagesSeconds,
			deadLetterQueueDestinationUUID, ownerEmail, startFrom, isMultiZone, activeZone, zoneConfigs, delaySeconds, options,
			uuid)

		batch.Query(cqlUpdateCGByName,
			uuid, destinationUUID, name, status, lockTimeoutSeconds, maxDeliveryCount, skipOlderMessagesSeconds,
			deadLetterQueueDestinationUUID, ownerEmail, startFrom, isMultiZone, activeZone, zoneConfigs, delaySeconds, options,
			destinationUUID, name)

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

func fixdestuuid(c *cli.Context) error {

	mc, err := newMetadataClient()

	if err != nil {
		fmt.Errorf("newMetadataClient error: %v", err)
		return nil
	}

	defer mc.Close()

	if !c.IsSet("all") && c.NArg() == 0 {
		fmt.Printf("fix destuuid: specify consumer-group or '-all'\n")
		return nil
	}

	verbose := c.Bool("verbose")

	cqlReadCG := "SELECT uuid, consumer_group.status, consumer_group.destination_uuid, destination_uuid FROM consumer_groups"

	if c.NArg() > 0 {
		cqlReadCG += fmt.Sprintf(" WHERE uuid=%s;", c.Args()[0])
	}

	cqlUpdateCG := "UPDATE consumer_groups SET destination_uuid = ? WHERE uuid = ?"

	var uuid gocql.UUID
	var destUUID gocql.UUID
	var destUUIDCol gocql.UUID
	var status int

	var nUpdated, nDeleted, nSkipped, nErrors int

	iter := mc.session.Query(cqlReadCG).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: 16 /*mc.retries*/}).Iter()

	for iter.Scan(&uuid, &status, &destUUID, &destUUIDCol) {

		if status == int(shared.ConsumerGroupStatus_DELETED) {

			if verbose {
				fmt.Printf("%v: deleted\n", uuid)
			}

			nDeleted++
			continue
		}

		if destUUIDCol == destUUID {

			if verbose {
				fmt.Printf("%v: skipped\n", uuid)
			}

			nSkipped++
			continue
		}

		err := mc.session.Query(cqlUpdateCG, destUUID, uuid).Exec()

		if err != nil {
			fmt.Printf("%v: error=%v\n", uuid, err)
			nErrors++
			continue
		}

		if verbose {
			fmt.Printf("%v: fixed\n", uuid)
		}

		nUpdated++
	}

	if err := iter.Close(); err != nil {
		fmt.Printf("error from query '%s' error: %v\n", cqlReadCG, err)
	}

	fmt.Printf("updated 'destination_uuid' column in 'consumer_groups' for %d rows (%d errors, %d skipped, %d deleted)\n",
		nUpdated, nErrors, nSkipped, nDeleted)

	return nil
}

func fixdestuuidttl(c *cli.Context) error {

	mc, err := newMetadataClient()

	if err != nil {
		fmt.Errorf("newMetadataClient error: %v", err)
		return nil
	}

	defer mc.Close()

	if !c.IsSet("all") && c.NArg() == 0 {
		fmt.Printf("fix destuuid: specify consumer-group or '-all'\n")
		return nil
	}

	verbose := c.Bool("verbose")

	cqlReadCG := "SELECT uuid, TTL(consumer_group), TTL(destination_uuid) FROM consumer_groups"

	if c.NArg() > 0 {
		cqlReadCG += fmt.Sprintf(" WHERE uuid=%s;", c.Args()[0])
	}

	cqlUpdateCG := "UPDATE consumer_groups SET destination_uuid = ? WHERE uuid = ?"

	var uuid gocql.UUID            // uuid
	var destinationUUID gocql.UUID // uuid

	var nUpdated, nErrors, nSkipped int

	iter := mc.session.Query(cqlReadCG).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: 16 /*mc.retries*/}).Iter()

	for iter.Scan(&uuid, &destinationUUID) {

		err := mc.session.Query(cqlUpdateCG, destinationUUID, uuid).Exec()

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

	fmt.Printf("updated 'destination_uuid' column in 'consumer_groups' for %d rows (%d errors, %d skipped)\n", nUpdated, nErrors, nSkipped)
	return nil
}
