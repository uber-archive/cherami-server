// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package main

import (
	"os"
	"time"

	"github.com/codegangsta/cli"
	"github.com/uber/cherami-server/common"
	lib "github.com/uber/cherami-server/tools/cli"
	com "github.com/uber/cherami-server/tools/common"
)

const (
	usageCGStartTime                  = `Consume messages newer than this time in unix-nanos (default: Now; ie, consume no previously published messages)`
	usageCGLockTimeoutSeconds         = `Acknowledgement timeout for prefetched/received messages`
	usageCGMaxDeliveryCount           = `Max number of times a message is delivered before it is sent to the DLQ (dead-letter queue)`
	usageCGSkipOlderMessagesInSeconds = `Skip messages older than this duration, in seconds ('0' to skip none)`
	usageCGDelaySeconds               = `Delay, in seconds, to defer all messages by`
	usageCGOwnerEmail                 = "Owner email"
	usageCGZoneConfig                 = "Zone configs for multi-zone CG. For each zone, specify \"Zone,PreferedActiveZone\"; ex: \"dca1a,false\""
)

func main() {
	app := cli.NewApp()
	cliHelper := common.NewCliHelper()
	// SetCanonicalZones. For now just "zone1", "zone2", "z1"
	// and "z2" are valid and they map to "zone1" and "zone2"
	// canonical zones.
	// We can use this API to set any valid zones
	cliHelper.SetCanonicalZones(map[string]string{
		"zone1": "zone1",
		"zone2": "zone2",
		"z1":    "zone1",
		"z2":    "zone2",
	})
	app.Name = "cherami"
	app.Usage = "A command-line tool for cherami users"
	app.Version = "1.1.10"
	app.Flags = []cli.Flag{
		cli.BoolTFlag{
			Name:  "hyperbahn",
			Usage: "Use hyperbahn",
		},
		cli.IntFlag{
			Name:  "timeout, t",
			Value: 60,
			Usage: "Timeout in seconds",
		},
		cli.StringFlag{
			Name:  "env",
			Value: "staging",
			Usage: "Deployment to connect to. By default connects to staging. Use \"prod\" to connect to production",
		},
		cli.StringFlag{
			Name:   "hyperbahn_bootstrap_file, hbfile",
			Value:  "/etc/uber/hyperbahn/hosts.json",
			Usage:  "hyperbahn boostrap file",
			EnvVar: "HYPERBAHN_BOOSTRAP_FILE",
		},
		cli.StringFlag{
			Name:   "hostport",
			Value:  "",
			Usage:  "Host:port for frontend host",
			EnvVar: "CHERAMI_FRONTEND_HOSTPORT",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:    "create",
			Aliases: []string{"c", "cr"},
			Usage:   "create (destination | consumergroup)",
			Subcommands: []cli.Command{
				{
					Name:    "destination",
					Aliases: []string{"d", "dst", "dest"},
					Usage:   "create destination <path> [options]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "type, t",
							Value: "plain",
							Usage: "Type of the destination: 'plain', 'timer', or 'kafka'",
						},
						cli.IntFlag{
							Name:  "consumed_messages_retention, cr",
							Value: com.DefaultConsumedMessagesRetention,
							Usage: "Consumed messages retention period specified in seconds. Default is 1 hour.",
						},
						cli.IntFlag{
							Name:  "unconsumed_messages_retention, ur",
							Value: com.DefaultUnconsumedMessagesRetention,
							Usage: "Unconsumed messages retention period specified in seconds. Default is two hours.",
						},
						cli.StringFlag{
							Name:  "checksum_option, co",
							Value: "crcIEEE",
							Usage: "Checksum_options, can be one of the crcIEEE, md5",
						},
						cli.StringFlag{
							Name:  "owner_email, oe",
							Value: cliHelper.GetDefaultOwnerEmail(),
							Usage: "The owner's email. Default is the $USER@uber.com",
						},
						cli.StringSliceFlag{
							Name:  "zone_config, zc",
							Usage: "Zone configs for multi_zone destinations. Format for each zone should be \"Zone,AllowPublish,AllowConsume,ReplicaCount\". Ex: \"sjc1a,true,true,3\"",
						},
						cli.StringFlag{
							Name:  "kafka_cluster, kc",
							Usage: "Name of the Kafka cluster to attach to",
						},
						cli.StringSliceFlag{
							Name:  "kafka_topics, kt",
							Usage: "List of kafka topics to subscribe to. Use multiple times, e.g. \"-kafka_topics topic_a -kafka_topics topic_b\"",
						},
					},
					Action: func(c *cli.Context) {
						lib.CreateDestination(c, cliHelper)
					},
				},
				{
					Name:    "consumergroup",
					Aliases: []string{"c", "cg"},
					Usage:   "create consumergroup [<destination_path>|<DLQ_UUID>] <consumer_group_name> [options]",
					Flags: []cli.Flag{
						cli.IntFlag{
							Name:  "start_time, s",
							Value: int(time.Now().Unix()),
							Usage: usageCGStartTime,
						},
						cli.IntFlag{
							Name:  "lock_timeout_seconds, l",
							Value: com.DefaultLockTimeoutSeconds,
							Usage: usageCGLockTimeoutSeconds,
						},
						cli.IntFlag{
							Name:  "max_delivery_count, m",
							Value: com.DefaultMaxDeliveryCount,
							Usage: usageCGMaxDeliveryCount,
						},
						cli.IntFlag{
							Name:  "skip_older_messages_in_seconds, k",
							Value: com.DefaultSkipOlderMessageSeconds,
							Usage: usageCGSkipOlderMessagesInSeconds,
						},
						cli.IntFlag{
							Name:  "delay_seconds, d",
							Value: com.DefaultDelayMessageSeconds,
							Usage: usageCGDelaySeconds,
						},
						cli.StringFlag{
							Name:  "owner_email, oe",
							Value: cliHelper.GetDefaultOwnerEmail(),
							Usage: usageCGOwnerEmail,
						},
						cli.StringSliceFlag{
							Name:  "zone_config, zc",
							Usage: usageCGZoneConfig,
						},
					},
					Action: func(c *cli.Context) {
						lib.CreateConsumerGroup(c, cliHelper)
					},
				},
			},
		},
		{
			Name:    "show",
			Aliases: []string{"s", "sh", "info", "i"},
			Usage:   "show (destination | consumergroup | message | dlq | cgBacklog)",
			Subcommands: []cli.Command{
				{
					Name:    "destination",
					Aliases: []string{"d", "dst", "dest"},
					Usage:   "show destination <name>",
					Flags: []cli.Flag{
						cli.BoolFlag{
							Name:  "showcg, cg",
							Usage: "show consumer groups for the destination",
						},
					},
					Action: func(c *cli.Context) {
						lib.ReadDestination(c)
					},
				},
				{
					Name:    "consumergroup",
					Aliases: []string{"c", "cg"},
					Usage:   "show consumergroup (<consumer_group_uuid> | <destination_path> <consumer_group_name>)",
					Action: func(c *cli.Context) {
						lib.ReadConsumerGroup(c)
					},
				},
				{
					Name:    "message",
					Aliases: []string{"m"},
					Usage:   "show message <extent_uuid> <address>",
					Action: func(c *cli.Context) {
						lib.ReadMessage(c)
					},
				},
				{
					Name:    "dlq",
					Aliases: []string{"dl"},
					Usage:   "show dlq <uuid>",
					Action: func(c *cli.Context) {
						lib.ReadDlq(c)
					},
				},
				{
					Name:    "cgBacklog",
					Aliases: []string{"cgb", "cb"},
					Usage:   "show cgBacklog (<consumer_group_uuid> | <destination_path> <consumer_group_name>)",
					Action: func(c *cli.Context) {
						lib.ReadCgBacklog(c)
					},
				},
			},
		},
		{
			Name:    "update",
			Aliases: []string{"u"},
			Usage:   "update (destination | consumergroup)",
			Subcommands: []cli.Command{
				{
					Name:    "destination",
					Aliases: []string{"d", "dst", "dest"},
					Usage:   "update destination <name>",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "status, s",
							Usage: "status: enabled | disabled | sendonly | recvonly",
						},
						cli.IntFlag{
							Name:  "consumed_messages_retention, cr",
							Usage: "Consumed messages retention period specified in seconds. Default is one hour.",
						},
						cli.IntFlag{
							Name:  "unconsumed_messages_retention, ur",
							Usage: "Unconsumed messages retention period specified in seconds. Default is two hours.",
						},
						cli.StringFlag{
							Name:  "checksum_option, co",
							Usage: "Checksum_options, can be one of the crcIEEE, md5",
						},
						cli.StringFlag{
							Name:  "owner_email, oe",
							Usage: "The updated owner's email",
						},
						cli.StringSliceFlag{
							Name:  "zone_config, zc",
							Usage: "Zone configs for multi_zone destinations. Format for each zone should be \"ZoneName,AllowPublish,AllowConsume,ReplicaCount\". For example: \"zone1,true,true,3\"",
						},
					},
					Action: func(c *cli.Context) {
						lib.UpdateDestination(c, cliHelper)
					},
				},
				{
					Name:    "consumergroup",
					Aliases: []string{"c", "cg"},
					Usage:   "update consumergroup (<consumer_group_uuid> | <destination_path> <consumer_group_name>)",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "status, s",
							Usage: "status: enabled | disabled",
						},
						cli.IntFlag{
							Name:  "lock_timeout_seconds, l",
							Usage: usageCGLockTimeoutSeconds,
						},
						cli.IntFlag{
							Name:  "max_delivery_count, m",
							Usage: usageCGMaxDeliveryCount,
						},
						cli.IntFlag{
							Name:  "skip_older_messages_in_seconds, k",
							Usage: usageCGSkipOlderMessagesInSeconds,
						},
						cli.IntFlag{
							Name:  "delay_seconds, d",
							Usage: usageCGDelaySeconds,
						},
						cli.StringFlag{
							Name:  "owner_email, oe",
							Usage: usageCGOwnerEmail,
						},
						cli.StringFlag{
							Name:  "active_zone, az",
							Usage: "The updated active zone",
						},
						cli.StringSliceFlag{
							Name:  "zone_config, zc",
							Usage: "Zone configs for multi_zone consumer group. Format for each zone should be \"ZoneName,PreferedActiveZone\". For example: \"zone1,false\"",
						},
					},
					Action: func(c *cli.Context) {
						lib.UpdateConsumerGroup(c, cliHelper)
					},
				},
			},
		},
		{
			Name:    "delete",
			Aliases: []string{"d"},
			Usage:   "delete (destination | consumergroup)",
			Subcommands: []cli.Command{
				{
					Name:    "destination",
					Aliases: []string{"d", "dst", "dest"},
					Usage:   "delete destination <name>",
					Action: func(c *cli.Context) {
						lib.DeleteDestination(c)
						println("deleted destination: ", c.Args().First())
					},
				},
				{
					Name:    "consumergroup",
					Aliases: []string{"c", "cg"},
					Usage:   "delete consumergroup [<destination_path>|<DLQ_UUID>] <consumer_group_name>",
					Action: func(c *cli.Context) {
						lib.DeleteConsumerGroup(c)
						println("deleted consumergroup: ", c.Args()[0], c.Args()[1])
					},
				},
			},
		},
		{
			Name:    "list",
			Aliases: []string{"l", "ls"},
			Usage:   "list (destination | consumergroup)",
			Subcommands: []cli.Command{
				{
					Name:    "destination",
					Aliases: []string{"d", "dst", "dest"},
					Usage:   "list destination [options]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "prefix, pf",
							Value: "/",
							Usage: "only show destinations of prefix",
						},
						cli.StringFlag{
							Name:  "status, s",
							Value: "",
							Usage: "status: enabled | disabled | sendonly | recvonly, if empty, return all",
						},
					},
					Action: func(c *cli.Context) {
						lib.ListDestinations(c)
					},
				},
				{
					Name:    "consumergroup",
					Aliases: []string{"c", "cg"},
					Usage:   "list consumergroup <destination_path> [<consumer_group>]",
					Action: func(c *cli.Context) {
						lib.ListConsumerGroups(c)
					},
				},
			},
		},
		{
			Name:    "publish",
			Aliases: []string{"p", "pub", "w", "write"},
			Usage:   "publish <destination_name>",
			Action: func(c *cli.Context) {
				lib.Publish(c)
			},
		},
		{
			Name:    "consume",
			Aliases: []string{"sub", "r", "read"},
			Usage:   "consume <destination_name> <consumer_group_name> [options]",
			Flags: []cli.Flag{
				cli.BoolTFlag{
					Name:  "autoack, a",
					Usage: "automatically ack each message as it's printed",
				},
				cli.IntFlag{
					Name:  "prefetch_count, p",
					Value: 1,
					Usage: "prefetch count",
				},
			},
			Action: func(c *cli.Context) {
				lib.Consume(c)
			},
		},
		{
			Name:    "merge_dlq",
			Aliases: []string{"mdlq"},
			Usage:   "merge_dlq  (<consumer_group_uuid> | <destination_path> <consumer_group_name>)",

			Action: func(c *cli.Context) {
				lib.MergeDLQForConsumerGroup(c)
			},
		},
		{
			Name:    "purge_dlq",
			Aliases: []string{"pdlq"},
			Usage:   "purge_dlq  (<consumer_group_uuid> | <destination_path> <consumer_group_name>)",

			Action: func(c *cli.Context) {
				lib.PurgeDLQForConsumerGroup(c)
			},
		},
	}

	app.Run(os.Args)
}
