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

	"github.com/codegangsta/cli"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/tools/admin"
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
	app.Usage = "A command-line tool for cherami developer, including debugging tool"
	app.Version = "1.1"
	app.Flags = []cli.Flag{
		cli.BoolTFlag{
			Name:  "hyperbahn",
			Usage: "use hyperbahn",
		},
		cli.IntFlag{
			Name:  "timeout, t",
			Value: 60,
			Usage: "timeout in seconds",
		},
		cli.StringFlag{
			Name:  "env",
			Value: "",
			Usage: "env to connect. By default connects to prod, use \"staging\" to connect to staging",
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
					Aliases: []string{"d", "dst"},
					Usage:   "create destination <path> [options]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "type, t",
							Value: "plain",
							Usage: "Type of the destination: 'plain' or 'timer'",
						},

						cli.IntFlag{
							Name:  "consumed_messages_retention, cr",
							Value: 3600,
							Usage: "Consumed messages retention period specified in seconds. Default is 1 hour.",
						},

						cli.IntFlag{
							Name:  "unconsumed_messages_retention, ur",
							Value: 7200,
							Usage: "Unconsumed messages retention period specified in seconds. Default is two hours.",
						},
						cli.StringFlag{
							Name:  "checksum_option, co",
							Value: "crcIEEE",
							Usage: "Checksum_options, can be one of the crcIEEE, md5",
						},
						cli.StringFlag{
							Name:  "owner_email, oe",
							Value: "",
							Usage: "The owner's email who commits the request. Default is the $USER@uber.com",
						},
					},
					Action: func(c *cli.Context) {
						admin.CreateDestination(c, cliHelper)
					},
				},
				{
					Name:    "consumergroup",
					Aliases: []string{"c", "cg"},
					Usage:   "create consumergroup <destination_path> <consumer_group_name> [options]",
					Flags: []cli.Flag{
						cli.IntFlag{
							Name:  "start_time, s",
							Value: 0,
							Usage: "Start this consumer group at this UNIX timestamp; by default we start at this Unix timestamp (seconds since 1970-1-1)",
						},
						cli.IntFlag{
							Name:  "lock_timeout_seconds, l",
							Value: 60,
							Usage: "Ack timeout for each message",
						},
						cli.IntFlag{
							Name:  "max_delivery_count, m",
							Value: 10,
							Usage: "Maximum delivery count for a message before it sents to dead-letter queue",
						},
						cli.IntFlag{
							Name:  "skip_older_messages_in_seconds, k",
							Value: 7200,
							Usage: "Skip messages older than this duration in seconds.",
						},
						cli.StringFlag{
							Name:  "owner_email, oe",
							Value: "",
							Usage: "The owner's email who commits the request. Default is the $USER@uber.com",
						},
					},
					Action: func(c *cli.Context) {
						admin.CreateConsumerGroup(c, cliHelper)
					},
				},
			},
		},
		{
			Name:    "show",
			Aliases: []string{"s", "sh", "info", "i"},
			Usage:   "show (destination | consumergroup | extent | storehost | message | dlq | cgAckID | cgqueue | destqueue | cgBacklog)",
			Subcommands: []cli.Command{
				{
					Name:    "destination",
					Aliases: []string{"d", "dst"},
					Usage:   "show destination <name>",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "showcg, sc",
							Value: "false",
							Usage: "show consumer group(false, true), default to false",
						},
					},
					Action: func(c *cli.Context) {
						admin.ReadDestination(c)
					},
				},
				{
					Name:    "consumergroup",
					Aliases: []string{"c", "cg"},
					Usage:   "show consumergroup (<consumer_group_uuid> | <destination_path> <consumer_group_name>)",
					Action: func(c *cli.Context) {
						admin.ReadConsumerGroup(c)
					},
				},
				{
					Name:    "extent",
					Aliases: []string{"e"},
					Usage:   "show extent <extent_uuid>",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "showcg, sc",
							Value: "false",
							Usage: "show consumer group(false, true), default to false",
						},
					},
					Action: func(c *cli.Context) {
						admin.ReadExtent(c)
					},
				},
				{
					Name:    "storehost",
					Aliases: []string{"s"},
					Usage:   "show storehost <storehostAddr>",
					Flags: []cli.Flag{
						cli.IntFlag{
							Name:  "top, tp",
							Value: 5,
							Usage: "show the top k heavy extents in this storehost",
						},
					},
					Action: func(c *cli.Context) {
						admin.ReadStoreHost(c)
					},
				},
				{
					Name:    "message",
					Aliases: []string{"m"},
					Usage:   "show message <extent_uuid> <address>",
					Action: func(c *cli.Context) {
						admin.ReadMessage(c)
					},
				},
				{
					Name:    "dlq",
					Aliases: []string{"dl"},
					Usage:   "show dlq <uuid>",
					Action: func(c *cli.Context) {
						admin.ReadDlq(c)
					},
				},
				{
					Name:    "cgAckID",
					Aliases: []string{"aid"},
					Usage:   "show cgAckID <cgAckID>",
					Action: func(c *cli.Context) {
						admin.ReadCgAckID(c)
					},
				},
				{
					Name:    "cgqueue",
					Aliases: []string{"cq", "cgq"},
					Usage:   "show cgqueue (<consumer_group_uuid> | <consumer_group_uuid> <extent_uuid>)",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "status, s",
							Value: "",
							Usage: "status: open | consumed | deleted, if empty, return all",
						},
					},
					Action: func(c *cli.Context) {
						admin.ReadCgQueue(c)
					},
				},
				{
					Name:    "destqueue",
					Aliases: []string{"dq", "destq"},
					Usage:   "show destqueue (<destination_uuid> | <destination_path>)",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "status, s",
							Value: "open",
							Usage: "status: open | sealed | consumed archived | deleted, if empty, return all",
						},
					},
					Action: func(c *cli.Context) {
						admin.ReadDestQueue(c)
					},
				},
				{
					Name:    "cgBacklog",
					Aliases: []string{"cgb", "cb"},
					Usage:   "show cgBacklog <consumer_group_uuid>",
					Action: func(c *cli.Context) {
						admin.ReadCgBacklog(c)
					},
				},
			},
		},
		{
			Name:    "update",
			Aliases: []string{"u"},
			Usage:   "update (destination | consumergroup | storehost)",
			Subcommands: []cli.Command{
				{
					Name:    "destination",
					Aliases: []string{"d", "dst"},
					Usage:   "update destination <name>",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "status, s",
							Value: "enabled",
							Usage: "status: enabled | disabled | sendonly | recvonly",
						},
						cli.IntFlag{
							Name:  "consumed_messages_retention, cr",
							Value: 3600,
							Usage: "Consumed messages retention period specified in seconds. Default is one hour.",
						},
						cli.IntFlag{
							Name:  "unconsumed_messages_retention, ur",
							Value: 7200,
							Usage: "Unconsumed messages retention period specified in seconds. Default is two hours.",
						},
						cli.StringFlag{
							Name:  "checksum_option, co",
							Value: "",
							Usage: "Checksum_options, can be one of the crcIEEE, md5",
						},
						cli.StringFlag{
							Name:  "owner_email, oe",
							Value: "",
							Usage: "The updated owner's email",
						},
					},
					Action: func(c *cli.Context) {

						admin.UpdateDestination(c)
					},
				},
				{
					Name:    "consumergroup",
					Aliases: []string{"c", "cg"},
					Usage:   "update consumergroup <destination_path> <consumer_group_name>",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "status, s",
							Value: "enabled",
							Usage: "status: enabled | disabled",
						},
						cli.IntFlag{
							Name:  "lock_timeout_seconds, l",
							Value: 60,
							Usage: "Ack timeout for each message",
						},
						cli.IntFlag{
							Name:  "max_delivery_count, m",
							Value: 10,
							Usage: "Maximum delivery count for a message before it sents to dead-letter queue",
						},
						cli.IntFlag{
							Name:  "skip_older_messages_in_seconds, k",
							Value: 7200,
							Usage: "Skip messages older than this duration in seconds.",
						},
						cli.StringFlag{
							Name:  "owner_email, oe",
							Value: "",
							Usage: "The updated owner's email",
						},
					},
					Action: func(c *cli.Context) {
						admin.UpdateConsumerGroup(c)
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
					Aliases: []string{"d", "dst"},
					Usage:   "delete destination <name>",
					Action: func(c *cli.Context) {
						admin.DeleteDestination(c)
						println("deleted destination: ", c.Args().First())
					},
				},
				{
					Name:    "consumergroup",
					Aliases: []string{"c", "cg"},
					Usage:   "delete consumergroup <destination_path> <consumer_group_name>",
					Action: func(c *cli.Context) {
						admin.DeleteConsumerGroup(c)
						println("deleted consumergroup: ", c.Args()[0], c.Args()[1])
					},
				},
			},
		},
		{
			Name:    "list",
			Aliases: []string{"l", "ls"},
			Usage:   "list (destination | consumergroup | extents | consumergroupextents | hosts)",
			Subcommands: []cli.Command{
				{
					Name:    "destination",
					Aliases: []string{"d", "dst"},
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
						admin.ListDestinations(c)
					},
				},
				{
					Name:    "consumergroup",
					Aliases: []string{"c", "cg"},
					Usage:   "list consumergroup <destination_path> [<consumer_group>]",
					Action: func(c *cli.Context) {
						admin.ListConsumerGroups(c)
					},
				},
				{
					Name:    "extents",
					Aliases: []string{"e", "es"},
					Usage:   "list extents <destination_path>",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "prefix, pf",
							Usage: "only show extents of prefix",
						},
					},
					Action: func(c *cli.Context) {
						admin.ListExtents(c)
					},
				},
				{
					Name:    "consumergroupextents",
					Aliases: []string{"cge", "cges"},
					Usage:   "list consumergroupextents <destination_path> <consumergroup_path>",
					Flags: []cli.Flag{
						cli.IntFlag{
							Name:  "limit, lm",
							Value: 10,
							Usage: "show top n consumer group extents",
						},
					},
					Action: func(c *cli.Context) {
						admin.ListConsumerGroupExtents(c)
					},
				},
				{
					Name:    "hosts",
					Aliases: []string{"h", "hs"},
					Usage:   "list hosts [options] ",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "service, s",
							Usage: "only show hosts of service(input,output,frontend,store,controller)",
						},
						cli.StringFlag{
							Name:  "type, t",
							Value: "active",
							Usage: "show hosts from specific table(active, history), default to active",
						},
					},
					Action: func(c *cli.Context) {
						admin.ListHosts(c)
					},
				},
			},
		},
		{
			Name:    "listAll",
			Aliases: []string{"la", "lsa"},
			Usage:   "listAll (destination)",
			Subcommands: []cli.Command{
				{
					Name:    "destination",
					Aliases: []string{"d", "dst"},
					Usage:   "listAll destination [options]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "include, id",
							Value: "",
							Usage: "only show destinations match include regexp",
						},
						cli.StringFlag{
							Name:  "exclude, ed",
							Value: "",
							Usage: "only show destinations not match excluded regexp",
						},
						cli.StringFlag{
							Name:  "status, s",
							Value: "",
							Usage: "status: enabled | disabled | sendonly | recvonly | deleting | deleted, if empty, return all",
						},
					},
					Action: func(c *cli.Context) {
						admin.ListAllDestinations(c)
					},
				},
			},
		},
		{
			Name:    "publish",
			Aliases: []string{"p", "pub", "w", "write"},
			Usage:   "publish <destination_name>",
			Action: func(c *cli.Context) {
				admin.Publish(c)
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
				admin.Consume(c)
			},
		},
		{
			Name:    "uuid2hostport",
			Aliases: []string{"u2h"},
			Usage:   "uuid2hostport <uuid>",
			Action: func(c *cli.Context) {
				admin.UUID2hostAddr(c)
			},
		},
		{
			Name:    "audit",
			Aliases: []string{"at"},
			Usage:   "audit",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "uuid",
					Value: "",
					Usage: "destination uuid",
				},
				cli.StringFlag{
					Name:  "name",
					Value: "",
					Usage: "destination path",
				},
				cli.IntFlag{
					Name:  "limit",
					Value: 100,
					Usage: "maximum returned ops number",
				},
			},
			Action: func(c *cli.Context) {
				admin.ListEntityOps(c)
			},
		},
		{
			Name:    "hostport2uuid",
			Aliases: []string{"h2u"},
			Usage:   "hostport2uuid <host:port>",
			Action: func(c *cli.Context) {
				admin.HostAddr2uuid(c)
			},
		},
		{
			Name:    "merge_dlq",
			Aliases: []string{"m"},
			Usage:   "merge_dlq <consumer_group_name> [options]",
			Action: func(c *cli.Context) {
				println("**not implemented** merged DLQ in consumer group: ", c.Args().First())
			},
		},
		{
			Name:    "unload",
			Aliases: []string{"ul"},
			Usage:   "unload (consumergroup)",
			Subcommands: []cli.Command{
				{
					Name:    "consumergroup",
					Aliases: []string{"c", "cg"},
					Usage:   "unload consumergroup <hostport> [options]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "cg_uuid, u",
							Value: "",
							Usage: "The consumergroup UUID which should be unloaded",
						},
					},
					Action: func(c *cli.Context) {
						admin.UnloadConsumerGroup(c)
					},
				},
			},
		},
		{
			Name:    "serviceconfig",
			Aliases: []string{"cfg"},
			Usage:   "serviceconfig (get|set|delete)",
			Subcommands: []cli.Command{
				{
					Name:    "get",
					Aliases: []string{"g"},
					Usage:   "serviceconfig get <service-name> [options]",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "key, k",
							Value: "",
							Usage: "The config key whose value is to be fetched",
						},
					},
					Action: func(c *cli.Context) {
						admin.GetServiceConfig(c)
					},
				},
				{
					Name:    "set",
					Aliases: []string{"s"},
					Usage:   "serviceconfig set <service-name.version.sku.hostname.config-key> <config-value>",
					Action: func(c *cli.Context) {
						admin.SetServiceConfig(c)
					},
				},
				{
					Name:    "delete",
					Aliases: []string{"d"},
					Usage:   "serviceconfig delete <service-name.version.sku.hostname.config-key>",
					Action: func(c *cli.Context) {
						admin.DeleteServiceConfig(c)
					},
				},
			},
		},
	}

	app.Run(os.Args)
}
