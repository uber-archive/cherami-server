package main

import (
	"github.com/urfave/cli"
	"math"
	"time"
)

var cmqOptions = []cli.Flag{
	cli.StringFlag{
		Name:  "config, cfg",
		Usage: "cmq config yaml",
		Value: "/etc/cmq.yaml",
	},
	cli.StringFlag{
		Name:   "zone, z",
		Usage:  "Cherami Zone",
		EnvVar: "CMQ_ZONE",
	},
	cli.StringFlag{
		Name:   "host, h",
		Usage:  "Cassandra host",
		EnvVar: "CASSANDRA_HOST",
	},
	cli.IntFlag{
		Name:   "port",
		Usage:  "Cassandra port",
		EnvVar: "CASSANDRA_PORT",
	},
	cli.StringFlag{
		Name:   "username, u",
		Usage:  "Cassandra username",
		EnvVar: "CASSANDRA_USERNAME",
	},
	cli.StringFlag{
		Name:   "password, pw",
		Usage:  "Cassandra password",
		EnvVar: "CASSANDRA_PASSWORD",
	},
	cli.StringFlag{
		Name:  "consistency, cons",
		Usage: "Consistency to use in queries: One, Two, Quorum, etc ",
	},
	cli.StringFlag{
		Name:   "keyspace, env, k",
		Usage:  "Cassandra keyspace",
		EnvVar: "CHERAMI_KEYSPACE",
	},
	cli.DurationFlag{
		Name:  "timeout, t",
		Value: time.Minute,
		Usage: "Cassandra query timeout",
	},
	cli.IntFlag{
		Name:  "retries, r",
		Value: 32,
		Usage: "Cassandra query retries",
	},
	cli.IntFlag{
		Name:  "pagesize, p",
		Value: 5000,
		Usage: "Cassandra query page-size",
	},
	cli.StringSliceFlag{
		Name:   "output, out, o",
		Usage:  "cmq output format: short, json, cql, delete, undo, none [default: json]",
		EnvVar: "CMQ_OUTPUT",
	},
}

var cmqCommands = []cli.Command{
	cli.Command{
		Name:    "gc",
		Aliases: []string{"chk"},
		Flags: []cli.Flag{
			cli.BoolFlag{
				Name: "all, a",
			},
			cli.BoolFlag{
				Name: "destinations, d",
			},
			cli.BoolFlag{
				Name: "consumer_groups, cg, c",
			},
			cli.BoolFlag{
				Name: "destination_extents, de, e, x",
			},
			cli.BoolFlag{
				Name: "consumer_group_extents, cge, cgx",
			},
			cli.BoolFlag{
				Name: "store_extents, se, sx",
			},
			cli.BoolFlag{
				Name: "input_host_extents, ie, ix",
			},
		},
		Subcommands: []cli.Command{
			{
				Name:    "all",
				Aliases: []string{"a"},
				Flags: []cli.Flag{
					cli.StringSliceFlag{
						Name:  "table",
						Usage: "destinations (d), consumer_groups (cg), destination_extents (e, de, x), consumer_group_extents (cgx, cge), store_extents (s, se, sx), input_host_extents (i, ie, ix)",
					},
				},
			},
			{
				Name:    "destinations_by_path",
				Aliases: []string{"dbp", "path"},
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "destination, dest, d",
						Value: "",
						Usage: "destination path",
					},
					cli.StringFlag{
						Name:  "status, s",
						Value: "",
						Usage: "status",
					},
				},
			},
			{
				Name:    "destinations",
				Aliases: []string{"d"},
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "destination, dest, d",
						Value: "",
						Usage: "destination uuid",
					},
					cli.StringFlag{
						Name:  "status, s",
						Value: "",
						Usage: "status",
					},
				},
			},
			{
				Name: "consumer_groups",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "consumergroup, cg",
						Usage: "consumer-group uuid",
					},
					cli.StringFlag{
						Name:  "status, s",
						Usage: "status",
					},
				},
			},
			{
				Name:    "destination_extents",
				Aliases: []string{"de", "x"},
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "extent, x",
						Usage: "extent uuid",
					},
					cli.StringFlag{
						Name:  "status, s",
						Usage: "status",
					},
				},
			},
			{
				Name:    "consumer_group_extents",
				Aliases: []string{"cge", "cgx"},
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "extent, x",
						Usage: "extent uuid",
					},
					cli.StringFlag{
						Name:  "status, s",
						Usage: "status",
					},
				},
			},
			{
				Name: "extents",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "extent, x",
						Usage: "extent uuid",
					},
					cli.StringFlag{
						Name:  "status, s",
						Usage: "status",
					},
				},
			},
		},
		Action: gc,
	},
	cli.Command{
		Name:    "list",
		Aliases: []string{"l"},
		Subcommands: []cli.Command{
			{
				Name:    "destinations",
				Aliases: []string{"d"},
				Flags: []cli.Flag{
					cli.StringSliceFlag{
						Name:  "destination, dest, dst, d",
						Usage: "destination uuid(s)",
					},
					cli.StringSliceFlag{
						Name:  "path, p, n",
						Usage: "destination path(s)",
					},
					cli.StringSliceFlag{
						Name:  "status, s",
						Usage: "destination status (enabled, disabled, sendonly, receiveonly, deleting, deleted) ",
					},
					cli.StringSliceFlag{
						Name:  "type, t",
						Usage: "destination type (plain, timer, log)",
					},
					cli.BoolFlag{
						Name:  "multizone, mz",
						Usage: "multizone",
					},
				},
				Action: listDestinations,
			},
			{
				Name:    "destinations_by_path",
				Aliases: []string{"dbp"},
				Flags: []cli.Flag{
					cli.StringSliceFlag{
						Name:  "path, p, n",
						Usage: "destination path(s)",
					},
					cli.StringSliceFlag{
						Name:  "destination, dest, dst, d",
						Usage: "destination uuid(s)",
					},
					cli.StringSliceFlag{
						Name:  "status, s",
						Usage: "status",
					},
				},
				Action: listDestinationsByPath,
			},
			{
				Name:    "consumer_groups",
				Aliases: []string{"cg"},
				Flags: []cli.Flag{
					cli.StringSliceFlag{
						Name:  "consumergroup, cg",
						Usage: "consumer-group uuid(s)",
					},
					cli.StringSliceFlag{
						Name:  "name, n, p",
						Usage: "consumer-group name(s)",
					},
					cli.StringSliceFlag{
						Name:  "destination, dest, dst, d",
						Usage: "destination uuid(s)",
					},
					cli.StringSliceFlag{
						Name:  "status, s",
						Usage: "status",
					},
				},
				Action: listConsumerGroups,
			},
			{
				Name:    "destination_extents",
				Aliases: []string{"extent", "x"},
				Flags: []cli.Flag{
					cli.StringSliceFlag{
						Name:  "destination, dest, dst, d",
						Usage: "destination uuid(s)",
					},
					cli.StringSliceFlag{
						Name:  "extent, e, x",
						Usage: "extent uuid(s)",
					},
					cli.StringSliceFlag{
						Name:  "status, s",
						Usage: "status",
					},
				},
				Action: listDestinationExtents,
			},
			{
				Name:    "consumer_group_extents",
				Aliases: []string{"cgx"},
				Flags: []cli.Flag{
					cli.StringSliceFlag{
						Name:  "consumergroup, cg",
						Usage: "consumer-group uuid(s)",
					},
					cli.StringSliceFlag{
						Name:  "extent, e, x",
						Usage: "extent uuid(s)",
					},
					cli.StringSliceFlag{
						Name:  "status, s",
						Usage: "status",
					},
				},
				Action: listConsumerGroupExtents,
			},
			{
				Name:    "store_extents",
				Aliases: []string{"s", "se", "sx"},
				Flags: []cli.Flag{
					cli.StringSliceFlag{
						Name:  "store",
						Usage: "store uuid(s)",
					},
					cli.StringSliceFlag{
						Name:  "extent, e, x",
						Usage: "extent uuid(s)",
					},
					cli.StringSliceFlag{
						Name:  "status, s",
						Usage: "status",
					},
				},
				Action: listStoreExtents,
			},
			{
				Name:    "user_operations_by_entity_uuid",
				Aliases: []string{"operations", "op", "o"},
				Flags: []cli.Flag{
					cli.StringSliceFlag{
						Name:  "type, t",
						Usage: "operation type: create, update, delete",
					},
				},
				Action: listOperations,
			},
		},
	},
	cli.Command{
		Name:    "count",
		Aliases: []string{"c"},
		Subcommands: []cli.Command{
			{
				Name:    "all",
				Aliases: []string{"a"},
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "destination, dest, d",
						Usage: "destination",
					},
					cli.StringFlag{
						Name:  "consumergroup, cg",
						Usage: "consumer-group",
					},
					cli.StringFlag{
						Name:  "extent, x",
						Usage: "extent",
					},
				},
			},
			{
				Name:    "destinations_by_path",
				Aliases: []string{"dbp", "path"},
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "destination, dest, d",
						Value: "",
						Usage: "destination path",
					},
					cli.StringFlag{
						Name:  "status, s",
						Value: "",
						Usage: "status",
					},
				},
			},
			{
				Name:    "destinations",
				Aliases: []string{"d"},
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "destination, dest, d",
						Value: "",
						Usage: "destination uuid",
					},
					cli.StringFlag{
						Name:  "status, s",
						Value: "",
						Usage: "status",
					},
				},
			},
			{
				Name: "consumer_groups",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "consumergroup, cg",
						Usage: "consumer-group uuid",
					},
					cli.StringFlag{
						Name:  "status, s",
						Usage: "status",
					},
				},
			},
			{
				Name:    "destination_extents",
				Aliases: []string{"de", "x"},
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "extent, x",
						Usage: "extent uuid",
					},
					cli.StringFlag{
						Name:  "status, s",
						Usage: "status",
					},
				},
			},
			{
				Name:    "consumer_group_extents",
				Aliases: []string{"cge", "cgx"},
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "extent, x",
						Usage: "extent uuid",
					},
					cli.StringFlag{
						Name:  "status, s",
						Usage: "status",
					},
				},
			},
			{
				Name: "extents",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "extent, x",
						Usage: "extent uuid",
					},
					cli.StringFlag{
						Name:  "status, s",
						Usage: "status",
					},
				},
			},
		},
		Action: count,
	},
	cli.Command{
		Name:    "show",
		Aliases: []string{"s"},
		Subcommands: []cli.Command{
			{
				Name:    "destination_by_path",
				Aliases: []string{"dbp"},
				Flags:   []cli.Flag{},
				Action:  showDestinationByPath,
			},
			{
				Name:    "destination",
				Aliases: []string{"d"},
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "destination, dest, d",
						Value: "",
						Usage: "destination uuid",
					},
				},
				Action: showDestination,
			},
			{
				Name:    "consumergroup",
				Aliases: []string{"cg"},
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "consumergroup, cg",
						Value: "",
						Usage: "consumer-group uuid",
					},
				},
				Action: showConsumerGroup,
			},
			{
				Name:    "extent",
				Aliases: []string{"x"},
				Flags:   []cli.Flag{},
				Action:  showExtent,
			},
			{
				Name:    "cgextent",
				Aliases: []string{"cgx"},
				Flags:   []cli.Flag{},
				Action:  showCGExtent,
			},
		},
	},
	cli.Command{
		Name:    "destination",
		Aliases: []string{"d"},
		Flags: []cli.Flag{
			cli.StringSliceFlag{
				Name:  "destination, dest, dst, d",
				Usage: "destination uuid(s)",
			},
			cli.StringSliceFlag{
				Name:  "path, p, n",
				Usage: "destination path(s)",
			},
			cli.StringSliceFlag{
				Name:  "status, s",
				Usage: "destination status (enabled, disabled, sendonly, receiveonly, deleting, deleted) ",
			},
			cli.StringSliceFlag{
				Name:  "type, t",
				Usage: "destination type (plain, timer, log)",
			},
			cli.BoolFlag{
				Name:  "multizone, mz",
				Usage: "multizone",
			},
		},
		Action: listDestinations,
	},
	cli.Command{
		Name:    "consumergroup",
		Aliases: []string{"cg"},
		Flags: []cli.Flag{
			cli.StringSliceFlag{
				Name:  "consumergroup, cg",
				Usage: "consumer-group uuid(s)",
			},
			cli.StringSliceFlag{
				Name:  "name, n, p",
				Usage: "consumer-group name(s)",
			},
			cli.StringSliceFlag{
				Name:  "destination, dest, dst, d",
				Usage: "destination uuid(s)",
			},
			cli.StringSliceFlag{
				Name:  "status, s",
				Usage: "status",
			},
		},
		Action: listConsumerGroups,
	},
	cli.Command{
		Name:    "extent",
		Aliases: []string{"x"},
		Flags: []cli.Flag{
			cli.StringSliceFlag{
				Name:  "destination, dest, dst, d",
				Usage: "destination uuid(s)",
			},
			cli.StringSliceFlag{
				Name:  "extent, e, x",
				Usage: "extent uuid(s)",
			},
			cli.StringSliceFlag{
				Name:  "status, s",
				Usage: "status",
			},
		},
		Action: listDestinationExtents,
	},
	cli.Command{
		Name:    "cgextents",
		Aliases: []string{"cgx"},
		Flags: []cli.Flag{
			cli.StringSliceFlag{
				Name:  "consumergroup, cg",
				Usage: "consumer-group uuid(s)",
			},
			cli.StringSliceFlag{
				Name:  "extent, e, x",
				Usage: "extent uuid(s)",
			},
			cli.StringSliceFlag{
				Name:  "status, s",
				Usage: "status",
			},
		},
		Action: listConsumerGroupExtents,
	},
	cli.Command{
		Name:    "storeextents",
		Aliases: []string{"sx"},
		Flags: []cli.Flag{
			cli.StringSliceFlag{
				Name:  "store",
				Usage: "store uuid(s)",
			},
			cli.StringSliceFlag{
				Name:  "extent, e, x",
				Usage: "extent uuid(s)",
			},
			cli.StringSliceFlag{
				Name:  "status, s",
				Usage: "status",
			},
		},
		Action: listStoreExtents,
	},
	cli.Command{
		Name:   "stats",
		Action: stats,
	},
	cli.Command{
		Name: "fix",
		Subcommands: []cli.Command{
			{
				Name:    "smartretry",
				Aliases: []string{"fsr"},
				Flags: []cli.Flag{
					cli.BoolFlag{
						Name:  "all",
						Usage: "to apply on all CGs",
					},
					cli.BoolFlag{
						Name:  "verbose, v",
						Usage: "verbose",
					},
				},
				Action: fixsmartretry,
			},
			{
				Name: "destuuid",
				Flags: []cli.Flag{
					cli.BoolFlag{
						Name:  "all",
						Usage: "to apply on all CGs",
					},
					cli.BoolFlag{
						Name:  "verbose, v",
						Usage: "verbose",
					},
				},
				Action: fixdestuuid,
			},
		},
		Usage: "fix",
	},
	cli.Command{
		Name:    "watch",
		Aliases: []string{"m"},
		Action:  watch,
		Flags: []cli.Flag{
			cli.DurationFlag{
				Name:  "delay, d",
				Value: 2 * time.Second,
				Usage: "delay between refreshes",
			},
			cli.IntFlag{
				Name:  "number, num, n",
				Value: math.MaxInt64,
				Usage: "number of times to run",
			},
		},
	},
	cli.Command{
		Name:   "repair",
		Action: repair,
		Flags: []cli.Flag{
			cli.BoolFlag{
				Name: "run, r",
			},
			cli.DurationFlag{
				Name:  "timeout",
				Value: time.Second,
				Usage: "timeout for the cassandra call (default: 1 sec)",
			},
			cli.StringFlag{
				Name:  "consistency",
				Value: "all",
				Usage: "consistency level for cassandra call",
			},
		},
	},
	cli.Command{
		Name:    "test",
		Aliases: []string{"t"},
		Action:  test,
	},
}
