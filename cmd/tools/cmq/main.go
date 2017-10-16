package main

import (
	"fmt"
	"os"
	"time"

	"github.com/urfave/cli"
)

func main() {

	var mc *MetadataClient
	var err error

	app := cli.NewApp()
	app.Name = "cmq"
	app.Usage = "Cherami Metadata Query-er"
	app.Version = "0.1"
	app.HideHelp = true
	app.EnableBashCompletion = true

	app.Before = func(c *cli.Context) error {

		var opts = &Opts{
			Host:        c.String("host"),
			Port:        c.Int("port"),
			Keyspace:    c.String("keyspace"),
			Consistency: c.String("consistency"),
			Username:    c.String("username"),
			Password:    c.String("password"),
			Timeout:     c.Duration("timeout"),
			Retries:     c.Int("retries"),
			PageSize:    c.Int("pagesize"),
		}

		mc, err = NewMetadataClient(opts)

		if err != nil {
			fmt.Errorf("NewMetadataClient error: %v\n", err)
			// return
		}

		return nil
	}

	app.After = func(c *cli.Context) error {

		if mc != nil {
			mc.Close()
		}

		return nil
	}

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "host, h",
			Usage:  "Cassandra host",
			EnvVar: "CASSANDRA_HOST",
		},
		cli.IntFlag{
			Name:   "port",
			Value:  9042,
			Usage:  "Cassandra port",
			EnvVar: "CASSANDRA_PORT",
		},
		cli.StringFlag{
			Name:   "username, u",
			Value:  "",
			Usage:  "Cassandra username",
			EnvVar: "CASSANDRA_USERNAME",
		},
		cli.StringFlag{
			Name:   "password, pw",
			Value:  "",
			Usage:  "Cassandra password",
			EnvVar: "CASSANDRA_PASSWORD",
		},
		cli.StringFlag{
			Name:  "consistency, cons",
			Value: "One",
			Usage: "Consistency to use in queries: One, Two, Quorum, etc ",
		},
		cli.StringFlag{
			Name:   "keyspace, env, k",
			Value:  "cherami",
			Usage:  "Cassandra keyspace suffix (ex: 'cherami_staging_dca1a')",
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
	}

	app.Commands = []cli.Command{
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
			Action: func(c *cli.Context) error {
				return gc(c, mc)
			},
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
					Action: func(c *cli.Context) error {
						return list_destinations(c, mc)
					},
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
					Action: func(c *cli.Context) error {
						return list_destinations_by_path(c, mc)
					},
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
					Action: func(c *cli.Context) error {
						return list_consumer_groups(c, mc)
					},
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
					Action: func(c *cli.Context) error {
						return list_destination_extents(c, mc)
					},
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
					Action: func(c *cli.Context) error {
						return list_consumer_group_extents(c, mc)
					},
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
					Action: func(c *cli.Context) error {
						return list_store_extents(c, mc)
					},
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
					Action: func(c *cli.Context) error {
						return list_operations(c, mc)
					},
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
			Action: func(c *cli.Context) error {
				return count(c, mc)
			},
		},
		cli.Command{
			Name:    "show",
			Aliases: []string{"s"},
			Subcommands: []cli.Command{
				{
					Name:    "destination_by_path",
					Aliases: []string{"dbp"},
					Flags:   []cli.Flag{},
					Action: func(c *cli.Context) error {
						return show_destination_by_path(c, mc)
					},
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
					Action: func(c *cli.Context) error {
						return show_destination(c, mc)
					},
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
					Action: func(c *cli.Context) error {
						return show_consumergroup(c, mc)
					},
				},
				{
					Name:    "extent",
					Aliases: []string{"x"},
					Flags:   []cli.Flag{},
					Action: func(c *cli.Context) error {
						return show_extent(c, mc)
					},
				},
				{
					Name:    "cgextent",
					Aliases: []string{"cgx"},
					Flags:   []cli.Flag{},
					Action: func(c *cli.Context) error {
						return show_cgextent(c, mc)
					},
				},
			},
		},
		cli.Command{
			Name:    "destination",
			Aliases: []string{"d"},
			Action: func(c *cli.Context) error {
				return show_destination(c, mc)
			},
		},
		cli.Command{
			Name:    "consumergroup",
			Aliases: []string{"cg"},
			Action: func(c *cli.Context) error {
				return show_consumergroup(c, mc)
			},
		},
		cli.Command{
			Name:    "extent",
			Aliases: []string{"x"},
			Action: func(c *cli.Context) error {
				return show_extent(c, mc)
			},
		},
		cli.Command{
			Name: "stats",
			Action: func(c *cli.Context) error {
				return stats(c, mc)
			},
		},
		cli.Command{
			Name:    "fixsmartretry",
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
			Action: func(c *cli.Context) error {
				return fixsmartretry(c, mc)
			},
		},
		cli.Command{
			Name:    "monitor",
			Aliases: []string{"m"},
			Action: func(c *cli.Context) error {
				return monitor(c, mc)
			},
		},
		cli.Command{
			Name:    "test",
			Aliases: []string{"t"},
			Action: func(c *cli.Context) error {
				return test(c, mc)
			},
		},
	}

	app.Run(os.Args)

	return
}
