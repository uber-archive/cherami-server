package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"
)

var cliContext *cli.Context // global cli context

func main() {

	app := cli.NewApp()
	app.Name = "cmq"
	app.Usage = "Cherami Metadata Query-er"
	app.Version = "0.1"
	app.HideHelp = true
	app.EnableBashCompletion = true

	app.Before = func(c *cli.Context) error {
		cliContext = c
		return nil
	}

	app.After = func(c *cli.Context) error {
		print("\n")
		return nil
	}

	app.Flags = cmqOptions
	app.Commands = cmqCommands

	app.Run(os.Args)
}

type ZoneConfig struct {
	Hosts    string `yaml:"hosts"` // TODO: add support for multiple hosts
	Port     int    `yaml:"port"`
	Keyspace string `yaml:"keyspace"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

func getOpts(c *cli.Context) *Opts {

	var opts = &Opts{
		Port:        9042,
		Consistency: "One",
		Keyspace:    "cherami",
	}

	if c.IsSet("zone") {

		if configYaml, err := ioutil.ReadFile(c.String("config")); err == nil {

			config := make(map[string]ZoneConfig)

			if err = yaml.Unmarshal(configYaml, &config); err != nil {

				fmt.Printf("error parsing yaml (%s): %v\n", c.String("config"), err)

			} else {

				if cfg, ok := config[c.String("zone")]; !ok {

					fmt.Printf("config not found for zone '%v'\n", c.String("zone"))

				} else {

					opts.Hosts = cfg.Hosts
					if cfg.Port != 0 {
						opts.Port = cfg.Port
					}
					opts.Keyspace = cfg.Keyspace
					opts.Username = cfg.Username
					opts.Password = cfg.Password
				}
			}
		}
	}

	if c.IsSet("hosts") {
		opts.Hosts = c.String("hosts")
	}

	if c.IsSet("port") {
		opts.Port = c.Int("port")
	}

	if c.IsSet("keyspace") {
		opts.Keyspace = c.String("keyspace")
	}

	if c.IsSet("consistency") {
		opts.Consistency = c.String("consistency")
	}

	if c.IsSet("username") {
		opts.Username = c.String("username")
	}

	if c.IsSet("password") {
		opts.Password = c.String("password")
	}

	if c.IsSet("timeout") {
		opts.Timeout = c.Duration("timeout")
	}

	if c.IsSet("retries") {
		opts.Retries = c.Int("retries")
	}

	if c.IsSet("pagesize") {
		opts.PageSize = c.Int("pagesize")
	}

	return opts
}
