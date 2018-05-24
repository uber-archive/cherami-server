package main

import (
	"os"

	"github.com/urfave/cli"
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
		return nil
	}

	app.Flags = cmqOptions
	app.Commands = cmqCommands

	app.Run(os.Args)
}
