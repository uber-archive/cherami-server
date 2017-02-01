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
	"errors"
	"os"

	"github.com/codegangsta/cli"
	"github.com/uber/cherami-server/tools/common"
	"github.com/uber/cherami-server/tools/store"
)

func main() {
	app := cli.NewApp()
	app.Name = "store-tool"
	app.Usage = "A command-line tool for storage debugging"
	app.Version = "1.0.1"
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
			Value: "staging",
			Usage: "env to connect. By default connects to staging, use \"prod\" to connect to production",
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
			Name:    "checkconsistency",
			Aliases: []string{"cc"},
			Usage:   "checkconsistency store <hostname>",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "rootdir, rd",
					Usage: "the root folder where extents will be created at",
				},
			},
			Action: func(c *cli.Context) {
				if len(c.Args()) < 1 {
					common.ExitIfError(errors.New("not enough arguments"))
				}
				store.CheckStoreConsistency(c)
			},
		},
	}

	app.Run(os.Args)
}
