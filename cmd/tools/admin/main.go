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
	"github.com/uber/cherami-client-go/client/cherami"
	lib "github.com/uber/cherami-server/cmd/tools/common"
)

const (
	adminToolService = "cherami-admin"
)

func main() {
	app := cli.NewApp()
	app.Name = "cherami"
	app.Usage = "A command-line tool for cherami developer, including debugging tool"
	app.Version = "1.2.1"

	lib.SetCommonFlags(&app.Flags, false)
	lib.SetAdminFlags(&app.Flags)

	cliHelper := lib.GetCommonCliHelper()

	lib.SetCommonCommands(&app.Commands, cliHelper, adminToolService, false,
		func(c *cli.Context) cherami.AuthProvider {
			return nil
		})
	lib.SetAdminCommands(&app.Commands)

	app.Run(os.Args)
}
