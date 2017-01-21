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
	"log"
	"os"
	"strings"
	"sync"

	"github.com/codegangsta/cli"
	"github.com/uber/cherami-server/cmd/servicecmd"
)

var wg sync.WaitGroup

func main() {
	funcMap := map[string]func(){
		"inputhost":    servicecmd.StartInputHostService,
		"outputhost":   servicecmd.StartOutputHostService,
		"controller":   servicecmd.StartControllerService,
		"frontendhost": servicecmd.StartFrontendHostService,
		"storehost":    servicecmd.StartStoreHostService,
	}
	app := cli.NewApp()
	app.Name = "standalone"
	app.Usage = "a standalone command-line tool for start cherami"
	app.Version = "1.1"
	app.Commands = []cli.Command{
		{
			Name:  "start",
			Usage: "start <services>, arg <services> example:inputhost,outputhost,controller,frontendhost,storehost default is all services",
			Action: func(c *cli.Context) {
				var inputServiceNames string
				if len(c.Args()) < 1 {
					inputServiceNames = "all"
				} else {
					inputServiceNames = strings.ToLower(c.Args().Get(0))
				}

				if inputServiceNames == "all" {
					inputServiceNames = "inputhost,outputhost,controller,frontendhost,storehost"
				}

				serviceNameList := strings.Split(inputServiceNames, ",")
				for _, serviceName := range serviceNameList {
					service, ok := funcMap[serviceName]
					if ok {
						wg.Add(1)
						go func() {
							service()
							wg.Done()
						}()
					} else {
						log.Fatal(serviceName + ` is not a correct service name, correct services: inputhost, outputhost, storehost, controller, frontendhost`)
					}
				}
				wg.Wait()
			},
		},
	}
	app.Run(os.Args)
}
