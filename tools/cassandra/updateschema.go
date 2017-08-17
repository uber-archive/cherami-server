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

package cassandra

import (
	"flag"
	"fmt"
)

const (
	defaultProtoVersion = 4
)

// RunSchemaUpdateTool runs the schema update tool
func RunSchemaUpdateTool(args []string) {
	config := parseSchemaUpadaterConfig(args)
	if config == nil {
		return
	}
	updateTask, err := NewSchemaUpdateTask(config)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("Running with Config=%+v\n\n", config)
	updateTask.Run()
}

func parseSchemaUpadaterConfig(args []string) *SchemaUpdaterConfig {
	var config SchemaUpdaterConfig
	cmd := flag.NewFlagSet("update-schema", flag.ExitOnError)
	cmd.StringVar(&config.HostsCsv, "h", "", "Path to json file containing cassandra hosts")
	cmd.IntVar(&config.Port, "port", 9042, "Port to cassandra hosts")
	cmd.StringVar(&config.Username, "u", "", "Cassandra username")
	cmd.StringVar(&config.Password, "pw", "", "Cassandra password")
	cmd.StringVar(&config.Keyspace, "k", "", "Cassandra keyspace")
	cmd.BoolVar(&config.IsDryRun, "d", true, "Dry run")
	cmd.BoolVar(&config.SkipVersionCheck, "v", false, "Skip previous version check")
	cmd.StringVar(&config.ManifestFilePath, "m", "", "Mainfest file path")
	cmd.IntVar(&config.ProtoVersion, "p", defaultProtoVersion, "Cassandra CQL proto version")

	fmt.Printf("update-schema args:%v\n", args)

	if len(args) < 1 || args[0] != "update-schema" {
		printHelp()
		return nil
	}

	if err := cmd.Parse(args[1:]); err != nil {
		printHelp()
		return nil
	}

	if len(config.HostsCsv) == 0 || len(config.Keyspace) == 0 || len(config.ManifestFilePath) == 0 {
		printHelp()
		return nil
	}
	return &config
}

func printHelp() {
	helpMessage := `updateSchema -h [cassandra_host1,host2..] -k [keyspace] -m [/tmp/manifest.json] [-d false] [-v false]
    -h
        List of cassandra hosts to connect to
    -port
        Port to cassandra cluster, 9042 by default
    -u
        Cassandra username
    -pw
        Cassandra password
    -k
        Cassandra keyspace
    -m
        Path to the manifest json file
    -d
        Dry run, defaults to true
    -v
        Skip version check, defaults to false
    -p
    	Cassandra CQL protocol version, defaults to 4
	`
	fmt.Println(helpMessage)
}
