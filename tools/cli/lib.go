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

package cli

import (
	"github.com/codegangsta/cli"
	scommon "github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/tools/common"
)

const (
	serviceName = "cherami-cli"
)

// ReadCgBacklog reads cg backlog
func ReadCgBacklog(c *cli.Context) {
	cClient := common.GetCClient(c, serviceName)
	common.ReadCgBacklog(c, cClient)
}

// CreateDestination creates a destination
func CreateDestination(c *cli.Context, cliHelper scommon.CliHelper) {
	cClient := common.GetCClient(c, serviceName)
	common.CreateDestination(c, cClient, cliHelper)
}

// UpdateDestination updates the destination
func UpdateDestination(c *cli.Context) {
	mClient := common.GetMClient(c, serviceName)
	cClient := common.GetCClient(c, serviceName)
	common.UpdateDestination(c, cClient, mClient)
}

// CreateConsumerGroup creates the CG
func CreateConsumerGroup(c *cli.Context, cliHelper scommon.CliHelper) {
	mClient := common.GetMClient(c, serviceName)
	cClient := common.GetCClient(c, serviceName)
	common.CreateConsumerGroup(c, cClient, mClient, cliHelper)
}

// UpdateConsumerGroup updates the CG
func UpdateConsumerGroup(c *cli.Context) {
	cClient := common.GetCClient(c, serviceName)
	common.UpdateConsumerGroup(c, cClient)
}

// ReadDestination is used to get info about the destination
func ReadDestination(c *cli.Context) {
	mClient := common.GetMClient(c, serviceName)
	common.ReadDestination(c, mClient)
}

// ReadDlq is used to get info about the DLQ destination
func ReadDlq(c *cli.Context) {
	mClient := common.GetMClient(c, serviceName)
	common.ReadDlq(c, mClient)
}

// DeleteDestination deletes the given destination
func DeleteDestination(c *cli.Context) {
	cClient := common.GetCClient(c, serviceName)
	common.DeleteDestination(c, cClient)
}

// DeleteConsumerGroup deletes the given CG
func DeleteConsumerGroup(c *cli.Context) {
	cClient := common.GetCClient(c, serviceName)
	common.DeleteConsumerGroup(c, cClient)
}

// ReadConsumerGroup gets info about the CG
func ReadConsumerGroup(c *cli.Context) {
	mClient := common.GetMClient(c, serviceName)
	common.ReadConsumerGroup(c, mClient)
}

// ReadMessage is used to read a message
func ReadMessage(c *cli.Context) {
	mClient := common.GetMClient(c, serviceName)
	common.ReadMessage(c, mClient)
}

// ListDestinations is used to list all destinations
func ListDestinations(c *cli.Context) {
	mClient := common.GetMClient(c, serviceName)
	common.ListDestinations(c, mClient)
}

// ListConsumerGroups is used to list all CGs
func ListConsumerGroups(c *cli.Context) {
	cClient := common.GetCClient(c, serviceName)
	common.ListConsumerGroups(c, cClient)
}

// Publish is used to publish to a given destination
func Publish(c *cli.Context) {
	cClient := common.GetCClient(c, serviceName)
	common.Publish(c, cClient)
}

// Consume is used to consume from the given destination for
// the given CG
func Consume(c *cli.Context) {
	cClient := common.GetCClient(c, serviceName)
	common.Consume(c, cClient)
}

// MergeDLQForConsumerGroup merges the DLQ for this CG
func MergeDLQForConsumerGroup(c *cli.Context) {
	cClient := common.GetCClient(c, serviceName)
	common.MergeDLQForConsumerGroup(c, cClient)
}

// PurgeDLQForConsumerGroup purges the DLQ for this CG
func PurgeDLQForConsumerGroup(c *cli.Context) {
	cClient := common.GetCClient(c, serviceName)
	common.PurgeDLQForConsumerGroup(c, cClient)
}
