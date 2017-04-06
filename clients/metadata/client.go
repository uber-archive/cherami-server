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

package metadata

import (
	"fmt"
	"sync"
	"time"

	"github.com/uber/tchannel-go/thrift"
	"golang.org/x/net/context"

	ccli "github.com/uber/cherami-client-go/client/cherami"
	"github.com/uber/cherami-server/common"
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/tchannel-go"
)

type (
	clientImpl struct {
		connection *tchannel.Channel
		client     m.TChanMetadataExposable
		options    *ccli.ClientOptions

		sync.Mutex
	}
)

// NewClient returns the singleton metadata client used for communicating with the service at given port
func NewClient(serviceName string, host string, port int, options *ccli.ClientOptions) (Client, error) {
	ch, err := tchannel.NewChannel(serviceName, nil)
	if err != nil {
		return nil, err
	}

	ch.Peers().Add(fmt.Sprintf("%s:%d", host, port))
	return newClientWithTChannel(ch, options)
}

// NewHyperbahnClient returns the singleton metadata client used for communicating with the service via hyperbahn.
func NewHyperbahnClient(serviceName string, bootstrapFile string, options *ccli.ClientOptions) (Client, error) {
	ch, err := tchannel.NewChannel(serviceName, nil)
	if err != nil {
		return nil, err
	}

	common.CreateHyperbahnClient(ch, bootstrapFile)
	return newClientWithTChannel(ch, options)
}

func newClientWithTChannel(ch *tchannel.Channel, options *ccli.ClientOptions) (Client, error) {
	if options == nil {
		options = getDefaultOptions()
	}
	tClient := thrift.NewClient(ch, getFrontEndServiceName(options.DeploymentStr), nil)

	client := &clientImpl{
		connection: ch,
		client:     m.NewTChanMetadataExposableClient(tClient),
		options:    options,
	}
	return client, nil
}

// Close shuts down the connection to Cherami frontend
func (c *clientImpl) Close() {
	c.Lock()
	defer c.Unlock()

	if c.connection != nil {
		c.connection.Close()
	}
}

func (c *clientImpl) createContext() (thrift.Context, context.CancelFunc) {
	return thrift.NewContext(c.options.Timeout)
}

func (c *clientImpl) UUIDToHostAddr(hostUUID string) (string, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.UUIDToHostAddr(ctx, hostUUID)
}

func (c *clientImpl) HostAddrToUUID(hostport string) (string, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.HostAddrToUUID(ctx, hostport)
}

func (c *clientImpl) ReadExtentStats(request *m.ReadExtentStatsRequest) (*m.ReadExtentStatsResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ReadExtentStats(ctx, request)
}

func (c *clientImpl) ReadDestination(request *shared.ReadDestinationRequest) (*shared.DestinationDescription, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ReadDestination(ctx, request)
}

func (c *clientImpl) ListDestinations(request *shared.ListDestinationsRequest) (*shared.ListDestinationsResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ListDestinations(ctx, request)
}

func (c *clientImpl) ListDestinationsByUUID(request *shared.ListDestinationsByUUIDRequest) (*shared.ListDestinationsResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ListDestinationsByUUID(ctx, request)
}

func (c *clientImpl) ListExtentsStats(request *shared.ListExtentsStatsRequest) (*shared.ListExtentsStatsResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ListExtentsStats(ctx, request)
}

func (c *clientImpl) ListStoreExtentsStats(request *m.ListStoreExtentsStatsRequest) (*m.ListStoreExtentsStatsResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ListStoreExtentsStats(ctx, request)
}

func (c *clientImpl) ListHosts(request *m.ListHostsRequest) (*m.ListHostsResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ListHosts(ctx, request)
}

func (c *clientImpl) ListAllConsumerGroups(request *shared.ListConsumerGroupRequest) (*shared.ListConsumerGroupResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ListAllConsumerGroups(ctx, request)
}

func (c *clientImpl) ListEntityOps(request *m.ListEntityOpsRequest) (*m.ListEntityOpsResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ListEntityOps(ctx, request)
}

func (c *clientImpl) ListConsumerGroups(request *shared.ListConsumerGroupRequest) (*shared.ListConsumerGroupResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ListConsumerGroups(ctx, request)
}

func (c *clientImpl) ReadConsumerGroupExtentsByExtUUID(request *m.ReadConsumerGroupExtentsByExtUUIDRequest) (*m.ReadConsumerGroupExtentsByExtUUIDResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ReadConsumerGroupExtentsByExtUUID(ctx, request)
}

func (c *clientImpl) ReadConsumerGroupExtents(request *shared.ReadConsumerGroupExtentsRequest) (*shared.ReadConsumerGroupExtentsResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ReadConsumerGroupExtents(ctx, request)
}

func (c *clientImpl) ReadConsumerGroupExtent(request *m.ReadConsumerGroupExtentRequest) (*m.ReadConsumerGroupExtentResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ReadConsumerGroupExtent(ctx, request)
}

func (c *clientImpl) ReadConsumerGroup(request *shared.ReadConsumerGroupRequest) (*shared.ConsumerGroupDescription, error) {

	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ReadConsumerGroup(ctx, request)
}

func (c *clientImpl) ReadConsumerGroupByUUID(request *shared.ReadConsumerGroupRequest) (*shared.ConsumerGroupDescription, error) {

	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ReadConsumerGroupByUUID(ctx, request)
}

func (c *clientImpl) UpdateServiceConfig(request *m.UpdateServiceConfigRequest) error {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.UpdateServiceConfig(ctx, request)
}

func (c *clientImpl) ReadServiceConfig(request *m.ReadServiceConfigRequest) (*m.ReadServiceConfigResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()
	return c.client.ReadServiceConfig(ctx, request)
}

func (c *clientImpl) DeleteServiceConfig(request *m.DeleteServiceConfigRequest) error {
	ctx, cancel := c.createContext()
	defer cancel()
	return c.client.DeleteServiceConfig(ctx, request)
}

func getDefaultOptions() *ccli.ClientOptions {
	return &ccli.ClientOptions{Timeout: time.Minute}
}

func getFrontEndServiceName(deploymentStr string) string {
	if len(deploymentStr) == 0 {
		return common.FrontendServiceName
	}
	return fmt.Sprintf("%v_%v", common.FrontendServiceName, deploymentStr)
}
