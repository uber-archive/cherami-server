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

package cherami_boy_private

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/uber/cherami-client-go/common"
	"github.com/uber/cherami-client-go/common/backoff"
	"github.com/uber/cherami-client-go/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/cherami"

	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"

	"code.uber.internal/odp/cherami/security"
	cherami_client "github.com/uber/cherami-client-go/client/cherami"
	"golang.org/x/net/context"
)

type (
	ClientImpl struct {
		Connection  *tchannel.Channel
		Client      cherami.TChanBFrontend
		Options     *cherami_client.ClientOptions
		RetryPolicy backoff.RetryPolicy

		sync.Mutex
		hostPort string
	}
)

const (
	clientRetryInterval                   = 50 * time.Millisecond
	clientRetryMaxInterval                = 10 * time.Second
	clientRetryExpirationInterval         = 1 * time.Minute
	defaultReconfigurationPollingInterval = 10 * time.Second
)

var envUserName = os.Getenv("USER")
var envHostName, _ = os.Hostname()

// NewClient returns the singleton Cherami Client used for communicating with the service at given port
func NewClient(serviceName string, host string, port int, options *cherami_client.ClientOptions) (cherami_client.Client, error) {
	ch, err := tchannel.NewChannel(serviceName, nil)
	if err != nil {
		return nil, err
	}

	ch.Peers().Add(fmt.Sprintf("%s:%d", host, port))
	return newClientWithTChannel(ch, options)
}

// NewHyperbahnClient returns the singleton Cherami Client used for communicating with the service via Hyperbahn or
// Muttley. Streaming methods (for LOG/Consistent destinations) will not work.
func NewHyperbahnClient(serviceName string, bootstrapFile string, options *cherami_client.ClientOptions) (cherami_client.Client, error) {
	ch, err := tchannel.NewChannel(serviceName, nil)
	if err != nil {
		return nil, err
	}

	common.CreateHyperbahnClient(ch, bootstrapFile)
	return newClientWithTChannel(ch, options)
}

// NewClientWithFEClient is used by Frontend to create a Cherami Client for itself.
// It is used by non-streaming publish/consume APIs.
// ** Internal Cherami Use Only **
func NewClientWithFEClient(feClient cherami.TChanBFrontend, options *cherami_client.ClientOptions) (cherami_client.Client, error) {
	options, err := verifyOptions(options)
	if err != nil {
		return nil, err
	}

	return &ClientImpl{
		Client:      feClient,
		Options:     options,
		RetryPolicy: createDefaultRetryPolicy(),
	}, nil
}

func newClientWithTChannel(ch *tchannel.Channel, options *cherami_client.ClientOptions) (cherami_client.Client, error) {
	options, err := verifyOptions(options)
	if err != nil {
		return nil, err
	}

	tClient := thrift.NewClient(ch, getFrontEndServiceName(options.DeploymentStr), nil)

	client := &ClientImpl{
		Connection:  ch,
		Client:      cherami.NewTChanBFrontendClient(tClient),
		Options:     options,
		RetryPolicy: createDefaultRetryPolicy(),
	}
	return client, nil
}

func (c *ClientImpl) createStreamingClient() (cherami.TChanBFrontend, error) {
	// create a streaming Client directly connecting to the frontend
	c.Lock()
	defer c.Unlock()
	// if hostPort is not known (e.g. hyperbahn Client), need to query the
	// frontened to find its IP
	if c.hostPort == "" {
		ctx, cancel := c.createContext()
		defer cancel()
		hostport, err := c.Client.HostPort(ctx)
		if err != nil {
			return nil, err
		}

		c.hostPort = hostport
	}

	ch, err := tchannel.NewChannel(uuid.New(), nil)
	if err != nil {
		return nil, err
	}

	tClient := thrift.NewClient(ch, getFrontEndServiceName(c.Options.DeploymentStr), &thrift.ClientOptions{
		HostPort: c.hostPort,
	})

	streamingClient := cherami.NewTChanBFrontendClient(tClient)

	return streamingClient, nil
}

// Close shuts down the Connection to Cherami frontend
func (c *ClientImpl) Close() {
	c.Lock()
	defer c.Unlock()

	if c.Connection != nil {
		c.Connection.Close()
	}
}

func (c *ClientImpl) CreateDestination(request *cherami.CreateDestinationRequest) (*cherami.DestinationDescription, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	log.Warn("********** xxxxxxxxxx NewWonkaAuthProvider")
	authProvider, err := security.NewWonkaAuthProvider("cherami_user_01", "/Users/boyang/gocode/src/code.uber.internal/odp/cherami/wonka/cherami_user_01_private.pem")
	if err != nil {
		return nil, err
	}
	ctx, err = authProvider.CreateSecurityContext(ctx)
	if err != nil {
		return nil, err
	}

	return c.Client.CreateDestination(ctx, request)
}

func (c *ClientImpl) ReadDestination(request *cherami.ReadDestinationRequest) (*cherami.DestinationDescription, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.Client.ReadDestination(ctx, request)
}

func (c *ClientImpl) UpdateDestination(request *cherami.UpdateDestinationRequest) (*cherami.DestinationDescription, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.Client.UpdateDestination(ctx, request)
}

func (c *ClientImpl) DeleteDestination(request *cherami.DeleteDestinationRequest) error {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.Client.DeleteDestination(ctx, request)
}

func (c *ClientImpl) ListDestinations(request *cherami.ListDestinationsRequest) (*cherami.ListDestinationsResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.Client.ListDestinations(ctx, request)
}

func (c *ClientImpl) CreateConsumerGroup(request *cherami.CreateConsumerGroupRequest) (*cherami.ConsumerGroupDescription, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.Client.CreateConsumerGroup(ctx, request)
}

func (c *ClientImpl) ReadConsumerGroup(request *cherami.ReadConsumerGroupRequest) (*cherami.ConsumerGroupDescription, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.Client.ReadConsumerGroup(ctx, request)
}

func (c *ClientImpl) UpdateConsumerGroup(request *cherami.UpdateConsumerGroupRequest) (*cherami.ConsumerGroupDescription, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.Client.UpdateConsumerGroup(ctx, request)
}

func (c *ClientImpl) MergeDLQForConsumerGroup(request *cherami.MergeDLQForConsumerGroupRequest) error {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.Client.MergeDLQForConsumerGroup(ctx, request)
}

func (c *ClientImpl) PurgeDLQForConsumerGroup(request *cherami.PurgeDLQForConsumerGroupRequest) error {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.Client.PurgeDLQForConsumerGroup(ctx, request)
}

func (c *ClientImpl) DeleteConsumerGroup(request *cherami.DeleteConsumerGroupRequest) error {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.Client.DeleteConsumerGroup(ctx, request)
}

func (c *ClientImpl) ListConsumerGroups(request *cherami.ListConsumerGroupRequest) (*cherami.ListConsumerGroupResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.Client.ListConsumerGroups(ctx, request)
}

func (c *ClientImpl) GetQueueDepthInfo(request *cherami.GetQueueDepthInfoRequest) (*cherami.GetQueueDepthInfoResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.Client.GetQueueDepthInfo(ctx, request)
}

func (c *ClientImpl) CreatePublisher(request *cherami_client.CreatePublisherRequest) cherami_client.Publisher {
	return nil
}

func (c *ClientImpl) CreateConsumer(request *cherami_client.CreateConsumerRequest) cherami_client.Consumer {
	return nil
}

func (c *ClientImpl) createContext() (thrift.Context, context.CancelFunc) {
	ctx, cancel := thrift.NewContext(c.Options.Timeout)
	return thrift.WithHeaders(ctx, map[string]string{
		common.HeaderClientVersion: common.ClientVersion,
		common.HeaderUserName:      envUserName,
		common.HeaderHostName:      envHostName,
	}), cancel
}

func (c *ClientImpl) ReadPublisherOptions(path string) (*cherami.ReadPublisherOptionsResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	var result *cherami.ReadPublisherOptionsResult_
	readOp := func() error {
		var e error
		request := &cherami.ReadPublisherOptionsRequest{
			Path: common.StringPtr(path),
		}

		result, e = c.Client.ReadPublisherOptions(ctx, request)

		return e
	}

	err := backoff.Retry(readOp, c.RetryPolicy, isTransientError)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (c *ClientImpl) ReadConsumerGroupHosts(path string, consumerGroupName string) (*cherami.ReadConsumerGroupHostsResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	var result *cherami.ReadConsumerGroupHostsResult_
	readOp := func() error {
		var e error
		request := &cherami.ReadConsumerGroupHostsRequest{
			DestinationPath:   common.StringPtr(path),
			ConsumerGroupName: common.StringPtr(consumerGroupName),
		}

		result, e = c.Client.ReadConsumerGroupHosts(ctx, request)

		return e
	}

	err := backoff.Retry(readOp, c.RetryPolicy, isTransientError)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func getFrontEndServiceName(deploymentStr string) string {
	if len(deploymentStr) == 0 || strings.HasPrefix(strings.ToLower(deploymentStr), `prod`) || strings.HasPrefix(strings.ToLower(deploymentStr), `dev`) {
		return common.FrontendServiceName
	}
	return fmt.Sprintf("%v_%v", common.FrontendServiceName, deploymentStr)
}

func getDefaultLogger() bark.Logger {
	return bark.NewLoggerFromLogrus(log.StandardLogger())
}

func getDefaultOptions() *cherami_client.ClientOptions {
	return &cherami_client.ClientOptions{
		Timeout:                        time.Minute,
		Logger:                         getDefaultLogger(),
		MetricsReporter:                metrics.NewNullReporter(),
		ReconfigurationPollingInterval: defaultReconfigurationPollingInterval,
	}
}

// verifyOptions is used to verify if we have a metrics reporter and
// a logger. If not, just setup a default logger and a null reporter
// it also validate the timeout is sane
func verifyOptions(opts *cherami_client.ClientOptions) (*cherami_client.ClientOptions, error) {
	if opts == nil {
		opts = getDefaultOptions()
	}
	if opts.Timeout.Nanoseconds() == 0 {
		opts.Timeout = getDefaultOptions().Timeout
	}
	if err := common.ValidateTimeout(opts.Timeout); err != nil {
		return nil, err
	}

	if opts.Logger == nil {
		opts.Logger = getDefaultLogger()
	}

	if opts.MetricsReporter == nil {
		opts.MetricsReporter = metrics.NewNullReporter()
	}
	// Now make sure we init the default metrics as well
	opts.MetricsReporter.InitMetrics(metrics.MetricDefs)

	if int64(opts.ReconfigurationPollingInterval/time.Second) == 0 {
		opts.ReconfigurationPollingInterval = defaultReconfigurationPollingInterval
	}

	return opts, nil
}

func createDefaultRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(clientRetryInterval)
	policy.SetMaximumInterval(clientRetryMaxInterval)
	policy.SetExpirationInterval(clientRetryExpirationInterval)

	return policy
}

func isTransientError(err error) bool {
	// Only EntityNotExistsError/EntityDisabledError from Cherami is treated as non-transient error today
	switch err.(type) {
	case *cherami.EntityNotExistsError:
		return false
	case *cherami.EntityDisabledError:
		return false
	default:
		return true
	}
}
