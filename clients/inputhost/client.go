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

package inputhost

import (
	"fmt"
	"time"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/admin"

	tchannel "github.com/uber/tchannel-go"
	tcthrift "github.com/uber/tchannel-go/thrift"
)

const (
	// defaultThriftTimeout for all the tchannel-thrift calls
	defaultThriftTimeout = 10 * time.Second
)

// InClientImpl is a inputhost tchannel client
type InClientImpl struct {
	connection *tchannel.Channel
	client     admin.TChanInputHostAdmin
}

// NewClient returns a new instance of tchannel client
func NewClient(instanceID int, hostAddr string) (*InClientImpl, error) {
	ch, err := tchannel.NewChannel(fmt.Sprintf("inputhost-client-%v", instanceID), nil)
	if err != nil {
		return nil, err
	}

	tClient := tcthrift.NewClient(ch, common.InputServiceName, &tcthrift.ClientOptions{
		HostPort: hostAddr,
	})
	client := admin.NewTChanInputHostAdminClient(tClient)

	return &InClientImpl{
		connection: ch,
		client:     client,
	}, nil
}

// Close closes the client
func (s *InClientImpl) Close() {
	s.connection.Close()
}

// UnloadDestinations unloads the destination from the inputhost
func (s *InClientImpl) UnloadDestinations(req *admin.UnloadDestinationsRequest) error {
	ctx, cancel := tcthrift.NewContext(defaultThriftTimeout)
	defer cancel()

	return s.client.UnloadDestinations(ctx, req)
}

// ListLoadedDestinations lists all the loaded destinations from the inputhost
func (s *InClientImpl) ListLoadedDestinations() (*admin.ListLoadedDestinationsResult_, error) {
	ctx, cancel := tcthrift.NewContext(defaultThriftTimeout)
	defer cancel()

	return s.client.ListLoadedDestinations(ctx)
}

// ReadDestState reads the given destination's state from the inputhost
func (s *InClientImpl) ReadDestState(req *admin.ReadDestinationStateRequest) (*admin.ReadDestinationStateResult_, error) {
	ctx, cancel := tcthrift.NewContext(defaultThriftTimeout)
	defer cancel()

	return s.client.ReadDestState(ctx, req)
}
