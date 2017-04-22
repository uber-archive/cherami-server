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

package outputhost

import (
	"fmt"
	"time"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/admin"

	tchannel "github.com/uber/tchannel-go"
	tcthrift "github.com/uber/tchannel-go/thrift"
)

// OutClientImpl is a outputhost cherami tchannel client
type OutClientImpl struct {
	connection *tchannel.Channel
	client     admin.TChanOutputHostAdmin
}

// NewClient returns a new instance of cherami tchannel client
func NewClient(instanceID int, hostAddr string) (*OutClientImpl, error) {
	ch, err := tchannel.NewChannel(fmt.Sprintf("outputhost-client-%v", instanceID), nil)
	if err != nil {
		return nil, err
	}

	tClient := tcthrift.NewClient(ch, common.OutputServiceName, &tcthrift.ClientOptions{
		HostPort: hostAddr,
	})
	client := admin.NewTChanOutputHostAdminClient(tClient)

	return &OutClientImpl{
		connection: ch,
		client:     client,
	}, nil
}

// Close closes the client
func (s *OutClientImpl) Close() {
	s.connection.Close()
}

// UnloadConsumerGroups unloads the CG from the outputhost
func (s *OutClientImpl) UnloadConsumerGroups(req *admin.UnloadConsumerGroupsRequest) error {
	ctx, cancel := tcthrift.NewContext(2 * time.Second)
	defer cancel()

	return s.client.UnloadConsumerGroups(ctx, req)
}

// ListLoadedConsumerGroups lists all the loaded cgs from the outputhost
func (s *OutClientImpl) ListLoadedConsumerGroups() (*admin.ListConsumerGroupsResult_, error) {
	ctx, cancel := tcthrift.NewContext(15 * time.Second)
	defer cancel()

	return s.client.ListLoadedConsumerGroups(ctx)
}

// ReadCgState returns consumer group state inside output host
func (s *OutClientImpl) ReadCgState(req *admin.ReadConsumerGroupStateRequest) (*admin.ReadConsumerGroupStateResult_, error) {
	ctx, cancel := tcthrift.NewContext(15 * time.Second)
	defer cancel()

	return s.client.ReadCgState(ctx, req)
}
