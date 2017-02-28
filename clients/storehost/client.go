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

package storehost

import (
	"fmt"
	"time"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/store"

	tchannel "github.com/uber/tchannel-go"
	tcthrift "github.com/uber/tchannel-go/thrift"
)

// StoreClientImpl is a storehost cherami tchannel client
type StoreClientImpl struct {
	connection *tchannel.Channel
	client     store.TChanBStore
}

// NewClient returns a new instance of cherami tchannel client
func NewClient(storeUUID, hostAddr string) (*StoreClientImpl, error) {
	ch, err := tchannel.NewChannel(fmt.Sprintf("storehost-client-%v", storeUUID), nil)
	if err != nil {
		return nil, err
	}

	tClient := tcthrift.NewClient(ch, common.StoreServiceName, &tcthrift.ClientOptions{
		HostPort: hostAddr,
	})
	client := store.NewTChanBStoreClient(tClient)

	return &StoreClientImpl{
		connection: ch,
		client:     client,
	}, nil
}

// Close closes the client
func (s *StoreClientImpl) Close() {
	s.connection.Close()
}

// ReadMessages reads a sequence of messages from the store
func (s *StoreClientImpl) ReadMessages(req *store.ReadMessagesRequest) (*store.ReadMessagesResult_, error) {
	ctx, cancel := tcthrift.NewContext(2 * time.Second)
	defer cancel()

	return s.client.ReadMessages(ctx, req)
}

// GetAddressFromTimestamp queries store for the address corresponding to the given timestamp
func (s *StoreClientImpl) GetAddressFromTimestamp(req *store.GetAddressFromTimestampRequest) (*store.GetAddressFromTimestampResult_, error) {
	ctx, cancel := tcthrift.NewContext(2 * time.Second)
	defer cancel()

	return s.client.GetAddressFromTimestamp(ctx, req)
}

// SealExtent seals an extent on the specified store
func (s *StoreClientImpl) SealExtent(req *store.SealExtentRequest) error {
	ctx, cancel := tcthrift.NewContext(2 * time.Second)
	defer cancel()

	return s.client.SealExtent(ctx, req)
}

// PurgeMessages seals an extent on the specified store
func (s *StoreClientImpl) PurgeMessages(req *store.PurgeMessagesRequest) (*store.PurgeMessagesResult_, error) {
	ctx, cancel := tcthrift.NewContext(2 * time.Second)
	defer cancel()

	return s.client.PurgeMessages(ctx, req)
}

// ListExtents lists the extents on the specified store
func (s *StoreClientImpl) ListExtents() (*store.ListExtentsResult_, error) {
	ctx, cancel := tcthrift.NewContext(60 * time.Second)
	defer cancel()

	return s.client.ListExtents(ctx)
}
