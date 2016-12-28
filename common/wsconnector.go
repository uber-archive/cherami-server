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

package common

import (
	"fmt"
	"net/http"
	"reflect"

	"github.com/uber/cherami-client-go/common"
	"github.com/uber/cherami-client-go/common/websocket"
	"github.com/uber/cherami-client-go/stream"
	serverStream "github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

type (
	// WSConnector takes care of establishing connection via websocket stream
	WSConnector interface {
		OpenPublisherStream(hostPort string, requestHeader http.Header) (stream.BInOpenPublisherStreamOutCall, error)
		AcceptPublisherStream(w http.ResponseWriter, r *http.Request) (serverStream.BInOpenPublisherStreamInCall, error)
		OpenConsumerStream(hostPort string, requestHeader http.Header) (stream.BOutOpenConsumerStreamOutCall, error)
		AcceptConsumerStream(w http.ResponseWriter, r *http.Request) (serverStream.BOutOpenConsumerStreamInCall, error)
		OpenAppendStream(hostPort string, requestHeader http.Header) (serverStream.BStoreOpenAppendStreamOutCall, error)
		AcceptAppendStream(w http.ResponseWriter, r *http.Request) (serverStream.BStoreOpenAppendStreamInCall, error)
		OpenReadStream(hostPort string, requestHeader http.Header) (serverStream.BStoreOpenReadStreamOutCall, error)
		AcceptReadStream(w http.ResponseWriter, r *http.Request) (serverStream.BStoreOpenReadStreamInCall, error)
		OpenReplicationReadStream(hostPort string, requestHeader http.Header) (serverStream.BStoreOpenReadStreamOutCall, error)
		AcceptReplicationReadStream(w http.ResponseWriter, r *http.Request) (serverStream.BStoreOpenReadStreamInCall, error)
		OpenReplicationRemoteReadStream(hostPort string, requestHeader http.Header) (serverStream.BStoreOpenReadStreamOutCall, error)
		AcceptReplicationRemoteReadStream(w http.ResponseWriter, r *http.Request) (serverStream.BStoreOpenReadStreamInCall, error)
	}

	wsConnectorImpl struct {
		wsHub websocket.Hub

		openPublisherOutStreamReadType reflect.Type
		openPublisherInStreamReadType  reflect.Type
		openConsumerOutStreamReadType  reflect.Type
		openConsumerInStreamReadType   reflect.Type
		openAppendOutStreamReadType    reflect.Type
		openAppendInStreamReadType     reflect.Type
		openReadOutStreamReadType      reflect.Type
		openReadInStreamReadType       reflect.Type
	}

	// OpenPublisherOutWebsocketStream is a wrapper for websocket to work with OpenPublisherStream
	OpenPublisherOutWebsocketStream struct {
		stream websocket.StreamClient
	}

	// OpenPublisherInWebsocketStream is a wrapper for websocket to work with OpenPublisherStream
	OpenPublisherInWebsocketStream struct {
		stream websocket.StreamServer
	}

	// OpenConsumerOutWebsocketStream is a wrapper for websocket to work with OpenConsumerStream
	OpenConsumerOutWebsocketStream struct {
		stream websocket.StreamClient
	}

	// OpenConsumerInWebsocketStream is a wrapper for websocket to work with OpenConsumerStream
	OpenConsumerInWebsocketStream struct {
		stream websocket.StreamServer
	}

	// OpenAppendOutWebsocketStream is a wrapper for websocket to work with OpenAppendStream (hopefully rename to OpenAppendStream)
	OpenAppendOutWebsocketStream struct {
		stream websocket.StreamClient
	}

	// OpenAppendInWebsocketStream is a wrapper for websocket to work with OpenAppendStream (hopefully rename to OpenAppendStream)
	OpenAppendInWebsocketStream struct {
		stream websocket.StreamServer
	}

	// OpenReadOutWebsocketStream is a wrapper for websocket to work with OpenReadStream
	OpenReadOutWebsocketStream struct {
		stream websocket.StreamClient
	}

	// OpenReadInWebsocketStream is a wrapper for websocket to work with OpenReadStream
	OpenReadInWebsocketStream struct {
		stream websocket.StreamServer
	}
)

// interface implementation check
var _ WSConnector = (*wsConnectorImpl)(nil)
var _ stream.BInOpenPublisherStreamOutCall = (*OpenPublisherOutWebsocketStream)(nil)
var _ serverStream.BInOpenPublisherStreamInCall = (*OpenPublisherInWebsocketStream)(nil)
var _ stream.BOutOpenConsumerStreamOutCall = (*OpenConsumerOutWebsocketStream)(nil)
var _ serverStream.BOutOpenConsumerStreamInCall = (*OpenConsumerInWebsocketStream)(nil)
var _ serverStream.BStoreOpenAppendStreamOutCall = (*OpenAppendOutWebsocketStream)(nil)
var _ serverStream.BStoreOpenAppendStreamInCall = (*OpenAppendInWebsocketStream)(nil)
var _ serverStream.BStoreOpenReadStreamOutCall = (*OpenReadOutWebsocketStream)(nil)
var _ serverStream.BStoreOpenReadStreamInCall = (*OpenReadInWebsocketStream)(nil)

// NewWSConnector creates a WSConnector
func NewWSConnector() WSConnector {
	return &wsConnectorImpl{
		wsHub: websocket.NewWebsocketHub(),
		openPublisherOutStreamReadType: reflect.TypeOf((*cherami.InputHostCommand)(nil)).Elem(),
		openPublisherInStreamReadType:  reflect.TypeOf((*cherami.PutMessage)(nil)).Elem(),
		openConsumerOutStreamReadType:  reflect.TypeOf((*cherami.OutputHostCommand)(nil)).Elem(),
		openConsumerInStreamReadType:   reflect.TypeOf((*cherami.ControlFlow)(nil)).Elem(),
		openAppendOutStreamReadType:    reflect.TypeOf((*store.AppendMessageAck)(nil)).Elem(),
		openAppendInStreamReadType:     reflect.TypeOf((*store.AppendMessage)(nil)).Elem(),
		openReadOutStreamReadType:      reflect.TypeOf((*store.ReadMessageContent)(nil)).Elem(),
		openReadInStreamReadType:       reflect.TypeOf((*cherami.ControlFlow)(nil)).Elem(),
	}
}

func (c *wsConnectorImpl) OpenPublisherStream(hostPort string, requestHeader http.Header) (stream.BInOpenPublisherStreamOutCall, error) {

	url := fmt.Sprintf(common.WSUrlFormat, hostPort, common.EndpointOpenPublisherStream)

	stream := websocket.NewStreamClient(url, requestHeader, c.wsHub, c.openPublisherOutStreamReadType)

	if err := stream.Start(); err != nil {
		return nil, err
	}

	return NewOpenPublisherOutWebsocketStream(stream), nil
}

func (c *wsConnectorImpl) AcceptPublisherStream(w http.ResponseWriter, r *http.Request) (serverStream.BInOpenPublisherStreamInCall, error) {

	stream := websocket.NewStreamServer(w, r, c.wsHub, c.openPublisherInStreamReadType)

	if err := stream.Start(); err != nil {
		return nil, err
	}

	return NewOpenPublisherInWebsocketStream(stream), nil
}

func (c *wsConnectorImpl) OpenConsumerStream(hostPort string, requestHeader http.Header) (stream.BOutOpenConsumerStreamOutCall, error) {

	url := fmt.Sprintf(common.WSUrlFormat, hostPort, common.EndpointOpenConsumerStream)

	stream := websocket.NewStreamClient(url, requestHeader, c.wsHub, c.openConsumerOutStreamReadType)

	if err := stream.Start(); err != nil {
		return nil, err
	}

	return NewOpenConsumerOutWebsocketStream(stream), nil
}

func (c *wsConnectorImpl) AcceptConsumerStream(w http.ResponseWriter, r *http.Request) (serverStream.BOutOpenConsumerStreamInCall, error) {

	stream := websocket.NewStreamServer(w, r, c.wsHub, c.openConsumerInStreamReadType)

	if err := stream.Start(); err != nil {
		return nil, err
	}

	return NewOpenConsumerInWebsocketStream(stream), nil
}

func (c *wsConnectorImpl) OpenAppendStream(hostPort string, requestHeader http.Header) (serverStream.BStoreOpenAppendStreamOutCall, error) {

	url := fmt.Sprintf(common.WSUrlFormat, hostPort, common.EndpointOpenAppendStream)

	stream := websocket.NewStreamClient(url, requestHeader, c.wsHub, c.openAppendOutStreamReadType)

	if err := stream.Start(); err != nil {
		return nil, err
	}

	return NewOpenAppendOutWebsocketStream(stream), nil
}

func (c *wsConnectorImpl) AcceptAppendStream(w http.ResponseWriter, r *http.Request) (serverStream.BStoreOpenAppendStreamInCall, error) {

	stream := websocket.NewStreamServer(w, r, c.wsHub, c.openAppendInStreamReadType)

	if err := stream.Start(); err != nil {
		return nil, err
	}

	return NewOpenAppendInWebsocketStream(stream), nil
}

func (c *wsConnectorImpl) OpenReadStream(hostPort string, requestHeader http.Header) (serverStream.BStoreOpenReadStreamOutCall, error) {

	url := fmt.Sprintf(common.WSUrlFormat, hostPort, common.EndpointOpenReadStream)

	stream := websocket.NewStreamClient(url, requestHeader, c.wsHub, c.openReadOutStreamReadType)

	if err := stream.Start(); err != nil {
		return nil, err
	}

	return NewOpenReadOutWebsocketStream(stream), nil
}

func (c *wsConnectorImpl) AcceptReadStream(w http.ResponseWriter, r *http.Request) (serverStream.BStoreOpenReadStreamInCall, error) {

	stream := websocket.NewStreamServer(w, r, c.wsHub, c.openReadInStreamReadType)

	if err := stream.Start(); err != nil {
		return nil, err
	}

	return NewOpenReadInWebsocketStream(stream), nil
}

func (c *wsConnectorImpl) OpenReplicationReadStream(hostPort string, requestHeader http.Header) (serverStream.BStoreOpenReadStreamOutCall, error) {

	url := fmt.Sprintf(common.WSUrlFormat, hostPort, common.EndpointOpenReplicationReadStream)

	stream := websocket.NewStreamClient(url, requestHeader, c.wsHub, c.openReadOutStreamReadType)

	if err := stream.Start(); err != nil {
		return nil, err
	}

	return NewOpenReadOutWebsocketStream(stream), nil
}

func (c *wsConnectorImpl) AcceptReplicationReadStream(w http.ResponseWriter, r *http.Request) (serverStream.BStoreOpenReadStreamInCall, error) {

	stream := websocket.NewStreamServer(w, r, c.wsHub, c.openReadInStreamReadType)

	if err := stream.Start(); err != nil {
		return nil, err
	}

	return NewOpenReadInWebsocketStream(stream), nil
}

func (c *wsConnectorImpl) OpenReplicationRemoteReadStream(hostPort string, requestHeader http.Header) (serverStream.BStoreOpenReadStreamOutCall, error) {

	url := fmt.Sprintf(common.WSUrlFormat, hostPort, common.EndpointOpenReplicationRemoteReadStream)

	stream := websocket.NewStreamClient(url, requestHeader, c.wsHub, c.openReadOutStreamReadType)

	if err := stream.Start(); err != nil {
		return nil, err
	}

	return NewOpenReadOutWebsocketStream(stream), nil
}

func (c *wsConnectorImpl) AcceptReplicationRemoteReadStream(w http.ResponseWriter, r *http.Request) (serverStream.BStoreOpenReadStreamInCall, error) {

	stream := websocket.NewStreamServer(w, r, c.wsHub, c.openReadInStreamReadType)

	if err := stream.Start(); err != nil {
		return nil, err
	}

	return NewOpenReadInWebsocketStream(stream), nil
}

// NewOpenPublisherOutWebsocketStream returns a new OpenPublisherOutWebsocketStream object
func NewOpenPublisherOutWebsocketStream(stream websocket.StreamClient) *OpenPublisherOutWebsocketStream {
	return &OpenPublisherOutWebsocketStream{stream: stream}
}

// Write writes a result to the response stream
func (s *OpenPublisherOutWebsocketStream) Write(arg *cherami.PutMessage) error {
	return s.stream.Write(arg)
}

// Flush flushes all written arguments.
func (s *OpenPublisherOutWebsocketStream) Flush() error {
	return s.stream.Flush()
}

// Read returns the next argument, if any is available.
func (s *OpenPublisherOutWebsocketStream) Read() (*cherami.InputHostCommand, error) {

	msg, err := s.stream.Read()
	if err != nil {
		return nil, err
	}

	return msg.(*cherami.InputHostCommand), err
}

// ResponseHeaders is defined to conform to the tchannel-stream .*OutCall interface
func (s *OpenPublisherOutWebsocketStream) ResponseHeaders() (map[string]string, error) {
	return map[string]string{}, nil
}

// Done closes the request stream and should be called after all arguments have been written.
func (s *OpenPublisherOutWebsocketStream) Done() error {
	return s.stream.Done()
}

// NewOpenPublisherInWebsocketStream returns a new OpenPublisherInWebsocketStream object
func NewOpenPublisherInWebsocketStream(stream websocket.StreamServer) *OpenPublisherInWebsocketStream {
	return &OpenPublisherInWebsocketStream{stream: stream}
}

// Write writes a result to the response stream
func (s *OpenPublisherInWebsocketStream) Write(arg *cherami.InputHostCommand) error {
	return s.stream.Write(arg)
}

// Flush flushes all written arguments.
func (s *OpenPublisherInWebsocketStream) Flush() error {
	return s.stream.Flush()
}

// Read returns the next argument, if any is available.
func (s *OpenPublisherInWebsocketStream) Read() (*cherami.PutMessage, error) {

	msg, err := s.stream.Read()
	if err != nil {
		return nil, err
	}

	return msg.(*cherami.PutMessage), err
}

// SetResponseHeaders sets the response headers.
func (s *OpenPublisherInWebsocketStream) SetResponseHeaders(headers map[string]string) error {
	return nil
}

// Done closes the request stream and should be called after all arguments have been written.
func (s *OpenPublisherInWebsocketStream) Done() error {
	return s.stream.Done()
}

// NewOpenConsumerOutWebsocketStream returns a new OpenConsumerOutWebsocketStream object
func NewOpenConsumerOutWebsocketStream(stream websocket.StreamClient) *OpenConsumerOutWebsocketStream {
	return &OpenConsumerOutWebsocketStream{stream: stream}
}

// Write writes a result to the response stream
func (s *OpenConsumerOutWebsocketStream) Write(arg *cherami.ControlFlow) error {
	return s.stream.Write(arg)
}

// Flush flushes all written arguments.
func (s *OpenConsumerOutWebsocketStream) Flush() error {
	return s.stream.Flush()
}

// Read returns the next argument, if any is available.
func (s *OpenConsumerOutWebsocketStream) Read() (*cherami.OutputHostCommand, error) {

	msg, err := s.stream.Read()
	if err != nil {
		return nil, err
	}

	return msg.(*cherami.OutputHostCommand), err
}

// ResponseHeaders is defined to conform to the tchannel-stream .*OutCall interface
func (s *OpenConsumerOutWebsocketStream) ResponseHeaders() (map[string]string, error) {
	return map[string]string{}, nil
}

// Done closes the request stream and should be called after all arguments have been written.
func (s *OpenConsumerOutWebsocketStream) Done() error {
	return s.stream.Done()
}

// NewOpenConsumerInWebsocketStream returns a new OpenConsumerInWebsocketStream object
func NewOpenConsumerInWebsocketStream(stream websocket.StreamServer) *OpenConsumerInWebsocketStream {
	return &OpenConsumerInWebsocketStream{stream: stream}
}

// Write writes a result to the response stream
func (s *OpenConsumerInWebsocketStream) Write(arg *cherami.OutputHostCommand) error {
	return s.stream.Write(arg)
}

// Flush flushes all written arguments.
func (s *OpenConsumerInWebsocketStream) Flush() error {
	return s.stream.Flush()
}

// Read returns the next argument, if any is available.
func (s *OpenConsumerInWebsocketStream) Read() (*cherami.ControlFlow, error) {

	msg, err := s.stream.Read()
	if err != nil {
		return nil, err
	}

	return msg.(*cherami.ControlFlow), err
}

// SetResponseHeaders sets the response headers.
func (s *OpenConsumerInWebsocketStream) SetResponseHeaders(headers map[string]string) error {
	return nil
}

// Done closes the request stream and should be called after all arguments have been written.
func (s *OpenConsumerInWebsocketStream) Done() error {
	return s.stream.Done()
}

// NewOpenAppendOutWebsocketStream returns a new OpenAppendOutWebsocketStream object
func NewOpenAppendOutWebsocketStream(stream websocket.StreamClient) *OpenAppendOutWebsocketStream {
	return &OpenAppendOutWebsocketStream{stream: stream}
}

// Write writes a result to the response stream
func (s *OpenAppendOutWebsocketStream) Write(arg *store.AppendMessage) error {
	return s.stream.Write(arg)
}

// Flush flushes all written arguments.
func (s *OpenAppendOutWebsocketStream) Flush() error {
	return s.stream.Flush()
}

// Read returns the next argument, if any is available.
func (s *OpenAppendOutWebsocketStream) Read() (*store.AppendMessageAck, error) {

	msg, err := s.stream.Read()
	if err != nil {
		return nil, err
	}

	return msg.(*store.AppendMessageAck), err
}

// ResponseHeaders is defined to conform to the tchannel-stream .*OutCall interface
func (s *OpenAppendOutWebsocketStream) ResponseHeaders() (map[string]string, error) {
	return map[string]string{}, nil
}

// Done closes the request stream and should be called after all arguments have been written.
func (s *OpenAppendOutWebsocketStream) Done() error {
	return s.stream.Done()
}

// NewOpenAppendInWebsocketStream returns a new OpenAppendInWebsocketStream object
func NewOpenAppendInWebsocketStream(stream websocket.StreamServer) *OpenAppendInWebsocketStream {
	return &OpenAppendInWebsocketStream{stream: stream}
}

// Write writes a result to the response stream
func (s *OpenAppendInWebsocketStream) Write(arg *store.AppendMessageAck) error {
	return s.stream.Write(arg)
}

// Flush flushes all written arguments.
func (s *OpenAppendInWebsocketStream) Flush() error {
	return s.stream.Flush()
}

// Read returns the next argument, if any is available.
func (s *OpenAppendInWebsocketStream) Read() (*store.AppendMessage, error) {

	msg, err := s.stream.Read()
	if err != nil {
		return nil, err
	}

	return msg.(*store.AppendMessage), err
}

// SetResponseHeaders sets the response headers.
func (s *OpenAppendInWebsocketStream) SetResponseHeaders(headers map[string]string) error {
	return nil
}

// Done closes the request stream and should be called after all arguments have been written.
func (s *OpenAppendInWebsocketStream) Done() error {
	return s.stream.Done()
}

// NewOpenReadOutWebsocketStream returns a new OpenReadOutWebsocketStream object
func NewOpenReadOutWebsocketStream(stream websocket.StreamClient) *OpenReadOutWebsocketStream {
	return &OpenReadOutWebsocketStream{stream: stream}
}

// Write writes a result to the response stream
func (s *OpenReadOutWebsocketStream) Write(arg *cherami.ControlFlow) error {
	return s.stream.Write(arg)
}

// Flush flushes all written arguments.
func (s *OpenReadOutWebsocketStream) Flush() error {
	return s.stream.Flush()
}

// Read returns the next argument, if any is available.
func (s *OpenReadOutWebsocketStream) Read() (*store.ReadMessageContent, error) {

	msg, err := s.stream.Read()
	if err != nil {
		return nil, err
	}

	return msg.(*store.ReadMessageContent), err
}

// ResponseHeaders is defined to conform to the tchannel-stream .*OutCall interface
func (s *OpenReadOutWebsocketStream) ResponseHeaders() (map[string]string, error) {
	return map[string]string{}, nil
}

// Done closes the request stream and should be called after all arguments have been written.
func (s *OpenReadOutWebsocketStream) Done() error {
	return s.stream.Done()
}

// NewOpenReadInWebsocketStream returns a new OpenReadInWebsocketStream object
func NewOpenReadInWebsocketStream(stream websocket.StreamServer) *OpenReadInWebsocketStream {
	return &OpenReadInWebsocketStream{stream: stream}
}

// Write writes a result to the response stream
func (s *OpenReadInWebsocketStream) Write(arg *store.ReadMessageContent) error {
	return s.stream.Write(arg)
}

// Flush flushes all written arguments.
func (s *OpenReadInWebsocketStream) Flush() error {
	return s.stream.Flush()
}

// Read returns the next argument, if any is available.
func (s *OpenReadInWebsocketStream) Read() (*cherami.ControlFlow, error) {

	msg, err := s.stream.Read()
	if err != nil {
		return nil, err
	}

	return msg.(*cherami.ControlFlow), err
}

// SetResponseHeaders sets the response headers.
func (s *OpenReadInWebsocketStream) SetResponseHeaders(headers map[string]string) error {
	return nil
}

// Done closes the request stream and should be called after all arguments have been written.
func (s *OpenReadInWebsocketStream) Done() error {
	return s.stream.Done()
}
