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

package stream

import (
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

// BInOpenPublisherStreamInCall is the object used to stream arguments and write
// response headers for incoming calls.
type BInOpenPublisherStreamInCall interface {
	// Read returns the next argument, if any is available. If there are no more arguments left,
	// it will return io.EOF.
	Read() (*cherami.PutMessage, error)

	// SetResponseHeaders sets the response headers. This must be called before any
	// streaming responses are sent.
	SetResponseHeaders(headers map[string]string) error

	// Write writes a result to the response stream. The written items may not
	// be sent till Flush or Done is called.
	Write(arg *cherami.InputHostCommand) error

	// Flush flushes headers (if they have not yet been sent) and any written results.
	Flush() error

	// Done closes the response stream and should be called after all results have been written.
	Done() error
}

// BOutOpenConsumerStreamInCall is the object used to stream arguments and write
// response headers for incoming calls.
type BOutOpenConsumerStreamInCall interface {
	// Read returns the next argument, if any is available. If there are no more arguments left,
	// it will return io.EOF.
	Read() (*cherami.ControlFlow, error)

	// SetResponseHeaders sets the response headers. This must be called before any
	// streaming responses are sent.
	SetResponseHeaders(headers map[string]string) error

	// Write writes a result to the response stream. The written items may not
	// be sent till Flush or Done is called.
	Write(arg *cherami.OutputHostCommand) error

	// Flush flushes headers (if they have not yet been sent) and any written results.
	Flush() error

	// Done closes the response stream and should be called after all results have been written.
	Done() error
}

// BOutOpenStreamingConsumerStreamInCall is the object used to stream arguments and write
// response headers for incoming calls.
type BOutOpenStreamingConsumerStreamInCall interface {
	// Read returns the next argument, if any is available. If there are no more arguments left,
	// it will return io.EOF.
	Read() (*cherami.ControlFlow, error)

	// SetResponseHeaders sets the response headers. This must be called before any
	// streaming responses are sent.
	SetResponseHeaders(headers map[string]string) error

	// Write writes a result to the response stream. The written items may not
	// be sent till Flush or Done is called.
	Write(arg *cherami.OutputHostCommand) error

	// Flush flushes headers (if they have not yet been sent) and any written results.
	Flush() error

	// Done closes the response stream and should be called after all results have been written.
	Done() error
}

// BStoreOpenAppendStreamInCall is the object used to stream arguments and write
// response headers for incoming calls.
type BStoreOpenAppendStreamInCall interface {
	// Read returns the next argument, if any is available. If there are no more arguments left,
	// it will return io.EOF.
	Read() (*store.AppendMessage, error)

	// SetResponseHeaders sets the response headers. This must be called before any
	// streaming responses are sent.
	SetResponseHeaders(headers map[string]string) error

	// Write writes a result to the response stream. The written items may not
	// be sent till Flush or Done is called.
	Write(arg *store.AppendMessageAck) error

	// Flush flushes headers (if they have not yet been sent) and any written results.
	Flush() error

	// Done closes the response stream and should be called after all results have been written.
	Done() error
}

// BStoreOpenAppendStreamOutCall is the object used to stream arguments/results and
// read response headers for outgoing calls.
type BStoreOpenAppendStreamOutCall interface {
	// Write writes an argument to the request stream. The written items may not
	// be sent till Flush or Done is called.
	Write(arg *store.AppendMessage) error

	// Flush flushes all written arguments.
	Flush() error

	// Done closes the request stream and should be called after all arguments have been written.
	Done() error

	// Read returns the next result, if any is available. If there are no more
	// results left, it will return io.EOF.
	Read() (*store.AppendMessageAck, error)

	// ResponseHeaders returns the response headers sent from the server. This will
	// block until server headers have been received.
	ResponseHeaders() (map[string]string, error)
}

// BStoreOpenReadStreamInCall is the object used to stream arguments and write
// response headers for incoming calls.
type BStoreOpenReadStreamInCall interface {
	// Read returns the next argument, if any is available. If there are no more arguments left,
	// it will return io.EOF.
	Read() (*cherami.ControlFlow, error)

	// SetResponseHeaders sets the response headers. This must be called before any
	// streaming responses are sent.
	SetResponseHeaders(headers map[string]string) error

	// Write writes a result to the response stream. The written items may not
	// be sent till Flush or Done is called.
	Write(arg *store.ReadMessageContent) error

	// Flush flushes headers (if they have not yet been sent) and any written results.
	Flush() error

	// Done closes the response stream and should be called after all results have been written.
	Done() error
}

// BStoreOpenReadStreamOutCall is the object used to stream arguments/results and
// read response headers for outgoing calls.
type BStoreOpenReadStreamOutCall interface {
	// Write writes an argument to the request stream. The written items may not
	// be sent till Flush or Done is called.
	Write(arg *cherami.ControlFlow) error

	// Flush flushes all written arguments.
	Flush() error

	// Done closes the request stream and should be called after all arguments have been written.
	Done() error

	// Read returns the next result, if any is available. If there are no more
	// results left, it will return io.EOF.
	Read() (*store.ReadMessageContent, error)

	// ResponseHeaders returns the response headers sent from the server. This will
	// block until server headers have been received.
	ResponseHeaders() (map[string]string, error)
}
