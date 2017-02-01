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

// This file holds all the default values which will eventually be dynamically
// tunable using our cassandra backed config.

import (
	"time"
)

// some default values for the limits
// TODO: this will be moved behind a separate "limits" interface which is also dynamically tunable
const (
	// MaxHostOverallConn is the maximam overall connection limit for this host
	// TODO: Need to figure out the suitable values
	MaxHostOverallConn = 100000
	// HostOverallConnLimit is the overall connection limit for this host
	HostOverallConnLimit = 10000

	// MaxHostPerSecondConn is the maximam per second  rate limit for this host
	// TODO: Need to figure out the suitable values
	MaxHostPerSecondConn = 10000
	// HostPerSecondConnLimit is the per second rate limit for this host
	HostPerSecondConnLimit = 1000

	//MaxHostPerConnMsgsLimitPerSecond is the maximam for per connection messages limit
	// TODO: Need to figure out the suitable values
	MaxHostPerConnMsgsLimitPerSecond = 800000
	// HostPerConnMsgsLimitPerSecond is the per connection messages limit
	HostPerConnMsgsLimitPerSecond = 80000

	//MaxHostPerExtentMsgsLimitPerSecond is the maximam for per extent messages limit
	// TODO: Need to figure out the suitable values
	MaxHostPerExtentMsgsLimitPerSecond = 200000
	// HostPerExtentMsgsLimitPerSecond is the per extent messages limit
	HostPerExtentMsgsLimitPerSecond = 20000

	// MaxHostMaxConnPerDestination is the maximam for max connections per destination
	// TODO: Need to figure out the suitable values
	MaxHostMaxConnPerDestination = 10000
	// HostMaxConnPerDestination is the max connections per destination
	HostMaxConnPerDestination = 1000
)

// Flush stream thresholds; this is used by the "pumps" that wrap the websocket-stream
// and provide go-channel interface to read/write from the stream. the flush thresholds
// below control how often we do a "Flush" on the tchannel-stream.
// Currently configured for every 64 messages sent or every 5 milliseconds (whichever is sooner)
const (
	FlushThreshold int           = 64
	FlushTimeout   time.Duration = 5 * time.Millisecond
)
