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
	"math"
	"time"

	"github.com/uber/cherami-client-go/common"
)

// Extent sequence const
const (
	// SequenceBegin refers to the beginning of an extent
	SequenceBegin = 0
	// SequenceEnd refers to the end of an extent
	SequenceEnd = math.MaxInt64
	// CallerUserName is the name of thrift context header contains current user name
	CallerUserName = "user-name"
	// CallerHostName is the name of thrift context header contains current host name
	CallerHostName = "host-name"
	// CallerServiceName is the name of thrift context header contains current service name
	CallerServiceName = "cn"
)

// ServiceToPort is service name to ports mapping
// This map should be syced with the port nums in config file and use by command line.
// We need this because for command line, we can not generated the config path automatically.
var ServiceToPort = map[string]string{
	InputServiceName:      "4240",
	OutputServiceName:     "4254",
	StoreServiceName:      "4253",
	FrontendServiceName:   "4922",
	ControllerServiceName: "5425",
	ReplicatorServiceName: "6280",
}

const (
	// DLQMaxMergeAge is the maximum time that we expect a partition to exist
	DLQMaxMergeAge = common.UnixNanoTime(int64(time.Hour)) * 24 * 1 // one day

	// TenancyProd is the tenancy of production
	// Deployment name can be in the format of <tenancy>_<zone>
	TenancyProd = "prod"

	// InputHostForRemoteExtent is a special (and fake) input host ID for remote extent
	InputHostForRemoteExtent = "88888888-8888-8888-8888-888888888888"

	// ZeroUUID defines a UUID that is all zeroes
	ZeroUUID = "00000000-0000-0000-0000-000000000000"

	// KafkaPhantomExtentInputhost is placeholder/phantom inputhost uuid used for Kafka extents
	KafkaPhantomExtentInputhost = "00000000-0000-0000-0000-000000000000"

	// KafkaPhantomExtentStorehost is placeholder/phantom storehost uuid used for Kafka extents
	KafkaPhantomExtentStorehost = "00000000-0000-0000-0000-000000000000"
)

const (
	// FlagDisableNackThrottling is the flag string for disabling Nack throttling
	FlagDisableNackThrottling = "disable_nack_throttling"

	// FlagEnableSmartRetry is the flag string for enabling smart retry
	FlagEnableSmartRetry = "enable_smart_retry"

	// FlagEnableQueueDepthTabulation is the flag string for enabling queue depth tabulation
	FlagEnableQueueDepthTabulation = "enable_queue_depth_tabulation"
)
