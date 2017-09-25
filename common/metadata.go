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
	"strings"

	"github.com/uber/cherami-thrift/.generated/go/shared"
)

// IsDLQDestination checks whether a destination is dlq type
func IsDLQDestination(dstDesc *shared.DestinationDescription) bool {
	return dstDesc != nil && IsDLQDestinationPath(dstDesc.GetPath())
}

// IsDLQDestinationPath checks whether a destination path is dlq type
func IsDLQDestinationPath(path string) bool {
	return len(path) > 4 && strings.HasSuffix(path, ".dlq")
}

// IsKafkaConsumerGroupExtent determines if a consumer group extent is a Kafka consumption assignment
func IsKafkaConsumerGroupExtent(e *shared.ConsumerGroupExtent) bool {
	return AreKafkaPhantomStores(e.GetStoreUUIDs())
}
