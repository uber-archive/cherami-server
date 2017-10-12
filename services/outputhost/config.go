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
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/dconfig"
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
)

type (
	// OutputCgConfig is the per cg config used by the
	// cassandra config manager
	OutputCgConfig struct {
		// MessageCacheSize is used to configure the per CG cache size.
		// This is a string slice, where each entry is a tuple with the
		// destination/CG_name=value.
		// For example, we can ideally have two CGs for the same destination
		// with different size config as follows:
		// "/test/destination//test/cg_1=50,/test/destination//test/cg_2=100"
		MessageCacheSize []string `name:"messagecachesize" default:"/=10000"`
	}
)

// newConfigManager creates and returns a new instance
// of CassandraConfigManager.
func newConfigManager(mClient m.TChanMetadataService, logger bark.Logger) dconfig.ConfigManager {
	cfgTypes := map[string]interface{}{
		common.OutputServiceName: OutputCgConfig{},
	}
	return dconfig.NewCassandraConfigManager(mClient, cfgTypes, logger)
}
