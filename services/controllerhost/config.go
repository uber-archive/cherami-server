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

package controllerhost

import (
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/dconfig"
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
)

type (
	// StorePlacementConfig contains the config
	// parameters needed for store placement
	StorePlacementConfig struct {
		AdminStatus string `name:"adminStatus" default:"enabled"`
		// MinFreeDiskSpaceBytes represents the min
		// required free disk space on a store host
		// to host an extent. The default value
		// translates to 40GB which is 2 percent
		// for a 2TB drive
		MinFreeDiskSpaceBytes int64 `name:"minFreeDiskSpaceBytes" default:"40000000000"`
	}
	// InputPlacementConfig contains the config
	// parameters needed for inputhost placement
	InputPlacementConfig struct {
		AdminStatus string `name:"adminStatus" default:"enabled"`
	}
	// OutputPlacementConfig contains the config
	// parameters needed for outputhost placement
	OutputPlacementConfig struct {
		AdminStatus string `name:"adminStatus" default:"enabled"`
	}

	// ControllerDynamicConfig contains the config
	// parameters needed for controller
	ControllerDynamicConfig struct {
		NumPublisherExtentsByPath      []string `name:"numPublisherExtentsByPath" default:"/=4"`
		NumConsumerExtentsByPath       []string `name:"numConsumerExtentsByPath" default:"/=8"`
		NumRemoteConsumerExtentsByPath []string `name:"numRemoteConsumerExtentsByPath" default:"/=4"`

		// configs for multi_zone consumer group
		ActiveZone   string `name:"activeZone" default:""`
		FailoverMode string `name:"failoverMode" default:"disabled"`
	}
)

// newConfigManager creates and returns a new instance
// of CassandraConfigManager.
func newConfigManager(mClient m.TChanMetadataService, logger bark.Logger) dconfig.ConfigManager {
	cfgTypes := map[string]interface{}{
		common.InputServiceName:      InputPlacementConfig{},
		common.OutputServiceName:     OutputPlacementConfig{},
		common.StoreServiceName:      StorePlacementConfig{},
		common.ControllerServiceName: ControllerDynamicConfig{},
	}
	return dconfig.NewCassandraConfigManager(mClient, cfgTypes, logger)
}
