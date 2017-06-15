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

package dconfig

import (
	"os"
	"reflect"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	mc "github.com/uber/cherami-server/clients/metadata"
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
)

type (
	ConfigManagerTestSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
		cdb    *mc.TestCluster
		logger bark.Logger
	}

	configGetterTestInput struct {
		svc  string
		vers string
		sku  string
		host string
	}
)

func TestConfigManagerSuite(t *testing.T) {
	suite.Run(t, new(ConfigManagerTestSuite))
}

func (s *ConfigManagerTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	if testing.Verbose() {
		logrus.SetOutput(os.Stdout)
	}

	s.logger = (bark.NewLoggerFromLogrus(logrus.New())).WithField("testSuite", "ConfigManagerTestSuite")
	s.cdb = &mc.TestCluster{}
	s.cdb.SetupTestCluster()
}

func (s *ConfigManagerTestSuite) TestDefaults() {

	type testConfig1 struct {
		IntItem     int      `name:"int-item" default:"50"`
		Int64Item   int64    `name:"int64-item" default:"32767"`
		Float64Item float64  `name:"float64-item" default:"15.64"`
		SliceItem   []string `name:"states" default:"a,b,c"`
		CaseItem    string   `name:"camelCase" default:"Case"` // intentionally make sure the value is caseSensitive
	}

	configTypes := make(map[string]interface{})
	configTypes["test"] = testConfig1{}

	cfgMgr := NewCassandraConfigManager(s.cdb.GetClient(), configTypes, s.logger)

	inputs := []configGetterTestInput{
		{"test", "", "", ""},
		{"test", "*", "", ""},
		{"test", "", "*", ""},
		{"test", "", "", "*"},
		{"test", "*", "*", "*"},
		{"test", "v1", "", ""},
		{"test", "v1", "sku1", ""},
		{"test", "v1", "sku1", "host1"},
	}

	for i, in := range inputs {
		cfg, err := cfgMgr.Get(in.svc, in.vers, in.sku, in.host)
		s.Nil(err, "configStore.Get() failed for input %v", i)
		value := cfg.(testConfig1)
		s.Equal(50, value.IntItem, "Wrong int value for input %v", i)
		s.Equal(int64(32767), value.Int64Item, "Wrong int64 value for input %v", i)
		s.Equal(15.64, value.Float64Item, "Wrong float64 value for input %v", i)
		s.Equal("Case", value.CaseItem, "Wrong valuse for CaseItem %v", i)
		s.True(reflect.DeepEqual([]string{"a", "b", "c"}, value.SliceItem), "Wrong slice value for input %v", i)
	}
}

func strp(val string) *string {
	return &val
}

func (s *ConfigManagerTestSuite) TestConfigOverrides() {

	type testInputConfig1 struct {
		MaxConns    int64  `name:"max-conns" default:"777"`
		MaxExtents  int64  `name:"max-extents" default:"13"`
		AdminStatus string `name:"admin-status" default:"enabled"`
	}

	type testStoreConfig1 struct {
		MaxInConns  int64    `name:"max-inconns" default:"111"`
		MaxOutConns int64    `name:"max-outconns" default:"222"`
		AdminStatus string   `name:"admin-status" default:"enabled"`
		CamelCase   string   `name:"camelCase" default:"Case"`
		SliceItem   []string `name:"slice-set" default:"a,b,c"`
	}

	type testOutputConfig1 struct {
		CacheSizeBytes int64  `name:"cache-size-bytes" default:"1024"`
		AdminStatus    string `name:"admin-status" default:"enabled"`
	}

	cfgItems := []m.ServiceConfigItem{
		{ServiceName: strp("input"), ServiceVersion: strp("*"), Sku: strp("*"), Hostname: strp("*"), ConfigKey: strp("max-extents"), ConfigValue: strp("100")},
		{ServiceName: strp("input"), ServiceVersion: strp("*"), Sku: strp("*"), Hostname: strp("*"), ConfigKey: strp("max-conns"), ConfigValue: strp("1000")},
		{ServiceName: strp("input"), ServiceVersion: strp("*"), Sku: strp("whitesnake"), Hostname: strp("*"), ConfigKey: strp("max-extents"), ConfigValue: strp("150")},
		{ServiceName: strp("input"), ServiceVersion: strp("*"), Sku: strp("whitesnake"), Hostname: strp("cherami14"), ConfigKey: strp("max-extents"), ConfigValue: strp("0")},
		{ServiceName: strp("input"), ServiceVersion: strp("v1"), Sku: strp("*"), Hostname: strp("*"), ConfigKey: strp("max-extents"), ConfigValue: strp("50")},
		{ServiceName: strp("input"), ServiceVersion: strp("v1"), Sku: strp("m1.12"), Hostname: strp("cherami15"), ConfigKey: strp("max-extents"), ConfigValue: strp("1")},
		{ServiceName: strp("input"), ServiceVersion: strp("v2"), Sku: strp("blackeye"), Hostname: strp("*"), ConfigKey: strp("max-extents"), ConfigValue: strp("200")},
		{ServiceName: strp("input"), ServiceVersion: strp("v2"), Sku: strp("whitesnake"), Hostname: strp("*"), ConfigKey: strp("max-extents"), ConfigValue: strp("170")},
		{ServiceName: strp("store"), ServiceVersion: strp("*"), Sku: strp("*"), Hostname: strp("*"), ConfigKey: strp("max-inconns"), ConfigValue: strp("2000")},
		{ServiceName: strp("store"), ServiceVersion: strp("*"), Sku: strp("*"), Hostname: strp("*"), ConfigKey: strp("max-outconns"), ConfigValue: strp("1000")},
		{ServiceName: strp("store"), ServiceVersion: strp("*"), Sku: strp("*"), Hostname: strp("*"), ConfigKey: strp("camelcase"), ConfigValue: strp("normalize")}, // intentionally use a normalized key to set in cassandra
		{ServiceName: strp("store"), ServiceVersion: strp("*"), Sku: strp("*"), Hostname: strp("*"), ConfigKey: strp("slice-set"), ConfigValue: strp("Z")},
		{ServiceName: strp("output"), ServiceVersion: strp("*"), Sku: strp("*"), Hostname: strp("cherami22"), ConfigKey: strp("admin-status"), ConfigValue: strp("disabled")},
		{ServiceName: strp("output"), ServiceVersion: strp("*"), Sku: strp("*"), Hostname: strp("*"), ConfigKey: strp("cache-size-bytes"), ConfigValue: strp("10000")},
		{ServiceName: strp("output"), ServiceVersion: strp("*"), Sku: strp("haswell"), Hostname: strp("*"), ConfigKey: strp("cache-size-bytes"), ConfigValue: strp("20000")},
		{ServiceName: strp("output"), ServiceVersion: strp("v2"), Sku: strp("*"), Hostname: strp("*"), ConfigKey: strp("cache-size-bytes"), ConfigValue: strp("15000")},
		{ServiceName: strp("output"), ServiceVersion: strp("v2"), Sku: strp("skylake"), Hostname: strp("*"), ConfigKey: strp("cache-size-bytes"), ConfigValue: strp("12000")},
	}

	for i, item := range cfgItems {
		req := &m.CreateServiceConfigRequest{ConfigItem: &item}
		err := s.cdb.GetClient().CreateServiceConfig(nil, req)
		s.Nil(err, "Failed to add service config item %v", i)
	}

	type inputTestCase1 struct {
		input  configGetterTestInput
		output testInputConfig1
	}

	type storeTestCase1 struct {
		input  configGetterTestInput
		output testStoreConfig1
	}

	type outputTestCase1 struct {
		input  configGetterTestInput
		output testOutputConfig1
	}

	inputTestCases := []inputTestCase1{
		{configGetterTestInput{svc: "input"}, testInputConfig1{MaxConns: 1000, MaxExtents: 100, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "input", sku: "whitesnake"}, testInputConfig1{MaxConns: 1000, MaxExtents: 150, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "input", sku: "whitesnake", host: "cherami14"}, testInputConfig1{MaxConns: 1000, MaxExtents: 0, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "input", vers: "v1"}, testInputConfig1{MaxConns: 1000, MaxExtents: 50, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "input", vers: "v1", sku: "whitesnake"}, testInputConfig1{MaxConns: 1000, MaxExtents: 50, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "input", vers: "v1", sku: "whitesnake", host: "cherami14"}, testInputConfig1{MaxConns: 1000, MaxExtents: 0, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "input", vers: "v1", sku: "m1.b ", host: "cherami15"}, testInputConfig1{MaxConns: 1000, MaxExtents: 1, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "input", vers: "v1", sku: "m1.12", host: "cherami14"}, testInputConfig1{MaxConns: 1000, MaxExtents: 0, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "input", vers: "v1", sku: "k1.12"}, testInputConfig1{MaxConns: 1000, MaxExtents: 50, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "input", vers: "v2"}, testInputConfig1{MaxConns: 1000, MaxExtents: 100, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "input", vers: "v2", sku: "blackeye"}, testInputConfig1{MaxConns: 1000, MaxExtents: 200, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "input", vers: "v2", sku: "blackeye", host: "cherami20"}, testInputConfig1{MaxConns: 1000, MaxExtents: 200, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "input", vers: "v2", sku: "whitesnake"}, testInputConfig1{MaxConns: 1000, MaxExtents: 170, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "input", vers: "v2", sku: "whitesnake", host: "cherami60"}, testInputConfig1{MaxConns: 1000, MaxExtents: 170, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "input", vers: "v3", sku: "whitesnake", host: "cherami55"}, testInputConfig1{MaxConns: 1000, MaxExtents: 150, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "input", vers: "v3"}, testInputConfig1{MaxConns: 1000, MaxExtents: 100, AdminStatus: "enabled"}},
	}

	storeTestCases := []storeTestCase1{
		{configGetterTestInput{svc: "store"}, testStoreConfig1{MaxInConns: 2000, MaxOutConns: 1000, AdminStatus: "enabled", CamelCase: "normalize", SliceItem: []string{"Z"}}},
		{configGetterTestInput{svc: "store", vers: "*", sku: "*", host: "*"}, testStoreConfig1{MaxInConns: 2000, MaxOutConns: 1000, AdminStatus: "enabled", CamelCase: "normalize", SliceItem: []string{"Z"}}},
		{configGetterTestInput{svc: "store", vers: "v1"}, testStoreConfig1{MaxInConns: 2000, MaxOutConns: 1000, AdminStatus: "enabled", CamelCase: "normalize", SliceItem: []string{"Z"}}},
		{configGetterTestInput{svc: "store", vers: "v1", sku: "sku1"}, testStoreConfig1{MaxInConns: 2000, MaxOutConns: 1000, AdminStatus: "enabled", CamelCase: "normalize", SliceItem: []string{"Z"}}},
		{configGetterTestInput{svc: "store", vers: "v1", sku: "sku1", host: "host1"}, testStoreConfig1{MaxInConns: 2000, MaxOutConns: 1000, AdminStatus: "enabled", CamelCase: "normalize", SliceItem: []string{"Z"}}},
	}

	outputTestCases := []outputTestCase1{
		{configGetterTestInput{svc: "output"}, testOutputConfig1{CacheSizeBytes: 10000, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "output", sku: "haswell"}, testOutputConfig1{CacheSizeBytes: 20000, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "output", vers: "v2"}, testOutputConfig1{CacheSizeBytes: 15000, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "output", vers: "v2", sku: "haswell"}, testOutputConfig1{CacheSizeBytes: 15000, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "output", vers: "v2", sku: "skylake"}, testOutputConfig1{CacheSizeBytes: 12000, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "output", vers: "v2", sku: "ivy"}, testOutputConfig1{CacheSizeBytes: 15000, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "output", vers: "v1", sku: "haswell"}, testOutputConfig1{CacheSizeBytes: 20000, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "output", vers: "v1", sku: "ivy"}, testOutputConfig1{CacheSizeBytes: 10000, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "output", host: "cherami22"}, testOutputConfig1{CacheSizeBytes: 10000, AdminStatus: "disabled"}},
		{configGetterTestInput{svc: "output", host: "cherami44"}, testOutputConfig1{CacheSizeBytes: 10000, AdminStatus: "enabled"}},
		{configGetterTestInput{svc: "output", vers: "v2", sku: "haswell", host: "cherami22"}, testOutputConfig1{CacheSizeBytes: 15000, AdminStatus: "disabled"}},
		{configGetterTestInput{svc: "output", vers: "v1", sku: "ivy", host: "cherami22"}, testOutputConfig1{CacheSizeBytes: 10000, AdminStatus: "disabled"}},
		{configGetterTestInput{svc: "output", sku: "haswell", host: "cherami22"}, testOutputConfig1{CacheSizeBytes: 20000, AdminStatus: "disabled"}},
		{configGetterTestInput{svc: "output", host: "cherami22"}, testOutputConfig1{CacheSizeBytes: 10000, AdminStatus: "disabled"}},
	}

	configTypes := make(map[string]interface{})
	configTypes["input"] = testInputConfig1{}
	configTypes["store"] = testStoreConfig1{}
	configTypes["output"] = testOutputConfig1{}

	cfgMgrIface := NewCassandraConfigManager(s.cdb.GetClient(), configTypes, s.logger)
	cfgMgr := cfgMgrIface.(*CassandraConfigManager)
	cfgMgr.refresh() // force refresh from cassandra

	for i, tc := range inputTestCases {
		cfg, err := cfgMgr.Get(tc.input.svc, tc.input.vers, tc.input.sku, tc.input.host)
		s.Nil(err, "configStore.Get() failed for input %v", i)
		value := cfg.(testInputConfig1)
		s.True(reflect.DeepEqual(tc.output, value), "Wrong output for input %v, output=%+v", i, value)
	}

	for i, tc := range storeTestCases {
		cfg, err := cfgMgr.Get(tc.input.svc, tc.input.vers, tc.input.sku, tc.input.host)
		s.Nil(err, "configStore.Get() failed for input %v", i)
		value := cfg.(testStoreConfig1)
		s.True(reflect.DeepEqual(tc.output, value), "Wrong output for input %v, output=%+v", i, value)
	}

	for i, tc := range outputTestCases {
		cfg, err := cfgMgr.Get(tc.input.svc, tc.input.vers, tc.input.sku, tc.input.host)
		s.Nil(err, "configStore.Get() failed for input %v", i)
		value := cfg.(testOutputConfig1)
		s.True(reflect.DeepEqual(tc.output, value), "Wrong output for input %v, output=%+v", i, value)
	}
}
