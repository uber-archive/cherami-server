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

package metadata

import (
	"github.com/uber/cherami-server/common"
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
)

func (s *CassandraSuite) TestHostInfoCRUD() {

	props := map[string]string{
		"sku":  "blackeye",
		"rack": "rack-1010",
		"az":   "zone2",
	}

	hostname := common.StringPtr("cherami-test-1")

	assertReadResult := func(props map[string]string) {
		rr := &m.ReadHostInfoRequest{Hostname: hostname}
		result, err := s.client.ReadHostInfo(nil, rr)
		s.Nil(err, "ReadHostInfo failed")
		s.Equal(result.GetHostname(), "cherami-test-1", "Wrong host name in result")
		s.Equal(len(props), len(result.GetProperties()), "Wrong number of properties for host")

		for k, v := range props {
			v1, ok := result.Properties[k]
			s.True(ok, "ReadHostInfo failed to return stored property")
			s.Equal(v, v1, "ReadHostInfo returned wrong value for property %v", k)
		}
	}

	cr := &m.CreateHostInfoRequest{
		Hostname:   hostname,
		Properties: props,
	}

	err := s.client.CreateHostInfo(nil, cr)
	s.Nil(err, "CreateHostInfo failed")
	assertReadResult(props)

	propsv2 := map[string]string{
		"sku":  "whitesnake",
		"rack": "rack-1111",
		"az":   "zone1",
	}

	ur := &m.UpdateHostInfoRequest{
		Hostname:   hostname,
		Properties: propsv2,
	}

	err = s.client.UpdateHostInfo(nil, ur)
	s.Nil(err, "UpdateHostInfo failed")
	assertReadResult(propsv2)

	dr := &m.DeleteHostInfoRequest{
		Hostname:    hostname,
		PropertyKey: common.StringPtr("rack"),
	}

	err = s.client.DeleteHostInfo(nil, dr)
	s.Nil(err, "DeleteHostInfo failed")
	delete(propsv2, "rack")
	assertReadResult(propsv2)

	dr.PropertyKey = common.StringPtr("")
	err = s.client.DeleteHostInfo(nil, dr)
	s.Nil(err, "DeleteHostInfo failed")
	assertReadResult(map[string]string{})
}

func (s *CassandraSuite) TestServiceConfigCRUD() {

	configItems := []m.ServiceConfigItem{
		{
			ServiceName:    common.StringPtr("input"),
			ServiceVersion: common.StringPtr("*"),
			Sku:            common.StringPtr("*"),
			Hostname:       common.StringPtr("*"),
			ConfigKey:      common.StringPtr("maxConns"),
			ConfigValue:    common.StringPtr("1000"),
		},
		{
			ServiceName:    common.StringPtr("input"),
			ServiceVersion: common.StringPtr("*"),
			Sku:            common.StringPtr("blackeye"),
			Hostname:       common.StringPtr("*"),
			ConfigKey:      common.StringPtr("maxConns"),
			ConfigValue:    common.StringPtr("5000"),
		},
		{
			ServiceName:    common.StringPtr("input"),
			ServiceVersion: common.StringPtr("2.0"),
			Sku:            common.StringPtr("*"),
			Hostname:       common.StringPtr("*"),
			ConfigKey:      common.StringPtr("maxConns"),
			ConfigValue:    common.StringPtr("2000"),
		},
		{
			ServiceName:    common.StringPtr("input"),
			ServiceVersion: common.StringPtr("2.0"),
			Sku:            common.StringPtr("blackeye"),
			Hostname:       common.StringPtr("*"),
			ConfigKey:      common.StringPtr("maxConns"),
			ConfigValue:    common.StringPtr("8000"),
		},
		{
			ServiceName:    common.StringPtr("output"),
			ServiceVersion: common.StringPtr("*"),
			Sku:            common.StringPtr("*"),
			Hostname:       common.StringPtr("*"),
			ConfigKey:      common.StringPtr("maxConns"),
			ConfigValue:    common.StringPtr("777"),
		},
		{
			ServiceName:    common.StringPtr("store"),
			ServiceVersion: common.StringPtr("*"),
			Sku:            common.StringPtr("kauri"),
			Hostname:       common.StringPtr("*"),
			ConfigKey:      common.StringPtr("maxConns"),
			ConfigValue:    common.StringPtr("444"),
		},
		{
			ServiceName:    common.StringPtr("store"),
			ServiceVersion: common.StringPtr("*"),
			Sku:            common.StringPtr("kauri"),
			Hostname:       common.StringPtr("cherami86-pek1"),
			ConfigKey:      common.StringPtr("adminStatus"),
			ConfigValue:    common.StringPtr("down"),
		},
	}

	cr := new(m.CreateServiceConfigRequest)
	for _, item := range configItems {
		cr.ConfigItem = &item
		err := s.client.CreateServiceConfig(nil, cr)
		s.Nil(err, "CreateServiceConfig failed")
	}

	newReadRequest := func(name string, version string, sku string, host string, key string) *m.ReadServiceConfigRequest {
		return &m.ReadServiceConfigRequest{
			ServiceName:    common.StringPtr(name),
			ServiceVersion: common.StringPtr(version),
			Sku:            common.StringPtr(sku),
			Hostname:       common.StringPtr(host),
			ConfigKey:      common.StringPtr(key),
		}
	}

	newDeleteRequest := func(name string, version string, sku string, host string, key string) *m.DeleteServiceConfigRequest {
		return &m.DeleteServiceConfigRequest{
			ServiceName:    common.StringPtr(name),
			ServiceVersion: common.StringPtr(version),
			Sku:            common.StringPtr(sku),
			Hostname:       common.StringPtr(host),
			ConfigKey:      common.StringPtr(key),
		}
	}

	assertItemsEqual := func(item1 *m.ServiceConfigItem, item2 *m.ServiceConfigItem) {
		s.Equal(item1.GetServiceName(), item2.GetServiceName(), "Service names mismatch")
		s.Equal(item1.GetServiceVersion(), item2.GetServiceVersion(), "Service version mismatch")
		s.Equal(item1.GetSku(), item2.GetSku(), "Sku mismatch")
		s.Equal(item1.GetHostname(), item2.GetHostname(), "Hostname mismatch")
		s.Equal(item1.GetConfigKey(), item2.GetConfigKey(), "Config key mismatch")
		s.Equal(item1.GetConfigValue(), item2.GetConfigValue(), "Config value mismatch")
	}

	type readTestCase struct {
		input  *m.ReadServiceConfigRequest
		output []*m.ServiceConfigItem
	}

	var readTestCases = []readTestCase{
		{
			input:  newReadRequest("input", "", "", "", ""),
			output: []*m.ServiceConfigItem{&configItems[0], &configItems[1], &configItems[2], &configItems[3]},
		},
		{
			input:  newReadRequest("input", "2.0", "", "", ""),
			output: []*m.ServiceConfigItem{&configItems[2], &configItems[3]},
		},
		{
			input:  newReadRequest("input", "2.0", "blackeye", "", ""),
			output: []*m.ServiceConfigItem{&configItems[3]},
		},
		{
			input:  newReadRequest("input", "*", "*", "*", "maxConns"),
			output: []*m.ServiceConfigItem{&configItems[0]},
		},
		{
			input:  newReadRequest("input", "*", "blackeye", "*", "maxConns"),
			output: []*m.ServiceConfigItem{&configItems[1]},
		},
		{
			input:  newReadRequest("input", "2.0", "blackeye", "*", "maxConns"),
			output: []*m.ServiceConfigItem{&configItems[3]},
		},
		{
			input:  newReadRequest("output", "", "", "", ""),
			output: []*m.ServiceConfigItem{&configItems[4]},
		},
		{
			input:  newReadRequest("store", "*", "kauri", "*", "maxConns"),
			output: []*m.ServiceConfigItem{&configItems[5]},
		},
		{
			input:  newReadRequest("store", "*", "kauri", "*", "adminStatus"),
			output: []*m.ServiceConfigItem{},
		},
		{
			input:  newReadRequest("store", "*", "kauri", "cherami86-pek1", "adminStatus"),
			output: []*m.ServiceConfigItem{&configItems[6]},
		},
	}

	for i, ts := range readTestCases {
		ans, err := s.client.ReadServiceConfig(nil, ts.input)
		s.Nil(err, "ReadServiceConfig failed for input %d", i)
		s.Equal(len(ts.output), len(ans.GetConfigItems()), "ReadServiceConfig returned wrong number of items for input %d", i)
		if len(ans.GetConfigItems()) == 1 {
			assertItemsEqual(ts.output[0], ans.GetConfigItems()[0])
		}
	}

	for i, item := range configItems {
		item.ConfigValue = common.StringPtr(*item.ConfigValue + "1")
		// We updated the config value above. Make sure the expectation is updated as well!
		configItems[i].ConfigValue = item.ConfigValue
		ur := &m.UpdateServiceConfigRequest{ConfigItem: &item}
		err := s.client.UpdateServiceConfig(nil, ur)
		s.Nil(err, "UpdateServiceConfig failed for input %d", i)
	}

	for i, ts := range readTestCases {
		ans, err := s.client.ReadServiceConfig(nil, ts.input)
		s.Nil(err, "ReadServiceConfig failed for input %d", i)
		s.Equal(len(ts.output), len(ans.GetConfigItems()), "ReadServiceConfig returned wrong number of items for input %d", i)
		if len(ans.GetConfigItems()) == 1 {
			assertItemsEqual(ts.output[0], ans.GetConfigItems()[0])
		}
	}

	dr := newDeleteRequest("store", "", "", "", "")
	err := s.client.DeleteServiceConfig(nil, dr)
	s.Nil(err, "DeleteServiceConfig failed")
	ans, err := s.client.ReadServiceConfig(nil, newReadRequest("store", "", "", "", ""))
	s.Nil(err, "ReadServiceConfig failed")
	s.Equal(0, len(ans.GetConfigItems()), "DeleteServiceConfig failed delete all records")

	dr = newDeleteRequest("input", "*", "*", "*", "maxConns")
	err = s.client.DeleteServiceConfig(nil, dr)
	s.Nil(err, "DeleteServiceConfig failed")
	ans, err = s.client.ReadServiceConfig(nil, newReadRequest("input", "*", "*", "*", "maxConns"))
	s.Nil(err, "ReadServiceConfig failed")
	s.Equal(0, len(ans.GetConfigItems()), "DeleteServiceConfig failed delete a record")
}
