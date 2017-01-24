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
	"fmt"

	"github.com/gocql/gocql"
	"github.com/uber/cherami-server/common"
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/tchannel-go/thrift"
)

var unsupportedOperationError = &shared.InternalServiceError{Message: "Unsupported Operation"}

const (
	cqlCreateHostInfo     = "INSERT INTO host_info(hostname, key, value) VALUES(?,?,?)"
	cqlUpdateHostInfo     = "UPDATE host_info SET value=? WHERE hostname=? and key=?"
	cqlReadHostInfo       = "SELECT key,value from host_info where hostname=?"
	cqlDeleteHostInfo     = "DELETE FROM host_info where hostname=?"
	cqlDeleteHostInfoItem = "DELETE FROM host_info where hostname=? and key=?"
)

// CreateHostInfo adds a single hardware property for a single hostname
func (s *CassandraMetadataService) CreateHostInfo(ctx thrift.Context, request *m.CreateHostInfoRequest) error {

	var host = request.GetHostname()
	var props = request.GetProperties()

	if len(host) < 1 || len(props) < 1 {
		return &shared.BadRequestError{}
	}

	// all the updates are destined to the same
	// partition (hostname), so unlogged batches
	// does help i.e. all inserts go in one shot
	batch := s.session.NewBatch(gocql.UnloggedBatch)
	batch.Cons = s.midConsLevel

	for k, v := range props {
		batch.Query(cqlCreateHostInfo, host, k, v)
	}

	if err := s.session.ExecuteBatch(batch); err != nil {
		return &shared.InternalServiceError{Message: err.Error()}
	}

	return nil
}

// UpdateHostInfo updates a single hardware property for a single hostname
func (s *CassandraMetadataService) UpdateHostInfo(ctx thrift.Context, request *m.UpdateHostInfoRequest) error {

	var host = request.GetHostname()
	var props = request.GetProperties()

	if len(host) < 1 || len(props) < 1 {
		return &shared.BadRequestError{}
	}

	// all the updates are destined to the same
	// partition (hostname), so unlogged batches
	// does help i.e. all inserts go in one shot
	batch := s.session.NewBatch(gocql.UnloggedBatch)
	batch.Cons = s.midConsLevel

	for k, v := range props {
		batch.Query(cqlUpdateHostInfo, v, host, k)
	}

	if err := s.session.ExecuteBatch(batch); err != nil {
		return &shared.InternalServiceError{Message: err.Error()}
	}

	return nil
}

// ReadHostInfo returns list of hardware properties for a single hostname
func (s *CassandraMetadataService) ReadHostInfo(ctx thrift.Context, request *m.ReadHostInfoRequest) (*m.ReadHostInfoResult_, error) {

	var host = request.GetHostname()
	if len(host) < 1 {
		return nil, &shared.BadRequestError{}
	}

	iter := s.session.Query(cqlReadHostInfo, host).Consistency(s.lowConsLevel).Iter()
	if iter == nil {
		return nil, &shared.InternalServiceError{Message: "No data found"}
	}

	result := &m.ReadHostInfoResult_{
		Hostname:   request.Hostname,
		Properties: make(map[string]string),
	}

	var k, v string
	for iter.Scan(&k, &v) {
		result.Properties[k] = v
	}

	return result, nil
}

// DeleteHostInfo deletes all hardware properties associated with a
// single hostname. If a propertyKey is specified, this API will only
// delete the specific property
func (s *CassandraMetadataService) DeleteHostInfo(ctx thrift.Context, request *m.DeleteHostInfoRequest) error {

	var host = request.GetHostname()
	var propKey = request.GetPropertyKey()

	if len(host) < 1 {
		return &shared.BadRequestError{}
	}

	if len(propKey) > 0 {
		return s.session.Query(cqlDeleteHostInfoItem, host, propKey).Consistency(s.midConsLevel).Exec()
	}

	return s.session.Query(cqlDeleteHostInfo, host).Consistency(s.midConsLevel).Exec()
}

const (
	cqlCreateServiceConfig = `
		INSERT INTO 
		service_config(cluster, service_name, service_version, sku, hostname, config_key, config_value)
		VALUES (?, ?, ?, ?, ?, ?, ?)`

	cqlUpdateServiceConfig = `
		UPDATE service_config SET config_value=? where
		cluster=? and service_name=? and service_version=? and sku=? and hostname=? and config_key=?`

	cqlReadServiceConfig = `
		SELECT 
		service_version, sku, hostname, config_key, config_value 
		from service_config where cluster=? and service_name=?`

	cqlDeleteServiceConfig = `DELETE from service_config where cluster=? and service_name=?`
)

var errInvalidQueryFilter = &shared.BadRequestError{Message: "Invalid query filter parameters"}

func setServiceConfigDefaults(item *m.ServiceConfigItem) {
	if len(item.GetServiceVersion()) < 1 {
		item.ServiceVersion = common.StringPtr("*")
	}
	if len(item.GetSku()) < 1 {
		item.Sku = common.StringPtr("*")
	}
	if len(item.GetHostname()) < 1 {
		item.Hostname = common.StringPtr("*")
	}
}

func appendQueryFilters(stmt string, serviceVersion string, sku string, hostname string, configKey string) (string, error) {
	if len(serviceVersion) > 0 {
		stmt += fmt.Sprintf(" and service_version='%s'", serviceVersion)
		if len(sku) > 0 {
			stmt += fmt.Sprintf(" and sku='%s'", sku)
			if len(hostname) > 0 {
				stmt += fmt.Sprintf(" and hostname='%s'", hostname)
				if len(configKey) > 0 {
					stmt += fmt.Sprintf(" and config_key='%s'", configKey)
				}
			} else if len(configKey) > 0 {
				return "", errInvalidQueryFilter
			}
		} else if len(hostname) > 0 || len(configKey) > 0 {
			return "", errInvalidQueryFilter
		}
	} else if len(sku) > 0 || len(hostname) > 0 || len(configKey) > 0 {
		return "", errInvalidQueryFilter
	}
	return stmt, nil
}

func newConfigItem() *m.ServiceConfigItem {
	cItem := new(m.ServiceConfigItem)
	cItem.ServiceName = common.StringPtr("")
	cItem.ServiceVersion = common.StringPtr("")
	cItem.Sku = common.StringPtr("")
	cItem.Hostname = common.StringPtr("")
	cItem.ConfigKey = common.StringPtr("")
	cItem.ConfigValue = common.StringPtr("")
	return cItem
}

// CreateServiceConfig adds a single config value
func (s *CassandraMetadataService) CreateServiceConfig(ctx thrift.Context, request *m.CreateServiceConfigRequest) error {

	var item = request.GetConfigItem()
	if len(item.GetConfigKey()) < 1 || len(item.GetServiceName()) < 1 {
		return &shared.BadRequestError{Message: "Missing ConfigKey or ServiceName"}
	}

	setServiceConfigDefaults(item)

	if err := s.session.Query(cqlCreateServiceConfig,
		s.clusterName,
		item.GetServiceName(),
		item.GetServiceVersion(),
		item.GetSku(),
		item.GetHostname(),
		item.GetConfigKey(),
		item.GetConfigValue()).Consistency(s.midConsLevel).Exec(); err != nil {
		return &shared.InternalServiceError{Message: err.Error()}
	}

	return nil
}

// UpdateServiceConfig updates a single config value
func (s *CassandraMetadataService) UpdateServiceConfig(ctx thrift.Context, request *m.UpdateServiceConfigRequest) error {
	var item = request.GetConfigItem()
	if len(item.GetConfigKey()) < 1 || len(item.GetServiceName()) < 1 {
		return &shared.BadRequestError{Message: "Missing ConfigKey or ServiceName"}
	}

	setServiceConfigDefaults(item)

	if err := s.session.Query(cqlUpdateServiceConfig,
		item.GetConfigValue(),
		s.clusterName,
		item.GetServiceName(),
		item.GetServiceVersion(),
		item.GetSku(),
		item.GetHostname(),
		item.GetConfigKey()).Consistency(s.midConsLevel).Exec(); err != nil {
		return &shared.InternalServiceError{Message: err.Error()}
	}

	return nil
}

// ReadServiceConfig returns all config that matches the
// given set of input criteria. The returned result is a
// list of config key,values
func (s *CassandraMetadataService) ReadServiceConfig(ctx thrift.Context, request *m.ReadServiceConfigRequest) (*m.ReadServiceConfigResult_, error) {

	if len(request.GetServiceName()) < 1 {
		return nil, &shared.BadRequestError{Message: "Missing ServiceName"}
	}

	result := &m.ReadServiceConfigResult_{
		ConfigItems: make([]*m.ServiceConfigItem, 0),
	}

	cqlStmt, err := appendQueryFilters(cqlReadServiceConfig,
		request.GetServiceVersion(),
		request.GetSku(),
		request.GetHostname(),
		request.GetConfigKey())

	if err != nil {
		return nil, err
	}

	iter := s.session.Query(cqlStmt, s.clusterName, request.GetServiceName()).Consistency(s.lowConsLevel).Iter()

	cItem := newConfigItem()

	for iter.Scan(
		cItem.ServiceVersion,
		cItem.Sku,
		cItem.Hostname,
		cItem.ConfigKey,
		cItem.ConfigValue,
	) {
		cItem.ServiceName = request.ServiceName
		result.ConfigItems = append(result.ConfigItems, cItem)
		cItem = newConfigItem()
	}

	return result, nil
}

// DeleteServiceConfig deletes one or more config values matching the given criteria
func (s *CassandraMetadataService) DeleteServiceConfig(ctx thrift.Context, request *m.DeleteServiceConfigRequest) error {

	if len(request.GetServiceName()) < 1 {
		return &shared.BadRequestError{Message: "Missing ServiceName"}
	}

	cqlStmt, err := appendQueryFilters(cqlDeleteServiceConfig,
		request.GetServiceVersion(),
		request.GetSku(),
		request.GetHostname(),
		request.GetConfigKey())

	if err != nil {
		return err
	}

	if err := s.session.Query(cqlStmt,
		s.clusterName,
		request.GetServiceName()).Consistency(s.midConsLevel).Exec(); err != nil {
		return &shared.InternalServiceError{Message: err.Error()}
	}

	return nil
}
