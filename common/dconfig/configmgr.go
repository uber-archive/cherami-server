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
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
)

type (
	// ConfigManager refers to any implementation
	// that vends config, backed by a config store
	// which supports a hierarchial config structure
	// with 4 pre-defined levels :
	// (service, serviceVersion, sku, hostname)
	ConfigManager interface {
		// Get fetches and returns a config object that
		// best matches the specified input criteria. The
		// input parameters refer to the 4 levels in the
		// config hierarchy
		Get(svc string, version string, sku string, host string) (interface{}, error)

		//Start starts the config manager
		Start()

		//Stop stops the running config manager
		Stop()
	}

	// CassandraConfigManager is an implementation of
	// ConfigManager backed by a config store that's
	// built on top of Cassandra.
	CassandraConfigManager struct {
		configValues atomic.Value           // *configTree, copy on write
		configTypes  map[string]interface{} // service name to config object type
		mClient      m.TChanMetadataService
		started      int64
		shutdownC    chan struct{}
		shutdownWG   sync.WaitGroup
		logger       bark.Logger
	}

	// kvTreeNode refers to a node within the
	// config tree whose value is a set of
	// key values at that level
	kvTreeNode struct {
		keyValues map[string]string      // raw config values stored in config store
		children  map[string]*kvTreeNode // pointers to the next level in the tree
	}

	// kvTree refers to a config tree that
	// stores config parameters as key value
	// strings. This data structure holds the
	// raw config data retrieved from the
	// backing config store (cassandra)
	kvTree struct {
		children map[string]*kvTreeNode
	}

	// configTreeNode refers to a node within
	// a config tree whose value is a config
	// object
	configTreeNode struct {
		value    interface{}                // config object, type specified in constructor
		children map[string]*configTreeNode // pointers to next level
	}

	// configTree refers to a config tree
	// that stores parameters as config
	// objects. This data structure is
	// constructed out of kvTree and its
	// values are of the same type as
	// expected by the caller
	configTree struct {
		children map[string]*configTreeNode
	}
)

const (
	wildcardToken   = "*"
	sliceSplitToken = ","
)

var cfgRefreshInterval = time.Minute

// NewCassandraConfigManager constructs and returns an
// implementation of config managed backed by a store
// that's built on top of cassandra. The underlying
// config store supports configuration of key,values
// in a tree like structure with 4 pre-defined levels:
//
// serviceName.serviceVersion.sku.hostname.key=value
//
//  where
//     serviceName is the name of the service (ex - inputhost,outputhost, etc)
//     serviceVersion identifes software version (ex -  manyrocks, v1.0 etc)
//     sku identifies the hardware sku (ex - ec2.m1.12)
//     hostname identifies a specific host (ex - cherami14)
//
// The underlying config store supports wildcards to specify
// default values that apply to a broad category, subject to
// the following constraints:
//
//  (1) if hostname is specified, sku is always ignored (i.e. treated as wildcard)
//
// Config Evaluation:
//   In general, the config evaluator ALWAYS returns the most specific config
//  value that matches the given input constraints. The config levels from
//  most to least specific are (hostname, sku, serviceVersion, serviceName)
//
//  Example config:
//       inputhost.*.*.*.maxExtents=111
//       inputhost.*.ec2m112.*.maxExtents=222
//       inputhost.v1.*.*.maxExtents=333
//       inputhost.v1.m1large.*.maxExtents=444
//
//  Example Queries:
//       Get(inputhost, *, *, *)        = 111
//       Get(inputhost, v1, *, *)       = 333
//       Get(inputhost, v1, ec2m112, *) = 333
//       Get(inputhost, v1, m1large, *) = 444
//       Get(inputhost, v2, ec2m112, *) = 222 <-- SKU default applied
//
// The constructor to this class takes a configTypes param, which is a map
// of serviceName to configObjects. The config object MUST be of struct type
// and all members must be PUBLIC. The ConfigObject supports two pre-defined
// struct tags - name and default. name refers to the name of the config on
// the underlying store and default refers to the default value for that config
// param. Following are the data types supported for config params within the struct:
//
//   * Int64
//   * Float64
//   * String
//   * String slice ([]string - raw config value must be a csv)
//
//
//  Example ConfigObject and config manager initialization
//
//     type InputConfig struct {
//			MaxExtents  int64   `name:"max-extents" default:"50"`
//  		AdminStatus string  `name:"admin-status" default:"enabled"`
//     }
//
//     cfgTypes := []map[string]interface{}{"inputhost": InputConfig{}}
//     cfgMgr := NewCassandraConfigManager(mClient, cfgTypes, logger)
//
// Automatic Refresh:
//   The returned config manager automatically syncs with the underlying
//   config store every minute. Callers must AVOID caching the result of
//   configManager.Get() to be able to respond to config changes.
//
// Future Work:
//   Notifications to observers on config changes
func NewCassandraConfigManager(mClient m.TChanMetadataService, configTypes map[string]interface{}, logger bark.Logger) ConfigManager {
	cfgMgr := new(CassandraConfigManager)
	cfgMgr.mClient = mClient
	cfgMgr.configTypes = configTypes
	cfgMgr.shutdownC = make(chan struct{})
	cfgMgr.shutdownWG = sync.WaitGroup{}
	cfgMgr.logger = logger
	cfgMgr.configValues.Store(cfgMgr.mkDefaultTree()) // must be last line
	return cfgMgr
}

// Start starts the config manager
func (cfgMgr *CassandraConfigManager) Start() {
	if !atomic.CompareAndSwapInt64(&cfgMgr.started, 0, 1) {
		return
	}
	cfgMgr.shutdownWG.Add(1)
	go cfgMgr.runLoop()
}

// Stop stops the config manager
func (cfgMgr *CassandraConfigManager) Stop() {
	close(cfgMgr.shutdownC)
	if !common.AwaitWaitGroup(&cfgMgr.shutdownWG, time.Second) {
		cfgMgr.logger.Warn("Stop() - Timed out waiting for config refresher to stop")
	}
}

func (cfgMgr *CassandraConfigManager) runLoop() {
	quit := false
	refreshTimeout := time.After(0)
	cfgMgr.logger.Info("Config refresher started")

	for !quit {
		select {
		case <-refreshTimeout:
			cfgMgr.refresh()
			refreshTimeout = time.After(cfgRefreshInterval)
		case <-cfgMgr.shutdownC:
			quit = true
		}
	}

	cfgMgr.shutdownWG.Done()
	cfgMgr.logger.Info("Config refresher stopped")
}

// Get returns the config value that best matches the given
// input criteria. svc param cannot be empty.
func (cfgMgr *CassandraConfigManager) Get(svc string, version string, sku string, host string) (interface{}, error) {

	tree := cfgMgr.configValues.Load().(*configTree)

	curr, ok := tree.children[svc]
	if !ok {
		return nil, fmt.Errorf("Unknown service %v", svc)
	}

	for _, k := range []string{version, sku, host} {

		if len(k) < 1 {
			k = "*"
		}

		next, hasNext := curr.children[k]
		if !hasNext {
			if k == wildcardToken {
				break
			}
			// if we don't have this value, substitute wildcard & retry
			next, hasNext = curr.children[wildcardToken]
			if !hasNext {
				break
			}
		}
		curr = next
	}

	return curr.value, nil
}

func (cfgMgr *CassandraConfigManager) refresh() {

	var svcToResult = make(map[string]*m.ReadServiceConfigResult_)

	for svc := range cfgMgr.configTypes {
		req := &m.ReadServiceConfigRequest{ServiceName: common.StringPtr(svc)}
		result, err := cfgMgr.mClient.ReadServiceConfig(nil, req)
		if err != nil {
			cfgMgr.logger.WithField(common.TagErr, err).Error("ReadServiceConfig failed")
			return
		}
		svcToResult[svc] = result
	}

	tree := cfgMgr.mkConfigTree(cfgMgr.mkKVTree(svcToResult))
	cfgMgr.configValues.Store(tree)
}

func (cfgMgr *CassandraConfigManager) mkDefaultTree() *configTree {
	tree := new(configTree)
	tree.children = make(map[string]*configTreeNode)
	for k, v := range cfgMgr.configTypes {
		tree.children[k] = new(configTreeNode)
		tree.children[k].value = cfgMgr.mkConfig(v, nil, map[string]string{})
		tree.children[k].children = make(map[string]*configTreeNode)
	}
	return tree
}

func (cfgMgr *CassandraConfigManager) mkKVTree(input map[string]*m.ReadServiceConfigResult_) *kvTree {
	tree := new(kvTree)
	tree.children = make(map[string]*kvTreeNode)
	for k, v := range input {
		tree.children[k] = cfgMgr.mkKVTreeForSvc(k, v.GetConfigItems())
	}
	return tree
}

func (cfgMgr *CassandraConfigManager) mkConfigTree(inputKVTree *kvTree) *configTree {

	result := new(configTree)
	result.children = make(map[string]*configTreeNode)

	for k, v := range cfgMgr.configTypes {
		tree := inputKVTree.children[k]
		result.children[k] = cfgMgr.mkConfigTreeForSvc(v, nil, tree)
	}

	return result
}

func (cfgMgr *CassandraConfigManager) mkConfigTreeForSvc(cfgType interface{}, defaults interface{}, inputTree *kvTreeNode) *configTreeNode {

	result := new(configTreeNode)
	result.children = make(map[string]*configTreeNode)

	result.value = cfgMgr.mkConfig(cfgType, defaults, inputTree.keyValues)

	for k, v := range inputTree.children {
		result.children[k] = cfgMgr.mkConfigTreeForSvc(cfgType, result.value, v)
	}

	return result
}

// mkKVTreeForSvc builds a hierarchical tree of key values
// for a speicific service. The tree hierarchy is from
// svc -> version -> sku -> host. The key values at each
// level serve as the default for its children nodes.
//
// The actual construction of the tree is a 3 step process:
// (1) Take every config item of form svc.version.sku.host.key=value and add it to tree
//        (a) If the version & sku are wildcard (i.e. *), then the config item
//            MUST serve as a default for every version/ every sku, so keep track of
//            these separately
//        (b) If the version is a wildcard but sku is not, then this config item
//			  MUST serve as a default for every version.sku, so trace this separately
//        (c) If the version is not wildcard, but sku is wildcard, then the
//            config item MUST serve as a default for every sku for that version
//            so keep track of these separately
// (2) Add the items tracked in 1(b) to the tree (for every sku)
// (3) Add the items tracked in 1(a) for every version
//
// Its important to apply the config items in the order (1).(2).(3) to honor
// the hierarchial structure
func (cfgMgr *CassandraConfigManager) mkKVTreeForSvc(service string, items []*m.ServiceConfigItem) *kvTreeNode {

	root := newKVTreeNode()

	allSkus := make(map[string]struct{})
	allVersions := make(map[string]struct{})
	wildcardVersionItems := make([]*m.ServiceConfigItem, 0, len(items))              // items with version as wildcard
	wildcardVersionSkuItems := make([]*m.ServiceConfigItem, 0, len(items))           // items with version as wildcard and sku not a wildcard
	versionToWildcardSkuItems := make(map[string][]*m.ServiceConfigItem, len(items)) // items with sku as wildcard

	for i, item := range items {

		cfgKey := strings.ToLower(item.GetConfigKey())
		cfgValue := item.GetConfigValue()

		sku := item.GetSku()
		host := item.GetHostname()
		version := item.GetServiceVersion()

		if len(sku) < 1 || len(host) < 1 || len(version) < 1 {
			cfgMgr.logger.Errorf("Skipping illegal config item with nil values, item=%v", item)
			continue
		}

		if sku != wildcardToken && host != wildcardToken {
			key := strings.Join([]string{service, version, sku, host, cfgKey}, ".")
			sku = wildcardToken
			items[i].Sku = common.StringPtr(wildcardToken)
			cfgMgr.logger.WithFields(bark.Fields{
				"key":   key,
				"value": cfgValue,
			}).Warn("Forcing sku=*;host specific items MUST be sku agnostic")
		}

		allSkus[sku] = struct{}{}
		allVersions[version] = struct{}{}

		if version == wildcardToken {
			if sku == wildcardToken {
				if host == wildcardToken {
					// service.*.*.*.key, add it at the root level
					root.keyValues[cfgKey] = cfgValue
					continue
				}
				// 1(a) keep track of svc.* config items
				wildcardVersionItems = append(wildcardVersionItems, item)
				continue
			}
			// 1(b) Keep track of svc.*.sku config items
			wildcardVersionSkuItems = append(wildcardVersionSkuItems, item)
			continue
		}

		if sku == wildcardToken {
			defaults, ok := versionToWildcardSkuItems[version]
			if !ok {
				defaults = make([]*m.ServiceConfigItem, 0, 4)
				versionToWildcardSkuItems[version] = defaults
			}
			// 1(c) keep track of svc.ver.* items
			versionToWildcardSkuItems[version] = append(defaults, item)
			continue
		}

		addToKVTree(root, version, sku, host, cfgKey, cfgValue)
	}

	// apply svc.ver.* items as default for every svc.ver.sku
	for ver, items := range versionToWildcardSkuItems {
		for _, item := range items {
			for s := range allSkus {
				addToKVTree(root, ver, s, item.GetHostname(), item.GetConfigKey(), item.GetConfigValue())
			}
		}
	}

	// apply svc.*.sku. items as default for every svc.ver.sku items
	for _, item := range wildcardVersionSkuItems {
		for ver := range allVersions {
			addToKVTree(root, ver, item.GetSku(), item.GetHostname(), item.GetConfigKey(), item.GetConfigValue())
		}
	}

	// apply svc.*. items as default for every svc.ver.*
	for _, item := range wildcardVersionItems {
		for ver := range allVersions {
			for sku := range allSkus {
				addToKVTree(root, ver, sku, item.GetHostname(), item.GetConfigKey(), item.GetConfigValue())
			}
		}
	}

	return root
}

// mkConfig constructs a config object of given type using the
// values from the given set of key,value strings. If the given
// map of (k,v) is missing a config key, then this method will
// attempt to use value in the default config. If the default
// config is nil, then this method will use the default value
// specified in the struct tag.
func (cfgMgr *CassandraConfigManager) mkConfig(configType interface{}, defaults interface{}, keyValues map[string]string) interface{} {

	var valueType = reflect.TypeOf(configType)
	var result = reflect.New(valueType).Elem()
	var defaultCfg = reflect.ValueOf(defaults) // valueOf(nil) returns empty value

	for i := 0; i < valueType.NumField(); i++ {

		var field = result.Field(i)
		if !field.CanSet() {
			cfgMgr.logger.Fatalf("Un-Settable config field %v", field)
		}

		var defaultVal reflect.Value
		if defaultCfg.IsValid() {
			defaultVal = defaultCfg.Field(i)
		}

		var name = strings.ToLower(valueType.Field(i).Tag.Get("name"))
		var defaultStr = valueType.Field(i).Tag.Get("default")

		switch field.Kind() {
		case reflect.Int:
			setIntField(field, name, keyValues, defaultVal, defaultStr)
		case reflect.Int64:
			setIntField(field, name, keyValues, defaultVal, defaultStr)
		case reflect.String:
			setStringField(field, name, keyValues, defaultVal, defaultStr)
		case reflect.Float64:
			setFloatField(field, name, keyValues, defaultVal, defaultStr)
		case reflect.Slice:
			setSliceField(field, name, keyValues, defaultVal, defaultStr)
		default:
			cfgMgr.logger.Fatalf("Unknown field data-type, %v %v", name, field.Kind())
		}
	}

	return result.Interface()
}

func (cfgMgr *CassandraConfigManager) String() string {
	tree := cfgMgr.configValues.Load().(*configTree)
	if tree == nil {
		return "empty"
	}
	buf := new(bytes.Buffer)
	for k, v := range tree.children {
		writeTree(k, v, buf)
	}
	return buf.String()
}

func writeTree(key string, root *configTreeNode, buf *bytes.Buffer) {
	buf.WriteString(fmt.Sprintf("%v = %+v\n", key, root.value))
	for k, v := range root.children {
		writeTree(key+"."+k, v, buf)
	}
}

func newKVTreeNode() *kvTreeNode {
	node := new(kvTreeNode)
	node.keyValues = make(map[string]string)
	node.children = make(map[string]*kvTreeNode)
	return node
}

func addToKVTree(root *kvTreeNode, version, sku, host, cfgKey, cfgValue string) {

	var maxDepth int // max depth of tree to traverse

	switch {
	case host != wildcardToken:
		maxDepth = 3
	case sku != wildcardToken:
		maxDepth = 2
	default:
		maxDepth = 1
	}

	curr := root

	for depth, key := range []string{version, sku, host} {

		if depth >= maxDepth {
			break
		}

		child, ok := curr.children[key]
		if !ok {
			child = newKVTreeNode()
			curr.children[key] = child
		}

		curr = child
	}

	if _, ok := curr.keyValues[cfgKey]; !ok {
		// do not overwrite old value
		curr.keyValues[cfgKey] = cfgValue
	}
}

func setIntField(field reflect.Value, fieldName string, keyValues map[string]string, defaultVal reflect.Value, defaultStr string) {
	if val, ok := keyValues[fieldName]; ok {
		if v, err := strconv.ParseInt(val, 0, 64); err == nil {
			field.SetInt(int64(v))
			return
		}
	}
	if defaultVal.IsValid() {
		field.Set(defaultVal)
		return
	}
	if v, err := strconv.ParseInt(defaultStr, 0, 64); err == nil {
		field.SetInt(int64(v))
		return
	}
}

func setFloatField(field reflect.Value, fieldName string, keyValues map[string]string, defaultVal reflect.Value, defaultStr string) {
	if val, ok := keyValues[fieldName]; ok {
		if v, err := strconv.ParseFloat(val, 64); err == nil {
			field.SetFloat(v)
			return
		}
	}
	if defaultVal.IsValid() {
		field.Set(defaultVal)
		return
	}
	if v, err := strconv.ParseFloat(defaultStr, 64); err == nil {
		field.SetFloat(v)
		return
	}
}

func setSliceField(field reflect.Value, fieldName string, keyValues map[string]string, defaultVal reflect.Value, defaultStr string) {
	if val, ok := keyValues[fieldName]; ok {
		v := strings.Split(val, sliceSplitToken)
		field.Set(reflect.ValueOf(v))
		return
	}
	if defaultVal.IsValid() && !defaultVal.IsNil() {
		field.Set(defaultVal)
		return
	}
	v := strings.Split(defaultStr, sliceSplitToken)
	field.Set(reflect.ValueOf(v))
}

func setStringField(field reflect.Value, fieldName string, keyValues map[string]string, defaultVal reflect.Value, defaultStr string) {
	if val, ok := keyValues[fieldName]; ok {
		field.SetString(val)
		return
	}
	if defaultVal.IsValid() {
		field.Set(defaultVal)
		return
	}
	field.SetString(defaultStr)
}

// SetRefreshInterval is used to set the refresh interval
func SetRefreshInterval(interval time.Duration) {
	cfgRefreshInterval = interval
}
