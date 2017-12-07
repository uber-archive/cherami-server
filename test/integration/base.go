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

package integration

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cherami-server/clients/metadata"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	cassConfig "github.com/uber/cherami-server/common/dconfig"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
	"github.com/uber/cherami-server/services/controllerhost"
	"github.com/uber/cherami-server/services/frontendhost"
	"github.com/uber/cherami-server/services/inputhost"
	"github.com/uber/cherami-server/services/outputhost"
	"github.com/uber/cherami-server/services/storehost"
	"github.com/uber/cherami-server/test"
	thriftM "github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/tchannel-go/thrift"
)

type (
	testBase struct {
		Frontends      map[string]*frontendhost.Frontend
		InputHosts     map[string]*inputhost.InputHost
		OutputHosts    map[string]*outputhost.OutputHost
		StoreHosts     map[string]*storehost.StoreHost
		Controllers    map[string]*controllerhost.Mcp
		mClient        *metadata.CassandraMetadataService
		UUIDResolver   common.UUIDResolver
		keyspace       string
		storageBaseDir string
		auth           configure.Authentication

		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}
)

func (tb *testBase) buildConfig(clusterSz map[string]int, numReplicas int) map[string][]*configure.AppConfig {

	services := []string{common.StoreServiceName, common.InputServiceName, common.OutputServiceName, common.FrontendServiceName, common.ControllerServiceName}

	appCfgMap := make(map[string][]*configure.AppConfig)

	ringHosts := ""
	cfg := common.SetupServerConfig(configure.NewCommonConfigure())
	for _, s := range services {

		if _, ok := clusterSz[s]; !ok {
			clusterSz[s] = 1
		}

		nPorts := clusterSz[s]
		appCfgMap[s] = make([]*configure.AppConfig, nPorts)

		for i := 0; i < nPorts; i++ {

			hostPort, listenIP, port, err := test.FindEphemeralPort()
			tb.Nil(err)
			_, _, wsPort, err := test.FindEphemeralPort()
			tb.Nil(err)
			log.Debugf("serviceName: %#q addr:%v", common.StoreServiceName, hostPort)

			if ringHosts == "" {
				ringHosts = fmt.Sprintf("%v:%d", listenIP, port)
			}
			/*else {
				ringHosts = fmt.Sprintf("%v:%d,%v", listenIP, port, ringHosts)
			}*/

			log.Infof("ringHosts is: %v", ringHosts)
			serviceCfg := &configure.ServiceConfig{
				Port:          port,
				WebsocketPort: wsPort,
				RingHosts:     ringHosts,
				ListenAddress: listenIP,
			}

			appCfg := cfg.(*configure.AppConfig)
			appCfg.DefaultDestinationConfig.Replicas = int16(numReplicas)
			appCfg.ServiceConfig[s] = serviceCfg
			appCfgMap[s][i] = appCfg
		}
	}

	return appCfgMap
}

var singletonTestBaseRefCount int32
var singletonTestBase testBase
var singletonTestBaseLock sync.Mutex

func (tb *testBase) SetupSuite(t *testing.T) {
	singletonTestBaseLock.Lock()
	defer singletonTestBaseLock.Unlock()
	singletonTestBaseRefCount++

	if singletonTestBaseRefCount != 1 {
		*tb = singletonTestBase
		return
	}

	singletonTestBase.setupSuiteImpl(t)
	singletonTestBase.SetUp(map[string]int{}, 1)
	singletonTestBase.setupServiceConfig(t)
	*tb = singletonTestBase
}

func (tb *testBase) setupServiceConfig(t *testing.T) {
	cItem := &thriftM.ServiceConfigItem{
		ServiceName:    common.StringPtr("cherami-storehost"),
		ServiceVersion: common.StringPtr("*"),
		Sku:            common.StringPtr("*"),
		Hostname:       common.StringPtr("*"),
		ConfigKey:      common.StringPtr("minfreediskspacebytes"),
		ConfigValue:    common.StringPtr("100"), // set to a very low value - 100 bytes for the test
	}

	req := &thriftM.UpdateServiceConfigRequest{ConfigItem: cItem}

	ctx, cancel := thrift.NewContext(15 * time.Second)
	defer cancel()

	err := tb.mClient.UpdateServiceConfig(ctx, req)
	tb.NoError(err)

}

func (tb *testBase) setupSuiteImpl(t *testing.T) {
	tb.SetT(t)
	tb.keyspace = "integration_test"
	tb.Assertions = require.New(tb.T())

	tb.auth = configure.Authentication{
		Enabled:  true,
		Username: "cassandra",
		Password: "cassandra",
	}

	// create the keyspace first
	err := metadata.CreateKeyspaceNoSession("127.0.0.1", 9042, tb.keyspace, 1, true, tb.auth)
	tb.NoError(err)

	tb.mClient = tb.GetNewMetadataClient()
	tb.NotNil(tb.mClient)

	// Drop the keyspace, if it exists. This preserves the keyspace for inspection if the test fails, and simplifies cleanup
	metadata.DropKeyspace(tb.mClient.GetSession(), tb.keyspace)

	// Make sure we load the keyspace and setup schema
	err = metadata.CreateKeyspace(tb.mClient.GetSession(), tb.keyspace, 1, true)
	tb.NoError(err)

	err = metadata.LoadSchema("/usr/local/bin/cqlsh", "../../clients/metadata/schema/metadata.cql", tb.keyspace)
	tb.NoError(err)

	// Adjust the controller and storehost scan intervals
	controllerhost.IntervalBtwnScans = time.Second
	controllerhost.SetDrainExtentTimeout(5 * time.Second)
	storehost.ExtStatsReporterSetReportInterval(time.Second)
	storehost.ExtStatsReporterResume()

	// Make sure the cassandra config refresh interval is small
	cassConfig.SetRefreshInterval(10 * time.Millisecond)
}

func (tb *testBase) GetNewMetadataClient() *metadata.CassandraMetadataService {
	s, _ := metadata.NewCassandraMetadataService(&configure.MetadataConfig{
		CassandraHosts: "127.0.0.1",
		Port:           9042,
		Keyspace:       tb.keyspace,
		Consistency:    "One",
		Authentication: tb.auth,
	}, nil)

	return s
}

func (tb *testBase) TearDownSuite() {
}

func (tb *testBase) SetUp(clusterSz map[string]int, numReplicas int) {
	var err error

	// create temp dir for storage
	tb.storageBaseDir, err = ioutil.TempDir("", "cherami_integration_test_")
	tb.NoError(err)

	tb.mClient = tb.GetNewMetadataClient()
	tb.NotNil(tb.mClient)
	tb.UUIDResolver = common.NewUUIDResolver(tb.mClient)
	hwInfoReader := common.NewHostHardwareInfoReader(tb.mClient)

	cfgMap := tb.buildConfig(clusterSz, numReplicas)

	tb.StoreHosts = make(map[string]*storehost.StoreHost, clusterSz[common.StoreServiceName])
	tb.InputHosts = make(map[string]*inputhost.InputHost, clusterSz[common.InputServiceName])
	tb.OutputHosts = make(map[string]*outputhost.OutputHost, clusterSz[common.OutputServiceName])
	tb.Frontends = make(map[string]*frontendhost.Frontend, clusterSz[common.FrontendServiceName])
	tb.Controllers = make(map[string]*controllerhost.Mcp, clusterSz[common.ControllerServiceName])

	os.Setenv("CHERAMI_STOREHOST_WS_PORT", "test")
	// Handle the storehosts separately
	for i := 0; i < clusterSz[common.StoreServiceName]; i++ {
		hostID := uuid.New()
		storehostOpts := &storehost.Options{
			BaseDir: tb.storageBaseDir,
		}

		cfg := cfgMap[common.StoreServiceName][i].ServiceConfig[common.StoreServiceName]
		reporter := common.NewTestMetricsReporter()
		dClient := dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.StoreServiceName)
		sCommon := common.NewService(common.StoreServiceName, hostID, cfg, tb.UUIDResolver, hwInfoReader, reporter, dClient, common.NewBypassAuthManager())
		log.Infof("store ringHosts: %v", cfg.GetRingHosts())
		sh, tc := storehost.NewStoreHost(common.StoreServiceName, sCommon, tb.GetNewMetadataClient(), storehostOpts)
		sh.Start(tc)

		// start websocket server
		common.WSStart(cfg.GetListenAddress().String(), cfg.GetWebsocketPort(), sh)

		// set the websocket port of the store
		// XXX: We use the environment variable of the format
		// IP_IPv4_A_DD_R_PORT of the replica and set the
		// websocket port corresponding to this replica.
		// This is needed to connect to the appropriate replica using
		// websocket
		envVar := common.GetEnvVariableFromHostPort(sh.GetHostPort())
		os.Setenv(envVar, strconv.FormatInt(int64(cfg.GetWebsocketPort()), 10))
		tb.StoreHosts[hostID] = sh
	}

	for i := 0; i < clusterSz[common.InputServiceName]; i++ {
		hostID := uuid.New()
		cfg := cfgMap[common.InputServiceName][i].ServiceConfig[common.InputServiceName]
		reporter := common.NewTestMetricsReporter()
		dClient := dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.InputServiceName)
		sCommon := common.NewService(common.InputServiceName, hostID, cfg, tb.UUIDResolver, hwInfoReader, reporter, dClient, common.NewBypassAuthManager())
		log.Infof("input ringHosts: %v", cfg.GetRingHosts())
		ih, tc := inputhost.NewInputHost(common.InputServiceName, sCommon, tb.GetNewMetadataClient(), nil)
		ih.Start(tc)
		// start websocket server
		common.WSStart(cfg.GetListenAddress().String(), cfg.GetWebsocketPort(), ih)
		tb.InputHosts[hostID] = ih
	}

	var frontendForOut *frontendhost.Frontend
	for i := 0; i < clusterSz[common.FrontendServiceName]; i++ {
		hostID := uuid.New()
		cfg := cfgMap[common.FrontendServiceName][i].ServiceConfig[common.FrontendServiceName]
		reporter := common.NewTestMetricsReporter()
		dClient := dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.FrontendServiceName)

		sCommon := common.NewService(common.FrontendServiceName, hostID, cfg, tb.UUIDResolver, hwInfoReader, reporter, dClient, common.NewBypassAuthManager())
		log.Infof("front ringHosts: %v", cfg.GetRingHosts())
		fh, tc := frontendhost.NewFrontendHost(common.FrontendServiceName, sCommon, tb.GetNewMetadataClient(), cfgMap[common.FrontendServiceName][i])
		fh.Start(tc)
		tb.Frontends[hostID] = fh
		frontendForOut = fh
	}

	for i := 0; i < clusterSz[common.OutputServiceName]; i++ {
		hostID := uuid.New()
		cfg := cfgMap[common.OutputServiceName][i].ServiceConfig[common.OutputServiceName]
		reporter := common.NewTestMetricsReporter()
		dClient := dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.OutputServiceName)
		sCommon := common.NewService(common.OutputServiceName, hostID, cfg, tb.UUIDResolver, hwInfoReader, reporter, dClient, common.NewBypassAuthManager())
		log.Infof("output ringHosts: %v", cfg.GetRingHosts())
		oh, tc := outputhost.NewOutputHost(
			common.OutputServiceName,
			sCommon,
			tb.GetNewMetadataClient(),
			frontendForOut,
			nil,
			cfgMap[common.OutputServiceName][i].GetKafkaConfig(),
		)
		oh.Start(tc)
		// start websocket server
		common.WSStart(cfg.GetListenAddress().String(), cfg.GetWebsocketPort(), oh)

		tb.OutputHosts[hostID] = oh
	}

	for i := 0; i < clusterSz[common.ControllerServiceName]; i++ {
		hostID := uuid.New()
		cfg := cfgMap[common.ControllerServiceName][i]
		log.Infof("ctrlr ringHosts: %v", cfg.ServiceConfig[common.ControllerServiceName].RingHosts)
		serviceName := common.ControllerServiceName
		reporter := common.NewTestMetricsReporter()
		dClient := dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.ControllerServiceName)
		sVice := common.NewService(serviceName, uuid.New(), cfg.ServiceConfig[serviceName], tb.UUIDResolver, hwInfoReader, reporter, dClient, common.NewBypassAuthManager())
		ch, tc := controllerhost.NewController(cfg, sVice, tb.GetNewMetadataClient(), common.NewDummyZoneFailoverManager())
		ch.Start(tc)
		tb.Controllers[hostID] = ch
	}

	tappedServices := []string{common.InputServiceName, common.OutputServiceName, common.StoreServiceName, common.FrontendServiceName}
	for _, s := range tappedServices {
		var ch *controllerhost.Mcp
		for _, ch = range tb.Controllers {
			break
		}
		cond := func() bool {
			name := s
			hosts, e := ch.GetRingpopMonitor().GetHosts(name)
			return e == nil && len(hosts) == clusterSz[s]
		}
		success := common.SpinWaitOnCondition(cond, 60*time.Second)
		tb.True(success, "Failed to bootstrap service in ringpop")
	}

	tappedServices = []string{common.ControllerServiceName}
	for _, s := range tappedServices {
		var fh *frontendhost.Frontend
		for _, fh = range tb.Frontends {
			break
		}
		cond := func() bool {
			name := s
			hosts, e := fh.GetRingpopMonitor().GetHosts(name)
			return e == nil && len(hosts) == clusterSz[s]
		}
		success := common.SpinWaitOnCondition(cond, 60*time.Second)
		tb.True(success, "Failed to bootstrap service in ringpop")
	}
}

func (tb *testBase) TearDown() {
	log.Info("Stopping Output Hosts")
	for _, oh := range tb.OutputHosts {
		oh.Stop()
		oh.Shutdown()
	}
	log.Info("Stopping Input Hosts")
	for _, in := range tb.InputHosts {
		in.Stop()
		in.Shutdown()
	}
	log.Info("Stopping Store Hosts")
	for _, sh := range tb.StoreHosts {
		sh.Stop()
		sh.Shutdown()
		os.RemoveAll(tb.storageBaseDir)
	}
	log.Info("Stopping Frontend Hosts")
	for _, fh := range tb.Frontends {
		fh.Shutdown()
		fh.Stop()
	}
	log.Info("Stopping Controller Hosts")
	for _, ch := range tb.Controllers {
		ch.Stop()
	}
}

// GetFrontend returns a random frontend instance
func (tb *testBase) GetFrontend() *frontendhost.Frontend {
	for _, fh := range tb.Frontends {
		return fh
	}
	return nil
}

// GetOutput returns a random output host instance
func (tb *testBase) GetOutput() *outputhost.OutputHost {
	for _, oh := range tb.OutputHosts {
		return oh
	}
	return nil
}

// GetInput returns a random input host instance
func (tb *testBase) GetInput() *inputhost.InputHost {
	for _, ih := range tb.InputHosts {
		return ih
	}
	return nil
}

// GetController returns a random controller
// instance object from the set of controllers
// configured for this test. This method asssumes
// that the test is configured for a single controller
func (tb *testBase) GetController() *controllerhost.Mcp {
	tb.Equal(1, len(tb.Controllers), "Cannot return primary controller")
	for _, ch := range tb.Controllers {
		return ch
	}
	return nil
}

func toLevelPtr(level log.Level) *log.Level {
	return &level
}
