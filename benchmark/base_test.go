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

package benchmark

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/apache/thrift/lib/go/thrift"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	clientStream "github.com/uber/cherami-client-go/stream"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
	"github.com/uber/cherami-server/services/inputhost"
	"github.com/uber/cherami-server/services/outputhost"
	"github.com/uber/cherami-server/services/storehost"
	serverStream "github.com/uber/cherami-server/stream"
	mockmeta "github.com/uber/cherami-server/test/mocks/metadata"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
	"golang.org/x/net/context"

	log "github.com/sirupsen/logrus"
)

const (
	defaultDestinationUUID   = "00000000-0000-0000-0000-000000000000"
	defaultDestinationType   = cherami.DestinationType_PLAIN
	defaultConsumerGroupUUID = "11111111-1111-1111-1111-111111111111"
	defaultWSPingInterval    = 60 * time.Second
)

type base struct {
	storageBaseDir string

	destinationName   string
	destinationUUID   string
	consumerGroupName string
	consumerGroupUUID string
	extentUUID        string

	hwInfoReader common.HostHardwareInfoReader
	uuidResolver *mockUUIDResolver
	mClient      *mockmeta.TChanMetadataService
	ringHosts    string
	listenIP     string

	storeHosts       []*storehost.StoreHost
	storeHostPorts   []string
	storeHostWSPorts []int

	inputHost       *inputhost.InputHost
	inputHostPort   string
	inputHostWSPort int

	outputHost       *outputhost.OutputHost
	outputHostPort   string
	outputHostWSPort int

	loglevel log.Level
	cfg      configure.CommonAppConfig

	sync.Mutex
}

func newBase() *base {
	storageBase, _ := ioutil.TempDir("", "cherami_benchmark")

	ipAddr, _ := tchannel.ListenIP()
	listenIP := ipAddr.String()

	ret := &base{
		storageBaseDir:    storageBase,
		destinationName:   "/dest/benchmark",
		destinationUUID:   uuid.New(),
		consumerGroupName: "/dest/benchmark_reader",
		consumerGroupUUID: uuid.New(),
		extentUUID:        uuid.New(),
		uuidResolver:      newMockUUIDResolver(),
		hwInfoReader:      newMockHostInfoReader(),
		mClient:           new(mockmeta.TChanMetadataService),
		loglevel:          log.WarnLevel,
		cfg:               common.SetupServerConfig(configure.NewCommonConfigure()),
		listenIP:          listenIP,
	}

	ret.mClient.On("RegisterHostUUID", mock.Anything, mock.Anything).Return(nil)
	ret.mClient.On("UpdateStoreExtentReplicaStats").Return(nil)
	ret.mClient.On("UpdateStoreExtentReplicaStats", nil, mock.Anything).Return(nil)
	ret.mClient.On("UpdateStoreExtentReplicaStats", mock.Anything, mock.Anything).Return(nil)
	ret.mClient.On("ReadServiceConfig", mock.Anything, mock.Anything).Return(nil, fmt.Errorf(`unimplemented`))
	return ret
}

func (b *base) setupMetadata() {
	destDesc := shared.NewDestinationDescription()
	destDesc.Path = &b.destinationName
	destDesc.DestinationUUID = &b.destinationUUID
	log.Infof("destinationUUID %s", *destDesc.DestinationUUID)
	b.mClient.On("ReadDestination", mock.Anything, mock.Anything).Return(destDesc, nil)

	mExt := shared.NewExtent()
	mExt.ExtentUUID = &b.extentUUID
	log.Infof("extentUUID %s", *mExt.ExtentUUID)
	mExt.StoreUUIDs = []string{}
	for _, sh := range b.storeHosts {
		mExt.StoreUUIDs = append(mExt.StoreUUIDs, sh.GetHostUUID())
	}

	mExtStats := shared.NewExtentStats()
	mExtStats.Extent = mExt
	mExtStats.Status = common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN)

	mListExtStats := metadata.NewListInputHostExtentsStatsResult_()
	mListExtStats.ExtentStatsList = []*shared.ExtentStats{mExtStats}

	b.mClient.On("ListInputHostExtentsStats", mock.Anything, mock.Anything).Return(mListExtStats, nil)

	if b.outputHost != nil {
		cgDesc := shared.NewConsumerGroupDescription()
		cgDesc.DestinationUUID = destDesc.DestinationUUID
		cgDesc.ConsumerGroupUUID = &b.consumerGroupUUID
		log.Infof("consumerGroupUUID %s", *cgDesc.ConsumerGroupUUID)
		cgDesc.LockTimeoutSeconds = common.Int32Ptr(60)
		cgDesc.StartFrom = common.Int64Ptr(0)
		b.mClient.On("ReadConsumerGroup", mock.Anything, mock.Anything).Return(cgDesc, nil)

		mCGExt := shared.NewConsumerGroupExtent()
		mCGExt.ExtentUUID = mExt.ExtentUUID
		mCGExt.ConsumerGroupUUID = cgDesc.ConsumerGroupUUID
		mCGExt.Status = common.MetadataConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_OPEN)
		mCGExt.AckLevelOffset = common.Int64Ptr(0)
		mCGExt.OutputHostUUID = common.StringPtr(b.outputHost.GetHostUUID())
		mCGExt.StoreUUIDs = mExt.StoreUUIDs

		mCGExtResult := shared.NewReadConsumerGroupExtentsResult_()
		mCGExtResult.Extents = []*shared.ConsumerGroupExtent{mCGExt}
		b.mClient.On("ReadConsumerGroupExtents", mock.Anything, mock.Anything).Return(mCGExtResult, nil)
		outcall := new(mockmeta.MetadataServiceReadConsumerGroupExtentsStreamOutCall)
		b.mClient.On("ReadConsumerGroupExtentsStream", mock.Anything, mock.Anything).Return(outcall, fmt.Errorf(`unimplemented`))
		b.mClient.On("SetAckOffset", mock.Anything, mock.Anything).Return(nil)
		b.mClient.On("ReadExtentStats", mock.Anything, mock.Anything).Return(&metadata.ReadExtentStatsResult_{ExtentStats: mExtStats}, nil)
	}
}

func (b *base) setupStore(replicas int) {
	b.mClient.On("ListStoreExtentsStats", mock.Anything, mock.Anything).Return(metadata.NewListStoreExtentsStatsResult_(), nil)

	b.storeHosts = make([]*storehost.StoreHost, replicas)
	b.storeHostPorts = make([]string, replicas)
	b.storeHostWSPorts = make([]int, replicas)
	ports := make([]int, replicas)

	// set the CHERAMI_STOREHOST_WS_PORT as "test" and set the wsPort corresponding to
	// each of the replica's host port.
	os.Setenv("CHERAMI_STOREHOST_WS_PORT", "test")
	for i := 0; i < replicas; i++ {
		b.storeHostPorts[i], ports[i] = findEphemeralPort()
	}

	log.Infof("storehosts: %v\n", b.storeHostPorts)

	ringhosts := ""
	if replicas > 1 {
		ringhosts = strings.Join(b.storeHostPorts, ",")
	} else if replicas == 1 {
		ringhosts = b.storeHostPorts[0]
	}

	if b.ringHosts == "" {
		b.ringHosts = ringhosts
	} else {
		b.ringHosts = fmt.Sprintf("%s,%s", ringhosts, b.ringHosts)
	}

	for i := 0; i < replicas; i++ {
		go func(i int) {

			hostUUID := uuid.New()
			cfg := &configure.ServiceConfig{
				Port:          ports[i],
				RingHosts:     ringhosts,
				LimitsEnabled: false,
				ListenAddress: b.listenIP,
			}
			b.Lock()
			b.cfg.SetServiceConfig(common.StoreServiceName, cfg)
			b.Unlock()
			reporter := common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig())
			dClient := dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.StoreServiceName)

			sCommon := common.NewService(common.StoreServiceName,
				hostUUID,
				cfg,
				b.uuidResolver,
				b.hwInfoReader,
				reporter,
				dClient,
				common.NewBypassAuthManager(),
			)

			storehostOpts := &storehost.Options{
				BaseDir: b.storageBaseDir,
			}

			sh, tc := storehost.NewStoreHost(common.StoreServiceName, sCommon, b.mClient, storehostOpts)
			sh.Start(tc)

			// start websocket server
			_, b.storeHostWSPorts[i] = findEphemeralPort()
			common.WSStart(b.listenIP, b.storeHostWSPorts[i], sh)

			storeHostPort := sh.GetTChannel().PeerInfo().HostPort
			// XXX: We use the environment variable of the format
			// IP_IPv4_A_DD_R_PORT of the replica and set the
			// websocket port corresponding to this replica.
			// This is needed to connect to the appropriate replica using
			// websocket
			envVar := common.GetEnvVariableFromHostPort(storeHostPort)
			os.Setenv(envVar, strconv.FormatInt(int64(b.storeHostWSPorts[i]), 10))
			b.Lock()
			b.uuidResolver.Set(hostUUID, storeHostPort)
			b.Unlock()

			log.Infof("storehost %s %s", storeHostPort, hostUUID)
			b.storeHosts[i] = sh
		}(i)
	}

	b.mClient.On("UpdateStoreExtentReplicaStats", mock.Anything, mock.Anything).Return(nil)

	time.Sleep(2 * time.Second)
}

func (b *base) setupStoreCall(i int, extentUUID string) (serverStream.BStoreOpenAppendStreamOutCall, context.CancelFunc, chan bool) {
	var call serverStream.BStoreOpenAppendStreamOutCall
	var cancel context.CancelFunc
	var err error
	ackStreamClosed := make(chan bool, 1)

	req := &store.OpenAppendStreamRequest{
		DestinationUUID: common.StringPtr(defaultDestinationUUID),
		DestinationType: cherami.DestinationTypePtr(defaultDestinationType),
		ExtentUUID:      common.StringPtr(extentUUID),
	}
	reqHeaders := common.GetOpenAppendStreamRequestHeaders(req)

	host, _, _ := net.SplitHostPort(b.storeHostPorts[i])
	hostPort := net.JoinHostPort(host, strconv.Itoa(b.storeHostWSPorts[i]))
	httpHeaders := http.Header{}
	for k, v := range reqHeaders {
		httpHeaders.Add(k, v)
	}

	log.Infof("client: starting websocket to connect to store host for write %s", hostPort)
	time.Sleep(time.Second)
	wsConnector := common.NewWSConnector()
	call, err = wsConnector.OpenAppendStream(hostPort, httpHeaders)
	if err != nil {
		log.Errorf("client: error opening websocket connection to store host for write %s: %v", hostPort, err)
		return call, cancel, ackStreamClosed
	}
	cancel = nil

	go func() {
		for {
			_, errGo := call.Read()
			if errGo == io.EOF {
				log.Infof("client: ack stream closed")
				ackStreamClosed <- true
				return
			}

			if errGo != nil {
				log.Errorf("client: error reading ack stream: %v", errGo)
				ackStreamClosed <- false
				return
			}
		}
	}()

	return call, cancel, ackStreamClosed
}

func (b *base) setupStoreReadCall(i int, extentUUID string) (serverStream.BStoreOpenReadStreamOutCall, context.CancelFunc) {
	var call serverStream.BStoreOpenReadStreamOutCall
	var cancel context.CancelFunc
	var err error

	req := &store.OpenReadStreamRequest{
		DestinationUUID:   common.StringPtr(defaultDestinationUUID),
		DestinationType:   cherami.DestinationTypePtr(defaultDestinationType),
		ExtentUUID:        common.StringPtr(extentUUID),
		ConsumerGroupUUID: common.StringPtr(defaultConsumerGroupUUID),
		Address:           common.Int64Ptr(store.ADDR_BEGIN), // start reading from the first address the beginning
		Inclusive:         common.BoolPtr(false),             // non-inclusive
	}
	reqHeaders := common.GetOpenReadStreamRequestHeaders(req)

	host, _, _ := net.SplitHostPort(b.storeHostPorts[i])
	hostPort := net.JoinHostPort(host, strconv.Itoa(b.storeHostWSPorts[i]))
	httpHeaders := http.Header{}
	for k, v := range reqHeaders {
		httpHeaders.Add(k, v)
	}

	log.Infof("client: starting websocket to connect to store host for read %s", hostPort)
	time.Sleep(time.Second)
	wsConnecotr := common.NewWSConnector()
	call, err = wsConnecotr.OpenReadStream(hostPort, httpHeaders)
	if err != nil {
		log.Errorf("client: error opening websocket connection to store host for read %s: %v", hostPort, err)
		return call, cancel
	}
	cancel = nil

	return call, cancel
}

func (b *base) shutdownStore() {
	for _, sh := range b.storeHosts {
		sh.Shutdown()
		sh.Stop()
	}

	os.RemoveAll(b.storageBaseDir)
}

func (b *base) setupOutput() {
	port := 0
	b.outputHostPort, port = findEphemeralPort()
	if b.ringHosts == "" {
		b.ringHosts = b.outputHostPort
	} else {
		b.ringHosts = fmt.Sprintf("%s,%s", b.outputHostPort, b.ringHosts)
	}

	cfg := &configure.ServiceConfig{
		Port:          port,
		RingHosts:     b.ringHosts,
		LimitsEnabled: false,
		ListenAddress: b.listenIP,
	}
	b.cfg.SetServiceConfig(common.OutputServiceName, cfg)

	reporter := common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig())
	dClient := dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.OutputServiceName)

	sCommonOut := common.NewService(common.OutputServiceName, uuid.New(), cfg,
		b.uuidResolver, b.hwInfoReader, reporter, dClient, common.NewBypassAuthManager())
	oh, tc := outputhost.NewOutputHost(common.OutputServiceName, sCommonOut,
		b.mClient, nil, nil, b.cfg.GetKafkaConfig())
	oh.Start(tc)
	// start websocket server
	_, b.outputHostWSPort = findEphemeralPort()
	common.WSStart(b.listenIP, b.outputHostWSPort, oh)

	b.outputHost = oh
	b.uuidResolver.Set(oh.GetHostUUID(), b.outputHostPort)

	log.Infof("outputhost %s %s", b.outputHostPort, oh.GetHostUUID())
}

func (b *base) setupOutCall() (clientStream.BOutOpenConsumerStreamOutCall, context.CancelFunc, chan<- int, chan<- string) {
	var call clientStream.BOutOpenConsumerStreamOutCall
	var cancel context.CancelFunc
	var err error
	creditsCh := make(chan int, 100)
	ackCh := make(chan string, 25)

	host, _, _ := net.SplitHostPort(b.outputHostPort)
	hostPort := net.JoinHostPort(host, strconv.Itoa(b.outputHostWSPort))

	log.Infof("client: starting websocket to connect to output host %s", hostPort)
	time.Sleep(time.Second)
	wsConnecotr := common.NewWSConnector()
	call, err = wsConnecotr.OpenConsumerStream(hostPort, http.Header{
		"path":              {b.destinationName},
		"consumerGroupName": {b.consumerGroupName},
	})
	if err != nil {
		log.Errorf("client: error opening websocket connection to output host %s: %v", hostPort, err)
		return call, cancel, creditsCh, ackCh
	}
	cancel = nil

	go func() {
		for i := range creditsCh {
			err = call.Write(&cherami.ControlFlow{
				Credits: common.Int32Ptr(int32(i)),
			})
			if err != nil {
				log.Errorf("client: consumer stream write error %v", err)
				return
			}

			err = call.Flush()
			if err != nil {
				log.Errorf("client: consumer stream flush error %v", err)
				return
			}
		}
	}()

	go func() {
		var errGo error
		ch, _ := tchannel.NewChannel("outputhost-client",
			&tchannel.ChannelOptions{})

		ch.Peers().Add(b.outputHostPort)
		tClient := thrift.NewClient(ch, common.OutputServiceName, nil)
		client := cherami.NewTChanBOutClient(tClient)
		ackIDs := []string{}

		doAck := func() {
			ctxGo, cancelGo := thrift.NewContext(10 * time.Second)
			errGo = client.AckMessages(ctxGo, &cherami.AckMessagesRequest{
				AckIds:  ackIDs,
				NackIds: []string{},
			})

			if errGo != nil {
				log.Errorf("client: ack error %v", errGo)
			}
			cancelGo()
		}

		for id := range ackCh {
			ackIDs = append(ackIDs, id)
			if len(ackIDs) >= 25 {
				doAck()
				ackIDs = []string{}
			}
		}

		if len(ackIDs) > 0 {
			doAck()
		}
	}()

	return call, cancel, creditsCh, ackCh
}

func (b *base) shutdownOutput() {
	b.outputHost.Shutdown()
	b.outputHost.GetRingpopMonitor().Stop()
}

func (b *base) setupInput() {
	port := 0
	b.inputHostPort, port = findEphemeralPort()
	if b.ringHosts == "" {
		b.ringHosts = b.inputHostPort
	} else {
		b.ringHosts = fmt.Sprintf("%s,%s", b.inputHostPort, b.ringHosts)
	}

	cfg := &configure.ServiceConfig{
		Port:          port,
		RingHosts:     b.ringHosts,
		LimitsEnabled: false,
		ListenAddress: b.listenIP,
	}
	b.cfg.SetServiceConfig(common.InputServiceName, cfg)
	reporter := common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig())
	dClient := dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.InputServiceName)

	sCommonIn := common.NewService(common.InputServiceName, uuid.New(), cfg,
		b.uuidResolver, b.hwInfoReader, reporter, dClient, common.NewBypassAuthManager())
	ih, tc := inputhost.NewInputHost(common.InputServiceName, sCommonIn, b.mClient, nil)
	ih.Start(tc)
	// start websocket server
	_, b.inputHostWSPort = findEphemeralPort()

	common.WSStart(b.listenIP, b.inputHostWSPort, ih)

	b.inputHost = ih
	b.uuidResolver.Set(ih.GetHostUUID(), b.inputHostPort)

	log.Infof("inputhost %s %s", b.inputHostPort, ih.GetHostUUID())
}

func (b *base) setupInCall() (clientStream.BInOpenPublisherStreamOutCall, context.CancelFunc, chan bool) {
	var call clientStream.BInOpenPublisherStreamOutCall
	var cancel context.CancelFunc
	var err error
	ackStreamClosed := make(chan bool, 1)

	host, _, _ := net.SplitHostPort(b.inputHostPort)
	hostPort := net.JoinHostPort(host, strconv.Itoa(b.inputHostWSPort))

	log.Infof("client: starting websocket connection to input host %s", hostPort)
	time.Sleep(time.Second)
	wsConnecotr := common.NewWSConnector()
	call, err = wsConnecotr.OpenPublisherStream(hostPort, http.Header{
		"path": {b.destinationName},
	})
	if err != nil {
		log.Errorf("client: error opening websocket connection to input host %s: %v", hostPort, err)
		return call, cancel, ackStreamClosed
	}
	cancel = nil

	go func() {
		for {
			cmd, err := call.Read()
			if err == io.EOF {
				log.Infof("client: ack stream closed")
				ackStreamClosed <- true
				return
			}

			if err != nil {
				log.Errorf("client: error reading ack stream: %v", err)
				ackStreamClosed <- false
				return
			}

			if cmd.GetAck().GetStatus() != cherami.Status_OK {
				log.Errorf("PutMessageAck err %s %v %v %v", cmd.GetAck().GetID(), cmd.GetAck().GetStatus(), cmd.GetAck().GetMessage(), cmd.GetAck().GetReceipt())
			}
		}
	}()

	return call, cancel, ackStreamClosed
}

func (b *base) shutdownInput() {
	b.inputHost.Shutdown()
	b.inputHost.GetRingpopMonitor().Stop()
}
