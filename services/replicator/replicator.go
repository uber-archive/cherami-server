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

package replicator

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"

	ccommon "github.com/uber/cherami-client-go/common"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
	mm "github.com/uber/cherami-server/common/metadata"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/common/set"
	storeStream "github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	rgen "github.com/uber/cherami-thrift/.generated/go/replicator"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

type (
	// Replicator is the main server class for replicator
	Replicator struct {
		logger   bark.Logger
		m3Client metrics.Client
		common.SCommon
		hostIDHeartbeater        common.HostIDHeartbeater
		AppConfig                configure.CommonAppConfig
		uconfigClient            dconfig.Client
		metaClient               metadata.TChanMetadataService
		allZones                 map[string][]string
		localZone                string
		authoritativeZone        string
		tenancy                  string
		defaultAuthoritativeZone string
		replicatorclientFactory  ClientFactory

		// for debug via admin API only
		remoteReplicatorConn      map[string]*outConnection
		remoteReplicatorConnMutex sync.RWMutex
		storehostConn             map[string]*outConnection
		storehostConnMutex        sync.RWMutex

		knownCgExtents set.Set

		metadataReconciler MetadataReconciler
	}
)

const (
	remoteReplicatorCallTimeOut = 30 * time.Second

	remoteReplicatorSetAcklevelCallTimeOut = 5 * time.Second

	localReplicatorCallTimeOut = 30 * time.Second
)

// interface implementation check
var _ rgen.TChanReplicator = (*Replicator)(nil)

func getAllZones(replicatorHosts map[string][]string) map[string][]string {
	allZones := make(map[string][]string)

	// recognize all zones by tenancy from replicatorHosts config key
	for zone := range replicatorHosts {
		zone = strings.ToLower(zone)
		tenancy := common.TenancyProd
		parts := strings.Split(zone, "_")
		if len(parts) == 2 {
			tenancy = parts[0]
			zone = parts[1]
		}
		if _, ok := allZones[tenancy]; !ok {
			allZones[tenancy] = make([]string, 0)
		}
		allZones[tenancy] = append(allZones[tenancy], zone)
	}

	return allZones
}

// NewReplicator is the constructor for Replicator
func NewReplicator(serviceName string, sVice common.SCommon, metadataClient metadata.TChanMetadataService, replicatorClientFactory ClientFactory, config configure.CommonAppConfig) (*Replicator, []thrift.TChanServer) {
	deployment := strings.ToLower(sVice.GetConfig().GetDeploymentName())
	zone, tenancy := common.GetLocalClusterInfo(deployment)
	allZones := getAllZones(replicatorClientFactory.GetHostsForAllDeployment())
	logger := (sVice.GetConfig().GetLogger()).WithFields(bark.Fields{
		common.TagReplicator: common.FmtOut(sVice.GetHostUUID()),
		common.TagDplName:    common.FmtDplName(deployment),
		common.TagZoneName:   common.FmtZoneName(zone),
		common.TagTenancy:    common.FmtTenancy(tenancy),
	})

	r := &Replicator{
		logger:                   logger,
		m3Client:                 metrics.NewClient(sVice.GetMetricsReporter(), metrics.Replicator),
		SCommon:                  sVice,
		AppConfig:                config,
		allZones:                 allZones,
		localZone:                zone,
		defaultAuthoritativeZone: config.GetReplicatorConfig().GetDefaultAuthoritativeZone(),
		tenancy:                  tenancy,
		replicatorclientFactory:  replicatorClientFactory,
		remoteReplicatorConn:     make(map[string]*outConnection),
		storehostConn:            make(map[string]*outConnection),
		knownCgExtents:           set.NewConcurrent(0),
	}

	r.metaClient = mm.NewMetadataMetricsMgr(metadataClient, r.m3Client, r.logger)

	r.uconfigClient = sVice.GetDConfigClient()
	r.dynamicConfigManage()

	return r, []thrift.TChanServer{rgen.NewTChanReplicatorServer(r), admin.NewTChanReplicatorAdminServer(r)}
}

// Start starts the replicator service
func (r *Replicator) Start(thriftService []thrift.TChanServer) {
	r.SCommon.Start(thriftService)
	r.hostIDHeartbeater = common.NewHostIDHeartbeater(r.metaClient, r.GetHostUUID(), r.GetHostPort(), r.GetHostName(), r.logger)
	r.hostIDHeartbeater.Start()
	r.replicatorclientFactory.SetTChannel(r.GetTChannel())

	r.metadataReconciler = NewMetadataReconciler(r.metaClient, r, r.localZone, r.logger, r.m3Client)
	r.metadataReconciler.Start()
}

// Stop stops the service
func (r *Replicator) Stop() {
	r.hostIDHeartbeater.Stop()
	for _, conn := range r.remoteReplicatorConn {
		conn.shutdown()
	}
	for _, conn := range r.storehostConn {
		conn.shutdown()
	}
	r.metadataReconciler.Stop()
	r.SCommon.Stop()
}

// RegisterWSHandler is the implementation of WSService interface
func (r *Replicator) RegisterWSHandler() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc(fmt.Sprintf(ccommon.HTTPHandlerPattern, ccommon.EndpointOpenReplicationRemoteReadStream), r.OpenReplicationRemoteReadStreamHandler)
	mux.HandleFunc(fmt.Sprintf(ccommon.HTTPHandlerPattern, ccommon.EndpointOpenReplicationReadStream), r.OpenReplicationReadStreamHandler)
	return mux
}

func (r *Replicator) getAuthoritativeZone() string {
	return r.authoritativeZone
}

func (r *Replicator) setAuthoritativeZone(zone string) {
	r.authoritativeZone = zone
}

// OpenReplicationReadStreamHandler is websocket handler for opening replication read stream.
// This is called by remote replicator to start a replication request for a local extent
// Internally the API will connect to local store to read the actual message
func (r *Replicator) OpenReplicationReadStreamHandler(w http.ResponseWriter, req *http.Request) {
	r.m3Client.IncCounter(metrics.OpenReplicationReadScope, metrics.ReplicatorRequests)
	request, err := common.GetOpenReplicationReadStreamRequestHTTP(req.Header)
	if err != nil {
		r.logger.WithField(common.TagErr, err).Error("unable to parse all needed headers")
		r.m3Client.IncCounter(metrics.OpenReplicationReadScope, metrics.ReplicatorBadRequest)
		r.m3Client.IncCounter(metrics.OpenReplicationReadScope, metrics.ReplicatorFailures)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// setup websocket server
	inStream, err := r.GetWSConnector().AcceptReplicationReadStream(w, req)
	if err != nil {
		r.logger.WithField(common.TagErr, err).Error("unable to upgrade websocket connection")
		r.m3Client.IncCounter(metrics.OpenReplicationReadScope, metrics.ReplicatorCreateInStreamFailure)
		r.m3Client.IncCounter(metrics.OpenReplicationReadScope, metrics.ReplicatorFailures)
		return
	}

	r.logger.WithFields(bark.Fields{
		common.TagExt:  common.FmtExt(*request.OpenReadStreamRequest.ExtentUUID),
		common.TagDst:  common.FmtDst(*request.OpenReadStreamRequest.DestinationUUID),
		common.TagAddr: common.FmtAddr(request.OpenReadStreamRequest.GetAddress()),
	}).Info(`Received OpenReplicationRead request`)

	destUUID := *request.DestinationUUID
	extUUID := *request.ExtentUUID

	// get the websocket stream to store host
	outStream, err := r.createStoreHostReadStream(destUUID, extUUID, request)
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagErr: err,
			common.TagExt: common.FmtExt(*request.OpenReadStreamRequest.ExtentUUID),
			common.TagDst: common.FmtDst(*request.OpenReadStreamRequest.DestinationUUID),
		}).Error("Can't open store host read stream")
		r.m3Client.IncCounter(metrics.OpenReplicationReadScope, metrics.ReplicatorCreateOutStreamFailure)
		r.m3Client.IncCounter(metrics.OpenReplicationReadScope, metrics.ReplicatorFailures)

		// Must close the connection on server side because closing on client side doesn't actually close the
		// underlying TCP connection
		inStream.Done()
		return
	}

	destDesc, err := r.metaClient.ReadDestination(nil, &shared.ReadDestinationRequest{
		DestinationUUID: common.StringPtr(destUUID),
	})
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagErr: err,
			common.TagExt: common.FmtExt(*request.OpenReadStreamRequest.ExtentUUID),
			common.TagDst: common.FmtDst(*request.OpenReadStreamRequest.DestinationUUID),
		}).Error("Failed to read destination")
		r.m3Client.IncCounter(metrics.OpenReplicationReadScope, metrics.ReplicatorFailures)

		// Must close the connection on server side because closing on client side doesn't actually close the
		// underlying TCP connection
		inStream.Done()
		return
	}

	outConn := newOutConnection(extUUID, destDesc.GetPath(), outStream, r.logger, r.m3Client, metrics.OpenReplicationReadScope)
	outConn.open()
	r.addStoreHostConn(outConn)
	defer r.removeStoreHostConn(outConn)

	inConn := newInConnection(extUUID, destDesc.GetPath(), inStream, outConn.msgsCh, r.logger, r.m3Client, metrics.OpenReplicationReadScope, metrics.OpenReplicationReadPerDestScope)
	inConn.open()

	outConn.WaitUntilDone()
	inConn.WaitUntilDone()
	return
}

// OpenReplicationRemoteReadStreamHandler is websocket handler for opening replication remote read stream.
// This is called by local store host to initiate a replication request for a remote extent
// Internally the API will connect to a remote replicator to read message
func (r *Replicator) OpenReplicationRemoteReadStreamHandler(w http.ResponseWriter, req *http.Request) {
	r.m3Client.IncCounter(metrics.OpenReplicationRemoteReadScope, metrics.ReplicatorRequests)
	request, err := common.GetOpenReplicationRemoteReadStreamRequestHTTP(req.Header)
	if err != nil {
		r.logger.WithField(common.TagErr, err).Error("unable to parse all needed headers")
		r.m3Client.IncCounter(metrics.OpenReplicationRemoteReadScope, metrics.ReplicatorBadRequest)
		r.m3Client.IncCounter(metrics.OpenReplicationRemoteReadScope, metrics.ReplicatorFailures)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// setup websocket server
	inStream, err := r.GetWSConnector().AcceptReplicationRemoteReadStream(w, req)
	if err != nil {
		r.logger.WithField(common.TagErr, err).Error("unable to upgrade websocket connection")
		r.m3Client.IncCounter(metrics.OpenReplicationRemoteReadScope, metrics.ReplicatorCreateInStreamFailure)
		r.m3Client.IncCounter(metrics.OpenReplicationRemoteReadScope, metrics.ReplicatorFailures)
		return
	}

	r.logger.WithFields(bark.Fields{
		common.TagExt:  common.FmtExt(*request.OpenReadStreamRequest.ExtentUUID),
		common.TagDst:  common.FmtDst(*request.OpenReadStreamRequest.DestinationUUID),
		common.TagAddr: common.FmtAddr(request.OpenReadStreamRequest.GetAddress()),
	}).Info(`Received OpenReplicationRemoteRead request`)

	extUUID := request.GetExtentUUID()
	destUUID := request.GetDestinationUUID()

	// get the websocket stream to a remote replicator
	outStream, err := r.createRemoteReplicationReadStream(extUUID, destUUID, request)
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagErr: err,
			common.TagExt: common.FmtExt(*request.OpenReadStreamRequest.ExtentUUID),
			common.TagDst: common.FmtDst(*request.OpenReadStreamRequest.DestinationUUID),
		}).Error("Can't open remote replication read stream")
		r.m3Client.IncCounter(metrics.OpenReplicationRemoteReadScope, metrics.ReplicatorCreateOutStreamFailure)
		r.m3Client.IncCounter(metrics.OpenReplicationRemoteReadScope, metrics.ReplicatorFailures)

		// Must close the connection on server side because closing on client side doesn't actually close the
		// underlying TCP connection
		inStream.Done()
		return
	}

	destDesc, err := r.metaClient.ReadDestination(nil, &shared.ReadDestinationRequest{
		DestinationUUID: common.StringPtr(destUUID),
	})
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagErr: err,
			common.TagExt: common.FmtExt(*request.OpenReadStreamRequest.ExtentUUID),
			common.TagDst: common.FmtDst(*request.OpenReadStreamRequest.DestinationUUID),
		}).Error("Failed to read destination")
		r.m3Client.IncCounter(metrics.OpenReplicationRemoteReadScope, metrics.ReplicatorFailures)

		// Must close the connection on server side because closing on client side doesn't actually close the
		// underlying TCP connection
		inStream.Done()
		return
	}

	outConn := newOutConnection(extUUID, destDesc.GetPath(), outStream, r.logger, r.m3Client, metrics.OpenReplicationRemoteReadScope)
	outConn.open()
	r.addRemoteReplicatorConn(outConn)
	defer r.removeRemoteReplicatorConn(outConn)

	inConn := newInConnection(extUUID, destDesc.GetPath(), inStream, outConn.msgsCh, r.logger, r.m3Client, metrics.OpenReplicationRemoteReadScope, metrics.OpenReplicationRemoteReadPerDestScope)
	inConn.open()

	outConn.WaitUntilDone()
	inConn.WaitUntilDone()
	return
}

// CreateDestinationUUID creates destination at local zone, expect to be called by replicator from authoritative zone
func (r *Replicator) CreateDestinationUUID(ctx thrift.Context, createRequest *shared.CreateDestinationUUIDRequest) (*shared.DestinationDescription, error) {
	r.m3Client.IncCounter(metrics.ReplicatorCreateDestUUIDScope, metrics.ReplicatorRequests)

	destDesc, err := r.metaClient.CreateDestinationUUID(ctx, createRequest)
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagDst:    common.FmtDst(createRequest.GetDestinationUUID()),
			common.TagDstPth: common.FmtDstPth(createRequest.GetRequest().GetPath()),
			common.TagErr:    err,
		}).Error(`Error creating destination`)
		r.m3Client.IncCounter(metrics.ReplicatorCreateDestUUIDScope, metrics.ReplicatorFailures)
		return nil, err
	}

	r.logger.WithFields(bark.Fields{
		common.TagDst:                 common.FmtDst(destDesc.GetDestinationUUID()),
		common.TagDstPth:              common.FmtDstPth(destDesc.GetPath()),
		`Type`:                        destDesc.GetType(),
		`Status`:                      destDesc.GetStatus(),
		`ConsumedMessagesRetention`:   destDesc.GetConsumedMessagesRetention(),
		`UnconsumedMessagesRetention`: destDesc.GetUnconsumedMessagesRetention(),
		`OwnerEmail`:                  destDesc.GetOwnerEmail(),
		`ChecksumOption`:              destDesc.GetChecksumOption(),
		`IsMultiZone`:                 destDesc.GetIsMultiZone(), // expected to be true
	}).Info(`Created Destination`)

	return destDesc, nil
}

// CreateRemoteDestinationUUID propagates creation to multiple remote zones, expect to be called only in authoritative zone
func (r *Replicator) CreateRemoteDestinationUUID(ctx thrift.Context, createRequest *shared.CreateDestinationUUIDRequest) error {
	r.m3Client.IncCounter(metrics.ReplicatorCreateRmtDestUUIDScope, metrics.ReplicatorRequests)

	if createRequest == nil || !createRequest.IsSetRequest() || !createRequest.IsSetDestinationUUID() {
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtDestUUIDScope, metrics.ReplicatorFailures)
		err := &shared.BadRequestError{Message: `Create remote destination request has nil request or nil uuid`}
		r.logger.WithField(common.TagErr, err).Error(`Create remote destination request verification failed`)
		return err
	}

	lclLg := r.logger.WithFields(bark.Fields{
		common.TagDst:    common.FmtDst(createRequest.GetDestinationUUID()),
		common.TagDstPth: common.FmtDstPth(createRequest.GetRequest().GetPath()),
	})

	if !createRequest.GetRequest().GetIsMultiZone() || !createRequest.GetRequest().IsSetZoneConfigs() || len(createRequest.GetRequest().GetZoneConfigs()) == 0 {
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtDestUUIDScope, metrics.ReplicatorFailures)
		err := &shared.BadRequestError{Message: fmt.Sprintf(`Not a valid create remote dest request for IsMultiZone [%v] or ZoneConfigs not set`, createRequest.GetRequest().GetIsMultiZone())}
		lclLg.WithField(common.TagErr, err).Error(`Create remote destination request verification failed`)
		return err
	}

	// in case no zone configured for current tenancy
	if _, ok := r.allZones[r.tenancy]; !ok {
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtDestUUIDScope, metrics.ReplicatorFailures)
		err := &shared.BadRequestError{Message: fmt.Sprintf(`Unknown tenancy [%s]`, r.tenancy)}
		lclLg.WithField(common.TagErr, err).Error(`Create remote destination failed with unknown tenancy`)
		return err
	}

	// for all the zones of current tenancy
	for _, zone := range r.allZones[r.tenancy] {
		// skip local zone
		if strings.EqualFold(zone, r.localZone) {
			continue
		}

		// call remote replicators in a goroutine. Errors can be ignored since reconciliation will fix the inconsistency eventually
		go r.createDestinationRemoteCall(zone, lclLg, createRequest)
	}

	return nil
}

func (r *Replicator) createDestinationRemoteCall(zone string, logger bark.Logger, createRequest *shared.CreateDestinationUUIDRequest) error {
	// acquire remote zone replicator thrift client
	client, err := r.replicatorclientFactory.GetReplicatorClient(zone)
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtDestUUIDScope, metrics.ReplicatorFailures)
		logger.WithFields(bark.Fields{
			common.TagErr:      err,
			common.TagZoneName: common.FmtZoneName(zone),
		}).Error(`Get remote replicator client failed`)
		return err
	}

	// send to remote zone replicator
	ctx, cancel := thrift.NewContext(remoteReplicatorCallTimeOut)
	defer cancel()
	_, err = client.CreateDestinationUUID(ctx, createRequest)
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtDestUUIDScope, metrics.ReplicatorFailures)
		logger.WithFields(bark.Fields{
			common.TagErr:      err,
			common.TagZoneName: common.FmtZoneName(zone),
		}).Error(`Create remote destination call failed`)
		return err
	}

	return nil
}

// UpdateDestination updates destination at local zone, expect to be called by replicator from authoritative zone
func (r *Replicator) UpdateDestination(ctx thrift.Context, updateRequest *shared.UpdateDestinationRequest) (*shared.DestinationDescription, error) {
	r.m3Client.IncCounter(metrics.ReplicatorUpdateDestScope, metrics.ReplicatorRequests)

	destDesc, err := r.metaClient.UpdateDestination(ctx, updateRequest)
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagDst: common.FmtDst(updateRequest.GetDestinationUUID()),
			common.TagErr: err,
		}).Error(`Error updating destination`)
		r.m3Client.IncCounter(metrics.ReplicatorUpdateDestScope, metrics.ReplicatorFailures)
		return nil, err
	}

	r.logger.WithFields(bark.Fields{
		common.TagDst:                 common.FmtDst(updateRequest.GetDestinationUUID()),
		`Type`:                        destDesc.GetType(),
		`Status`:                      destDesc.GetStatus(),
		`ConsumedMessagesRetention`:   destDesc.GetConsumedMessagesRetention(),
		`UnconsumedMessagesRetention`: destDesc.GetUnconsumedMessagesRetention(),
		`OwnerEmail`:                  destDesc.GetOwnerEmail(),
		`ChecksumOption`:              destDesc.GetChecksumOption(),
		`IsMultiZone`:                 destDesc.GetIsMultiZone(), // expected to be true
	}).Info(`Updated destination`)
	return destDesc, nil
}

// UpdateRemoteDestination propagates update to multiple remote zones, expect to be called only in authoritative zone
func (r *Replicator) UpdateRemoteDestination(ctx thrift.Context, updateRequest *shared.UpdateDestinationRequest) error {
	r.m3Client.IncCounter(metrics.ReplicatorUpdateRmtDestScope, metrics.ReplicatorRequests)

	if updateRequest == nil {
		r.m3Client.IncCounter(metrics.ReplicatorUpdateRmtDestScope, metrics.ReplicatorFailures)
		err := &shared.BadRequestError{Message: `Update remote destination request has nil request`}
		r.logger.WithField(common.TagErr, err).Error(`Update remote destination request verification failed`)
		return err
	}

	lclLg := r.logger.WithFields(bark.Fields{
		common.TagDst: common.FmtDst(updateRequest.GetDestinationUUID()),
	})

	// in case no zone configured for current tenancy
	if _, ok := r.allZones[r.tenancy]; !ok {
		r.m3Client.IncCounter(metrics.ReplicatorUpdateRmtDestScope, metrics.ReplicatorFailures)
		err := &shared.BadRequestError{Message: fmt.Sprintf(`Unknown tenancy [%s]`, r.tenancy)}
		lclLg.WithField(common.TagErr, err).Error(`Update remote destination failed with unknown tenancy`)
		return err
	}

	// for all the zones of current tenancy
	for _, zone := range r.allZones[r.tenancy] {
		// skip local zone
		if strings.EqualFold(zone, r.localZone) {
			continue
		}

		// call remote replicators in a goroutine. Errors can be ignored since reconciliation will fix the inconsistency eventually
		go r.updateDestinationRemoteCall(zone, lclLg, updateRequest)
	}

	return nil
}

func (r *Replicator) updateDestinationRemoteCall(zone string, logger bark.Logger, updateRequest *shared.UpdateDestinationRequest) error {
	// acquire remote zone replicator thrift client
	client, err := r.replicatorclientFactory.GetReplicatorClient(zone)
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorUpdateRmtDestScope, metrics.ReplicatorFailures)
		logger.WithFields(bark.Fields{
			common.TagErr:      err,
			common.TagZoneName: common.FmtZoneName(zone),
		}).Error(`Get remote replicator client failed`)
		return err
	}

	// send to remote zone replicator
	ctx, cancel := thrift.NewContext(remoteReplicatorCallTimeOut)
	defer cancel()
	_, err = client.UpdateDestination(ctx, updateRequest)
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorUpdateRmtDestScope, metrics.ReplicatorFailures)
		logger.WithFields(bark.Fields{
			common.TagErr:      err,
			common.TagZoneName: common.FmtZoneName(zone),
		}).Error(`Update remote destination call failed`)
		return err
	}

	return nil
}

// DeleteDestination deletes destination at local zone, expect to be called by replicator from authoritative zone
func (r *Replicator) DeleteDestination(ctx thrift.Context, deleteRequest *shared.DeleteDestinationRequest) error {
	r.m3Client.IncCounter(metrics.ReplicatorDeleteDestScope, metrics.ReplicatorRequests)

	err := r.metaClient.DeleteDestination(ctx, deleteRequest)
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagDstPth: common.FmtDstPth(deleteRequest.GetPath()),
			common.TagErr:    err,
		}).Error(`Error deleting destination`)
		r.m3Client.IncCounter(metrics.ReplicatorDeleteDestScope, metrics.ReplicatorFailures)
		return err
	}

	return nil
}

// DeleteRemoteDestination propagate deletion to multiple remote zones, expect to be called only in authoritative zone
func (r *Replicator) DeleteRemoteDestination(ctx thrift.Context, deleteRequest *shared.DeleteDestinationRequest) error {
	r.m3Client.IncCounter(metrics.ReplicatorDeleteRmtDestScope, metrics.ReplicatorRequests)

	if deleteRequest == nil {
		r.m3Client.IncCounter(metrics.ReplicatorDeleteRmtDestScope, metrics.ReplicatorFailures)
		err := &shared.BadRequestError{Message: `Delete remote destination request has nil request`}
		r.logger.WithField(common.TagErr, err).Error(`Delete remote destination request verification failed`)
		return err
	}

	lclLg := r.logger.WithFields(bark.Fields{
		common.TagDstPth: common.FmtDstPth(deleteRequest.GetPath()),
	})

	// in case no zone configured for current tenancy
	if _, ok := r.allZones[r.tenancy]; !ok {
		r.m3Client.IncCounter(metrics.ReplicatorDeleteRmtDestScope, metrics.ReplicatorFailures)
		err := &shared.BadRequestError{Message: fmt.Sprintf(`Unknown tenancy [%s]`, r.tenancy)}
		lclLg.WithField(common.TagErr, err).Error(`Update remote destination failed with unknown tenancy`)
		return err
	}

	// for all the zones of current tenancy
	for _, zone := range r.allZones[r.tenancy] {
		// skip local zone
		if strings.EqualFold(zone, r.localZone) {
			continue
		}

		// call remote replicators in a goroutine. Errors can be ignored since reconciliation will fix the inconsistency eventually
		go r.deleteDestinationRemoteCall(zone, lclLg, deleteRequest)
	}

	return nil
}

func (r *Replicator) deleteDestinationRemoteCall(zone string, logger bark.Logger, deleteRequest *shared.DeleteDestinationRequest) error {
	// acquire remote zone replicator thrift client
	client, err := r.replicatorclientFactory.GetReplicatorClient(zone)
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorDeleteRmtDestScope, metrics.ReplicatorFailures)
		logger.WithFields(bark.Fields{
			common.TagErr:      err,
			common.TagZoneName: common.FmtZoneName(zone),
		}).Error(`Get remote replicator client failed`)
		return err
	}

	// send to remote zone replicator
	ctx, cancel := thrift.NewContext(remoteReplicatorCallTimeOut)
	defer cancel()
	err = client.DeleteDestination(ctx, deleteRequest)
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorDeleteRmtDestScope, metrics.ReplicatorFailures)
		logger.WithFields(bark.Fields{
			common.TagErr:      err,
			common.TagZoneName: common.FmtZoneName(zone),
		}).Error(`Delete remote destination call failed`)
		return err
	}

	return nil
}

// CreateConsumerGroupUUID creates consumer group at local zone, expect to be called by replicator from authoritative zone
func (r *Replicator) CreateConsumerGroupUUID(ctx thrift.Context, createRequest *shared.CreateConsumerGroupUUIDRequest) (*shared.ConsumerGroupDescription, error) {
	r.m3Client.IncCounter(metrics.ReplicatorCreateCgUUIDScope, metrics.ReplicatorRequests)

	cgDesc, err := r.metaClient.CreateConsumerGroupUUID(ctx, createRequest)
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagCnsm:   common.FmtCnsm(createRequest.GetConsumerGroupUUID()),
			common.TagCnsPth: common.FmtCnsPth(createRequest.GetRequest().GetConsumerGroupName()),
			common.TagDstPth: common.FmtDstPth(createRequest.GetRequest().GetDestinationPath()),
			common.TagErr:    err,
		}).Error(`Error creating consumer group`)
		r.m3Client.IncCounter(metrics.ReplicatorCreateCgUUIDScope, metrics.ReplicatorFailures)
		return nil, err
	}

	r.logger.WithFields(bark.Fields{
		common.TagCnsm:   common.FmtCnsm(createRequest.GetConsumerGroupUUID()),
		common.TagCnsPth: common.FmtCnsPth(createRequest.GetRequest().GetConsumerGroupName()),
		common.TagDstPth: common.FmtDstPth(createRequest.GetRequest().GetDestinationPath()),
		common.TagDLQID:  common.FmtDLQID(cgDesc.GetDeadLetterQueueDestinationUUID()),
		`IsMultiZone`:    cgDesc.GetIsMultiZone(),
		`ActiveZone`:     cgDesc.GetActiveZone(),
	}).Info(`Created consumer group`)

	return cgDesc, nil
}

// CreateRemoteConsumerGroupUUID propagate creation to multiple remote zones, expect to be called only in authoritative zone
func (r *Replicator) CreateRemoteConsumerGroupUUID(ctx thrift.Context, createRequest *shared.CreateConsumerGroupUUIDRequest) error {
	r.m3Client.IncCounter(metrics.ReplicatorCreateRmtCgUUIDScope, metrics.ReplicatorRequests)

	if createRequest == nil || !createRequest.IsSetRequest() || !createRequest.IsSetConsumerGroupUUID() {
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtCgUUIDScope, metrics.ReplicatorFailures)
		err := &shared.BadRequestError{Message: `Create remote cg request has nil request or nil uuid`}
		r.logger.WithField(common.TagErr, err).Error(`Create remote cg request verification failed`)
		return err
	}

	lclLg := r.logger.WithFields(bark.Fields{
		common.TagCnsm:   common.FmtCnsm(createRequest.GetConsumerGroupUUID()),
		common.TagDstPth: common.FmtDstPth(createRequest.GetRequest().GetDestinationPath()),
		common.TagCnsPth: common.FmtCnsPth(createRequest.GetRequest().GetConsumerGroupName()),
	})

	if !createRequest.GetRequest().GetIsMultiZone() || !createRequest.GetRequest().IsSetZoneConfigs() || len(createRequest.GetRequest().GetZoneConfigs()) == 0 {
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtCgUUIDScope, metrics.ReplicatorFailures)
		err := &shared.BadRequestError{Message: fmt.Sprintf(`Not a valid create remote cg request for IsMultiZone [%v] or ZoneConfigs not set`, createRequest.GetRequest().GetIsMultiZone())}
		lclLg.WithField(common.TagErr, err).Error(`Create remote cg request verification failed`)
		return err
	}

	// in case no zone configured for current tenancy
	if _, ok := r.allZones[r.tenancy]; !ok {
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtCgUUIDScope, metrics.ReplicatorFailures)
		err := &shared.BadRequestError{Message: fmt.Sprintf(`Unknown tenancy [%s]`, r.tenancy)}
		lclLg.WithField(common.TagErr, err).Error(`Create remote cg failed with unknown tenancy`)
		return err
	}

	// for all the zones of current tenancy
	for _, zone := range r.allZones[r.tenancy] {
		// skip local zone
		if strings.EqualFold(zone, r.localZone) {
			continue
		}

		// call remote replicators in a goroutine. Errors can be ignored since reconciliation will fix the inconsistency eventually
		go r.createConsumerGroupRemoteCall(zone, lclLg, createRequest)
	}

	return nil
}

func (r *Replicator) createConsumerGroupRemoteCall(zone string, logger bark.Logger, createRequest *shared.CreateConsumerGroupUUIDRequest) error {
	// acquire remote zone replicator thrift client
	client, err := r.replicatorclientFactory.GetReplicatorClient(zone)
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtCgUUIDScope, metrics.ReplicatorFailures)
		logger.WithFields(bark.Fields{
			common.TagErr:      err,
			common.TagZoneName: common.FmtZoneName(zone),
		}).Error(`Get remote replicator client failed`)
		return err
	}

	// send to remote zone replicator
	ctx, cancel := thrift.NewContext(remoteReplicatorCallTimeOut)
	defer cancel()
	_, err = client.CreateConsumerGroupUUID(ctx, createRequest)
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtCgUUIDScope, metrics.ReplicatorFailures)
		logger.WithFields(bark.Fields{
			common.TagErr:      err,
			common.TagZoneName: common.FmtZoneName(zone),
		}).Error(`Create remote cg call failed`)
		return err
	}

	return nil
}

// UpdateConsumerGroup updates consumer group at local zone, expect to be called by replicator from authoritative zone
func (r *Replicator) UpdateConsumerGroup(ctx thrift.Context, updateRequest *shared.UpdateConsumerGroupRequest) (*shared.ConsumerGroupDescription, error) {
	r.m3Client.IncCounter(metrics.ReplicatorUpdateCgScope, metrics.ReplicatorRequests)

	cgDesc, err := r.metaClient.UpdateConsumerGroup(ctx, updateRequest)
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagCnsPth: common.FmtCnsPth(updateRequest.GetConsumerGroupName()),
			common.TagDstPth: common.FmtDstPth(updateRequest.GetDestinationPath()),
			common.TagDst:    common.FmtDst(cgDesc.GetDestinationUUID()),
			common.TagErr:    err,
		}).Error(`Error updating cg`)
		r.m3Client.IncCounter(metrics.ReplicatorUpdateCgScope, metrics.ReplicatorFailures)
		return nil, err
	}

	r.logger.WithFields(bark.Fields{
		common.TagCnsPth: common.FmtCnsPth(updateRequest.GetConsumerGroupName()),
		common.TagCnsm:   common.FmtCnsm(cgDesc.GetConsumerGroupUUID()),
		common.TagDstPth: common.FmtDstPth(updateRequest.GetDestinationPath()),
		common.TagDst:    common.FmtDst(cgDesc.GetDestinationUUID()),
		common.TagDLQID:  common.FmtDLQID(cgDesc.GetDeadLetterQueueDestinationUUID()),
		`IsMultiZone`:    cgDesc.GetIsMultiZone(),
		`ActiveZone`:     cgDesc.GetActiveZone(),
	}).Info(`Updated cg`)
	return cgDesc, nil
}

// UpdateRemoteConsumerGroup propagate update to multiple remote zones, expect to be called only in authoritative zone
func (r *Replicator) UpdateRemoteConsumerGroup(ctx thrift.Context, updateRequest *shared.UpdateConsumerGroupRequest) error {
	r.m3Client.IncCounter(metrics.ReplicatorUpdateRmtCgScope, metrics.ReplicatorRequests)

	if updateRequest == nil {
		r.m3Client.IncCounter(metrics.ReplicatorUpdateRmtCgScope, metrics.ReplicatorFailures)
		err := &shared.BadRequestError{Message: `Update remote cg request has nil request`}
		r.logger.WithField(common.TagErr, err).Error(`Update remote cg request verification failed`)
		return err
	}

	lclLg := r.logger.WithFields(bark.Fields{
		common.TagCnsPth: common.FmtCnsPth(updateRequest.GetConsumerGroupName()),
		common.TagDstPth: common.FmtDstPth(updateRequest.GetDestinationPath()),
	})

	// in case no zone configured for current tenancy
	if _, ok := r.allZones[r.tenancy]; !ok {
		r.m3Client.IncCounter(metrics.ReplicatorUpdateRmtCgScope, metrics.ReplicatorFailures)
		err := &shared.BadRequestError{Message: fmt.Sprintf(`Unknown tenancy [%s]`, r.tenancy)}
		lclLg.WithField(common.TagErr, err).Error(`Update remote destination failed with unknown tenancy`)
		return err
	}

	// for all the zones of current tenancy
	for _, zone := range r.allZones[r.tenancy] {
		// skip local zone
		if strings.EqualFold(zone, r.localZone) {
			continue
		}

		// call remote replicators in a goroutine. Errors can be ignored since reconciliation will fix the inconsistency eventually
		go r.updateCgRemoteCall(zone, lclLg, updateRequest)
	}

	return nil
}

func (r *Replicator) updateCgRemoteCall(zone string, logger bark.Logger, updateRequest *shared.UpdateConsumerGroupRequest) error {
	// acquire remote zone replicator thrift client
	client, err := r.replicatorclientFactory.GetReplicatorClient(zone)
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorUpdateRmtCgScope, metrics.ReplicatorFailures)
		logger.WithFields(bark.Fields{
			common.TagErr:      err,
			common.TagZoneName: common.FmtZoneName(zone),
		}).Error(`Get remote replicator client failed`)
		return err
	}

	// send to remote zone replicator
	ctx, cancel := thrift.NewContext(remoteReplicatorCallTimeOut)
	defer cancel()
	_, err = client.UpdateConsumerGroup(ctx, updateRequest)
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorUpdateRmtCgScope, metrics.ReplicatorFailures)
		logger.WithFields(bark.Fields{
			common.TagErr:      err,
			common.TagZoneName: common.FmtZoneName(zone),
		}).Error(`Update remote cg call failed`)
		return err
	}

	return nil
}

// DeleteConsumerGroup deletes consumer group at local zone, expect to be called by replicator from authoritative zone
func (r *Replicator) DeleteConsumerGroup(ctx thrift.Context, deleteRequest *shared.DeleteConsumerGroupRequest) error {
	r.m3Client.IncCounter(metrics.ReplicatorDeleteCgScope, metrics.ReplicatorRequests)

	err := r.metaClient.DeleteConsumerGroup(ctx, deleteRequest)
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagCnsPth: common.FmtCnsPth(deleteRequest.GetConsumerGroupName()),
			common.TagDstPth: common.FmtDstPth(deleteRequest.GetDestinationPath()),
			common.TagErr:    err,
		}).Error(`Error deleting cg`)
		r.m3Client.IncCounter(metrics.ReplicatorDeleteCgScope, metrics.ReplicatorFailures)
		return err
	}

	r.logger.WithFields(bark.Fields{
		common.TagCnsPth: common.FmtCnsPth(deleteRequest.GetConsumerGroupName()),
		common.TagDstPth: common.FmtDstPth(deleteRequest.GetDestinationPath()),
	}).Info(`Deleted cg`)

	return nil
}

// DeleteRemoteConsumerGroup propagate deletion to multiple remote zones, expect to be called only in authoritative zone
func (r *Replicator) DeleteRemoteConsumerGroup(ctx thrift.Context, deleteRequest *shared.DeleteConsumerGroupRequest) error {
	r.m3Client.IncCounter(metrics.ReplicatorDeleteRmtCgScope, metrics.ReplicatorRequests)

	if deleteRequest == nil {
		r.m3Client.IncCounter(metrics.ReplicatorDeleteRmtCgScope, metrics.ReplicatorFailures)
		err := &shared.BadRequestError{Message: `Delete remote cg request has nil request`}
		r.logger.WithField(common.TagErr, err).Error(`Delete remote cg request verification failed`)
		return err
	}

	lclLg := r.logger.WithFields(bark.Fields{
		common.TagCnsPth: common.FmtCnsPth(deleteRequest.GetConsumerGroupName()),
		common.TagDstPth: common.FmtDstPth(deleteRequest.GetDestinationPath()),
	})

	// in case no zone configured for current tenancy
	if _, ok := r.allZones[r.tenancy]; !ok {
		r.m3Client.IncCounter(metrics.ReplicatorDeleteRmtCgScope, metrics.ReplicatorFailures)
		err := &shared.BadRequestError{Message: fmt.Sprintf(`Unknown tenancy [%s]`, r.tenancy)}
		lclLg.WithField(common.TagErr, err).Error(`Delete remote cg failed with unknown tenancy`)
		return err
	}

	// for all the zones of current tenancy
	for _, zone := range r.allZones[r.tenancy] {
		// skip local zone
		if strings.EqualFold(zone, r.localZone) {
			continue
		}

		// call remote replicators in a goroutine. Errors can be ignored since reconciliation will fix the inconsistency eventually
		go r.deleteCgRemoteCall(zone, lclLg, deleteRequest)
	}

	return nil
}

func (r *Replicator) deleteCgRemoteCall(zone string, logger bark.Logger, deleteRequest *shared.DeleteConsumerGroupRequest) error {
	// acquire remote zone replicator thrift client
	client, err := r.replicatorclientFactory.GetReplicatorClient(zone)
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorDeleteRmtCgScope, metrics.ReplicatorFailures)
		logger.WithFields(bark.Fields{
			common.TagErr:      err,
			common.TagZoneName: common.FmtZoneName(zone),
		}).Error(`Get remote replicator client failed`)
		return err
	}

	// send to remote zone replicator
	ctx, cancel := thrift.NewContext(remoteReplicatorCallTimeOut)
	defer cancel()
	err = client.DeleteConsumerGroup(ctx, deleteRequest)
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorDeleteRmtCgScope, metrics.ReplicatorFailures)
		logger.WithFields(bark.Fields{
			common.TagErr:      err,
			common.TagZoneName: common.FmtZoneName(zone),
		}).Error(`Delete remote cg call failed`)
		return err
	}

	return nil
}

// CreateExtent create extent at local zone, expect to be called by remote replicator
func (r *Replicator) CreateExtent(ctx thrift.Context, createRequest *shared.CreateExtentRequest) (*shared.CreateExtentResult_, error) {
	lcllg := r.logger.WithFields(bark.Fields{
		common.TagDst:      common.FmtDst(createRequest.GetExtent().GetDestinationUUID()),
		common.TagExt:      common.FmtExt(createRequest.GetExtent().GetExtentUUID()),
		common.TagZoneName: common.FmtZoneName(createRequest.GetExtent().GetOriginZone()),
	})
	r.m3Client.IncCounter(metrics.ReplicatorCreateExtentScope, metrics.ReplicatorRequests)

	controller, err := r.GetClientFactory().GetControllerClient()
	if err != nil {
		lcllg.WithField(common.TagErr, err).Error(`Error getting controller client`)
		r.m3Client.IncCounter(metrics.ReplicatorCreateExtentScope, metrics.ReplicatorFailures)
		return nil, err
	}
	res, err := controller.CreateRemoteZoneExtent(ctx, createRequest)
	if err != nil {
		lcllg.WithField(common.TagErr, err).Error(`Error calling controller to create extent`)
		r.m3Client.IncCounter(metrics.ReplicatorCreateExtentScope, metrics.ReplicatorFailures)
		return nil, err
	}

	lcllg.Info(`Called controller to create extent`)
	return res, nil
}

// CreateRemoteExtent propagate creation request to multiple remote zones, expect to be called by local zone services
func (r *Replicator) CreateRemoteExtent(ctx thrift.Context, createRequest *shared.CreateExtentRequest) error {
	r.m3Client.IncCounter(metrics.ReplicatorCreateRmtExtentScope, metrics.ReplicatorRequests)

	var err error

	if createRequest == nil || !createRequest.IsSetExtent() || !createRequest.GetExtent().IsSetOriginZone() {
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtExtentScope, metrics.ReplicatorFailures)
		err := &shared.BadRequestError{Message: fmt.Sprintf(`Create remote extent request invalid. IsSetExtent: [%v], IsSetOriginZone: [%v]`,
			createRequest.IsSetExtent(), createRequest.IsSetExtent() && createRequest.GetExtent().IsSetOriginZone())}
		r.logger.WithField(common.TagErr, err).Error(`Create remote extent request verification failed`)
		return err
	}

	lcllg := r.logger.WithFields(bark.Fields{
		common.TagDst:      common.FmtDst(createRequest.GetExtent().GetDestinationUUID()),
		common.TagExt:      common.FmtExt(createRequest.GetExtent().GetExtentUUID()),
		common.TagZoneName: common.FmtZoneName(createRequest.GetExtent().GetOriginZone()),
	})

	readDestRequest := shared.NewReadDestinationRequest()
	readDestRequest.DestinationUUID = common.StringPtr(createRequest.GetExtent().GetDestinationUUID())
	destDesc, err := r.metaClient.ReadDestination(nil, readDestRequest)
	if err != nil {
		lcllg.WithField(common.TagErr, err).Error(`Error reading destination from metadata`)
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtExtentScope, metrics.ReplicatorFailures)
		return err
	}

	if !destDesc.GetIsMultiZone() {
		err = &shared.BadRequestError{Message: fmt.Sprintf(`Destination [%v] is not multi zone destination`, createRequest.GetExtent().GetDestinationUUID())}
		lcllg.WithField(common.TagErr, err).Error(`Destination is not multi zone destination`)
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtExtentScope, metrics.ReplicatorFailures)
		return err
	}

	if !destDesc.IsSetZoneConfigs() || len(destDesc.GetZoneConfigs()) == 0 {
		err = &shared.BadRequestError{Message: fmt.Sprintf(`Zone config for Destination [%v] is not set`, createRequest.GetExtent().GetDestinationUUID())}
		lcllg.WithField(common.TagErr, err).Error(`Zone config for destination is not set`)
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtExtentScope, metrics.ReplicatorFailures)
		return err
	}

	for _, zoneConfig := range destDesc.GetZoneConfigs() {
		// skip local zone
		if strings.EqualFold(zoneConfig.GetZone(), r.localZone) {
			continue
		}

		// only forward the call to remote zone if that zone allows consuming messages, or has AlwaysReplicateTo set to true
		if !zoneConfig.GetAllowConsume() && !zoneConfig.GetAlwaysReplicateTo() {
			continue
		}

		// call remote replicators in a goroutine. Errors can be ignored since reconciliation will fix the inconsistency eventually
		go r.createExtentRemoteCall(zoneConfig.GetZone(), lcllg, createRequest)
	}

	return nil
}

// CreateConsumerGroupExtent create cg extent at local zone, expect to be called by remote replicator
func (r *Replicator) CreateConsumerGroupExtent(ctx thrift.Context, createRequest *shared.CreateConsumerGroupExtentRequest) error {
	lcllg := r.logger.WithFields(bark.Fields{
		common.TagDst:  common.FmtDst(createRequest.GetDestinationUUID()),
		common.TagCnsm: common.FmtDst(createRequest.GetConsumerGroupUUID()),
		common.TagExt:  common.FmtExt(createRequest.GetExtentUUID()),
	})
	r.m3Client.IncCounter(metrics.ReplicatorCreateCgExtentScope, metrics.ReplicatorRequests)

	controller, err := r.GetClientFactory().GetControllerClient()
	if err != nil {
		lcllg.WithField(common.TagErr, err).Error(`Error getting controller client`)
		r.m3Client.IncCounter(metrics.ReplicatorCreateCgExtentScope, metrics.ReplicatorFailures)
		return err
	}
	err = controller.CreateRemoteZoneConsumerGroupExtent(ctx, createRequest)
	if err != nil {
		lcllg.WithField(common.TagErr, err).Error(`Error calling controller to create cg extent`)
		r.m3Client.IncCounter(metrics.ReplicatorCreateCgExtentScope, metrics.ReplicatorFailures)
		return err
	}

	lcllg.Info(`Called controller to create cg extent`)
	return nil
}

// CreateRemoteConsumerGroupExtent propagate creation request to multiple remote zones, expect to be called by local zone services
func (r *Replicator) CreateRemoteConsumerGroupExtent(ctx thrift.Context, createRequest *shared.CreateConsumerGroupExtentRequest) error {
	r.m3Client.IncCounter(metrics.ReplicatorCreateRmtCgExtentScope, metrics.ReplicatorRequests)

	var err error
	if createRequest == nil || !createRequest.IsSetDestinationUUID() || !createRequest.IsSetConsumerGroupUUID() || !createRequest.IsSetExtentUUID() {
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtCgExtentScope, metrics.ReplicatorFailures)
		err = &shared.BadRequestError{Message: fmt.Sprintf(`Create remote cg extent request invalid. IsSetDestinationUUID: [%v], IsSetConsumerGroupUUID: [%v], IsSetExtentUUID: [%v]`,
			createRequest.IsSetDestinationUUID(), createRequest.IsSetConsumerGroupUUID(), createRequest.IsSetExtentUUID())}
		r.logger.WithField(common.TagErr, err).Error(`Create remote cg extent request verification failed`)
		return err
	}

	lcllg := r.logger.WithFields(bark.Fields{
		common.TagDst:  common.FmtDst(createRequest.GetDestinationUUID()),
		common.TagCnsm: common.FmtDst(createRequest.GetConsumerGroupUUID()),
		common.TagExt:  common.FmtExt(createRequest.GetExtentUUID()),
	})

	readCgRequest := shared.ReadConsumerGroupRequest{
		ConsumerGroupUUID: common.StringPtr(createRequest.GetConsumerGroupUUID()),
	}
	cgDesc, err := r.metaClient.ReadConsumerGroup(nil, &readCgRequest)
	if err != nil {
		lcllg.WithField(common.TagErr, err).Error(`Error reading cg from metadata`)
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtCgExtentScope, metrics.ReplicatorFailures)
		return err
	}

	if !cgDesc.GetIsMultiZone() {
		err = &shared.BadRequestError{Message: fmt.Sprintf(`Consumer group [%v] is not multi zone`, createRequest.GetConsumerGroupUUID())}
		lcllg.WithField(common.TagErr, err).Error(`Consumer group is not multi zone destination`)
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtCgExtentScope, metrics.ReplicatorFailures)
		return err
	}

	if !cgDesc.IsSetZoneConfigs() || len(cgDesc.GetZoneConfigs()) == 0 {
		err = &shared.BadRequestError{Message: fmt.Sprintf(`Zone config for consumer group [%v] is not set`, createRequest.GetConsumerGroupUUID())}
		lcllg.WithField(common.TagErr, err).Error(`Zone config for consumer group is not set`)
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtCgExtentScope, metrics.ReplicatorFailures)
		return err
	}

	for _, zoneConfig := range cgDesc.GetZoneConfigs() {
		// skip local zone
		if strings.EqualFold(zoneConfig.GetZone(), r.localZone) {
			continue
		}

		// only forward the call to remote zone if consumer group is visible in that zone
		if !zoneConfig.GetVisible() {
			continue
		}

		// call remote replicators in a goroutine. Errors can be ignored since reconciliation will fix the inconsistency eventually
		go r.createCgExtentRemoteCall(zoneConfig.GetZone(), lcllg, createRequest)
	}

	return nil
}

func (r *Replicator) createCgExtentRemoteCall(zone string, logger bark.Logger, createRequest *shared.CreateConsumerGroupExtentRequest) error {
	// acquire remote zone replicator thrift client
	client, err := r.replicatorclientFactory.GetReplicatorClient(zone)
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtCgExtentScope, metrics.ReplicatorFailures)
		logger.WithField(common.TagErr, err).Error(`Get remote replicator client failed`)
		return err
	}

	// send to remote zone replicator
	ctx, cancel := thrift.NewContext(remoteReplicatorCallTimeOut)
	defer cancel()
	err = client.CreateConsumerGroupExtent(ctx, createRequest)
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtCgExtentScope, metrics.ReplicatorFailures)
		logger.WithField(common.TagErr, err).Error(`Create cg extent call failed`)
		return err
	}

	return nil
}

// SetAckOffset updates ack offsets in local zone
func (r *Replicator) SetAckOffset(ctx thrift.Context, request *shared.SetAckOffsetRequest) error {
	lcllg := r.logger.WithFields(bark.Fields{
		common.TagCnsm: common.FmtDst(request.GetConsumerGroupUUID()),
		common.TagExt:  common.FmtExt(request.GetExtentUUID()),
	})
	r.m3Client.IncCounter(metrics.ReplicatorSetAckOffsetScope, metrics.ReplicatorRequests)

	if !r.knownCgExtents.Contains(request.GetExtentUUID()) {
		// make sure the cg extent is created locally before accepting the SetAckOffset call.
		// otherwise SetAckOffset will create the cg extent entry with no store uuid or output host uuid
		// and we may not be able to clean up the entry eventually.
		_, err := r.metaClient.ReadConsumerGroupExtent(nil, &metadata.ReadConsumerGroupExtentRequest{
			ConsumerGroupUUID: common.StringPtr(request.GetConsumerGroupUUID()),
			ExtentUUID:        common.StringPtr(request.GetExtentUUID()),
		})
		if err != nil {
			lcllg.WithField(common.TagErr, err).Error(`SetAckOffset: Failed to read cg extent locally`)
			r.m3Client.IncCounter(metrics.ReplicatorSetAckOffsetScope, metrics.ReplicatorFailures)
			return err
		}

		r.knownCgExtents.Insert(request.GetExtentUUID())
	}

	err := r.metaClient.SetAckOffset(nil, request)
	if err != nil {
		lcllg.WithField(common.TagErr, err).Error(`Error calling metadata to set ack offset`)
		r.m3Client.IncCounter(metrics.ReplicatorSetAckOffsetScope, metrics.ReplicatorFailures)
		return err
	}

	return nil
}

// SetAckOffsetInRemote updates ack offsets in remote zone
func (r *Replicator) SetAckOffsetInRemote(ctx thrift.Context, request *shared.SetAckOffsetRequest) error {
	r.m3Client.IncCounter(metrics.ReplicatorSetAckOffsetInRemoteScope, metrics.ReplicatorRequests)

	var err error

	if request == nil || !request.IsSetConsumerGroupUUID() || !request.IsSetExtentUUID() {
		r.m3Client.IncCounter(metrics.ReplicatorSetAckOffsetInRemoteScope, metrics.ReplicatorFailures)
		err = &shared.BadRequestError{Message: fmt.Sprintf(`Set ack offset request invalid. IsSetConsumerGroupUUID: [%v], IsSetExtentUUID: [%v]`,
			request.IsSetConsumerGroupUUID(), request.IsSetExtentUUID())}
		r.logger.WithField(common.TagErr, err).Error(`Set ack offset request verification failed`)
		return err
	}

	lcllg := r.logger.WithFields(bark.Fields{
		common.TagCnsm: common.FmtDst(request.GetConsumerGroupUUID()),
		common.TagExt:  common.FmtExt(request.GetExtentUUID()),
	})

	readCgRequest := shared.ReadConsumerGroupRequest{
		ConsumerGroupUUID: common.StringPtr(request.GetConsumerGroupUUID()),
	}
	cgDesc, err := r.metaClient.ReadConsumerGroup(nil, &readCgRequest)
	if err != nil {
		lcllg.WithField(common.TagErr, err).Error(`Error reading cg from metadata`)
		r.m3Client.IncCounter(metrics.ReplicatorSetAckOffsetInRemoteScope, metrics.ReplicatorFailures)
		return err
	}

	if !cgDesc.GetIsMultiZone() {
		err = &shared.BadRequestError{Message: fmt.Sprintf(`Consumer group [%v] is not multi zone destination`, request.GetConsumerGroupUUID())}
		lcllg.WithField(common.TagErr, err).Error(`Consumer group is not multi zone destination`)
		r.m3Client.IncCounter(metrics.ReplicatorSetAckOffsetInRemoteScope, metrics.ReplicatorFailures)
		return err
	}

	if !cgDesc.IsSetZoneConfigs() || len(cgDesc.GetZoneConfigs()) == 0 {
		err = &shared.BadRequestError{Message: fmt.Sprintf(`Zone config for consumer group [%v] is not set`, request.GetConsumerGroupUUID())}
		lcllg.WithField(common.TagErr, err).Error(`Zone config for consumer group is not set`)
		r.m3Client.IncCounter(metrics.ReplicatorSetAckOffsetInRemoteScope, metrics.ReplicatorFailures)
		return err
	}

	for _, zoneConfig := range cgDesc.GetZoneConfigs() {
		// skip local zone
		if strings.EqualFold(zoneConfig.GetZone(), r.localZone) {
			continue
		}

		// only forward the call to remote zone if consumer group is visible in that zone
		if !zoneConfig.GetVisible() {
			continue
		}

		// call remote replicators in a goroutine. Errors can be ignored since reconciliation will fix the inconsistency eventually
		go r.setAckOffsetRemoteCall(zoneConfig.GetZone(), lcllg, request)
	}

	return nil
}

func (r *Replicator) setAckOffsetRemoteCall(zone string, logger bark.Logger, request *shared.SetAckOffsetRequest) error {
	// acquire remote zone replicator thrift client
	client, err := r.replicatorclientFactory.GetReplicatorClient(zone)
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorSetAckOffsetInRemoteScope, metrics.ReplicatorFailures)
		logger.WithField(common.TagErr, err).Error(`Get remote replicator client failed`)
		return err
	}

	// send to remote zone replicator
	// Note: intentionally use a shorter timeout as the ack level is being updated frequently.
	ctx, cancel := thrift.NewContext(remoteReplicatorSetAcklevelCallTimeOut)
	defer cancel()
	err = client.SetAckOffset(ctx, request)
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorSetAckOffsetInRemoteScope, metrics.ReplicatorFailures)
		logger.WithField(common.TagErr, err).Error(`Set ack offset call failed`)
		return err
	}

	return nil
}

// ListDestinations returns a list of destinations
func (r *Replicator) ListDestinations(ctx thrift.Context, listRequest *shared.ListDestinationsRequest) (*shared.ListDestinationsResult_, error) {
	return r.metaClient.ListDestinations(ctx, listRequest)
}

// ListDestinationsByUUID returns a list of destinations by UUID
func (r *Replicator) ListDestinationsByUUID(ctx thrift.Context, listRequest *shared.ListDestinationsByUUIDRequest) (*shared.ListDestinationsResult_, error) {
	return r.metaClient.ListDestinationsByUUID(ctx, listRequest)
}

// ListExtentsStats returns a list of extents
func (r *Replicator) ListExtentsStats(ctx thrift.Context, listRequest *shared.ListExtentsStatsRequest) (*shared.ListExtentsStatsResult_, error) {
	return r.metaClient.ListExtentsStats(ctx, listRequest)
}

// ReadDestination reads a destination
func (r *Replicator) ReadDestination(ctx thrift.Context, getRequest *shared.ReadDestinationRequest) (*shared.DestinationDescription, error) {
	return r.metaClient.ReadDestination(ctx, getRequest)
}

// ReadDestinationInRemoteZone reads a destination in remote zone
func (r *Replicator) ReadDestinationInRemoteZone(ctx thrift.Context, getRequest *shared.ReadDestinationInRemoteZoneRequest) (*shared.DestinationDescription, error) {
	// acquire remote zone replicator thrift client
	client, err := r.replicatorclientFactory.GetReplicatorClient(getRequest.GetZone())
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorReadDestinationInRemoteZoneScope, metrics.ReplicatorFailures)
		r.logger.WithField(common.TagErr, err).Error(`Get remote replicator client failed`)
		return nil, err
	}

	// send to remote zone replicator
	ctx, cancel := thrift.NewContext(remoteReplicatorCallTimeOut)
	defer cancel()
	return client.ReadDestination(ctx, getRequest.GetRequest())
}

// ReadConsumerGroup reads a cg
func (r *Replicator) ReadConsumerGroup(ctx thrift.Context, getRequest *shared.ReadConsumerGroupRequest) (*shared.ConsumerGroupDescription, error) {
	return r.metaClient.ReadConsumerGroup(ctx, getRequest)
}

// ReadConsumerGroupInRemoteZone reads a cg in remote zone
func (r *Replicator) ReadConsumerGroupInRemoteZone(ctx thrift.Context, getRequest *shared.ReadConsumerGroupInRemoteRequest) (*shared.ConsumerGroupDescription, error) {
	// acquire remote zone replicator thrift client
	client, err := r.replicatorclientFactory.GetReplicatorClient(getRequest.GetZone())
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorReadCgInRemoteZoneScope, metrics.ReplicatorFailures)
		r.logger.WithField(common.TagErr, err).Error(`Get remote replicator client failed`)
		return nil, err
	}

	// send to remote zone replicator
	ctx, cancel := thrift.NewContext(remoteReplicatorCallTimeOut)
	defer cancel()
	return client.ReadConsumerGroup(ctx, getRequest.GetRequest())
}

// ListConsumerGroups list consumer groups
func (r *Replicator) ListConsumerGroups(ctx thrift.Context, getRequest *shared.ListConsumerGroupRequest) (*shared.ListConsumerGroupResult_, error) {
	return r.metaClient.ListConsumerGroups(ctx, getRequest)
}

// ReadConsumerGroupExtents list consumer group extents
func (r *Replicator) ReadConsumerGroupExtents(ctx thrift.Context, getRequest *shared.ReadConsumerGroupExtentsRequest) (*shared.ReadConsumerGroupExtentsResult_, error) {
	return r.metaClient.ReadConsumerGroupExtents(ctx, getRequest)
}

func (r *Replicator) createExtentRemoteCall(zone string, logger bark.Logger, createRequest *shared.CreateExtentRequest) error {
	// acquire remote zone replicator thrift client
	client, err := r.replicatorclientFactory.GetReplicatorClient(zone)
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtExtentScope, metrics.ReplicatorFailures)
		logger.WithField(common.TagErr, err).Error(`Get remote replicator client failed`)
		return err
	}

	// send to remote zone replicator
	ctx, cancel := thrift.NewContext(remoteReplicatorCallTimeOut)
	defer cancel()
	_, err = client.CreateExtent(ctx, createRequest)
	if err != nil {
		r.m3Client.IncCounter(metrics.ReplicatorCreateRmtExtentScope, metrics.ReplicatorFailures)
		logger.WithField(common.TagErr, err).Error(`Create extent call failed`)
		return err
	}

	return nil
}

func (r *Replicator) addRemoteReplicatorConn(conn *outConnection) {
	r.remoteReplicatorConnMutex.Lock()
	defer r.remoteReplicatorConnMutex.Unlock()
	r.remoteReplicatorConn[conn.connUUID] = conn
}

func (r *Replicator) removeRemoteReplicatorConn(conn *outConnection) {
	r.remoteReplicatorConnMutex.Lock()
	defer r.remoteReplicatorConnMutex.Unlock()
	delete(r.remoteReplicatorConn, conn.connUUID)
}

func (r *Replicator) addStoreHostConn(conn *outConnection) {
	r.storehostConnMutex.Lock()
	defer r.storehostConnMutex.Unlock()
	r.storehostConn[conn.connUUID] = conn
}

func (r *Replicator) removeStoreHostConn(conn *outConnection) {
	r.storehostConnMutex.Lock()
	defer r.storehostConnMutex.Unlock()
	delete(r.storehostConn, conn.connUUID)
}

func (r *Replicator) createRemoteReplicationReadStream(extUUID string, destUUID string, request *common.OpenReplicationRemoteReadStreamRequest) (stream storeStream.BStoreOpenReadStreamOutCall, err error) {
	readExtentStats := &metadata.ReadExtentStatsRequest{
		DestinationUUID: common.StringPtr(destUUID),
		ExtentUUID:      common.StringPtr(extUUID)}
	extentStatsResult, err := r.metaClient.ReadExtentStats(nil, readExtentStats)
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagErr: err,
			common.TagDst: common.FmtDst(destUUID),
			common.TagExt: common.FmtExt(extUUID),
		}).Error(`replicator: Failed to read extent stats from metadata`)
		return
	}

	remoteZone := extentStatsResult.GetExtentStats().GetExtent().GetOriginZone()
	remoteDeployment := fmt.Sprintf("%v_%v", r.tenancy, remoteZone)
	hosts := r.replicatorclientFactory.GetHostsForDeployment(remoteDeployment)
	if len(hosts) < 1 {
		err = &shared.BadRequestError{Message: fmt.Sprintf("Deployment [%v] doesn't have any host in config", remoteDeployment)}
		r.logger.WithFields(bark.Fields{common.TagErr: err, common.TagDeploymentName: remoteDeployment}).Error("Deployment doesn't have any host in config")
		return
	}

	host := hosts[rand.Intn(len(hosts))]
	port := strconv.Itoa(r.AppConfig.GetServiceConfig(common.ReplicatorServiceName).GetWebsocketPort())
	hostPort := net.JoinHostPort(host, port)

	r.logger.WithFields(bark.Fields{
		common.TagExt:      common.FmtExt(extUUID),
		common.TagHostPort: common.FmtHostPort(hostPort),
	}).Info(`dialing host`)

	httpHeaders := common.GetOpenReplicationRemoteReadStreamRequestHTTPHeaders(request)
	stream, err = r.GetWSConnector().OpenReplicationReadStream(hostPort, httpHeaders)
	if err != nil {
		r.logger.WithField(common.TagErr, err).Error(`replicator: Websocket dial remote replicator: failed`)
		return
	}
	return
}

func (r *Replicator) createStoreHostReadStream(destUUID string, extUUID string, request *common.OpenReplicationReadStreamRequest) (stream storeStream.BStoreOpenReadStreamOutCall, err error) {
	readExtentStats := &metadata.ReadExtentStatsRequest{
		DestinationUUID: common.StringPtr(destUUID),
		ExtentUUID:      common.StringPtr(extUUID),
	}

	extentStatsResult, err := r.metaClient.ReadExtentStats(nil, readExtentStats)
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagErr: err,
			common.TagDst: common.FmtDst(destUUID),
			common.TagExt: common.FmtExt(extUUID),
		}).Error(`replicator: Failed to read extent stats from metadata`)
		return
	}

	replicaUUIDs := extentStatsResult.GetExtentStats().GetExtent().GetStoreUUIDs()
	if len(replicaUUIDs) == 0 {
		err = fmt.Errorf("No replica for extent %v", extUUID)
		r.logger.WithFields(bark.Fields{
			common.TagErr: err,
			common.TagDst: common.FmtDst(destUUID),
			common.TagExt: common.FmtExt(extUUID),
		}).Error(`replicator: No replica for extent`)
		return
	}

	var rpm = r.GetRingpopMonitor()
	hostPort, err := rpm.ResolveUUID(common.StoreServiceName, replicaUUIDs[0])
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagErr:  err,
			common.TagDst:  common.FmtDst(destUUID),
			common.TagExt:  common.FmtExt(extUUID),
			common.TagStor: common.FmtStor(replicaUUIDs[0]),
		}).Error(`replicator: Error resolving storehost`)
		return
	}

	host, _, _ := net.SplitHostPort(hostPort)
	port := strconv.Itoa(r.AppConfig.GetServiceConfig(common.StoreServiceName).GetWebsocketPort())
	hostPort = net.JoinHostPort(host, port)

	r.logger.WithFields(bark.Fields{
		common.TagExt:      common.FmtExt(extUUID),
		common.TagHostPort: common.FmtHostPort(hostPort),
	}).Info(`dialing host`)

	httpHeaders := common.GetOpenReadStreamRequestHTTPHeaders(&request.OpenReadStreamRequest)
	stream, err = r.GetWSConnector().OpenReadStream(hostPort, httpHeaders)
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagErr:  err,
			common.TagDst:  common.FmtDst(destUUID),
			common.TagExt:  common.FmtExt(extUUID),
			common.TagStor: common.FmtStor(replicaUUIDs[0]),
		}).Error(`replicator: Websocket dial store host: failed`)
	}

	return
}
