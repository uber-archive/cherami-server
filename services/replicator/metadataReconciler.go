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
	"strings"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

type (
	// MetadataReconciler periodically queries metadata and
	// compare with the metadata from remote replicator and try
	// to reconcile the states
	MetadataReconciler interface {
		common.Daemon
	}

	// metadataReconciler is an implementation of MetadataReconciler.
	metadataReconciler struct {
		replicator *Replicator
		localZone  string

		mClient  metadata.TChanMetadataService
		logger   bark.Logger
		m3Client metrics.Client

		closeChannel chan struct{}

		ticker  *time.Ticker
		running int64
	}
)

const (
	// runInterval determines how often the reconciler will run
	runInterval                 = time.Duration(10 * time.Minute)
	metadataListRequestPageSize = 50
)

// NewMetadataReconciler returns an instance of MetadataReconciler
func NewMetadataReconciler(mClient metadata.TChanMetadataService, replicator *Replicator, localZone string, logger bark.Logger, m3client metrics.Client) MetadataReconciler {
	return &metadataReconciler{
		replicator: replicator,
		localZone:  localZone,
		mClient:    mClient,
		logger:     logger,
		m3Client:   m3client,
		ticker:     time.NewTicker(runInterval),
		running:    0,
	}
}

func (r *metadataReconciler) Start() {
	r.logger.Info("MetadataReconciler: started")

	r.closeChannel = make(chan struct{})
	go r.run()
	go r.houseKeep()
}

func (r *metadataReconciler) Stop() {
	close(r.closeChannel)

	r.logger.Info("MetadataReconciler: stopped")
}

func (r *metadataReconciler) run() {
	primaryHost, err := r.replicator.GetRingpopMonitor().FindHostForKey(common.ReplicatorServiceName, common.ReplicatorServiceName)
	if err != nil {
		r.logger.WithField(common.TagErr, err).Error(`Error getting primary replicator from ringpop`)
		return
	}

	// reconciler only needs to run on primary replicator
	if primaryHost.UUID != r.replicator.GetHostUUID() {
		return
	}

	if !atomic.CompareAndSwapInt64(&r.running, 0, 1) {
		r.logger.Warn("Prev run is still ongoing...")
		return
	}

	// destination metadata reconciliation is only needed if this is a non-authoritative zone
	if r.localZone != r.replicator.getAuthoritativeZone() {
		r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileDestRun, 1)
		err = r.reconcileDestMetadata()
		if err != nil {
			r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileDestFail, 1)
		}
	}

	// reconcile destination extents
	r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileDestExtentRun, 1)
	err = r.reconcileDestExtentMetadata()
	if err != nil {
		r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileDestExtentFail, 1)
	}

	atomic.StoreInt64(&r.running, 0)
}

func (r *metadataReconciler) reconcileDestMetadata() error {
	localDests, err := r.getAllMultiZoneDestInLocalZone()
	if err != nil {
		return err
	}

	remoteDests, err := r.getAllMultiZoneDestInAuthoritativeZone()
	if err != nil {
		return err
	}

	return r.reconcileDest(localDests, remoteDests)
}

func (r *metadataReconciler) reconcileDest(localDests []*shared.DestinationDescription, remoteDests []*shared.DestinationDescription) error {
	localDestsSet := make(map[string]*shared.DestinationDescription)
	for _, dest := range localDests {
		localDestsSet[dest.GetDestinationUUID()] = dest
	}

	for _, remoteDest := range remoteDests {
		localDest, ok := localDestsSet[remoteDest.GetDestinationUUID()]
		if ok {
			if remoteDest.GetStatus() == shared.DestinationStatus_DELETING || remoteDest.GetStatus() == shared.DestinationStatus_DELETED {
				// case #1: destination gets deleted in remote, but not deleted in local. Delete the destination locally
				if !(localDest.GetStatus() == shared.DestinationStatus_DELETING || localDest.GetStatus() == shared.DestinationStatus_DELETED) {
					r.logger.WithField(common.TagDst, common.FmtDst(remoteDest.GetDestinationUUID())).Info(`Found deleted/deleting destination from remote but not deleted/deleting locally`)
					deleteRequest := &shared.DeleteDestinationRequest{
						Path: common.StringPtr(remoteDest.GetPath()),
					}
					ctx, cancel := thrift.NewContext(localReplicatorCallTimeOut)
					defer cancel()
					err := r.replicator.DeleteDestination(ctx, deleteRequest)
					if err != nil {
						r.logger.WithFields(bark.Fields{
							common.TagErr: err,
							common.TagDst: common.FmtDst(remoteDest.GetDestinationUUID()),
						}).Error(`Failed to delete destination in local zone for reconciliation`)
						continue
					}
				} else {
					r.logger.WithField(common.TagDst, common.FmtDst(remoteDest.GetDestinationUUID())).Info(`Found destination is deleted/deleting in both remote and local`)
					continue
				}
				continue
			}

			// case #2: destination exists in both remote and local, try to compare the property to see if anything gets updated
			updateRequest := &shared.UpdateDestinationRequest{
				DestinationUUID: common.StringPtr(remoteDest.GetDestinationUUID()),
			}
			destUpdated := false

			// TODO: Do we need to support updating retention?
			//if localDest.GetConsumedMessagesRetention() != remoteDest.GetConsumedMessagesRetention() {
			//	updateRequest.ConsumedMessagesRetention = common.Int32Ptr(remoteDest.GetConsumedMessagesRetention())
			//	destUpdated = true
			//}
			//if localDest.GetUnconsumedMessagesRetention() != remoteDest.GetUnconsumedMessagesRetention() {
			//	updateRequest.UnconsumedMessagesRetention = common.Int32Ptr(remoteDest.GetUnconsumedMessagesRetention())
			//	destUpdated = true
			//}
			if localDest.GetOwnerEmail() != remoteDest.GetOwnerEmail() {
				updateRequest.OwnerEmail = common.StringPtr(remoteDest.GetOwnerEmail())
				destUpdated = true
			}
			if localDest.GetChecksumOption() != remoteDest.GetChecksumOption() {
				updateRequest.ChecksumOption = common.InternalChecksumOptionPtr(remoteDest.GetChecksumOption())
				destUpdated = true
			}

			if destUpdated {
				r.logger.WithField(common.TagDst, common.FmtDst(remoteDest.GetDestinationUUID())).Info(`Found destination gets updated in remote but not in local`)
				ctx, cancel := thrift.NewContext(localReplicatorCallTimeOut)
				defer cancel()
				_, err := r.replicator.UpdateDestination(ctx, updateRequest)
				if err != nil {
					r.logger.WithFields(bark.Fields{
						common.TagErr: err,
						common.TagDst: common.FmtDst(remoteDest.GetDestinationUUID()),
					}).Error(`Failed to update destination in local zone for reconciliation`)
					continue
				}
			}
		} else {
			// case #3: destination exists in remote, but not in local. Create the destination locally
			r.logger.WithField(common.TagDst, common.FmtDst(remoteDest.GetDestinationUUID())).Warn(`Found missing destination from remote!`)
			r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileDestFoundMissing, 1)

			// If the missing destination is in deleting/deleted status, we don't need to create the destination locally
			if remoteDest.GetStatus() == shared.DestinationStatus_DELETING || remoteDest.GetStatus() == shared.DestinationStatus_DELETED {
				r.logger.WithField(common.TagDst, common.FmtDst(remoteDest.GetDestinationUUID())).Info(`Found missing destination from remote but in deleted/deleting state`)
				continue
			}
			createRequest := &shared.CreateDestinationUUIDRequest{
				Request: &shared.CreateDestinationRequest{
					Path: common.StringPtr(remoteDest.GetPath()),
					Type: common.InternalDestinationTypePtr(remoteDest.GetType()),
					ConsumedMessagesRetention:   common.Int32Ptr(remoteDest.GetConsumedMessagesRetention()),
					UnconsumedMessagesRetention: common.Int32Ptr(remoteDest.GetUnconsumedMessagesRetention()),
					OwnerEmail:                  common.StringPtr(remoteDest.GetOwnerEmail()),
					ChecksumOption:              common.InternalChecksumOptionPtr(remoteDest.GetChecksumOption()),
					IsMultiZone:                 common.BoolPtr(remoteDest.GetIsMultiZone()),
					ZoneConfigs:                 remoteDest.GetZoneConfigs(),
					SchemaInfo:                  remoteDest.GetSchemaInfo(),
				},
				DestinationUUID: common.StringPtr(remoteDest.GetDestinationUUID()),
			}

			ctx, cancel := thrift.NewContext(localReplicatorCallTimeOut)
			defer cancel()
			_, err := r.replicator.CreateDestinationUUID(ctx, createRequest)
			if err != nil {
				r.logger.WithFields(bark.Fields{
					common.TagErr: err,
					common.TagDst: common.FmtDst(remoteDest.GetDestinationUUID()),
				}).Error(`Failed to create destination in local zone for reconciliation`)
				continue
			}
		}
	}
	return nil
}

func (r *metadataReconciler) getAllMultiZoneDestInLocalZone() ([]*shared.DestinationDescription, error) {
	listReq := &shared.ListDestinationsByUUIDRequest{
		MultiZoneOnly:            common.BoolPtr(true),
		ValidateAgainstPathTable: common.BoolPtr(true),
		Limit: common.Int64Ptr(metadataListRequestPageSize),
	}

	var dests []*shared.DestinationDescription

	for {
		res, err := r.mClient.ListDestinationsByUUID(nil, listReq)
		if err != nil {
			r.logger.WithField(common.TagErr, err).Error(`Metadata call ListDestinationsByUUID failed`)
			return nil, err
		}

		dests = append(dests, res.GetDestinations()...)

		if len(res.GetNextPageToken()) == 0 {
			break
		}

		listReq.PageToken = res.GetNextPageToken()
	}
	return dests, nil
}

func (r *metadataReconciler) getAllMultiZoneDestInAuthoritativeZone() ([]*shared.DestinationDescription, error) {
	var err error
	authoritativeZone := r.replicator.getAuthoritativeZone()
	remoteReplicator, err := r.replicator.clientFactory.GetReplicatorClient(authoritativeZone)
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagErr:      err,
			common.TagZoneName: common.FmtZoneName(authoritativeZone),
		}).Error(`Failed to get remote replicator client`)
		return nil, err
	}

	listReq := &shared.ListDestinationsByUUIDRequest{
		MultiZoneOnly:            common.BoolPtr(true),
		ValidateAgainstPathTable: common.BoolPtr(true),
		Limit: common.Int64Ptr(metadataListRequestPageSize),
	}

	var dests []*shared.DestinationDescription

	for {
		ctx, cancel := thrift.NewContext(remoteReplicatorCallTimeOut)
		defer cancel()
		res, err := remoteReplicator.ListDestinationsByUUID(ctx, listReq)
		if err != nil {
			r.logger.WithField(common.TagErr, err).Error(`Remote replicator call ListDestinationsByUUID failed`)
			return nil, err
		}

		dests = append(dests, res.GetDestinations()...)

		if len(res.GetNextPageToken()) == 0 {
			break
		}

		listReq.PageToken = res.GetNextPageToken()
	}
	return dests, nil
}

func (r *metadataReconciler) reconcileDestExtentMetadata() error {
	dests, err := r.getAllMultiZoneDestInLocalZone()
	if err != nil {
		return err
	}

	for _, dest := range dests {
		localExtents, errCur := r.getAllDestExtentInCurrentZone(dest.GetDestinationUUID())
		if errCur != nil {
			continue
		}
		for _, zoneConfig := range dest.GetZoneConfigs() {
			// skip local zone
			if strings.EqualFold(zoneConfig.GetZone(), r.localZone) {
				continue
			}

			if zoneConfig.GetAllowPublish() {
				remoteExtents, errRemote := r.getAllDestExtentInRemoteZone(zoneConfig.GetZone(), dest.GetDestinationUUID())
				if errRemote != nil {
					continue
				}

				if err = r.reconcileDestExtent(dest.GetDestinationUUID(), localExtents, remoteExtents, zoneConfig.GetZone()); err != nil {
					continue
				}
			}
		}
	}
	return nil
}

func (r *metadataReconciler) getAllDestExtentInRemoteZone(zone string, destUUID string) (map[string]shared.ExtentStatus, error) {
	var err error
	remoteReplicator, err := r.replicator.clientFactory.GetReplicatorClient(zone)
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagErr:      err,
			common.TagZoneName: common.FmtZoneName(zone),
		}).Error(`Failed to get remote replicator client`)
		return nil, err
	}

	listReq := &shared.ListExtentsStatsRequest{
		DestinationUUID:  common.StringPtr(destUUID),
		LocalExtentsOnly: common.BoolPtr(true),
		Limit:            common.Int64Ptr(metadataListRequestPageSize),
	}

	extents := make(map[string]shared.ExtentStatus)
	for {
		ctx, cancel := thrift.NewContext(remoteReplicatorCallTimeOut)
		defer cancel()
		res, err := remoteReplicator.ListExtentsStats(ctx, listReq)
		if err != nil {
			r.logger.WithField(common.TagErr, err).Error(`Remote replicator call ListExtentsStats failed`)
			return nil, err
		}

		for _, ext := range res.GetExtentStatsList() {
			extents[ext.GetExtent().GetExtentUUID()] = ext.GetStatus()
		}

		if len(res.GetNextPageToken()) == 0 {
			break
		}

		listReq.PageToken = res.GetNextPageToken()
	}
	return extents, nil
}

func (r *metadataReconciler) getAllDestExtentInCurrentZone(destUUID string) (map[string]shared.ExtentStatus, error) {
	listReq := &shared.ListExtentsStatsRequest{
		DestinationUUID:  common.StringPtr(destUUID),
		LocalExtentsOnly: common.BoolPtr(false),
		Limit:            common.Int64Ptr(metadataListRequestPageSize),
	}

	extents := make(map[string]shared.ExtentStatus)
	for {
		res, err := r.mClient.ListExtentsStats(nil, listReq)
		if err != nil {
			r.logger.WithField(common.TagErr, err).Error(`Metadata call ListExtentsStats failed`)
			return nil, err
		}

		for _, ext := range res.GetExtentStatsList() {
			extents[ext.GetExtent().GetExtentUUID()] = ext.GetStatus()
		}

		if len(res.GetNextPageToken()) == 0 {
			break
		}

		listReq.PageToken = res.GetNextPageToken()
	}
	return extents, nil
}

func (r *metadataReconciler) reconcileDestExtent(destUUID string, localExtents map[string]shared.ExtentStatus, remoteExtents map[string]shared.ExtentStatus, remoteZone string) error {
	for remoteExtentUUID, remoteExtentStatus := range remoteExtents {
		localExtentStatus, ok := localExtents[remoteExtentUUID]
		if !ok {
			r.logger.WithFields(bark.Fields{
				common.TagDst:      common.FmtDst(destUUID),
				common.TagExt:      common.FmtExt(remoteExtentUUID),
				common.TagZoneName: common.FmtZoneName(remoteZone),
			}).Warn(`Found missing extent from remote!`)
			r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileDestExtentFoundMissing, 1)

			createRequest := &shared.CreateExtentRequest{
				Extent: &shared.Extent{
					ExtentUUID:      common.StringPtr(remoteExtentUUID),
					DestinationUUID: common.StringPtr(destUUID),
					InputHostUUID:   common.StringPtr(common.InputHostForRemoteExtent),
					StoreUUIDs:      []string{},
					OriginZone:      common.StringPtr(remoteZone),
				},
			}
			ctx, cancel := thrift.NewContext(localReplicatorCallTimeOut)
			defer cancel()
			_, err := r.replicator.CreateExtent(ctx, createRequest)
			if err != nil {
				r.logger.WithFields(bark.Fields{
					common.TagErr:      err,
					common.TagDst:      common.FmtDst(destUUID),
					common.TagExt:      common.FmtExt(remoteExtentUUID),
					common.TagZoneName: common.FmtZoneName(remoteZone),
				}).Error(`Failed to create extent in local zone for reconciliation`)
				continue
			}
		} else {
			if remoteExtentStatus != shared.ExtentStatus_OPEN && localExtentStatus == shared.ExtentStatus_OPEN {
				r.logger.WithFields(bark.Fields{
					common.TagDst:      common.FmtDst(destUUID),
					common.TagExt:      common.FmtExt(remoteExtentUUID),
					common.TagZoneName: common.FmtZoneName(remoteZone),
				}).Info(`Inconsistent extent status`)
				r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileDestExtentInconsistentStatus, 1)

				updateRequest := &metadata.UpdateExtentStatsRequest{
					ExtentUUID:      common.StringPtr(remoteExtentUUID),
					DestinationUUID: common.StringPtr(destUUID),
					Status:          common.MetadataExtentStatusPtr(shared.ExtentStatus_SEALED),
				}
				_, err := r.mClient.UpdateExtentStats(nil, updateRequest)
				if err != nil {
					r.logger.WithFields(bark.Fields{
						common.TagErr: err,
						common.TagDst: common.FmtDst(destUUID),
						common.TagExt: common.FmtExt(remoteExtentUUID),
					}).Error(`Failed to update extent status to sealed`)
					continue
				}

				r.logger.WithFields(bark.Fields{
					common.TagDst: common.FmtDst(destUUID),
					common.TagExt: common.FmtExt(remoteExtentUUID),
				}).Info(`Extent SEALED`)

			}
		}
	}
	return nil
}

func (r *metadataReconciler) houseKeep() {
	for {
		select {
		case <-r.ticker.C:
			go r.run()
		case <-r.closeChannel:
			return
		}
	}
}
