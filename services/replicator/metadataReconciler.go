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
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"
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
		replicator            *Replicator
		localZone             string
		suspectMissingExtents map[string]missingExtentInfo

		mClient  metadata.TChanMetadataService
		logger   bark.Logger
		m3Client metrics.Client

		closeChannel chan struct{}

		ticker  *time.Ticker
		running int64
	}

	missingExtentInfo struct {
		missingSince time.Time
		destUUID     string
	}
)

const (
	// runInterval determines how often the reconciler will run
	runInterval                    = time.Duration(10 * time.Minute)
	metadataListRequestPageSize    = 50
	storeCallTimeout               = 10 * time.Second
	extentMissingDurationThreshold = time.Duration(1 * time.Hour)
)

// NewMetadataReconciler returns an instance of MetadataReconciler
func NewMetadataReconciler(mClient metadata.TChanMetadataService, replicator *Replicator, localZone string, logger bark.Logger, m3client metrics.Client) MetadataReconciler {
	return &metadataReconciler{
		replicator:            replicator,
		localZone:             localZone,
		suspectMissingExtents: make(map[string]missingExtentInfo),
		mClient:               mClient,
		logger:                logger,
		m3Client:              m3client,
		ticker:                time.NewTicker(runInterval),
		running:               0,
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

	// Get the local destination and cgs
	localDests, err := r.getAllMultiZoneDestInLocalZone()
	if err != nil {
		r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileFail, 1)
		return
	}
	localCgs, err := r.getAllMultiZoneCgInLocalZone(localDests)
	if err != nil {
		r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileFail, 1)
		return
	}

	// destination/cg metadata reconciliation is only needed if this is a non-authoritative zone
	if r.localZone != r.replicator.getAuthoritativeZone() {
		r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileDestRun, 1)
		authoritativeZoneDests, err := r.getAllMultiZoneDestInAuthoritativeZone()
		if err != nil {
			r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileDestFail, 1)
		} else {
			r.reconcileDest(localDests, authoritativeZoneDests)

			r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileCgRun, 1)
			authoritativeZoneCgs, err := r.getAllMultiZoneCgInAuthoritativeZone(authoritativeZoneDests)
			if err != nil {
				r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileCgFail, 1)
			} else {
				r.reconcileCg(localCgs, authoritativeZoneCgs)
			}
		}
	}

	// reconcile destination extents
	r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileDestExtentRun, 1)
	err = r.reconcileDestExtentMetadata()
	if err != nil {
		r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileDestExtentFail, 1)
	}

	// reconcile cg extents
	r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileCgExtentRun, 1)
	err = r.reconcileCgExtentMetadata(localCgs)
	if err != nil {
		r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileCgExtentFail, 1)
	}

	atomic.StoreInt64(&r.running, 0)
}

func (r *metadataReconciler) reconcileDest(localDests []*shared.DestinationDescription, remoteDests []*shared.DestinationDescription) {
	var replicatorReconcileDestFoundMissingCount int64
	localDestsSet := make(map[string]*shared.DestinationDescription, len(localDests))
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

			if !common.AreDestinationZoneConfigsEqual(localDest.GetZoneConfigs(), remoteDest.GetZoneConfigs()) {
				updateRequest.ZoneConfigs = remoteDest.GetZoneConfigs()
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
			replicatorReconcileDestFoundMissingCount = replicatorReconcileDestFoundMissingCount + 1

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

	// case 4: destination missing in remote but exists locally.
	// We don't need to handle this because deleted destination will still be in the uuid table for 30 days, so it should be covered by case #1

	r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileDestFoundMissing, replicatorReconcileDestFoundMissingCount)
}

func (r *metadataReconciler) reconcileCg(localCgs []*shared.ConsumerGroupDescription, remoteCgs []*shared.ConsumerGroupDescription) {
	var replicatorReconcileCgFoundMissingCount int64
	var replicatorReconcileCgFoundUpdatedCount int64
	localCgsSet := make(map[string]*shared.ConsumerGroupDescription, len(localCgs))
	for _, cg := range localCgs {
		localCgsSet[cg.GetConsumerGroupUUID()] = cg
	}

	for _, remoteCg := range remoteCgs {
		lclLg := r.logger.WithFields(bark.Fields{
			common.TagCnsm: common.FmtCnsm(remoteCg.GetConsumerGroupUUID()),
			common.TagDst:  common.FmtDst(remoteCg.GetDestinationUUID()),
		})
		localCg, ok := localCgsSet[remoteCg.GetConsumerGroupUUID()]
		if ok {
			if remoteCg.GetStatus() == shared.ConsumerGroupStatus_DELETING ||
				remoteCg.GetStatus() == shared.ConsumerGroupStatus_DELETED {
				// case #1: cg gets deleted in remote, but not deleted in local. Delete the cg locally
				if !(localCg.GetStatus() == shared.ConsumerGroupStatus_DELETING ||
					localCg.GetStatus() == shared.ConsumerGroupStatus_DELETED) {
					lclLg.Info(`Found deleted cg from remote but not deleted locally`)
					deleteRequest := &shared.DeleteConsumerGroupRequest{
						DestinationUUID:   common.StringPtr(remoteCg.GetDestinationUUID()),
						ConsumerGroupName: common.StringPtr(remoteCg.GetConsumerGroupName()),
					}
					ctx, cancel := thrift.NewContext(localReplicatorCallTimeOut)
					defer cancel()
					err := r.replicator.DeleteConsumerGroup(ctx, deleteRequest)
					if err != nil {
						lclLg.Error(`Failed to delete ConsumerGroup in local zone for reconciliation`)
						continue
					}
				} else {
					lclLg.Info(`Found ConsumerGroup is deleted in both remote and local`)
					continue
				}
				continue
			}

			// case #2: cg exists in both remote and local, try to compare and update cg
			r.compareAndUpdateCg(remoteCg, localCg, lclLg, &replicatorReconcileCgFoundUpdatedCount)
		} else {
			// case #3: cg exists in remote, but not in local. Create the cg locally
			lclLg.Warn(`Found missing ConsumerGroup from remote!`)
			replicatorReconcileCgFoundMissingCount = replicatorReconcileCgFoundMissingCount + 1

			// If the missing ConsumerGroup is in deleted status, we don't need to create the ConsumerGroup locally
			if remoteCg.GetStatus() == shared.ConsumerGroupStatus_DELETING ||
				remoteCg.GetStatus() == shared.ConsumerGroupStatus_DELETED {
				lclLg.Info(`Found missing ConsumerGroup from remote but in deleted state`)
				continue
			}

			destDesc, err := r.readDestinationInAuthoritativeZone(remoteCg.GetDestinationUUID())
			if err != nil {
				lclLg.WithFields(bark.Fields{
					common.TagErr: err,
				}).Error(`Failed to create ConsumerGroup in local zone because read destination failed in remote zone`)
				continue
			}

			createRequest := &shared.CreateConsumerGroupUUIDRequest{
				Request: &shared.CreateConsumerGroupRequest{
					DestinationPath:          common.StringPtr(destDesc.GetPath()),
					ConsumerGroupName:        common.StringPtr(remoteCg.GetConsumerGroupName()),
					StartFrom:                common.Int64Ptr(remoteCg.GetStartFrom()),
					LockTimeoutSeconds:       common.Int32Ptr(remoteCg.GetLockTimeoutSeconds()),
					MaxDeliveryCount:         common.Int32Ptr(remoteCg.GetMaxDeliveryCount()),
					SkipOlderMessagesSeconds: common.Int32Ptr(remoteCg.GetSkipOlderMessagesSeconds()),
					DelaySeconds:             common.Int32Ptr(remoteCg.GetDelaySeconds()),
					OwnerEmail:               common.StringPtr(remoteCg.GetOwnerEmail()),
					IsMultiZone:              common.BoolPtr(remoteCg.GetIsMultiZone()),
					ActiveZone:               common.StringPtr(remoteCg.GetActiveZone()),
					ZoneConfigs:              remoteCg.GetZoneConfigs(),
					Options:                  remoteCg.Options,
				},
				ConsumerGroupUUID: common.StringPtr(remoteCg.GetConsumerGroupUUID()),
			}

			ctx, cancel := thrift.NewContext(localReplicatorCallTimeOut)
			defer cancel()
			_, err = r.replicator.CreateConsumerGroupUUID(ctx, createRequest)
			if err != nil {
				r.logger.WithFields(bark.Fields{
					common.TagErr: err,
				}).Error(`Failed to create ConsumerGroup in local zone for reconciliation`)
				continue
			}
			continue
		}
	}

	// case 4: cg missing in remote but exists locally.
	// We don't need to handle this because deleted cg will still be in the uuid table for 30 days, so it should be covered by case #1

	r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileCgFoundMissing, replicatorReconcileCgFoundMissingCount)
	r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileCgFoundUpdated, replicatorReconcileCgFoundUpdatedCount)
}

func (r *metadataReconciler) compareAndUpdateCg(remoteCg *shared.ConsumerGroupDescription, localCg *shared.ConsumerGroupDescription, logger bark.Logger, replicatorReconcileCgFoundUpdatedCount *int64) {
	updateRequest := &shared.UpdateConsumerGroupRequest{}
	cgUpdated := false

	if localCg.GetLockTimeoutSeconds() != remoteCg.GetLockTimeoutSeconds() {
		updateRequest.LockTimeoutSeconds = common.Int32Ptr(remoteCg.GetLockTimeoutSeconds())
		cgUpdated = true
	}
	if localCg.GetMaxDeliveryCount() != remoteCg.GetMaxDeliveryCount() {
		updateRequest.MaxDeliveryCount = common.Int32Ptr(remoteCg.GetMaxDeliveryCount())
		cgUpdated = true
	}
	if localCg.GetSkipOlderMessagesSeconds() != remoteCg.GetSkipOlderMessagesSeconds() {
		updateRequest.SkipOlderMessagesSeconds = common.Int32Ptr(remoteCg.GetSkipOlderMessagesSeconds())
		cgUpdated = true
	}
	if localCg.GetDelaySeconds() != remoteCg.GetDelaySeconds() {
		updateRequest.DelaySeconds = common.Int32Ptr(remoteCg.GetDelaySeconds())
		cgUpdated = true
	}
	if localCg.GetStatus() != remoteCg.GetStatus() {
		updateRequest.Status = common.InternalConsumerGroupStatusPtr(remoteCg.GetStatus())
		cgUpdated = true
	}
	if localCg.GetOwnerEmail() != remoteCg.GetOwnerEmail() {
		updateRequest.OwnerEmail = common.StringPtr(remoteCg.GetOwnerEmail())
		cgUpdated = true
	}
	if localCg.GetActiveZone() != remoteCg.GetActiveZone() {
		updateRequest.ActiveZone = common.StringPtr(remoteCg.GetActiveZone())
		cgUpdated = true
	}
	if !common.AreCgZoneConfigsEqual(localCg.GetZoneConfigs(), remoteCg.GetZoneConfigs()) {
		updateRequest.ZoneConfigs = remoteCg.GetZoneConfigs()
		cgUpdated = true
	}
	if !reflect.DeepEqual(localCg.GetOptions(), remoteCg.GetOptions()) {
		updateRequest.Options = remoteCg.GetOptions()
		cgUpdated = true
	}

	if cgUpdated {
		logger.Info(`Found cg gets updated in remote but not in local`)
		*replicatorReconcileCgFoundUpdatedCount = *replicatorReconcileCgFoundUpdatedCount + 1

		destDesc, err := r.readDestinationInAuthoritativeZone(remoteCg.GetDestinationUUID())
		if err != nil {
			logger.WithFields(bark.Fields{
				common.TagErr: err,
			}).Error(`Failed to update ConsumerGroup in local zone because read destination failed in remote zone`)
			return
		}
		updateRequest.DestinationPath = common.StringPtr(destDesc.GetPath())
		updateRequest.ConsumerGroupName = common.StringPtr(remoteCg.GetConsumerGroupName())

		ctx, cancel := thrift.NewContext(localReplicatorCallTimeOut)
		defer cancel()
		_, err = r.replicator.UpdateConsumerGroup(ctx, updateRequest)
		if err != nil {
			logger.WithField(common.TagErr, err).Error(`Failed to update cg in local zone for reconciliation`)
		}
	}
}

func (r *metadataReconciler) readDestinationInAuthoritativeZone(destUUID string) (*shared.DestinationDescription, error) {
	var err error
	authoritativeZone := r.replicator.getAuthoritativeZone()
	remoteReplicator, err := r.replicator.replicatorclientFactory.GetReplicatorClient(authoritativeZone)
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagErr:      err,
			common.TagZoneName: common.FmtZoneName(authoritativeZone),
		}).Error(`Failed to get remote replicator client`)
		return nil, err
	}

	readReq := &shared.ReadDestinationRequest{
		DestinationUUID: common.StringPtr(destUUID),
	}

	ctx, cancel := thrift.NewContext(remoteReplicatorCallTimeOut)
	defer cancel()
	res, err := remoteReplicator.ReadDestination(ctx, readReq)
	if err != nil {
		r.logger.WithField(common.TagErr, err).Error(`Remote replicator call ReadDestination failed`)
		return nil, err
	}

	return res, nil
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
	remoteReplicator, err := r.replicator.replicatorclientFactory.GetReplicatorClient(authoritativeZone)
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

func (r *metadataReconciler) getAllMultiZoneCgInLocalZone(dests []*shared.DestinationDescription) ([]*shared.ConsumerGroupDescription, error) {
	var cgs []*shared.ConsumerGroupDescription
	for _, dest := range dests {
		listCgReq := &shared.ListConsumerGroupRequest{
			DestinationUUID: common.StringPtr(dest.GetDestinationUUID()),
			Limit:           common.Int64Ptr(metadataListRequestPageSize),
		}

		for {
			cgRes, err := r.mClient.ListConsumerGroups(nil, listCgReq)
			if err != nil {
				r.logger.WithField(common.TagErr, err).Error(`Metadata call ListConsumerGroups failed`)
				return nil, err
			}

			for _, cg := range cgRes.GetConsumerGroups() {
				if cg.GetIsMultiZone() {
					cgs = append(cgs, cg)
				}
			}

			if len(cgRes.GetNextPageToken()) == 0 {
				break
			}

			listCgReq.PageToken = cgRes.GetNextPageToken()
		}
	}

	return cgs, nil
}

func (r *metadataReconciler) getAllMultiZoneCgInAuthoritativeZone(dests []*shared.DestinationDescription) ([]*shared.ConsumerGroupDescription, error) {
	var err error
	authoritativeZone := r.replicator.getAuthoritativeZone()
	remoteReplicator, err := r.replicator.replicatorclientFactory.GetReplicatorClient(authoritativeZone)
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagErr:      err,
			common.TagZoneName: common.FmtZoneName(authoritativeZone),
		}).Error(`Failed to get remote replicator client`)
		return nil, err
	}

	var cgs []*shared.ConsumerGroupDescription
	for _, dest := range dests {
		listCgReq := &shared.ListConsumerGroupRequest{
			DestinationUUID: common.StringPtr(dest.GetDestinationUUID()),
			Limit:           common.Int64Ptr(metadataListRequestPageSize),
		}

		for {
			ctx, cancel := thrift.NewContext(remoteReplicatorCallTimeOut)
			defer cancel()
			cgRes, err := remoteReplicator.ListConsumerGroups(ctx, listCgReq)
			if err != nil {
				r.logger.WithField(common.TagErr, err).Error(`Remote replicator call ListConsumerGroups failed`)
				return nil, err
			}

			for _, cg := range cgRes.GetConsumerGroups() {
				if cg.GetIsMultiZone() {
					cgs = append(cgs, cg)
				}
			}

			if len(cgRes.GetNextPageToken()) == 0 {
				break
			}

			listCgReq.PageToken = cgRes.GetNextPageToken()
		}
	}

	return cgs, nil
}

func (r *metadataReconciler) reconcileDestExtentMetadata() error {
	dests, err := r.getAllMultiZoneDestInLocalZone()
	if err != nil {
		return err
	}

	for _, dest := range dests {
		localExtentsPerZone, errCur := r.getAllDestExtentInCurrentZone(dest.GetDestinationUUID())
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

				// only need to reconcile for extents that are originated from the remote zone
				localExtents := localExtentsPerZone[zoneConfig.GetZone()]
				r.reconcileDestExtent(dest.GetDestinationUUID(), localExtents, remoteExtents, zoneConfig.GetZone())
			}
		}
	}
	return nil
}

func (r *metadataReconciler) reconcileCgExtentMetadata(localCgs []*shared.ConsumerGroupDescription) error {
	for _, cg := range localCgs {
		localExtents, err := r.getAllCgExtentInCurrentZone(cg.GetDestinationUUID(), cg.GetConsumerGroupUUID())
		if err != nil {
			continue
		}
		for _, zoneConfig := range cg.GetZoneConfigs() {
			// skip local zone
			if strings.EqualFold(zoneConfig.GetZone(), r.localZone) {
				continue
			}

			if zoneConfig.GetVisible() {
				remoteExtents, err := r.getAllCgExtentInRemoteZone(zoneConfig.GetZone(), cg.GetDestinationUUID(), cg.GetConsumerGroupUUID())
				if err != nil {
					continue
				}

				r.reconcileCgExtent(cg.GetDestinationUUID(), cg.GetConsumerGroupUUID(), localExtents, remoteExtents, zoneConfig.GetZone())
			}
		}
	}
	return nil
}

func (r *metadataReconciler) getAllDestExtentInRemoteZone(zone string, destUUID string) (map[string]*shared.ExtentStats, error) {
	var err error
	remoteReplicator, err := r.replicator.replicatorclientFactory.GetReplicatorClient(zone)
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

	extents := make(map[string]*shared.ExtentStats)
	for {
		ctx, cancel := thrift.NewContext(remoteReplicatorCallTimeOut)
		defer cancel()
		res, err := remoteReplicator.ListExtentsStats(ctx, listReq)
		if err != nil {
			r.logger.WithField(common.TagErr, err).Error(`Remote replicator call ListExtentsStats failed`)
			return nil, err
		}

		for _, ext := range res.GetExtentStatsList() {
			extents[ext.GetExtent().GetExtentUUID()] = ext
		}

		if len(res.GetNextPageToken()) == 0 {
			break
		}

		listReq.PageToken = res.GetNextPageToken()
	}

	return extents, nil
}

func (r *metadataReconciler) getAllDestExtentInCurrentZone(destUUID string) (map[string]map[string]*shared.ExtentStats, error) {
	listReq := &shared.ListExtentsStatsRequest{
		DestinationUUID:  common.StringPtr(destUUID),
		LocalExtentsOnly: common.BoolPtr(false),
		Limit:            common.Int64Ptr(metadataListRequestPageSize),
	}

	perZoneExtents := make(map[string]map[string]*shared.ExtentStats)
	for {
		res, err := r.mClient.ListExtentsStats(nil, listReq)
		if err != nil {
			r.logger.WithField(common.TagErr, err).Error(`Metadata call ListExtentsStats failed`)
			return nil, err
		}

		for _, ext := range res.GetExtentStatsList() {
			zone := ext.GetExtent().GetOriginZone()
			if _, ok := perZoneExtents[zone]; !ok {
				perZoneExtents[zone] = make(map[string]*shared.ExtentStats)
			}
			perZoneExtents[zone][ext.GetExtent().GetExtentUUID()] = ext
		}

		if len(res.GetNextPageToken()) == 0 {
			break
		}

		listReq.PageToken = res.GetNextPageToken()
	}
	return perZoneExtents, nil
}

func (r *metadataReconciler) getAllCgExtentInRemoteZone(zone string, destUUID string, cgUUID string) (map[string]*shared.ConsumerGroupExtent, error) {
	lclLg := r.logger.WithFields(bark.Fields{
		common.TagDst:      common.FmtDst(destUUID),
		common.TagCnsm:     common.FmtCnsm(cgUUID),
		common.TagZoneName: common.FmtZoneName(zone),
	})

	var err error
	remoteReplicator, err := r.replicator.replicatorclientFactory.GetReplicatorClient(zone)
	if err != nil {
		lclLg.WithField(common.TagErr, err).Error(`getAllCgExtentInRemoteZone: Failed to get remote replicator client`)
		return nil, err
	}

	listReq := &shared.ReadConsumerGroupExtentsRequest{
		DestinationUUID:   common.StringPtr(destUUID),
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		MaxResults:        common.Int32Ptr(metadataListRequestPageSize),
	}

	res := make(map[string]*shared.ConsumerGroupExtent)
	for {
		ctx, cancel := thrift.NewContext(remoteReplicatorCallTimeOut)
		defer cancel()
		listRes, err := remoteReplicator.ReadConsumerGroupExtents(ctx, listReq)
		if err != nil {
			lclLg.WithField(common.TagErr, err).Error(`getAllCgExtentInRemoteZone: Remote replicator call ReadConsumerGroupExtents failed`)
			return nil, err
		}

		for _, cgExt := range listRes.GetExtents() {
			res[cgExt.GetExtentUUID()] = cgExt
		}

		if len(listRes.GetNextPageToken()) == 0 {
			break
		}

		listReq.PageToken = listRes.GetNextPageToken()
	}
	return res, nil
}

func (r *metadataReconciler) getAllCgExtentInCurrentZone(destUUID string, cgUUID string) (map[string]*shared.ConsumerGroupExtent, error) {
	lclLg := r.logger.WithFields(bark.Fields{
		common.TagDst:  common.FmtDst(destUUID),
		common.TagCnsm: common.FmtCnsm(cgUUID),
	})

	listReq := &shared.ReadConsumerGroupExtentsRequest{
		DestinationUUID:   common.StringPtr(destUUID),
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		MaxResults:        common.Int32Ptr(metadataListRequestPageSize),
	}

	cgExtents := make(map[string]*shared.ConsumerGroupExtent)
	for {
		res, err := r.mClient.ReadConsumerGroupExtents(nil, listReq)
		if err != nil {
			lclLg.WithField(common.TagErr, err).Error(`getAllCgExtentInCurrentZone: Metadata call ReadConsumerGroupExtents failed`)
			return nil, err
		}

		for _, cgExt := range res.GetExtents() {
			cgExtents[cgExt.GetExtentUUID()] = cgExt
		}

		if len(res.GetNextPageToken()) == 0 {
			break
		}

		listReq.PageToken = res.GetNextPageToken()
	}
	return cgExtents, nil
}

func (r *metadataReconciler) reconcileDestExtent(destUUID string, localExtents map[string]*shared.ExtentStats, remoteExtents map[string]*shared.ExtentStats, remoteZone string) {
	var remoteDeletedLocalNotCount int64
	var remoteConsumedLocalMissingCount int64
	var remoteDeletedLocalMissingCount int64
	var foundMissingCount int64
	for remoteExtentUUID, remoteExtentStats := range remoteExtents {
		remoteExtentStatus := remoteExtentStats.GetStatus()
		localExtentStats, ok := localExtents[remoteExtentUUID]
		if !ok {
			r.logger.WithFields(bark.Fields{
				common.TagDst:          common.FmtDst(destUUID),
				common.TagExt:          common.FmtExt(remoteExtentUUID),
				common.TagZoneName:     common.FmtZoneName(remoteZone),
				common.TagExtentStatus: common.FmtExtentStatus(remoteExtentStatus),
			}).Warn(`Found missing extent from remote!`)

			// if the extent is already in consumed/deleted state on remote side, don't bother to create the extent locally because all data is gone on remote side already
			if remoteExtentStatus == shared.ExtentStatus_CONSUMED {
				remoteConsumedLocalMissingCount = remoteConsumedLocalMissingCount + 1
				continue
			}
			if remoteExtentStatus == shared.ExtentStatus_DELETED {
				remoteDeletedLocalMissingCount = remoteDeletedLocalMissingCount + 1
				continue
			}

			createRequest := &shared.CreateExtentRequest{
				Extent: &shared.Extent{
					ExtentUUID:      common.StringPtr(remoteExtentUUID),
					DestinationUUID: common.StringPtr(destUUID),
					InputHostUUID:   common.StringPtr(common.InputHostForRemoteExtent),
					StoreUUIDs:      []string{},
					OriginZone:      common.StringPtr(remoteZone),
				},
			}

			// If the remote extent has cg visibility set (a merged dlq extent), only create the extent if
			// the cg exists in local (i.e. if the cg is a local only cg from remote, we don't need to replicate this extent)
			if remoteExtentStats.IsSetConsumerGroupVisibility() {
				exist, err := r.cgExistInLocal(remoteExtentStats.GetConsumerGroupVisibility())
				if err != nil {
					r.logger.WithFields(bark.Fields{
						common.TagErr:      err,
						common.TagDst:      common.FmtDst(destUUID),
						common.TagCnsm:     common.FmtCnsm(remoteExtentStats.GetConsumerGroupVisibility()),
						common.TagExt:      common.FmtExt(remoteExtentUUID),
						common.TagZoneName: common.FmtZoneName(remoteZone),
					}).Error(`Failed to decide whether cg exist in local for dlq extent`)
					continue
				}
				if !exist {
					r.logger.WithFields(bark.Fields{
						common.TagErr:      err,
						common.TagDst:      common.FmtDst(destUUID),
						common.TagCnsm:     common.FmtCnsm(remoteExtentStats.GetConsumerGroupVisibility()),
						common.TagExt:      common.FmtExt(remoteExtentUUID),
						common.TagZoneName: common.FmtZoneName(remoteZone),
					}).Info(`Found missing dlq extent but cg doesn't exist in local`)
					continue
				}
				createRequest.ConsumerGroupVisibility = common.StringPtr(remoteExtentStats.GetConsumerGroupVisibility())
			}

			foundMissingCount = foundMissingCount + 1

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
			localExtentStatus := localExtentStats.GetStatus()
			if (remoteExtentStatus == shared.ExtentStatus_SEALED || remoteExtentStatus == shared.ExtentStatus_CONSUMED) && localExtentStatus == shared.ExtentStatus_OPEN {
				r.sealExtentInMetadata(destUUID, remoteExtentUUID)
			}
			if remoteExtentStatus == shared.ExtentStatus_DELETED {
				r.handleExtentDeletedOrMissingInRemote(destUUID, remoteExtentUUID, localExtentStatus, &remoteDeletedLocalNotCount)
			}
		}
	}

	// now try to find all the orphaned extents(extent already gone in remote zone but not in local zone), and try to seal them

	// note we're not 100% sure if the extent is really gone even if cassandra doesn't return that extent because of
	// the eventual consistency nature of cassandra
	// so we maintain a timestamp to track how long the extent has been missing
	// we act on it only after it has been missing for a certain period of time

	remoteMissingExtents := make(map[string]struct{})
	for localExtentUUID, localExtentStats := range localExtents {
		// we're going to delete this extent soon locally so no need to act on it
		if localExtentStats.GetStatus() == shared.ExtentStatus_CONSUMED || localExtentStats.GetStatus() == shared.ExtentStatus_DELETED {
			continue
		}
		if _, ok := remoteExtents[localExtentUUID]; !ok {
			// extent exists in local but not in remote
			remoteMissingExtents[localExtentUUID] = struct{}{}
		}
	}

	for suspectExtent, suspectExtentInfo := range r.suspectMissingExtents {
		if suspectExtentInfo.destUUID != destUUID {
			continue
		}
		if _, ok := remoteMissingExtents[suspectExtent]; !ok {
			// re-appeared in remote, remove it from suspect list
			delete(r.suspectMissingExtents, suspectExtent)
		} else {
			if time.Since(suspectExtentInfo.missingSince) > extentMissingDurationThreshold {
				localStats, ok := localExtents[suspectExtent]
				if !ok {
					r.logger.WithFields(bark.Fields{
						common.TagDst: common.FmtDst(suspectExtentInfo.destUUID),
						common.TagExt: common.FmtExt(suspectExtent),
					}).Error(`code bug!! suspect extent should in local extent map!!`)
					continue
				}
				r.handleExtentDeletedOrMissingInRemote(suspectExtentInfo.destUUID, suspectExtent, localStats.GetStatus(), &remoteDeletedLocalNotCount)
			}
		}
	}

	// now add new suspect if there's any
	for missingExtent := range remoteMissingExtents {
		if _, ok := r.suspectMissingExtents[missingExtent]; !ok {
			r.suspectMissingExtents[missingExtent] = missingExtentInfo{
				destUUID:     destUUID,
				missingSince: time.Now(),
			}
		}
	}

	r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileDestExtentFoundMissing, foundMissingCount)
	r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileDestExtentRemoteConsumedLocalMissing, remoteConsumedLocalMissingCount)
	r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileDestExtentRemoteDeletedLocalMissing, remoteDeletedLocalMissingCount)
	r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileDestExtentRemoteDeletedLocalNot, remoteDeletedLocalNotCount)
	r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileDestExtentSuspectMissingExtents, int64(len(r.suspectMissingExtents)))
}

func (r *metadataReconciler) reconcileCgExtent(destUUID string, cgUUID string, localExtents map[string]*shared.ConsumerGroupExtent, remoteExtents map[string]*shared.ConsumerGroupExtent, remoteZone string) {
	lclLg := r.logger.WithFields(bark.Fields{
		common.TagDst:      common.FmtDst(destUUID),
		common.TagCnsm:     common.FmtCnsm(cgUUID),
		common.TagZoneName: common.FmtZoneName(remoteZone),
	})

	var remoteConsumedLocalMissingCount int64
	var remoteDeletedLocalMissingCount int64
	var foundMissingCount int64

	for remoteExtentUUID, remoteExtent := range remoteExtents {
		remoteExtentStatus := remoteExtent.GetStatus()
		localExtent, ok := localExtents[remoteExtentUUID]
		if !ok {
			lclLg.WithFields(bark.Fields{
				common.TagExt:            common.FmtExt(remoteExtentUUID),
				common.TagCGExtentStatus: common.FmtCGExtentStatus(remoteExtentStatus),
			}).Warn(`reconcileCgExtent: Found missing cg extent from remote!`)

			// if the extent is already in consumed/deleted state on remote side, don't bother to create the extent locally
			if remoteExtentStatus == shared.ConsumerGroupExtentStatus_CONSUMED {
				remoteConsumedLocalMissingCount = remoteConsumedLocalMissingCount + 1
				continue
			}
			if remoteExtentStatus == shared.ConsumerGroupExtentStatus_DELETED {
				remoteDeletedLocalMissingCount = remoteDeletedLocalMissingCount + 1
				continue
			}

			foundMissingCount = foundMissingCount + 1

			createRequest := &shared.CreateConsumerGroupExtentRequest{
				ExtentUUID:        common.StringPtr(remoteExtentUUID),
				DestinationUUID:   common.StringPtr(destUUID),
				ConsumerGroupUUID: common.StringPtr(cgUUID),
			}
			ctx, cancel := thrift.NewContext(localReplicatorCallTimeOut)
			defer cancel()
			err := r.replicator.CreateConsumerGroupExtent(ctx, createRequest)
			if err != nil {
				lclLg.WithFields(bark.Fields{
					common.TagErr: err,
					common.TagExt: common.FmtExt(remoteExtentUUID),
				}).Error(`reconcileCgExtent: Failed to create cg extent in local zone for reconciliation`)
				continue
			}
		} else {
			localExtentStatus := localExtent.GetStatus()
			if localExtentStatus != remoteExtentStatus {
				// update the status of local cg extent, if the local cg extent status is "behind" remote
				if localExtentStatus == shared.ConsumerGroupExtentStatus_OPEN ||
					(localExtentStatus == shared.ConsumerGroupExtentStatus_CONSUMED && remoteExtentStatus == shared.ConsumerGroupExtentStatus_DELETED) {
					err := r.mClient.UpdateConsumerGroupExtentStatus(nil, &shared.UpdateConsumerGroupExtentStatusRequest{
						ConsumerGroupUUID: common.StringPtr(cgUUID),
						ExtentUUID:        common.StringPtr(remoteExtentUUID),
						Status:            common.CheramiConsumerGroupExtentStatusPtr(remoteExtentStatus),
					})

					if err != nil {
						lclLg.WithFields(bark.Fields{
							common.TagErr:            err,
							common.TagExt:            common.FmtExt(remoteExtentUUID),
							common.TagCGExtentStatus: common.FmtCGExtentStatus(remoteExtentStatus),
						}).Error(`reconcileCgExtent: Failed to update cg extent status`)
						continue
					}
				}
			} else if localExtent.GetAckLevelOffset() < remoteExtent.GetAckLevelOffset() {
				err := r.mClient.SetAckOffset(nil, &shared.SetAckOffsetRequest{
					ExtentUUID:        common.StringPtr(remoteExtentUUID),
					ConsumerGroupUUID: common.StringPtr(cgUUID),
					Status:            common.CheramiConsumerGroupExtentStatusPtr(localExtentStatus),
					AckLevelAddress:   common.Int64Ptr(remoteExtent.GetAckLevelOffset()),
					AckLevelSeqNo:     common.Int64Ptr(remoteExtent.GetAckLevelSeqNo()),
					ReadLevelAddress:  common.Int64Ptr(remoteExtent.GetReadLevelOffset()),
					ReadLevelSeqNo:    common.Int64Ptr(remoteExtent.GetReadLevelSeqNo()),
				})
				if err != nil {
					lclLg.WithFields(bark.Fields{
						common.TagErr: err,
						common.TagExt: common.FmtExt(remoteExtentUUID),
					}).Error(`reconcileCgExtent: Failed to update ack/read level offset`)
					continue
				}
			}
		}
	}

	r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileCgExtentFoundMissing, foundMissingCount)
	r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileCgExtentRemoteConsumedLocalMissing, remoteConsumedLocalMissingCount)
	r.m3Client.UpdateGauge(metrics.ReplicatorReconcileScope, metrics.ReplicatorReconcileCgExtentRemoteDeletedLocalMissing, remoteDeletedLocalMissingCount)
}

func (r *metadataReconciler) handleExtentDeletedOrMissingInRemote(destUUID string, extentUUID string, localStatus shared.ExtentStatus, destExtentRemoteDeletedLocalNotCount *int64) {
	// lifecycle for extent in local zone(origin of the extent is remote):
	// open->sealed: after extent becomes sealed in remote(origin) zone
	// sealed->consumed: decided by local retention manager
	// consumed->deleted: decided by local retention manager

	// we're going to delete this extent soon locally so no need to act on it
	if localStatus == shared.ExtentStatus_CONSUMED || localStatus == shared.ExtentStatus_DELETED {
		return
	}

	*destExtentRemoteDeletedLocalNotCount = *destExtentRemoteDeletedLocalNotCount + 1

	// if extent is still open in metadata, seal the extent in metadata
	if localStatus == shared.ExtentStatus_OPEN {
		r.sealExtentInMetadata(destUUID, extentUUID)
	}

	// also seal the extent in store as there's no message available on remote side at this point
	// this is not absolutely needed in most cases as local store should have already been sealed after reading the seal marker.
	// However in some corner cases(or software bugs), store may not see the seal marker before extent is deleted on remote side.
	// If that happens, this call becomes necessary to move the extent to consumed state(extent won't
	// be moved to consumed state if it's not sealed on store)
	err := r.sealExtentInStore(destUUID, extentUUID)
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagErr: err,
			common.TagDst: common.FmtDst(destUUID),
			common.TagExt: common.FmtExt(extentUUID),
		}).Error(`Failed to seal extent in store`)
	} else {
		r.logger.WithFields(bark.Fields{
			common.TagDst: common.FmtDst(destUUID),
			common.TagExt: common.FmtExt(extentUUID),
		}).Info(`Extent sealed in store`)
	}
}

func (r *metadataReconciler) cgExistInLocal(cgUUID string) (bool, error) {
	_, err := r.mClient.ReadConsumerGroupByUUID(nil, &shared.ReadConsumerGroupRequest{
		ConsumerGroupUUID: common.StringPtr(cgUUID),
	})
	if err != nil {
		if _, ok := err.(*shared.EntityNotExistsError); ok {
			return false, nil
		}
		r.logger.WithFields(bark.Fields{
			common.TagErr:  err,
			common.TagCnsm: common.FmtCnsm(cgUUID),
		}).Error(`cgExistInLocal: failed to call ReadConsumerGroupByUUID`)
		return false, err
	}

	return true, nil
}

func (r *metadataReconciler) sealExtentInMetadata(destUUID string, extentUUID string) {
	updateRequest := &metadata.UpdateExtentStatsRequest{
		ExtentUUID:      common.StringPtr(extentUUID),
		DestinationUUID: common.StringPtr(destUUID),
		Status:          common.MetadataExtentStatusPtr(shared.ExtentStatus_SEALED),
	}
	_, err := r.mClient.UpdateExtentStats(nil, updateRequest)
	if err != nil {
		r.logger.WithFields(bark.Fields{
			common.TagErr: err,
			common.TagDst: common.FmtDst(destUUID),
			common.TagExt: common.FmtExt(extentUUID),
		}).Error(`Failed to seal extent in metadata`)
	} else {
		r.logger.WithFields(bark.Fields{
			common.TagDst: common.FmtDst(destUUID),
			common.TagExt: common.FmtExt(extentUUID),
		}).Info(`Extent sealed in metadata`)
	}
}

func (r *metadataReconciler) sealExtentInStore(destUUID string, extentUUID string) error {
	readExtentRequest := &metadata.ReadExtentStatsRequest{
		DestinationUUID: common.StringPtr(destUUID),
		ExtentUUID:      common.StringPtr(extentUUID),
	}

	lclLg := r.logger.WithFields(bark.Fields{
		common.TagDst: common.FmtDst(destUUID),
		common.TagExt: common.FmtExt(extentUUID),
	})

	readExtentResult, err := r.mClient.ReadExtentStats(nil, readExtentRequest)
	if err != nil {
		lclLg.WithField(common.TagErr, err).Error(`Metadata call ReadExtentStats failed`)
		return err
	}

	sealReq := &store.SealExtentRequest{
		ExtentUUID: common.StringPtr(extentUUID),
	}

	var errorOccured bool
	for _, store := range readExtentResult.GetExtentStats().GetExtent().GetStoreUUIDs() {
		storeClient, _, err := r.replicator.GetClientFactory().GetThriftStoreClientUUID(store, destUUID)
		if err != nil {
			lclLg.WithFields(bark.Fields{
				common.TagErr:  err,
				common.TagStor: common.FmtStor(store),
			}).Error(`Failed to get store client`)
			errorOccured = true
			continue
		}

		ctx, cancel := thrift.NewContext(storeCallTimeout)
		defer cancel()
		err = storeClient.SealExtent(ctx, sealReq)
		if err != nil {
			lclLg.WithFields(bark.Fields{
				common.TagErr:  err,
				common.TagStor: common.FmtStor(store),
			}).Error(`Failed to seal extent on store`)
			errorOccured = true
			continue
		}
	}

	if errorOccured {
		return errors.New(`seal extent on store failed for at least one replica`)
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
