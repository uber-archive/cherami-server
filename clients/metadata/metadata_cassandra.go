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
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/cache"
	"github.com/uber/cherami-server/common/configure"
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"

	"github.com/gocql/gocql"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"
)

// Currently a single global directory is used for all destinations
const directoryUUID string = "CC3B477C-E6F2-4465-9A98-7FE71B68CD1F"

var uuidRegex, _ = regexp.Compile(`^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$`)

const (
	defaultSessionTimeout         = 30 * time.Second
	defaultDLQConsumedRetention   = 7 * 24 * 3600 // One Week
	defaultDLQUnconsumedRetention = 7 * 24 * 3600 // One Week
)

const (
	opsCreate = "create"
	opsDelete = "delete"
	opsUpdate = "update"
)

const (
	entityTypeDst = "destinaton"
	entityTypeCG  = "consumer_group"
)

const (
	tableConsumerGroupExtents   = "consumer_group_extents"
	tableConsumerGroups         = "consumer_groups"
	tableConsumerGroupsByName   = "consumer_groups_by_name"
	tableDestinationExtents     = "destination_extents"
	tableDestinations           = "destinations"
	tableDestinationsByPath     = "destinations_by_path"
	tableHostAddrToUUID         = "host_addr_to_uuid"
	tableInputHostExtents       = "input_host_extents"
	tableOperationsByEntityName = "user_operations_by_entity_name"
	tableOperationsByEntityUUID = "user_operations_by_entity_uuid"
	tableStoreExtents           = "store_extents"
	tableUUIDToHostAddr         = "uuid_to_host_addr"
)

const (
	columnAckLevelOffset                 = "ack_level_offset"
	columnAckLevelSequence               = "ack_level_sequence"
	columnAckLevelSequenceRate           = "ack_level_sequence_rate"
	columnActiveZone                     = "active_zone"
	columnAllowConsume                   = "allow_consume"
	columnAllowPublish                   = "allow_publish"
	columnAlwaysReplicatedTo             = "always_replicate_to"
	columnArchivalLocation               = "archival_location"
	columnAvailableAddress               = "available_address"
	columnAvailableEnqueueTime           = "available_enqueue_time"
	columnAvailableSequence              = "available_sequence"
	columnAvailableSequenceRate          = "available_sequence_rate"
	columnBeginAddress                   = "begin_address"
	columnBeginEnqueueTime               = "begin_enqueue_time"
	columnBeginSequence                  = "begin_sequence"
	columnBeginTime                      = "begin_time"
	columnCallerHostName                 = "caller_host_name"
	columnCallerServiceName              = "caller_service_name"
	columnChecksumOption                 = "checksum_option"
	columnConnectedStore                 = "connected_store"
	columnConsumedMessagesRetention      = "consumed_messages_retention"
	columnConsumerGroup                  = "consumer_group"
	columnConsumerGroupUUID              = "consumer_group_uuid"
	columnConsumerGroupVisibility        = "consumer_group_visibility"
	columnCreatedTime                    = "created_time"
	columnDelaySeconds                   = "delay_seconds"
	columnDLQConsumerGroup               = "dlq_consumer_group"
	columnDLQMergeBefore                 = "dlq_merge_before"
	columnDLQPurgeBefore                 = "dlq_purge_before"
	columnDeadLetterQueueDestinationUUID = "dead_letter_queue_destination_uuid"
	columnDestination                    = "destination"
	columnDestinationUUID                = "destination_uuid"
	columnDirectoryUUID                  = "directory_uuid"
	columnEndTime                        = "end_time"
	columnEntityName                     = "entity_name"
	columnEntityType                     = "entity_type"
	columnEntityUUID                     = "entity_uuid"
	columnExtent                         = "extent"
	columnExtentUUID                     = "extent_uuid"
	columnHostAddr                       = "hostaddr"
	columnHostName                       = "hostname"
	columnInitiatorInfo                  = "initiator"
	columnInputHostUUID                  = "input_host_uuid"
	columnIsMultiZone                    = "is_multi_zone"
	columnKafkaCluster                   = "kafka_cluster"
	columnKafkaTopics                    = "kafka_topics"
	columnLastAddress                    = "last_address"
	columnLastEnqueueTime                = "last_enqueue_time"
	columnLastSequence                   = "last_sequence"
	columnLastSequenceRate               = "last_sequence_rate"
	columnLockTimeoutSeconds             = "lock_timeout_seconds"
	columnMaxDeliveryCount               = "max_delivery_count"
	columnName                           = "name"
	columnOpsContent                     = "operation_content"
	columnOpsTime                        = "operation_time"
	columnOpsType                        = "operation_type"
	columnOriginZone                     = "origin_zone"
	columnOutputHostUUID                 = "output_host_uuid"
	columnOwnerEmail                     = "owner_email"
	columnPath                           = "path"
	columnReceivedLevelOffset            = "received_level_offset"
	columnReceivedLevelSequence          = "received_level_sequence"
	columnReceivedLevelSequenceRate      = "received_level_sequence_rate"
	columnRemoteExtentPrimaryStore       = "remote_extent_primary_store"
	columnRemoteExtentReplicaNum         = "remote_extent_replica_num"
	columnReplicaStats                   = "replica_stats"
	columnReplicationStatus              = "replication_status"
	columnSizeInBytes                    = "size_in_bytes"
	columnSizeInBytesRate                = "size_in_bytes_rate"
	columnSkipOlderMessagesSeconds       = "skip_older_messages_seconds"
	columnStartFrom                      = "start_from"
	columnStatus                         = "status"
	columnStatusUpdatedTime              = "status_updated_time"
	columnStore                          = "store"
	columnStoreUUID                      = "store_uuid"
	columnStoreUUIDS                     = "store_uuids"
	columnStoreVersion                   = "store_version"
	columnType                           = "type"
	columnUUID                           = "uuid"
	columnUnconsumedMessagesRetention    = "unconsumed_messages_retention"
	columnUserEmail                      = "user_email"
	columnUserName                       = "user_name"
	columnVisible                        = "visible"
	columnZone                           = "zone"
	columnZoneConfigs                    = "zone_configs"
	columnOptions                        = "options"
)

const userOperationTTL = "2592000" // 30 days

const cassandraProtoVersion = 4

// Large TTLs for extents will in turn make the
// listExtents() API expensive. Keep the extent
// around just for a day after its deleted for
// visibility
const deleteExtentTTLSeconds = int64(time.Hour*24) / int64(time.Second)
const defaultDeleteTTLSeconds = int64(time.Hour*24*30) / int64(time.Second)

const destinationCacheSize = 1048576
const consumerGroupCacheSize = 1048576
const cacheTTL = time.Second

// CassandraMetadataService Implements TChanMetadataServiceClient interface
// TODO: Convert all errors to the ones defined in the thrift API.
type CassandraMetadataService struct {
	session       *gocql.Session
	lowConsLevel  gocql.Consistency
	midConsLevel  gocql.Consistency
	highConsLevel gocql.Consistency // Strongest cons level that can be used for this session
	clusterName   string
	log           bark.Logger

	destinationCache   cache.Cache
	consumerGroupCache cache.Cache
}

// interface implementation check
var _ m.TChanMetadataService = (*CassandraMetadataService)(nil)

func parseConsistency(cfgCons string) (lowCons gocql.Consistency, midCons gocql.Consistency, highCons gocql.Consistency) {

	// use a minimum default consistency of 'One'
	lowCons, midCons, highCons = gocql.One, gocql.One, gocql.One

	switch cons := strings.Split(cfgCons, ","); len(cons) {
	case 3:
		lowCons = gocql.ParseConsistency(strings.TrimSpace(cons[2]))
		fallthrough

	case 2:
		midCons = gocql.ParseConsistency(strings.TrimSpace(cons[1]))
		if len(cons) == 2 {
			lowCons = midCons
		}
		fallthrough

	case 1:
		highCons = gocql.ParseConsistency(strings.TrimSpace(cons[0]))
	}

	return
}

// NewCassandraMetadataService creates an instance of TChanMetadataServiceClient backed up by Cassandra.
func NewCassandraMetadataService(cfg configure.CommonMetadataConfig, log bark.Logger) (*CassandraMetadataService, error) {

	if log == nil {
		log = bark.NewLoggerFromLogrus(logrus.StandardLogger())
	}

	lowCons, midCons, highCons := parseConsistency(cfg.GetConsistency())

	if highCons == gocql.One && configure.NewCommonConfigure().GetEnvironment() == configure.EnvProduction {
		log.Panic("Highest consistency level of ONE should only be used in TestEnvironment")
	}

	clusterName := cfg.GetClusterName()
	if len(clusterName) < 1 {
		clusterName = "unknown"
	}

	cluster := newCluster(cfg.GetCassandraHosts())
	cluster.Port = cfg.GetPort()
	cluster.Keyspace = cfg.GetKeyspace()
	cluster.ProtoVersion = cassandraProtoVersion

	if auth := cfg.GetAuthentication(); auth.Enabled {

		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: auth.Username,
			Password: auth.Password,
		}
	}

	// Our clusters usually don't span across
	// multiple DCs. If they do and the data lives
	// in only one DC, the dc filter allows for
	// limiting the host discovery to just the
	// local DC. That way, all of your queries
	// will be local.
	if filter, ok := cfg.GetDcFilter()[clusterName]; ok {
		cluster.HostFilter = gocql.DataCentreHostFilter(strings.TrimSpace(filter))
	}

	numConns := cfg.GetNumConns()
	if numConns > 0 {
		cluster.NumConns = numConns
	}

	// Highest consistency level applies to all sessions/queries derived from this cluster
	// When lower consitency level is preferred, the APIs do a query level override
	cluster.Consistency = highCons
	cluster.Timeout = defaultSessionTimeout
	session, err := cluster.CreateSession()
	if err != nil {
		log.WithField(common.TagErr, err).Error("NewCassandraMetadataService failed")
		return nil, fmt.Errorf("createSession: %v", err)
	}

	return &CassandraMetadataService{
		session:       session,
		lowConsLevel:  lowCons,
		midConsLevel:  midCons,
		highConsLevel: highCons,
		clusterName:   clusterName,
		log:           log.WithField(common.TagModule, `metadata`),

		destinationCache:   cache.New(destinationCacheSize, &cache.Options{TTL: cacheTTL}),
		consumerGroupCache: cache.New(consumerGroupCacheSize, &cache.Options{TTL: cacheTTL}),
	}, nil
}

// GetSession returns the underlying cassandra sesion object
// This method is only intended for unit test
func (s *CassandraMetadataService) GetSession() *gocql.Session {
	return s.session
}

const (
	sqlUserInfoType = `{` +
		columnUserName + `: ?, ` +
		columnUserEmail + `: ?}`

	sqlRecordUserOperationByEntityName = `INSERT INTO ` + tableOperationsByEntityName +
		`(` + columnEntityName + `, ` + columnEntityUUID + `, ` + columnEntityType + `, ` + columnInitiatorInfo +
		`, ` + columnCallerServiceName + `, ` + columnCallerHostName + `, ` + columnOpsType + `, ` + columnOpsTime + `, ` + columnOpsContent + `) ` +
		` VALUES (?, ?, ?, ` + sqlUserInfoType + `, ?, ?, ?, ?, ?) USING TTL ` + userOperationTTL

	sqlRecordUserOperationByEntityUUID = `INSERT INTO ` + tableOperationsByEntityUUID +
		`(` + columnEntityName + `, ` + columnEntityUUID + `, ` + columnEntityType + `, ` + columnInitiatorInfo +
		`, ` + columnCallerServiceName + `, ` + columnCallerHostName + `, ` + columnOpsType + `, ` + columnOpsTime + `, ` + columnOpsContent + `) ` +
		` VALUES (?, ?, ?, ` + sqlUserInfoType + `, ?, ?, ?, ?, ?) USING TTL ` + userOperationTTL
)

func marshalRequest(request interface{}) (opsContentStr string) {
	opsContent, err := json.Marshal(request)
	opsContentStr = ""
	if err == nil {
		opsContentStr = string(opsContent)
	}
	return
}

func (s *CassandraMetadataService) recordUserOperation(entityName string, entityUUID string, entityType string, userName string, userEmail string, callerServiceName string, callerHostName string, operationType string, operationTime time.Time, operationContent string) error {

	batch := s.session.NewBatch(gocql.LoggedBatch)

	batch.Query(
		sqlRecordUserOperationByEntityName,
		entityName,
		entityUUID,
		entityType,
		userName,
		userEmail,
		callerServiceName,
		callerHostName,
		operationType,
		operationTime,
		operationContent)

	batch.Query(
		sqlRecordUserOperationByEntityUUID,
		entityName,
		entityUUID,
		entityType,
		userName,
		userEmail,
		callerServiceName,
		callerHostName,
		operationType,
		operationTime,
		operationContent)

	batch.Cons = s.highConsLevel

	if err := s.session.ExecuteBatch(batch); err != nil {
		s.log.WithFields(bark.Fields{common.TagErr: err}).Error("recordUserOperation failed")
		return fmt.Errorf("recordUserOperation error: %v", err)
	}
	return nil
}

// DestinationCRUD CQL commands go here
const (
	sqlDstType = `{` +
		columnUUID + `: ?, ` +
		columnPath + `: ?, ` +
		columnType + `: ?, ` +
		columnStatus + `: ?, ` +
		columnConsumedMessagesRetention + `: ?, ` +
		columnUnconsumedMessagesRetention + `: ?, ` +
		columnOwnerEmail + `: ?, ` +
		columnChecksumOption + `: ?, ` +
		columnIsMultiZone + `: ?, ` +
		columnZoneConfigs + `: ?}`

	sqlInsertDstByUUID = `INSERT INTO ` + tableDestinations +
		`(` + columnUUID + `, ` + columnIsMultiZone + `, ` + columnDestination + `, ` + columnKafkaCluster + `, ` + columnKafkaTopics + `, ` + columnDLQConsumerGroup + `, ` + columnDLQPurgeBefore + `, ` + columnDLQMergeBefore + `) ` +
		` VALUES (?, ?, ` + sqlDstType + `, ?, ?, ?, ?, ?)`

	sqlInsertDstByPath = `INSERT INTO ` + tableDestinationsByPath +
		`(` +
		columnDirectoryUUID + `, ` +
		columnPath + `, ` +
		columnIsMultiZone + `, ` +
		columnDestination + `, ` +
		columnKafkaCluster + `, ` +
		columnKafkaTopics +
		`) VALUES (?, ?, ?, ` + sqlDstType + `, ?, ?) IF NOT EXISTS`

	sqlGetDstByPath = `SELECT ` +
		columnDestination + `.` + columnUUID + `, ` +
		columnPath + `, ` +
		columnDestination + `.` + columnType + `, ` +
		columnDestination + `.` + columnStatus + `, ` +
		columnDestination + `.` + columnConsumedMessagesRetention + `, ` +
		columnDestination + `.` + columnUnconsumedMessagesRetention + `, ` +
		columnDestination + `.` + columnOwnerEmail + `, ` +
		columnDestination + `.` + columnChecksumOption + `, ` +
		columnDestination + `.` + columnIsMultiZone + `, ` +
		columnDestination + `.` + columnZoneConfigs + `, ` +
		columnKafkaCluster + `, ` +
		columnKafkaTopics +
		` FROM ` + tableDestinationsByPath +
		` WHERE ` + columnDirectoryUUID + `=? and ` + columnPath + `=?`

	sqlListDestinationsByUUID = `SELECT ` +
		columnDestination + `.` + columnUUID + `, ` +
		columnDestination + `.` + columnPath + `, ` +
		columnDestination + `.` + columnType + `, ` +
		columnDestination + `.` + columnStatus + `, ` +
		columnDestination + `.` + columnConsumedMessagesRetention + `, ` +
		columnDestination + `.` + columnUnconsumedMessagesRetention + `, ` +
		columnDestination + `.` + columnOwnerEmail + `, ` +
		columnDestination + `.` + columnChecksumOption + `, ` +
		columnDestination + `.` + columnIsMultiZone + `, ` +
		columnDestination + `.` + columnZoneConfigs + `, ` +
		columnDLQConsumerGroup + `, ` +
		columnDLQPurgeBefore + `, ` +
		columnDLQMergeBefore + `, ` +
		columnKafkaCluster + `, ` +
		columnKafkaTopics +
		` FROM ` + tableDestinations

	sqlUpdateDstByUUID = `UPDATE ` + tableDestinations + ` SET ` +
		columnDestination + `=` + sqlDstType +
		`, ` + columnIsMultiZone + ` = ? ` +
		` WHERE ` + columnUUID + `=?`

	sqlUpdateDstDLQCursors = `UPDATE ` + tableDestinations + ` SET ` +
		columnDLQPurgeBefore + ` = ?,` +
		columnDLQMergeBefore + ` = ?` +
		` WHERE ` + columnUUID + `=?`

	sqlUpdateDstByPath = `UPDATE ` + tableDestinationsByPath + ` SET ` +
		columnDestination + `=` + sqlDstType +
		`, ` + columnIsMultiZone + ` = ? ` +
		` WHERE ` + columnPath + `=? and ` + columnDirectoryUUID + `=?`

	sqlDeleteDst = `DELETE FROM ` + tableDestinationsByPath +
		` WHERE ` + columnDirectoryUUID + `=? and ` + columnPath + `=?`

	sqlDeleteDstUUID = sqlInsertDstByUUID + ` USING TTL ?`
)

// CreateDestination implements the corresponding TChanMetadataServiceClient API
// Cassandra doesn't support conditional (IF NOT EXISTS) updates across multiple tables.
// The workaround is to create a record in "destinations" table and then insert a corresponding record into
// the "destinations_by_path" table using IF NOT EXISTS. If the insert into "destinations_by_path" is rejected due to
// the record existance tries to cleanup the orphaned record from "destinations" and returns "AlradyExists" error.
// Cleanup failure is not a problem as the destination that is not referenced by path is not accessible by any client.
// If large number of orphaned records is ever generated an offline process can perform lazy cleanup by performing
// full "destinations" table scan.
// DeleteDestination deletes row from "destinations_by_path" table, but keeps row in "destinations" by updating its
// status to DELETED.
func (s *CassandraMetadataService) CreateDestination(ctx thrift.Context, request *shared.CreateDestinationRequest) (*shared.DestinationDescription, error) {
	uuidRequest := shared.NewCreateDestinationUUIDRequest()
	uuidRequest.Request = request
	uuidRequest.DestinationUUID = common.StringPtr(uuid.New())
	return s.CreateDestinationUUID(ctx, uuidRequest)
}

// CreateDestinationUUID creates destination with given destination uuid
func (s *CassandraMetadataService) CreateDestinationUUID(ctx thrift.Context, uuidRequest *shared.CreateDestinationUUIDRequest) (*shared.DestinationDescription, error) {
	destinationUUID := uuidRequest.GetDestinationUUID()
	request := uuidRequest.GetRequest()

	if request.GetDLQConsumerGroupUUID() == `` {
		request.DLQConsumerGroupUUID = nil // CQL doesn't accept empty string as a UUID type value; force nil
	}

	if err := s.session.Query(
		sqlInsertDstByUUID,
		destinationUUID,
		request.GetIsMultiZone(),
		destinationUUID,
		request.GetPath(),
		request.GetType(),
		shared.DestinationStatus_ENABLED,
		request.GetConsumedMessagesRetention(),
		request.GetUnconsumedMessagesRetention(),
		request.GetOwnerEmail(),
		request.GetChecksumOption(),
		request.GetIsMultiZone(),
		marshalDstZoneConfigs(request.GetZoneConfigs()),
		request.KafkaCluster,
		request.KafkaTopics,
		request.DLQConsumerGroupUUID, // may be nil
		int64(0), int64(0),           // dlq_{purge,merge}_before default to '0'
	).Consistency(s.highConsLevel).Exec(); err != nil {
		return nil, &shared.InternalServiceError{
			Message: fmt.Sprintf("CreateDestination failure while inserting into destinations: %v", err),
		}
	}

	// If this is a DLQ destination, don't insert it in the path table
	if !common.PathDLQRegex.MatchString(request.GetPath()) {
		// In case of conflict the existing row values are written to these variables.
		previous := make(map[string]interface{})
		query := s.session.Query(
			sqlInsertDstByPath,
			directoryUUID,
			request.GetPath(),
			request.GetIsMultiZone(),
			destinationUUID,
			request.GetPath(),
			request.GetType(),
			shared.DestinationStatus_ENABLED,
			request.GetConsumedMessagesRetention(),
			request.GetUnconsumedMessagesRetention(),
			request.GetOwnerEmail(),
			request.GetChecksumOption(),
			request.GetIsMultiZone(),
			marshalDstZoneConfigs(request.GetZoneConfigs()),
			request.KafkaCluster,
			request.KafkaTopics).Consistency(s.highConsLevel)
		applied, err := query.MapScanCAS(previous)
		if err != nil {
			return nil, &shared.InternalServiceError{
				Message: fmt.Sprintf("CreateDestination failure while inserting into destinations_by_path: %v", err),
			}
		}
		if !applied {
			// Record already exists
			deleteOrphanRecord := `DELETE FROM ` + tableDestinations + ` WHERE ` + columnUUID + `=?`
			if err = s.session.Query(deleteOrphanRecord, destinationUUID).Exec(); err != nil {
				// Just warn as orphaned records are not breaking correctness
				s.log.WithFields(bark.Fields{common.TagDst: common.FmtDst(destinationUUID), common.TagErr: err}).Warn(`CreateDestination failure deleting orphan record from destinations`)
			}
			return nil, &shared.EntityAlreadyExistsError{
				Message: fmt.Sprintf("CreateDestination: Destination \"%v\" already exists with id=%v",
					request.GetPath(), previous[columnDirectoryUUID]),
			}
		}
	}
	callerUserName := getThriftContextValue(ctx, common.CallerUserName)
	callerHostName := getThriftContextValue(ctx, common.CallerHostName)
	callerServiceName := getThriftContextValue(ctx, common.CallerServiceName)

	s.recordUserOperation(
		request.GetPath(),
		destinationUUID,
		entityTypeDst,
		callerUserName,
		"", //place holder for user's email
		callerServiceName,
		callerHostName,
		opsCreate,
		time.Now(),
		marshalRequest(request))

	return &shared.DestinationDescription{
		DestinationUUID: common.StringPtr(destinationUUID),
		Path:            common.StringPtr(request.GetPath()),
		Type:            common.InternalDestinationTypePtr(request.GetType()),
		Status:          common.InternalDestinationStatusPtr(shared.DestinationStatus_ENABLED),
		ConsumedMessagesRetention:   common.Int32Ptr(request.GetConsumedMessagesRetention()),
		UnconsumedMessagesRetention: common.Int32Ptr(request.GetUnconsumedMessagesRetention()),
		OwnerEmail:                  common.StringPtr(request.GetOwnerEmail()),
		ChecksumOption:              common.InternalChecksumOptionPtr(request.GetChecksumOption()),
		IsMultiZone:                 common.BoolPtr(request.GetIsMultiZone()),
		ZoneConfigs:                 request.GetZoneConfigs(),
		DLQConsumerGroupUUID:        common.StringPtr(request.GetDLQConsumerGroupUUID()),
		KafkaCluster:                common.StringPtr(request.GetKafkaCluster()),
		KafkaTopics:                 request.KafkaTopics,
	}, nil
}

func getUtilDestinationDescription() *shared.DestinationDescription {
	result := &shared.DestinationDescription{}
	result.DestinationUUID = common.StringPtr("")
	result.Path = common.StringPtr("")
	result.Type = common.InternalDestinationTypePtr(0)
	result.Status = common.InternalDestinationStatusPtr(0)
	result.ConsumedMessagesRetention = common.Int32Ptr(0)
	result.UnconsumedMessagesRetention = common.Int32Ptr(0)
	result.OwnerEmail = common.StringPtr("")
	result.ChecksumOption = common.InternalChecksumOptionPtr(0)
	result.DLQConsumerGroupUUID = common.StringPtr("")
	result.DLQPurgeBefore = common.Int64Ptr(0)
	result.DLQMergeBefore = common.Int64Ptr(0)
	result.IsMultiZone = common.BoolPtr(false)
	result.ZoneConfigs = shared.DestinationDescription_ZoneConfigs_DEFAULT
	result.KafkaCluster = common.StringPtr("")
	result.KafkaTopics = make([]string, 0)

	return result
}

func getUtilConsumerGroupDescription() *shared.ConsumerGroupDescription {
	result := &shared.ConsumerGroupDescription{}
	result.ConsumerGroupUUID = common.StringPtr("")
	result.DestinationUUID = common.StringPtr("")
	result.ConsumerGroupName = common.StringPtr("")
	result.StartFrom = common.Int64Ptr(0)
	result.Status = common.InternalConsumerGroupStatusPtr(0)
	result.LockTimeoutSeconds = common.Int32Ptr(0)
	result.MaxDeliveryCount = common.Int32Ptr(0)
	result.SkipOlderMessagesSeconds = common.Int32Ptr(0)
	result.DelaySeconds = common.Int32Ptr(0)
	result.OwnerEmail = common.StringPtr("")
	result.IsMultiZone = common.BoolPtr(false)
	result.ActiveZone = common.StringPtr("")
	result.ZoneConfigs = shared.ConsumerGroupDescription_ZoneConfigs_DEFAULT
	result.Options = make(map[string]string, 0)

	return result
}

func getUtilEntityOpsDescription() *shared.EntityOpsDescription {
	result := &shared.EntityOpsDescription{}
	result.EntityUUID = common.StringPtr("")
	result.EntityName = common.StringPtr("")
	result.EntityType = common.StringPtr("")
	result.HostName = common.StringPtr("")
	result.ServiceName = common.StringPtr("")
	result.UserName = common.StringPtr("")
	result.OpsType = common.StringPtr("")
	result.OpsTime = common.StringPtr("")
	result.OpsContent = common.StringPtr("")

	return result
}

func getUtilHostDescription() *m.HostDescription {
	result := &m.HostDescription{}
	result.HostUUID = common.StringPtr("")
	result.HostAddr = common.StringPtr("")
	result.HostName = common.StringPtr("")
	return result
}

func marshalDstZoneConfigs(configs []*shared.DestinationZoneConfig) []map[string]interface{} {
	configsData := make([]map[string]interface{}, len(configs))
	for i, config := range configs {
		configsData[i] = map[string]interface{}{
			columnZone:                   config.GetZone(),
			columnAllowPublish:           config.GetAllowPublish(),
			columnAllowConsume:           config.GetAllowConsume(),
			columnAlwaysReplicatedTo:     config.GetAlwaysReplicateTo(),
			columnRemoteExtentReplicaNum: config.GetRemoteExtentReplicaNum(),
		}
	}
	return configsData
}

func unmarshalDstZoneConfigs(configsData []map[string]interface{}) []*shared.DestinationZoneConfig {
	configs := make([]*shared.DestinationZoneConfig, len(configsData))
	for i, configMap := range configsData {
		configs[i] = &shared.DestinationZoneConfig{
			Zone:                   common.StringPtr(toString(configMap[columnZone])),
			AllowPublish:           common.BoolPtr(toBool(configMap[columnAllowPublish])),
			AllowConsume:           common.BoolPtr(toBool(configMap[columnAllowConsume])),
			AlwaysReplicateTo:      common.BoolPtr(toBool(configMap[columnAlwaysReplicatedTo])),
			RemoteExtentReplicaNum: common.Int32Ptr(int32(toInt(configMap[columnRemoteExtentReplicaNum]))),
		}
	}
	return configs
}

// ReadDestination implements the corresponding TChanMetadataServiceClient API
// Either path or destinationUUID can be specified.
// Deleted destinations are returned with DELETED status only when destinationUUID is used.
func (s *CassandraMetadataService) ReadDestination(ctx thrift.Context, getRequest *shared.ReadDestinationRequest) (result *shared.DestinationDescription, err error) {
	var query *gocql.Query
	var uuid, sql string
	if getRequest.Path != nil {
		if getRequest.DestinationUUID != nil {
			return nil, &shared.BadRequestError{Message: "ReadDestination: both DestinationUUID and Path are specified"}
		}

		// Detect if this is a normal path or a UUID-as-path; swap for the latter
		// !!!TODO: I don't think we need this check any more since we use consumer group name plus '.dlq' as dlq path
		// DEVNOTE: We definitely do need this check. DLQs are not to be accessed by name. Their name is for reporting purposes ONLY.
		if uuidRegex.MatchString(getRequest.GetPath()) {
			getRequest.DestinationUUID = getRequest.Path
			getRequest.Path = nil
		}
	}

	var key string
	if getRequest.Path != nil {
		key = "dstpath:" + getRequest.GetPath()
	} else {
		key = "dstuuid:" + getRequest.GetDestinationUUID()
	}

	cached := s.destinationCache.Get(key)
	if cached != nil {
		return cached.(*shared.DestinationDescription), nil
	}

	result = getUtilDestinationDescription()
	var zoneConfigsData []map[string]interface{}
	if getRequest.Path != nil {
		query = s.session.Query(sqlGetDstByPath).Consistency(s.lowConsLevel)
		query.Bind(directoryUUID, getRequest.GetPath())
		err = query.Scan(
			result.DestinationUUID,
			result.Path,
			result.Type,
			result.Status,
			result.ConsumedMessagesRetention,
			result.UnconsumedMessagesRetention,
			result.OwnerEmail,
			result.ChecksumOption,
			result.IsMultiZone,
			&zoneConfigsData,
			result.KafkaCluster,
			&result.KafkaTopics,
		)
	} else {
		sql = sqlListDestinationsByUUID + ` WHERE ` + columnUUID + `=?`
		query = s.session.Query(sql).Consistency(s.lowConsLevel)
		uuid = getRequest.GetDestinationUUID()
		query.Bind(uuid)
		err = query.Scan(
			result.DestinationUUID,
			result.Path,
			result.Type,
			result.Status,
			result.ConsumedMessagesRetention,
			result.UnconsumedMessagesRetention,
			result.OwnerEmail,
			result.ChecksumOption,
			result.IsMultiZone,
			&zoneConfigsData,
			result.DLQConsumerGroupUUID, //
			result.DLQPurgeBefore,       // Only a UUID lookup can populate these values; this is OK since DLQ destinations can only be found by UUID anyway
			result.DLQMergeBefore,       //
			result.KafkaCluster,
			&result.KafkaTopics,
		)
	}
	result.ZoneConfigs = unmarshalDstZoneConfigs(zoneConfigsData)

	if err != nil {
		if err == gocql.ErrNotFound {
			var dest string
			if getRequest.Path != nil {
				dest = getRequest.GetPath()
			} else {
				dest = getRequest.GetDestinationUUID()
			}

			return nil, &shared.EntityNotExistsError{
				Message: fmt.Sprintf("Destination %s does not exist", dest),
			}
		}

		return nil, &shared.InternalServiceError{
			Message: err.Error(),
		}
	}

	*result.DLQPurgeBefore = int64(cqlTimestampToUnixNano(*result.DLQPurgeBefore))
	*result.DLQMergeBefore = int64(cqlTimestampToUnixNano(*result.DLQMergeBefore))

	s.destinationCache.Put(key, result)
	return result, nil
}

// UpdateDestination implements the corresponding TChanMetadataServiceClient API
func (s *CassandraMetadataService) UpdateDestination(ctx thrift.Context, updateRequest *shared.UpdateDestinationRequest) (*shared.DestinationDescription, error) {
	if updateRequest.GetStatus() == shared.DestinationStatus_DELETED {
		return nil, &shared.BadRequestError{
			Message: "UpdateDestination: Update to DELETED status is not allowed. Use DeleteDestination instead.",
		}
	}
	getDestination := &shared.ReadDestinationRequest{
		DestinationUUID: common.StringPtr(updateRequest.GetDestinationUUID()),
	}
	existing, err := s.ReadDestination(nil, getDestination)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, &shared.BadRequestError{
			Message: "Bad destination UUID",
		}
	}

	// Note: if we add a new updatable property here, we also need to update the metadataReconciler in replicator to do reconcilation
	if !updateRequest.IsSetStatus() {
		updateRequest.Status = common.InternalDestinationStatusPtr(existing.GetStatus())
	}
	if !updateRequest.IsSetConsumedMessagesRetention() {
		updateRequest.ConsumedMessagesRetention = common.Int32Ptr(existing.GetConsumedMessagesRetention())
	}
	if !updateRequest.IsSetUnconsumedMessagesRetention() {
		updateRequest.UnconsumedMessagesRetention = common.Int32Ptr(existing.GetUnconsumedMessagesRetention())
	}
	if !updateRequest.IsSetOwnerEmail() {
		updateRequest.OwnerEmail = common.StringPtr(existing.GetOwnerEmail())
	}
	if !updateRequest.IsSetChecksumOption() {
		updateRequest.ChecksumOption = common.InternalChecksumOptionPtr(existing.GetChecksumOption())
	}
	isMultiZone := existing.GetIsMultiZone()
	if !updateRequest.IsSetZoneConfigs() {
		updateRequest.ZoneConfigs = existing.GetZoneConfigs()
	} else {
		isMultiZone = true
	}
	batch := s.session.NewBatch(gocql.LoggedBatch) // Consider switching to unlogged

	batch.Query(
		sqlUpdateDstByUUID,
		updateRequest.GetDestinationUUID(),
		existing.GetPath(),
		existing.GetType(),
		updateRequest.GetStatus(),
		updateRequest.GetConsumedMessagesRetention(),
		updateRequest.GetUnconsumedMessagesRetention(),
		updateRequest.GetOwnerEmail(),
		updateRequest.GetChecksumOption(),
		isMultiZone,
		marshalDstZoneConfigs(updateRequest.GetZoneConfigs()),
		isMultiZone,
		updateRequest.GetDestinationUUID())

	batch.Query(
		sqlUpdateDstByPath,
		updateRequest.GetDestinationUUID(),
		existing.GetPath(),
		existing.GetType(),
		updateRequest.GetStatus(),
		updateRequest.GetConsumedMessagesRetention(),
		updateRequest.GetUnconsumedMessagesRetention(),
		updateRequest.GetOwnerEmail(),
		updateRequest.GetChecksumOption(),
		isMultiZone,
		marshalDstZoneConfigs(updateRequest.GetZoneConfigs()),
		isMultiZone,
		existing.GetPath(),
		directoryUUID)

	batch.Cons = s.highConsLevel

	if err = s.session.ExecuteBatch(batch); err != nil {
		return nil, &shared.InternalServiceError{
			Message: "UpdateDestination: " + err.Error(),
		}
	}

	callerUserName := getThriftContextValue(ctx, common.CallerUserName)
	callerHostName := getThriftContextValue(ctx, common.CallerHostName)
	callerServiceName := getThriftContextValue(ctx, common.CallerServiceName)

	s.recordUserOperation(
		existing.GetPath(),
		updateRequest.GetDestinationUUID(),
		entityTypeDst,
		callerUserName,
		"", //place holder for user's email
		callerServiceName,
		callerHostName,
		opsUpdate,
		time.Now(),
		marshalRequest(updateRequest))

	if updateRequest.IsSetStatus() {
		existing.Status = common.InternalDestinationStatusPtr(updateRequest.GetStatus())
	}
	if updateRequest.IsSetConsumedMessagesRetention() {
		existing.ConsumedMessagesRetention = common.Int32Ptr(updateRequest.GetConsumedMessagesRetention())
	}
	if updateRequest.IsSetUnconsumedMessagesRetention() {
		existing.UnconsumedMessagesRetention = common.Int32Ptr(updateRequest.GetUnconsumedMessagesRetention())
	}
	if updateRequest.IsSetOwnerEmail() {
		existing.OwnerEmail = common.StringPtr(updateRequest.GetOwnerEmail())
	}
	if updateRequest.IsSetChecksumOption() {
		existing.ChecksumOption = common.InternalChecksumOptionPtr(updateRequest.GetChecksumOption())
	}
	if updateRequest.IsSetZoneConfigs() {
		existing.ZoneConfigs = updateRequest.GetZoneConfigs()
	}
	existing.IsMultiZone = common.BoolPtr(isMultiZone)

	s.destinationCache.Delete("dstpath:" + existing.GetPath())
	s.destinationCache.Delete("dstuuid:" + updateRequest.GetDestinationUUID())
	return existing, nil
}

// DeleteDestination implements the corresponding TChanMetadataServiceClient API
func (s *CassandraMetadataService) DeleteDestination(ctx thrift.Context, deleteRequest *shared.DeleteDestinationRequest) error {
	getDestination := &shared.ReadDestinationRequest{
		Path: common.StringPtr(deleteRequest.GetPath()),
	}
	existing, err := s.ReadDestination(nil, getDestination)
	if err != nil {
		return err
	}
	batch := s.session.NewBatch(gocql.LoggedBatch)
	batch.Query(
		sqlUpdateDstByUUID,
		existing.GetDestinationUUID(),
		existing.GetPath(),
		existing.GetType(),
		shared.DestinationStatus_DELETING,
		existing.GetConsumedMessagesRetention(),
		existing.GetUnconsumedMessagesRetention(),
		existing.GetOwnerEmail(),
		existing.GetChecksumOption(),
		existing.GetIsMultiZone(),
		marshalDstZoneConfigs(existing.GetZoneConfigs()),
		existing.GetIsMultiZone(),
		existing.GetDestinationUUID())

	batch.Query(sqlDeleteDst, directoryUUID, existing.GetPath())

	batch.Cons = s.midConsLevel

	if err = s.session.ExecuteBatch(batch); err != nil {
		return &shared.InternalServiceError{
			Message: "DeleteDestination: " + err.Error(),
		}
	}

	callerUserName := getThriftContextValue(ctx, common.CallerUserName)
	callerHostName := getThriftContextValue(ctx, common.CallerHostName)
	callerServiceName := getThriftContextValue(ctx, common.CallerServiceName)

	s.recordUserOperation(
		existing.GetPath(),
		existing.GetDestinationUUID(),
		entityTypeDst,
		callerUserName,
		"", //place holder for user's email
		callerServiceName,
		callerHostName,
		opsDelete,
		time.Now(),
		marshalRequest(deleteRequest))

	s.destinationCache.Delete("dstpath:" + existing.GetPath())
	s.destinationCache.Delete("dstuuid:" + existing.GetDestinationUUID())
	return nil
}

// DeleteDestinationUUID deletes the destination corresponding to the given UUID
// from the destinations table
func (s *CassandraMetadataService) DeleteDestinationUUID(ctx thrift.Context, deleteRequest *m.DeleteDestinationUUIDRequest) error {

	if deleteRequest.UUID == nil {
		return &shared.BadRequestError{Message: "Missing UUID param"}
	}

	getDestination := &shared.ReadDestinationRequest{
		DestinationUUID: common.StringPtr(deleteRequest.GetUUID()),
	}
	existing, err := s.ReadDestination(nil, getDestination)
	if err != nil {
		return err
	}

	if existing.GetDLQConsumerGroupUUID() == `` {
		existing.DLQConsumerGroupUUID = nil
	}

	query := s.session.Query(sqlDeleteDstUUID,
		existing.GetDestinationUUID(),
		existing.GetIsMultiZone(),
		existing.GetDestinationUUID(),
		existing.GetPath(),
		existing.GetType(),
		shared.DestinationStatus_DELETED,
		existing.GetConsumedMessagesRetention(),
		existing.GetUnconsumedMessagesRetention(),
		existing.GetOwnerEmail(),
		existing.GetChecksumOption(),
		existing.GetIsMultiZone(),
		marshalDstZoneConfigs(existing.GetZoneConfigs()),
		existing.KafkaCluster,
		existing.KafkaTopics,
		existing.DLQConsumerGroupUUID, // May be nil
		existing.DLQPurgeBefore,       // May be nil
		existing.DLQMergeBefore,       // May be nil
		defaultDeleteTTLSeconds).Consistency(s.midConsLevel)

	if err = query.Exec(); err != nil {
		return &shared.InternalServiceError{
			Message: fmt.Sprintf("DeleteDestinationUUID:%v (%v)", *existing.DLQConsumerGroupUUID, err),
		}
	}

	callerUserName := getThriftContextValue(ctx, common.CallerUserName)
	callerHostName := getThriftContextValue(ctx, common.CallerHostName)
	callerServiceName := getThriftContextValue(ctx, common.CallerServiceName)

	s.recordUserOperation(
		existing.GetPath(),
		existing.GetDestinationUUID(),
		entityTypeDst,
		callerUserName,
		"", //place holder for user's email
		callerServiceName,
		callerHostName,
		opsDelete,
		time.Now(),
		marshalRequest(deleteRequest))

	s.destinationCache.Delete("dstpath:" + existing.GetPath())
	s.destinationCache.Delete("dstuuid:" + existing.GetDestinationUUID())
	return nil
}

// ListDestinations implements the corresponding TChanMetadataServiceClient API
func (s *CassandraMetadataService) ListDestinations(ctx thrift.Context, listRequest *shared.ListDestinationsRequest) (*shared.ListDestinationsResult_, error) {
	sql := `SELECT ` +
		columnDestination + `.` + columnUUID + `, ` +
		columnPath + `, ` +
		columnDestination + `.` + columnType + `, ` +
		columnDestination + `.` + columnStatus + `, ` +
		columnDestination + `.` + columnConsumedMessagesRetention + `, ` +
		columnDestination + `.` + columnUnconsumedMessagesRetention + `, ` +
		columnDestination + `.` + columnOwnerEmail + `, ` +
		columnDestination + `.` + columnChecksumOption + `, ` +
		columnDestination + `.` + columnIsMultiZone + `, ` +
		columnDestination + `.` + columnZoneConfigs +
		` FROM ` + tableDestinationsByPath +
		` WHERE ` + columnDirectoryUUID + `=? and ` + columnPath + `>=? and ` + columnPath + `<?`
	if listRequest.GetMultiZoneOnly() {
		sql = sql + ` and ` + columnIsMultiZone + `=true`
	}

	query := s.session.Query(sql).Consistency(s.lowConsLevel)
	// Next string after prefix to be used as non inclusive boundary for range select
	// NOTE: The following assumes the last character in the provided prefix is not 0xFF
	// because it is not an allowed destination path character.
	prefix := listRequest.GetPrefix()
	lastIndex := len(prefix) - 1
	right := prefix[:lastIndex] + string([]byte{prefix[lastIndex] + 1})
	query.Bind(directoryUUID, prefix, right)
	iter := query.PageSize(int(listRequest.GetLimit())).PageState(listRequest.GetPageToken()).Iter()

	if iter == nil {
		return nil, &shared.InternalServiceError{
			Message: "Query returned nil iterator",
		}
	}

	result := &shared.ListDestinationsResult_{
		Destinations:  []*shared.DestinationDescription{},
		NextPageToken: listRequest.PageToken,
	}
	d := getUtilDestinationDescription()
	var zoneConfigsData []map[string]interface{}
	for iter.Scan(
		d.DestinationUUID,
		d.Path,
		d.Type,
		d.Status,
		d.ConsumedMessagesRetention,
		d.UnconsumedMessagesRetention,
		d.OwnerEmail,
		d.ChecksumOption,
		d.IsMultiZone,
		&zoneConfigsData) {
		d.ZoneConfigs = unmarshalDstZoneConfigs(zoneConfigsData)

		// Get a new item within limit
		result.Destinations = append(result.Destinations, d)
		d = getUtilDestinationDescription()
		zoneConfigsData = nil
	}

	nextPageToken := iter.PageState()
	result.NextPageToken = make([]byte, len(nextPageToken))
	copy(result.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, &shared.InternalServiceError{
			Message: err.Error(),
		}
	}

	return result, nil
}

// ListDestinationsByUUID implements the corresponding TChanMetadataServiceClient API
func (s *CassandraMetadataService) ListDestinationsByUUID(ctx thrift.Context, listRequest *shared.ListDestinationsByUUIDRequest) (*shared.ListDestinationsResult_, error) {
	sql := sqlListDestinationsByUUID
	if listRequest.GetMultiZoneOnly() {
		sql = sql + ` WHERE ` + columnIsMultiZone + `=true`
	}

	iter := s.session.Query(sql).Consistency(s.lowConsLevel).PageSize(int(listRequest.GetLimit())).PageState(listRequest.GetPageToken()).Iter()

	if iter == nil {
		return nil, &shared.InternalServiceError{
			Message: "Query returned nil iterator",
		}
	}

	result := &shared.ListDestinationsResult_{
		Destinations:  []*shared.DestinationDescription{},
		NextPageToken: listRequest.PageToken,
	}
	d := getUtilDestinationDescription()
	var zoneConfigsData []map[string]interface{}
	for iter.Scan(
		d.DestinationUUID,
		d.Path,
		d.Type,
		d.Status,
		d.ConsumedMessagesRetention,
		d.UnconsumedMessagesRetention,
		d.OwnerEmail,
		d.ChecksumOption,
		d.IsMultiZone,
		&zoneConfigsData,
		d.DLQConsumerGroupUUID,
		d.DLQPurgeBefore,
		d.DLQMergeBefore,
		d.KafkaCluster,
		&d.KafkaTopics) {
		d.ZoneConfigs = unmarshalDstZoneConfigs(zoneConfigsData)

		// Get a new item within limit
		*d.DLQPurgeBefore = int64(cqlTimestampToUnixNano(*d.DLQPurgeBefore))
		*d.DLQMergeBefore = int64(cqlTimestampToUnixNano(*d.DLQMergeBefore))

		if listRequest.GetValidateAgainstPathTable() {
			readRequest := &shared.ReadDestinationRequest{
				Path: d.Path,
			}

			desc, err := s.ReadDestination(nil, readRequest)
			if err == nil && desc.GetDestinationUUID() == d.GetDestinationUUID() {
				result.Destinations = append(result.Destinations, d)
			}
		} else {
			result.Destinations = append(result.Destinations, d)
		}

		d = getUtilDestinationDescription()
		zoneConfigsData = nil
	}

	nextPageToken := iter.PageState()
	result.NextPageToken = make([]byte, len(nextPageToken))
	copy(result.NextPageToken, nextPageToken)

	if err := iter.Close(); err != nil {
		return nil, &shared.InternalServiceError{
			Message: err.Error(),
		}
	}

	return result, nil
}

// ListAllDestinations implements the corresponding TChanMetadataServiceClient API
func (s *CassandraMetadataService) ListAllDestinations(ctx thrift.Context, listRequest *shared.ListDestinationsRequest) (*shared.ListDestinationsResult_, error) {
	//return s.ListDestinationsByUUID(ctx, listRequest)
	query := s.session.Query(sqlListDestinationsByUUID).Consistency(s.lowConsLevel)
	iter := query.PageSize(int(listRequest.GetLimit())).PageState(listRequest.GetPageToken()).Iter()
	if iter == nil {
		return nil, &shared.BadRequestError{
			Message: "Query returned nil iterator",
		}
	}

	result := &shared.ListDestinationsResult_{
		Destinations:  []*shared.DestinationDescription{},
		NextPageToken: listRequest.PageToken,
	}
	d := getUtilDestinationDescription()
	var zoneConfigsData []map[string]interface{}
	count := int64(0)
	for iter.Scan(
		d.DestinationUUID,
		d.Path,
		d.Type,
		d.Status,
		d.ConsumedMessagesRetention,
		d.UnconsumedMessagesRetention,
		d.OwnerEmail,
		d.ChecksumOption,
		d.IsMultiZone,
		&zoneConfigsData,
		d.DLQConsumerGroupUUID,
		d.DLQPurgeBefore,
		d.DLQMergeBefore,
		d.KafkaCluster,
		&d.KafkaTopics) && count < listRequest.GetLimit() {
		d.ZoneConfigs = unmarshalDstZoneConfigs(zoneConfigsData)
		*d.DLQPurgeBefore = int64(cqlTimestampToUnixNano(*d.DLQPurgeBefore))
		*d.DLQMergeBefore = int64(cqlTimestampToUnixNano(*d.DLQMergeBefore))
		// Get a new item within limit
		result.Destinations = append(result.Destinations, d)
		d = getUtilDestinationDescription()
		zoneConfigsData = nil
		count++
	}

	nextPageToken := iter.PageState()
	result.NextPageToken = make([]byte, len(nextPageToken))
	copy(result.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, &shared.InternalServiceError{
			Message: err.Error(),
		}
	}

	return result, nil
}

// CQL commands for ConsumerGroup CRUD go here
const (
	sqlCGValue = `{` +
		columnUUID + `: ?, ` +
		columnDestinationUUID + `: ?, ` +
		columnName + `: ?, ` +
		columnStartFrom + `: ?, ` +
		columnStatus + `: ?, ` +
		columnLockTimeoutSeconds + `: ?, ` +
		columnMaxDeliveryCount + `: ?, ` +
		columnSkipOlderMessagesSeconds + `: ?, ` +
		columnDelaySeconds + `: ?, ` +
		columnDeadLetterQueueDestinationUUID + `: ?, ` +
		columnOwnerEmail + `: ?, ` +
		columnIsMultiZone + `: ?, ` +
		columnActiveZone + `: ?, ` +
		columnZoneConfigs + `: ?, ` +
		columnOptions + `: ? }`

	sqlConsumerGroupType = columnConsumerGroup + `.` + columnUUID + "," +
		columnConsumerGroup + `.` + columnDestinationUUID + "," +
		columnConsumerGroup + `.` + columnName + "," +
		columnConsumerGroup + `.` + columnStartFrom + "," +
		columnConsumerGroup + `.` + columnStatus + "," +
		columnConsumerGroup + `.` + columnLockTimeoutSeconds + "," +
		columnConsumerGroup + `.` + columnMaxDeliveryCount + "," +
		columnConsumerGroup + `.` + columnSkipOlderMessagesSeconds + "," +
		columnConsumerGroup + `.` + columnDelaySeconds + "," +
		columnConsumerGroup + `.` + columnDeadLetterQueueDestinationUUID + "," +
		columnConsumerGroup + `.` + columnOwnerEmail + "," +
		columnConsumerGroup + `.` + columnIsMultiZone + "," +
		columnConsumerGroup + `.` + columnActiveZone + "," +
		columnConsumerGroup + `.` + columnZoneConfigs + "," +
		columnConsumerGroup + `.` + columnOptions

	sqlInsertCGByUUID = `INSERT INTO ` + tableConsumerGroups +
		`(` +
		columnUUID + `, ` +
		columnDestinationUUID + `, ` +
		columnIsMultiZone + `, ` +
		columnConsumerGroup +
		`) VALUES (?, ?, ?, ` + sqlCGValue + `)`

	sqlInsertCGByName = `INSERT INTO ` + tableConsumerGroupsByName +
		`(` +
		columnDestinationUUID + `, ` +
		columnName + `, ` +
		columnIsMultiZone + `, ` +
		columnConsumerGroup +
		`) VALUES (?, ?, ?, ` + sqlCGValue + `) IF NOT EXISTS`

	sqlGetCGByName = `SELECT  ` +
		sqlConsumerGroupType +
		` FROM ` + tableConsumerGroupsByName +
		` WHERE ` + columnDestinationUUID + `=? and ` + columnName + `=?`

	sqlGetCG = `SELECT  ` +
		sqlConsumerGroupType +
		` FROM ` + tableConsumerGroups

	sqlGetCGByUUID = sqlGetCG + ` WHERE ` + columnUUID + `=?`

	sqlListCGsByDestUUID = `SELECT  ` +
		sqlConsumerGroupType +
		` FROM ` + tableConsumerGroupsByName +
		` WHERE ` + columnDestinationUUID + `=?`

	sqlListCGsUUID = `SELECT  ` +
		sqlConsumerGroupType +
		` FROM ` + tableConsumerGroups +
		` WHERE ` + columnDestinationUUID + `=?`

	sqlUpdateCGByUUID = `UPDATE ` + tableConsumerGroups +
		` SET ` + columnDestinationUUID + ` = ?, ` + columnIsMultiZone + ` = ?, ` + columnConsumerGroup + `= ` + sqlCGValue +
		` WHERE ` + columnUUID + `=?`

	sqlUpdateCGByName = `UPDATE ` + tableConsumerGroupsByName +
		` SET ` + columnIsMultiZone + ` = ?, ` + columnConsumerGroup + `= ` + sqlCGValue +
		` WHERE ` + columnDestinationUUID + `=? and ` + columnName + `=?`

	sqlDeleteCGByUUIDWithTTL = sqlInsertCGByUUID + ` USING TTL ?`

	sqlDeleteCGByUUID = `DELETE from ` + tableConsumerGroups + ` WHERE uuid=?`

	sqlDeleteCGByName = `DELETE FROM ` + tableConsumerGroupsByName +
		` WHERE ` + columnDestinationUUID + `=? and ` + columnName + `=?`
)

// CreateConsumerGroup creates a consumer group.
func (s *CassandraMetadataService) CreateConsumerGroup(ctx thrift.Context, request *shared.CreateConsumerGroupRequest) (*shared.ConsumerGroupDescription, error) {
	uuidRequest := shared.NewCreateConsumerGroupUUIDRequest()
	uuidRequest.Request = request
	uuidRequest.ConsumerGroupUUID = common.StringPtr(uuid.New())
	return s.CreateConsumerGroupUUID(ctx, uuidRequest)
}

// CreateConsumerGroupUUID creates a ConsumerGroup for the given destination, if it doesn't already exist
// ConsumerGroups are tied to a destination path, so the same ConsumerGroupName can be used across
// multiple destination paths. If the requested [destinationPath, consumerGroupName] already exists,
// this method will return an EntityAlreadyExistsError.
func (s *CassandraMetadataService) CreateConsumerGroupUUID(ctx thrift.Context, request *shared.CreateConsumerGroupUUIDRequest) (*shared.ConsumerGroupDescription, error) {
	createRequest := request.GetRequest()
	cgUUID := request.GetConsumerGroupUUID()

	// Dead Letter Queue destination creation

	// Only non-UUID (non-DLQ) destinations get a DLQ for the corresponding consumer groups
	// We may create a consumer group consume a DLQ destination and no DLQ destination creation needed in this case
	var dlqUUID *string
	if common.PathRegex.MatchString(createRequest.GetDestinationPath()) {
		dlqDestDesc, err := s.createDlqDestination(cgUUID, createRequest.GetConsumerGroupName(), createRequest.GetOwnerEmail())
		if err != nil {
			return nil, err
		}

		dlqUUID = common.StringPtr(dlqDestDesc.GetDestinationUUID())
	} else {
		s.log.WithFields(bark.Fields{common.TagCnsm: common.FmtCnsm(cgUUID)}).Info("DeadLetterQueue destination not being created")
	}

	dstInfo, err := s.ReadDestination(nil, &shared.ReadDestinationRequest{Path: common.StringPtr(createRequest.GetDestinationPath())})
	if err != nil {

		if dlqUUID != nil {

			if e := s.DeleteDestinationUUID(nil, &m.DeleteDestinationUUIDRequest{UUID: dlqUUID}); e != nil {
				s.log.WithFields(bark.Fields{
					common.TagDst:  *dlqUUID,
					common.TagCnsm: cgUUID,
					common.TagErr:  err,
				}).Error(`CreateConsumerGroup - failed to cleanup DLQ destination`)
			}
		}

		return nil, err
	}
	dstUUID := dstInfo.GetDestinationUUID()

	/*
		   Every ConsumerGroup is assigned a UUID, which identifies the group uniquely. We need to
			 be able to retrieve a ConsumerGroup given either a UUID or a (destinationPath, groupName)
			 tuple. To enable this, we maintain the ConsumerGroup information across two tables, one
			 indexed by the UUID and the other indexed by the (destinationUUID, groupName) tuple. Creation
			 of a new consumer group happens as follows:
			     1. Assign a UUID and add an entry into the consumer_groups(UUID) table
			     2. If (1) succeeds, add an entry into the consumer_groups_by_name(destUUId, name) table
					     - This step is done as a Compare and Swap (CAS) operation in CassandraDB
			     3. If (2) fails, delete orphan record created in (1)
			     4. If (3) fails, an offline job should cleanup the orphan

			A batch query cannot be used here, because CassandraDB requires all the queries in a batch
			to be from the same partition.
	*/

	err = s.session.Query(sqlInsertCGByUUID,
		cgUUID,
		dstUUID,
		createRequest.GetIsMultiZone(),
		cgUUID,
		dstUUID,
		createRequest.GetConsumerGroupName(),
		createRequest.GetStartFrom(),
		shared.ConsumerGroupStatus_ENABLED,
		createRequest.GetLockTimeoutSeconds(),
		createRequest.GetMaxDeliveryCount(),
		createRequest.GetSkipOlderMessagesSeconds(),
		createRequest.GetDelaySeconds(),
		dlqUUID,
		createRequest.GetOwnerEmail(),
		createRequest.GetIsMultiZone(),
		createRequest.GetActiveZone(),
		marshalCgZoneConfigs(createRequest.GetZoneConfigs()),
		createRequest.GetOptions()).Consistency(s.highConsLevel).Exec()

	if err != nil {

		if dlqUUID != nil {

			if e := s.DeleteDestinationUUID(nil, &m.DeleteDestinationUUIDRequest{UUID: dlqUUID}); e != nil {
				s.log.WithFields(bark.Fields{
					common.TagDst:  *dlqUUID,
					common.TagCnsm: cgUUID,
					common.TagErr:  err,
				}).Error(`CreateConsumerGroup - failed to cleanup DLQ destination`)
			}
		}

		return nil, &shared.InternalServiceError{
			Message: fmt.Sprintf("CreateConsumerGroup - insert into consumer_groups table failed, dst=%v, cg=%v, err=%v",
				createRequest.GetDestinationPath(), createRequest.GetConsumerGroupName(), err),
		}
	}

	query := s.session.Query(sqlInsertCGByName,
		dstUUID,
		createRequest.GetConsumerGroupName(),
		createRequest.GetIsMultiZone(),
		cgUUID,
		dstUUID,
		createRequest.GetConsumerGroupName(),
		createRequest.GetStartFrom(),
		shared.ConsumerGroupStatus_ENABLED,
		createRequest.GetLockTimeoutSeconds(),
		createRequest.GetMaxDeliveryCount(),
		createRequest.GetSkipOlderMessagesSeconds(),
		createRequest.GetDelaySeconds(),
		dlqUUID,
		createRequest.GetOwnerEmail(),
		createRequest.GetIsMultiZone(),
		createRequest.GetActiveZone(),
		marshalCgZoneConfigs(createRequest.GetZoneConfigs()),
		createRequest.GetOptions()).Consistency(s.highConsLevel)

	previous := make(map[string]interface{}) // We actually throw away the old values below, but passing nil causes a panic

	applied, err := query.MapScanCAS(previous)
	if !applied {
		if err = s.session.Query(sqlDeleteCGByUUID, cgUUID).Exec(); err != nil {
			s.log.WithFields(bark.Fields{common.TagCnsm: common.FmtCnsm(cgUUID), common.TagErr: err}).Warn(`CreateConsumerGroup - failed to delete orphan record after a failed CAS attempt, ,`)
		}

		if dlqUUID != nil {

			if e := s.DeleteDestinationUUID(nil, &m.DeleteDestinationUUIDRequest{UUID: dlqUUID}); e != nil {
				s.log.WithFields(bark.Fields{
					common.TagDst:  *dlqUUID,
					common.TagCnsm: cgUUID,
					common.TagErr:  err,
				}).Error(`CreateConsumerGroup - failed to cleanup DLQ destination`)
			}
		}

		return nil, &shared.EntityAlreadyExistsError{
			Message: fmt.Sprintf("CreateConsumerGroup - Group exists, dst=%v cg=%v err=%v", createRequest.GetDestinationPath(), createRequest.GetConsumerGroupName(), err),
		}
	}

	callerUserName := getThriftContextValue(ctx, common.CallerUserName)
	callerHostName := getThriftContextValue(ctx, common.CallerHostName)
	callerServiceName := getThriftContextValue(ctx, common.CallerServiceName)

	s.recordUserOperation(
		createRequest.GetConsumerGroupName(),
		cgUUID,
		entityTypeCG,
		callerUserName,
		"", //place holder for user's email
		callerServiceName,
		callerHostName,
		opsCreate,
		time.Now(),
		marshalRequest(createRequest))

	return &shared.ConsumerGroupDescription{
		ConsumerGroupUUID:              common.StringPtr(cgUUID),
		DestinationUUID:                common.StringPtr(dstUUID),
		ConsumerGroupName:              common.StringPtr(createRequest.GetConsumerGroupName()),
		StartFrom:                      common.Int64Ptr(createRequest.GetStartFrom()),
		Status:                         common.InternalConsumerGroupStatusPtr(shared.ConsumerGroupStatus_ENABLED),
		LockTimeoutSeconds:             common.Int32Ptr(createRequest.GetLockTimeoutSeconds()),
		MaxDeliveryCount:               common.Int32Ptr(createRequest.GetMaxDeliveryCount()),
		SkipOlderMessagesSeconds:       common.Int32Ptr(createRequest.GetSkipOlderMessagesSeconds()),
		DelaySeconds:                   common.Int32Ptr(createRequest.GetDelaySeconds()),
		DeadLetterQueueDestinationUUID: dlqUUID,
		OwnerEmail:                     common.StringPtr(createRequest.GetOwnerEmail()),
		IsMultiZone:                    common.BoolPtr(createRequest.GetIsMultiZone()),
		ActiveZone:                     common.StringPtr(createRequest.GetActiveZone()),
		ZoneConfigs:                    createRequest.GetZoneConfigs(),
		Options:                        createRequest.GetOptions(),
	}, nil
}

func (s *CassandraMetadataService) createDlqDestination(cgUUID string, cgName string, ownerEmail string) (*shared.DestinationDescription, error) {
	dlqCreateRequest := shared.NewCreateDestinationRequest()
	dlqCreateRequest.ConsumedMessagesRetention = common.Int32Ptr(defaultDLQConsumedRetention)
	dlqCreateRequest.UnconsumedMessagesRetention = common.Int32Ptr(defaultDLQUnconsumedRetention)
	dlqCreateRequest.OwnerEmail = common.StringPtr(ownerEmail)
	dlqCreateRequest.Type = common.InternalDestinationTypePtr(shared.DestinationType_PLAIN)
	dlqCreateRequest.DLQConsumerGroupUUID = common.StringPtr(cgUUID)
	dlqPath, _ := common.GetDLQPathNameFromCGName(cgName)
	dlqCreateRequest.Path = common.StringPtr(dlqPath)

	var dlqDestDesc *shared.DestinationDescription
	dlqDestDesc, err := s.CreateDestination(nil, dlqCreateRequest)

	if err != nil || dlqDestDesc == nil {
		switch err.(type) {
		case *shared.EntityAlreadyExistsError:
			s.log.WithFields(bark.Fields{common.TagCnsm: common.FmtCnsm(cgUUID)}).Info("DeadLetterQueue destination already existed")
			mDLQReadRequest := shared.ReadDestinationRequest{
				Path: dlqCreateRequest.Path,
			}

			dlqDestDesc, err = s.ReadDestination(nil, &mDLQReadRequest)
			if err != nil || dlqDestDesc == nil {
				s.log.WithFields(bark.Fields{common.TagCnsm: common.FmtCnsm(cgUUID), common.TagErr: err}).Error(`Can't read existing DeadLetterQueue destination`)
				return nil, err
			}
			return dlqDestDesc, nil
		default:
			s.log.WithFields(bark.Fields{common.TagCnsm: common.FmtCnsm(cgUUID), common.TagErr: err}).Error(`Can't create DeadLetterQueue destination`)
			return nil, err
		}
	}
	return dlqDestDesc, err
}

func (s *CassandraMetadataService) readConsumerGroupByDstUUID(dstUUID string, cgName string) (*shared.ConsumerGroupDescription, error) {
	result := getUtilConsumerGroupDescription()

	key := "dstuuid_cgname:" + dstUUID + cgName
	cached := s.consumerGroupCache.Get(key)
	if cached != nil {
		return cached.(*shared.ConsumerGroupDescription), nil
	}

	var zoneConfigsData []map[string]interface{}
	query := s.session.Query(sqlGetCGByName, dstUUID, cgName).Consistency(s.lowConsLevel)
	if err := query.Scan(
		result.ConsumerGroupUUID,
		result.DestinationUUID,
		result.ConsumerGroupName,
		result.StartFrom,
		result.Status,
		result.LockTimeoutSeconds,
		result.MaxDeliveryCount,
		result.SkipOlderMessagesSeconds,
		result.DelaySeconds,
		&result.DeadLetterQueueDestinationUUID,
		result.OwnerEmail,
		result.IsMultiZone,
		result.ActiveZone,
		&zoneConfigsData,
		&result.Options); err != nil {
		if err == gocql.ErrNotFound {
			return nil, &shared.EntityNotExistsError{
				Message: fmt.Sprintf("ConsumerGroup %s of destinationUUID %s does not exist", cgName, dstUUID),
			}
		}

		return nil, &shared.InternalServiceError{
			Message: err.Error(),
		}
	}

	result.ZoneConfigs = unmarshalCgZoneConfigs(zoneConfigsData)
	s.consumerGroupCache.Put(key, result)
	if result.ConsumerGroupUUID != nil {
		s.consumerGroupCache.Put("cguuid:"+*result.ConsumerGroupUUID, result)
	}

	return result, nil
}

// ReadConsumerGroup returns the ConsumerGroupDescription for the [destinationPath, groupName].
// When destination path is specified as input, this method only returns result, if the
// destination has not been DELETED. When destination UUID is specified as input, this
// method will always return result, if the consumer group exist.
func (s *CassandraMetadataService) ReadConsumerGroup(ctx thrift.Context, request *shared.ReadConsumerGroupRequest) (*shared.ConsumerGroupDescription, error) {

	if request.ConsumerGroupName == nil {
		if request.ConsumerGroupUUID != nil {
			return s.ReadConsumerGroupByUUID(ctx, request)
		}
		return nil, &shared.BadRequestError{Message: "ConsumerGroupName cannot be nil"}
	}

	if request.DestinationPath == nil && request.DestinationUUID == nil {
		return nil, &shared.BadRequestError{Message: "Either destinationPath or destinationUUID is required"}
	}

	var dstUUID string

	if request.DestinationPath != nil {
		dstInfo, err := s.ReadDestination(nil, &shared.ReadDestinationRequest{Path: common.StringPtr(request.GetDestinationPath())})
		if err != nil {
			return nil, err
		}
		dstUUID = dstInfo.GetDestinationUUID()
	} else {
		dstUUID = request.GetDestinationUUID()
	}

	return s.readConsumerGroupByDstUUID(dstUUID, request.GetConsumerGroupName())
}

// ReadConsumerGroupByUUID returns the ConsumerGroupDescription for the [consumerGroupUUID].
func (s *CassandraMetadataService) ReadConsumerGroupByUUID(ctx thrift.Context, request *shared.ReadConsumerGroupRequest) (*shared.ConsumerGroupDescription, error) {

	if request.ConsumerGroupUUID == nil {
		return nil, &shared.BadRequestError{Message: "ConsumerGroupUUID cannot be nil"}
	}

	key := "cguuid:" + request.GetConsumerGroupUUID()
	cached := s.consumerGroupCache.Get(key)
	if cached != nil {
		return cached.(*shared.ConsumerGroupDescription), nil
	}

	result := getUtilConsumerGroupDescription()
	var zoneConfigsData []map[string]interface{}
	query := s.session.Query(sqlGetCGByUUID, request.GetConsumerGroupUUID()).Consistency(s.lowConsLevel)
	if err := query.Scan(
		result.ConsumerGroupUUID,
		result.DestinationUUID,
		result.ConsumerGroupName,
		result.StartFrom,
		result.Status,
		result.LockTimeoutSeconds,
		result.MaxDeliveryCount,
		result.SkipOlderMessagesSeconds,
		result.DelaySeconds,
		&result.DeadLetterQueueDestinationUUID,
		result.OwnerEmail,
		result.IsMultiZone,
		result.ActiveZone,
		&zoneConfigsData,
		&result.Options); err != nil {
		if err == gocql.ErrNotFound {
			return nil, &shared.EntityNotExistsError{
				Message: fmt.Sprintf("ConsumerGroup %s does not exist", *request.ConsumerGroupUUID),
			}
		}

		return nil, &shared.InternalServiceError{
			Message: err.Error(),
		}
	}

	result.ZoneConfigs = unmarshalCgZoneConfigs(zoneConfigsData)
	s.consumerGroupCache.Put(key, result)
	if result.DestinationUUID != nil && result.ConsumerGroupName != nil {
		s.consumerGroupCache.Put("dstuuid_cgname:"+*result.DestinationUUID+*result.ConsumerGroupName, result)
	}
	return result, nil
}

func updateCGDescIfChanged(req *shared.UpdateConsumerGroupRequest, cgDesc *shared.ConsumerGroupDescription) bool {
	isChanged := false

	// Note: for any updatable property here, we also need to reconcile it in replicator
	if req.IsSetLockTimeoutSeconds() && req.GetLockTimeoutSeconds() != cgDesc.GetLockTimeoutSeconds() {
		isChanged = true
		cgDesc.LockTimeoutSeconds = common.Int32Ptr(req.GetLockTimeoutSeconds())
	}

	if req.IsSetMaxDeliveryCount() && req.GetMaxDeliveryCount() != cgDesc.GetMaxDeliveryCount() {
		isChanged = true
		cgDesc.MaxDeliveryCount = common.Int32Ptr(req.GetMaxDeliveryCount())
	}

	if req.IsSetSkipOlderMessagesSeconds() && req.GetSkipOlderMessagesSeconds() != cgDesc.GetSkipOlderMessagesSeconds() {
		isChanged = true
		cgDesc.SkipOlderMessagesSeconds = common.Int32Ptr(req.GetSkipOlderMessagesSeconds())
	}

	if req.IsSetDelaySeconds() && req.GetDelaySeconds() != cgDesc.GetDelaySeconds() {
		isChanged = true
		cgDesc.DelaySeconds = common.Int32Ptr(req.GetDelaySeconds())
	}

	if req.IsSetStatus() && req.GetStatus() != cgDesc.GetStatus() {
		isChanged = true
		cgDesc.Status = common.InternalConsumerGroupStatusPtr(req.GetStatus())
	}

	if req.IsSetOwnerEmail() && req.GetOwnerEmail() != cgDesc.GetOwnerEmail() {
		isChanged = true
		cgDesc.OwnerEmail = common.StringPtr(req.GetOwnerEmail())
	}

	if req.IsSetActiveZone() && req.GetActiveZone() != cgDesc.GetActiveZone() {
		isChanged = true
		cgDesc.ActiveZone = common.StringPtr(req.GetActiveZone())
	}

	if req.IsSetZoneConfigs() && !common.AreCgZoneConfigsEqual(cgDesc.GetZoneConfigs(), req.GetZoneConfigs()) {
		isChanged = true
		cgDesc.ZoneConfigs = req.GetZoneConfigs()
		cgDesc.IsMultiZone = common.BoolPtr(true)
	}

	if req.IsSetOptions() && !reflect.DeepEqual(req.GetOptions(), cgDesc.GetOptions()) {
		isChanged = true
		cgDesc.Options = req.Options
	}

	return isChanged
}

// UpdateConsumerGroup updates the consumer group information for the given group
// This method can only be called for an existing consumer group
func (s *CassandraMetadataService) UpdateConsumerGroup(ctx thrift.Context, request *shared.UpdateConsumerGroupRequest) (*shared.ConsumerGroupDescription, error) {

	readCGReq := &shared.ReadConsumerGroupRequest{
		DestinationPath:   common.StringPtr(request.GetDestinationPath()),
		ConsumerGroupName: common.StringPtr(request.GetConsumerGroupName()),
	}

	existingCG, err := s.ReadConsumerGroup(nil, readCGReq)
	if err != nil {
		return nil, err
	}

	if existingCG.GetStatus() == shared.ConsumerGroupStatus_DELETING ||
		existingCG.GetStatus() == shared.ConsumerGroupStatus_DELETED {
		return nil, &shared.BadRequestError{
			Message: fmt.Sprintf("UpdateConsumerGroup - Attempt to update DELETED consumer group, dst=%v, cg=%v",
				request.GetDestinationPath(), request.GetConsumerGroupName()),
		}
	}

	if !updateCGDescIfChanged(request, existingCG) {
		return existingCG, nil
	}

	newCG := existingCG
	batch := s.session.NewBatch(gocql.LoggedBatch)

	batch.Query(sqlUpdateCGByUUID,
		// Value columns
		newCG.GetDestinationUUID(),
		newCG.GetIsMultiZone(),
		newCG.GetConsumerGroupUUID(),
		newCG.GetDestinationUUID(),
		newCG.GetConsumerGroupName(),
		newCG.GetStartFrom(),
		newCG.GetStatus(),
		newCG.GetLockTimeoutSeconds(),
		newCG.GetMaxDeliveryCount(),
		newCG.GetSkipOlderMessagesSeconds(),
		newCG.GetDelaySeconds(),
		newCG.DeadLetterQueueDestinationUUID, // May be null
		newCG.GetOwnerEmail(),
		newCG.GetIsMultiZone(),
		newCG.GetActiveZone(),
		marshalCgZoneConfigs(newCG.GetZoneConfigs()),
		newCG.GetOptions(),
		// Query columns
		newCG.GetConsumerGroupUUID())

	batch.Query(sqlUpdateCGByName,
		// Value columns
		newCG.GetIsMultiZone(),
		newCG.GetConsumerGroupUUID(),
		newCG.GetDestinationUUID(),
		newCG.GetConsumerGroupName(),
		newCG.GetStartFrom(),
		newCG.GetStatus(),
		newCG.GetLockTimeoutSeconds(),
		newCG.GetMaxDeliveryCount(),
		newCG.GetSkipOlderMessagesSeconds(),
		newCG.GetDelaySeconds(),
		newCG.DeadLetterQueueDestinationUUID, // May be null
		newCG.GetOwnerEmail(),
		newCG.GetIsMultiZone(),
		newCG.GetActiveZone(),
		marshalCgZoneConfigs(newCG.GetZoneConfigs()),
		newCG.GetOptions(),
		// Query columns
		newCG.GetDestinationUUID(),
		newCG.GetConsumerGroupName())

	batch.Cons = s.highConsLevel

	if err = s.session.ExecuteBatch(batch); err != nil {
		return nil, &shared.InternalServiceError{
			Message: fmt.Sprintf("UpdateConsumerGroup - Batch operation failed, dst=%v cg=%v err=%v",
				request.GetDestinationPath(), request.GetConsumerGroupName(), err),
		}
	}

	callerUserName := getThriftContextValue(ctx, common.CallerUserName)
	callerHostName := getThriftContextValue(ctx, common.CallerHostName)
	callerServiceName := getThriftContextValue(ctx, common.CallerServiceName)

	s.recordUserOperation(
		newCG.GetConsumerGroupName(),
		newCG.GetConsumerGroupUUID(),
		entityTypeCG,
		callerUserName,
		"", //place holder for user's email
		callerServiceName,
		callerHostName,
		opsUpdate,
		time.Now(),
		marshalRequest(request))

	s.consumerGroupCache.Delete("dstuuid_cgname:" + newCG.GetDestinationUUID() + newCG.GetConsumerGroupName())
	s.consumerGroupCache.Delete("cguuid:" + newCG.GetConsumerGroupUUID())

	return newCG, nil
}

// DeleteConsumerGroup deletes the given consumer group, if its not already deleted
// Returns success if the group was previously deleted
// TODO Add TTLs to DELETE commands
func (s *CassandraMetadataService) DeleteConsumerGroup(ctx thrift.Context, request *shared.DeleteConsumerGroupRequest) (e error) {

	if request.ConsumerGroupName == nil {
		return &shared.BadRequestError{Message: "ConsumerGroupName cannot be nil"}
	}

	if request.DestinationPath == nil && request.DestinationUUID == nil {
		return &shared.BadRequestError{Message: "Both destinationUUID and destinationPath cannot be nil"}
	}

	var existingCG *shared.ConsumerGroupDescription

	if request.DestinationPath != nil {
		readCGReq := &shared.ReadConsumerGroupRequest{
			DestinationPath:   common.StringPtr(request.GetDestinationPath()),
			ConsumerGroupName: common.StringPtr(request.GetConsumerGroupName()),
		}
		existingCG, e = s.ReadConsumerGroup(nil, readCGReq)
		if e != nil {
			return e
		}
	} else {
		existingCG, e = s.readConsumerGroupByDstUUID(request.GetDestinationUUID(), request.GetConsumerGroupName())
		if e != nil {
			return e
		}
	}

	if existingCG.GetStatus() == shared.ConsumerGroupStatus_DELETING ||
		existingCG.GetStatus() == shared.ConsumerGroupStatus_DELETED {
		return nil
	}

	batch := s.session.NewBatch(gocql.LoggedBatch)

	// Every consumer group has an associated DLQ destination
	// that was created at the time CG was created. The deletion
	// of a consumer group should also mark the DLQ destination as
	// DELETED. The following code adds the DLQ destination delete
	// to the batch operation, if there is one.
	dlqDstID := existingCG.GetDeadLetterQueueDestinationUUID()
	// Not all CGs have a DLQ, only do this if there is a DLQ
	if len(dlqDstID) > 0 && dlqDstID != common.ZeroUUID {
		var dlqDstDesc *shared.DestinationDescription
		// this is the same as DeleteDestination()
		readReq := &shared.ReadDestinationRequest{
			Path: common.StringPtr(dlqDstID),
		}
		dlqDstDesc, e = s.ReadDestination(nil, readReq)
		if e != nil {
			return &shared.InternalServiceError{
				Message: fmt.Sprintf("Error reading DLQ destination for consumer group: %v", e),
			}
		}

		batch.Query(
			sqlUpdateDstByUUID,
			dlqDstDesc.GetDestinationUUID(),
			dlqDstDesc.GetPath(),
			dlqDstDesc.GetType(),
			shared.DestinationStatus_DELETING,
			dlqDstDesc.GetConsumedMessagesRetention(),
			dlqDstDesc.GetUnconsumedMessagesRetention(),
			dlqDstDesc.GetOwnerEmail(),
			dlqDstDesc.GetChecksumOption(),
			dlqDstDesc.IsMultiZone,
			marshalDstZoneConfigs(dlqDstDesc.ZoneConfigs),
			dlqDstDesc.IsMultiZone,
			dlqDstDesc.GetDestinationUUID())
	}

	batch.Query(sqlUpdateCGByUUID,
		// Value columns
		existingCG.GetDestinationUUID(),
		existingCG.GetIsMultiZone(),
		existingCG.GetConsumerGroupUUID(),
		existingCG.GetDestinationUUID(),
		existingCG.GetConsumerGroupName(),
		existingCG.GetStartFrom(),
		shared.ConsumerGroupStatus_DELETING,
		existingCG.GetLockTimeoutSeconds(),
		existingCG.GetMaxDeliveryCount(),
		existingCG.GetSkipOlderMessagesSeconds(),
		existingCG.GetDelaySeconds(),
		existingCG.DeadLetterQueueDestinationUUID, // May be Null
		existingCG.GetOwnerEmail(),
		existingCG.GetIsMultiZone(),
		existingCG.GetActiveZone(),
		marshalCgZoneConfigs(existingCG.GetZoneConfigs()),
		existingCG.GetOptions(),
		// Query columns
		existingCG.GetConsumerGroupUUID())

	batch.Query(sqlDeleteCGByName,
		existingCG.GetDestinationUUID(),
		existingCG.GetConsumerGroupName())

	batch.Cons = s.midConsLevel

	if e = s.session.ExecuteBatch(batch); e != nil {
		return &shared.InternalServiceError{
			Message: fmt.Sprintf("DeleteConsumerGroup - Batch operation failed, dst=%v cg=%v err=%v",
				request.GetDestinationPath(), request.GetConsumerGroupName(), e),
		}
	}

	callerUserName := getThriftContextValue(ctx, common.CallerUserName)
	callerHostName := getThriftContextValue(ctx, common.CallerHostName)
	callerServiceName := getThriftContextValue(ctx, common.CallerServiceName)

	s.recordUserOperation(
		existingCG.GetConsumerGroupName(),
		existingCG.GetConsumerGroupUUID(),
		entityTypeCG,
		callerUserName,
		"", //place holder for user's email
		callerServiceName,
		callerHostName,
		opsDelete,
		time.Now(),
		marshalRequest(request))

	s.consumerGroupCache.Delete("dstuuid_cgname:" + existingCG.GetDestinationUUID() + existingCG.GetConsumerGroupName())
	s.consumerGroupCache.Delete("cguuid:" + existingCG.GetConsumerGroupUUID())
	s.destinationCache.Delete("dstuuid:" + dlqDstID)

	return nil
}

// DeleteConsumerGroupUUID deletes the consumer-group corresponding to the given UUID
// from the consumer_groups table
func (s *CassandraMetadataService) DeleteConsumerGroupUUID(ctx thrift.Context, request *m.DeleteConsumerGroupUUIDRequest) error {

	if !request.IsSetUUID() {
		return &shared.BadRequestError{Message: "Missing UUID param"}
	}

	reqReadConsumerGroup := &shared.ReadConsumerGroupRequest{
		ConsumerGroupUUID: common.StringPtr(request.GetUUID()),
	}

	existing, err := s.ReadConsumerGroupByUUID(nil, reqReadConsumerGroup)
	if err != nil {
		return err
	}

	query := s.session.Query(sqlDeleteCGByUUIDWithTTL,
		existing.GetConsumerGroupUUID(),
		existing.GetDestinationUUID(),
		existing.GetIsMultiZone(),
		existing.GetConsumerGroupUUID(),
		existing.GetDestinationUUID(),
		existing.GetConsumerGroupName(),
		existing.GetStartFrom(),
		shared.ConsumerGroupStatus_DELETED,
		existing.GetLockTimeoutSeconds(),
		existing.GetMaxDeliveryCount(),
		existing.GetSkipOlderMessagesSeconds(),
		existing.GetDelaySeconds(),
		existing.DeadLetterQueueDestinationUUID,
		existing.GetOwnerEmail(),
		existing.GetIsMultiZone(),
		existing.GetActiveZone(),
		marshalCgZoneConfigs(existing.GetZoneConfigs()),
		existing.GetOptions(),
		defaultDeleteTTLSeconds).Consistency(s.midConsLevel)

	if err = query.Exec(); err != nil {
		return &shared.InternalServiceError{
			Message: fmt.Sprintf("DeleteConsumerGroupUUID(%v): %v", existing.GetConsumerGroupUUID(), err),
		}
	}

	callerUserName := getThriftContextValue(ctx, common.CallerUserName)
	callerHostName := getThriftContextValue(ctx, common.CallerHostName)
	callerServiceName := getThriftContextValue(ctx, common.CallerServiceName)

	s.recordUserOperation(
		existing.GetConsumerGroupName(),
		existing.GetConsumerGroupUUID(),
		entityTypeCG,
		callerUserName,
		"", //place holder for user's email
		callerServiceName,
		callerHostName,
		opsDelete,
		time.Now(),
		marshalRequest(request))

	s.consumerGroupCache.Delete("dstuuid_cgname:" + existing.GetDestinationUUID() + existing.GetConsumerGroupName())
	s.consumerGroupCache.Delete("cguuid:" + existing.GetConsumerGroupUUID())
	return nil
}

// ListConsumerGroups returns all ConsumerGroups matching the given [destinationPath or dstUUID, consumerGroupName] tuple
// If the dstUUID is given, that will be used. Otherwise, the given path will be resolved into a dstUUID.
// If the ConsumerGroupName parameter is empty, this method will return all ConsumerGroups for the given
// destination path/uuid. The returned value is an implementation of MetadataServiceListConsumerGroupsOutCall interface.
// Callers must repeatedly invoke the Read() operation on the returned type until either an EOF or error is returned.
func (s *CassandraMetadataService) ListConsumerGroups(ctx thrift.Context, request *shared.ListConsumerGroupRequest) (*shared.ListConsumerGroupResult_, error) {

	if request.DestinationPath == nil && request.DestinationUUID == nil {
		return nil, &shared.BadRequestError{
			Message: fmt.Sprintf("DestinatinPath and DestinationUUID both cannot be nil"),
		}
	}

	dstUUID := request.GetDestinationUUID()
	if path := request.DestinationPath; path != nil {
		dstInfo, err := s.ReadDestination(nil, &shared.ReadDestinationRequest{Path: common.StringPtr(*path)})
		if err != nil {
			preMsg := fmt.Sprintf("ListConsumerGroups - destinationUUID lookup failed, dst=%v, cg=%v, err=", request.GetDestinationPath(), request.GetConsumerGroupName())
			switch e := err.(type) {
			case *shared.BadRequestError:
				e.Message = preMsg + e.Message
				return nil, e
			case *shared.EntityNotExistsError:
				e.Message = preMsg + e.Message
				return nil, e
			case *shared.InternalServiceError:
				e.Message = preMsg + e.Message
				return nil, e
			default:
				return nil, err // unexpected error
			}
		}

		dstUUID = dstInfo.GetDestinationUUID()
	}

	var iter *gocql.Iter
	if len(request.GetConsumerGroupName()) > 0 {
		iter = s.session.Query(sqlGetCGByName, dstUUID, request.GetConsumerGroupName()).Consistency(s.lowConsLevel).Iter()
		request.Limit = common.Int64Ptr(1)
	} else {
		// Return all consumer groups if the name is empty
		iter = s.session.Query(sqlListCGsByDestUUID, dstUUID).Consistency(s.lowConsLevel).PageSize(int(request.GetLimit())).PageState(request.PageToken).Iter()
	}

	if iter == nil {
		return nil, &shared.InternalServiceError{
			Message: "Query returned nil iterator",
		}
	}

	result := &shared.ListConsumerGroupResult_{
		ConsumerGroups: []*shared.ConsumerGroupDescription{},
		NextPageToken:  request.PageToken,
	}
	cg := getUtilConsumerGroupDescription()
	var zoneConfigsData []map[string]interface{}
	for iter.Scan(
		cg.ConsumerGroupUUID,
		cg.DestinationUUID,
		cg.ConsumerGroupName,
		cg.StartFrom,
		cg.Status,
		cg.LockTimeoutSeconds,
		cg.MaxDeliveryCount,
		cg.SkipOlderMessagesSeconds,
		cg.DelaySeconds,
		&cg.DeadLetterQueueDestinationUUID,
		cg.OwnerEmail,
		cg.IsMultiZone,
		cg.ActiveZone,
		&zoneConfigsData,
		&cg.Options) {

		// Get a new item within limit
		if cg.GetStatus() == shared.ConsumerGroupStatus_DELETED {
			zoneConfigsData = nil
			continue
		}

		cg.ZoneConfigs = unmarshalCgZoneConfigs(zoneConfigsData)
		result.ConsumerGroups = append(result.ConsumerGroups, cg)
		cg = getUtilConsumerGroupDescription()
		zoneConfigsData = nil
	}

	nextPageToken := iter.PageState()
	result.NextPageToken = make([]byte, len(nextPageToken))
	copy(result.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, &shared.InternalServiceError{
			Message: err.Error(),
		}
	}

	return result, nil
}

// ListConsumerGroupsUUID returns all ConsumerGroups matching the given destination-uuid.
func (s *CassandraMetadataService) ListConsumerGroupsUUID(ctx thrift.Context, request *shared.ListConsumerGroupsUUIDRequest) (*shared.ListConsumerGroupsUUIDResult_, error) {

	if !request.IsSetDestinationUUID() {
		return nil, &shared.BadRequestError{
			Message: fmt.Sprintf("DestinationUUID not specified"),
		}
	}

	dstUUID := request.GetDestinationUUID()

	var iter *gocql.Iter
	iter = s.session.Query(sqlListCGsUUID, dstUUID).Consistency(s.lowConsLevel).PageSize(int(request.GetLimit())).PageState(request.PageToken).Iter()

	if iter == nil {
		return nil, &shared.InternalServiceError{
			Message: "Query returned nil iterator",
		}
	}

	result := &shared.ListConsumerGroupsUUIDResult_{
		ConsumerGroups: []*shared.ConsumerGroupDescription{},
		NextPageToken:  request.PageToken,
	}
	cg := getUtilConsumerGroupDescription()
	var zoneConfigsData []map[string]interface{}
	for iter.Scan(
		cg.ConsumerGroupUUID,
		cg.DestinationUUID,
		cg.ConsumerGroupName,
		cg.StartFrom,
		cg.Status,
		cg.LockTimeoutSeconds,
		cg.MaxDeliveryCount,
		cg.SkipOlderMessagesSeconds,
		cg.DelaySeconds,
		&cg.DeadLetterQueueDestinationUUID,
		cg.OwnerEmail,
		cg.IsMultiZone,
		cg.ActiveZone,
		&zoneConfigsData,
		&cg.Options) {

		// skip over deleted rows
		if cg.GetStatus() == shared.ConsumerGroupStatus_DELETED {
			zoneConfigsData = nil
			continue
		}

		cg.ZoneConfigs = unmarshalCgZoneConfigs(zoneConfigsData)
		result.ConsumerGroups = append(result.ConsumerGroups, cg)
		cg = getUtilConsumerGroupDescription()
		zoneConfigsData = nil
	}

	nextPageToken := iter.PageState()
	result.NextPageToken = make([]byte, len(nextPageToken))
	copy(result.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, &shared.InternalServiceError{
			Message: err.Error(),
		}
	}

	return result, nil
}

// ListAllConsumerGroups returns all ConsumerGroups in ConsumerGroups Table. This API is only used for debuging tool
func (s *CassandraMetadataService) ListAllConsumerGroups(ctx thrift.Context, request *shared.ListConsumerGroupRequest) (*shared.ListConsumerGroupResult_, error) {

	if request.GetLimit() <= 0 {
		return nil, &shared.BadRequestError{
			Message: "ListAllConsumerGroups: non-positive limit is not allowed in pagination endpoint",
		}
	}

	query := s.session.Query(sqlGetCG).Consistency(s.lowConsLevel)
	iter := query.PageSize(int(request.GetLimit())).PageState(request.GetPageToken()).Iter()
	if iter == nil {
		return nil, &shared.InternalServiceError{
			Message: "Query returned nil iterator",
		}
	}

	result := &shared.ListConsumerGroupResult_{
		ConsumerGroups: []*shared.ConsumerGroupDescription{},
		NextPageToken:  request.PageToken,
	}
	cg := getUtilConsumerGroupDescription()
	var zoneConfigsData []map[string]interface{}
	for iter.Scan(
		cg.ConsumerGroupUUID,
		cg.DestinationUUID,
		cg.ConsumerGroupName,
		cg.StartFrom,
		cg.Status,
		cg.LockTimeoutSeconds,
		cg.MaxDeliveryCount,
		cg.SkipOlderMessagesSeconds,
		cg.DelaySeconds,
		&cg.DeadLetterQueueDestinationUUID,
		cg.OwnerEmail,
		cg.IsMultiZone,
		cg.ActiveZone,
		&zoneConfigsData,
		&cg.Options) {

		cg.ZoneConfigs = unmarshalCgZoneConfigs(zoneConfigsData)
		result.ConsumerGroups = append(result.ConsumerGroups, cg)
		cg = getUtilConsumerGroupDescription()
		zoneConfigsData = nil
	}

	nextPageToken := iter.PageState()
	result.NextPageToken = make([]byte, len(nextPageToken))
	copy(result.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, &shared.InternalServiceError{
			Message: err.Error(),
		}
	}

	return result, nil
}

func marshalCgZoneConfigs(configs []*shared.ConsumerGroupZoneConfig) []map[string]interface{} {
	configsData := make([]map[string]interface{}, len(configs))
	for i, config := range configs {
		configsData[i] = map[string]interface{}{
			columnZone:    config.GetZone(),
			columnVisible: config.GetVisible(),
		}
	}
	return configsData
}

func unmarshalCgZoneConfigs(configsData []map[string]interface{}) []*shared.ConsumerGroupZoneConfig {
	configs := make([]*shared.ConsumerGroupZoneConfig, len(configsData))
	for i, configMap := range configsData {
		configs[i] = &shared.ConsumerGroupZoneConfig{
			Zone:    common.StringPtr(toString(configMap[columnZone])),
			Visible: common.BoolPtr(toBool(configMap[columnVisible])),
		}
	}
	return configs
}

const (
	sqlGetOpsByID = `SELECT  ` +
		columnEntityName + "," +
		columnEntityUUID + "," +
		columnEntityType + "," +
		columnInitiatorInfo + `.` + columnUserName + "," +
		columnCallerServiceName + "," +
		columnCallerHostName + "," +
		columnOpsType + "," +
		columnOpsTime + "," +
		columnOpsContent +
		` FROM ` + tableOperationsByEntityUUID +
		` WHERE ` + columnEntityUUID + `=?`

	sqlGetOpsByName = `SELECT  ` +
		columnEntityName + "," +
		columnEntityUUID + "," +
		columnEntityType + "," +
		columnInitiatorInfo + `.` + columnUserName + "," +
		columnCallerServiceName + "," +
		columnCallerHostName + "," +
		columnOpsType + "," +
		columnOpsTime + "," +
		columnOpsContent +
		` FROM ` + tableOperationsByEntityName +
		` WHERE ` + columnEntityName + `=?`
)

// ListEntityOps returns related entity ops auditing information in UserOperation Table. This API is only used for debuging tool
func (s *CassandraMetadataService) ListEntityOps(ctx thrift.Context, request *m.ListEntityOpsRequest) (*m.ListEntityOpsResult_, error) {

	if request.GetLimit() <= 0 {
		return nil, &shared.BadRequestError{
			Message: "ListEntityOps: non-positive limit is not allowed in pagination endpoint",
		}
	}

	var query *gocql.Query

	if len(request.GetEntityUUID()) > 0 {
		query = s.session.Query(sqlGetOpsByID, request.GetEntityUUID()).Consistency(s.lowConsLevel)
	} else if len(request.GetEntityName()) > 0 {
		query = s.session.Query(sqlGetOpsByName, request.GetEntityName()).Consistency(s.lowConsLevel)
	} else {
		return nil, &shared.InternalServiceError{
			Message: "Query returned nil iterator",
		}
	}

	iter := query.PageSize(int(request.GetLimit())).PageState(request.GetPageToken()).Iter()
	if iter == nil {
		return nil, &shared.InternalServiceError{
			Message: "Query returned nil iterator",
		}
	}

	result := &m.ListEntityOpsResult_{
		EntityOps:     []*shared.EntityOpsDescription{},
		NextPageToken: request.PageToken,
	}
	ops := getUtilEntityOpsDescription()
	eventTime := common.TSPtr(time.Now())
	for iter.Scan(
		ops.EntityUUID,
		ops.EntityName,
		ops.EntityType,
		ops.UserName,
		ops.ServiceName,
		ops.HostName,
		ops.OpsType,
		eventTime,
		ops.OpsContent,
	) {
		timeStr := (*eventTime).String()
		ops.OpsTime = &timeStr
		result.EntityOps = append(result.EntityOps, ops)
		ops = getUtilEntityOpsDescription()
	}

	nextPageToken := iter.PageState()
	result.NextPageToken = make([]byte, len(nextPageToken))
	copy(result.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, &shared.InternalServiceError{
			Message: err.Error(),
		}
	}

	return result, nil
}

// CQL commands for Extent CRUD go here
const (
	sqlExtentType = `{` +
		columnUUID + `: ?, ` +
		columnDestinationUUID + `: ?, ` +
		columnStoreUUIDS + `: ?, ` +
		columnInputHostUUID + `: ?, ` +
		columnOriginZone + `: ?, ` +
		columnRemoteExtentPrimaryStore + `: ?, ` +
		columnStatus + `: ?}`

	sqlInsertDstExent = `INSERT INTO ` + tableDestinationExtents + ` (` +
		columnDestinationUUID + `, ` +
		columnExtentUUID + `, ` +
		columnStatus + `, ` +
		columnStatusUpdatedTime + `, ` +
		columnCreatedTime + `, ` +
		columnOriginZone + `, ` +
		columnExtent + `, ` +
		columnReplicaStats + `, ` +
		columnConsumerGroupVisibility + `)` +
		` VALUES (?, ?, ?, ?, ?, ?, ` + sqlExtentType + `, ?, ?)`

	sqlInsertInputExtent = `INSERT INTO ` + tableInputHostExtents + ` (` +
		columnDestinationUUID + `, ` +
		columnInputHostUUID + `, ` +
		columnExtentUUID + `, ` +
		columnStatus + `, ` +
		columnCreatedTime + `, ` +
		columnExtent + `, ` +
		columnReplicaStats + `) ` +
		`VALUES (?, ?, ?, ?, ?, ` + sqlExtentType + `, ?)`

	sqlInsertStoreExent = `INSERT INTO ` + tableStoreExtents + ` (` +
		columnStoreUUID + `, ` +
		columnExtentUUID + `, ` +
		columnStatus + `, ` +
		columnCreatedTime + `, ` +
		columnReplicationStatus + `, ` +
		columnExtent + `, ` +
		columnReplicaStats + `) ` +
		`VALUES (?, ?, ?, ?, ?, ` + sqlExtentType + `, ?)`

	sqlDeleteDstExtent       = sqlInsertDstExent + ` USING TTL ?`
	sqlDeleteInputHostExtent = sqlInsertInputExtent + ` USING TTL ?`
	sqlDeleteStoreExtent     = sqlInsertStoreExent + ` USING TTL ?`

	extentUpdate = `{` +
		columnUUID + `: ?, ` +
		columnDestinationUUID + `: ?, ` +
		columnStoreUUIDS + `: ?, ` +
		columnInputHostUUID + `: ?, ` +
		columnOriginZone + `: ?, ` +
		columnRemoteExtentPrimaryStore + `: ?, ` +
		columnStatus + `: ?, ` +
		columnArchivalLocation + `: ?` +
		`}`

	sqlUpdateDstExtents = `UPDATE ` + tableDestinationExtents +
		` SET ` + columnStatus + ` = ?, ` + columnOriginZone + ` = ?, ` + columnStatusUpdatedTime + ` = ?, ` + columnExtent + ` = ` + extentUpdate +
		` WHERE ` + columnDestinationUUID + `= ? AND ` + columnExtentUUID + `= ?`

	sqlUpdateDstExtentsCGVisibility = `UPDATE ` + tableDestinationExtents +
		` SET ` + columnConsumerGroupVisibility + ` = ? ` +
		` WHERE ` + columnDestinationUUID + `= ? AND ` + columnExtentUUID + `= ?`

	sqlUpdateInputHostExtents = `UPDATE ` + tableInputHostExtents +
		` SET ` + columnStatus + ` = ?, ` + columnExtent + ` = ` + extentUpdate +
		` WHERE ` + columnDestinationUUID + `= ? AND ` + columnInputHostUUID + `= ? AND ` + columnExtentUUID + `= ?`

	sqlUpdateStoreExtents = `UPDATE ` + tableStoreExtents +
		` SET ` + columnStatus + ` = ?, ` + columnExtent + ` = ` + extentUpdate +
		` WHERE ` + columnStoreUUID + `= ? AND ` + columnExtentUUID + `= ?`

	// Use this only for debugging purpose, NEVER use it in real prod running code
	sqlGetExtentStatsByExtentUUID = `SELECT ` + columnCreatedTime + `, ` + columnStatusUpdatedTime + `, ` + columnExtent + `, ` + columnReplicaStats + `, ` + columnConsumerGroupVisibility + ` FROM ` + tableDestinationExtents +
		` WHERE ` + columnExtentUUID + `=? ALLOW FILTERING`

	sqlGetExtentStats = `SELECT ` + columnCreatedTime + `, ` + columnStatusUpdatedTime + `, ` + columnExtent + `, ` + columnReplicaStats + `, ` + columnConsumerGroupVisibility + ` FROM ` + tableDestinationExtents +
		` WHERE ` + columnDestinationUUID + `=? AND ` + columnExtentUUID + `=?`

	sqlListExtentStats = `SELECT ` + columnCreatedTime + `, ` + columnStatusUpdatedTime + `, ` + columnExtent + `, ` + columnReplicaStats + `, ` + columnConsumerGroupVisibility + ` FROM ` + tableDestinationExtents +
		` WHERE ` + columnDestinationUUID + `=?`

	sqlListStoreExtentStats = `SELECT ` + columnCreatedTime + `, ` + columnExtent + `, ` + columnReplicaStats +
		` FROM ` + tableStoreExtents + ` WHERE ` + columnStoreUUID + `=?`

	sqlGetExtentStatsByInputID = `SELECT ` + columnCreatedTime + `, ` + columnExtent + `, ` + columnReplicaStats +
		` FROM ` + tableInputHostExtents +
		` WHERE ` + columnInputHostUUID + ` = ?`

	sqlGetExtentStatsByInputDst = sqlGetExtentStatsByInputID + ` AND ` + columnDestinationUUID + ` = ?`

	sqlGetStoreExtentStatsByStoreAndExtent = `SELECT ` +
		columnCreatedTime + `, ` +
		columnExtent + `, ` +
		columnReplicaStats + `, ` +
		`writeTime( ` + columnReplicaStats + `)` +
		` FROM ` + tableStoreExtents + ` WHERE ` + columnStoreUUID + ` =? AND ` + columnExtentUUID + ` =? LIMIT 1`

	sqlListDstExtents = `SELECT ` + columnExtentUUID + `, ` + columnStatus + `, ` + columnCreatedTime + `, ` +
		columnStatusUpdatedTime + `, ` + columnConsumerGroupVisibility + `, ` + columnOriginZone +
		`, extent.` + columnInputHostUUID + `, extent.` + columnStoreUUIDS +
		` FROM ` + tableDestinationExtents +
		` WHERE ` + columnDestinationUUID + `=?`
)

var errDstUUIDNil = &shared.BadRequestError{Message: "DestinationUUID is nil"}
var errPageLimitOutOfRange = &shared.BadRequestError{Message: "PageLimit out of range, must be > 0"}

// CreateExtent implements the corresponding TChanMetadataServiceClient API
// TODO Have a storage background job to reconcile store view of extents with the metadata
func (s *CassandraMetadataService) CreateExtent(ctx thrift.Context, request *shared.CreateExtentRequest) (*shared.CreateExtentResult_, error) {
	extent := request.GetExtent()
	batch := s.session.NewBatch(gocql.LoggedBatch)
	batch.Cons = s.midConsLevel // Tradeoff between durability and availability

	// map[storeId]map[fieldName]fieldValue
	replicaStatsList := make(map[string]map[string]interface{})
	for i := 0; i < len(extent.StoreUUIDs); i++ {
		replicaStatsList[extent.StoreUUIDs[i]] = map[string]interface{}{
			columnExtentUUID:            extent.ExtentUUID,
			columnStoreUUID:             extent.StoreUUIDs[i],
			columnDestinationUUID:       extent.DestinationUUID,
			columnAvailableAddress:      0,
			columnAvailableSequence:     0,
			columnAvailableSequenceRate: 0.0,
			columnBeginAddress:          0,
			columnLastAddress:           0,
			columnBeginSequence:         0,
			columnLastSequence:          0,
			columnLastSequenceRate:      0.0,
			columnBeginEnqueueTime:      nil,
			columnLastEnqueueTime:       nil,
			columnSizeInBytes:           0,
			columnSizeInBytesRate:       0.0,
			columnStatus:                shared.ExtentReplicaStatus_OPEN,
			columnBeginTime:             nil,
			columnCreatedTime:           nil,
			columnEndTime:               nil,
			columnStore:                 "ManyRocks", // FIXME: hardcoded for now
			columnStoreVersion:          "0.2",       // FIXME: hardcoded for now
		}
	}

	var consumerGroupVisibility *string
	if len(request.GetConsumerGroupVisibility()) > 0 {
		consumerGroupVisibility = common.StringPtr(request.GetConsumerGroupVisibility())
	}
	epochMillisNow := s.createExtentImpl(extent, shared.ExtentStatus_OPEN, replicaStatsList, consumerGroupVisibility, batch)
	if err := s.session.ExecuteBatch(batch); err != nil {
		return nil, &shared.InternalServiceError{
			Message: "CreateExtent: " + err.Error(),
		}
	}
	return &shared.CreateExtentResult_{
		ExtentStats: &shared.ExtentStats{
			Extent:                  extent,
			CreatedTimeMillis:       common.Int64Ptr(epochMillisNow),
			Status:                  common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN),
			StatusUpdatedTimeMillis: common.Int64Ptr(epochMillisNow),
		},
	}, nil
}

func (s *CassandraMetadataService) createExtentImpl(extent *shared.Extent, extentStatus shared.ExtentStatus, replicaStatsList map[string]map[string]interface{}, consumerGroupVisibility *string, batch *gocql.Batch) (epochMillisNow int64) {
	epochMillisNow = timeToMilliseconds(time.Now())

	batch.Query(
		sqlInsertDstExent,
		extent.GetDestinationUUID(),
		extent.GetExtentUUID(),
		extentStatus,
		epochMillisNow,
		epochMillisNow,
		extent.GetOriginZone(),
		extent.GetExtentUUID(),
		extent.GetDestinationUUID(),
		extent.GetStoreUUIDs(),
		extent.GetInputHostUUID(),
		extent.GetOriginZone(),
		extent.GetRemoteExtentPrimaryStore(),
		extentStatus,
		replicaStatsList,
		consumerGroupVisibility,
	)

	batch.Query(
		sqlInsertInputExtent,
		extent.GetDestinationUUID(),
		extent.GetInputHostUUID(),
		extent.GetExtentUUID(),
		extentStatus,
		epochMillisNow,
		extent.GetExtentUUID(),
		extent.GetDestinationUUID(),
		extent.GetStoreUUIDs(),
		extent.GetInputHostUUID(),
		extent.GetOriginZone(),
		extent.GetRemoteExtentPrimaryStore(),
		extentStatus,
		replicaStatsList,
	)

	replicationStatus := shared.ExtentReplicaReplicationStatus_INVALID
	if len(extent.GetOriginZone()) > 0 {
		replicationStatus = shared.ExtentReplicaReplicationStatus_PENDING
	}
	for storeID, replicaStats := range replicaStatsList {
		batch.Query(
			sqlInsertStoreExent,
			storeID,
			extent.GetExtentUUID(),
			extentStatus,
			epochMillisNow,
			replicationStatus,
			extent.GetExtentUUID(),
			extent.GetDestinationUUID(),
			extent.GetStoreUUIDs(),
			extent.GetInputHostUUID(),
			extent.GetOriginZone(),
			extent.GetRemoteExtentPrimaryStore(),
			extentStatus,
			replicaStats,
		)
	}

	return
}

func (s *CassandraMetadataService) deleteExtent(dstUUID string, extentUUID string) (*shared.ExtentStats, error) {
	// First read all columns and then insert the same columns with TTL
	query := s.session.Query(sqlGetExtentStats).Consistency(s.midConsLevel)
	query.Bind(dstUUID, extentUUID)
	var createdTime time.Time
	var statusUpdatedTime time.Time
	extentMap := make(map[string]interface{})
	extentStatsMap := make(map[string]map[string]interface{})
	var consumerGroupVisibilityUUID string
	if err := query.Scan(&createdTime, &statusUpdatedTime, &extentMap, &extentStatsMap, &consumerGroupVisibilityUUID); err != nil {
		return nil, &shared.InternalServiceError{
			Message: "deleteExtent: failed to read extent stats " + err.Error(),
		}
	}
	resultExtentID := extentMap[columnUUID].(gocql.UUID).String()
	if resultExtentID != extentUUID {
		return nil, &shared.InternalServiceError{
			Message: fmt.Sprintf("deleteExtent: request.extentUUID (%v) != result.extentUUID (%v)",
				extentUUID, resultExtentID),
		}
	}

	extentStats := convertExtentStats(extentMap, extentStatsMap)
	extentStats.CreatedTimeMillis = common.Int64Ptr(timeToMilliseconds(createdTime))
	extentStats.StatusUpdatedTimeMillis = common.Int64Ptr(timeToMilliseconds(statusUpdatedTime))
	if len(consumerGroupVisibilityUUID) > 0 {
		extentStats.ConsumerGroupVisibility = common.StringPtr(consumerGroupVisibilityUUID)
	}
	extent := extentStats.GetExtent()

	if extentStats.GetStatus() == shared.ExtentStatus_DELETED {
		return extentStats, nil
	}

	batch := s.session.NewBatch(gocql.LoggedBatch)
	batch.Cons = s.midConsLevel // Tradeoff between durability and availability

	s.deleteExtentImpl(extent, extentStatsMap, extentStats, batch, false /* ! forMove */)
	if err := s.session.ExecuteBatch(batch); err != nil {
		return nil, &shared.InternalServiceError{
			Message: "deleteExtent: " + err.Error(),
		}
	}
	return extentStats, nil
}

func (s *CassandraMetadataService) deleteExtentImpl(extent *shared.Extent, extentStatsMap map[string]map[string]interface{}, extentStats *shared.ExtentStats, batch *gocql.Batch, forMove bool) {
	batch.Query(
		sqlDeleteDstExtent,
		extent.GetDestinationUUID(),
		extent.GetExtentUUID(),
		shared.ExtentStatus_DELETED,
		timeToMilliseconds(time.Now()),
		extentStats.GetCreatedTimeMillis(),
		extent.GetOriginZone(),
		extent.GetExtentUUID(),
		extent.GetDestinationUUID(),
		extent.GetStoreUUIDs(),
		extent.GetInputHostUUID(),
		extent.GetOriginZone(),
		extent.GetRemoteExtentPrimaryStore(),
		shared.ExtentStatus_DELETED,
		extentStatsMap,
		extentStats.ConsumerGroupVisibility,
		deleteExtentTTLSeconds,
	)

	batch.Query(
		sqlDeleteInputHostExtent,
		extent.GetDestinationUUID(),
		extent.GetInputHostUUID(),
		extent.GetExtentUUID(),
		shared.ExtentStatus_DELETED,
		extentStats.GetCreatedTimeMillis(),
		extent.GetExtentUUID(),
		extent.GetDestinationUUID(),
		extent.GetStoreUUIDs(),
		extent.GetInputHostUUID(),
		extent.GetOriginZone(),
		extent.GetRemoteExtentPrimaryStore(),
		shared.ExtentStatus_DELETED,
		extentStatsMap,
		deleteExtentTTLSeconds,
	)

	if !forMove { // Shouldn't mark store extents as deleted for move extent
		for storeID, replicaStats := range extentStatsMap {
			batch.Query(
				sqlDeleteStoreExtent,
				storeID,
				extent.GetExtentUUID(),
				shared.ExtentStatus_DELETED,
				extentStats.GetCreatedTimeMillis(),
				shared.ExtentReplicaReplicationStatus_INVALID,
				extent.GetExtentUUID(),
				extent.GetDestinationUUID(),
				extent.GetStoreUUIDs(),
				extent.GetInputHostUUID(),
				extent.GetOriginZone(),
				extent.GetRemoteExtentPrimaryStore(),
				shared.ExtentStatus_DELETED,
				replicaStats,
				deleteExtentTTLSeconds,
			)
		}
	}
}

func (s *CassandraMetadataService) updateExtent(extentStats *shared.ExtentStats, newArchivalLocation string, newStatus shared.ExtentStatus, statusUpdatedTimeMillis int64) error {

	batch := s.session.NewBatch(gocql.LoggedBatch)
	batch.Cons = s.midConsLevel

	extent := extentStats.GetExtent()

	batch.Query(
		sqlUpdateDstExtents,
		newStatus,
		extent.GetOriginZone(),
		statusUpdatedTimeMillis,
		extent.GetExtentUUID(),
		extent.GetDestinationUUID(),
		extent.GetStoreUUIDs(),
		extent.GetInputHostUUID(),
		extent.GetOriginZone(),
		extent.GetRemoteExtentPrimaryStore(),
		newStatus,
		newArchivalLocation,
		extent.GetDestinationUUID(),
		extent.GetExtentUUID(),
	)

	batch.Query(
		sqlUpdateInputHostExtents,
		newStatus,
		extent.GetExtentUUID(),
		extent.GetDestinationUUID(),
		extent.GetStoreUUIDs(),
		extent.GetInputHostUUID(),
		extent.GetOriginZone(),
		extent.GetRemoteExtentPrimaryStore(),
		newStatus,
		newArchivalLocation,
		extent.GetDestinationUUID(),
		extent.GetInputHostUUID(),
		extent.GetExtentUUID(),
	)

	for _, storeID := range extent.GetStoreUUIDs() {
		batch.Query(
			sqlUpdateStoreExtents,
			newStatus,
			extent.GetExtentUUID(),
			extent.GetDestinationUUID(),
			extent.GetStoreUUIDs(),
			extent.GetInputHostUUID(),
			extent.GetOriginZone(),
			extent.GetRemoteExtentPrimaryStore(),
			newStatus,
			newArchivalLocation,
			storeID,
			extent.GetExtentUUID(),
		)
	}
	if err := s.session.ExecuteBatch(batch); err != nil {
		return &shared.InternalServiceError{
			Message: "updateExtent: " + err.Error(),
		}
	}
	return nil
}

// UpdateExtentStats implements the corresponding TChanMetadataServiceClient API
func (s *CassandraMetadataService) UpdateExtentStats(ctx thrift.Context, request *m.UpdateExtentStatsRequest) (*m.UpdateExtentStatsResult_, error) {

	if request.GetStatus() == shared.ExtentStatus_DELETED {
		stats, err := s.deleteExtent(request.GetDestinationUUID(), request.GetExtentUUID())
		if err != nil {
			return nil, err
		}
		return &m.UpdateExtentStatsResult_{ExtentStats: stats}, nil
	}

	readExtentStats := &m.ReadExtentStatsRequest{DestinationUUID: common.StringPtr(request.GetDestinationUUID()), ExtentUUID: common.StringPtr(request.GetExtentUUID())}
	extentStatsResult, err := s.ReadExtentStats(nil, readExtentStats)
	if err != nil {
		return nil, &shared.InternalServiceError{
			Message: "UpdateExtentStats read: " + err.Error(),
		}
	}

	extent := extentStatsResult.ExtentStats.Extent
	if request.IsSetRemoteExtentPrimaryStore() {
		extent.RemoteExtentPrimaryStore = common.StringPtr(request.GetRemoteExtentPrimaryStore())
	}

	if len(request.GetArchivalLocation()) == 0 {
		request.ArchivalLocation = common.StringPtr(extentStatsResult.ExtentStats.GetArchivalLocation())
	}

	var statusUpdatedTimeMillis = timeToMilliseconds(time.Now()) // change status update time

	if request.GetStatus() == shared.ExtentStatus_OPEN {
		request.Status = common.MetadataExtentStatusPtr(extentStatsResult.ExtentStats.GetStatus())

		// no change in status; leave the statusUpdatedTimeMillis unchanged
		statusUpdatedTimeMillis = extentStatsResult.ExtentStats.GetStatusUpdatedTimeMillis()
	}

	err = s.updateExtent(extentStatsResult.GetExtentStats(), request.GetArchivalLocation(), request.GetStatus(), statusUpdatedTimeMillis)
	if err != nil {
		return nil, err
	}

	return &m.UpdateExtentStatsResult_{
		ExtentStats: &shared.ExtentStats{
			Extent:                  extent,
			Status:                  common.MetadataExtentStatusPtr(request.GetStatus()),
			StatusUpdatedTimeMillis: common.Int64Ptr(statusUpdatedTimeMillis),
			CreatedTimeMillis:       extentStatsResult.ExtentStats.CreatedTimeMillis,
			ReplicaStats:            extentStatsResult.ExtentStats.ReplicaStats,
			ArchivalLocation:        common.StringPtr(request.GetArchivalLocation()),
		},
	}, nil
}

func toString(i interface{}) string {
	// if the interface is nil, bail immediately
	if i == nil {
		return ""
	}
	return i.(string)
}

func toUUIDString(i interface{}) string {
	// if the interface is nil, bail immediately
	if i == nil {
		return ""
	}
	return i.(gocql.UUID).String()
}

func toBool(i interface{}) bool {
	// if the interface is nil, bail immediately
	if i == nil {
		return false
	}
	return i.(bool)
}

func toInt(i interface{}) int {
	// if the interface is nil, bail immediately
	if i == nil {
		return 0
	}
	return i.(int)
}

func toInt32(i interface{}) int32 {
	// if the interface is nil, bail immediately
	if i == nil {
		return 0
	}
	return i.(int32)
}

func toInt64(i interface{}) int64 {
	// if the interface is nil, bail immediately
	if i == nil {
		return 0
	}
	return i.(int64)
}

func toFloat64(i interface{}) float64 {
	// if the interface is nil, bail immediately
	if i == nil {
		return 0
	}
	return i.(float64)
}

func timeToMilliseconds(i interface{}) int64 {
	// if the interface is nil, bail immediately
	if i == nil {
		return 0
	}
	t := i.(time.Time)
	if t.IsZero() {
		return 0
	}
	return t.UnixNano() / int64(time.Millisecond)
}

func timeToUnixNano(i interface{}) int64 {
	// if the interface is nil, bail immediately
	if i == nil {
		return 0
	}
	t := i.(time.Time)
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func cqlTimestampToUnixNano(milliseconds int64) common.UnixNanoTime {
	return common.UnixNanoTime(milliseconds * 1000 * 1000) // Milliseconds are 10⁻³, nanoseconds are 10⁻⁹, (-3) - (-9) = 6, so multiply by 10⁶
}

func unixNanoToCQLTimestamp(time common.UnixNanoTime) int64 {
	return int64(time) / (1000 * 1000) // Milliseconds are 10⁻³, nanoseconds are 10⁻⁹, (-9) - (-3) = -6, so divide by 10⁶
}

func writeTimeToUnixNano(microseconds int64) common.UnixNanoTime {
	return common.UnixNanoTime(microseconds * 1000)
}

func uuidSliceToStringSlice(i interface{}) []string {
	// if the interface is nil, bail immediately
	if i == nil {
		return []string{}
	}
	uuids := i.([]gocql.UUID)
	result := make([]string, len(uuids))
	for i := 0; i < len(uuids); i++ {
		result[i] = uuids[i].String()
	}
	return result
}

func convertExtentStats(extentMap map[string]interface{}, extentStatsMap map[string]map[string]interface{}) *shared.ExtentStats {
	result := &shared.ExtentStats{
		Extent: &shared.Extent{
			ExtentUUID:               common.StringPtr(toUUIDString(extentMap[columnUUID])),
			DestinationUUID:          common.StringPtr(toUUIDString(extentMap[columnDestinationUUID])),
			StoreUUIDs:               uuidSliceToStringSlice(extentMap[columnStoreUUIDS]),
			InputHostUUID:            common.StringPtr(toUUIDString(extentMap[columnInputHostUUID])),
			OriginZone:               common.StringPtr(toString(extentMap[columnOriginZone])),
			RemoteExtentPrimaryStore: common.StringPtr(toString(extentMap[columnRemoteExtentPrimaryStore])),
		},
		Status:           common.MetadataExtentStatusPtr(shared.ExtentStatus(toInt(extentMap[columnStatus]))),
		ArchivalLocation: common.StringPtr(toString(extentMap[columnArchivalLocation])),
	}

	result.ReplicaStats = make([]*shared.ExtentReplicaStats, len(extentStatsMap))
	i := 0
	for storeID, r := range extentStatsMap {
		result.ReplicaStats[i] = convertReplicaStatsMap(r)
		if result.ReplicaStats[i].GetStoreUUID() == `` {
			result.ReplicaStats[i].StoreUUID = common.StringPtr(storeID)
		}
		i++
	}

	return result
}

func convertReplicaStatsMap(r map[string]interface{}) *shared.ExtentReplicaStats {
	return &shared.ExtentReplicaStats{
		AvailableAddress:      common.Int64Ptr(toInt64(r[columnAvailableAddress])),
		AvailableSequence:     common.Int64Ptr(toInt64(r[columnAvailableSequence])),
		AvailableSequenceRate: common.Float64Ptr(toFloat64(r[columnAvailableSequenceRate])),
		BeginAddress:          common.Int64Ptr(toInt64(r[columnBeginAddress])),
		BeginEnqueueTimeUtc:   common.Int64Ptr(timeToUnixNano(r[columnBeginEnqueueTime])),
		BeginSequence:         common.Int64Ptr(toInt64(r[columnBeginSequence])),
		BeginTime:             common.Int64Ptr(timeToUnixNano(r[columnBeginTime])),
		CreatedAt:             common.Int64Ptr(timeToMilliseconds(r[columnCreatedTime])),
		/* DestinationUUID is in the CQL but not the thrift */
		EndTime:            common.Int64Ptr(timeToUnixNano(r[columnEndTime])),
		ExtentUUID:         common.StringPtr(toUUIDString(r[columnExtentUUID])),
		LastAddress:        common.Int64Ptr(toInt64(r[columnLastAddress])),
		LastEnqueueTimeUtc: common.Int64Ptr(timeToUnixNano(r[columnLastEnqueueTime])),
		LastSequence:       common.Int64Ptr(toInt64(r[columnLastSequence])),
		LastSequenceRate:   common.Float64Ptr(toFloat64(r[columnLastSequenceRate])),
		SizeInBytes:        common.Int64Ptr(toInt64(r[columnSizeInBytes])),
		SizeInBytesRate:    common.Float64Ptr(toFloat64(r[columnSizeInBytesRate])),
		Status:             common.MetadataExtentReplicaStatusPtr(shared.ExtentReplicaStatus(toInt(r[columnStatus]))),
		StoreUUID:          common.StringPtr(toUUIDString(r[columnStoreUUID])),
	}
}

func makeReplicaStatsMap(rs []*shared.ExtentReplicaStats, destUUID string) map[string]map[string]interface{} {
	replicaStatsList := make(map[string]map[string]interface{})

	for _, r := range rs {
		replicaStatsList[r.GetStoreUUID()] = map[string]interface{}{
			columnExtentUUID:            r.GetExtentUUID(),
			columnStoreUUID:             r.GetStoreUUID(),
			columnDestinationUUID:       destUUID,
			columnAvailableAddress:      r.GetAvailableAddress(),
			columnAvailableSequence:     r.GetAvailableSequence(),
			columnAvailableSequenceRate: r.GetAvailableSequenceRate(),
			columnBeginAddress:          r.GetBeginAddress(),
			columnLastAddress:           r.GetLastAddress(),
			columnBeginSequence:         r.GetBeginSequence(),
			columnLastSequence:          r.GetLastSequence(),
			columnLastSequenceRate:      r.GetLastSequenceRate(),
			columnBeginEnqueueTime:      time.Unix(0, r.GetBeginEnqueueTimeUtc()),
			columnLastEnqueueTime:       time.Unix(0, r.GetLastEnqueueTimeUtc()),
			columnSizeInBytes:           r.GetSizeInBytes(),
			columnSizeInBytesRate:       r.GetSizeInBytesRate(),
			columnStatus:                r.GetStatus(),
			columnBeginTime:             time.Unix(0, r.GetBeginTime()),
			columnCreatedTime:           r.GetCreatedAt(),
			columnEndTime:               time.Unix(0, r.GetEndTime()),
			columnStore:                 "ManyRocks", // FIXME: hardcoded for now
			columnStoreVersion:          "0.2",       // FIXME: hardcoded for now
		}
	}
	return replicaStatsList
}

// ReadExtentStats implements the corresponding TChanMetadataServiceClient API
// If DestinationUUID is empty, this API uses a SQL query with 'ALLOW FILTERING' which is not scalable for production.
func (s *CassandraMetadataService) ReadExtentStats(ctx thrift.Context, request *m.ReadExtentStatsRequest) (*m.ReadExtentStatsResult_, error) {
	if len(request.GetExtentUUID()) == 0 {
		return nil, &shared.BadRequestError{
			Message: "ExtentUUID not set",
		}
	}

	var query *gocql.Query
	if len(request.GetDestinationUUID()) == 0 {
		if !common.IsDevelopmentEnvironment(s.clusterName) {
			s.log.WithField(common.TagExt, common.FmtExt(request.GetExtentUUID())).
				Error(`ReadExtentStats: ALLOW FILTERING being used in production`)
		}
		query = s.session.Query(sqlGetExtentStatsByExtentUUID).Consistency(s.lowConsLevel)
		query.Bind(request.GetExtentUUID())
	} else {
		query = s.session.Query(sqlGetExtentStats).Consistency(s.lowConsLevel)
		query.Bind(request.GetDestinationUUID(), request.GetExtentUUID())
	}

	var createdTime time.Time
	var statusUpdatedTime time.Time
	extentMap := make(map[string]interface{})
	extentStatsMap := make(map[string]map[string]interface{})
	var consumerGroupVisibilityUUID string
	if err := query.Scan(&createdTime, &statusUpdatedTime, &extentMap, &extentStatsMap, &consumerGroupVisibilityUUID); err != nil {
		return nil, &shared.InternalServiceError{
			Message: "ReadExtentStats: " + err.Error(),
		}
	}
	resultExtentID := extentMap[columnUUID].(gocql.UUID).String()
	if resultExtentID != request.GetExtentUUID() {
		return nil, &shared.InternalServiceError{
			Message: fmt.Sprintf("ReadExtentStats: request.extentUUID (%v) != result.extentUUID (%v)",
				request.GetExtentUUID(), resultExtentID),
		}
	}
	result := convertExtentStats(extentMap, extentStatsMap)
	result.CreatedTimeMillis = common.Int64Ptr(timeToMilliseconds(createdTime))
	result.StatusUpdatedTimeMillis = common.Int64Ptr(timeToMilliseconds(statusUpdatedTime))
	if len(consumerGroupVisibilityUUID) > 0 {
		result.ConsumerGroupVisibility = common.StringPtr(consumerGroupVisibilityUUID)
	}

	return &m.ReadExtentStatsResult_{ExtentStats: result}, nil
}

func (s *CassandraMetadataService) extractExtentsStats(query *gocql.Query, filterByStatus *shared.ExtentStatus) ([]*shared.ExtentStats, error) {
	iter := query.Iter()
	var createdTime time.Time
	extentMap := make(map[string]interface{})
	extentStatsMap := make(map[string]map[string]interface{})
	extentStatsList := []*shared.ExtentStats{}
	for iter.Scan(&createdTime, &extentMap, &extentStatsMap) {
		extentStats := convertExtentStats(extentMap, extentStatsMap)
		extentStats.CreatedTimeMillis = common.Int64Ptr(timeToMilliseconds(createdTime))
		if filterByStatus != nil && extentStats.GetStatus() != *filterByStatus {
			continue
		}
		extentStatsList = append(extentStatsList, extentStats)
	}

	if err := iter.Close(); err != nil {
		return nil, &shared.InternalServiceError{
			Message: err.Error(),
		}
	}

	return extentStatsList, nil
}

// ListExtentsStats implements the corresponding TChanMetadataServiceClient API
func (s *CassandraMetadataService) ListExtentsStats(ctx thrift.Context, request *shared.ListExtentsStatsRequest) (*shared.ListExtentsStatsResult_, error) {
	if request.GetLimit() <= 0 {
		return nil, &shared.BadRequestError{
			Message: "ListExtentsStats: non-positive limit is not allowed in pagination endpoint",
		}
	}

	filterLocally := false
	// Don't default to filtering by the secondary
	// index field `status` on the WHERE clause.
	// Cassandra is inefficient w.r.t predicates
	// on secondary index, when the number of
	// entries that map to a single index value
	// are a lot (> 10). So, only use secondary
	// index for filtering by OPEN status
	if request.IsSetStatus() && request.GetStatus() != shared.ExtentStatus_OPEN {
		filterLocally = true
	}

	iter, err := s.listExtentsStatsHelper(request, filterLocally)
	if err != nil {
		return nil, err
	}

	var result = &shared.ListExtentsStatsResult_{
		ExtentStatsList: []*shared.ExtentStats{},
		NextPageToken:   request.PageToken,
	}

	var createdTime time.Time
	var statusUpdatedTime time.Time
	extentMap := make(map[string]interface{})
	extentStatsMap := make(map[string]map[string]interface{})
	var consumerGroupVisibilityUUID string
	for iter.Scan(&createdTime, &statusUpdatedTime, &extentMap, &extentStatsMap, &consumerGroupVisibilityUUID) {
		// Get a new item within limit
		extentStats := convertExtentStats(extentMap, extentStatsMap)
		extentStats.CreatedTimeMillis = common.Int64Ptr(timeToMilliseconds(createdTime))
		extentStats.StatusUpdatedTimeMillis = common.Int64Ptr(timeToMilliseconds(statusUpdatedTime))
		if len(consumerGroupVisibilityUUID) > 0 {
			extentStats.ConsumerGroupVisibility = common.StringPtr(consumerGroupVisibilityUUID)
		}
		if filterLocally && extentStats.GetStatus() != request.GetStatus() {
			continue
		}
		result.ExtentStatsList = append(result.ExtentStatsList, extentStats)
		extentMap = make(map[string]interface{})
		extentStatsMap = make(map[string]map[string]interface{})
	}

	nextPageToken := iter.PageState()
	result.NextPageToken = make([]byte, len(nextPageToken))
	copy(result.NextPageToken, nextPageToken)

	if err = iter.Close(); err != nil {
		return nil, &shared.InternalServiceError{
			Message: err.Error(),
		}
	}

	return result, nil
}

func (s *CassandraMetadataService) listExtentsStatsHelper(request *shared.ListExtentsStatsRequest, ignoreStatusFilter bool) (*gocql.Iter, error) {
	if len(request.GetDestinationUUID()) == 0 {
		return nil, &shared.BadRequestError{
			Message: "DestinationUUID not set",
		}
	}

	sql := sqlListExtentStats
	if !ignoreStatusFilter && request.IsSetStatus() {
		sql = fmt.Sprintf("%s AND %s=%d", sqlListExtentStats, columnStatus, request.GetStatus())
	}
	if request.GetLocalExtentsOnly() {
		sql = fmt.Sprintf("%s AND %s=%s", sql, columnOriginZone, `''`)
	}
	query := s.session.Query(sql).Consistency(s.lowConsLevel)
	query.Bind(request.GetDestinationUUID())

	// apply limit and page token only for positive limit (backward compatibility)
	if request.GetLimit() > 0 {
		query = query.PageSize(int(request.GetLimit())).PageState(request.GetPageToken())
	}
	return query.Iter(), nil
}

// ListDestinationExtents lists all the extents mapped to a given destination
func (s *CassandraMetadataService) ListDestinationExtents(ctx thrift.Context, request *m.ListDestinationExtentsRequest) (*m.ListDestinationExtentsResult_, error) {

	if len(request.GetDestinationUUID()) == 0 {
		return nil, errDstUUIDNil
	}

	if request.GetLimit() <= 0 {
		return nil, errPageLimitOutOfRange
	}

	filterLocally := false
	if request.IsSetStatus() && request.GetStatus() != shared.ExtentStatus_OPEN {
		filterLocally = true
	}

	sql := sqlListDstExtents
	if !filterLocally && request.IsSetStatus() {
		sql = fmt.Sprintf("%s AND %s=%d", sqlListDstExtents, columnStatus, request.GetStatus())
	}

	qry := s.session.Query(sql).Consistency(s.lowConsLevel).Bind(request.GetDestinationUUID())
	qry = qry.PageSize(int(request.GetLimit())).PageState(request.GetPageToken())

	iter := qry.Iter()

	allocSize := iter.NumRows()
	if filterLocally {
		allocSize = common.MinInt(allocSize, 8)
	}

	allocSize = common.MaxInt(allocSize, 1)

	result := m.NewListDestinationExtentsResult_()
	result.Extents = make([]*m.DestinationExtent, 0, allocSize)
	extents := make([]m.DestinationExtent, allocSize)

	var cnt int
	var createdTime time.Time
	var statusUpdatedTime time.Time

	for iter.Scan(&extents[cnt].ExtentUUID,
		&extents[cnt].Status,
		&createdTime,
		&statusUpdatedTime,
		&extents[cnt].ConsumerGroupVisibility,
		&extents[cnt].OriginZone,
		&extents[cnt].InputHostUUID,
		&extents[cnt].StoreUUIDs) {

		if filterLocally && request.GetStatus() != extents[cnt].GetStatus() {
			continue
		}

		extents[cnt].CreatedTimeMillis = common.Int64Ptr(timeToMilliseconds(createdTime))
		extents[cnt].StatusUpdatedTimeMillis = common.Int64Ptr(timeToMilliseconds(statusUpdatedTime))
		result.Extents = append(result.Extents, &extents[cnt])

		cnt++
		if cnt == len(extents) {
			cnt = 0
			extents = make([]m.DestinationExtent, allocSize)
		}
	}

	nextPageToken := iter.PageState()
	result.NextPageToken = make([]byte, len(nextPageToken))
	copy(result.NextPageToken, nextPageToken)

	if err := iter.Close(); err != nil {
		return nil, &shared.InternalServiceError{
			Message: err.Error(),
		}
	}

	return result, nil
}

// ListInputHostExtentsStats returns a list of extent stats for the given DstID/InputHostID
// If the destinationID is not specified, this method will return all extent stats matching
// the given input host id
func (s *CassandraMetadataService) ListInputHostExtentsStats(ctx thrift.Context, request *m.ListInputHostExtentsStatsRequest) (*m.ListInputHostExtentsStatsResult_, error) {

	if len(request.GetInputHostUUID()) == 0 {
		return nil, &shared.BadRequestError{
			Message: "InputHostUUID cannot be nil",
		}
	}

	var sqlStatus string
	var filterByStatus *shared.ExtentStatus
	if request.IsSetStatus() {
		// Don't default to filtering by the secondary
		// index field `status` on the WHERE clause.
		// Cassandra is inefficient w.r.t predicates
		// on secondary index, when the number of
		// entries that map to a single index value
		// are a lot (> 10). So, only use secondary
		// index for filtering by OPEN status
		if request.GetStatus() == shared.ExtentStatus_OPEN {
			sqlStatus = fmt.Sprintf(" AND %s=%d", columnStatus, request.GetStatus())
		} else {
			filterByStatus = request.Status
		}
	}

	var query *gocql.Query
	if len(request.GetDestinationUUID()) > 0 {
		query = s.session.Query(sqlGetExtentStatsByInputDst + sqlStatus).Consistency(s.lowConsLevel)
		query.Bind(request.GetInputHostUUID(), request.GetDestinationUUID())
	} else {
		query = s.session.Query(sqlGetExtentStatsByInputID + sqlStatus).Consistency(s.lowConsLevel)
		query.Bind(request.GetInputHostUUID())
	}

	extentStats, err := s.extractExtentsStats(query, filterByStatus)
	if err != nil {
		return nil, err
	}
	return &m.ListInputHostExtentsStatsResult_{ExtentStatsList: extentStats}, nil
}

// ListStoreExtentsStats implements the corresponding TChanMetadataServiceClient API
func (s *CassandraMetadataService) ListStoreExtentsStats(ctx thrift.Context, request *m.ListStoreExtentsStatsRequest) (*m.ListStoreExtentsStatsResult_, error) {
	if len(request.GetStoreUUID()) == 0 {
		return nil, &shared.BadRequestError{
			Message: "StoreUUID not specified",
		}
	}

	filterLocally := false
	sql := sqlListStoreExtentStats
	// Don't default to filtering by the secondary
	// index field `status` on the WHERE clause.
	// Cassandra is inefficient w.r.t predicates
	// on secondary index, when the number of
	// entries that map to a single index value
	// are a lot (> 10). So, only use secondary
	// index for filtering by OPEN status
	if request.IsSetStatus() {
		if request.GetStatus() == shared.ExtentStatus_OPEN {
			sql += fmt.Sprintf(" AND %s=%d", columnStatus, request.GetStatus())
		} else {
			filterLocally = true
		}
	}
	if request.IsSetReplicationStatus() {
		sql += fmt.Sprintf(" AND %s=%d", columnReplicationStatus, request.GetReplicationStatus())
	}

	query := s.session.Query(sql).Consistency(s.lowConsLevel)
	query.Bind(request.GetStoreUUID())
	iter := query.Iter()
	var createdTime time.Time
	extentMap := make(map[string]interface{})
	singleExtentStatsMap := make(map[string]interface{})
	extentStatsList := []*shared.ExtentStats{}
	for iter.Scan(&createdTime, &extentMap, &singleExtentStatsMap) {
		extentStatsMap := make(map[string]map[string]interface{})
		extentStatsMap[request.GetStoreUUID()] = singleExtentStatsMap
		extentStats := convertExtentStats(extentMap, extentStatsMap)
		extentStats.CreatedTimeMillis = common.Int64Ptr(timeToMilliseconds(createdTime))
		if filterLocally && extentStats.GetStatus() != request.GetStatus() {
			continue
		}
		extentStatsList = append(extentStatsList, extentStats)
	}

	if err := iter.Close(); err != nil {
		return nil, &shared.InternalServiceError{
			Message: err.Error(),
		}
	}

	return &m.ListStoreExtentsStatsResult_{ExtentStatsList: extentStatsList}, nil
}

// ReadStoreExtentReplicaStats implements the corresponding TChanMetadataServiceClient API
func (s *CassandraMetadataService) ReadStoreExtentReplicaStats(ctx thrift.Context, request *m.ReadStoreExtentReplicaStatsRequest) (*m.ReadStoreExtentReplicaStatsResult_, error) {
	var res m.ReadStoreExtentReplicaStatsResult_
	var writeTime int64

	if len(request.GetStoreUUID()) == 0 {
		return nil, &shared.BadRequestError{
			Message: "StoreUUID not set",
		}
	}

	if len(request.GetExtentUUID()) == 0 {
		return nil, &shared.BadRequestError{
			Message: "ExtentUUID not set",
		}
	}

	var createdTime time.Time
	extentMap := make(map[string]interface{})
	replicaStatsMap := make(map[string]interface{})

	query := s.session.Query(sqlGetStoreExtentStatsByStoreAndExtent).Consistency(s.lowConsLevel)
	query.Bind(request.GetStoreUUID(), request.GetExtentUUID())
	err := query.Scan(&createdTime, &extentMap, &replicaStatsMap, &writeTime)
	if err != nil {
		return nil, &shared.InternalServiceError{
			Message: "ReadStoreExtentReplicaStats: err" + err.Error(),
		}
	}

	// convertExtentStats expects multiple replicaStats in a map, so we should convert our single such stat appropriately
	extentStatsMap := map[string]map[string]interface{}{request.GetStoreUUID(): replicaStatsMap}

	extentStats := convertExtentStats(extentMap, extentStatsMap) // never returns nil
	extentStats.CreatedTimeMillis = common.Int64Ptr(timeToMilliseconds(createdTime))
	extentStats.GetReplicaStats()[0].WriteTime = common.Int64Ptr(int64(writeTimeToUnixNano(writeTime)))
	res.Extent = extentStats
	return &res, nil
}

// SealExtent implements the corresponding TChanMetadataServiceClient API
func (s *CassandraMetadataService) SealExtent(ctx thrift.Context, request *m.SealExtentRequest) error {

	mReq := &m.ReadExtentStatsRequest{
		DestinationUUID: common.StringPtr(request.GetDestinationUUID()),
		ExtentUUID:      common.StringPtr(request.GetExtentUUID()),
	}

	stats, err := s.ReadExtentStats(ctx, mReq)
	if err != nil {
		return err
	}

	status := stats.GetExtentStats().GetStatus()
	if status != shared.ExtentStatus_OPEN {
		if status == shared.ExtentStatus_SEALED {
			return nil
		}
		return &m.IllegalStateError{Message: fmt.Sprintf("Cannot seal extent, extentStatus=%v", status)}
	}

	err = s.updateExtent(stats.GetExtentStats(), stats.GetExtentStats().GetArchivalLocation(), shared.ExtentStatus_SEALED, timeToMilliseconds(time.Now()))
	if err != nil {
		return err
	}

	return nil
}

// MoveExtent is used to move DLQ extents into the consumer group's normal destination. Any other use IS AT YOUR OWN RISK.
func (s *CassandraMetadataService) MoveExtent(ctx thrift.Context, request *m.MoveExtentRequest) error {

	mReq := &m.ReadExtentStatsRequest{
		DestinationUUID: common.StringPtr(request.GetDestinationUUID()),
		ExtentUUID:      common.StringPtr(request.GetExtentUUID()),
	}

	stats, err := s.ReadExtentStats(ctx, mReq)
	if err != nil {
		return err
	}

	// Can't move the extent if it is not in the SEALED/CONSUMED state, since it would interfere with the seal.
	status := stats.GetExtentStats().GetStatus()
	if status != shared.ExtentStatus_SEALED && status != shared.ExtentStatus_CONSUMED {
		return &m.IllegalStateError{Message: fmt.Sprintf("Cannot move extent, extentStatus=%v", status)}
	}

	return s.moveExtentImpl(stats.GetExtentStats(), request.GetNewDestinationUUID_(), request.GetConsumerGroupVisibilityUUID())
}

func (s *CassandraMetadataService) moveExtentImpl(extentStats *shared.ExtentStats, newDestinationUUID, consumerGroupVisibilityUUID string) error {

	extent := extentStats.GetExtent()

	// If the original and new destinations are the same, just update the CG visibility
	if newDestinationUUID == extentStats.GetExtent().GetDestinationUUID() {
		// Allow nil values, which allows us to clear the cg visibility
		var cgvis *string
		if len(consumerGroupVisibilityUUID) != 0 {
			cgvis = common.StringPtr(consumerGroupVisibilityUUID)
		}

		err := s.session.Query(
			sqlUpdateDstExtentsCGVisibility,
			cgvis,
			extent.GetDestinationUUID(),
			extent.GetExtentUUID(),
		).
			Consistency(s.midConsLevel).
			Exec()
		if err != nil {
			return &shared.InternalServiceError{
				Message: "moveExtent (update): " + err.Error(),
			}
		}
		return nil
	}

	// Replace the destination replica_stats that we have with the replica stats from the stores
	// If we don't do this, we will lose the AvailableSequence and other levels set by the storehost
	rsersReq := m.NewReadStoreExtentReplicaStatsRequest()
	rsersReq.ExtentUUID = common.StringPtr(extent.GetExtentUUID())
	replicaStats := extentStats.GetReplicaStats()
	for i, r := range replicaStats {
		rsersReq.StoreUUID = common.StringPtr(r.GetStoreUUID())
		sers, rsersErr := s.ReadStoreExtentReplicaStats(nil, rsersReq)
		if rsersErr != nil {
			return &shared.InternalServiceError{
				Message: "moveExtent (move): " + rsersErr.Error(),
			}
		}

		if len(sers.GetExtent().GetReplicaStats()) != 1 {
			return &shared.InternalServiceError{
				Message: "moveExtent (move): unexpected ReadStoreExtentReplicaStatsResult",
			}
		}
		replicaStats[i] = sers.GetExtent().GetReplicaStats()[0] // TODO: some kind of merge between the two replica stats types (store & destination)
	}

	batch := s.session.NewBatch(gocql.LoggedBatch)
	batch.Cons = s.midConsLevel

	// The usual case where we are both setting cg visiblity and moving the extent
	// The first map has the original destination UUID
	extentStatsMap := makeReplicaStatsMap(extentStats.GetReplicaStats(), extentStats.GetExtent().GetDestinationUUID())

	// Add deletion of the original extent everywhere to the batch
	s.deleteExtentImpl(extent, extentStatsMap, extentStats, batch, true /* forMove */)

	// The second map has the new destination UUID
	extentStatsMap = makeReplicaStatsMap(extentStats.GetReplicaStats(), newDestinationUUID)
	extent.DestinationUUID = common.StringPtr(newDestinationUUID)

	// Add creation of the new extent everywhere to the batch
	s.createExtentImpl(extent, shared.ExtentStatus_SEALED, extentStatsMap, &consumerGroupVisibilityUUID, batch)

	if err := s.session.ExecuteBatch(batch); err != nil {
		return &shared.InternalServiceError{
			Message: "moveExtent: " + err.Error(),
		}
	}

	return nil
}

// UpdateStoreExtentReplicaStats implements the corresponding TChanMetadataServiceClient API
func (s *CassandraMetadataService) UpdateStoreExtentReplicaStats(ctx thrift.Context, request *m.UpdateStoreExtentReplicaStatsRequest) error {
	if len(request.GetExtentUUID()) == 0 {
		return &shared.BadRequestError{
			Message: "ExtentUUID not set",
		}
	}

	batch := s.session.NewBatch(gocql.LoggedBatch)
	batch.Cons = s.lowConsLevel
	for _, stats := range request.GetReplicaStats() {
		sqlUpdatedInStores := `UPDATE ` + tableStoreExtents + ` SET ` +
			columnReplicaStats + ` = {` +
			columnExtentUUID + `: ?,` +
			columnStoreUUID + `: ?,` +
			columnBeginAddress + `: ?,` +
			columnLastAddress + `: ?,` +
			columnBeginSequence + `: ?,` +
			columnLastSequence + `: ?,` +
			columnBeginEnqueueTime + `: ?,` +
			columnLastEnqueueTime + `: ?,` +
			columnSizeInBytes + `: ?,` +
			columnStatus + `: ?,` +
			columnBeginTime + `: ?,` +
			columnEndTime + `: ?,` +
			columnAvailableSequence + `: ?,` +
			columnAvailableSequenceRate + `: ?,` +
			columnLastSequenceRate + `: ?,` +
			columnSizeInBytesRate + `: ?` +
			`} WHERE ` +
			columnStoreUUID + `= ? AND ` + columnExtentUUID + `= ?`
		batch.Query(
			sqlUpdatedInStores,
			request.GetExtentUUID(),
			stats.GetStoreUUID(),
			stats.GetBeginAddress(),
			stats.GetLastAddress(),
			stats.GetBeginSequence(),
			stats.GetLastSequence(),
			time.Unix(0, stats.GetBeginEnqueueTimeUtc()),
			time.Unix(0, stats.GetLastEnqueueTimeUtc()),
			stats.GetSizeInBytes(),
			stats.GetStatus(),
			time.Unix(0, stats.GetBeginTime()),
			time.Unix(0, stats.GetEndTime()),
			stats.GetAvailableSequence(),
			stats.GetAvailableSequenceRate(),
			stats.GetLastSequenceRate(),
			stats.GetSizeInBytesRate(),
			stats.GetStoreUUID(),
			request.GetExtentUUID(),
		)
	}

	if request.IsSetReplicationStatus() {
		if !request.IsSetStoreUUID() {
			return &shared.BadRequestError{
				Message: "StoreUUID not set",
			}
		}

		sqlUpdateReplicationStatus := `UPDATE ` + tableStoreExtents + ` SET ` +
			columnReplicationStatus + ` = ? WHERE ` +
			columnStoreUUID + `= ? AND ` + columnExtentUUID + `= ?`
		batch.Query(
			sqlUpdateReplicationStatus,
			request.GetReplicationStatus(),
			request.GetStoreUUID(),
			request.GetExtentUUID(),
		)
	}
	if err := s.session.ExecuteBatch(batch); err != nil {
		s.log.WithFields(bark.Fields{
			common.TagExt: request.GetExtentUUID(),
			common.TagErr: err,
		}).Error("UpdateExtentReplicaStats failed")
		return &shared.InternalServiceError{
			Message: "UpdateStoreExtentReplicaStats: %v" + err.Error(),
		}
	}
	return nil
}

// UpdateExtentReplicaStats implements the corresponding TChanMetadataServiceClient API
func (s *CassandraMetadataService) UpdateExtentReplicaStats(ctx thrift.Context, request *m.UpdateExtentReplicaStatsRequest) error {
	if len(request.GetDestinationUUID()) == 0 || len(request.GetExtentUUID()) == 0 || len(request.GetInputHostUUID()) == 0 {
		return &shared.BadRequestError{
			Message: "UpdateExtentReplicaStats: some required request field is not set",
		}
	}
	batch := s.session.NewBatch(gocql.LoggedBatch)
	batch.Cons = s.lowConsLevel
	sqlUpdate := `UPDATE ` + tableDestinationExtents + ` SET ` +
		columnReplicaStats + `[?] = {` +
		columnExtentUUID + `: ?,` +
		columnStoreUUID + `: ?,` +
		columnDestinationUUID + `: ?,` +
		columnBeginAddress + `: ?,` +
		columnLastAddress + `: ?,` +
		columnBeginSequence + `: ?,` +
		columnLastSequence + `: ?,` +
		columnBeginEnqueueTime + `: ?,` +
		columnLastEnqueueTime + `: ?,` +
		columnSizeInBytes + `: ?,` +
		columnStatus + `: ?,` +
		columnBeginTime + `: ?,` +
		columnEndTime + `: ?` +
		`} WHERE ` + columnDestinationUUID + `= ? AND ` + columnExtentUUID + `= ?`
	for _, stats := range request.GetReplicaStats() {
		batch.Query(
			sqlUpdate,
			stats.GetStoreUUID(),
			request.GetExtentUUID(),
			stats.GetStoreUUID(),
			request.GetDestinationUUID(),
			stats.GetBeginAddress(),
			stats.GetLastAddress(),
			stats.GetBeginSequence(),
			stats.GetLastSequence(),
			time.Unix(0, stats.GetBeginEnqueueTimeUtc()),
			time.Unix(0, stats.GetLastEnqueueTimeUtc()),
			stats.GetSizeInBytes(),
			stats.GetStatus(),
			time.Unix(0, stats.GetBeginTime()),
			time.Unix(0, stats.GetEndTime()),
			request.GetDestinationUUID(),
			request.GetExtentUUID(),
		)

		sqlUpdatedInputHosts := `UPDATE ` + tableInputHostExtents + ` SET ` +
			columnReplicaStats + `[?] = {` +
			columnExtentUUID + `: ?,` +
			columnStoreUUID + `: ?,` +
			columnDestinationUUID + `: ?,` +
			columnBeginAddress + `: ?,` +
			columnLastAddress + `: ?,` +
			columnBeginSequence + `: ?,` +
			columnLastSequence + `: ?,` +
			columnBeginEnqueueTime + `: ?,` +
			columnLastEnqueueTime + `: ?,` +
			columnSizeInBytes + `: ?,` +
			columnStatus + `: ?,` +
			columnBeginTime + `: ?,` +
			columnEndTime + `: ?` +
			`} WHERE ` +
			columnDestinationUUID + ` = ? AND ` + columnInputHostUUID + `= ? AND ` + columnExtentUUID + ` = ?`
		batch.Query(
			sqlUpdatedInputHosts,
			stats.GetStoreUUID(),
			request.GetExtentUUID(),
			stats.GetStoreUUID(),
			request.GetDestinationUUID(),
			stats.GetBeginAddress(),
			stats.GetLastAddress(),
			stats.GetBeginSequence(),
			stats.GetLastSequence(),
			time.Unix(0, stats.GetBeginEnqueueTimeUtc()),
			time.Unix(0, stats.GetLastEnqueueTimeUtc()),
			stats.GetSizeInBytes(),
			stats.GetStatus(),
			time.Unix(0, stats.GetBeginTime()),
			time.Unix(0, stats.GetEndTime()),
			request.GetDestinationUUID(),
			request.GetInputHostUUID(),
			request.GetExtentUUID(),
		)
		sqlUpdatedInStores := `UPDATE ` + tableStoreExtents + ` SET ` +
			columnReplicaStats + ` = {` +
			columnExtentUUID + `: ?,` +
			columnStoreUUID + `: ?,` +
			columnDestinationUUID + `: ?,` +
			columnBeginAddress + `: ?,` +
			columnLastAddress + `: ?,` +
			columnBeginSequence + `: ?,` +
			columnLastSequence + `: ?,` +
			columnBeginEnqueueTime + `: ?,` +
			columnLastEnqueueTime + `: ?,` +
			columnSizeInBytes + `: ?,` +
			columnStatus + `: ?,` +
			columnBeginTime + `: ?,` +
			columnEndTime + `: ?,` +
			columnAvailableSequence + `: ?,` +
			columnAvailableSequenceRate + `: ?,` +
			columnLastSequenceRate + `: ?,` +
			columnSizeInBytesRate + `: ?` +
			`} WHERE ` +
			columnStoreUUID + `= ? AND ` + columnExtentUUID + `= ?`
		batch.Query(
			sqlUpdatedInStores,
			request.GetExtentUUID(),
			stats.GetStoreUUID(),
			request.GetDestinationUUID(),
			stats.GetBeginAddress(),
			stats.GetLastAddress(),
			stats.GetBeginSequence(),
			stats.GetLastSequence(),
			time.Unix(0, stats.GetBeginEnqueueTimeUtc()),
			time.Unix(0, stats.GetLastEnqueueTimeUtc()),
			stats.GetSizeInBytes(),
			stats.GetStatus(),
			time.Unix(0, stats.GetBeginTime()),
			time.Unix(0, stats.GetEndTime()),
			stats.GetAvailableSequence(),
			stats.GetAvailableSequenceRate(),
			stats.GetLastSequenceRate(),
			stats.GetSizeInBytesRate(),
			stats.GetStoreUUID(),
			request.GetExtentUUID(),
		)
	}
	if err := s.session.ExecuteBatch(batch); err != nil {
		return &shared.InternalServiceError{
			Message: "UpdateExtentReplicaStats: " + err.Error(),
		}
	}
	return nil
}

// CQL commands for consumer group exent CRUD go here
const (
	sqlCGPutExtent = `INSERT INTO ` + tableConsumerGroupExtents + `(` +
		columnConsumerGroupUUID + `,` + columnExtentUUID + `,` + columnOutputHostUUID + `,` + columnStatus + `,` + columnStoreUUIDS +
		`) VALUES(?, ?, ?, ?, ?)`

	sqlCGDeleteExtent = `INSERT INTO ` + tableConsumerGroupExtents + `(` +
		columnConsumerGroupUUID + `,` +
		columnExtentUUID + `,` +
		columnOutputHostUUID + `,` +
		columnStatus + `,` +
		columnAckLevelOffset + `, ` +
		columnAckLevelSequence + `, ` +
		columnAckLevelSequenceRate + `, ` +
		columnReceivedLevelOffset + `, ` +
		columnReceivedLevelSequence + `, ` +
		columnReceivedLevelSequenceRate + `, ` +
		columnConnectedStore + `, ` +
		columnStoreUUIDS +
		`) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)` +
		` USING TTL ?`

	sqlCGGetExtent = `SELECT ` +
		columnStatus + `, ` +
		columnOutputHostUUID + `, ` +
		columnAckLevelOffset + `, ` +
		columnAckLevelSequence + `, ` +
		columnAckLevelSequenceRate + `, ` +
		columnReceivedLevelOffset + `, ` +
		columnReceivedLevelSequence + `, ` +
		columnReceivedLevelSequenceRate + `, ` +
		columnConnectedStore + `, ` +
		columnStoreUUIDS + `, ` +
		`WRITETIME( ` + columnAckLevelSequence + ` )` +
		` FROM ` + tableConsumerGroupExtents +
		` WHERE ` + columnConsumerGroupUUID + `=? AND ` + columnExtentUUID + `=?`

	sqlCGGetAllExtents = `SELECT ` +
		columnExtentUUID + `, ` +
		columnOutputHostUUID + `, ` +
		columnStatus + `, ` +
		columnAckLevelOffset + `,` +
		columnStoreUUIDS + `,` +
		columnAckLevelSequence + `, ` +
		columnAckLevelSequenceRate + `, ` +
		columnReceivedLevelOffset + `, ` +
		columnReceivedLevelSequence + `, ` +
		columnReceivedLevelSequenceRate + `, ` +
		columnConnectedStore + `, ` +
		`WRITETIME( ` + columnAckLevelSequence + ` )` +
		` FROM ` + tableConsumerGroupExtents +
		` WHERE ` + columnConsumerGroupUUID + `=?`

	sqlCGGetAllExtentsLite = `SELECT ` +
		columnStatus + `, ` +
		columnExtentUUID + `, ` +
		columnOutputHostUUID + `, ` +
		columnStoreUUIDS +
		` FROM ` + tableConsumerGroupExtents +
		` WHERE ` + columnConsumerGroupUUID + `=?`

	sqlExtGetAllExtents = `SELECT ` +
		columnConsumerGroupUUID + `, ` +
		columnOutputHostUUID + `, ` +
		columnStatus + `, ` +
		columnAckLevelOffset + `,` +
		columnStoreUUIDS + `,` +
		columnAckLevelSequence + `, ` +
		columnAckLevelSequenceRate + `, ` +
		columnReceivedLevelOffset + `, ` +
		columnReceivedLevelSequence + `, ` +
		columnReceivedLevelSequenceRate + `, ` +
		columnConnectedStore +
		` FROM ` + tableConsumerGroupExtents +
		` WHERE ` + columnExtentUUID + `=? ALLOW FILTERING`

	sqlCGUpdateAckOffset = `UPDATE ` + tableConsumerGroupExtents +
		` SET ` +
		columnOutputHostUUID + `=?,` +
		columnAckLevelOffset + `=?, ` +
		columnAckLevelSequence + `=?,` +
		columnAckLevelSequenceRate + `=?,` +
		columnReceivedLevelOffset + `=?,` +
		columnReceivedLevelSequence + `=?,` +
		columnReceivedLevelSequenceRate + `=?,` +
		columnConnectedStore + `=?` +
		` WHERE ` + columnConsumerGroupUUID + `=? AND ` + columnExtentUUID + `=?`

	sqlCGUpdateAckOffsetNoOutputAndStore = `UPDATE ` + tableConsumerGroupExtents +
		` SET ` +
		columnAckLevelOffset + `=?, ` +
		columnAckLevelSequence + `=?,` +
		columnAckLevelSequenceRate + `=?,` +
		columnReceivedLevelOffset + `=?,` +
		columnReceivedLevelSequence + `=?,` +
		columnReceivedLevelSequenceRate + `=?` +
		` WHERE ` + columnConsumerGroupUUID + `=? AND ` + columnExtentUUID + `=?`

	sqlCGUpdateAckOffsetConsumed = `UPDATE ` + tableConsumerGroupExtents +
		` SET ` +
		columnStatus + `=?,` +
		columnOutputHostUUID + `=?,` +
		columnAckLevelOffset + `=?, ` +
		columnAckLevelSequence + `=?,` +
		columnAckLevelSequenceRate + `=?,` +
		columnReceivedLevelOffset + `=?, ` +
		columnReceivedLevelSequence + `=?,` +
		columnReceivedLevelSequenceRate + `=?,` +
		columnConnectedStore + `=?` +
		` WHERE ` + columnConsumerGroupUUID + `=? AND ` + columnExtentUUID + `=?`

	sqlCGUpdateAckOffsetConsumedNoOutputAndStore = `UPDATE ` + tableConsumerGroupExtents +
		` SET ` +
		columnStatus + `=?,` +
		columnAckLevelOffset + `=?, ` +
		columnAckLevelSequence + `=?,` +
		columnAckLevelSequenceRate + `=?,` +
		columnReceivedLevelOffset + `=?, ` +
		columnReceivedLevelSequence + `=?,` +
		columnReceivedLevelSequenceRate + `=?` +
		` WHERE ` + columnConsumerGroupUUID + `=? AND ` + columnExtentUUID + `=?`

	sqlCGUpdateStatus = `UPDATE ` + tableConsumerGroupExtents + ` SET ` + columnStatus + `=?` +
		` WHERE ` + columnConsumerGroupUUID + `=? AND ` + columnExtentUUID + `=?`

	sqlCGUpdateOutputHost = `UPDATE ` + tableConsumerGroupExtents +
		` SET ` + columnOutputHostUUID + `=? ` +
		` WHERE ` + columnConsumerGroupUUID + `=? AND ` + columnExtentUUID + `=?`
)

const maxStoreHostsPerExtent = 3

func min(n1, n2 int) int {
	if n2 < n1 {
		return n2
	}
	return n1
}

func (s *CassandraMetadataService) deleteConsumerGroupExtent(cgUUID string, extentUUID string) error {

	readReq := &m.ReadConsumerGroupExtentRequest{
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		ExtentUUID:        common.StringPtr(extentUUID),
	}

	resp, err := s.ReadConsumerGroupExtent(nil, readReq)
	if err != nil {
		return err
	}

	var cge = resp.GetExtent()
	var connectedStore interface{}
	if cge.IsSetConnectedStoreUUID() {
		// the connected store uuid can be nil and
		// gocql client will fail to marshal empty
		// strings as uuids. So, use literal nil
		// *string when storeUUID is nil
		connectedStore = cge.GetConnectedStoreUUID()
	}

	query := s.session.Query(sqlCGDeleteExtent,
		cge.GetConsumerGroupUUID(),
		cge.GetExtentUUID(),
		cge.GetOutputHostUUID(),
		shared.ConsumerGroupExtentStatus_DELETED,
		cge.GetAckLevelOffset(),
		cge.GetAckLevelSeqNo(),
		cge.GetAckLevelSeqNoRate(),
		cge.GetReadLevelOffset(),
		cge.GetReadLevelSeqNo(),
		cge.GetReadLevelSeqNoRate(),
		connectedStore,
		cge.GetStoreUUIDs(),
		deleteExtentTTLSeconds)

	query.Consistency(s.midConsLevel)

	if err = query.Exec(); err != nil {
		return &shared.InternalServiceError{
			Message: fmt.Sprintf("deleteConsumerGroupExtent - query failed, cg=%v ext=%v, err=%v", cgUUID, extentUUID, err),
		}
	}

	return nil
}

// UpdateConsumerGroupExtentStatus updates the consumer group extent status
func (s *CassandraMetadataService) UpdateConsumerGroupExtentStatus(ctx thrift.Context, request *shared.UpdateConsumerGroupExtentStatusRequest) error {
	if request.Status == nil {
		return &shared.BadRequestError{
			Message: "Empty status",
		}
	}

	if request.GetStatus() == shared.ConsumerGroupExtentStatus_DELETED {
		return s.deleteConsumerGroupExtent(request.GetConsumerGroupUUID(), request.GetExtentUUID())
	}
	query := s.session.Query(sqlCGUpdateStatus, request.GetStatus(), request.GetConsumerGroupUUID(), request.GetExtentUUID())
	query.Consistency(s.midConsLevel)
	err := query.Exec()
	if err != nil {
		return &shared.InternalServiceError{
			Message: "UpdateConsumerGroupExtentStatus: " + err.Error(),
		}
	}
	return nil
}

// SetAckOffset updates the ack offset for the given [ConsumerGroup, Exent]
// If there is no existing record for a [ConsumerGroup, Extent], this method
// will automatically create a record with the given offset
func (s *CassandraMetadataService) SetAckOffset(ctx thrift.Context, request *shared.SetAckOffsetRequest) error {

	var err error
	var connectedStore interface{}
	if request.IsSetConnectedStoreUUID() {
		connectedStore = request.GetConnectedStoreUUID()
	}

	// check whether output host and stores are set. If not, no need to update these columns
	updateWithOutputHostAndStore := true
	if !request.IsSetOutputHostUUID() && !request.IsSetConnectedStoreUUID() {
		updateWithOutputHostAndStore = false
	}

	if request.Status != nil && request.GetStatus() == shared.ConsumerGroupExtentStatus_CONSUMED {
		var query *gocql.Query
		if updateWithOutputHostAndStore {
			query = s.session.Query(
				sqlCGUpdateAckOffsetConsumed,
				request.GetStatus(),
				request.GetOutputHostUUID(),
				request.GetAckLevelAddress(),
				request.GetAckLevelSeqNo(),
				request.GetAckLevelSeqNoRate(),
				request.GetReadLevelAddress(),
				request.GetReadLevelSeqNo(),
				request.GetReadLevelSeqNoRate(),
				connectedStore,
				request.GetConsumerGroupUUID(),
				request.GetExtentUUID())
		} else {
			query = s.session.Query(
				sqlCGUpdateAckOffsetConsumedNoOutputAndStore,
				request.GetStatus(),
				request.GetAckLevelAddress(),
				request.GetAckLevelSeqNo(),
				request.GetAckLevelSeqNoRate(),
				request.GetReadLevelAddress(),
				request.GetReadLevelSeqNo(),
				request.GetReadLevelSeqNoRate(),
				request.GetConsumerGroupUUID(),
				request.GetExtentUUID())
		}
		// this query updates the offsets + changes the status of the
		// extent, prefer mid level conistency
		query.Consistency(s.midConsLevel)
		err = query.Exec()
	} else {
		var query *gocql.Query
		if updateWithOutputHostAndStore {
			query = s.session.Query(
				sqlCGUpdateAckOffset,
				request.GetOutputHostUUID(),
				request.GetAckLevelAddress(),
				request.GetAckLevelSeqNo(),
				request.GetAckLevelSeqNoRate(),
				request.GetReadLevelAddress(),
				request.GetReadLevelSeqNo(),
				request.GetReadLevelSeqNoRate(),
				connectedStore,
				request.GetConsumerGroupUUID(),
				request.GetExtentUUID())
		} else {
			query = s.session.Query(
				sqlCGUpdateAckOffsetNoOutputAndStore,
				request.GetAckLevelAddress(),
				request.GetAckLevelSeqNo(),
				request.GetAckLevelSeqNoRate(),
				request.GetReadLevelAddress(),
				request.GetReadLevelSeqNo(),
				request.GetReadLevelSeqNoRate(),
				request.GetConsumerGroupUUID(),
				request.GetExtentUUID())
		}
		query.Consistency(s.lowConsLevel)
		err = query.Exec()
	}

	if err != nil {
		return &shared.InternalServiceError{
			Message: fmt.Sprintf("SetAckOffset - Update operation failed, cg=%v ext=%v, err=%v",
				request.GetConsumerGroupUUID(), request.GetExtentUUID(), err),
		}
	}

	return nil
}

// CreateConsumerGroupExtent creates a [ConsumerGroup, Extent, OutputHost] mapping
// If the mapping already exist, this method will overwrite the existing mapping
func (s *CassandraMetadataService) CreateConsumerGroupExtent(ctx thrift.Context, request *shared.CreateConsumerGroupExtentRequest) error {

	query := s.session.Query(sqlCGPutExtent,
		request.GetConsumerGroupUUID(),
		request.GetExtentUUID(),
		request.GetOutputHostUUID(),
		shared.ConsumerGroupExtentStatus_OPEN,
		request.GetStoreUUIDs())

	query.Consistency(s.midConsLevel)

	if err := query.Exec(); err != nil {
		return &shared.InternalServiceError{
			Message: fmt.Sprintf("CreateConsumerGroupExtent - query failed, dst=%v, cg=%v ext=%v, outputHost=%v",
				request.GetDestinationUUID(), request.GetConsumerGroupUUID(), request.GetExtentUUID(), request.GetOutputHostUUID()),
		}
	}

	return nil
}

// ReadConsumerGroupExtentsByExtUUID returns the extents corresponding to the given [extentUUID], if it exist
func (s *CassandraMetadataService) ReadConsumerGroupExtentsByExtUUID(ctx thrift.Context, listRequest *m.ReadConsumerGroupExtentsByExtUUIDRequest) (*m.ReadConsumerGroupExtentsByExtUUIDResult_, error) {

	extUUID := listRequest.GetExtentUUID()
	if len(extUUID) == 0 {
		return nil, &shared.BadRequestError{Message: "Missing ExtentUUID param, must provide ExtentUUID"}
	}
	query := s.session.Query(sqlExtGetAllExtents, extUUID).Consistency(s.lowConsLevel)

	iter := query.PageSize(int(listRequest.GetLimit())).PageState(listRequest.GetPageToken()).Iter()

	if iter == nil {
		return nil, &shared.InternalServiceError{
			Message: "Query returned nil iterator",
		}
	}

	result := &m.ReadConsumerGroupExtentsByExtUUIDResult_{
		CgExtents:     []*shared.ConsumerGroupExtent{},
		NextPageToken: listRequest.PageToken,
	}
	ext := &shared.ConsumerGroupExtent{}
	ext.ExtentUUID = common.StringPtr(listRequest.GetExtentUUID())
	var storeUUIDs []gocql.UUID
	count := int64(0)
	for iter.Scan(
		&ext.ConsumerGroupUUID,
		&ext.OutputHostUUID,
		&ext.Status,
		&ext.AckLevelOffset,
		&storeUUIDs,
		&ext.AckLevelSeqNo,
		&ext.AckLevelSeqNoRate,
		&ext.ReadLevelOffset,
		&ext.ReadLevelSeqNo,
		&ext.ReadLevelSeqNoRate,
		&ext.ConnectedStoreUUID) && count < listRequest.GetLimit() {
		// Get a new item within limit
		result.CgExtents = append(result.CgExtents, ext)
		ext = &shared.ConsumerGroupExtent{}
		ext.ExtentUUID = common.StringPtr(listRequest.GetExtentUUID())
		count++
	}

	nextPageToken := iter.PageState()
	result.NextPageToken = make([]byte, len(nextPageToken))
	copy(result.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, &shared.InternalServiceError{
			Message: err.Error(),
		}
	}
	return result, nil
}

// ReadConsumerGroupExtent returns the [Status, AckOffset] corresponding to the given [ConsumerGroup, Extent], if it exist
func (s *CassandraMetadataService) ReadConsumerGroupExtent(ctx thrift.Context, request *m.ReadConsumerGroupExtentRequest) (*m.ReadConsumerGroupExtentResult_, error) {
	result := &m.ReadConsumerGroupExtentResult_{Extent: &shared.ConsumerGroupExtent{}}
	result.Extent.ExtentUUID = common.StringPtr(request.GetExtentUUID())
	result.Extent.ConsumerGroupUUID = common.StringPtr(request.GetConsumerGroupUUID())

	query := s.session.Query(sqlCGGetExtent, request.GetConsumerGroupUUID(), request.GetExtentUUID())
	query.Consistency(s.lowConsLevel)
	iter := query.Iter()

	var storeUUIDs []gocql.UUID
	var writeTime int64

	if !iter.Scan(
		&result.Extent.Status,
		&result.Extent.OutputHostUUID,
		&result.Extent.AckLevelOffset,
		&result.Extent.AckLevelSeqNo,
		&result.Extent.AckLevelSeqNoRate,
		&result.Extent.ReadLevelOffset,
		&result.Extent.ReadLevelSeqNo,
		&result.Extent.ReadLevelSeqNoRate,
		&result.Extent.ConnectedStoreUUID,
		&storeUUIDs,
		&writeTime) {
		return nil, &shared.EntityNotExistsError{
			Message: fmt.Sprintf("ReadConsumerGroupExtent - no record found, dst=%v, cg=%v ext=%v",
				request.GetDestinationUUID(), request.GetConsumerGroupUUID(), request.GetExtentUUID()),
		}
	}

	if len(storeUUIDs) < 1 {
		// this can happen when there is a race condition between CreateConsumerGroupExtent and
		// SetAckOffset or SetOutputHost methods. Cassandra UPDATE operations implicity create a
		// row when the row doesn't already exist, so SetAckOffset operation could succeed before
		// CreateConsumerGroupExtent succeeds
		errMsg := fmt.Sprintf("ReadConsumerGroupExtent - got inconsistent record with no storehosts, dst=%v, cg=%v, ext=%v",
			request.GetDestinationUUID(), request.GetConsumerGroupUUID(), request.GetExtentUUID())
		s.log.WithFields(bark.Fields{common.TagDst: common.FmtDst(request.GetDestinationUUID()), common.TagCnsm: common.FmtCnsm(request.GetConsumerGroupUUID()), common.TagExt: common.FmtExt(request.GetExtentUUID())}).Error("ReadConsumerGroupExtent - got inconsistent record with no storehosts")
		return nil, &shared.InternalServiceError{
			Message: errMsg,
		}
	}

	result.Extent.StoreUUIDs = uuidSliceToStringSlice(storeUUIDs)
	result.Extent.WriteTime = common.Int64Ptr(int64(writeTimeToUnixNano(writeTime)))

	if err := iter.Close(); err != nil {
		return nil, &shared.InternalServiceError{
			Message: fmt.Sprintf("ReadConsumerGroupExtent - failed to close iterator, dst=%v, cg=%v ext=%v, err=%v",
				request.GetDestinationUUID(), request.GetConsumerGroupUUID(), request.GetExtentUUID(), err),
		}
	}

	return result, nil
}

// SetOutputHost updates the OutputHost for the given [ConsumerGroup, Extent]
// If there is no existing record for a [ConsumerGroup, Extent], this method
// will automatically create a record with the given outputHost
func (s *CassandraMetadataService) SetOutputHost(ctx thrift.Context, request *m.SetOutputHostRequest) error {
	query := s.session.Query(sqlCGUpdateOutputHost, request.GetOutputHostUUID(), request.GetConsumerGroupUUID(), request.GetExtentUUID())
	query.Consistency(s.midConsLevel)
	if err := query.Exec(); err != nil {
		return &shared.InternalServiceError{
			Message: fmt.Sprintf("SetOutputHost - Update operation failed, cg=%v ext=%v, err=%v",
				request.GetConsumerGroupUUID(), request.GetExtentUUID(), err),
		}
	}
	return nil
}

// ReadConsumerGroupExtents implements the corresponding TChanMetadataServiceClient API
func (s *CassandraMetadataService) ReadConsumerGroupExtents(ctx thrift.Context, request *shared.ReadConsumerGroupExtentsRequest) (*shared.ReadConsumerGroupExtentsResult_, error) {
	if request.GetMaxResults() < 1 {
		return nil, &shared.BadRequestError{
			Message: "MaxResults < 1",
		}
	}

	filterLocally := false
	// Don't default to filtering by the secondary
	// index field `status` on the WHERE clause.
	// Cassandra is inefficient w.r.t predicates
	// on secondary index, when the number of
	// entries that map to a single index value
	// are a lot (> 10). So, only use secondary
	// index for filtering by OPEN status
	if request.IsSetStatus() && request.GetStatus() != shared.ConsumerGroupExtentStatus_OPEN {
		filterLocally = true
	}

	iter, err := s.readConsumerGroupExtentsHelper(request, filterLocally)
	if err != nil {
		return nil, err
	}

	result := &shared.ReadConsumerGroupExtentsResult_{
		Extents:       []*shared.ConsumerGroupExtent{},
		NextPageToken: request.PageToken,
	}

	var status shared.ConsumerGroupExtentStatus
	var ackLevelOffset, ackLevelSeq, readLevelOffset, readLevelSeq, writeTime int64
	var extentUUID, outputHostUUID, connectedStore string
	var alsr, rlsr float64
	var storeUUIDs []gocql.UUID

	for iter.Scan(
		&extentUUID,
		&outputHostUUID,
		&status,
		&ackLevelOffset,
		&storeUUIDs,
		&ackLevelSeq,
		&alsr,
		&readLevelOffset,
		&readLevelSeq,
		&rlsr,
		&connectedStore,
		&writeTime,
	) {
		if request.OutputHostUUID != nil && strings.Compare(outputHostUUID, request.GetOutputHostUUID()) != 0 {
			continue
		}

		if len(storeUUIDs) < 1 {
			ll := s.log.WithField(common.TagCnsm, common.FmtCnsm(request.GetConsumerGroupUUID()))
			if len(request.GetDestinationUUID()) > 0 {
				ll = ll.WithField(common.TagDst, common.FmtDst(request.GetDestinationUUID()))
			}
			if len(request.GetOutputHostUUID()) > 0 {
				ll = ll.WithField(common.TagOut, common.FmtOut(request.GetOutputHostUUID()))
			}
			ll.Error("ReadConsumerGroupExtents - got inconsistent record with no storehosts")
			continue
		}

		if filterLocally && status != request.GetStatus() {
			continue
		}

		item := &shared.ConsumerGroupExtent{
			ExtentUUID:         common.StringPtr(extentUUID),
			ConsumerGroupUUID:  common.StringPtr(request.GetConsumerGroupUUID()),
			Status:             common.MetadataConsumerGroupExtentStatusPtr(status),
			AckLevelOffset:     common.Int64Ptr(ackLevelOffset),
			OutputHostUUID:     common.StringPtr(outputHostUUID),
			StoreUUIDs:         uuidSliceToStringSlice(storeUUIDs),
			ConnectedStoreUUID: common.StringPtr(connectedStore),
			AckLevelSeqNo:      common.Int64Ptr(ackLevelSeq),
			AckLevelSeqNoRate:  common.Float64Ptr(alsr),
			ReadLevelOffset:    common.Int64Ptr(readLevelOffset),
			ReadLevelSeqNo:     common.Int64Ptr(readLevelSeq),
			ReadLevelSeqNoRate: common.Float64Ptr(rlsr),
			WriteTime:          common.Int64Ptr(int64(writeTimeToUnixNano(writeTime))),
		}

		result.Extents = append(result.Extents, item)
	}

	nextPageToken := iter.PageState()
	result.NextPageToken = make([]byte, len(nextPageToken))
	copy(result.NextPageToken, nextPageToken)
	if err = iter.Close(); err != nil {
		return nil, &shared.InternalServiceError{
			Message: err.Error(),
		}
	}

	return result, nil
}

func (s *CassandraMetadataService) readConsumerGroupExtentsHelper(request *shared.ReadConsumerGroupExtentsRequest, ignoreStatusFilter bool) (*gocql.Iter, error) {

	sql := sqlCGGetAllExtents
	if !ignoreStatusFilter && request.IsSetStatus() {
		sql += fmt.Sprintf(" AND %s=%d", columnStatus, request.GetStatus())
	}

	query := s.session.Query(sql, request.GetConsumerGroupUUID())
	query.Consistency(s.lowConsLevel)

	// apply limit and page token only for positive max result
	if request.GetMaxResults() > 0 {
		query = query.PageSize(int(request.GetMaxResults())).PageState(request.GetPageToken())
	}
	return query.Iter(), nil
}

// ReadConsumerGroupExtentsLite returns the list all extents mapped to
// the given consumer group. This API only returns a few interesting
// columns for each extent in the result. For detailed info about
// extents, see ReadConsumerGroupExtents
func (s *CassandraMetadataService) ReadConsumerGroupExtentsLite(ctx thrift.Context, request *m.ReadConsumerGroupExtentsLiteRequest) (*m.ReadConsumerGroupExtentsLiteResult_, error) {

	if request.GetMaxResults() <= 0 {
		return nil, errPageLimitOutOfRange
	}

	filterLocally := false
	if request.IsSetStatus() && request.GetStatus() != shared.ConsumerGroupExtentStatus_OPEN {
		filterLocally = true
	}

	sql := sqlCGGetAllExtentsLite
	if !filterLocally && request.IsSetStatus() {
		sql += fmt.Sprintf(" AND %s=%d", columnStatus, request.GetStatus())
	}

	qry := s.session.Query(sql).Consistency(s.lowConsLevel).Bind(request.GetConsumerGroupUUID())
	qry = qry.PageSize(int(request.GetMaxResults())).PageState(request.GetPageToken())

	iter := qry.Iter()
	allocSize := iter.NumRows()
	if filterLocally {
		allocSize = common.MinInt(8, allocSize)
	}

	allocSize = common.MaxInt(allocSize, 1)

	result := m.NewReadConsumerGroupExtentsLiteResult_()
	result.Extents = make([]*m.ConsumerGroupExtentLite, 0, allocSize)
	extents := make([]m.ConsumerGroupExtentLite, allocSize)

	cnt := 0

	for iter.Scan(
		&extents[cnt].Status,
		&extents[cnt].ExtentUUID,
		&extents[cnt].OutputHostUUID,
		&extents[cnt].StoreUUIDs) {

		if filterLocally && extents[cnt].GetStatus() != request.GetStatus() {
			continue
		}

		if request.IsSetOutputHostUUID() &&
			strings.Compare(extents[cnt].GetOutputHostUUID(), request.GetOutputHostUUID()) != 0 {
			continue
		}

		result.Extents = append(result.Extents, &extents[cnt])

		cnt++
		if cnt == len(extents) {
			cnt = 0
			extents = make([]m.ConsumerGroupExtentLite, allocSize)
		}
	}

	nextPageToken := iter.PageState()
	result.NextPageToken = make([]byte, len(nextPageToken))
	copy(result.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, &shared.InternalServiceError{
			Message: err.Error(),
		}
	}

	return result, nil
}

// CQL commands for UUIDToHostAddr and HostAddrToUUID CRUD go here
const (
	sqlPutUUIDToHostAddr = `INSERT INTO ` + tableUUIDToHostAddr +
		`(` + columnUUID + `,` + columnHostAddr + `,` + columnHostName + `) VALUES(?, ?, ?) USING TTL ?`

	sqlPutHostAddrToUUID = `INSERT INTO ` + tableHostAddrToUUID +
		`(` + columnHostAddr + `,` + columnUUID + `,` + columnHostName + `) VALUES(?, ?, ?) USING TTL ?`

	sqlGetUUIDToHostAddr = `SELECT ` + columnHostAddr + ` FROM ` + tableUUIDToHostAddr +
		` WHERE ` + columnUUID + `=?`

	sqlGetHostAddrToUUID = `SELECT ` + columnUUID + ` FROM ` + tableHostAddrToUUID +
		` WHERE ` + columnHostAddr + `=?`
	sqlHost = `SELECT ` + columnHostName + `, ` + columnHostAddr + `, ` + columnUUID + ` FROM `

	sqlListUUIDToHostAddr = sqlHost + tableUUIDToHostAddr
	sqlListHostAddrToUUID = sqlHost + tableHostAddrToUUID
)

// RegisterHostUUID records a UUID to Host mapping within Cassandra
// TODO These UUID to Addr mapping doesn't belong in Cassandra, they
// are stop gap solution until we have Ringpop giving us the UUID
// along with the ip:port. Get rid these methods after the Ringpop
// patch is in
func (s *CassandraMetadataService) RegisterHostUUID(ctx thrift.Context, request *m.RegisterHostUUIDRequest) error {
	query := s.session.Query(sqlPutUUIDToHostAddr, request.GetHostUUID(), request.GetHostAddr(), request.GetHostName(), request.GetTtlSeconds())
	query.Consistency(s.lowConsLevel)
	if err := query.Exec(); err != nil {
		return &shared.InternalServiceError{Message: fmt.Sprintf("RegisterHostID - UUIDToHostAddr table update failed, uuid=%v, hostAddr=%v, hostName=%v, err=%v",
			request.GetHostUUID(), request.GetHostAddr(), request.GetHostName(), err)}
	}

	query = s.session.Query(sqlPutHostAddrToUUID, request.GetHostAddr(), request.GetHostUUID(), request.GetHostName(), request.GetTtlSeconds())
	query.Consistency(s.lowConsLevel)
	if err := query.Exec(); err != nil {
		return &shared.InternalServiceError{Message: fmt.Sprintf("RegisterHostID - HostAddrToUUID table update failed, uuid=%v, hostAddr=%v, hostName=%v err=%v",
			request.GetHostUUID(), request.GetHostAddr(), request.GetHostName(), err)}
	}

	return nil
}

// HostAddrToUUID returns the UUID corresponding to the hostID, if the mapping exist
func (s *CassandraMetadataService) HostAddrToUUID(ctx thrift.Context, hostAddr string) (string, error) {

	query := s.session.Query(sqlGetHostAddrToUUID, hostAddr)
	query.Consistency(s.lowConsLevel)
	iter := query.Iter()

	var result string

	if !iter.Scan(&result) {
		iter.Close()
		return "", &shared.EntityNotExistsError{Message: fmt.Sprintf("HostAddrToUUID - No mapping found, hostAddr=%v", hostAddr)}
	}

	if err := iter.Close(); err != nil {
		return "", &shared.InternalServiceError{Message: fmt.Sprintf("HostAddrToUUID - failed to close iterator")}
	}

	return result, nil
}

// UUIDToHostAddr returns the UUID corresponding to the hostID, if the mapping exist
func (s *CassandraMetadataService) UUIDToHostAddr(ctx thrift.Context, hostUUID string) (string, error) {

	query := s.session.Query(sqlGetUUIDToHostAddr, hostUUID)
	query.Consistency(s.lowConsLevel)
	iter := query.Iter()

	var result string

	if !iter.Scan(&result) {
		iter.Close()
		return "", &shared.EntityNotExistsError{Message: fmt.Sprintf("UUIDToHostAddr - No mapping found, uuid=%v", hostUUID)}
	}

	if err := iter.Close(); err != nil {
		return "", &shared.InternalServiceError{Message: fmt.Sprintf("UUIDToHostAddr - failed to close iterator")}
	}

	return result, nil
}

// ListHosts implements the corresponding TChanMetadataServiceClient API
func (s *CassandraMetadataService) ListHosts(ctx thrift.Context, listRequest *m.ListHostsRequest) (*m.ListHostsResult_, error) {
	if listRequest.GetLimit() <= 0 {
		return nil, &shared.BadRequestError{
			Message: "ListHosts: non-positive limit is not allowed in pagination endpoint",
		}
	}
	if listRequest.GetHostType() == m.HostType_UNKNOWN {
		return nil, &shared.BadRequestError{Message: "Missing HostType param, using HostToUUID or UUIDToHost"}
	}
	sql := sqlListUUIDToHostAddr
	if listRequest.GetHostType() == m.HostType_HOST {
		sql = sqlListHostAddrToUUID
	}
	query := s.session.Query(sql).Consistency(s.lowConsLevel)

	iter := query.PageSize(int(listRequest.GetLimit())).PageState(listRequest.GetPageToken()).Iter()

	if iter == nil {
		return nil, &shared.InternalServiceError{
			Message: "Query returned nil iterator",
		}
	}

	result := &m.ListHostsResult_{
		Hosts:         []*m.HostDescription{},
		NextPageToken: listRequest.PageToken,
	}
	d := getUtilHostDescription()
	count := int64(0)
	for iter.Scan(
		d.HostName,
		d.HostAddr,
		d.HostUUID) && count < listRequest.GetLimit() {
		// Get a new item within limit
		result.Hosts = append(result.Hosts, d)
		d = getUtilHostDescription()
		count++
	}

	nextPageToken := iter.PageState()
	result.NextPageToken = make([]byte, len(nextPageToken))
	copy(result.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, &shared.InternalServiceError{
			Message: err.Error(),
		}
	}

	return result, nil
}

// UpdateDestinationDLQCursors implements the corresponding TChanMetadataServiceClient API
func (s *CassandraMetadataService) UpdateDestinationDLQCursors(ctx thrift.Context, updateRequest *m.UpdateDestinationDLQCursorsRequest) (existing *shared.DestinationDescription, err error) {
	getDestination := &shared.ReadDestinationRequest{
		DestinationUUID: common.StringPtr(updateRequest.GetDestinationUUID()),
	}
	existing, err = s.ReadDestination(nil, getDestination)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, &shared.InternalServiceError{
			Message: "Empty results returned from ReadDestination without error",
		}
	}

	st := existing.GetStatus()
	if st != shared.DestinationStatus_ENABLED && st != shared.DestinationStatus_RECEIVEONLY && st != shared.DestinationStatus_SENDONLY {
		return nil, &m.IllegalStateError{Message: fmt.Sprintf("Can't purge/merge destination with status %v and id: %v", existing.GetStatus(), getDestination.GetDestinationUUID())}
	}

	var purgems, mergems *int64

	if updateRequest.DLQPurgeBefore != nil {
		purgems = common.Int64Ptr(unixNanoToCQLTimestamp(common.UnixNanoTime(updateRequest.GetDLQPurgeBefore())))
	} else {
		purgems = common.Int64Ptr(unixNanoToCQLTimestamp(common.UnixNanoTime(existing.GetDLQPurgeBefore())))
	}

	if updateRequest.DLQMergeBefore != nil {
		mergems = common.Int64Ptr(unixNanoToCQLTimestamp(common.UnixNanoTime(updateRequest.GetDLQMergeBefore())))
	} else {
		mergems = common.Int64Ptr(unixNanoToCQLTimestamp(common.UnixNanoTime(existing.GetDLQMergeBefore())))
	}

	query := s.session.Query(sqlUpdateDstDLQCursors, purgems, mergems, updateRequest.DestinationUUID)
	query.Consistency(s.highConsLevel)
	err = query.Exec()
	if err != nil {
		return nil, &shared.InternalServiceError{
			Message: "UpdateDestinationDLQCursors: " + err.Error(),
		}
	}

	if updateRequest.DLQPurgeBefore != nil {
		existing.DLQPurgeBefore = updateRequest.DLQPurgeBefore
	}
	if updateRequest.DLQMergeBefore != nil {
		existing.DLQMergeBefore = updateRequest.DLQMergeBefore
	}

	return existing, nil
}

func getThriftContextValue(ctx thrift.Context, valueName string) string {
	if ctx == nil {
		return "unknown"
	}
	headerDict := ctx.Headers()
	value, existed := headerDict[valueName]
	if !existed {
		return "unknown"
	}
	return value
}
