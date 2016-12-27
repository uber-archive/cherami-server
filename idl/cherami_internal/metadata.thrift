// Copyright (c) 2016 Uber Technologies, Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
namespace java com.uber.cherami

include "shared.thrift"

exception IllegalStateError {
  1: required string message
}

enum ConsumerGroupExtentStatus {
  OPEN,
  CONSUMED,
  DELETED
}

// Either path or destinationUUID are required
// Destination in DELETED state are returned only when destinationUUID is specified
// as multiple deleted destinations might exist for the same path.
struct ReadDestinationRequest {
  1: optional string path
  2: optional string destinationUUID
}

struct UpdateDestinationDLQCursorsRequest {
  1: optional string destinationUUID
  2: optional i64 (js.type = "Long") dLQPurgeBefore
  3: optional i64 (js.type = "Long") dLQMergeBefore
}

struct DeleteDestinationUUIDRequest {
  1: optional string UUID
  2: optional i32 ttlSeconds
}

enum HostType {
  UNKNOWN = -1,
  /**
   * UNKNOW is by default, which will get badrequest error. Type need to set to HOST or UUID
  **/
  HOST,
  /**
   * HOST type means getting the host information from hostaddr_to_uuid table
  **/
  UUID
  /**
   * UUID type means getting the host information from uuit_to_hostaddr table, which contains history data
  **/
}
struct HostDescription {
  1: optional string hostName
  2: optional string hostAddr
  3: optional string hostUUID
}

struct ListHostsRequest {
  1: optional HostType hostType
  2: optional binary pageToken
  3: optional i64 (js.type = "Long") limit
}

struct ListHostsResult {
  1: optional list<HostDescription> hosts
  2: optional binary nextPageToken
}

struct ReadConsumerGroupRequest {
  1: optional string destinationPath
  2: optional string consumerGroupName
  3: optional string destinationUUID
  4: optional string consumerGroupUUID
}

struct ListEntityOpsRequest {
  1: optional string entityUUID
  2: optional string entityName
  3: optional string entityType
  4: optional binary pageToken
  5: optional i64 (js.type = "Long") limit
}

struct ListEntityOpsResult {
  1: optional list<shared.EntityOpsDescription> entityOps
  2: optional binary nextPageToken
}

struct ListConsumerGroupRequest {
  1: optional string destinationPath
  2: optional string consumerGroupName
  3: optional string destinationUUID
  4: optional binary pageToken
  5: optional i64 (js.type = "Long") limit
}

struct ListConsumerGroupResult {
  1: optional list<shared.ConsumerGroupDescription> consumerGroups
  2: optional binary nextPageToken
}

struct ConsumerGroupExtent {
  1:  optional string extentUUID
  2:  optional string consumerGroupUUID
  3:  optional ConsumerGroupExtentStatus status
  4:  optional i64 (js.type = "Long") ackLevelOffset // TODO: Define inclusive or exclusive
  5:  optional string outputHostUUID // Mutable
  6:  optional list<string> storeUUIDs
  7:  optional string connectedStoreUUID
  8:  optional i64 (js.type = "Long") ackLevelSeqNo
  9:  optional double ackLevelSeqNoRate
  10: optional i64 (js.type = "Long") readLevelOffset
  11: optional i64 (js.type = "Long") readLevelSeqNo
  12: optional double readLevelSeqNoRate
  13: optional i64 (js.type = "Long") writeTime // from CQL writeTime(ack_level_offset)
}

struct UpdateExtentStatsRequest {
  1: optional string destinationUUID
  2: optional string extentUUID
  3: optional shared.ExtentStatus status
  4: optional string archivalLocation
}

struct UpdateExtentStatsResult {
  1: optional shared.ExtentStats extentStats
}

/***** Request and Reply structures *****/

struct ReadExtentStatsRequest {
  1: optional string destinationUUID
  2: optional string extentUUID
}

struct ReadExtentStatsResult {
  1: optional shared.ExtentStats extentStats
}

struct ListInputHostExtentsStatsRequest {
  1: optional string destinationUUID
  2: optional string inputHostUUID
  3: optional shared.ExtentStatus status
}

struct ListInputHostExtentsStatsResult {
  // TODO: Consider pagination
  1: optional list<shared.ExtentStats> extentStatsList
}

struct ListStoreExtentsStatsRequest {
  1: optional string storeUUID
  2: optional shared.ExtentStatus status
  3: optional shared.ExtentReplicaReplicationStatus replicationStatus
}

struct ListStoreExtentsStatsResult {
  // TODO: Consider pagination
  1: optional list<shared.ExtentStats> extentStatsList
}

struct ReadStoreExtentReplicaStatsRequest {
  1: optional string storeUUID
  2: optional string extentUUID
}

struct ReadStoreExtentReplicaStatsResult {
  1: optional shared.ExtentStats extent
}

struct SealExtentRequest {
  1: optional string destinationUUID // Required
  2: optional string extentUUID // Required
}

struct UpdateStoreExtentReplicaStatsRequest {
  1: optional string extentUUID
  // Each member of the list is used to update a member with the same storeUUID in ExtentStats.
  2: optional list<shared.ExtentReplicaStats> replicaStats
  3: optional string storeUUID
  4: optional shared.ExtentReplicaReplicationStatus replicationStatus
}

struct UpdateExtentReplicaStatsRequest {
  1: optional string destinationUUID
  2: optional string extentUUID
  3: optional string inputHostUUID
  // The list is not replacing the replicaStats in ExtentStats.
  // Each member of the list is used to update a member with the same storeUUID in ExtentStats.
  4: optional list<shared.ExtentReplicaStats> replicaStats
}

struct SetAckOffsetRequest {
  1:  optional string extentUUID
  2:  optional string consumerGroupUUID
  3:  optional string outputHostUUID
  4:  optional string connectedStoreUUID
  5:  optional ConsumerGroupExtentStatus status
  6:  optional i64 (js.type = "Long") ackLevelAddress
  7:  optional i64 (js.type = "Long") ackLevelSeqNo
  8:  optional double ackLevelSeqNoRate
  9:  optional i64 (js.type = "Long") readLevelAddress
  10: optional i64 (js.type = "Long") readLevelSeqNo
  11: optional double readLevelSeqNoRate
}

struct UpdateConsumerGroupExtentStatusRequest {
  1: optional string consumerGroupUUID
  2: optional string extentUUID
  3: optional ConsumerGroupExtentStatus status
}

struct CreateConsumerGroupExtentRequest {
  1: optional string destinationUUID
  2: optional string extentUUID
  3: optional string consumerGroupUUID
  4: optional string outputHostUUID
  5: optional list<string> storeUUIDs
}

struct ReadConsumerGroupExtentRequest {
  1: optional string destinationUUID
  2: optional string extentUUID
  3: optional string consumerGroupUUID
}

struct ReadConsumerGroupExtentResult {
  1: optional ConsumerGroupExtent extent
}

struct SetOutputHostRequest {
  1: optional string destinationUUID,
  2: optional string extentUUID
  3: optional string consumerGroupUUID
  4: optional string outputHostUUID
}

struct ReadConsumerGroupExtentsRequest {
  1: optional string destinationUUID // Required
  2: optional string consumerGroupUUID // Required
  3: optional i32 maxResults
  // When included return only extents that belong to the specified outputHost
  4: optional string outputHostUUID
  5: optional ConsumerGroupExtentStatus status
  6: optional binary pageToken
}

struct ReadConsumerGroupExtentsResult {
  1: optional list<ConsumerGroupExtent> extents
 10: optional binary nextPageToken
}

struct ReadConsumerGroupExtentsByExtUUIDRequest {
  1: optional string extentUUID
  2: optional binary pageToken
  3: optional i64 (js.type = "Long") limit
}

struct ReadConsumerGroupExtentsByExtUUIDResult {
  1: optional list<ConsumerGroupExtent> cgExtents
  2: optional binary nextPageToken
}

struct RegisterHostUUIDRequest {
  1: optional string hostUUID
  2: optional string hostAddr
  3: optional string hostName
  4: optional i64 (js.type = "Long") ttlSeconds
}

struct MoveExtentRequest {
  1: optional string destinationUUID // Required
  2: optional string extentUUID // Required
  3: optional string newDestinationUUID // Required
  4: optional string consumerGroupVisibilityUUID // Required
}

struct CreateHostInfoRequest {
  1: optional string hostname // Required
  2: optional map<string, string> properties // Required
}

struct UpdateHostInfoRequest {
  1: optional string hostname // Required
  2: optional map<string, string> properties // Required
}

struct DeleteHostInfoRequest {
  1: optional string hostname    // Required
  2: optional string propertyKey // optional
}

struct ReadHostInfoRequest {
  1: optional string hostname // Required
}

struct ReadHostInfoResult {
  1: optional string hostname // Required
  2: optional map<string, string> properties // Required
}

struct ServiceConfigItem {
  1: optional string serviceName
  2: optional string serviceVersion
  3: optional string sku
  4: optional string hostname
  5: optional string configKey
  6: optional string configValue
}

struct CreateServiceConfigRequest {
  1: optional ServiceConfigItem configItem // Required
}

struct UpdateServiceConfigRequest {
  1: optional ServiceConfigItem configItem // Required
}

struct DeleteServiceConfigRequest {
  1: optional string serviceName   // Required
  2: optional string serviceVersion
  3: optional string sku
  4: optional string hostname
  5: optional string configKey
}

struct ReadServiceConfigRequest {
  1: optional string serviceName  // Required
  2: optional string serviceVersion
  3: optional string sku
  4: optional string hostname
  5: optional string configKey
}

struct ReadServiceConfigResult {
  1: optional list<ServiceConfigItem> configItems // Required
}

service MetadataExposable {
  shared.DestinationDescription readDestination(1: ReadDestinationRequest getRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError
    )

  shared.ListDestinationsResult listDestinations(1: shared.ListDestinationsRequest listRequest)
    throws (
      1: shared.BadRequestError requestError,
      2: shared.InternalServiceError internalServiceError
    )

  shared.ListDestinationsResult listDestinationsByUUID(1: shared.ListDestinationsByUUIDRequest listRequest)
    throws (
      1: shared.BadRequestError requestError,
      2: shared.InternalServiceError internalServiceError
    )

  shared.ListExtentsStatsResult listExtentsStats(1: shared.ListExtentsStatsRequest request)
    throws (
      1: shared.BadRequestError requestError,
      2: shared.InternalServiceError internalServiceError
    )

  ListInputHostExtentsStatsResult listInputHostExtentsStats(1: ListInputHostExtentsStatsRequest request)
    throws (
      1: shared.BadRequestError requestError
      2: shared.InternalServiceError internalError
    )

  ListStoreExtentsStatsResult listStoreExtentsStats(1: ListStoreExtentsStatsRequest request)
    throws (
      1: shared.BadRequestError requestError,
      2: shared.InternalServiceError internalError
    )

  ReadExtentStatsResult readExtentStats(1: ReadExtentStatsRequest request)
    throws (
      1: shared.BadRequestError requestError,
      2: shared.InternalServiceError internalError
    )

  ReadConsumerGroupExtentResult readConsumerGroupExtent(1: ReadConsumerGroupExtentRequest request)
    throws (
      1: shared.BadRequestError requestError
      2: shared.InternalServiceError internalError
    )

  ReadConsumerGroupExtentsResult readConsumerGroupExtents(1: ReadConsumerGroupExtentsRequest request)
    throws (
      1: shared.BadRequestError requestError
      2: shared.InternalServiceError internalError
    )

  string hostAddrToUUID(1: string hostAddr) 
    throws (
      1: shared.EntityNotExistsError notExistsError, 
      2: shared.InternalServiceError internalError
    )

  string uUIDToHostAddr(1: string hostUUID) 
    throws (
      1: shared.EntityNotExistsError notExistsError, 
      2: shared.InternalServiceError internalError
    )

  ListHostsResult listHosts(1: ListHostsRequest request)
    throws (
      1: shared.BadRequestError requestError
      2: shared.InternalServiceError internalError
    )

  ListConsumerGroupResult listAllConsumerGroups(1: ListConsumerGroupRequest listRequest)
    throws (
      1: shared.BadRequestError requestError
      2: shared.InternalServiceError internalError
    )

  ListEntityOpsResult ListEntityOps(1: ListEntityOpsRequest listRequest)
    throws (
      1: shared.BadRequestError requestError
      2: shared.InternalServiceError internalError
    )

  ListConsumerGroupResult listConsumerGroups(1: ListConsumerGroupRequest listRequest)
    throws (
      1: shared.BadRequestError requestError
      2: shared.InternalServiceError internalError
    )

  ReadConsumerGroupExtentsByExtUUIDResult readConsumerGroupExtentsByExtUUID(1: ReadConsumerGroupExtentsByExtUUIDRequest request)
    throws (
      1: shared.BadRequestError requestError
      2: shared.InternalServiceError internalError
    )

  shared.ConsumerGroupDescription readConsumerGroup(1: ReadConsumerGroupRequest getRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError
    )

  shared.ConsumerGroupDescription readConsumerGroupByUUID(1: ReadConsumerGroupRequest request)
    throws (
      1: shared.BadRequestError requestError
      2: shared.EntityNotExistsError entityError,
      3: shared.InternalServiceError internalServiceError
    )

  // updateServiceConfig updates a single config value
  void updateServiceConfig(1: UpdateServiceConfigRequest request) 
    throws (
      1: shared.InternalServiceError error
    )
}

service MetadataService extends MetadataExposable {

  /*********************************************/
  /***** Destination CRUD **********************/
  shared.DestinationDescription createDestination(1: shared.CreateDestinationRequest createRequest)
    throws (
      1: shared.EntityAlreadyExistsError entityExistsError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError
    )

  shared.DestinationDescription createDestinationUUID(1: shared.CreateDestinationUUIDRequest createRequest)
    throws (
      1: shared.EntityAlreadyExistsError entityExistsError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError
    )

  shared.DestinationDescription updateDestination(1: shared.UpdateDestinationRequest updateRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError
    )

  shared.DestinationDescription updateDestinationDLQCursors(1: UpdateDestinationDLQCursorsRequest updateRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError
    )

  void deleteDestination(1: shared.DeleteDestinationRequest deleteRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError
    )

  void deleteDestinationUUID(1: DeleteDestinationUUIDRequest deleteRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError
    )


  /*********************************************/

  /***** ConsumerGroup CRUD ********************/
  shared.ConsumerGroupDescription createConsumerGroup(1: shared.CreateConsumerGroupRequest createRequest)
    throws (
      1: shared.EntityAlreadyExistsError entityExistsError,
      2: shared.BadRequestError requestError,
      3: shared.EntityNotExistsError entityNotExistsError,
      4: shared.InternalServiceError internalServiceError
    )

  shared.ConsumerGroupDescription updateConsumerGroup(1: shared.UpdateConsumerGroupRequest updateRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError
    )

  void deleteConsumerGroup(1: shared.DeleteConsumerGroupRequest deleteRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError
    )


  /*********************************************/

  /***** Extent Management ********************/
  shared.CreateExtentResult createExtent(1: shared.CreateExtentRequest request)
    throws (
      1: shared.BadRequestError requestError
      2: shared.EntityAlreadyExistsError entityExistsError
      3: shared.InternalServiceError internalServiceError
    )

  UpdateExtentStatsResult updateExtentStats(1: UpdateExtentStatsRequest request)
    throws (
      1: shared.BadRequestError requestError
      2: shared.EntityNotExistsError entityNotExistsError
      3: shared.InternalServiceError internalServiceError
    )

  // To be called by store nodes
  ReadStoreExtentReplicaStatsResult readStoreExtentReplicaStats(1: ReadStoreExtentReplicaStatsRequest request)
    throws (
      1: shared.BadRequestError requestError
      2: shared.InternalServiceError internalServiceError
    )

  void sealExtent(1: SealExtentRequest request)
    throws (
      1: shared.BadRequestError requestError
      2: IllegalStateError illegalStateError
      3: shared.InternalServiceError internalServiceError
    )

  void updateExtentReplicaStats(1: UpdateExtentReplicaStatsRequest request)
    throws (
      1: shared.BadRequestError requestError
      2: shared.InternalServiceError internalServiceError
    )

  void updateStoreExtentReplicaStats(1: UpdateStoreExtentReplicaStatsRequest request)
    throws (
      1: shared.BadRequestError requestError
      2: shared.InternalServiceError internalServiceError
    )

  void moveExtent(1: MoveExtentRequest request)
    throws (
      1: shared.BadRequestError requestError
      2: IllegalStateError illegalStateError
      3: shared.InternalServiceError internalServiceError
    )
  /*********************************************/

  /***** Consumer Group Extent Management ********************/

  void setAckOffset(1: SetAckOffsetRequest request)
    throws (
      1: shared.InternalServiceError internalServiceError
    )

  void updateConsumerGroupExtentStatus(1: UpdateConsumerGroupExtentStatusRequest request)
    throws (
      1: shared.BadRequestError requestError
      2: shared.InternalServiceError internalServiceError
      3: shared.EntityNotExistsError notExistsError
    )

  void createConsumerGroupExtent(1: CreateConsumerGroupExtentRequest request)
    throws (
      1: shared.InternalServiceError internalServiceError
    )

  void setOutputHost(1: SetOutputHostRequest request)
    throws (
      1: shared.InternalServiceError internalServiceError
    )

  void registerHostUUID(1: RegisterHostUUIDRequest request) throws (1: shared.InternalServiceError error)


  /**** Hardware Info and Config Management ****/

  // createHostInfo adds a single hardware property for a single hostname
  void createHostInfo(1: CreateHostInfoRequest request) throws (1: shared.InternalServiceError error)

  // updateHostInfo updates a single hardware property for a single hostname
  void updateHostInfo(1: UpdateHostInfoRequest request) throws (1: shared.InternalServiceError error)

  // deleteHostInfo deletes all hardware properties associated with a
  // single hostname. If a propertyKey is specified, this API will only
  // delete the specific property
  void deleteHostInfo(1: DeleteHostInfoRequest request) throws (1: shared.InternalServiceError error)

  // readHostInfo returns list of hardware properties for a single hostname
  ReadHostInfoResult readHostInfo(1: ReadHostInfoRequest request) throws (1: shared.InternalServiceError error)

  // createServiceConfig adds a single config value
  void createServiceConfig(1: CreateServiceConfigRequest request) throws (1: shared.InternalServiceError error)

  // deleteServiceConfig deletes one or more config values matching the given criteria
  void deleteServiceConfig(1: DeleteServiceConfigRequest request) throws (1: shared.InternalServiceError error)

  // readServiceConfig returns all config that matches the
  // given set of input criteria. The returned result is a
  // list of config key,values
  ReadServiceConfigResult readServiceConfig(1: ReadServiceConfigRequest request) throws (1: shared.InternalServiceError error)
}
