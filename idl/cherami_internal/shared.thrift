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

exception EntityNotExistsError {
  1: required string message
}

exception EntityAlreadyExistsError {
  1: required string message
}

exception EntityDisabledError {
  1: required string message
}

exception BadRequestError {
  1: required string message
}

exception InternalServiceError {
  1: required string message
}

/**
 * We support 2 types of Destinations, backed up by their own BStore implementations.
 * DestinationType needs to be specified at the time of creation and is immutable.
**/
enum DestinationType {
 /**
   * This is used when the destination type is unavailable/unknown.
  **/
  UNKNOWN = -1,

  /**
   * This type of destination supports very fast throughput but does not support scheduling tasks
   * with a delay.  It is backed by it's own BStore with WAL for writes and indexed reads on sequence numbers.
   *
  **/
  PLAIN,

  /**
   * This type of destination is designed to support scheduling tasks with a delay.  It is backed by a different
   * BStore with a WAL and indexed storage based on both Sequence numbers and Schedule Time.  These are slightly
   * heavier than PLAIN queues and not optimal for scenario's which does not require timers.
  **/
  TIMER,

  /**
   * Limited throughput destination that guarantees fully consistent odered view of messages.
   * Each message has associated sequence number. Sequence numbers are guaranteed to be sequential.
   * Messages with incorrect sequence number provided by publisher are rejected.
  **/
  LOG
}

enum DestinationStatus {
  ENABLED,
  DISABLED,
  SENDONLY,
  RECEIVEONLY,
  DELETING,
  DELETED,
}

/**
 * Keep this in sync with the ChecksumOption in cherami.thrift
**/
enum ChecksumOption {
  /**
   * Use CRC32 IEEE checksum option.
  **/
  CRC32IEEE,
  /**
   * Use md5 checksum option.
  **/
  MD5
}

enum ConsumerGroupType {
    /**
     * Each consumer receives disjoint set of messages and acks them individually.
    **/
    COMPETING,

    /**
     * Each consumer receives all messages from a specified address. Valid for LOG destinations only.
    **/
    STREAMING
}

enum ConsumerGroupStatus {
  ENABLED,
  DISABLED,
  DELETED
}

struct DestinationZoneConfig {
 // 10: optional Zone deprecatedZoneName
 11: optional string zone                        // zone name
 20: optional bool allowPublish                  // whether we allow publishing from this zone
 30: optional bool allowConsume                  // whether we allow consuming from this zone
 40: optional bool alwaysReplicateTo             // whether we need to replicate to this zone even if there’s no consumer group in that zone
 50: optional i32 remoteExtentReplicaNum         // the # of replica we need to have for remote extent
}

struct SchemaInfo {
  1: optional string type
  2: optional i32 version
  3: optional binary data // UTF-8 encoded byte array that stores schema data
  4: optional string source // A URI that refers to the downloading source of schema
  5: optional i64 (js.type = "Long") createdTimeUtc
}

struct DestinationDescription {
  1: optional string path
  2: optional string destinationUUID
  3: optional DestinationType type
  4: optional DestinationStatus status
  5: optional i32 consumedMessagesRetention
  6: optional i32 unconsumedMessagesRetention
  7: optional string ownerEmail
  8: optional string dLQConsumerGroupUUID
  9: optional i64 (js.type = "Long") dLQPurgeBefore
 10: optional i64 (js.type = "Long") dLQMergeBefore
 11: optional ChecksumOption checksumOption
 20: optional bool isMultiZone
 // 21: optional DestinationZoneConfigs zoneConfigs
 22: optional list<DestinationZoneConfig> zoneConfigs
 30: optional SchemaInfo schemaInfo // Latest schema for this destination
}

struct CreateDestinationRequest {
  1: optional string path
  2: optional DestinationType type
  3: optional i32 consumedMessagesRetention
  4: optional i32 unconsumedMessagesRetention
  5: optional string ownerEmail
  6: optional string dLQConsumerGroupUUID
  7: optional ChecksumOption checksumOption
 10: optional bool isMultiZone
 // 11: optional DestinationZoneConfigs zoneConfigs
 12: optional list<DestinationZoneConfig> zoneConfigs
 20: optional SchemaInfo schemaInfo
}

struct CreateDestinationUUIDRequest {
 10: optional CreateDestinationRequest request   // original request
 20: optional string destinationUUID	            // destination uuid to use instead of generating new
}

struct ListDestinationsRequest {
  1: optional string prefix
  4: optional bool multiZoneOnly
  2: optional binary pageToken
  3: optional i64 (js.type = "Long") limit
}

struct ListDestinationsByUUIDRequest {
  // 1: optional string destinationUUID
  2: optional bool multiZoneOnly
  3: optional bool validateAgainstPathTable
 10: optional binary pageToken
 11: optional i64 (js.type = "Long") limit
}

struct ListDestinationsResult {
  1: optional list<DestinationDescription> destinations
  2: optional binary nextPageToken
}

struct UpdateDestinationRequest {
  1: optional string destinationUUID
  2: optional DestinationStatus status
  3: optional i32 consumedMessagesRetention
  4: optional i32 unconsumedMessagesRetention
  5: optional string ownerEmail
  6: optional ChecksumOption checksumOption
 10: optional SchemaInfo schemaInfo
}

struct DeleteDestinationRequest {
  1: optional string path
}

struct ConsumerGroupZoneConfig {
 // 10: optional Zone deprecatedZoneName
 11: optional string zone                        // zone name
 20: optional bool visible                       // whether the consumer group is visible in this zone
}


struct EntityOpsDescription {
  1: optional string entityUUID
  2: optional string entityName
  3: optional string entityType
  4: optional string hostName
  5: optional string serviceName
  6: optional string userName
  7: optional string opsType
  8: optional string opsTime
  9: optional string opsContent
}

struct ConsumerGroupDescription {
  1: optional string consumerGroupUUID
  2: optional string destinationUUID
  3: optional string consumerGroupName
  4: optional i64 (js.type = "Long") startFrom
  5: optional ConsumerGroupStatus status
  6: optional i32 lockTimeoutSeconds
  7: optional i32 maxDeliveryCount
  8: optional i32 skipOlderMessagesSeconds
  9: optional string deadLetterQueueDestinationUUID
 10: optional string ownerEmail
 12: optional ConsumerGroupType consumerGroupType
 20: optional bool isMultiZone
 // 21: optional ConsumerGroupZoneConfigs zoneConfigs
 22: optional string activeZone
 23: optional list<ConsumerGroupZoneConfig> zoneConfigs
}

struct CreateConsumerGroupRequest {
  1: optional string destinationPath
  2: optional string consumerGroupName
  3: optional i64 (js.type = "Long") startFrom
  4: optional i32 lockTimeoutSeconds
  5: optional i32 maxDeliveryCount
  6: optional i32 skipOlderMessagesSeconds
  7: optional string deadLetterQueueDestinationUUID
  8: optional string ownerEmail
 10: optional bool isMultiZone
 // 11: optional ConsumerGroupZoneConfigs zoneConfigs
 12: optional string activeZone
 13: optional list<ConsumerGroupZoneConfig> zoneConfigs
}

struct CreateConsumerGroupUUIDRequest {
 10: optional CreateConsumerGroupRequest request // original request
 20: optional string consumerGroupUUID           // consumer group uuid to use instead of generating new
}

struct UpdateConsumerGroupRequest {
  1: optional string destinationPath
  2: optional string consumerGroupName
  3: optional ConsumerGroupStatus status
  4: optional i32 lockTimeoutSeconds
  5: optional i32 maxDeliveryCount
  6: optional i32 skipOlderMessagesSeconds
  7: optional string deadLetterQueueDestinationUUID
  8: optional string ownerEmail
}

struct DeleteConsumerGroupRequest {
  1: optional string destinationPath
  2: optional string consumerGroupName
  3: optional string destinationUUID
}

struct Extent {
  1: optional string extentUUID
  2: optional string destinationUUID
  3: optional list<string> storeUUIDs
  4: optional string inputHostUUID // Immutable
  5: optional string originZone
}

enum ExtentStatus {
  OPEN, // can be written to
  SEALED,
  CONSUMED, // all registered consumer groups consumed it
  ARCHIVED, // deleted from store node, but available from archive
  DELETED // deleted from both store node and archive
}

enum ExtentReplicaStatus {
  CREATED, // only metadata exists
  OPEN,
  SEALED,
  DELETED,
  CORRUPTED, // Checksumming found that extent is not valid
  MISSING // Metadata says that it exists but the store cannot find it
}

struct ExtentReplicaStats { // same as cherami.ExtentInfo, except for storeUUID and the status type
  1:  optional string storeUUID
  2:  optional string extentUUID
  3:  optional i64 (js.type = "Long") createdAt
  4:  optional i64 (js.type = "Long") beginAddress // first available message storage address
  5:  optional i64 (js.type = "Long") lastAddress // last message storage address
  6:  optional i64 (js.type = "Long") beginSequence // first available message sequence number; e.g. 0
  7:  optional i64 (js.type = "Long") lastSequence  // last message sequence number; non-inclusive, e.g. begin = 0, last = 0, means zero messages written
  8:  optional i64 (js.type = "Long") beginEnqueueTimeUtc // first message enqueue (write) time
  9:  optional i64 (js.type = "Long") lastEnqueueTimeUtc // last message enqueue (write) time
  10: optional i64 (js.type = "Long") sizeInBytes
  11: optional ExtentReplicaStatus status
  12: optional i64 (js.type = "Long") beginTime // first available message delivery time
  13: optional i64 (js.type = "Long") endTime // last available message delivery time
  14: optional i64 (js.type = "Long") availableAddress // last available message storage address
  15: optional i64 (js.type = "Long") availableSequence  // last available message sequence number; non-inclusive, e.g. begin = 0, last = 0, means zero messages available
  16: optional double availableSequenceRate // rate of change of the availableSequence, 1/seconds
  17: optional double lastSequenceRate // rate of change of the lastSequence, 1/seconds
  18: optional double sizeInBytesRate
  19: optional i64 (js.type = "Long") writeTime // from CQL writeTime(replica_stats)
}

struct ExtentStats {
  1: optional Extent extent
  2: optional ExtentStatus status
  9: optional i64 (js.type = "Long") statusUpdatedTimeMillis
  3: optional i64 (js.type = "Long") createdTimeMillis
  6: optional list<ExtentReplicaStats> replicaStats
  7: optional string archivalLocation
  8: optional string consumerGroupVisibility
}

enum ExtentReplicaReplicationStatus {
  INVALID,
  PENDING,
  DONE,
}

struct CreateExtentRequest {
  1: optional Extent extent
}

struct CreateExtentResult {
  1: optional ExtentStats extentStats
}

struct ListExtentsStatsRequest {
  1: optional string destinationUUID
  2: optional ExtentStatus status
  3: optional bool localExtentsOnly
 10: optional binary pageToken
 11: optional i64 (js.type = "Long") limit
}

struct ListExtentsStatsResult {
  1: optional list<ExtentStats> extentStatsList
 10: optional binary nextPageToken
}
