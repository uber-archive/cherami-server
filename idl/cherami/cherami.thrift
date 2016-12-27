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

exception InvalidAddressError {
  1: required string message
}

exception InternalServiceError {
  1: required string message
}

exception TimeoutError {
  1: required string message
}

exception QueueCacheMissError {
  1: required string message
}

enum Protocol {
  TCHANNEL,
  WS,       // websocket
  WSS       // websocket secure
}

enum DestinationStatus {
  ENABLED,
  DISABLED,
  SENDONLY,
  RECEIVEONLY
}

enum ConsumerGroupStatus {
  ENABLED,
  DISABLED
}

/**
 * We support 2 type of Destinations, backed up by their own BStore implementations.
 * DestinationType needs to be specified at the time of creation and is immutable.
**/
enum DestinationType {
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
   * !!!!! NOT YET SUPPORTED. !!!!!!
   * Limited throughput destination that guarantees fully consistent odered view of messages.
   * Each message has associated sequence number. Sequence numbers are guaranteed to be sequential.
   * Messages with incorrect sequence number provided by publisher are rejected.
  **/
  LOG
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

/**
 * This describes the entity and associated configuration, used by client application to send messages.
 *
 * @param path.  Path which uniquely identifies the destination.
 * @param type.  Type of destination (PLAIN, TIMER).
 * @param status.  Status of destination.
 * @param consumedMessagesRetention.  Time in seconds to keep consumed messages before deleting from storage.
 * @param unconsumedMessagesRetention.  Time in seconds to keep messages that may not have been consumed, before deleting from storage.
 * @param createdAt.  Time when destination was created.
**/
struct DestinationDescription {
  1: optional string path
  2: optional DestinationType type
  3: optional DestinationStatus status
  4: optional i32 consumedMessagesRetention
  5: optional i32 unconsumedMessagesRetention
  6: optional string destinationUUID
  7: optional string ownerEmail
  8: optional ChecksumOption checksumOption = ChecksumOption.CRC32IEEE
 10: optional bool isMultiZone
 11: optional DestinationZoneConfigs zoneConfigs
 20: optional SchemaInfo schemaInfo // Latest schema for this destination
}

struct SchemaInfo {
  1: optional string type
  2: optional i32 version
  3: optional binary data // UTF-8 encoded byte array that stores schema data
  4: optional string source // A URI that refers to the downloading source of schema
  5: optional i64 (js.type = "Long") createdTimeUtc
}

struct DestinationZoneConfig {
 // 10: optional Zone deprecatedZoneName
 11: optional string zone                        // zone name
 20: optional bool allowPublish                  // whether we allow publishing from this zone
 30: optional bool allowConsume                  // whether we allow consuming from this zone
 40: optional bool alwaysReplicateTo             // whether we need to replicate to this zone even if there’s no consumer group in that zone
 50: optional i32 remoteExtentReplicaNum         // the # of replica we need to have for remote extent
}

struct DestinationZoneConfigs {
 10: optional list<DestinationZoneConfig> configs
}

struct CreateDestinationRequest {
  1: optional string path
  2: optional DestinationType type
  3: optional i32 consumedMessagesRetention
  4: optional i32 unconsumedMessagesRetention
  5: optional string ownerEmail
  6: optional ChecksumOption checksumOption = ChecksumOption.CRC32IEEE
 10: optional bool isMultiZone
 11: optional DestinationZoneConfigs zoneConfigs
 20: optional SchemaInfo schemaInfo
}

struct ReadDestinationRequest {
  1: optional string path
}

struct UpdateDestinationRequest {
  1: optional string path
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

struct ListDestinationsRequest {
  1: optional string prefix
  2: optional binary pageToken
  3: optional i64 (js.type = "Long") limit
}

struct ListDestinationsResult {
  1: optional list<DestinationDescription> destinations
  2: optional binary nextPageToken
}

/**
* This describes the entity and associated configuration, used by client application to consume messages from
* a destination.
*
* @param destinationPath.  Path which uniquely identifies the destination.
* @param consumerGroupName.  Unique identifier for each group of consumers.
* @param startFrom.  Timestamp used to start consuming messages from destination.  This needs to be provided during
* registration of the ConsumerGroup and cannot be updated later.
* @param lockTimeoutInSeconds.  Seconds to wait before redelivering message to another consumer.
* @param maxDeliveryCount.  Number of times trying to deliver the message without Ack before giving up and moving the
* message to DLQ.
* @param skipOlderMessagesInSeconds.  This is useful for consumers who always wants to keep up and don't care about
* backlog older than certain duration.
* @param createdAt.  Time when ConsumerGroup was registered.
**/
struct ConsumerGroupDescription {
  1: optional string destinationPath
  2: optional string consumerGroupName
  3: optional i64 (js.type = "Long") startFrom
  4: optional ConsumerGroupStatus status
  5: optional i32 lockTimeoutInSeconds
  6: optional i32 maxDeliveryCount
  7: optional i32 skipOlderMessagesInSeconds
  8: optional string deadLetterQueueDestinationUUID
  9: optional string destinationUUID
 10: optional string consumerGroupUUID
 11: optional string ownerEmail
 12: optional ConsumerGroupType consumerGroupType
 20: optional bool isMultiZone
 21: optional ConsumerGroupZoneConfigs zoneConfigs
}

struct ConsumerGroupZoneConfig {
 // 10: optional Zone deprecatedZoneName
 11: optional string zone                        // zone name
 20: optional bool visible                       // whether the consumer group is visible in this zone
}

struct ConsumerGroupZoneConfigs {
 10: optional list<ConsumerGroupZoneConfig> configs
 // 20: optional Zone deprecatedActiveZone
 21: optional string activeZone
}

struct CreateConsumerGroupRequest {
  1: optional string destinationPath
  2: optional string consumerGroupName
  3: optional i64 (js.type = "Long") startFrom
  4: optional i32 lockTimeoutInSeconds
  5: optional i32 maxDeliveryCount
  6: optional i32 skipOlderMessagesInSeconds
  7: optional string ownerEmail
  8: optional ConsumerGroupType consumerGroupType // Default is COMPETING
 10: optional bool isMultiZone
 11: optional ConsumerGroupZoneConfigs zoneConfigs
}

struct ReadConsumerGroupRequest {
  1: optional string destinationPath
  2: optional string consumerGroupName
}

struct UpdateConsumerGroupRequest {
  1: optional string destinationPath
  2: optional string consumerGroupName
  3: optional ConsumerGroupStatus status
  4: optional i32 lockTimeoutInSeconds
  5: optional i32 maxDeliveryCount
  6: optional i32 skipOlderMessagesInSeconds
  7: optional string ownerEmail
}

struct DeleteConsumerGroupRequest {
  1: optional string destinationPath
  2: optional string consumerGroupName
}

struct ListConsumerGroupRequest {
  1: optional string destinationPath
  2: optional string consumerGroupName
  3: optional binary pageToken
  4: optional i64 (js.type = "Long") limit
}

struct ListConsumerGroupResult {
  1: optional list<ConsumerGroupDescription> consumerGroups
  2: optional binary nextPageToken
}

struct PurgeDLQForConsumerGroupRequest {
  1: optional string destinationPath
  2: optional string consumerGroupName
}

struct MergeDLQForConsumerGroupRequest {
  1: optional string destinationPath
  2: optional string consumerGroupName
}

/**
* Address of BIn/BOut nodes used by client applications to open direct streams for publishing/consuming messages.
* Publishers are expected to discover HostAddress for all BIn nodes serving a particular destination by calling
* ReadDestinationHosts API on the Frontend and then use the address to directly open a stream to BIn node for
* publishing messages.  Similarly Consumers are expected to discover HostAddress for all BOut nodes serving a
* particular pair of Destination-ConsumerGroup by calling the ReadConsumerGroupHosts API on the Frontend and then
* use the address to directly open a stream to BOut node for consuming messages.
**/
struct HostAddress {
  1: optional string host
  2: optional i32 port
}

struct HostProtocol {
 10: optional list<HostAddress> hostAddresses
 20: optional Protocol protocol
 30: optional bool deprecated
}

struct ReadDestinationHostsRequest {
  1: optional string path
}

struct ReadDestinationHostsResult {
  1: optional list<HostAddress> hostAddresses // To be deprecated by hostProtocols
 10: optional list<HostProtocol> hostProtocols
}

struct ReadPublisherOptionsRequest {
  1: optional string path
  2: optional i32 schema_version // The schema version publisher code is configured to
}

struct ReadPublisherOptionsResult {
  1: optional list<HostAddress> hostAddresses // To be deprecated by hostProtocols
 10: optional list<HostProtocol> hostProtocols
 20: optional ChecksumOption checksumOption
 31: optional SchemaInfo schemaInfo // When publish, publisher can use any version not higher than this one
}

struct ReadConsumerGroupHostsRequest {
  1: optional string destinationPath
  2: optional string consumerGroupName
}

struct ReadConsumerGroupHostsResult {
  1: optional list<HostAddress> hostAddresses // To be deprecated by hostProtocols
 10: optional list<HostProtocol> hostProtocols
}

enum Status {
  OK,
  FAILED,
  TIMEDOUT,
  THROTTLED
}

struct PutMessage {
  1: optional string id  // This is unique identifier of message
  2: optional i32 delayMessageInSeconds // Applies to TIMER destinations only.
  3: optional binary data
  4: optional map<string, string> userContext  // This is user specified context to pass through
  // Put is rejected if previousMessageId doesn't match the id of the previous message.
  // Applies to LOG destinations only.
  5: optional string previousMessageId
  6: optional i64 (js.type = "Long") crc32IEEEDataChecksum // This is the crc32 checksum using IEEE polynomial
  // 7: optional i64 (js.type = "Long") crc32CastagnoliDataChecksum // This is the crc32 checksum using Castagnoli polynomial
  8: optional binary md5DataChecksum // This is the md5 checksum for data field
  10: optional i32 schemaVersion // Version of the schema that encodes binary data, default to 0 means no schema used
}

struct PutMessageAck {
  1: optional string id  // This is unique identifier of message
  2: optional Status status
  3: optional string message  // This is for error message
  4: optional string receipt
  5: optional map<string, string> userContext  // This is user specified context to pass through
  6: optional i64 (js.type = "Long") lsn // LOG destination Log Sequence Number.
  7: optional i64 (js.type = "Long") address // LOG destination message address
}

exception InvalidAckIdError {
  1: required string message
  2: optional list<string> ackIds
  3: optional list<string> nackIds
}

struct ConsumerMessage {
  1: optional i64 (js.type = "Long") enqueueTimeUtc
  2: optional string ackId  // Global unique identifier to ack messages. Empty for STREAM consumer group.
  3: optional PutMessage payload
  4: optional i64 (js.type = "Long") lsn // LOG destination Log Sequence Number.
  5: optional i64 (js.type = "Long") address // LOG destination message address
}

struct AckMessagesRequest {
  1: optional list<string> ackIds
  2: optional list<string> nackIds
}

struct SetConsumedMessagesRequest {
  1: optional string destinationPath
  2: optional string consumerGroupName
  3: optional i64 (js.type = "Long") addressInclusive // Address of the last message to mark as consumed
}

struct PutMessageBatchRequest {
  1: optional string destinationPath
  2: optional list<PutMessage> messages
}

struct PutMessageBatchResult {
  1: optional list<PutMessageAck> failedMessages
  2: optional list<PutMessageAck> successMessages
}

struct ReceiveMessageBatchRequest {
  1: optional string destinationPath
  2: optional string consumerGroupName
  3: optional i32 maxNumberOfMessages
  4: optional i32 receiveTimeout
}

struct ReceiveMessageBatchResult {
  1: optional list<ConsumerMessage> messages
}

struct GetQueueDepthInfoRequest {
  1: optional string key
}

struct GetQueueDepthInfoResult {
  1: optional string value
}

service BFrontend {
  // returns the ip:port for this frontend (for discovery purpose)
  string HostPort()

  /*********************************************/
  /***** Destination CRUD **********************/
  DestinationDescription createDestination(1: CreateDestinationRequest createRequest)
    throws (
      1: EntityAlreadyExistsError entityExistsError,
      2: BadRequestError requestError)

  DestinationDescription readDestination(1: ReadDestinationRequest getRequest)
    throws (
      1: EntityNotExistsError entityError,
      2: BadRequestError requestError)

  DestinationDescription updateDestination(1: UpdateDestinationRequest updateRequest)
    throws (
      1: EntityNotExistsError entityError,
      2: BadRequestError requestError)

  void deleteDestination(1: DeleteDestinationRequest deleteRequest)
    throws (
      1: EntityNotExistsError entityError,
      2: BadRequestError requestError)

  ListDestinationsResult listDestinations(1: ListDestinationsRequest listRequest)
    throws (
      1: BadRequestError requestError)
  /*********************************************/

  /*********************************************/
  /***** ConsumerGroup CRUD ********************/
  ConsumerGroupDescription createConsumerGroup(1: CreateConsumerGroupRequest registerRequest)
    throws (
      1: EntityAlreadyExistsError entityExistsError,
      2: BadRequestError requestError)

  ConsumerGroupDescription readConsumerGroup(1: ReadConsumerGroupRequest getRequest)
    throws (
      1: EntityNotExistsError entityError,
      2: BadRequestError requestError)

  ConsumerGroupDescription updateConsumerGroup(1: UpdateConsumerGroupRequest updateRequest)
      throws (
        1: EntityNotExistsError entityError,
        2: BadRequestError requestError)

  void deleteConsumerGroup(1: DeleteConsumerGroupRequest deleteRequest)
    throws (
      1: EntityNotExistsError entityError,
      2: BadRequestError requestError)

  ListConsumerGroupResult listConsumerGroups(1: ListConsumerGroupRequest listRequest)
    throws (
      1: BadRequestError requestError)

  /*********************************************/

  /**
   * readDestinationHosts will be replaced by readPublisherOptions soon
  **/
  ReadDestinationHostsResult readDestinationHosts(1: ReadDestinationHostsRequest getHostsRequest)
    throws (
      1: EntityNotExistsError entityError,
      2: EntityDisabledError entityDisabled,
      3: BadRequestError requestError)

  ReadPublisherOptionsResult readPublisherOptions(1: ReadPublisherOptionsRequest getPublisherOptionsRequest)
    throws (
      1: EntityNotExistsError entityError,
      2: EntityDisabledError entityDisabled,
      3: BadRequestError requestError)

  ReadConsumerGroupHostsResult readConsumerGroupHosts(1: ReadConsumerGroupHostsRequest getHostsRequest)
    throws (
      1: EntityNotExistsError entityError,
      2: EntityDisabledError entityDisabled,
      3: BadRequestError requestError)

  /*********************************************/
  /***************** DLQ Management ************/

  void purgeDLQForConsumerGroup(1: PurgeDLQForConsumerGroupRequest purgeRequest)
    throws (
      1: EntityNotExistsError entityError,
      2: BadRequestError requestError)

  void mergeDLQForConsumerGroup(1: MergeDLQForConsumerGroupRequest mergeRequest)
    throws (
      1: EntityNotExistsError entityError,
      2: BadRequestError requestError)

  /*************************************************************************/
  /*********** Queue Information  ******************************/
 GetQueueDepthInfoResult getQueueDepthInfo(1: GetQueueDepthInfoRequest getQueueDepthInfoRequest)
    throws (
      1: QueueCacheMissError cacheMissError,
      2: BadRequestError requestError)

}

struct ReconfigureInfo {
  1: optional string updateUUID
}

enum InputHostCommandType {
  ACK,
  RECONFIGURE
}

struct InputHostCommand {
  1: optional InputHostCommandType type
  2: optional PutMessageAck ack
  3: optional ReconfigureInfo reconfigure
}

service BIn {
  /**
  * Non-streaming publish API
  **/
  PutMessageBatchResult putMessageBatch(1: PutMessageBatchRequest request)
    throws (
        1: EntityNotExistsError entityError,
        2: EntityDisabledError entityDisabled,
        3: BadRequestError requestError,
        4: InternalServiceError internalServiceError)
}


enum OutputHostCommandType {
  MESSAGE,
  RECONFIGURE,
  // No more backlogged messages. Inserted into the sream if insertEndOfStreamMarker is true.
  END_OF_STREAM // STREAMING consumer only.
}

struct OutputHostCommand {
  1: optional OutputHostCommandType type
  2: optional ConsumerMessage message
  3: optional ReconfigureInfo reconfigure
}

struct ControlFlow {
  1: optional i32 credits
}

service BOut {
  void ackMessages(1: AckMessagesRequest ackRequest)
    throws (
      1: InvalidAckIdError entityError,
      2: BadRequestError requestError)

  // STREAMING consumer only.
  // Mark all messages up to specified address as consumed by the consumer group.
  void setConsumedMessages(1: SetConsumedMessagesRequest request)
    throws (
      1: InvalidAddressError entityError,
      2: BadRequestError requestError)

  /**
  * Non-streaming consume API
  **/
  ReceiveMessageBatchResult receiveMessageBatch(1: ReceiveMessageBatchRequest request)
    throws (
      1: EntityNotExistsError entityError,
      2: EntityDisabledError entityDisabled,
      3: BadRequestError requestError,
      4: TimeoutError timeoutError)
}
