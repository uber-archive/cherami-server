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

include "../cherami/cherami.thrift"
include "shared.thrift"

exception BadStoreRequestError {
  1: required string message
}

exception StoreServiceError {
  1: required string message
}

exception InvalidStoreAddressError {
  1: required string message
}

exception ExtentSealedError {
  1: optional string extentUUID
  2: optional i64 (js.type = "Long") sequenceNumber
  3: required string message
}

exception ExtentNotFoundError {
  1: optional string extentUUID
  2: required string message
}

exception NoMoreMessagesError {
  1: required string extentUUID
  3: required string message
}

exception ExtentFailedToSealError {
  1: optional string extentUUID
  2: optional i64 (js.type = "Long") requestedSequenceNumber
  3: optional i64 (js.type = "Long") lastSequenceNumber
  4: required string message
}

// Contains either all fields or fullyReplicatedWatermark only
struct AppendMessage {
  1: optional i64 (js.type = "Long") sequenceNumber
  2: optional i64 (js.type = "Long") enqueueTimeUtc
  3: optional cherami.PutMessage payload
  4: optional i64 (js.type = "Long") fullyReplicatedWatermark
}

struct AppendMessageAck {
  1: optional i64 (js.type = "Long") sequenceNumber
  2: optional i64 (js.type = "Long") address
  3: optional cherami.Status status
  4: optional string message
}

// Currently, with tchannel-streams we are unable to return error and
// status codes to the remote caller of OpenReadStream. So we send
// any error and status codes via specially encoded messages in-stream.
enum ReadMessageContentType {
  MESSAGE,
  SEALED,
  ERROR
}

struct ReadMessage {
  1: optional i64 (js.type = "Long") address
  2: optional AppendMessage message
}

struct ReadMessageContent {
  1: optional ReadMessageContentType type
  2: optional ReadMessage message
  3: optional ExtentSealedError sealed
  4: optional StoreServiceError error
  5: optional NoMoreMessagesError noMoreMessage
}

// This is for reading from BStore directly without destination or cg, as long as there is extent stored
struct ReadMessagesRequest {
  1: optional string extentUUID
  2: optional string destinationUUID
  3: optional cherami.DestinationType destinationType
  4: optional string consumerGroupUUID
  5: optional i64 (js.type = "Long") startAddress // start address of the messages to read from
  6: optional bool startAddressInclusive // whether to include the message at startAddress
  7: optional i64 (js.type = "Long") endAddress // end address of the messages, non-inclusive, set ADDR_END to indicate read all
  8: optional i32 numMessages // number of messages to return
}

struct ReadMessagesResult {
  1: optional list<ReadMessageContent> messages
}

struct ExtentInfo {
  1:  optional string extentUUID
  2:  optional i64 (js.type = "Long") createdAt
  3:  optional i64 (js.type = "Long") beginAddress // first available message storage address
  4:  optional i64 (js.type = "Long") lastAddress // last message storage address
  5:  optional i64 (js.type = "Long") beginSequence // first available message sequence number; e.g. 0
  6:  optional i64 (js.type = "Long") lastSequence  // last message sequence number; non-inclusive, e.g. begin = 0, last = 0, means zero messages written
  7:  optional i64 (js.type = "Long") beginEnqueueTimeUtc // first message enqueue (write) time
  8:  optional i64 (js.type = "Long") lastEnqueueTimeUtc // last message enqueue (write) time
  9:  optional i64 (js.type = "Long") sizeInBytes
  10: optional shared.ExtentStatus status
  11: optional i64 (js.type = "Long") beginTime // first available message delivery time
  12: optional i64 (js.type = "Long") endTime // last available message delivery time
  13: optional i64 (js.type = "Long") availableAddress // last available message storage address
  14: optional i64 (js.type = "Long") availableSequence  // last available message sequence number; non-inclusive, e.g. begin = 0, last = 0, means zero messages available
  15: optional double availableSequenceRate // rate of change of the availableSequence, 1/seconds
  16: optional double lastSequenceRate // rate of change of the lastSequence, 1/seconds
  17: optional double sizeInBytesRate
}

// ADDR_BEGIN is a special address used to refer to the beginning of an extent
const i64 (js.type = "Long") ADDR_BEGIN = 0;

// ADDR_SEAL is a special address used to refer to where the extent was sealed
const i64 (js.type = "Long") ADDR_SEAL = -2; // thrift parser complains about '0x7ffffffffffffffd;'

// ADDR_END is a special address used to refer to the end of an extent
const i64 (js.type = "Long") ADDR_END = -1; // thrift parser complains about '0x7ffffffffffffffe;'

struct OpenAppendStreamRequest {
  2: optional string destinationUUID
  3: optional cherami.DestinationType destinationType
  1: optional string extentUUID
}

struct OpenReadStreamRequest {
  4: optional string destinationUUID
  5: optional cherami.DestinationType destinationType
  1: optional string extentUUID
  6: optional string consumerGroupUUID
  2: optional i64 (js.type = "Long") address
  3: optional bool inclusive
}

// GetAddressFromTimestampRequest contains the parameters to a GetAddressFromTimestamp operation.
// - the timestamp is in 'nanoseconds'
struct GetAddressFromTimestampRequest {
  1: optional string extentUUID
  2: optional i64 (js.type = "Long") timestamp
}

// GetAddressFromTimestampResult contains the results of a GetAddressFromTimestamp request.
// The 'address' contains the address of the last message that was found _before_ or at
// the given 'timeestamp' in GetAddressFromTimestampRequest. The 'address' can be 'ADDR_BEGIN',
// if there were no messages before the given timestamp. If the extent is sealed and there
// are no more messages after the given timestamp in the extent, the 'sealed' is set to true.
struct GetAddressFromTimestampResult {
  1: optional i64 (js.type = "Long") address
  2: optional bool sealed
  3: optional i64 (js.type = "Long") sequenceNumber
}

struct GetExtentInfoRequest {
  1: optional string extentUUID
}

struct SealExtentRequest {
  1: optional string extentUUID
  2: optional i64 (js.type = "Long") sequenceNumber
}

// PurgeMessagesRequst contains the parameters to a PurgeMessages operation.
// The 'address' contains the address of the message upto, and including which,
// all messages are to be purged.
// - if address is ADDR_SEAL, it woud delete the entire extent
// - if address is ADDR_END, it would delete all messages in the extent (*not*
// including the sealed extent marker)
struct PurgeMessagesRequest {
  1: optional string extentUUID
  2: optional i64 (js.type = "Long") address
}

// PurgeMessagesResult contains the results of a PurgeMessages operation.
// The 'address' contains the address of the next available message, after the
// point where it was purged.
// - if there are no more messages beyond purge-addr, and if the extent is not
//   sealed, this would contain ADDR_END
// - if there are no more messages beyond purge-addr, and if the extent is
//   sealed, this would contains ADDR_SEAL
struct PurgeMessagesResult {
  1: optional i64 (js.type = "Long") address
}

// ReplicateExtentRequest contains the parameters to a ReplicateExtent operation.
// - destinationUUID: is the destination UUID corresponding to the extent
// - destinationType: is the type of the destination
// - extentUUID: is the UUID of the extent to replicate
// - storeUUID: is the UUID of the replica to read this extent from
// TODO: in the future, this will also contain information about the store to use
// when reading from the source and writing to the destination
struct ReplicateExtentRequest {
  1: optional string destinationUUID
  2: optional cherami.DestinationType destinationType
  3: optional string extentUUID
  4: optional string storeUUID
}

// RemoteReplicateExtentRequest contains the parameters to a ReplicateExtent operation.
// - destinationUUID: is the destination UUID corresponding to the extent
// - destinationType: is the type of the destination
// - extentUUID: is the UUID of the extent to replicate
struct RemoteReplicateExtentRequest {
  1: optional string destinationUUID
  2: optional cherami.DestinationType destinationType
  3: optional string extentUUID
}

//void Delete(string extentUUID, i64 (js.type = "Long") address, bool inclusive)  // TODO: Remove and move to BStore.
service BStore {
  GetAddressFromTimestampResult getAddressFromTimestamp(1: GetAddressFromTimestampRequest getAddressRequest)
    throws (
      1: ExtentNotFoundError notFoundError,
      2: BadStoreRequestError requestError,
      3: StoreServiceError serviceError)

  ExtentInfo getExtentInfo(1: GetExtentInfoRequest extentInfoRequest)
    throws (
      1: ExtentNotFoundError notFoundError,
      2: BadStoreRequestError requestError)

  /**
    * This is used to update ExtentStatus to 'SEALED'.  Client needs to pass in 'sequence number' to seal the extent.
    * The reason this API is exposed for BIn nodes to consistently seal the extent on all Replicas in case of crashes.
    * If the last known sequence number on BStore is less what the client is requesting as part of SealExtentRequest
    * then it will return 'ExtentFailedToSealError'.  It can also fail to seal the extent if 2 clients simultaneously
    * tries to seal the extent, in which case one client wins and the other will recieve the 'ExtentSealedError'.
  **/
  void sealExtent(1: SealExtentRequest sealRequest)  // Fails if bigger than the last one.
    throws (
      1: ExtentSealedError sealedError,
      2: ExtentFailedToSealError failedError,
      3: BadStoreRequestError requestError,
      4: StoreServiceError serviceError)

  PurgeMessagesResult purgeMessages(1: PurgeMessagesRequest purgeRequest)
    throws (
      1: ExtentNotFoundError notFoundError,
      2: BadStoreRequestError requestError,
      3: StoreServiceError serviceError)

  ReadMessagesResult readMessages(1: ReadMessagesRequest readMessagesRequest)
    throws (
      1: ExtentNotFoundError extentNotFoundError,
      2: BadStoreRequestError requestError,
      3: StoreServiceError serviceError)

  void replicateExtent(1: ReplicateExtentRequest replicateExtentRequest)
    throws (
      1: ExtentNotFoundError extentNotFoundError,
      2: BadStoreRequestError requestError,
      3: StoreServiceError serviceError)

  void remoteReplicateExtent(1: RemoteReplicateExtentRequest request)
    throws (
      1: ExtentNotFoundError extentNotFoundError,
      2: BadStoreRequestError requestError,
      3: StoreServiceError serviceError)
}


/*******************************************************************/
/***********************Open Design Issues**************************/
/**
*
* 1. We need a design for DLQ's and maangement operations on those queues.  Basically we need to have a system
* created DLQ associated with each destination-consumerGroup pair, which is used to put messages in after
* maxDeliveryCount.  We need to support operations like Merge, Purge, etc on DLQ and also support querying various
* metric like backlog on the queue.  It should also support creating a consumerGroup on the DLQ and open a consumer
* stream to receive individual messages like regular queues.
*
* 2. We need to investigate how timeout's are going to work on PutMessageStream.  For instance if we are unable to
* write message on the tchannel-stream, then how does it responds back to the application.  Another thing is how server
* can communicate server timeout back to the client, so client can stop worrying about the pending put acknowledgement
* and retry the put operation.
*
* 3. We need to decide how our Ack Identifiers look like?  Will they be globally unique?  Or local to a ConsumerStream?
* We can make AckId very optimized if they are localized to particular Consumer stream and user is expected to Ack those
* over the same stream Vs if they are global and client can some through any frontend to Ack those messages.
*
* 4. Need API to directly expose Extents to client for various consumption patterns like tranfering entire extents
* effectively.
*
* 5. Details about Timestamp guarantees within the system. Monotonically increasing, Lookup with timestamp, etc

**/
/*******************************************************************/

/*******************************************************************/
/***********************Limits******************************/
/**
* 1. Max worker limit on ConsumerGroup
* 2. Max rate limit on ConsumerGroup
* 3. Min DestinationDescription.Retention = 60 seconds
* 4. Min ConsumerGroupDescription.SkipOlderMessages = 60 seconds
**/
/*******************************************************************/

/*******************************************************************/
/***********************vNext Features******************************/
/**
* 1. Design for Timer Queues.
*
* 2. Design for Priority Queues.
*
* 3. Design for Soft-Delete of destinations which gives oppurtunity to all Consumers to drain before deleting.
*
* 4. How do we allow lazy creation of entities (destinations, consumerGroups, etc) with some predefined
* configuatations?  This is specially helpful for unit testing scenarios.  Also think about clean-up of such
* entities.
**/
/*******************************************************************/
