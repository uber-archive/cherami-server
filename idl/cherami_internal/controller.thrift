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

exception QueueCacheMissError {
  1: required string message
}

struct GetInputHostsRequest {
  1: optional string destinationUUID
}

struct GetInputHostsResult {
  1: optional list<string> inputHostIds
}

struct GetOutputHostsRequest {
  1: optional string destinationUUID
  2: optional string consumerGroupUUID
}

struct GetOutputHostsResult {
  1: optional list<string> outputHostIds
}

enum SKU {
  Machine1
}

enum Role {
  IN,
  OUT,
  STORE
}

struct NodeMetrics {
  1: optional i64 (js.type = "Long") cpu
  2: optional i64 (js.type = "Long") memory
  3: optional i64 (js.type = "Long") remainingDiskSpace
  // extent metrics aggregated at host level
  4: optional i64 (js.type = "Long") numberOfActiveExtents
  5: optional i64 (js.type = "Long") numberOfConnections
  6: optional i64 (js.type = "Long") incomingMessagesCounter
  7: optional i64 (js.type = "Long") outgoingMessagesCounter
  8: optional i64 (js.type = "Long") incomingBytesCounter
  9: optional i64 (js.type = "Long") outgoingBytesCounter
}

struct DestinationMetrics {
  1: optional i64 (js.type = "Long") numberOfActiveExtents
  2: optional i64 (js.type = "Long") numberOfConnections
  3: optional i64 (js.type = "Long") incomingMessagesCounter
  4: optional i64 (js.type = "Long") incomingBytesCounter
}

struct ConsumerGroupMetrics {
  1: optional i64 (js.type = "Long") numberOfActiveExtents
  2: optional i64 (js.type = "Long") numberOfConnections
  3: optional i64 (js.type = "Long") outgoingMessagesCounter
  4: optional i64 (js.type = "Long") outgoingBytesCounter
  5: optional i64 (js.type = "Long") smartRetryOnCounter
}

struct DestinationExtentMetrics {
  1: optional i64 (js.type = "Long") incomingMessagesCounter
  2: optional i64 (js.type = "Long") incomingBytesCounter
  3: optional i64 (js.type = "Long") putMessageLatency
}

struct ConsumerGroupExtentMetrics {
  1: optional i64 (js.type = "Long") outgoingMessagesCounter
  2: optional i64 (js.type = "Long") outgoingBytesCounter
}

struct StoreExtentMetrics {
  1: optional i64 (js.type = "Long") numberOfConnections
  2: optional i64 (js.type = "Long") incomingMessagesCounter
  3: optional i64 (js.type = "Long") incomingBytesCounter
  4: optional i64 (js.type = "Long") writeMessageLatency
  5: optional i64 (js.type = "Long") outgoingMessagesCounter
  6: optional i64 (js.type = "Long") outgoingBytesCounter
  7: optional i64 (js.type = "Long") readMessageLatency
  8: optional shared.ExtentStatus extentStatus
}

struct ReportNodeMetricRequest {
  1: optional SKU sku
  2: optional Role role
  3: optional string hostId
  4: optional NodeMetrics metrics
  5: optional i64 (js.type = "Long") timestamp
}

struct ReportDestinationMetricRequest {
  1: optional SKU sku
  2: optional string hostId
  3: optional string destinationUUID
  4: optional DestinationMetrics metrics
  5: optional i64 (js.type = "Long") timestamp
}

struct ReportDestinationExtentMetricRequest {
  1: optional SKU sku
  2: optional string hostId
  3: optional string destinationUUID
  4: optional string extentUUID
  5: optional DestinationExtentMetrics metrics
  6: optional i64 (js.type = "Long") timestamp
}

struct ReportConsumerGroupMetricRequest {
  1: optional SKU sku
  2: optional string hostId
  3: optional string destinationUUID
  4: optional string consumerGroupUUID
  5: optional ConsumerGroupMetrics metrics
  6: optional i64 (js.type = "Long") timestamp
}

struct ReportConsumerGroupExtentMetricRequest {
  1: optional SKU sku
  2: optional string hostId
  3: optional string destinationUUID
  4: optional string consumerGroupUUID
  5: optional string extentUUID
  6: optional ConsumerGroupExtentMetrics metrics
  7: optional i64 (js.type = "Long") timestamp
}

struct ReportStoreExtentMetricRequest {
  1: optional SKU sku
  2: optional string storeId
  3: optional string extentUUID
  4: optional StoreExtentMetrics metrics
  5: optional i64 (js.type = "Long") timestamp
}

struct InputHostCapacities {
  1: optional i64 (js.type = "Long") cpu
  2: optional i64 (js.type = "Long") memory
  3: optional i64 (js.type = "Long") numberOfActiveExtents
  4: optional i64 (js.type = "Long") numberOfConnections
  5: optional i64 (js.type = "Long") incomingMessagesPerSecond
  6: optional i64 (js.type = "Long") incomingBytesPerSecond
}

struct OutputHostCapacities {
  1: optional i64 (js.type = "Long") cpu
  2: optional i64 (js.type = "Long") memory
  3: optional i64 (js.type = "Long") numberOfActiveExtents
  4: optional i64 (js.type = "Long") numberOfConnections
  5: optional i64 (js.type = "Long") outgoingMessagesPerSecond
  6: optional i64 (js.type = "Long") outgoingBytesPerSecond
}

struct StoreCapacities {
  1: optional i64 (js.type = "Long") cpu
  2: optional i64 (js.type = "Long") memory
  3: optional i64 (js.type = "Long") numberOfActiveExtents
  4: optional i64 (js.type = "Long") numberOfConnections
  5: optional i64 (js.type = "Long") remainingDiskSpace
}

struct UpsertInputHostCapacitiesRequest {
  1: optional SKU sku
  2: optional InputHostCapacities capacities
}

struct UpsertOutputHostCapacitiesRequest {
  1: optional SKU sku
  2: optional OutputHostCapacities capacities
}

struct UpsertStoreCapacitiesRequest {
  1: optional SKU sku
  2: optional StoreCapacities capacities
}

struct RemoveCapacitiesRequest {
  1: optional SKU sku
  2: optional Role role
}

struct GetCapacitiesRequest {
  1: optional SKU sku
  2: optional Role role
}

struct GetCapacitiesResult {
  1: optional InputHostCapacities inputHostCapacities
  2: optional OutputHostCapacities outputHostCapacities
  3: optional StoreCapacities storeCapacities
}

struct GetQueueDepthInfoRequest {
  1: optional string key
}

struct GetQueueDepthInfoResult {
  1: optional string value
}

/**
* InputHost Stats:
* 1) CPU
* 2) Memory
* 3) NumberOfActiveExtents
* 4) NumberOfConnections
*
* OutputHost Stats:
* 1) CPU
* 2) Memory
* 3) NumberOfActiveExtents
* 4) NumberOfConnections
*
* StoreHost Stats:
* 1) CPU
* 2) Memory
* 3) RemainingDiskSpace
* 4) NumberOfActiveExtents
* 5) NumberOfConnections
*
* Destination Stats: [Dimensions: ExtentId, InputHostId, DestinationId, TChannelId]
* 1) IncomingMessagesPerSecond
* 2) IncomingBytesPerSecond
* 3) PutMessageLatency
*
* ConsumerGroup Stats: [Dimensions: ExtentId, OutputHostId, ConsumerGroupId, TChannelId]
* 1) OutgoingMessagesPerSecond
* 2) OutgoingBytesPerSecond
*
* StoreExtent Stats: [Dimensions: ExtentId, StoreId, DestinationId, ConsumerGroupId, TChannelId]
*
**/

// TODO: If Store goes down Controller needs and API which for IN and OUT
service Controller {
  /*************************************************************************/
  /********************** Host Placement ***********************************/
  GetInputHostsResult getInputHosts(1: GetInputHostsRequest getHostsRequest)
  throws (
    1: shared.BadRequestError requestError,
    2: shared.InternalServiceError internalError)

  GetOutputHostsResult getOutputHosts(1: GetOutputHostsRequest getHostsRequest)
  throws (
    1: shared.BadRequestError requestError,
    2: shared.InternalServiceError internalError)

  /*************************************************************************/

  /*************************************************************************/
  /********************** Destination CUD **********************************/
  shared.DestinationDescription createDestination(1: shared.CreateDestinationRequest createRequest)
    throws (
      1: shared.EntityAlreadyExistsError entityExistsError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalError)

  shared.DestinationDescription updateDestination(1: shared.UpdateDestinationRequest updateRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalError)

  void deleteDestination(1: shared.DeleteDestinationRequest deleteRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalError)
  /*************************************************************************/

  /*************************************************************************/
  /********************** ConsumerGroup CUD ********************************/
  shared.ConsumerGroupDescription createConsumerGroup(1: shared.CreateConsumerGroupRequest createRequest)
    throws (
      1: shared.EntityAlreadyExistsError entityExistsError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalError)

  shared.ConsumerGroupDescription updateConsumerGroup(1: shared.UpdateConsumerGroupRequest updateRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalError)

  void deleteConsumerGroup(1: shared.DeleteConsumerGroupRequest deleteRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalError)
  /*************************************************************************/

  /***************************************************************************/
  /********************** Extent Creation ************************************/
  /*This is only used to create an extent that originates from another zone***/
  shared.CreateExtentResult createRemoteZoneExtent(1: shared.CreateExtentRequest createRequest)
    throws (
      1: shared.EntityAlreadyExistsError entityExistsError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalError)
  /*************************************************************************/

  /*************************************************************************/
  /******************** Report Load ****************************************/
  void reportNodeMetric(1: ReportNodeMetricRequest reportMetricRequest)

  void reportDestinationMetric(1: ReportDestinationMetricRequest reportMetricRequest)

  void reportDestinationExtentMetric(1: ReportDestinationExtentMetricRequest reportMetricRequest)

  void reportConsumerGroupMetric(1: ReportConsumerGroupMetricRequest reportMetricRequest)

  void reportConsumerGroupExtentMetric(1: ReportConsumerGroupExtentMetricRequest reportMetricRequest)

  void reportStoreExtentMetric(1: ReportStoreExtentMetricRequest reportMetricRequest)
  /*************************************************************************/

  /*************************************************************************/
  /*********** Node Capacity Constraints CRUD ******************************/
  void upsertInputHostCapacities(1: UpsertInputHostCapacitiesRequest upsertCapacitiesRequest)

  void upsertOutputHostCapacities(1: UpsertOutputHostCapacitiesRequest upsertCapacitiesRequest)

  void upsertStoreCapacities(1: UpsertStoreCapacitiesRequest upsertCapacitiesRequest)

  void removeCapacities(1: RemoveCapacitiesRequest removeCapacitiesRequest)

  GetCapacitiesResult getCapacities(1: GetCapacitiesRequest getCapacitiesRequest)
  /*************************************************************************/

  /*************************************************************************/
  /*********** Queue Information  ******************************/
 GetQueueDepthInfoResult getQueueDepthInfo(1: GetQueueDepthInfoRequest getQueueDepthInfoRequest)
    throws (
      1: QueueCacheMissError cacheMissError)
}
