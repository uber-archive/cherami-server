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

service Replicator {
  /*****************************************************/
  /***** Destination CUD (Read is handled locally) *****/

  // create at local zone, expect to be called by remote replicator
  shared.DestinationDescription createDestinationUUID(1: shared.CreateDestinationUUIDRequest createRequest)
    throws (
      1: shared.EntityAlreadyExistsError entityExistsError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError)

  // propagate to multiple remote zones, expect to be called by local zone services
  void createRemoteDestinationUUID(1: shared.CreateDestinationUUIDRequest createRequest)
    throws (
      1: shared.EntityAlreadyExistsError entityExistsError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError)
  
  // update at local zone, expect to be called by remote replicator
  shared.DestinationDescription updateDestination(1: shared.UpdateDestinationRequest updateRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError)

  // propagate to multiple remote zones, expect to be called by local zone services
  void updateRemoteDestination(1: shared.UpdateDestinationRequest updateRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError)

  // delete at local zone, expect to be called by remote replicator
  void deleteDestination(1: shared.DeleteDestinationRequest deleteRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError)

  // propagate to multiple remote zones, expect to be called by local zone services
  void deleteRemoteDestination(1: shared.DeleteDestinationRequest deleteRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError)

  /*******************************************************/
  /***** ConsumerGroup CUD (Read is handled locally) *****/

  // create at local zone, expect to be called by remote replicator
  shared.ConsumerGroupDescription createConsumerGroupUUID(1: shared.CreateConsumerGroupUUIDRequest createRequest)
    throws (
      1: shared.EntityAlreadyExistsError entityExistsError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError)

  // propagate to multiple remote zones, expect to be called by local zone services
  void createRemoteConsumerGroupUUID(1: shared.CreateConsumerGroupUUIDRequest createRequest)
    throws (
      1: shared.EntityAlreadyExistsError entityExistsError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError)

  // create at local zone, expect to be called by remote replicator
  shared.ConsumerGroupDescription updateConsumerGroup(1: shared.UpdateConsumerGroupRequest updateRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError)

  // propagate to multiple remote zones, expect to be called by local zone services
  void updateRemoteConsumerGroup(1: shared.UpdateConsumerGroupRequest updateRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError)

  // create at local zone, expect to be called by remote replicator
  void deleteConsumerGroup(1: shared.DeleteConsumerGroupRequest deleteRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError)

  // propagate to multiple remote zones, expect to be called by local zone services
  void deleteRemoteConsumerGroup(1: shared.DeleteConsumerGroupRequest deleteRequest)
    throws (
      1: shared.EntityNotExistsError entityError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError)

  /*******************************************************/
  /***** Dest Extent Creation ****************************/
  // create at local zone, expect to be called by remote replicator
  shared.CreateExtentResult createExtent(1: shared.CreateExtentRequest createRequest)
    throws (
      1: shared.EntityAlreadyExistsError entityExistsError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError)

  // propagate to multiple remote zones, expect to be called by local zone services
  void createRemoteExtent(1: shared.CreateExtentRequest createRequest)
    throws (
      1: shared.EntityAlreadyExistsError entityExistsError,
      2: shared.BadRequestError requestError,
      3: shared.InternalServiceError internalServiceError)


  /*******************************************************/
  /***** Reconciliation APIs ******************************/
  shared.ListDestinationsResult listDestinations(1: shared.ListDestinationsRequest listRequest)
    throws (
      1: shared.BadRequestError requestError,
      2: shared.InternalServiceError internalServiceError)

  shared.ListDestinationsResult listDestinationsByUUID(1: shared.ListDestinationsByUUIDRequest listRequest)
    throws (
      1: shared.BadRequestError requestError,
      2: shared.InternalServiceError internalServiceError)

  shared.ListExtentsStatsResult listExtentsStats(1: shared.ListExtentsStatsRequest request)
    throws (
      1: shared.BadRequestError requestError,
      2: shared.InternalServiceError internalServiceError)
}
