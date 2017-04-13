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

package common

import (
	"fmt"
	"strings"
	"time"

	"github.com/uber/tchannel-go"

	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

// IntPtr makes a copy and returns the pointer to an int.
func IntPtr(v int) *int {
	return &v
}

// TSPtr makes a copy and returns the pointer to an Time.
func TSPtr(v time.Time) *time.Time {
	return &v
}

// Int16Ptr makes a copy and returns the pointer to an int16.
func Int16Ptr(v int16) *int16 {
	return &v
}

// Int32Ptr makes a copy and returns the pointer to an int32.
func Int32Ptr(v int32) *int32 {
	return &v
}

// Int64Ptr makes a copy and returns the pointer to an int64.
func Int64Ptr(v int64) *int64 {
	return &v
}

// Uint32Ptr makes a copy and returns the pointer to a uint32.
func Uint32Ptr(v uint32) *uint32 {
	return &v
}

// Uint64Ptr makes a copy and returns the pointer to a uint64.
func Uint64Ptr(v uint64) *uint64 {
	return &v
}

// Float64Ptr makes a copy and returns the pointer to an int64.
func Float64Ptr(v float64) *float64 {
	return &v
}

// BoolPtr makes a copy and returns the pointer to a bool.
func BoolPtr(v bool) *bool {
	return &v
}

// StringPtr makes a copy and returns the pointer to a string.
func StringPtr(v string) *string {
	return &v
}

// CheramiStatusPtr makes a copy and returns the pointer to a CheramiStatus.
func CheramiStatusPtr(status cherami.Status) *cherami.Status {
	return &status
}

// CheramiProtocolPtr makes a copy and returns the pointer to a CheramiProtocol.
func CheramiProtocolPtr(protocol cherami.Protocol) *cherami.Protocol {
	return &protocol
}

// CheramiInputHostCommandTypePtr makes a copy and returns the pointer to a
// CheramiInputHostCommandType.
func CheramiInputHostCommandTypePtr(cmdType cherami.InputHostCommandType) *cherami.InputHostCommandType {
	return &cmdType
}

// CheramiOutputHostCommandTypePtr makes a copy and returns the pointer to a
// CheramiOutputHostCommandType.
func CheramiOutputHostCommandTypePtr(cmdType cherami.OutputHostCommandType) *cherami.OutputHostCommandType {
	return &cmdType
}

// CheramiDestinationTypePtr makes a copy and returns the pointer to a
// CheramiDestinationType.
func CheramiDestinationTypePtr(destType cherami.DestinationType) *cherami.DestinationType {
	return &destType
}

// CheramiDestinationStatusPtr makes a copy and returns the pointer to a
// CheramiDestinationStatus.
func CheramiDestinationStatusPtr(status cherami.DestinationStatus) *cherami.DestinationStatus {
	return &status
}

// CheramiConsumerGroupStatusPtr makes a copy and returns the pointer to a
// CheramiConsumerGroupStatus.
func CheramiConsumerGroupStatusPtr(status cherami.ConsumerGroupStatus) *cherami.ConsumerGroupStatus {
	return &status
}

// CheramiConsumerGroupExtentStatusPtr makes a copy and returns the pointer to a
// CheramiConsumerGroupExtentStatus.
func CheramiConsumerGroupExtentStatusPtr(status shared.ConsumerGroupExtentStatus) *shared.ConsumerGroupExtentStatus {
	return &status
}

// CheramiChecksumOptionPtr makes a copy and return the pointer too a
// CheramiChecksumOption.
func CheramiChecksumOptionPtr(checksumOption cherami.ChecksumOption) *cherami.ChecksumOption {
	return &checksumOption
}

// InternalChecksumOptionPtr makes a copy and return the pointer too a
// internal shared ChecksumOption.
func InternalChecksumOptionPtr(checksumOption shared.ChecksumOption) *shared.ChecksumOption {
	return &checksumOption
}

// MetadataExtentStatusPtr makes a copy and returns the pointer to a
// MetadataExtentStatus.
func MetadataExtentStatusPtr(status shared.ExtentStatus) *shared.ExtentStatus {
	return &status
}

// MetadataExtentReplicaStatusPtr makes a copy and returns the pointer to a
// MetadataExtentReplicaStatus.
func MetadataExtentReplicaStatusPtr(status shared.ExtentReplicaStatus) *shared.ExtentReplicaStatus {
	return &status
}

// InternalDestinationTypePtr makes a copy and returns the pointer to a
// internal shared DestinationType.
func InternalDestinationTypePtr(destType shared.DestinationType) *shared.DestinationType {
	return &destType
}

// InternalDestinationStatusPtr makes a copy and returns the pointer to a
// internal shared DestinationStatus.
func InternalDestinationStatusPtr(status shared.DestinationStatus) *shared.DestinationStatus {
	return &status
}

// InternalConsumerGroupStatusPtr makes a copy and returns the pointer to a
// internal shared ConsumerGroupStatus.
func InternalConsumerGroupStatusPtr(status shared.ConsumerGroupStatus) *shared.ConsumerGroupStatus {
	return &status
}

// InternalConsumerGroupTypePtr makes a copy and returns the pointer to a
// internal shared ConsumerGroupType.
func InternalConsumerGroupTypePtr(cgType shared.ConsumerGroupType) *shared.ConsumerGroupType {
	return &cgType
}

// InternalExtentReplicaReplicationStatusTypePtr makes a copy and returns the pointer to a ExtentReplicaReplicationStatus
func InternalExtentReplicaReplicationStatusTypePtr(status shared.ExtentReplicaReplicationStatus) *shared.ExtentReplicaReplicationStatus {
	return &status
}

// MetadataConsumerGroupExtentStatusPtr makes a copy and returns the pointer to
// a MetadataConsumerGroupExtentStatus.
func MetadataConsumerGroupExtentStatusPtr(status shared.ConsumerGroupExtentStatus) *shared.ConsumerGroupExtentStatus {
	return &status
}

// AdminNotificationTypePtr makes a copy and returns the pointer to
// a MetadataNotificationType.
func AdminNotificationTypePtr(notificationType admin.NotificationType) *admin.NotificationType {
	return &notificationType
}

// SKUPtr makes a copy and returns the pointer to a SKU.
func SKUPtr(sku controller.SKU) *controller.SKU {
	return &sku
}

// RolePtr makes a copy and returns the pointer to a SKU.
func RolePtr(role controller.Role) *controller.Role {
	return &role
}

// NodeStatusPtr makes a copy and returns the pointer to a NodeStatus.
func NodeStatusPtr(status controller.NodeStatus) *controller.NodeStatus {
	return &status
}

// NodeMetricsPtr makes a copy and returns the pointer to
// a NodeMetrics.
func NodeMetricsPtr(nodeMetrics controller.NodeMetrics) *controller.NodeMetrics {
	return &nodeMetrics
}

// DestinationMetricsPtr makes a copy and returns the pointer to
// a DestinationMetrics.
func DestinationMetricsPtr(dstMetrics controller.DestinationMetrics) *controller.DestinationMetrics {
	return &dstMetrics
}

// DestinationExtentMetricsPtr makes a copy and returns the pointer to
// a DestinationExtentMetrics.
func DestinationExtentMetricsPtr(dstExtMetrics controller.DestinationExtentMetrics) *controller.DestinationExtentMetrics {
	return &dstExtMetrics
}

// ConsumerGroupMetricsPtr makes a copy and returns the pointer to
// a ConsumerGroupMetrics.
func ConsumerGroupMetricsPtr(cgMetrics controller.ConsumerGroupMetrics) *controller.ConsumerGroupMetrics {
	return &cgMetrics
}

// ConsumerGroupExtentMetricsPtr makes a copy and returns the pointer to
// a ConsumerGroupExtentMetrics.
func ConsumerGroupExtentMetricsPtr(cgExtMetrics controller.ConsumerGroupExtentMetrics) *controller.ConsumerGroupExtentMetrics {
	return &cgExtMetrics
}

// StoreExtentMetricsPtr makes a copy and returns the pointer to
// a StoreExtentMetrics.
func StoreExtentMetricsPtr(storeExtMetrics controller.StoreExtentMetrics) *controller.StoreExtentMetrics {
	return &storeExtMetrics
}

func checkForWrappedError(l bark.Logger, in error, checkForInternalServiceError bool) {
	eStr := fmt.Sprintf(`%#v %v`, in, in)

	errList := []string{`BadRequestError`, `EntityNotExistsError`, `EntityAlreadyExistsError`, `EntityDisabledError`}
	if checkForInternalServiceError {
		errList = append(errList, `InternalServiceError`)
	}
	for _, s := range errList {
		if strings.Contains(eStr, s) {
			l.WithFields(bark.Fields{
				TagErr:    in,
				`wrapped`: s,
				`eStr`:    eStr,
			}).Error(`Wrapped error`)
		}
	}
}

// ConvertDownstreamErrors is a helper function to convert a error from
// metadata client or controller client to client-cherami.thrift error that
// can be returned to caller. It also classifies the error for metrics
func ConvertDownstreamErrors(l bark.Logger, in error) (metrics.ErrorClass, error) {
	switch e := in.(type) {
	case *shared.BadRequestError:
		return metrics.UserError, &cherami.BadRequestError{
			Message: e.Message,
		}
	case *shared.EntityNotExistsError:
		return metrics.UserError, &cherami.EntityNotExistsError{
			Message: e.Message,
		}
	case *shared.EntityAlreadyExistsError:
		return metrics.UserError, &cherami.EntityAlreadyExistsError{
			Message: e.Message,
		}
	case *shared.EntityDisabledError:
		return metrics.UserError, &cherami.EntityDisabledError{
			Message: e.Message,
		}
	case *shared.InternalServiceError:
		checkForWrappedError(l, in, false)
		return metrics.InternalError, &cherami.InternalServiceError{
			Message: e.Message,
		}
	case *cherami.BadRequestError:
		return metrics.UserError, in
	case *cherami.EntityNotExistsError:
		return metrics.UserError, in
	case *cherami.EntityAlreadyExistsError:
		return metrics.UserError, in
	case *cherami.EntityDisabledError:
		return metrics.UserError, in
	case *cherami.InternalServiceError:
		checkForWrappedError(l, in, false)
		return metrics.InternalError, in
	case tchannel.SystemError:
		switch {
		case strings.Contains(e.Error(), `EntityNotExistsError`):
			return metrics.UserError, &cherami.EntityNotExistsError{
				Message: e.Error(),
			}
		default:
			checkForWrappedError(l, in, false)
			return metrics.InternalError, &cherami.InternalServiceError{
				Message: e.Error(),
			}
		}
	default:
		checkForWrappedError(l, in, true /*Also check for InternalServiceError*/) // This also catches any Cherami errors that aren't pointers
		return metrics.InternalError, &cherami.InternalServiceError{
			Message: e.Error(),
		}
	}
}

// ClassifyErrorByType gives the metrics error class for any cherami or common error
func ClassifyErrorByType(in error) metrics.ErrorClass {
	switch in.(type) {
	case *shared.BadRequestError:
		return metrics.UserError
	case *shared.EntityNotExistsError:
		return metrics.UserError
	case *shared.EntityAlreadyExistsError:
		return metrics.UserError
	case *shared.EntityDisabledError:
		return metrics.UserError
	case *shared.InternalServiceError:
		return metrics.InternalError
	case *cherami.BadRequestError:
		return metrics.UserError
	case *cherami.EntityNotExistsError:
		return metrics.UserError
	case *cherami.EntityAlreadyExistsError:
		return metrics.UserError
	case *cherami.EntityDisabledError:
		return metrics.UserError
	case *cherami.InternalServiceError:
		return metrics.InternalError
	}
	return metrics.InternalError
}
