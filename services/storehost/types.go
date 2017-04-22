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

package storehost

import (
	"math"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/storage"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/store"

	"github.com/pborman/uuid"
)

const (
	// SeqnumInvalid designates an invalid or unknown sequence number
	SeqnumInvalid = int64(math.MaxInt64)
	// TimestampInvalid designates an invalid or unknown timestamp
	TimestampInvalid = int64(math.MaxInt64)
	// AddressInvalid designates an invalid or unknown address
	AddressInvalid = int64(math.MaxInt64)
)

// -- wrappers to create various error blobs -- //
func newBadRequestError(msg string) error {
	err := cherami.NewBadRequestError()
	err.Message = msg
	return err
}

func newInternalServiceError(msg string) error {
	err := cherami.NewInternalServiceError()
	err.Message = msg
	return err
}

func newExtentNotFoundError(extentID uuid.UUID, msg string) error {
	err := store.NewExtentNotFoundError()
	err.ExtentUUID = common.StringPtr(extentID.String())
	err.Message = msg
	return err
}

func newExtentFailedToSealError(extentID uuid.UUID, sealSeqNum int64, lastSeqNum int64, msg string) error {
	err := store.NewExtentFailedToSealError()
	err.ExtentUUID = common.StringPtr(extentID.String())
	err.RequestedSequenceNumber = common.Int64Ptr(sealSeqNum)
	err.LastSequenceNumber = common.Int64Ptr(lastSeqNum)
	err.Message = msg
	return err
}

func newExtentSealedError(extentID uuid.UUID, sealSeqNum int64, msg string) error {
	err := store.NewExtentSealedError()
	err.ExtentUUID = common.StringPtr(extentID.String())
	err.SequenceNumber = common.Int64Ptr(int64(sealSeqNum))
	err.Message = msg
	return err
}

func newInvalidAddressError(msg string) error {
	err := cherami.NewInvalidAddressError()
	err.Message = msg
	return err
}

// newReadMessageError: is used by calls outside 'outConn'
func newReadMessageError(err string) *store.ReadMessageContent {

	msg := store.NewReadMessageContent()
	msg.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_ERROR)

	msg.Error = store.NewStoreServiceError()
	msg.Error.Message = err

	return msg
}

func newReadMessageContent(addr storage.Address, payload *store.AppendMessage) *store.ReadMessageContent {
	msg := store.NewReadMessageContent()
	msg.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_MESSAGE)

	msg.Message = store.NewReadMessage()
	msg.Message.Address = common.Int64Ptr(int64(addr))
	msg.Message.Message = payload

	return msg
}

func newReadMessageContentSealed(extentID uuid.UUID, seqnum int64) *store.ReadMessageContent {

	msg := store.NewReadMessageContent()
	msg.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_SEALED)

	msg.Sealed = store.NewExtentSealedError()
	msg.Sealed.ExtentUUID = common.StringPtr(extentID.String())
	msg.Sealed.SequenceNumber = common.Int64Ptr(seqnum)
	msg.Sealed.Message = "extent sealed"

	return msg
}

func newReadMessageContentError(err string) *store.ReadMessageContent {

	msg := store.NewReadMessageContent()
	msg.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_ERROR)

	msg.Error = store.NewStoreServiceError()
	msg.Error.Message = err

	return msg
}

func newReadMessageContentNoMoreMsgs(ExtentUUID string, err string) *store.ReadMessageContent {

	msg := store.NewReadMessageContent()
	msg.Type = store.ReadMessageContentTypePtr(store.ReadMessageContentType_ERROR)

	msg.NoMoreMessage = store.NewNoMoreMessagesError()
	msg.NoMoreMessage.ExtentUUID = ExtentUUID
	msg.NoMoreMessage.Message = err

	return msg
}
