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
	"net/http"
	"strconv"

	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

// Key names to use in the stream headers for various args
const (
	keyDestinationUUID   = "DestinationUUID"
	keyDestinationType   = "DestinationType"
	keyExtentUUID        = "ExtentUUID"
	keyConsumerGroupUUID = "ConsumerGroupUUID"
	keyAddress           = "Address"
	keyInclusive         = "Inclusive"
)

func newBadRequestError(msg string) error {
	err := cherami.NewBadRequestError()
	err.Message = msg
	return err
}

// GetOpenAppendStreamRequestHeaders converts an OpenAppendStreamRequest struct to headers to pass as tchannel headers to OpenAppendStream
func GetOpenAppendStreamRequestHeaders(req *store.OpenAppendStreamRequest) (headers map[string]string) {

	headers = make(map[string]string)

	headers[keyDestinationUUID] = req.GetDestinationUUID()
	headers[keyDestinationType] = req.GetDestinationType().String()
	headers[keyExtentUUID] = req.GetExtentUUID()

	return
}

// GetOpenAppendStreamRequestStruct extracts OpenAppendStreamRequest from tchannel headers
func GetOpenAppendStreamRequestStruct(headers map[string]string) (req *store.OpenAppendStreamRequest, err error) {

	req = &store.OpenAppendStreamRequest{}

	req = store.NewOpenAppendStreamRequest()

	var ok bool

	// req.DestinationUUID
	destinationUUIDString, ok := headers[keyDestinationUUID]

	if !ok {
		return nil, newBadRequestError("DestinationUUID not specified")
	}

	req.DestinationUUID = StringPtr(destinationUUIDString)

	// req.DestinationType
	destinationTypeString, ok := headers[keyDestinationType]

	if !ok {
		return nil, newBadRequestError("DestinationType not specified")
	}

	destinationType, err := cherami.DestinationTypeFromString(destinationTypeString)

	if err != nil {
		return nil, newBadRequestError(fmt.Sprintf("error parsing DestinationType (%s)", destinationTypeString))
	}

	req.DestinationType = cherami.DestinationTypePtr(destinationType)

	// req.ExtentUUID
	extentUUIDString, ok := headers[keyExtentUUID]

	if !ok {
		return nil, newBadRequestError("ExtentUUID not specified")
	}

	req.ExtentUUID = StringPtr(extentUUIDString)

	return req, nil
}

// GetOpenAppendStreamRequestHTTP extracts OpenAppendStreamRequest from http headers
func GetOpenAppendStreamRequestHTTP(httpHeader http.Header) (req *store.OpenAppendStreamRequest, err error) {

	req = store.NewOpenAppendStreamRequest()

	// req.DestinationUUID
	destinationUUIDString := httpHeader.Get(keyDestinationUUID)

	if len(destinationUUIDString) == 0 {
		return nil, newBadRequestError("DestinationUUID not specified")
	}

	req.DestinationUUID = StringPtr(destinationUUIDString)

	// req.DestinationType
	destinationTypeString := httpHeader.Get(keyDestinationType)

	if len(destinationTypeString) == 0 {
		return nil, newBadRequestError("DestinationType not specified")
	}

	destinationType, err := cherami.DestinationTypeFromString(destinationTypeString)

	if err != nil {
		return nil, newBadRequestError(fmt.Sprintf("error parsing DestinationType (%s)", destinationTypeString))
	}

	req.DestinationType = cherami.DestinationTypePtr(destinationType)

	// req.ExtentUUID
	extentUUIDString := httpHeader.Get(keyExtentUUID)

	if len(extentUUIDString) == 0 {
		return nil, newBadRequestError("ExtentUUID not specified")
	}

	req.ExtentUUID = StringPtr(extentUUIDString)

	return req, nil
}

// GetOpenReadStreamRequestHeaders converts an OpenReadStreamRequest struct to headers to pass as tchannel headers to OpenReadStream
func GetOpenReadStreamRequestHeaders(req *store.OpenReadStreamRequest) (headers map[string]string) {

	headers = make(map[string]string)

	headers[keyDestinationUUID] = req.GetDestinationUUID()
	headers[keyDestinationType] = req.GetDestinationType().String()
	headers[keyExtentUUID] = req.GetExtentUUID()
	headers[keyConsumerGroupUUID] = req.GetConsumerGroupUUID()
	headers[keyAddress] = fmt.Sprintf("%v", req.GetAddress())
	headers[keyInclusive] = fmt.Sprintf("%v", req.GetInclusive())

	return
}

// GetOpenReplicationReadStreamRequestHTTPHeaders converts an OpenReplicationReadStreamRequest struct to http headers for OpenReplicationReadStreamRequest
func GetOpenReplicationReadStreamRequestHTTPHeaders(req *OpenReplicationReadStreamRequest) http.Header {
	headers := GetOpenReadStreamRequestHeaders(&req.OpenReadStreamRequest)
	httpHeaders := http.Header{}
	for k, v := range headers {
		httpHeaders.Add(k, v)
	}

	return httpHeaders
}

// GetOpenReplicationRemoteReadStreamRequestHTTPHeaders converts an OpenReplicationRemoteReadStreamRequest struct to http headers for OpenReplicationRemoteReadStreamRequest
func GetOpenReplicationRemoteReadStreamRequestHTTPHeaders(req *OpenReplicationRemoteReadStreamRequest) http.Header {
	headers := GetOpenReadStreamRequestHeaders(&req.OpenReadStreamRequest)
	httpHeaders := http.Header{}
	for k, v := range headers {
		httpHeaders.Add(k, v)
	}

	return httpHeaders
}

// GetOpenReadStreamRequestHTTPHeaders converts an OpenReadStreamRequest struct to http headers for OpenReadStream
func GetOpenReadStreamRequestHTTPHeaders(req *store.OpenReadStreamRequest) http.Header {
	headers := GetOpenReadStreamRequestHeaders(req)
	httpHeaders := http.Header{}
	for k, v := range headers {
		httpHeaders.Add(k, v)
	}

	return httpHeaders
}

// GetOpenReadStreamRequestStruct extracts OpenReadStreamRequest from tchannel headers
func GetOpenReadStreamRequestStruct(headers map[string]string) (req *store.OpenReadStreamRequest, err error) {

	req = &store.OpenReadStreamRequest{}

	var ok bool

	// req.DestinationUUID
	destinationUUIDString, ok := headers[keyDestinationUUID]

	if !ok {
		return nil, newBadRequestError("DestinationUUID not specified")
	}

	req.DestinationUUID = StringPtr(destinationUUIDString)

	// req.DestinationType
	destinationTypeString, ok := headers[keyDestinationType]

	if !ok {
		return nil, newBadRequestError("DestinationType not specified")
	}

	destinationType, err := cherami.DestinationTypeFromString(destinationTypeString)

	if err != nil {
		return nil, newBadRequestError(fmt.Sprintf("error parsing DestinationType (%s): %v", destinationTypeString, err))
	}

	req.DestinationType = cherami.DestinationTypePtr(destinationType)

	// req.ExtentUUID
	extentUUIDString, ok := headers[keyExtentUUID]

	if !ok {
		return nil, newBadRequestError("ExtentUUID not specified")
	}

	req.ExtentUUID = StringPtr(extentUUIDString)

	// req.ConsumerGroupUUID
	consumerGroupUUIDString, ok := headers[keyConsumerGroupUUID]

	if !ok {
		return nil, newBadRequestError("ConsumerGroupUUID not specified")
	}

	req.ConsumerGroupUUID = StringPtr(consumerGroupUUIDString)

	// req.Address
	addressString, ok := headers[keyAddress]

	if !ok {
		return nil, newBadRequestError("Address not specified")
	}

	address, err := strconv.ParseInt(addressString, 10, 64)

	if err != nil {
		return nil, newBadRequestError(fmt.Sprintf("error parsing Address (%s): %v", addressString, err))
	}

	req.Address = Int64Ptr(address)

	// req.Inclusive
	inclusiveString, ok := headers[keyInclusive]

	if !ok {
		return nil, newBadRequestError("Inclusive not specified")
	}

	inclusive, err := strconv.ParseBool(inclusiveString)

	if err != nil {
		return nil, newBadRequestError(fmt.Sprintf("error parsing Inclusive (%s): %v", inclusiveString, err))
	}

	req.Inclusive = BoolPtr(inclusive)

	return req, nil
}

// GetOpenReadStreamRequestHTTP extracts OpenReadStreamRequest from http headers
func GetOpenReadStreamRequestHTTP(httpHeader http.Header) (req *store.OpenReadStreamRequest, err error) {

	req = store.NewOpenReadStreamRequest()

	// req.DestinationUUID
	destinationUUIDString := httpHeader.Get(keyDestinationUUID)

	if len(destinationUUIDString) == 0 {
		return nil, newBadRequestError("DestinationUUID not specified")
	}

	req.DestinationUUID = StringPtr(destinationUUIDString)

	// req.DestinationType
	destinationTypeString := httpHeader.Get(keyDestinationType)

	if len(destinationTypeString) == 0 {
		return nil, newBadRequestError("DestinationType not specified")
	}

	destinationType, err := cherami.DestinationTypeFromString(destinationTypeString)

	if err != nil {
		return nil, newBadRequestError(fmt.Sprintf("error parsing DestinationType (%s): %v", destinationTypeString, err))
	}

	req.DestinationType = cherami.DestinationTypePtr(destinationType)

	// req.ExtentUUID
	extentUUIDString := httpHeader.Get(keyExtentUUID)

	if len(extentUUIDString) == 0 {
		return nil, newBadRequestError("ExtentUUID not specified")
	}

	req.ExtentUUID = StringPtr(extentUUIDString)

	// req.ConsumerGroupUUID
	consumerGroupUUIDString := httpHeader.Get(keyConsumerGroupUUID)

	if len(consumerGroupUUIDString) == 0 {
		return nil, newBadRequestError("ConsumerGroupUUID not specified")
	}

	req.ConsumerGroupUUID = StringPtr(consumerGroupUUIDString)

	// req.Address
	addressString := httpHeader.Get(keyAddress)

	if len(addressString) == 0 {
		return nil, newBadRequestError("Address not specified")
	}

	address, err := strconv.ParseInt(addressString, 10, 64)

	if err != nil {
		return nil, newBadRequestError(fmt.Sprintf("error parsing Address (%s): %v", addressString, err))
	}

	req.Address = Int64Ptr(address)

	// req.Inclusive
	inclusiveString := httpHeader.Get(keyInclusive)

	if len(inclusiveString) == 0 {
		return nil, newBadRequestError("Inclusive not specified")
	}

	inclusive, err := strconv.ParseBool(inclusiveString)

	if err != nil {
		return nil, newBadRequestError(fmt.Sprintf("error parsing Inclusive (%s): %v", inclusiveString, err))
	}

	req.Inclusive = BoolPtr(inclusive)

	return req, nil
}

// GetOpenReplicationRemoteReadStreamRequestHTTP extracts OpenReplicationRemoteReadStreamRequest from http headers
func GetOpenReplicationRemoteReadStreamRequestHTTP(httpHeader http.Header) (req *OpenReplicationRemoteReadStreamRequest, err error) {
	openReadStreamRequest, err := GetOpenReadStreamRequestHTTP(httpHeader)
	if err != nil {
		return
	}

	req = &OpenReplicationRemoteReadStreamRequest{*openReadStreamRequest}
	return
}

// GetOpenReplicationReadStreamRequestHTTP extracts OpenReplicationReadStreamRequest from http headers
func GetOpenReplicationReadStreamRequestHTTP(httpHeader http.Header) (req *OpenReplicationReadStreamRequest, err error) {
	openReadStreamRequest, err := GetOpenReadStreamRequestHTTP(httpHeader)
	if err != nil {
		return
	}

	req = &OpenReplicationReadStreamRequest{*openReadStreamRequest}
	return
}

// CheramiDestinationType converts from shared.DestinationType to cherami.DestinationType
func CheramiDestinationType(internalDestinationType shared.DestinationType) (cheramiDestinationType cherami.DestinationType, err error) {
	return cherami.DestinationTypeFromString(internalDestinationType.String())
}
