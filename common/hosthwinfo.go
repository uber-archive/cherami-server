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
	"errors"
	"strings"

	m "github.com/uber/cherami-thrift/.generated/go/metadata"
)

type (
	// HostHardwareInfoReader is the interface for any
	// implementation that vends hardware info related
	// to a given hostname
	HostHardwareInfoReader interface {
		// Read reads and returns the hardware info
		// corresponding to the given hostname
		Read(hostname string) (*HostHardwareInfo, error)
	}

	hostHardwareInfoReaderImpl struct {
		mClient m.TChanMetadataService
	}

	// HostHardwareInfo is the type that
	// contains hardware properties about a
	// cherami host
	HostHardwareInfo struct {
		Sku  string
		Rack string
		Zone string
	}
)

const (
	hostInfoKeyRack = "rack"
	hostInfoKeyZone = "zone"
	hostInfoKeySku  = "sku"
)

var errInvalidArg = errors.New("Invalid argument")

// NewHostHardwareInfoReader creates and returns an implementation
// of hardwareInfoReader that uses Cassandra as the backing store
func NewHostHardwareInfoReader(mClient m.TChanMetadataService) HostHardwareInfoReader {
	return &hostHardwareInfoReaderImpl{mClient: mClient}
}

// Read reads and returns the hardware info corresponding to the given hostname
func (reader *hostHardwareInfoReaderImpl) Read(hostname string) (*HostHardwareInfo, error) {

	if len(hostname) < 1 {
		return nil, errInvalidArg
	}

	hostname = strings.ToLower(hostname)

	ans, err := reader.mClient.ReadHostInfo(nil, &m.ReadHostInfoRequest{Hostname: StringPtr(hostname)})
	if err != nil {
		return nil, err
	}

	var result = new(HostHardwareInfo)

	for k, v := range ans.GetProperties() {

		key := strings.ToLower(k)

		switch key {
		case hostInfoKeyRack:
			result.Sku = v
		case hostInfoKeyZone:
			result.Zone = v
		case hostInfoKeySku:
			result.Sku = v
		}
	}

	return result, nil
}
