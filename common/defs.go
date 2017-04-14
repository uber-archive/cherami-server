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
	"time"

	"github.com/uber/cherami-thrift/.generated/go/store"
)

const (
	// InputServiceName refers to the name of the cherami in service
	InputServiceName = "cherami-inputhost"
	// OutputServiceName refers to the name of the cherami out service
	OutputServiceName = "cherami-outputhost"
	// FrontendServiceName refers to the name of the cherami frontend service
	FrontendServiceName = "cherami-frontendhost"
	// ControllerServiceName refers to the name of the cherami controller service
	ControllerServiceName = "cherami-controllerhost"
	// StoreServiceName refers to the name of the cherami store service
	StoreServiceName = "cherami-storehost"
	// ReplicatorServiceName refers to the name of the cherami replicator service
	ReplicatorServiceName = "cherami-replicator"
)

var services = []string{InputServiceName, OutputServiceName, StoreServiceName, ControllerServiceName, FrontendServiceName, ReplicatorServiceName}

const (
	// UUIDStringLength is the length of an UUID represented as a hex string
	UUIDStringLength = 36 // xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

	// DefaultUpgradeTimeout is the timeout to wait in the upgrade handler
	DefaultUpgradeTimeout = 10 * time.Second
)

// HostInfo is a type that contains the info about a cherami host
type HostInfo struct {
	UUID string
	Addr string // ip:port
	Name string
	Sku  string
	Rack string
	Zone string
}

// CliHelper is the interface to help with the args passed to CLI
type CliHelper interface {
	// GetDefaultOwnerEmail is used to get the default owner email
	GetDefaultOwnerEmail() string
	// SetDefaultOwnerEmail is used to set the default owner email to be used in the CLI
	SetDefaultOwnerEmail(string)
	// GetCanonicalZone is used to get the canonical zone name from the
	// given zone. This is useful in cases where we have short names for
	// zones. For example, if "zone1" and  "z1" both map to "zone1", then
	// use "zone1"
	GetCanonicalZone(zone string) (string, error)
	// SetCanonicalZones is used to populate all valid zones that can be given from CLI
	SetCanonicalZones(map[string]string)
}

func (hi *HostInfo) String() string {
	return fmt.Sprintf("[ uuid=%v, addr=%v, name=%v, sku=%v, rack=%v, zone=%v ]", hi.UUID, hi.Addr, hi.Name, hi.Sku, hi.Rack, hi.Zone)
}

// OpenReplicationRemoteReadStreamRequest is the request type for OpenReplicationRemoteReadStream API
type OpenReplicationRemoteReadStreamRequest struct {
	store.OpenReadStreamRequest
}

// OpenReplicationReadStreamRequest is the request type for OpenReplicationReadStream API
type OpenReplicationReadStreamRequest struct {
	store.OpenReadStreamRequest
}

// MinInt returns the min of given two integers
func MinInt(a, b int) int {
	if a > b {
		return b
	}
	return a
}

// MaxInt returns the max of given two integers
func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// MinInt64 returns the min of given two integers
func MinInt64(a, b int64) int64 {
	if a > b {
		return b
	}
	return a
}

// MaxInt64 returns the max of given two integers
func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// IsValidServiceName returns true if the
// given input is a valid service name,
// false otherwise
func IsValidServiceName(input string) bool {
	for _, svc := range services {
		if svc == input {
			return true
		}
	}
	return false
}
