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
	"math/rand"
	"strings"
	"sync"
)

// MockRingpopMonitor is an implementation of RingpopMonitor for UTs
type MockRingpopMonitor struct {
	sync.RWMutex
	serviceHosts map[string][]*HostInfo
}

// NewMockRingpopMonitor returns a new instance
func NewMockRingpopMonitor() *MockRingpopMonitor {
	return &MockRingpopMonitor{
		serviceHosts: make(map[string][]*HostInfo),
	}
}

// Add adds the given (uuid, addr) mapping
func (rpm *MockRingpopMonitor) Add(service string, uuid string, addr string) {
	rpm.Lock()
	defer rpm.Unlock()
	rpm.serviceHosts[service] = append(rpm.serviceHosts[service], &HostInfo{UUID: uuid, Addr: addr})
}

// Remove removes the host identified by given uuid from rpm
func (rpm *MockRingpopMonitor) Remove(service string, uuid string) {
	rpm.Lock()
	defer rpm.Unlock()
	curr, ok := rpm.serviceHosts[service]
	if !ok {
		return
	}
	hosts := make([]*HostInfo, 0, len(curr))
	for _, h := range curr {
		if strings.Compare(h.UUID, uuid) == 0 {
			continue
		}
		hosts = append(hosts, h)
	}
	rpm.serviceHosts[service] = hosts
}

// Start starts the ringpop monitor
func (rpm *MockRingpopMonitor) Start() {}

// Stop attempts to stop the RingpopMonitor routines
func (rpm *MockRingpopMonitor) Stop() {}

// GetBootstrappedChannel returns a channel, which will be closed once ringpop is bootstrapped
func (rpm *MockRingpopMonitor) GetBootstrappedChannel() chan struct{} {
	return nil
}

// GetHosts retrieves all the members for the given service
func (rpm *MockRingpopMonitor) GetHosts(service string) ([]*HostInfo, error) {
	rpm.RLock()
	defer rpm.RUnlock()
	return rpm.serviceHosts[service], nil
}

// FindHostForAddr finds and returns the host for the given address
func (rpm *MockRingpopMonitor) FindHostForAddr(service string, addr string) (*HostInfo, error) {
	rpm.RLock()
	defer rpm.RUnlock()

	if hosts, ok := rpm.serviceHosts[service]; ok {
		for _, hi := range hosts {
			if hi.Addr == addr {
				return hi, nil
			}
		}
	}
	return &HostInfo{}, ErrInsufficientHosts
}

// FindHostForKey finds and returns the host responsible for handling the given key
func (rpm *MockRingpopMonitor) FindHostForKey(service string, key string) (*HostInfo, error) {
	rpm.RLock()
	defer rpm.RUnlock()

	if hosts, ok := rpm.serviceHosts[service]; ok && len(hosts) > 0 {
		return hosts[0], nil
	}
	return &HostInfo{}, ErrInsufficientHosts
}

// FindRandomHost finds and returns a random host responsible for handling the given key
func (rpm *MockRingpopMonitor) FindRandomHost(service string) (*HostInfo, error) {
	rpm.RLock()
	defer rpm.RUnlock()

	if hosts, ok := rpm.serviceHosts[service]; ok && len(hosts) > 0 {
		return hosts[rand.Intn(len(hosts))], nil
	}
	return &HostInfo{}, ErrInsufficientHosts
}

// IsHostHealthy returns true if the given host is healthy and false otherwise
func (rpm *MockRingpopMonitor) IsHostHealthy(service string, uuid string) bool {
	rpm.RLock()
	defer rpm.RUnlock()

	if hosts, ok := rpm.serviceHosts[service]; ok {
		for _, hi := range hosts {
			if strings.Compare(hi.UUID, uuid) == 0 {
				return true
			}
		}
	}
	return false
}

// ResolveUUID converts the given UUID into a host:port string
// Returns true on success and false on lookup failure
func (rpm *MockRingpopMonitor) ResolveUUID(service string, uuid string) (string, error) {
	rpm.RLock()
	defer rpm.RUnlock()

	if hosts, ok := rpm.serviceHosts[service]; ok {
		for _, hi := range hosts {
			if strings.Compare(hi.UUID, uuid) == 0 {
				return hi.Addr, nil
			}
		}
	}
	return "", ErrUUIDLookupFailed
}

// SetMetadata sets the given metadata on the rp instance
func (rpm *MockRingpopMonitor) SetMetadata(key string, data string) error {
	return nil
}

// AddListener adds a listener for this service.
func (rpm *MockRingpopMonitor) AddListener(service string, name string, ch chan<- *RingpopListenerEvent) error {
	return nil
}

// RemoveListener removes a listener for this service.
func (rpm *MockRingpopMonitor) RemoveListener(service string, name string) error {
	return nil
}
