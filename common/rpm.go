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
	"hash/fnv"
	"math/rand"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/ringpop-go"
	"github.com/uber/ringpop-go/swim"
)

// ErrUnknownService is thrown for a service that is not tracked by this instance
var ErrUnknownService = errors.New("Service not tracked by RingpopMonitor")

// ErrInsufficientHosts is thrown when there are not enough hosts to serve the request
var ErrInsufficientHosts = errors.New("Not enough hosts to serve the request")

// ErrUUIDLookupFailed is thrown when a uuid cannot be mapped to addr
var ErrUUIDLookupFailed = errors.New("Cannot find ip:port corresponding to uuid")

// ErrListenerAlreadyExist is thrown on a duplicate AddListener call from the same listener
var ErrListenerAlreadyExist = errors.New("Listener already exist for the service")

type (

	// RingpopMonitor monitors and maintains a list of
	// healthy hosts for a set of ring pop services
	RingpopMonitor interface {
		// Start starts the RingpopMonitor
		Start()
		// Stop stops the RingpopMonitor
		Stop()
		// GetBootstrappedChannel returns a channel, which will be closed once ringpop is bootstrapped
		GetBootstrappedChannel() chan struct{}
		// GetHosts retrieves all the members for the given service
		GetHosts(service string) ([]*HostInfo, error)
		// FindHostForAddr finds and returns the host for the given service:addr
		FindHostForAddr(service string, addr string) (*HostInfo, error)
		// FindHostForKey finds and returns the host responsible for handling the given (service, key)
		FindHostForKey(service string, key string) (*HostInfo, error)
		// FindRandomHost finds and returns a random host responsible for handling the given service
		FindRandomHost(service string) (*HostInfo, error)
		// IsHostHealthy returns true if the given (service, host) is healthy
		IsHostHealthy(service string, uuid string) bool
		// ResolveUUID resovles a host UUID to an IP address, if the host is found
		ResolveUUID(service string, uuid string) (string, error)
		// AddListener adds a listener for this service.
		// The listener will get notified on the given
		// channel, whenever there is host joining/leaving
		// the ring.
		// @service: The service to be listened on
		// @name: The name for identifying the listener
		// @notifyChannel: The channel on which the caller receives notifications
		AddListener(service string, name string, notifyChannel chan<- *RingpopListenerEvent) error
		// RemoveListener removes a listener for this service.
		RemoveListener(service string, name string) error
		// SetMetadata is used to set the given metadata on this ringpop instance
		SetMetadata(key string, data string) error
	}

	// RingpopEventType is a value type that
	// identifies a RingpopListener event.
	RingpopEventType int

	// RingpopListenerEvent represents any event
	// that a RingpopListener can get notified about.
	RingpopListenerEvent struct {
		// Type returns the RingpopEventType
		Type RingpopEventType
		// Key returns the HostUUID that
		// this event is about
		Key string
	}

	// Implementation of RingpopMonitor that just tracks all the
	// members of the different types of roles.
	ringpopMonitorImpl struct {
		started          int32
		stopped          int32
		shutdownC        chan struct{}
		shutdownWG       sync.WaitGroup
		serviceToInfo    map[string]*serviceInfo
		rp               *ringpop.Ringpop
		serviceToMembers atomic.Value // service name to list of hosts, copy on write map[service]membershipInfo
		uuidResolver     UUIDResolver
		hwInfoReader     HostHardwareInfoReader
		logger           bark.Logger
		oldChecksum      uint32
		serverCount      int
		bootstrapped     bool
		bootstrappedC    chan struct{}
	}

	serviceInfo struct {
		listeners     map[string]chan<- *RingpopListenerEvent
		listenerMutex sync.RWMutex
	}
	membershipInfo struct {
		asList []*HostInfo
		asMap  map[string]*HostInfo
	}
)

const (
	// HostAddedEvent indicates that a new host joined the ring
	HostAddedEvent RingpopEventType = 1 << iota
	// HostRemovedEvent indicates that a host left the ring
	HostRemovedEvent
)

const (
	// Frequency at which the background routine refreshes
	// ringpop membership list for every service
	refreshInterval = time.Second
	// suffix for the local rings we create for tapped services
	tapRingNameSuffix = "-tap"
)

// NewRingpopMonitor returns a new instance of RingpopMonitor,
// rp:
//		Ringpop instance of the local node
// services:
//		list of services we need to track
// UUIDResolver:
//		Resolver instance that can map uuids to addrs and vice-versa
// HostHardWareInfoReader:
//		HwInfoReader instance that can get the hosts' hardware spec
func NewRingpopMonitor(rp *ringpop.Ringpop, services []string, resolver UUIDResolver, hwInfoReader HostHardwareInfoReader, log bark.Logger) RingpopMonitor {

	instance := &ringpopMonitorImpl{
		shutdownC:     make(chan struct{}),
		shutdownWG:    sync.WaitGroup{},
		uuidResolver:  resolver,
		hwInfoReader:  hwInfoReader,
		serviceToInfo: make(map[string]*serviceInfo),
		logger:        log,
		rp:            rp,
		bootstrappedC: make(chan struct{}),
	}

	membershipMap := make(map[string]*membershipInfo)

	// For every service that we monitor, we maintain a
	// hashring of discovered members (just for us).
	for _, k := range services {
		instance.serviceToInfo[k] = &serviceInfo{
			listeners: make(map[string]chan<- *RingpopListenerEvent),
		}

		membershipMap[k] = &membershipInfo{
			asList: make([]*HostInfo, 0, 8),
			asMap:  make(map[string]*HostInfo),
		}
	}

	instance.serviceToMembers.Store(membershipMap)
	return instance
}

// Start starts RingpopMonitor in the background
func (rpm *ringpopMonitorImpl) Start() {
	if atomic.LoadInt32(&rpm.started) == 1 {
		return
	}
	if atomic.SwapInt32(&rpm.started, 1) == 1 {
		return
	}
	rpm.shutdownWG.Add(1)
	go rpm.workerLoop()
}

// Stop attempts to stop the RingpopMonitor routines
func (rpm *ringpopMonitorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&rpm.stopped, 0, 1) {
		return
	}
	close(rpm.shutdownC)
	if !AwaitWaitGroup(&rpm.shutdownWG, time.Second) {
		rpm.logger.Warn("Stop() - timed out waiting for worker to finish")
	}
}

func (rpm *ringpopMonitorImpl) GetBootstrappedChannel() chan struct{} {
	return rpm.bootstrappedC
}

// GetHosts retrieves all the members for the given service
func (rpm *ringpopMonitorImpl) GetHosts(service string) ([]*HostInfo, error) {
	membershipMap := rpm.serviceToMembers.Load().(map[string]*membershipInfo)
	membersInfo, ok := membershipMap[service]
	if !ok {
		return []*HostInfo{}, ErrUnknownService
	}
	if len(membersInfo.asList) < 1 {
		return []*HostInfo{}, ErrInsufficientHosts
	}
	return membersInfo.asList, nil
}

// FindHostForAddr finds and returns the host for the given address
func (rpm *ringpopMonitorImpl) FindHostForAddr(service string, addr string) (*HostInfo, error) {
	uuid, err := rpm.uuidResolver.ReverseLookup(addr)
	if err != nil {
		return &HostInfo{}, ErrUUIDLookupFailed
	}
	membershipMap := rpm.serviceToMembers.Load().(map[string]*membershipInfo)
	membersInfo, ok := membershipMap[service]
	if !ok {
		return &HostInfo{}, ErrUnknownService
	}
	hi, ok := membersInfo.asMap[uuid]
	if !ok {
		return &HostInfo{}, ErrUUIDLookupFailed
	}
	// return a copy to prevent the
	// caller from mutating our original
	ans := new(HostInfo)
	*ans = *hi
	return ans, nil
}

// FindHostForKey finds and returns the host responsible for handling the given key
func (rpm *ringpopMonitorImpl) FindHostForKey(service string, key string) (*HostInfo, error) {

	// this function should be consistent in picking and returning the
	// same node for a given key on a specific 'set' of hosts. the list
	// of hosts returned by rpm.GetHosts is guaranteed to be sorted by
	// ip-address (it is re-sorted on every 'refresh');, so we simply
	// hash the key and use it to pick the host from the list.

	// compute FNV-1a hash of the key
	fnv1a := fnv.New32a()
	fnv1a.Write([]byte(key))
	hash := int(fnv1a.Sum32())

	// get list of hosts that for the given service
	members, err := rpm.GetHosts(service)
	if err != nil {
		return nil, err
	}

	if members == nil || len(members) == 0 {
		return nil, ErrUnknownService
	}

	// pick the host corresponding to the hash and get a pointer to
	// a copy of the HostInfo, since it is treated as immutable.
	var host = *members[hash%len(members)]

	return &host, nil
}

// FindRandomHost finds and returns a random host responsible for handling the given key
func (rpm *ringpopMonitorImpl) FindRandomHost(service string) (*HostInfo, error) {
	members, err := rpm.GetHosts(service)
	if err != nil {
		return nil, err
	}

	return members[rand.Intn(len(members))], nil
}

// IsHostHealthy returns true if the given host is healthy and false otherwise
func (rpm *ringpopMonitorImpl) IsHostHealthy(service string, uuid string) bool {
	_, err := rpm.ResolveUUID(service, uuid)
	return (err == nil)
}

// ResolveUUID converts the given UUID into a host:port string
// Returns true on success and false on lookup failure
func (rpm *ringpopMonitorImpl) ResolveUUID(service string, uuid string) (string, error) {
	membershipMap := rpm.serviceToMembers.Load().(map[string]*membershipInfo)
	membersInfo, ok := membershipMap[service]
	if !ok {
		return "", ErrUnknownService
	}
	hi, ok := membersInfo.asMap[uuid]
	if !ok {
		return "", ErrUUIDLookupFailed
	}
	return hi.Addr, nil
}

// AddListener adds a listener for the service
func (rpm *ringpopMonitorImpl) AddListener(service string, name string, notifyChannel chan<- *RingpopListenerEvent) error {
	info, ok := rpm.serviceToInfo[service]
	if !ok {
		return ErrUnknownService
	}
	info.listenerMutex.Lock()
	_, ok = info.listeners[name]
	if ok {
		info.listenerMutex.Unlock()
		return ErrListenerAlreadyExist
	}
	info.listeners[name] = notifyChannel
	info.listenerMutex.Unlock()
	return nil
}

// RemoveListener
func (rpm *ringpopMonitorImpl) RemoveListener(service string, name string) error {
	info, ok := rpm.serviceToInfo[service]
	if !ok {
		return ErrUnknownService
	}
	info.listenerMutex.Lock()
	_, ok = info.listeners[name]
	if !ok {
		info.listenerMutex.Unlock()
		return nil
	}
	delete(info.listeners, name)
	info.listenerMutex.Unlock()
	return nil
}

//SetMetadata is used to set the metadata on this rp instance.
func (rpm *ringpopMonitorImpl) SetMetadata(key string, data string) error {
	labels, err := rpm.rp.Labels()
	if err != nil {
		rpm.logger.WithFields(bark.Fields{`error`: err}).Error("Unable to access ringpop Labels")
		return err
	}
	return labels.Set(key, data)
}

func (rpm *ringpopMonitorImpl) workerLoop() {

	quit := false
	refreshTimeout := time.After(0)
	rpm.logger.Info("RingpopMonitor worker loop started")

	for !quit {

		select {
		case <-refreshTimeout:
			rpm.refreshAll()
			refreshTimeout = time.After(refreshInterval)

			// broadcast bootstrap is done by closing the channel
			if !rpm.bootstrapped {
				close(rpm.bootstrappedC)
				rpm.bootstrapped = true
			}
		case <-rpm.shutdownC:
			quit = true
		}
	}

	rpm.shutdownWG.Done()
	rpm.logger.Info("RingpopMonitor worker loop stopped")
}

func (rpm *ringpopMonitorImpl) refreshAll() {

	curr := rpm.serviceToMembers.Load().(map[string]*membershipInfo)
	new := make(map[string]*membershipInfo)

	// force re-resolve on every refresh
	rpm.uuidResolver.ClearCache()

	changed := false

	added := make(map[string][]*HostInfo)
	removed := make(map[string][]*HostInfo)
	updated := make(map[string][]*HostInfo)

	for k := range rpm.serviceToInfo {
		added[k], removed[k], updated[k], new[k] = rpm.refresh(k, curr[k])
		if new[k] == nil {
			new[k] = curr[k]
			continue
		}
		changed = true
	}

	if !changed {
		return
	}

	rpm.serviceToMembers.Store(new)
	rpm.logger.WithFields(bark.Fields{`newembershipMap`: new}).Debug("Ring membership updated, new membershipMap")

	// Notify the listeners
	for k, v := range rpm.serviceToInfo {
		if len(added[k]) < 1 && len(removed[k]) < 1 {
			continue
		}
		for _, hi := range added[k] {
			rpm.notifyListeners(v, HostAddedEvent, hi.UUID)
		}
		for _, hi := range removed[k] {
			rpm.notifyListeners(v, HostRemovedEvent, hi.UUID)
		}
	}
}

// sort.Interface methods to help sort HostInfo by IP-address
type hostInfoByAddr []*HostInfo

func (t hostInfoByAddr) Len() int {
	return len(t)
}

func (t hostInfoByAddr) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t hostInfoByAddr) Less(i, j int) bool {
	return t[i].Addr < t[j].Addr
}

func (rpm *ringpopMonitorImpl) refresh(service string, currInfo *membershipInfo) (added, removed, updated []*HostInfo, newInfo *membershipInfo) {

	added = make([]*HostInfo, 0, 2)
	updated = make([]*HostInfo, 0, 2)
	removed = make([]*HostInfo, 0, 2)

	newInfo = &membershipInfo{
		// asList will be populated later only if needed
		asMap: make(map[string]*HostInfo),
	}

	// XXX: We should be querying this only if something changed.
	// We can use rp.Checksum() to find if something changed on the ring.
	// But since we have the hwinfo, for now let's just do all these things
	// together but we should be able to gossip even the hwinfo.
	rp := rpm.rp
	addrs, _ := rp.GetReachableMembers(swim.MemberWithLabelAndValue(roleKey, service))

	// make sure we get a sorted list of reachable members
	if addrs != nil && len(addrs) > 0 {
		sort.Strings(addrs)
	}
	for _, addr := range addrs {
		uuid, err := rpm.uuidResolver.ReverseLookup(addr)
		if err != nil {
			rpm.logger.WithFields(bark.Fields{`ringpopAddress`: addr}).Info("ReverseLookup failed")
			continue
		}

		hi := &HostInfo{Addr: addr, UUID: uuid}
		newInfo.asMap[uuid] = hi

		currHost, ok := currInfo.asMap[hi.UUID]
		if !ok {
			added = append(added, hi)
			continue
		}

		// Check if the uuid remains the same but ip addr changed
		if strings.Compare(currHost.Addr, hi.Addr) != 0 {
			updated = append(updated, hi)
		}
	}

	// Anything that's not in newInfo is now removed
	for _, hi := range currInfo.asList {
		_, ok := newInfo.asMap[hi.UUID]
		if !ok {
			removed = append(removed, hi)
		}
	}

	for _, v := range newInfo.asMap {

		if len(v.Sku) < 1 && len(v.Zone) < 1 && len(v.Rack) < 1 {
			// this host is missing hardware info, populate it
			// by querying cassandra. if the call to cassandra
			// fails, we will populate this during next refresh

			tokens := strings.Split(v.Addr, ":")
			if len(tokens) != 2 {
				continue
			}

			names, err := net.LookupAddr(tokens[0])
			if err != nil || len(names) == 0 {
				continue
			}

			v.Name = strings.Split(names[0], ".")[0] // cache hostname to avoid dns reverse lookup

			hwInfo, err := rpm.hwInfoReader.Read(names[0])
			if err != nil {
				rpm.logger.WithFields(bark.Fields{TagErr: err, `hostname`: names[0]}).Warn("Cannot read host_info")
				continue
			}

			v.Sku = hwInfo.Sku
			v.Rack = hwInfo.Rack
			v.Zone = hwInfo.Zone
		}
	}

	changed := ((len(added) | len(updated) | len(removed)) != 0)
	if !changed {
		newInfo = nil
		return
	}

	i := 0
	// Now make sure to populate asList in the new membership info
	newInfo.asList = make([]*HostInfo, len(newInfo.asMap))
	for _, v := range newInfo.asMap {
		newInfo.asList[i] = v
		i++
	}

	// Whenever the list changes, keep the list sorted by ip-address, so
	// functions like FindHostForKey don't need to sort them every time.
	sort.Sort(hostInfoByAddr(newInfo.asList))

	return
}

func (rpm *ringpopMonitorImpl) notifyListeners(info *serviceInfo, eventType RingpopEventType, key string) {
	event := &RingpopListenerEvent{
		Type: eventType,
		Key:  key,
	}

	info.listenerMutex.RLock()
	for name, ch := range info.listeners {
		select {
		case ch <- event:
		default:
			rpm.logger.WithFields(bark.Fields{`listenerName`: name}).Error("Failed to send listener notification, channel full")
		}
	}
	info.listenerMutex.RUnlock()
}
