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
	"sync"

	"github.com/uber-common/bark"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/controller"
	"github.com/uber/cherami-thrift/.generated/go/replicator"
	"github.com/uber/cherami-thrift/.generated/go/store"

	tchannel "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

type (

	// ClientFactory is the thrift clients
	// interface which is used to get thrift clients for this
	// service. This make sure we don't allocate new thrift clients
	// all the time and also makes sure the returned client is up and
	// running by consulting ringpop monitor
	// TODO: Add store, in, out, etc here so that we don't end up creating new thrift clients all the time
	ClientFactory interface {
		// GetAdminControllerClient gets the thrift version of the controller client
		GetAdminControllerClient() (admin.TChanControllerHostAdmin, error)
		// GetThriftStoreClient gets the thrift version of the store client
		GetThriftStoreClient(replica string, pathUUID string) (store.TChanBStore, error)
		// ReleaseThriftStoreClient releases the ref on this pathUUID
		ReleaseThriftStoreClient(pathUUID string)
		// GetThriftStoreClientUUID first resolves the store UUID to an address, before creating a thrift client
		GetThriftStoreClientUUID(storeUUID string, contextID string) (store.TChanBStore, string, error)
		// GetControllerClient gets the thrift client for making calls to Cherami Controller
		GetControllerClient() (controller.TChanController, error)
		// GetReplicatorClient gets the thrift client for making calls to local Replicator
		GetReplicatorClient() (replicator.TChanReplicator, error)
	}

	// adminControlClient is the existing controller host client object
	adminControlClient struct {
		lk sync.Mutex // mutex protecting this object
		// currentHostAddr is the current addr of the controller client
		currentHostAddr string
		// map of host address to the client object
		currentClient map[string]admin.TChanControllerHostAdmin
	}

	// controllerClientCache is the cache to hold a client for primary controller instance
	controllerClientCache struct {
		// mutex protecting this object
		lk sync.RWMutex
		// currentHostAddr is the current addr of the controller
		currentHostAddr string
		// Actual client object to make service calls
		currentClient controller.TChanController
	}

	// replicatorClientCache is the cache to hold a client for local replicator instance
	replicatorClientCache struct {
		// mutex protecting this object
		lk sync.RWMutex
		// currentHostAddr is the current addr of the replicator
		currentHostAddr string
		// Actual client object to make service calls
		currentClient replicator.TChanReplicator
	}

	// chInfo holds the chInfo for this replica
	chInfo struct {
		ref            int
		ch             *tchannel.Channel // the tchannel against this replica/id pair
		currentClients map[string]store.TChanBStore
	}

	// storeClient is the existing store client object
	storeClient struct {
		lk sync.Mutex // mutex protecting this object
		// map of uuid(could be destination uuid or a context uuid specified by GetThriftStoreClientUUID) to the client object
		currentCh map[string]*chInfo
	}

	// Implementation of ClientFactory
	thriftClientImpl struct {
		rpm              RingpopMonitor
		ch               *tchannel.Channel
		adminCClient     adminControlClient
		storeClients     storeClient
		cClient          controllerClientCache
		replicatorClient replicatorClientCache
		logger           bark.Logger
	}
)

// ErrNoClient is returned when the host is already shutdown
var ErrNoClient = &cherami.InternalServiceError{Message: "Unable to create client"}

// NewClientFactory just instantiates a thriftClientImpl object
func NewClientFactory(rpm RingpopMonitor, ch *tchannel.Channel, log bark.Logger) ClientFactory {
	tClients := &thriftClientImpl{
		rpm: rpm,
		ch:  ch,
		adminCClient: adminControlClient{
			currentClient: make(map[string]admin.TChanControllerHostAdmin),
		},
		storeClients: storeClient{
			currentCh: make(map[string]*chInfo),
		},
		cClient: controllerClientCache{},
		logger:  log,
	}
	return tClients
}

// GetAdminControllerClient returns the thrift ControllerHostAdminClient
func (h *thriftClientImpl) GetAdminControllerClient() (admin.TChanControllerHostAdmin, error) {
	var adminClient admin.TChanControllerHostAdmin
	var ok bool

	hostInfo, err := h.rpm.FindHostForKey(ControllerServiceName, ControllerServiceName)
	if err != nil {
		h.logger.WithFields(bark.Fields{TagErr: err}).Error("getControllerClient - Failed to find controller host")
		return nil, err
	}

	// first lock the mutex and check if we have the latest admin control client
	h.adminCClient.lk.Lock()
	// check if the hostInfo address is already in the map
	if adminClient, ok = h.adminCClient.currentClient[hostInfo.Addr]; !ok {
		// make sure to remove the existing client from the map, if any
		if _, exists := h.adminCClient.currentClient[h.adminCClient.currentHostAddr]; exists {
			h.logger.WithField(TagHostPort, FmtHostPort(h.adminCClient.currentHostAddr)).
				Debug("thriftClients: removing existing admin client from the map")
			delete(h.adminCClient.currentClient, h.adminCClient.currentHostAddr)
		}
		h.logger.WithField(TagHostPort, FmtHostPort(hostInfo.Addr)).
			Debug("thriftClients: creating new adminControlClient")
		extClient := thrift.NewClient(h.ch, ControllerServiceName, &thrift.ClientOptions{
			HostPort: hostInfo.Addr,
		})
		adminClient = admin.NewTChanControllerHostAdminClient(extClient)
		h.adminCClient.currentClient[hostInfo.Addr] = adminClient
		h.adminCClient.currentHostAddr = hostInfo.Addr
	}
	h.adminCClient.lk.Unlock()

	return adminClient, nil
}

// GetControllerClient returns the thrift client for ControllerHost
func (h *thriftClientImpl) GetControllerClient() (controller.TChanController, error) {
	hostInfo, err := h.rpm.FindHostForKey(ControllerServiceName, ControllerServiceName)
	if err != nil {
		h.logger.WithFields(bark.Fields{TagErr: err}).Error("getControllerClient - Failed to find controller host")
		return nil, err
	}

	var currentClient controller.TChanController
	// Take a reader lock to see if we already have a valid client
	h.cClient.lk.RLock()
	if hostInfo.Addr == h.cClient.currentHostAddr {
		currentClient = h.cClient.currentClient
	}
	h.cClient.lk.RUnlock()

	if currentClient == nil {
		// Controller client is outdated
		// Let's acquire a write-lock to create a new controller client and update cache
		h.cClient.lk.Lock()
		currentClient = h.cClient.currentClient
		// Check to see if the client is already created by someone else
		if currentClient == nil || hostInfo.Addr != h.cClient.currentHostAddr {
			// create a new client to make calls to controller
			extClient := thrift.NewClient(h.ch, ControllerServiceName, &thrift.ClientOptions{
				HostPort: hostInfo.Addr,
			})
			currentClient = controller.NewTChanControllerClient(extClient)

			// Now remember this client for later calls
			h.cClient.currentClient = currentClient
			h.cClient.currentHostAddr = hostInfo.Addr
		}
		h.cClient.lk.Unlock()
	}

	return currentClient, nil
}

// GetReplicatorClient returns the thrift client for Replicator
func (h *thriftClientImpl) GetReplicatorClient() (replicator.TChanReplicator, error) {
	hostInfo, err := h.rpm.FindHostForKey(ReplicatorServiceName, ReplicatorServiceName)
	if err != nil {
		h.logger.WithFields(bark.Fields{TagErr: err}).Error("getReplicatorClient - Failed to find replicator")
		return nil, err
	}

	var currentClient replicator.TChanReplicator
	// Take a reader lock to see if we already have a valid client
	h.replicatorClient.lk.RLock()
	if hostInfo.Addr == h.replicatorClient.currentHostAddr {
		currentClient = h.replicatorClient.currentClient
	}
	h.replicatorClient.lk.RUnlock()

	if currentClient == nil {
		// Replicator client is outdated
		// Let's acquire a write-lock to create a new replicator client and update cache
		h.replicatorClient.lk.Lock()
		currentClient = h.replicatorClient.currentClient
		// Check to see if the client is already created by someone else
		if currentClient == nil || hostInfo.Addr != h.replicatorClient.currentHostAddr {
			// create a new client to make calls to controller
			client := thrift.NewClient(h.ch, ReplicatorServiceName, &thrift.ClientOptions{
				HostPort: hostInfo.Addr,
			})
			currentClient = replicator.NewTChanReplicatorClient(client)

			// Now remember this client for later calls
			h.replicatorClient.currentClient = currentClient
			h.replicatorClient.currentHostAddr = hostInfo.Addr

			h.logger.WithField(TagHostPort, FmtHostPort(hostInfo.Addr)).
				Info("thriftClients: creating new replicatorClient")
		}
		h.replicatorClient.lk.Unlock()
	}

	return currentClient, nil
}

// GetThriftStoreClient gets the thrift client for the given replica/destUUID pair
func (h *thriftClientImpl) GetThriftStoreClient(replica string, destUUID string) (store.TChanBStore, error) {
	var ok bool
	var bStoreClient store.TChanBStore

	// we create one new tchannel for each destUUID
	h.storeClients.lk.Lock()
	defer h.storeClients.lk.Unlock()

	// first get the channel
	var currChInfo *chInfo
	if currChInfo, ok = h.storeClients.currentCh[destUUID]; !ok {
		h.logger.WithField(TagDst, destUUID).
			Debug("thriftClients: creating new channel for path")
		ch, err := tchannel.NewChannel(fmt.Sprintf("storehost-client-%v", destUUID), nil)
		if err != nil {
			return nil, err
		}
		currChInfo = &chInfo{
			ch:             ch,
			currentClients: make(map[string]store.TChanBStore),
		}
		h.storeClients.currentCh[destUUID] = currChInfo
	}

	if currChInfo == nil {
		h.logger.Error("thriftClients: currChInfo cannot be nil at this point")
		return nil, ErrNoClient
	}

	// check if the replica is already in the map
	if bStoreClient, ok = currChInfo.currentClients[replica]; !ok {
		h.logger.WithField(TagDst, destUUID).
			WithField(TagHostPort, FmtHostPort(replica)).
			Debug("thriftClients: creating new storeClient")
		tClient := thrift.NewClient(currChInfo.ch, StoreServiceName, &thrift.ClientOptions{
			HostPort: replica,
		})

		bStoreClient = store.NewTChanBStoreClient(tClient)
		currChInfo.currentClients[replica] = bStoreClient
	}

	currChInfo.ref++

	return bStoreClient, nil
}

// ReleaseThriftStoreClient releases the ref on the object and removes it from the map, if needed
func (h *thriftClientImpl) ReleaseThriftStoreClient(destUUID string) {
	h.storeClients.lk.Lock()
	// check if the replica is already in the map and release the client
	if currChInfo, ok := h.storeClients.currentCh[destUUID]; ok {
		currChInfo.ref--
		if currChInfo.ref == 0 {
			h.logger.WithField(TagDst, destUUID).
				Debugf("thriftClients: closing channel")
			currChInfo.ch.Close()
			// remove this from the map
			delete(h.storeClients.currentCh, destUUID)
			// remove all clients as well
			for replica := range currChInfo.currentClients {
				delete(currChInfo.currentClients, replica)
			}
		}
	}
	h.storeClients.lk.Unlock()
}

// GetThriftStoreClientUUID first resolves the store UUID to an address, before creating a thrift client
func (h *thriftClientImpl) GetThriftStoreClientUUID(storeUUID string, contextID string) (store.TChanBStore, string, error) {

	hostPort, err := h.rpm.ResolveUUID(StoreServiceName, storeUUID)

	if err != nil {
		h.logger.WithFields(bark.Fields{TagStor: storeUUID, TagErr: err}).
			Error("thriftClients: error resolving storehost")
		return nil, "", fmt.Errorf("error resolving storehost %v: %v", storeUUID, err)
	}

	client, err := h.GetThriftStoreClient(hostPort, contextID)
	return client, hostPort, err
}
