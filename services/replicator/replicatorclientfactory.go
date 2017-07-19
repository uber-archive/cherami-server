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

package replicator

import (
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	"github.com/uber/cherami-thrift/.generated/go/replicator"
	"github.com/uber/cherami-thrift/.generated/go/shared"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

type (

	// ClientFactory manage discovery of remote replicator
	// and make sure we only allocate new thrift clients when necessary
	ClientFactory interface {
		// GetReplicatorClient gets the thrift client for making calls to remote Replicator
		GetReplicatorClient(zone string) (replicator.TChanReplicator, error)

		// SetTChannel sets the tchannel used by thrift client
		SetTChannel(ch *tchannel.Channel)

		// get hosts for a given deployment
		GetHostsForDeployment(deployment string) []string

		// set hosts for a given deployment
		UpdateHostsForDeployment(deployment string, hosts []string)

		// get hosts for all deployments
		GetHostsForAllDeployment() map[string][]string
	}

	// replicatorClientCache is the cache to hold a client for local replicator instance
	replicatorClienstCache struct {
	}

	// repCltFactoryImpl implements of ReplicatorClientFactory
	repCltFactoryImpl struct {
		AppConfig         configure.CommonAppConfig
		logger            bark.Logger
		ch                *tchannel.Channel
		deploymentClients map[string][]replicator.TChanReplicator
		lk                sync.RWMutex

		hosts  map[string][]string
		hostLk sync.RWMutex
	}
)

// NewReplicatorClientFactory instantiates a ReplicatorClientFactory object
func NewReplicatorClientFactory(config configure.CommonAppConfig, logger bark.Logger, hosts map[string][]string) ClientFactory {
	factory := &repCltFactoryImpl{
		AppConfig:         config,
		logger:            logger,
		deploymentClients: make(map[string][]replicator.TChanReplicator),
		hosts:             hosts,
	}
	return factory
}

// GetReplicatorClient gets the thrift client for making calls to remote Replicator
func (f *repCltFactoryImpl) GetReplicatorClient(zone string) (replicator.TChanReplicator, error) {
	_, tenancy := common.GetLocalClusterInfo(strings.ToLower(f.AppConfig.GetServiceConfig(common.ReplicatorServiceName).GetDeploymentName()))
	deployment := fmt.Sprintf("%v_%v", tenancy, zone)
	var client replicator.TChanReplicator

	// take a reader lock to see if we already have a valid client
	f.lk.RLock()
	if clients, ok := f.deploymentClients[deployment]; ok && len(clients) > 0 {
		// randomly select a client and return
		// TODO health check and proper load routing
		client = clients[rand.Intn(len(clients))]
	}
	f.lk.RUnlock()
	if client != nil {
		return client, nil
	}

	if !f.hasHostsForDeployment(deployment) {
		err := &shared.BadRequestError{Message: fmt.Sprintf("Deployment [%v] is not configured", deployment)}
		f.logger.WithFields(bark.Fields{common.TagErr: err, common.TagDeploymentName: deployment}).Error("Deployment is not configured")
		return nil, err
	}

	if f.ch == nil {
		err := &shared.BadRequestError{Message: "TChannel not set"}
		f.logger.WithFields(bark.Fields{common.TagErr: err, common.TagDeploymentName: deployment}).Error("TChannel not set")
		return nil, err
	}

	// take a writer lock to create replicator clients and update cache
	f.lk.Lock()
	defer f.lk.Unlock()

	// Check again to see if clients were already created by someone else
	if clients, ok := f.deploymentClients[deployment]; ok && len(clients) > 0 {
		return clients[rand.Intn(len(clients))], nil
	}

	hosts := f.GetHostsForDeployment(deployment)
	clients := make([]replicator.TChanReplicator, 0, len(hosts))

	for _, host := range hosts {
		clients = append(clients, replicator.NewTChanReplicatorClient(thrift.NewClient(
			f.ch,
			common.ReplicatorServiceName,
			&thrift.ClientOptions{HostPort: net.JoinHostPort(host, strconv.Itoa(f.AppConfig.GetServiceConfig(common.ReplicatorServiceName).GetPort()))},
		)))
	}
	f.deploymentClients[deployment] = clients

	return clients[rand.Intn(len(clients))], nil
}

// SetTChannel sets the tchannel used by thrift client
func (f *repCltFactoryImpl) SetTChannel(ch *tchannel.Channel) {
	f.ch = ch
}

func (f *repCltFactoryImpl) hasHostsForDeployment(deployment string) bool {
	f.hostLk.RLock()
	defer f.hostLk.RUnlock()

	_, hasHosts := f.hosts[deployment]
	return hasHosts
}

func (f *repCltFactoryImpl) GetHostsForDeployment(deployment string) []string {
	f.hostLk.RLock()
	defer f.hostLk.RUnlock()

	return f.hosts[deployment]
}

func (f *repCltFactoryImpl) GetHostsForAllDeployment() map[string][]string {
	f.hostLk.RLock()
	defer f.hostLk.RUnlock()

	return f.hosts
}

func (f *repCltFactoryImpl) UpdateHostsForDeployment(deployment string, hosts []string) {
	f.hostLk.Lock()
	defer f.hostLk.Unlock()

	f.hosts[deployment] = hosts
}
