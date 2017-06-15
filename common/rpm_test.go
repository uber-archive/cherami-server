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
	"strconv"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go"
)

type RpmSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
}

func TestRpmSuite(t *testing.T) {
	suite.Run(t, new(RpmSuite))
}

func (s *RpmSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *RpmSuite) TestRingpopMon() {

	resolver := newUUIDResolver()
	inputService := NewTestRingpopCluster("cherami-rpm-test", 3, "127.0.0.1", "", "in")
	s.NotNil(inputService, "Failed to create in service")
	outputService := NewTestRingpopCluster("cherami-rpm-test", 3, "127.0.0.1", inputService.GetSeedNode(), "out")
	s.NotNil(outputService, "Failed to create out service")

	for _, hi := range inputService.GetHostInfoList() {
		resolver.register(hi.UUID, hi.Addr)
	}

	for _, hi := range outputService.GetHostInfoList() {
		resolver.register(hi.UUID, hi.Addr)
	}

	channel, err := tchannel.NewChannel("tap-service", nil)
	s.Nil(err, "Failed to create tchannel for tap service")
	err = channel.ListenAndServe("127.0.0.1:0")
	s.Nil(err, "Failed to start tchannel on tap-service")

	services := []string{"in", "out"}

	rpm := NewRingpopMonitor(inputService.rings[0], services, resolver, newMockHostInfoReader(), bark.NewLoggerFromLogrus(log.New()))
	rpm.Start()

	inUUIDs := inputService.GetHostUUIDs()
	outUUIDs := outputService.GetHostUUIDs()

	inListenCh := make(chan *RingpopListenerEvent, 5)
	outListenCh := make(chan *RingpopListenerEvent, 5)

	err = rpm.AddListener("in", "in-listener", inListenCh)
	s.Nil(err, "AddListener failed")
	rpm.AddListener("out", "out-listener", outListenCh)
	s.Nil(err, "AddListener failed")

	detectedInHosts := make(map[string]bool)
	detectedOutHosts := make(map[string]bool)

	bootstrapped := rpm.GetBootstrappedChannel()
	bootstrapNotified := false

	timeoutCh := time.After(time.Minute)

	for {
		select {
		case e := <-inListenCh:
			s.Equal(HostAddedEvent, e.Type, "Wrong event type")
			detectedInHosts[e.Key] = true
		case e := <-outListenCh:
			s.Equal(HostAddedEvent, e.Type, "Wrong event type")
			detectedOutHosts[e.Key] = true
		case <-bootstrapped:
			bootstrapNotified = true
		case <-timeoutCh:
			s.Fail("Timed out waiting for hosts to be discovered")
		}

		if len(detectedInHosts) == len(inUUIDs) &&
			len(detectedOutHosts) == len(outUUIDs) {
			break
		}
	}

	for _, uuid := range inUUIDs {
		s.True(detectedInHosts[uuid], "Failed to receive HostAddedEvent for in-host")
		s.Equal(true, rpm.IsHostHealthy("in", uuid), "Ringpop monitor failed to detect in-service member")
		s.Equal(false, rpm.IsHostHealthy("out", uuid), "Ringpop monitor state is corrupted")
	}

	for _, uuid := range outUUIDs {
		s.True(detectedOutHosts[uuid], "Failed to receive HostAddedEvent for out-host")
		s.Equal(true, rpm.IsHostHealthy("out", uuid), "Ringpop monitor failed to detect out-service member")
		s.Equal(false, rpm.IsHostHealthy("in", uuid), "Ringpop monitor state is corrupted")
	}

	s.Equal(true, bootstrapNotified, `bootstrap not notified`)

	inAddrs := make(map[string]bool)
	outAddrs := make(map[string]bool)

	for _, addr := range inputService.GetHostAddrs() {
		inAddrs[addr] = true
	}
	for _, addr := range outputService.GetHostAddrs() {
		outAddrs[addr] = true
	}

	inputHost, err := rpm.FindHostForKey("in", "key")
	s.Nil(err, "Ringpop monitor failed to find host for key")
	s.Equal("rack1", inputHost.Rack, "Wrong rack for host")
	s.Equal("sku1", inputHost.Sku, "Wrong sku for host")
	s.Equal("zone1", inputHost.Zone, "Wrong zone for host")

	_, ok := inAddrs[inputHost.Addr]
	s.Equal(true, ok, "Ringpop monitor returned invalid in host addr")

	outputHost, err := rpm.FindHostForKey("out", "key")
	s.Nil(err, "Ringpop monitor failed to find host for key")
	s.Equal("rack1", inputHost.Rack, "Wrong rack for host")
	s.Equal("sku1", inputHost.Sku, "Wrong sku for host")
	s.Equal("zone1", inputHost.Zone, "Wrong zone for host")

	_, ok = outAddrs[outputHost.Addr]
	s.Equal(true, ok, "Ringpop monitor returned invalid out host addr")

	for i := 0; i < len(inUUIDs); i++ {
		_, err = rpm.ResolveUUID("in", inUUIDs[i])
		s.Nil(err, "Failed to resolve in host uuid")
	}

	outputService.KillHost(outUUIDs[0])

	select {
	case e := <-outListenCh:
		s.Equal(HostRemovedEvent, e.Type, "Wrong event type")
		s.Equal(outUUIDs[0], e.Key, "Wrong key for event")
	case <-time.After(time.Minute):
		s.Fail("Timed out waiting for failure to be detected by ringpop")
	}

	err = rpm.RemoveListener("in", "in-listener")
	s.Nil(err, "RemoveListener() failed")
	err = rpm.RemoveListener("out", "out-listener")
	s.Nil(err, "RemoveListener() failed")

	inputService.Stop()
	outputService.Stop()

	rpm.Stop()
	channel.Close()
}

func (s *RpmSuite) TestAddRemoveListeners() {

	resolver := newUUIDResolver()
	inputService := NewTestRingpopCluster("cherami-listen-test", 3, "127.0.0.1", "", "in")
	s.NotNil(inputService, "Failed to create in service")
	outputService := NewTestRingpopCluster("cherami-listen-test", 3, "127.0.0.1", inputService.GetSeedNode(), "out")
	s.NotNil(outputService, "Failed to create out service")

	for _, hi := range inputService.GetHostInfoList() {
		resolver.register(hi.UUID, hi.Addr)
	}

	for _, hi := range outputService.GetHostInfoList() {
		resolver.register(hi.UUID, hi.Addr)
	}

	channel, err := tchannel.NewChannel("tap-service", nil)
	s.Nil(err, "Failed to create tchannel for tap service")
	err = channel.ListenAndServe("127.0.0.1:0")
	s.Nil(err, "Failed to start tchannel on tap-service")

	services := []string{"in", "out"}

	rpm := NewRingpopMonitor(inputService.rings[0], services, resolver, newMockHostInfoReader(), bark.NewLoggerFromLogrus(log.New()))
	rpm.Start()

	inListenCh := make(chan *RingpopListenerEvent, 5)
	outListenCh := make(chan *RingpopListenerEvent, 5)

	for i := 0; i < 10; i++ {
		inName := "in-listener" + strconv.Itoa(i)
		err = rpm.AddListener("in", inName, inListenCh)
		s.Nil(err, "AddListener failed, name=%v", inName)
		err = rpm.AddListener("in", inName, inListenCh)
		s.NotNil(err, "AddListener msut fail, but succeeded")

		outName := "out-listener" + strconv.Itoa(i)
		err = rpm.AddListener("out", outName, outListenCh)
		s.Nil(err, "AddListener failed, name=%v", outName)
		err = rpm.AddListener("out", outName, inListenCh)
		s.NotNil(err, "AddListener msut fail, but succeeded")
	}

	for i := 0; i < 10; i++ {

		inName := "in-listener" + strconv.Itoa(i)
		err = rpm.RemoveListener("out", inName)
		s.Nil(err, "RemoveListener failed")
		err = rpm.RemoveListener("in", inName)
		s.Nil(err, "RemoveListener failed")
		err = rpm.RemoveListener("in", inName)
		s.Nil(err, "RemoveListener failed")

		outName := "out-listener" + strconv.Itoa(i)
		err = rpm.RemoveListener("in", outName)
		s.Nil(err, "RemoveListener failed")
		err = rpm.RemoveListener("out", outName)
		s.Nil(err, "RemoveListener failed")
		err = rpm.RemoveListener("out", outName)
		s.Nil(err, "RemoveListener failed")
	}

	inputService.Stop()
	outputService.Stop()

	rpm.Stop()
	channel.Close()
}

type mockHostInfoReader struct{}

func newMockHostInfoReader() *mockHostInfoReader {
	return &mockHostInfoReader{}
}

func (mock *mockHostInfoReader) Read(hostname string) (*HostHardwareInfo, error) {
	return &HostHardwareInfo{
		Sku:  "sku1",
		Rack: "rack1",
		Zone: "zone1",
	}, nil
}

type uuidResolver struct {
	mu    sync.Mutex
	cache map[string]string
}

func newUUIDResolver() *uuidResolver {
	return &uuidResolver{
		cache: make(map[string]string),
	}
}

func (r *uuidResolver) Lookup(uuid string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.cache[uuid]
	if !ok {
		log.WithField(`rpmUUID`, uuid).Info("lookup failed")
		return "", fmt.Errorf("lookup failed")
	}
	return v, nil
}

func (r *uuidResolver) ReverseLookup(addr string) (string, error) {
	return r.Lookup(addr)
}

func (r *uuidResolver) ClearCache() {
	return
}

func (r *uuidResolver) register(uuid string, addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache[uuid] = addr
	r.cache[addr] = uuid
	log.WithFields(log.Fields{`rpmUUID`: uuid, `rpmAddress`: addr}).Info("rpm register: added")
}
