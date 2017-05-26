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

package benchmark

import (
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/store"
	"github.com/uber/tchannel-go"
)

const msgSize = 1024
const msgsPerMB = 1 << 20 / msgSize
const totalMB = 256

const nMultiplier = msgsPerMB * totalMB // this is multiplied to b.N to control test size manually

var msgbuf []byte

const buflen = 1 << 20 // 1MB

func init() {
	msgbuf = make([]byte, buflen)
	for i := 0; i < buflen; i++ {
		msgbuf[i] = byte(rand.Intn(256))
	}
}

func getRandomData() []byte {
	offset := rand.Intn(buflen - msgSize) // be careful this serializes compute
	return msgbuf[offset : offset+msgSize]
}

func getAppendMessage(seq int) *store.AppendMessage {
	sequenceNumber := int64(seq)
	eutc := time.Now().UnixNano()
	id := strconv.Itoa(seq)

	appendMsg := store.NewAppendMessage()
	appendMsg.SequenceNumber = &sequenceNumber
	appendMsg.EnqueueTimeUtc = &eutc
	appendMsg.Payload = cherami.NewPutMessage()
	appendMsg.Payload.ID = &id
	appendMsg.Payload.Data = getRandomData()
	return appendMsg
}

func getPutMessage(seq int) *cherami.PutMessage {
	id := strconv.Itoa(seq)
	putMessage := cherami.NewPutMessage()
	putMessage.ID = &id
	putMessage.DelayMessageInSeconds = common.Int32Ptr(0)
	putMessage.Data = getRandomData()
	return putMessage
}

func findEphemeralPort() (string, int) {
	ip, _ := tchannel.ListenIP()
	addr := ip.String() + ":0"
	conn, err := net.Listen("tcp", addr)
	if err != nil {
		return "", 0
	}

	_, port, err := common.SplitHostPort(conn.Addr().String())
	if err != nil {
		conn.Close()
		return "", 0
	}

	conn.Close()
	return conn.Addr().String(), port
}

type mockHostInfoReader struct{}

func newMockHostInfoReader() *mockHostInfoReader {
	return &mockHostInfoReader{}
}

func (mock *mockHostInfoReader) Read(hostname string) (*common.HostHardwareInfo, error) {
	return &common.HostHardwareInfo{
		Sku:  "sku1",
		Rack: "rack1",
		Zone: "zone1",
	}, nil
}

type mockUUIDResolver struct {
	lk      sync.RWMutex
	data    map[string]string
	reverse map[string]string
}

func newMockUUIDResolver() *mockUUIDResolver {
	return &mockUUIDResolver{
		data:    make(map[string]string),
		reverse: make(map[string]string),
	}
}

func (m *mockUUIDResolver) Set(uuid string, hostport string) {
	m.lk.Lock()
	defer m.lk.Unlock()
	m.data[uuid] = hostport
	m.reverse[hostport] = uuid
}

func (m *mockUUIDResolver) Lookup(uuid string) (string, error) {
	m.lk.RLock()
	defer m.lk.RUnlock()
	return m.data[uuid], nil
}

func (m *mockUUIDResolver) ReverseLookup(addr string) (string, error) {
	m.lk.RLock()
	defer m.lk.RUnlock()
	return m.reverse[addr], nil
}

func (m *mockUUIDResolver) ClearCache() {}
