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
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/store"

	"github.com/pborman/uuid"
)

// type funcSetResponseHeaders func(map[string]string) error
// type funcRead func() (*cherami.AppendMessage, error)
// type funcWrite func(msg *cherami.AppendMessageAck) error
// type funcFlush func() error
// type funcDone func() error

type inputhostStreamMock struct {
	sync.RWMutex

	extentUUID   uuid.UUID
	rand         *rand.Rand
	seqNum       uint64
	seqNumEnd    uint64
	enqueueTime  int64
	dataSize     int
	minRandDelay int32
	maxRandDelay int32

	done     bool
	err      error
	sentMsgs int32
	recvAcks int32

	fSetResponseHeaders func(*inputhostStreamMock, map[string]string) error
	fRead               func(*inputhostStreamMock) (*store.AppendMessage, error)
	fWrite              func(*inputhostStreamMock, *store.AppendMessageAck) error
	fFlush              func(*inputhostStreamMock) error
	fDone               func(*inputhostStreamMock) error
}

// SetResponseHeaders is the mock of BStoreOpenReadStreamInCall::ResponseHeaders
func (t *inputhostStreamMock) SetResponseHeaders(headers map[string]string) error {

	t.Lock()
	done := t.done
	t.Unlock()

	switch {
	case done:
		return io.EOF

	case t.fSetResponseHeaders != nil:
		return t.fSetResponseHeaders(t, headers)

	default:
		return nil // default: return "success"
	}
}

// Read is the mock of BStoreOpenReadStreamInCall::Read
func (t *inputhostStreamMock) Read() (*store.AppendMessage, error) {

	t.Lock()
	done := t.done
	t.Unlock()

	switch {
	case done:
		return nil, io.EOF

	case t.fRead != nil:
		return t.fRead(t)

	default:
		if t.seqNum++; t.seqNum != t.seqNumEnd {

			// generate a message with random bytes seeded on 'seqNum'
			r := rand.New(rand.NewSource(int64(t.seqNum)))

			data := make([]byte, t.dataSize)
			for i := range data {
				data[i] = byte(r.Intn(256))
			}

			msg := &store.AppendMessage{
				SequenceNumber: common.Int64Ptr(int64(t.seqNum)),
				EnqueueTimeUtc: common.Int64Ptr(time.Now().UnixNano()),
				Payload: &cherami.PutMessage{
					ID: common.StringPtr(fmt.Sprintf("%08X", t.seqNum)),
					DelayMessageInSeconds: common.Int32Ptr(t.minRandDelay + r.Int31n(t.maxRandDelay-t.minRandDelay+1)),
					Data: data,
				},
			}

			atomic.AddInt32(&t.sentMsgs, 1)

			// fmt.Printf("\rinputhostStreamMock: %v sent: seq #%x", t.extentUUID, t.seqNum)
			return msg, nil // return a "default" message/payload
		}

		// fmt.Printf("\nRead(): sent EOF #%x\n", t.seqNum)
		return nil, io.EOF
	}
}

func (t *inputhostStreamMock) Write(msg *store.AppendMessageAck) error {

	t.Lock()
	done := t.done
	t.Unlock()

	switch {
	case done:
		panic("Write on a closed stream.")

	case t.fWrite != nil:
		return t.fWrite(t, msg)

	default:
		if msg.GetStatus() == cherami.Status_OK {
			atomic.AddInt32(&t.recvAcks, 1)
			// fmt.Printf("\rinputhostStreamMock::Write: %v ack: seq #%x -> %x", t.extentUUID, msg.SequenceNumber, msg.Address)
			return nil // default: return "success"
		}

		// fmt.Printf("\ninputhostStreamMock::Write: %v ack: seq #%x -> %x, status: %v\n", t.extentUUID, msg.SequenceNumber, msg.Address, msg.Status)
		return nil // default: return "success"
	}
}

func (t *inputhostStreamMock) Flush() error {

	t.Lock()
	done := t.done
	t.Unlock()

	switch {
	case done:
		panic("Flush on a closed stream.")

	case t.fFlush != nil:
		return t.fFlush(t)

	default:
		return nil // default: return "success"
	}
}

func (t *inputhostStreamMock) Done() error {

	t.Lock()
	done := t.done
	t.Unlock()

	switch {
	case done:
		return io.EOF

	case t.fDone != nil:
		return t.fDone(t)

	default:
		t.Lock()
		t.err = fmt.Errorf("Done") // mark as "done"
		t.Unlock()
		return nil
	}
}

type outputhostStreamMock struct {
	sync.RWMutex
	extentUUID uuid.UUID
	rand       *rand.Rand
	totalCred  int32

	done     bool
	err      error
	sentCred int32
	recvMsgs int32

	fSetResponseHeaders func(*outputhostStreamMock, map[string]string) error
	fRead               func(*outputhostStreamMock) (*cherami.ControlFlow, error)
	fWrite              func(*outputhostStreamMock, *store.ReadMessageContent) error
	fFlush              func(*outputhostStreamMock) error
	fDone               func(*outputhostStreamMock) error
}

// SetResponseHeaders is the mock of BStoreOpenReadStreamInCall::ResponseHeaders
func (t *outputhostStreamMock) SetResponseHeaders(headers map[string]string) error {

	if t.fSetResponseHeaders != nil {
		return t.fSetResponseHeaders(t, headers)
	}

	return nil // default: return "success"
}

// Read is the mock of BStoreOpenReadStreamInCall::Read
func (t *outputhostStreamMock) Read() (*cherami.ControlFlow, error) {

	t.Lock()
	done := t.done
	t.Unlock()

	switch {
	case done:
		return nil, io.EOF

	case t.fRead != nil:
		return t.fRead(t)

	default:
		if atomic.LoadInt32(&t.sentCred) < t.totalCred {

			time.Sleep(10 * time.Millisecond)
			cf := &cherami.ControlFlow{Credits: common.Int32Ptr(int32(10))}

			atomic.AddInt32(&t.sentCred, cf.GetCredits())
			// fmt.Printf("outputhostStreamMock::Read: sentCred:%d, totalCred:%d\n", atomic.LoadInt32(&t.sentCred), t.totalCred)

			return cf, nil
		}

		// fmt.Printf("Read: no more credits; sleep for a while before returning EOF\n")
		for i := 0; i < 10000; i++ {
			if atomic.LoadInt32(&t.recvMsgs) >= t.totalCred {
				break
			}
			time.Sleep(1 * time.Millisecond)
		}
		// fmt.Println()

		t.Lock()
		t.err = io.EOF
		t.Unlock()
		return nil, io.EOF
	}
}

func (t *outputhostStreamMock) Write(msg *store.ReadMessageContent) error {

	t.Lock()
	done := t.done
	t.Unlock()

	switch {
	case done:
		panic("Write on a closed stream.")

	default:
		// fmt.Printf("\routputhostStreamMock::Write: recv addr:%x seq:%x data:%d bytes", msg.Address, msg.Message.SequenceNumber, len(msg.Message.Payload.Data))

		switch msg.GetType() {
		case store.ReadMessageContentType_MESSAGE:
			appMsg := msg.GetMessage().GetMessage()
			data := appMsg.GetPayload().GetData()
			r := rand.New(rand.NewSource(appMsg.GetSequenceNumber())) // seed on message seqnum

			for i := range data {
				if data[i] != byte(r.Intn(256)) {
					t.Lock()
					t.err = fmt.Errorf("corrupt (seqNum: %x): data[%d]", appMsg.GetSequenceNumber(), i)
					fmt.Printf("message corrupt (addr: %x seq: %x): data[%d]\n", msg.GetMessage().GetAddress(), appMsg.GetSequenceNumber(), i)
					t.Unlock()
					break
				}
			}

			atomic.AddInt32(&t.recvMsgs, 1)
		case store.ReadMessageContentType_SEALED:
		case store.ReadMessageContentType_ERROR:
		}

		return nil
	}
}

func (t *outputhostStreamMock) Flush() error {

	t.Lock()
	done := t.done
	t.Unlock()

	switch {
	case done:
		panic("Write on a closed stream.")

	case t.fFlush != nil:
		return t.fFlush(t)

	default:
		return nil // default: return "success"
	}
}

func (t *outputhostStreamMock) Done() error {

	t.Lock()
	done := t.done
	t.Unlock()

	switch {
	case done:
		return io.EOF

	case t.fDone != nil:
		return t.fDone(t)

	default:
		t.Lock()
		t.err = fmt.Errorf("Done") // mark as "done"
		t.Unlock()
		return nil
	}
}

func benchmarkByDataSize(b *testing.B, dataSize int) {

	mode := AppendOnly
	storeC := ManyRocks
	numMessages := b.N // read in number of messages from the benchmark

	var seqNumStart uint64 = 0x10000

	sName := "storehost-test"

	reporter := common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig())
	dClient := dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.StoreServiceName)
	sCommon := common.NewService(sName, uuid.New(), configure.NewCommonServiceConfig(), nil, nil, reporter, dClient, common.NewBypassAuthManager())
	storeHost, tc := NewStoreHost(sName, sCommon, nil, &Options{Store: storeC})
	storeHost.Start(tc)

	extentUUID := uuid.NewRandom()

	tInCall := &inputhostStreamMock{
		extentUUID: extentUUID,
		seqNum:     seqNumStart,
		seqNumEnd:  seqNumStart + uint64(numMessages) + 1,
	}

	data := make([]byte, dataSize)

	tInCall.fRead = func(t *inputhostStreamMock) (*store.AppendMessage, error) {

		if t.seqNum++; t.seqNum != t.seqNumEnd {

			enqueueTime := time.Now().UnixNano()
			delayMsg := rand.Int31()

			for i := 0; i < dataSize; i++ {
				data[i] = byte(rand.Intn(255))
			}

			msg := &store.AppendMessage{
				SequenceNumber: common.Int64Ptr(int64(t.seqNum)),
				EnqueueTimeUtc: common.Int64Ptr(enqueueTime),
				Payload: &cherami.PutMessage{
					ID: common.StringPtr(fmt.Sprintf("%08X", t.seqNum)),
					DelayMessageInSeconds: common.Int32Ptr(int32(delayMsg)),
					Data: data,
				},
			}

			// fmt.Printf("\nRead(): sent message #%x", t.seqNum)
			return msg, nil
		}

		// fmt.Printf("\nRead(): sent EOF #%x\n", t.seqNum)
		return nil, io.EOF
	}

	b.StartTimer()
	err := storeHost.OpenAppendStream(tCtxAppend(extentUUID, mode), tInCall)
	b.StopTimer()

	if err != nil {
		fmt.Println("OpenAppendStream returned error:", err)
	}

	b.SetBytes(int64(dataSize))

	// s.Equal(numMessages, recvAcks)
	// s.NoError(err)
}

func BenchmarkStore16B(b *testing.B) {
	benchmarkByDataSize(b, 16)
}

func BenchmarkStore64B(b *testing.B) {
	benchmarkByDataSize(b, 64)
}

func BenchmarkStore256B(b *testing.B) {
	benchmarkByDataSize(b, 256)
}

func BenchmarkStore1kiB(b *testing.B) {
	benchmarkByDataSize(b, 1024)
}

func BenchmarkStore64kiB(b *testing.B) {
	benchmarkByDataSize(b, 65536)
}

func BenchmarkStore640kiB(b *testing.B) {
	benchmarkByDataSize(b, 655360)
}

func BenchmarkStore1MiB(b *testing.B) {
	benchmarkByDataSize(b, 1048576)
}

/*
func BenchmarkStore10MiB(b *testing.B) {
	benchmarkByDataSize(b, 10485760)
}

func BenchmarkStore100MiB(b *testing.B) {
	benchmarkByDataSize(b, 104857600)
}

func BenchmarkStore1GiB(b *testing.B) {
	benchmarkByDataSize(b, 1024*1048576)
}

func BenchmarkStore10GiB(b *testing.B) {
	benchmarkByDataSize(b, 10*1024*1048576)
}
*/

func benchmarkParallelExtents(b *testing.B, dataSize int, numExtents int) {

	mode := TimerQueue
	storeC := ManyRocks
	numMessages := b.N

	var seqNumStart uint64 = 0x10000

	sName := "storehost-test"

	reporter := common.NewMetricReporterWithHostname(configure.NewCommonServiceConfig())
	dClient := dconfig.NewDconfigClient(configure.NewCommonServiceConfig(), common.StoreServiceName)

	sCommon := common.NewService(sName, uuid.New(), configure.NewCommonServiceConfig(), nil, nil, reporter, dClient, common.NewBypassAuthManager())
	storeHost, tc := NewStoreHost(sName, sCommon, nil, &Options{Store: storeC})
	storeHost.Start(tc)

	data := make([]byte, dataSize)

	fRead := func(t *inputhostStreamMock) (*store.AppendMessage, error) {

		if t.seqNum++; t.seqNum != t.seqNumEnd {

			enqueueTime := time.Now().UnixNano()
			delayMsg := rand.Int31()

			for i := 0; i < dataSize; i++ {
				data[i] = byte(rand.Intn(255))
			}

			msg := &store.AppendMessage{
				SequenceNumber: common.Int64Ptr(int64(t.seqNum)),
				EnqueueTimeUtc: common.Int64Ptr(enqueueTime),
				Payload: &cherami.PutMessage{
					ID: common.StringPtr(fmt.Sprintf("%08d", t.seqNum)),
					DelayMessageInSeconds: common.Int32Ptr(int32(delayMsg)),
					Data: data,
				},
			}

			// fmt.Println("\nRead(): sent message #%x", t.seqNum)
			return msg, nil
		}

		// fmt.Println("\nRead(): sent EOF #%x\n", t.seqNum)
		return nil, io.EOF
	}

	var wg sync.WaitGroup

	b.StartTimer()

	for i := 0; i < numExtents; i++ {

		extentUUID := uuid.NewRandom()

		loadGen := &inputhostStreamMock{
			extentUUID: extentUUID,
			seqNum:     seqNumStart,
			seqNumEnd:  seqNumStart + uint64(numMessages) + 1,
			fRead:      fRead,
		}

		wg.Add(1)

		go func() {

			err := storeHost.OpenAppendStream(tCtxAppend(extentUUID, mode), loadGen)

			if err != nil {
				fmt.Println("OpenAppendStream returned error:", err)
			}

			wg.Done()
		}()
	}

	wg.Wait()

	b.StopTimer()

	b.SetBytes(int64(numExtents * dataSize))

	// fmt.Println("sent %d messages (of %d bytes each) over each of %d extents; recv %d acks (expected: %d)\n",
	// 	numMessages, dataSize, numExtents, recvAcks, int32(numExtents*numMessages))

	// s.Equal(int32(numExtents*numMessages), recvAcks)
}

func BenchmarkStore16BParallelExtent4(b *testing.B) {
	benchmarkParallelExtents(b, 16, 4)
}

func BenchmarkStore16BParallelExtent16(b *testing.B) {
	benchmarkParallelExtents(b, 16, 16)
}

func BenchmarkStore16BParallelExtent64(b *testing.B) {
	benchmarkParallelExtents(b, 16, 64)
}

/*
func BenchmarkStore16BParallelExtent256(b *testing.B) {
	benchmarkParallelExtents(b, 16, 256)
}

func BenchmarkStore16BParallelExtent1024(b *testing.B) {
	benchmarkParallelExtents(b, 16, 1024)
}
*/

func BenchmarkStore512BParallelExtent4(b *testing.B) {
	benchmarkParallelExtents(b, 512, 4)
}

func BenchmarkStore1kiBParallelExtent4(b *testing.B) {
	benchmarkParallelExtents(b, 1024, 4)
}

func BenchmarkStore1kiBParallelExtent16(b *testing.B) {
	benchmarkParallelExtents(b, 1024, 16)
}

func BenchmarkStore1kiBParallelExtent64(b *testing.B) {
	benchmarkParallelExtents(b, 1024, 64)
}

/*
func BenchmarkStore1kiBParallelExtent256(b *testing.B) {
	benchmarkParallelExtents(b, 1024, 256)
}

func BenchmarkStore1kiBParallelExtent1024(b *testing.B) {
	benchmarkParallelExtents(b, 1024, 1024)
}
*/

func BenchmarkStore64kiBParallelExtent4(b *testing.B) {
	benchmarkParallelExtents(b, 65536, 4)
}

func BenchmarkStore64kiBParallelExtent16(b *testing.B) {
	benchmarkParallelExtents(b, 65536, 16)
}

func BenchmarkStore64kiBParallelExtent64(b *testing.B) {
	benchmarkParallelExtents(b, 65536, 64)
}

/*
func BenchmarkStore64kiBParallelExtent256(b *testing.B) {
	benchmarkParallelExtents(b, 65536, 256)
}

func BenchmarkStore64kiBParallelExtent1024(b *testing.B) {
	benchmarkParallelExtents(b, 65536, 1024)
}
*/

/*
func BenchmarkStore64kiBParallelExtent2048(b *testing.B) {
	benchmarkParallelExtents(b, 2048)
}

func BenchmarkStore64kiBParallelExtent4096(b *testing.B) {
	benchmarkParallelExtents(b, 4096)
}
*/
