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

package inputhost

import (
	"sync"
	"time"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	storeStream "github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/store"

	"github.com/uber-common/bark"
	"golang.org/x/net/context"
)

type (
	replicaConnection struct {
		call          storeStream.BStoreOpenAppendStreamOutCall
		destM3Client  metrics.Client
		logger        bark.Logger
		replyCh       chan prepAck
		cancel        context.CancelFunc
		closeChannel  chan struct{}
		putMessagesCh chan *replicaPutMsg
		waitWG        sync.WaitGroup

		sentMsgs   int64
		recvAcks   int64
		failedMsgs int64

		lk     sync.Mutex
		opened bool
		closed bool
	}

	// replicaPutMsg is the message received from the exthost
	// this message holds the actual append message to be sent to the
	// store and the ack channel which will be used to ack this message
	// to the exthost
	replicaPutMsg struct {
		appendMsg      *store.AppendMessage
		appendMsgAckCh chan *store.AppendMessageAck
	}

	// prepAck is kept internally by replicaConnection to prepare inflight messages
	// this holds the sequence number and the append message ack
	prepAck struct {
		seqNo        int64
		appendMsgAck chan *store.AppendMessageAck
	}
)

func newReplicaConnection(stream storeStream.BStoreOpenAppendStreamOutCall, cancel context.CancelFunc, destM3Client metrics.Client, logger bark.Logger) *replicaConnection {
	conn := &replicaConnection{
		call:          stream,
		cancel:        cancel,
		destM3Client:  destM3Client,
		logger:        logger.WithField(common.TagModule, `replConn`),
		replyCh:       make(chan prepAck, defaultBufferSize),
		closeChannel:  make(chan struct{}),
		putMessagesCh: make(chan *replicaPutMsg, defaultBufferSize),
	}

	return conn
}

func (conn *replicaConnection) open() {
	conn.lk.Lock()
	defer conn.lk.Unlock()

	if !conn.opened {
		conn.waitWG.Add(2)
		go conn.readAcksPump()
		go conn.writeMessagesPump()

		conn.opened = true
		conn.logger.Info("replConn opened")
	}
}

func (conn *replicaConnection) close() {
	conn.lk.Lock()
	if !conn.closed {
		conn.logger.Debug("closing replica connection")
		conn.closed = true
		close(conn.closeChannel)
		if ok := common.AwaitWaitGroup(&conn.waitWG, defaultWGTimeout); !ok {
			conn.logger.Fatal("waitgroup timed out")
		}

		conn.logger.WithFields(bark.Fields{
			`sentMsgs`:   conn.sentMsgs,
			`recvAcks`:   conn.recvAcks,
			`failedMsgs`: conn.failedMsgs,
		}).Info("replConn closed")
	}
	conn.lk.Unlock()
}

func (conn *replicaConnection) writeMessagesPump() {
	defer conn.waitWG.Done()
	defer conn.call.Done()
	if conn.cancel != nil {
		defer conn.cancel()
	}

	var unflushedWrites, unflushedSize int
	flushTicker := time.NewTicker(common.FlushTimeout) // start ticker to flush websocket  stream
	defer flushTicker.Stop()

	flushMsgs := func() {

		if err := conn.call.Flush(); err != nil {
			conn.logger.WithFields(bark.Fields{common.TagErr: err, `unflushedWrites`: unflushedWrites, `putMessagesChLength`: len(conn.putMessagesCh), `replyChLength`: len(conn.replyCh)}).Error(`inputhost: error flushing messages to replica stream`)
			// since flush failed, trigger a close of the connection which will fail inflight messages
			go conn.close()
			return
		}

		conn.destM3Client.AddCounter(metrics.ReplicaConnectionScope, metrics.InputhostDestMessageSentBytes, int64(unflushedSize))
		unflushedWrites, unflushedSize = 0, 0
	}

	for {
		select {
		case msg, ok := <-conn.putMessagesCh:
			if !ok {
				conn.logger.Error("inputhost: replicaConnection: put message ch closed")
				go conn.close()
				return
			}
			if err := conn.call.Write(msg.appendMsg); err != nil {
				conn.logger.WithField(common.TagErr, err).Error(`inputhost: error writing messages to replica stream failed`)
				// since write failed, trigger a close of the connection which will fail inflight messages
				go conn.close()
				return
			}

			conn.sentMsgs++

			unflushedWrites++

			if msg.appendMsg.Payload != nil { // no inflight info for watermarks
				// prepare inflight map
				conn.replyCh <- prepAck{msg.appendMsg.GetSequenceNumber(), msg.appendMsgAckCh}
				unflushedSize += len(msg.appendMsg.Payload.Data)
			}

			if unflushedWrites >= common.FlushThreshold {
				flushMsgs()
			}

		case <-flushTicker.C:

			if unflushedWrites > 0 {
				flushMsgs()
			}

		case <-conn.closeChannel:
			return
		}
	}
}

func (conn *replicaConnection) readAcksPump() {
	inflightMessages := make(map[int64]chan *store.AppendMessageAck)

	isEOF := false
	for {
		select {
		case resCh := <-conn.replyCh:
			inflightMessages[resCh.seqNo] = resCh.appendMsgAck
		default:
			if isEOF || len(inflightMessages) == 0 {
				select {
				// We want to make sure that ackId is in the inflightMessages before we read a response for it
				case resCh := <-conn.replyCh:
					inflightMessages[resCh.seqNo] = resCh.appendMsgAck
					// Connection is closed and there is nothing in the inflightMessages
				case <-conn.closeChannel:
					// fail all inflight messages here
					conn.failInflightMessages(inflightMessages)
					return
				}
			} else {
				ack, err := conn.call.Read()
				if err != nil {
					go conn.close()
					conn.logger.WithField(common.TagErr, err).Error(`replica connection closed`)
					isEOF = true
				} else {
					ackChannel, exists := inflightMessages[ack.GetSequenceNumber()]
					// Note: we can get an ack for a message which is not in
					// the inflight map.
					// why? because we fill up the inflight map *if and only if*
					// all the replicas succeeded in writing the message
					// see: prepReplicas() in exthost.go
					if exists {
						// log disabled due to CPU cost
						// conn.logger.WithField(common.TagSeq, ack.GetSequenceNumber()).Debug(`Ack received from replica for msg`)
						delete(inflightMessages, ack.GetSequenceNumber())
						ackChannel <- ack
					} else {
						// we got an ack from the replica which is not in the map. log it!
						conn.logger.WithField(common.TagSeq, ack.GetSequenceNumber()).Debug("Ack received from replica for msg not in the inflightMap (because the write failed on other replica(s)?)")
					}

					conn.recvAcks++
				}
			}
		}
	}
}

func (conn *replicaConnection) failInflightMessages(inflightMessages map[int64]chan *store.AppendMessageAck) {
	defer conn.waitWG.Done()
	for id, msgCh := range inflightMessages {
		appendMsgAck := &store.AppendMessageAck{
			SequenceNumber: common.Int64Ptr(id),
			Status:         common.CheramiStatusPtr(cherami.Status_FAILED),
			Message:        common.StringPtr("closing down"),
		}
		select {
		case msgCh <- appendMsgAck:
			conn.failedMsgs++
		default:
		}
	}
}
