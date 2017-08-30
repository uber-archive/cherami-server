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
	"sync"
	"time"

	"github.com/uber-common/bark"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	storeStream "github.com/uber/cherami-server/stream"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

type (
	inConnection struct {
		extUUID string
		stream  storeStream.BStoreOpenReadStreamInCall
		msgCh   <-chan *store.ReadMessageContent

		logger              bark.Logger
		m3Client            metrics.Client
		destM3Client        metrics.Client
		metricsScope        int
		perDestMetricsScope int

		creditsCh            chan int32 // channel to pass credits from readCreditsStream to writeMsgsStream
		creditFlowExpiration time.Time  // credit expiration is used to close the stream if we don't receive any credit for some period of time

		wg         sync.WaitGroup
		shutdownCh chan struct{}
	}
)

const (
	creditFlowTimeout = 10 * time.Minute

	flushTimeout = 50 * time.Millisecond
)

func newInConnection(extUUID string, destPath string, stream storeStream.BStoreOpenReadStreamInCall, msgCh <-chan *store.ReadMessageContent, logger bark.Logger, m3Client metrics.Client, metricsScope int, perDestMetricsScope int) *inConnection {
	localLogger := logger.WithFields(bark.Fields{
		common.TagExt:    extUUID,
		common.TagDstPth: destPath,
		`scope`:          `inConnection`,
	})
	conn := &inConnection{
		extUUID:              extUUID,
		stream:               stream,
		msgCh:                msgCh,
		logger:               localLogger,
		m3Client:             m3Client,
		destM3Client:         metrics.NewClientWithTags(m3Client, metrics.Replicator, common.GetDestinationTags(destPath, localLogger)),
		metricsScope:         metricsScope,
		perDestMetricsScope:  perDestMetricsScope,
		creditsCh:            make(chan int32, 5),
		creditFlowExpiration: time.Now().Add(creditFlowTimeout),
		shutdownCh:           make(chan struct{}),
	}

	return conn
}

func (conn *inConnection) open() {
	conn.wg.Add(2)
	go conn.writeMsgsStream()
	go conn.readCreditsStream()
	conn.logger.Info("in connection opened")
}

func (conn *inConnection) WaitUntilDone() {
	conn.wg.Wait()
}

func (conn *inConnection) shutdown() {
	close(conn.shutdownCh)
	conn.logger.Info(`in connection shutdown`)
}

func (conn *inConnection) readCreditsStream() {
	defer conn.wg.Done()
	defer close(conn.creditsCh)
	for {
		msg, err := conn.stream.Read()
		if err != nil {
			conn.logger.WithField(common.TagErr, err).Info("read credit failed")
			return
		}

		conn.m3Client.AddCounter(conn.metricsScope, metrics.ReplicatorInConnCreditsReceived, int64(msg.GetCredits()))

		// send this to writeMsgsPump which keeps track of the local credits
		// Make this non-blocking because writeMsgsStream could be closed before this
		select {
		case conn.creditsCh <- msg.GetCredits():
		case <-conn.shutdownCh:
			return
		default:
			conn.logger.
				WithField(`channelLen`, len(conn.creditsCh)).
				WithField(`credits`, msg.GetCredits()).
				Warn(`Dropped credits because of blocked channel`)
		}
	}
}

func (conn *inConnection) writeMsgsStream() {
	defer conn.wg.Done()
	defer conn.stream.Done()

	flushTicker := time.NewTicker(flushTimeout)
	defer flushTicker.Stop()

	var localCredits int32
	for {
		if localCredits == 0 {
			select {
			case credit, ok := <-conn.creditsCh:
				if !ok {
					conn.logger.Info(`internal credit channel closed`)
					return
				}
				conn.extentCreditExpiration()
				localCredits += credit
			case <-time.After(creditFlowTimeout):
				conn.logger.Warn("credit flow timeout")
				if conn.isCreditFlowExpired() {
					conn.logger.Warn("credit flow expired")
					return
				}
			case <-conn.shutdownCh:
				return
			}
		} else {
			select {
			case msg, ok := <-conn.msgCh:
				if !ok {
					conn.logger.Info("msg channel closed")
					return
				}
				if err := conn.stream.Write(msg); err != nil {
					conn.logger.Error("write msg failed")
					return
				}

				if msg.GetType() == store.ReadMessageContentType_SEALED {
					conn.logger.Info(`sealed msg read`)
				}

				conn.m3Client.IncCounter(conn.metricsScope, metrics.ReplicatorInConnMsgWritten)

				// Update per destination metrics after msg is sent to local store (call is OpenReplicationRemoteRead)
				// so they're most accurate.
				if conn.metricsScope == metrics.OpenReplicationRemoteReadScope && msg.GetType() == store.ReadMessageContentType_MESSAGE {
					conn.destM3Client.IncCounter(conn.perDestMetricsScope, metrics.ReplicatorInConnPerDestMsgWritten)
					latency := time.Duration(time.Now().UnixNano() - msg.GetMessage().Message.GetEnqueueTimeUtc())
					conn.destM3Client.RecordTimer(conn.perDestMetricsScope, metrics.ReplicatorInConnPerDestMsgLatency, latency)
				}

				localCredits--
			case credit, ok := <-conn.creditsCh:
				if !ok {
					conn.logger.Info(`internal credit channel closed`)
					return
				}
				conn.extentCreditExpiration()
				localCredits += credit
			case <-flushTicker.C:
				if err := conn.stream.Flush(); err != nil {
					conn.logger.Error(`flush msg failed`)
					return
				}
			case <-conn.shutdownCh:
				return
			}
		}
	}
}

func (conn *inConnection) isCreditFlowExpired() (ret bool) {
	ret = conn.creditFlowExpiration.Before(time.Now())
	return
}

func (conn *inConnection) extentCreditExpiration() {
	conn.creditFlowExpiration = time.Now().Add(creditFlowTimeout)
}
