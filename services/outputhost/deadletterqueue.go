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

package outputhost

import (
	"fmt"
	"sync"
	"time"

	client "github.com/uber/cherami-client-go/client/cherami"
	"github.com/uber/cherami-client-go/common/backoff"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"
)

//                                                   +-------------------------------+
//                                                   |  manageMessageDeliveryCache   |
//                                                   +-------------------------------+
//                                                     |
//                                                     | dlq.publishCh (*msg)
//                                                     v
//  ...........................................      #################################
//  :           FIFO Buffer (*msg)            : <--> #         publishBuffer         #
//  :.........................................:      #################################
//                                                     |
//                                                     | dlq.publishBufferedCh (*msg)
//                                                     v
//  +-----------------------------------------+      #################################
//  | TChanBOutputHost.AckMessages(msg.AckID) | <--- #           publisher           #
//  +-----------------------------------------+      #################################
//                                                     |
//                                                     |
//                                                     v
//                                                   +-------------------------------+
//                                                   |     publisher.Publish(*msg)      |
//                                                   +-------------------------------+
//                                                     :
//                                                     :
//                                                     v
//                                                   +-------------------------------+
//                                                   |        DLQ Destination        |
//                                                   +-------------------------------+
//
// ~/perl5/bin/graph-easy --input=DeadLetterQueue.dot -as_ascii | sed "s#^#//  #" | pbcopy

// TODO: We need to limit how far back the last unacknowledged message can be, and stop consumption.
// This will limit growth of the "unlimited" delivery queue buffer.

var ackProcessorInitialized int32
var ackCh chan<- AckID

type deadLetterQueue struct {
	lclLg             bark.Logger
	isConfigured      bool // Indicates whether there is a DLQ destination configured; if not, we simply acknowledge the messages and log them
	publisher         client.Publisher
	publishCh         chan *cherami.ConsumerMessage
	publishBufferedCh chan *cherami.ConsumerMessage
	closeCh           chan struct{}
	consumerM3Client  metrics.Client
	m3Client          metrics.Client
	wg                sync.WaitGroup
}

func newDeadLetterQueue(ctx thrift.Context, lclLg bark.Logger, cgDesc shared.ConsumerGroupDescription, metaClient metadata.TChanMetadataService, m3Client metrics.Client, consumerM3Client metrics.Client) (dlq *deadLetterQueue, err error) {
	dlq = &deadLetterQueue{
		lclLg:             lclLg,
		isConfigured:      false,
		publishCh:         make(chan *cherami.ConsumerMessage, 1000),
		publishBufferedCh: make(chan *cherami.ConsumerMessage, 1000),
		closeCh:           make(chan struct{}),
		consumerM3Client:  consumerM3Client,
		m3Client:          m3Client,
	}

	dlq.wg.Add(2)
	go dlq.publishBuffer()
	go dlq.publish()

	// If no DLQ is configured, note that and return success; It would normally not be configured for the consumer groups of a DLQ destination
	if len(cgDesc.GetDeadLetterQueueDestinationUUID()) == 0 ||
		cgDesc.GetDeadLetterQueueDestinationUUID() == common.ZeroUUID {
		lclLg.Info("No DLQ destination configured")
		return
	}

	// At this point we can assign the DLQ tag. Before this point, the UUID might have zero length, which would cause a panic
	dlq.lclLg = lclLg.WithField(common.TagDLQID, common.FmtDLQID(cgDesc.GetDeadLetterQueueDestinationUUID()))
	lg := dlq.lclLg

	// Create the cheramiClient
	cheramiClient, err := client.NewClientWithFEClient(thisOutputHost.frontendClient, nil)
	if err != nil {
		lg.WithField(common.TagErr, err).Error("Unable to create DLQ publisher client")
		return nil, err
	}

	cPublisherReq := &client.CreatePublisherRequest{
		Path: cgDesc.GetDeadLetterQueueDestinationUUID(),
	}

	dlq.publisher = cheramiClient.CreatePublisher(cPublisherReq)

	if dlq.publisher == nil {
		errMsg := `Couldn't create DLQ publisher`
		lg.WithField(common.TagErr, err).Error("newDeadLetterQueue: CreatePublisher error")
		return nil, &cherami.InternalServiceError{Message: errMsg}
	}

	// Open the publisher
	err = dlq.publisher.Open()
	if err != nil {
		errMsg := fmt.Sprintf("Couldn't open DLQ publisher: Err:%#q", err)
		lg.WithField(common.TagErr, err).Error("newDeadLetterQueue: publisher Open error")
		return nil, &cherami.InternalServiceError{Message: errMsg}
	}

	dlq.isConfigured = true
	return
}

func (dlq *deadLetterQueue) publishBuffer() {
	defer dlq.wg.Done()
	var buffer []*cherami.ConsumerMessage
	var peekVal *cherami.ConsumerMessage
	var dlqPublishBufferOutCh chan *cherami.ConsumerMessage
	var bufferSizeLogTicker <-chan time.Time

	for {
		select {

		case msg := <-dlq.publishCh:
			if msg == nil {
				dlq.lclLg.Error("DLQ publisher received nil")
				continue
			}

			buffer = append(buffer, msg)
			peekVal = buffer[0]

			dlqPublishBufferOutCh = dlq.publishBufferedCh // Enable

			if bufferSizeLogTicker == nil { // Enable buffer log ticker, but don't reset it if it's already enabled
				bufferSizeLogTicker = time.Tick(time.Minute * 5) // TODO: make this minutes for production
			}

		case dlqPublishBufferOutCh <- peekVal:
			if len(buffer) > 1 { // There are 2 items on the buffer at least: current peekVal, and the new peekVal
				buffer = buffer[1:]
				peekVal = buffer[0]
			} else { // Peeked value is already published, queue should be empty, and this case should be disabled
				buffer = nil
				dlqPublishBufferOutCh = nil // disable this case
				bufferSizeLogTicker = nil   // disable buffer log ticker
			}

		case <-bufferSizeLogTicker:
			dlq.lclLg.WithField(`bufferLength`, len(buffer)).Debug(`DLQ publish buffer length`)
			// TODO: maybe some kind of signal to stop consumption

		case <-dlq.closeCh:
			// dlq.lclLg.WithField(`bufferLength`, len(buffer)).WithField(`publishCh)`, len(dlq.publishCh)).WithField(`publishBufferedChLength`, len(dlq.publishBufferedCh)).Error(`DLQ publish buffer closed`)
			for i, v := range buffer {
				dlq.lclLg.WithFields(bark.Fields{`i`: i + 1, `bufferLength`: len(buffer), common.TagAckID: common.FmtAckID(v.GetAckId())}).Errorf(`DLQ Didn't publish`)
			}
			return
		}
	}
}

func (dlq *deadLetterQueue) publish() {
	defer dlq.wg.Done()
	var err error
	for {
		select {
		case msg := <-dlq.publishBufferedCh:
			// All requests are noted in metrics
			dlq.m3Client.IncCounter(metrics.ConsConnectionStreamScope, metrics.OutputhostDLQMessageRequests)
			dlq.consumerM3Client.IncCounter(metrics.ConsConnectionScope, metrics.OutputhostCGDLQMessageRequests)

			id := msg.GetAckId()
			// dlq.lclLg.WithField(`id`, id).Debug(`DLQ publisher received`)

			// If configured, publish the message
			if dlq.isConfigured {
				policy := backoff.NewExponentialRetryPolicy(time.Second / 10)
				err = backoff.Retry(func() error {
					receipt := dlq.publisher.Publish(&client.PublisherMessage{
						Data:        msg.GetPayload().GetData(),
						UserContext: msg.GetPayload().GetUserContext(),
					})
					if receipt.Error != nil {
						dlq.lclLg.WithFields(bark.Fields{
							common.TagAckID: common.FmtAckID(string(id)),
							common.TagErr:   receipt.Error,
						}).Error(`Couldn't publish; error`)

						select { // Bail here on close
						default:
						case <-dlq.closeCh:
							return fmt.Errorf("closed") // Force retry loop to exit
						}

						// All failures are reported in metrics... this means we could have more failures than requests
						dlq.m3Client.IncCounter(metrics.ConsConnectionStreamScope, metrics.OutputhostDLQMessageFailures)
						dlq.consumerM3Client.IncCounter(metrics.ConsConnectionScope, metrics.OutputhostCGDLQMessageFailures)
					}
					return receipt.Error
				}, policy, func(_ error) bool { return true })
				//dlq.lclLg.WithField(common.TagAckID, common.FmtAckID(string(id))).Info(`Published message to DLQ`)
			}

			// If we exited based on a close signal, we will bail here without acknowledgement
			if err == nil {
				// In all cases, we should send the acknowledgement to the ack manager, who will release the message from the msg cache

				ackRequest := &cherami.AckMessagesRequest{AckIds: []string{string(id)}}
				ctx, _ := thrift.NewContext(time.Minute)
				invalidIDs := thisOutputHost.AckMessages(ctx, ackRequest)

				if invalidIDs != nil {
					dlq.lclLg.WithField(common.TagAckID, common.FmtAckID(string(id))).Error(`Couldn't acknowledge message published to DLQ`)
				} else {
					//dlq.lclLg.WithField(common.TagAckID, common.FmtAckID(string(id))).Info(`DLQ sent acknowledgement`)
				}
			}
		case <-dlq.closeCh:
			return
		}
	}
}

func (dlq *deadLetterQueue) getPublishCh() chan<- *cherami.ConsumerMessage {
	return dlq.publishCh
}

func (dlq *deadLetterQueue) close() {
	if dlq != nil {
		if dlq.publisher != nil {
			dlq.publisher.Close()
		}
		close(dlq.closeCh)
		dlq.wg.Wait()
	}
}
