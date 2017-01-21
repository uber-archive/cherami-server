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

package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
)

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	httpHeaders := http.Header{}
	httpHeaders.Add("DestinationUUID", "481c4d7e-7a1c-4ce0-9731-180115e390ea")
	httpHeaders.Add("DestinationType", "PLAIN")
	httpHeaders.Add("ExtentUUID", "80c3f72d-1463-4843-8b9d-4023043f7b13")
	httpHeaders.Add("ConsumerGroupUUID", "9d334ff5-3419-418a-b168-0db99697a29a")
	httpHeaders.Add("Address", "0")
	httpHeaders.Add("Inclusive", "false")

	stream, err := common.NewWSConnector().OpenReplicationReadStream("localhost:6310", httpHeaders)
	if err != nil {
		fmt.Printf("dial error")
		return
	}

	fmt.Printf("starting pumps")

	// receive msg
	doneSingal := make(chan struct{})
	go func() {
		defer stream.Done()
		defer close(doneSingal)
		for {
			message, err := stream.Read()
			if err != nil {
				fmt.Printf("[Client] recv err: %v\n", err)
				return
			}
			fmt.Printf("recv msg: [type]%s, [seqNum]%v, [data]%v\n", message.GetType(), message.GetMessage().Message.GetSequenceNumber(), message.GetMessage().Message.Payload.Data)
		}
	}()

	// send credit
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case t := <-ticker.C:
			flows := cherami.NewControlFlow()
			flows.Credits = common.Int32Ptr(5)
			err := stream.Write(flows)
			if err != nil {
				fmt.Printf("[Client] send err: %v\n", err)
				return
			}
			fmt.Printf("sent credit: %s\n", t.String())
		case <-interrupt:
			fmt.Printf("interrupt\n")

			stream.Done()
			return
		}
	}
}
