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

package controllerhost

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
)

var errRetryable = errors.New("Retry later")

type (
	// Event represents the API interface that
	// all events must obey. The API is defined
	// to follow the try-catch-finally pattern.
	// When a new event arrives, the Handle()
	// method will be called to handle it. If
	// Handle returns a retryable error, it will
	// retried one or more times. After the event
	// is processed (success or failure), the Done
	// method will be called.
	Event interface {
		// Handle is the handler for the event.
		Handle(context *Context) error
		// Done is a callback to indicate that the
		// event processing is done. This is
		// an opportunity to cleanup any state that
		// the event had. The err arg is set to the
		// error value from the last attempt. Nil
		// indicates the handler finished successfully.
		Done(context *Context, err error)
	}

	// RetryableEventExecutor is an executor for
	// executing retryable event handlers. The
	// executor has in-bult retry/backoff logic
	// for retryable failures
	RetryableEventExecutor interface {
		// Submit takes a retryable event for
		// executing in the background. Returns
		// true on success, false otherwise
		Submit(event Event) bool
		// Stop stops the executor
		Stop()
	}

	// EventPipeline represents the
	// execution pipeline that
	// handles events in the background
	EventPipeline interface {
		common.Daemon
		// Add adds an event to the event pipeline
		// returns true on success
		// returns false when the event queue is full
		Add(event Event) bool
		// GetRetryableEventExecutor returns the executor for
		// running retryable events
		GetRetryableEventExecutor() RetryableEventExecutor
	}

	eventPipelineImpl struct {
		nWorkers      int // number of worker routines that handle events
		context       *Context
		eventQueue    chan Event // workers handle events from this queue
		retryExecutor RetryableEventExecutor
		started       int32
		log           bark.Logger
		shutdownC     chan struct{}
		startWG       sync.WaitGroup
		shutdownWG    sync.WaitGroup
	}
)

const (
	eventQueueSize = 2048
	// maxRetryWorkers determines the max number of event handlers
	// that can be retried in the background at given point of time
	maxRetryWorkers = 8192
)

// NewEventPipeline creates a new instance of EventPipeline
// The returned pipeline will have nWorkers number of concurrent
// worker routines that handle events in the background
func NewEventPipeline(context *Context, nWorkers int) EventPipeline {
	return &eventPipelineImpl{
		nWorkers:      nWorkers,
		context:       context,
		eventQueue:    make(chan Event, eventQueueSize),
		retryExecutor: NewRetryableEventExecutor(context, maxRetryWorkers),
		shutdownC:     make(chan struct{}),
		log:           context.log.WithField(common.TagModule, `EventPipeline`),
	}
}

func (ep *eventPipelineImpl) Start() {
	if !ep.setStarted() {
		return
	}
	// Spin up nWorkers routines in the
	// background and wait for them to
	// all start before returning from
	// this method
	for i := 0; i < ep.nWorkers; i++ {
		ep.shutdownWG.Add(1)
		ep.startWG.Add(1)
		go ep.eventLoop()
	}
	if !common.AwaitWaitGroup(&ep.startWG, time.Second) {
		ep.log.Fatal("Timed out waiting for workers to start")
	}

	ep.log.Info("Event pipeline started")
}

func (ep *eventPipelineImpl) Stop() {
	close(ep.shutdownC)
	ep.retryExecutor.Stop()
	if !common.AwaitWaitGroup(&ep.shutdownWG, time.Second) {
		ep.log.Error("Timed out waiting for event workers to shutdown")
		return
	}

	ep.log.Info("Event pipeline stopped")
}

// GetRetryableEventExecutor returns the retryable event executor
func (ep *eventPipelineImpl) GetRetryableEventExecutor() RetryableEventExecutor {
	return ep.retryExecutor
}

func (ep *eventPipelineImpl) Add(event Event) bool {
	if !ep.isStarted() {
		ep.log.Fatal("Attempt to add event before starting the event pipeline")
	}
	select {
	case ep.eventQueue <- event:
		return true
	default:
		ep.context.m3Client.IncCounter(metrics.EventPipelineScope, metrics.ControllerEventsDropped)
		ep.log.WithField(common.TagEvent, event).Warn("EventQueue full, dropping event")
		return false
	}
}

func (ep *eventPipelineImpl) isStarted() bool {
	return atomic.LoadInt32(&ep.started) == 1
}

func (ep *eventPipelineImpl) setStarted() bool {
	return atomic.CompareAndSwapInt32(&ep.started, 0, 1)
}

// eventLoop is the main routine that dispatches
// events to handlers. Multiple event loops run
// simultaneously, picking up work from the event
// queue.
func (ep *eventPipelineImpl) eventLoop() {
	ep.startWG.Done()
	for {
		select {
		case event := <-ep.eventQueue:
			err := event.Handle(ep.context)
			if err == errRetryable {
				if ep.retryExecutor.Submit(event) {
					break
				}
			}
			event.Done(ep.context, err)
		case <-ep.shutdownC:
			ep.shutdownWG.Done()
			return
		}
	}
}

// retryableEventExecutor is an executor that runs
// retryable event handlers. The executor automatically
// retries processing of events on retryable
// failures.
type retryableEventExecutor struct {
	sync.RWMutex
	tokens     int // number of concurrent workers
	stopped    int32
	shutdownC  chan struct{}
	shutdownWG sync.WaitGroup
	context    *Context
}

const (
	eventHandlerRetryMaxAttempts = 20
	eventHandlerRetryInterval    = 4 * time.Minute
)

// NewRetryableEventExecutor returns a new instance of RetryableEventExecutor
// This executor spins up a separate go routine for processing every event
// that's submitted. maxWorkers is an upper bound on the number of events
// that can be processed simulatenously. There are a few reasons for having
// a separate executor for retries as opposed to using the event pipeline:
//
//   * Retries can span upto an hour
//   * Retry workers are mostly idle, sleeping, we can have lots of them
//     in the background without consuming any resources
//   * We want the ability to spin up go routines when there is lots
//     retries needed, but in steady state, we dont need these many
//     go routines
//   * Retries should not impact or delay the processing of other
//     events
func NewRetryableEventExecutor(context *Context, maxWorkers int) RetryableEventExecutor {
	return &retryableEventExecutor{
		tokens:    maxWorkers,
		context:   context,
		shutdownC: make(chan struct{}),
	}
}

// Submit accepts an retryable event and runs it in a go routine
// returns true if the task was accepted, false otherwise
func (executor *retryableEventExecutor) Submit(event Event) bool {
	if !executor.acquireToken() {
		executor.context.m3Client.IncCounter(metrics.EventPipelineScope, metrics.ControllerErrNoRetryWorkers)
		return false
	}
	go executor.execute(event)
	return true
}

// Stop stops all the tasks started by this executor
func (executor *retryableEventExecutor) Stop() {
	executor.setStopped()
	close(executor.shutdownC)
	common.AwaitWaitGroup(&executor.shutdownWG, time.Second)
}

func (executor *retryableEventExecutor) execute(event Event) {

	defer executor.shutdownWG.Done()
	defer executor.releaseToken()

	context := executor.context

	if !executor.sleep(computeRetryInterval()) {
		// we are shutting down, exit asap
		event.Done(executor.context, errRetryable)
		return
	}

	var err error

	for i := 0; i < eventHandlerRetryMaxAttempts; i++ {

		context.m3Client.IncCounter(metrics.EventPipelineScope, metrics.ControllerRetries)

		err = event.Handle(executor.context)
		if err != errRetryable || !executor.sleep(computeRetryInterval()) {
			// break if non retryable event, or retryable, but cannot sleep due to shut down
			break
		}
	}

	if err != nil {
		context.m3Client.IncCounter(metrics.EventPipelineScope, metrics.ControllerRetriesExceeded)
	}

	event.Done(context, err)
}

func computeRetryInterval() time.Duration {
	jitter := int64(rand.Intn(60))
	return eventHandlerRetryInterval + time.Duration(int64(time.Second)*jitter)
}

func (executor *retryableEventExecutor) sleep(duration time.Duration) bool {
	select {
	case <-time.After(duration):
		return true
	case <-executor.shutdownC:
		return false
	}
}

func (executor *retryableEventExecutor) acquireToken() bool {
	executor.Lock()
	defer executor.Unlock()
	if executor.stopped == 1 || executor.tokens == 0 {
		return false
	}
	executor.tokens--
	executor.shutdownWG.Add(1)
	return true
}

func (executor *retryableEventExecutor) releaseToken() {
	executor.Lock()
	defer executor.Unlock()
	executor.tokens++
}

func (executor *retryableEventExecutor) setStopped() {
	executor.Lock()
	defer executor.Unlock()
	executor.stopped = 1
}
