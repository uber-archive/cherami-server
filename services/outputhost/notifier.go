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
	"sync"
)

// NotifyType is the notify channel type
type NotifyType int

const (
	// ThrottleDown is the message to stop throttling the connections
	ThrottleDown = iota
	// ThrottleUp is the messages to start throttling connections
	ThrottleUp
	// NumNotifyTypes is the overall notification types
	NumNotifyTypes
)

type (
	// Notifier is the interface to notify connections about nacks and potentially
	// slowing them down, if needed.
	// Any one who wants to get notified should Register with the notifier by specifying a
	// unique ID and a channel to receive the notification.
	Notifier interface {
		// Register is the rotuine to register this connection on the notifier
		Register(id int, notifyCh chan NotifyMsg)

		// Unregister is the routine to unregister this connection from the notifier
		Unregister(id int)

		// Notify is the routine to send a notification on the appropriate channel
		Notify(id int, notifyMsg NotifyMsg)
	}

	// NotifyMsg is the message sent on the notify channel
	NotifyMsg struct {
		notifyType NotifyType
	}

	notifierImpl struct {
		lk           sync.RWMutex           // lock protecting the map
		regCustomers map[int]chan NotifyMsg // regCustomers is the map of all registered customers
	}
)

// newNotifier returns a Notifier object
func newNotifier() Notifier {
	return &notifierImpl{
		regCustomers: make(map[int]chan NotifyMsg),
	}
}

// Register this id with the notifier, if it is already registered, this is
// a no-op
func (n *notifierImpl) Register(id int, notifyCh chan NotifyMsg) {
	n.lk.Lock()
	if _, ok := n.regCustomers[id]; !ok {
		n.regCustomers[id] = notifyCh
	}
	n.lk.Unlock()
}

// Unregister removes this id from the registered customers map
func (n *notifierImpl) Unregister(id int) {
	n.lk.Lock()
	delete(n.regCustomers, id)
	n.lk.Unlock()
}

// Notify finds the given id and sends the appropriate notification
func (n *notifierImpl) Notify(id int, notifyMsg NotifyMsg) {
	n.lk.RLock()
	ch, ok := n.regCustomers[id]
	n.lk.RUnlock()

	// if we found a registered customers, send the notification in a
	// non-blocking manner. We can do this in a non-blocking way because
	// it is ok to drop a notification.
	if ok {
		select {
		case ch <- notifyMsg:
		default:
		}
	}
}
