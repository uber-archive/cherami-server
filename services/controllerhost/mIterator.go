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
	"fmt"

	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"

	"github.com/uber-common/bark"
)

// Metadata iterator - mIterator

type mIteratorEventType uint8

const (
	eIterStart mIteratorEventType = 42 + iota
	eIterEnd
	eDestStart
	eDestEnd
	eExtentIterStart
	eExtentIterEnd
	eExtent
	eCnsmIterStart
	eCnsmIterEnd
	eCnsmStart
	eCnsmEnd
	eCnsmExtentIterStart
	eCnsmExtentIterEnd
	eCnsmExtent
)

type level uint8
type movement uint8

const (
	up movement = 242 + iota
	down
	stay
)

type levelCheck struct {
	level
	movement
}

type mIterator struct {
	level
	*Context
	lclLg         bark.Logger
	current       mIteratorEvent
	pubCh         chan *mIteratorEvent
	eventHandlers []mIteratorHandler
}

type mIteratorEvent struct {
	t          mIteratorEventType
	dest       *shared.DestinationDescription
	extent     *shared.ExtentStats
	cnsm       *shared.ConsumerGroupDescription
	cnsmExtent *metadata.ConsumerGroupExtent
}

type mIteratorHandler interface {
	handleEvent(*mIteratorEvent)
	initialize(*Context)
}

const maxLevel = 5

// This map is used to check that the events are being emitted at the proper level of the iteration. up increases the level, and down reduces it
var eventLevelCheckMap = map[mIteratorEventType]levelCheck{
	eIterStart:           {level: 0, movement: up},
	eIterEnd:             {level: 0, movement: down},
	eDestStart:           {level: 1, movement: up},
	eDestEnd:             {level: 1, movement: down},
	eExtentIterStart:     {level: 2, movement: up},
	eExtentIterEnd:       {level: 2, movement: down},
	eExtent:              {level: 3, movement: stay},
	eCnsmIterStart:       {level: 2, movement: up},
	eCnsmIterEnd:         {level: 2, movement: down},
	eCnsmStart:           {level: 3, movement: up},
	eCnsmEnd:             {level: 3, movement: down},
	eCnsmExtentIterStart: {level: 4, movement: up},
	eCnsmExtentIterEnd:   {level: 4, movement: down},
	eCnsmExtent:          {level: 5, movement: stay},
}

type eventHandlerName string

func (e mIteratorEventType) String() string {
	switch e {
	case eIterStart:
		return `eIterStart`
	case eIterEnd:
		return `eIterEnd`
	case eDestStart:
		return `eDestStart`
	case eDestEnd:
		return `eDestEnd`
	case eExtentIterStart:
		return `eExtentIterStart`
	case eExtentIterEnd:
		return `eExtentIterEnd`
	case eExtent:
		return `eExtent`
	case eCnsmIterStart:
		return `eCnsmIterStart`
	case eCnsmIterEnd:
		return `eCnsmIterEnd`
	case eCnsmStart:
		return `eCnsmStart`
	case eCnsmEnd:
		return `eCnsmEnd`
	case eCnsmExtentIterStart:
		return `eCnsmExtentIterStart`
	case eCnsmExtentIterEnd:
		return `eCnsmExtentIterEnd`
	case eCnsmExtent:
		return `eCnsmExtent`
	case 0:
		return `nil`
	default:
		return `INVALID` + fmt.Sprintf(`%d`, e)
	}
}

func (i *mIterator) newMIteratorEvent(t mIteratorEventType) *mIteratorEvent {
	mIteratorEventVal := i.current
	mIteratorEventVal.t = t
	return &mIteratorEventVal
}

func newMIterator(c *Context) *mIterator {
	mi := &mIterator{
		level:   0,
		lclLg:   c.log,
		Context: c,
		pubCh:   make(chan *mIteratorEvent, 1),
		eventHandlers: []mIteratorHandler{
			&dlqMonitor{},
		},
	}

	// Initialize all event handlers with the context
	for _, h := range mi.eventHandlers {
		h.initialize(c)
	}

	go mi.handleEvents()

	return mi
}

func (i *mIterator) publishEvent(t mIteratorEventType, obj interface{}) {
	// Evalulate the WithFields only if we actually log
	lg := func() bark.Logger { return i.lclLg.WithFields(bark.Fields{`mIteratorEventType`: t, `object`: obj}) }

	// TODO: instead of panicing, on production, we should immediately end the iteration by sending some end
	// events to unravel the stack, and then ignore new publishes until we get an iter start event

	lc, lcOK := eventLevelCheckMap[t]
	if !lcOK {
		lg().Panic(`Bad event type`)
	}

	// Check level movements and panic if something wrong happens
	if lc.movement == down {
		if i.level == 0 {
			lg().Panic(`can't descend past level zero`)
		}
		i.level--
	}
	if lc.level != i.level {
		lg().WithField(`iterLevel`, i.level).Panic(`wrong level`)
	}
	if lc.movement == up {
		if i.level >= maxLevel {
			lg().Panic(`can't ascend past max level`)
		}
		i.level++
	}

	// This adds parts of the current state as needed
	switch t {
	case eDestStart:
		i.current.dest = obj.(*shared.DestinationDescription)
	case eExtent:
		i.current.extent = obj.(*shared.ExtentStats)
	case eCnsmStart:
		i.current.cnsm = obj.(*shared.ConsumerGroupDescription)
	case eCnsmExtent:
		i.current.cnsmExtent = obj.(*metadata.ConsumerGroupExtent)
	case eIterStart:
	case eIterEnd:
	case eDestEnd:
	case eExtentIterStart:
	case eExtentIterEnd:
	case eCnsmEnd:
	case eCnsmIterEnd:
	case eCnsmIterStart:
	case eCnsmExtentIterEnd:
	case eCnsmExtentIterStart:
	default:
		lg().Panic(`unimplemented`)
	}

	// Publish a copy of the current event
	i.pubCh <- i.newMIteratorEvent(t)

	// This removes parts of the current state as needed
	switch t {
	case eIterEnd:
		i.clear() // Ideally, this does nothing, since we clear each element at the end iterations below
	case eDestEnd:
		i.current.dest = nil
	case eExtentIterEnd:
		i.current.extent = nil
	case eCnsmEnd:
		i.current.cnsm = nil
	case eCnsmExtentIterEnd:
		i.current.cnsmExtent = nil
	case eCnsmExtent:
	case eIterStart:
	case eDestStart:
	case eExtentIterStart:
	case eExtent:
	case eCnsmStart:
	case eCnsmExtentIterStart:
	case eCnsmIterEnd:
	case eCnsmIterStart:
	default:
		lg().Panic(`unimplemented`)
	}
}

func (i *mIterator) handleEvents() {
	for e := range i.pubCh {
		for _, h := range i.eventHandlers {
			h.handleEvent(e)
		}
	}
}

func (i *mIterator) clear() {
	i.current = mIteratorEvent{}
}
