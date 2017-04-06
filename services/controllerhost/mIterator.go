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
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"

	"github.com/uber-common/bark"

	"strconv"
)

// Metadata iterator - mIterator

type (
	mIteratorEventType uint8
	level              uint8
	movement           uint8

	levelCheck struct {
		level
		movement
	}

	mIterator struct {
		level
		*Context
		lclLg         bark.Logger
		current       mIteratorEvent
		pubCh         chan *mIteratorEvent
		eventHandlers []mIteratorHandler
	}

	mIteratorEvent struct {
		t          mIteratorEventType
		dest       *shared.DestinationDescription
		extent     *metadata.DestinationExtent
		cnsm       *shared.ConsumerGroupDescription
		cnsmExtent *shared.ConsumerGroupExtent
	}

	mIteratorHandler interface {
		handleEvent(*mIteratorEvent)
	}
)

const maxLevel = 5

const (
	up movement = 242 + iota
	down
	stay
)

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

var iteratorEventNames = map[mIteratorEventType]string{
	eIterStart:           "eIterStart",
	eIterEnd:             "eIterEnd",
	eDestStart:           "eDestStart",
	eDestEnd:             "eDestEnd",
	eExtentIterStart:     "eExtentIterStart",
	eExtentIterEnd:       "eExtentIterEnd",
	eExtent:              "eExtent",
	eCnsmIterStart:       "eCnsmIterStart",
	eCnsmIterEnd:         "eCnsmIterEnd",
	eCnsmStart:           "eCnsmStart",
	eCnsmEnd:             "eCnsmEnd",
	eCnsmExtentIterStart: "eCnsmExtentIterStart",
	eCnsmExtentIterEnd:   "eCnsmExtentIterEnd",
	eCnsmExtent:          "eCnsmExtent",
}

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
	if name, ok := iteratorEventNames[e]; ok {
		return name
	}
	return "invalid event " + strconv.Itoa(int(e))
}

func (i *mIterator) newMIteratorEvent(t mIteratorEventType) *mIteratorEvent {
	mIteratorEventVal := i.current
	mIteratorEventVal.t = t
	return &mIteratorEventVal
}

func newMIterator(c *Context, handlers ...mIteratorHandler) *mIterator {
	mi := &mIterator{
		level:         0,
		lclLg:         c.log,
		Context:       c,
		pubCh:         make(chan *mIteratorEvent, 1),
		eventHandlers: handlers,
	}

	go mi.handleEvents()

	return mi
}

func (i *mIterator) validateNextEvent(t mIteratorEventType, lg func() bark.Logger) {
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
}

func (i *mIterator) publishEvent(t mIteratorEventType, obj interface{}) {
	// Evalulate the WithFields only if we actually log
	lg := func() bark.Logger { return i.lclLg.WithFields(bark.Fields{`mIteratorEventType`: t, `object`: obj}) }

	i.validateNextEvent(t, lg)

	// This adds parts of the current state as needed
	switch t {
	case eDestStart:
		i.current.dest = obj.(*shared.DestinationDescription)
	case eExtent:
		i.current.extent = obj.(*metadata.DestinationExtent)
	case eCnsmStart:
		i.current.cnsm = obj.(*shared.ConsumerGroupDescription)
	case eCnsmExtent:
		i.current.cnsmExtent = obj.(*shared.ConsumerGroupExtent)
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
