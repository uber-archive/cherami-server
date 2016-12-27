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

package common

import (
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
)

// TODO: Use less aggressive settings for checkin
var startedEKG = false
var maxAge = time.Minute * 15      // How recently a heart must have beat to be counted as 'beating'
var initialWait = time.Minute * 25 // How long we wait for the first heartbeat
var cleanupAge = maxAge * 3        // How long we wait before cleaning up a defunct heartbeat

const checkInterval = time.Minute * 2 // How often we check all of the hearts and do the too many open files check
const randomVariationPercent = 50     // ± 50% will be added to the max age and initialWait

// Heartbeat is just a timestamp and an identifier
type Heartbeat struct {
	timestamp UnixNanoTime
	id        *string
}

var hearts struct {
	m         map[*string]*Heartbeat
	startTime UnixNanoTime
	suicided  bool
	sync.Mutex
	lclLg bark.Logger
}

// NewHeartbeat creates a new Heartbeat object
func NewHeartbeat(id *string) *Heartbeat {
	hearts.Lock()
	if hearts.m == nil {
		hearts.m = make(map[*string]*Heartbeat)
	}
	h, ok := hearts.m[id]
	if !ok || h == nil {
		h = &Heartbeat{
			timestamp: Now(),
			id:        id,
		}
		hearts.m[id] = h
	}

	hearts.Unlock()
	return h
}

// Beat updates the timestamp on the Heartbeat
func (h *Heartbeat) Beat() {
	t := Now()
	atomic.StoreInt64((*int64)(&h.timestamp), int64(t))
}

// CloseHeartbeat removes the given heart from the Heartbeat system
func (h *Heartbeat) CloseHeartbeat() {
	hearts.Lock()
	delete(hearts.m, h.id)
	hearts.Unlock()
}

func adjustRandom(v *time.Duration) {
	var orig = *v
	*v -= (*v * time.Duration(randomVariationPercent)) / 100
	r := time.Duration(rand.Intn(randomVariationPercent * 2))
	*v += (r * orig) / 100
}

// StartEKG starts a goroutine to check the heartbeats
func StartEKG(lclLg bark.Logger) {
	hearts.Lock()
	hearts.lclLg = lclLg
	hearts.startTime = Now()
	if !startedEKG {
		adjustRandom(&initialWait)
		adjustRandom(&maxAge)
		startedEKG = true
		go ekg()
	}
	hearts.Unlock()
}

func ekg() {
	for {
		time.Sleep(checkInterval)
		checkEKG()
	}
}

func checkEKG() {
	hearts.Lock()
	beating := false
	oldestAllowedBeatAsTime := time.Now().Add(-maxAge)
	oldestAllowedBeat := UnixNanoTime(oldestAllowedBeatAsTime.UnixNano())
	cleanupTime := UnixNanoTime(time.Now().Add(-cleanupAge).UnixNano())

	// Check if *any* beat is recent enough. If so, we are still alive
	for _, h := range hearts.m {
		ts := UnixNanoTime(atomic.LoadInt64((*int64)(&h.timestamp)))
		if ts > oldestAllowedBeat {
			beating = true
		} else if ts < cleanupTime {
			hearts.lclLg.
				WithField(`heartbeat`, *h.id).
				Info(`Cleaning up Heartbeat`)
			delete(hearts.m, h.id)
		}
	}

	if !beating {
		now := Now()

		// Only panic if we have hearts or we have exceeded the initial wait time
		if time.Duration(now-hearts.startTime) < initialWait && len(hearts.m) == 0 {
			// XXX: fix the log race
			//hearts.lclLg.Info(`No hearts registered. Initialization time not exceeded, yet.`)
		} else {
			hearts.lclLg.
				WithField(`maxAllowedAge`, DurationToSeconds(maxAge)).
				Error(`No hearts beating. Dumping hearts.`)

			for _, h := range hearts.m {
				ts := UnixNanoTime(atomic.LoadInt64((*int64)(&h.timestamp)))
				hearts.lclLg.
					WithField(`heartbeat`, *h.id).
					WithField(`LastHearbeatSecondsAgo`, (now - ts).ToSeconds()).
					Error(`Heart`)
			}

			hearts.suicided = true
			hearts.lclLg.
				WithField(`hearts`, len(hearts.m)).
				WithField(`since`, oldestAllowedBeatAsTime.Format(time.RFC3339)).
				Error(`KILLING SELF: No hearts have beat`)
		}
	} else {
		if hearts.suicided {
			hearts.lclLg.Error(`Just kidding about that "KILLING SELF" thing...`)
			hearts.suicided = false
		}
	}

	checkOpenFiles()

	hearts.Unlock()
}

func checkOpenFiles() {
	file, err := os.Open(`/bin/ls`)
	if err != nil {
		if strings.Contains(err.Error(), `too many open files`) {
			hearts.lclLg.
				WithField(TagErr, err).
				Panic(`KILLING SELF: couldn't open a file`)
		} else {
			hearts.lclLg.
				WithField(TagErr, err).
				Error(`Heartbeat checker: couldn't open a file`)
		}
	} else {
		file.Close()
	}
}
