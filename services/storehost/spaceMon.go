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
	"syscall"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/storehost/load"
	"go.uber.org/atomic"
)

var (
	// TODO: make these dynamically configurable!

	// SpaceMonThresholdWarn if free-space below this, emit warning logs
	SpaceMonThresholdWarn uint64 = 250 * GiB

	// SpaceMonThresholdReadOnly if free-space below this, switch store to read-only
	SpaceMonThresholdReadOnly uint64 = 200 * GiB

	// SpaceMonThresholdResumeWrites if read-only and free-space exceeds this, then make read-write
	SpaceMonThresholdResumeWrites uint64 = 275 * GiB

	// SpaceMonInterval interval at which to monitor free-space and take action
	SpaceMonInterval = 2 * time.Minute
)

const (
	// KiB kibibytes
	KiB = 1024
	// MiB mebibytes
	MiB = 1024 * KiB
	// GiB gibibytes
	GiB = 1024 * MiB
	// TiB tebibytes
	TiB = 1024 * GiB
)

type (
	// SpaceMon monitors free-space and switches stores to read-only on low-space
	SpaceMon struct {
		storeHost   *StoreHost
		logger      bark.Logger
		m3Client    metrics.Client
		hostMetrics *load.HostMetrics

		stopCh   chan struct{}
		path     string
		readonly *atomic.Bool // read-only
	}
)

// NewSpaceMon returns an instance of SpaceMon.
func NewSpaceMon(store *StoreHost, m3Client metrics.Client, hostMetrics *load.HostMetrics, logger bark.Logger, path string) *SpaceMon {

	return &SpaceMon{
		storeHost:   store,
		logger:      logger.WithField(common.TagModule, `spaceMon`),
		m3Client:    m3Client,
		hostMetrics: hostMetrics,
		path:        path,
		readonly:    atomic.NewBool(false), // default: read-write
	}
}

// Start starts the monitoring
func (s *SpaceMon) Start() {

	s.stopCh = make(chan struct{})
	go s.pump()
	s.storeHost.DisableReadonly()

	s.logger.Info("SpaceMon: started")
}

// Stop stops the monitoring
func (s *SpaceMon) Stop() {

	close(s.stopCh)
	s.logger.Info("SpaceMon: stopped")
}

func (s *SpaceMon) pump() {

	log := s.logger.WithField(`path`, s.path)

	ticker := time.NewTicker(SpaceMonInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return // done
		case <-ticker.C:
			// continue below to check free-space, etc
		}

		// query available/total space
		var stat syscall.Statfs_t
		err := syscall.Statfs(s.path, &stat)
		if err != nil {
			log.WithField(common.TagErr, err).Error("SpaceMon: syscall.Statfs failed")
			continue
		}

		avail := stat.Bavail * uint64(stat.Bsize)
		total := stat.Blocks * uint64(stat.Bsize)

		if total <= 0 {
			log.Error(`SpaceMon: total space unavailable`)
			continue
		}

		availMiBs, totalMiBs := avail/MiB, total/MiB
		availPercent := 100.0 * float64(avail) / float64(total)

		s.hostMetrics.Set(load.HostMetricFreeDiskSpaceBytes, int64(avail))
		s.m3Client.UpdateGauge(metrics.SystemResourceScope, metrics.StorageDiskAvailableSpaceMB, int64(availMiBs))

		xlog := log.WithFields(bark.Fields{
			`avail`:   availMiBs,
			`total`:   totalMiBs,
			`percent`: availPercent,
		})

		switch {
		case s.readonly.Load(): // disable readonly, if above resume-writes threshold

			// if below resume-writes threshold, do nothing
			if avail < SpaceMonThresholdResumeWrites {
				xlog.Warn(`SpaceMon: continuing in read-only`)
				continue
			}

			// disable readonly, if we have recovered free-space
			if s.readonly.CAS(true, false) {

				xlog.Info("SpaceMon: disabling read-only")
				s.storeHost.EnableReadonly()
			}

		case avail < SpaceMonThresholdReadOnly: // enable read-only, if below alert-threshold

			if s.readonly.CAS(false, true) {

				xlog.Error("SpaceMon: switching to read-only")
				s.storeHost.DisableReadonly()
			}

		case avail < SpaceMonThresholdWarn: // warn, if below warn-threshold
			xlog.Warn("SpaceMon: warning: running low on space")

		default:
			xlog.Debug("SpaceMon: monitoring")
		}
	}
}
