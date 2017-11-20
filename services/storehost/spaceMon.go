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
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-server/services/storehost/load"
)

const (
	warnThreshold         = 200 * gigaBytes
	alertThreshold        = 100 * gigaBytes
	resumeWritesThreshold = 200 * gigaBytes
)

// interval at which to monitor space
const spaceMonInterval = 2 * time.Minute

const (
	kiloBytes = 1024
	megaBytes = 1024 * kiloBytes
	gigaBytes = 1024 * megaBytes
	teraBytes = 1024 * gigaBytes
)

// StorageMode defines the read write mode of the storage host
type StorageMode int32

const (
	// StorageModeReadWrite read/write
	StorageModeReadWrite StorageMode = iota
	// StorageModeReadOnly read only
	StorageModeReadOnly
)

type (
	// SpaceMon keep monitoring disk usage, and log/alert/trigger necessary handling
	SpaceMon interface {
		common.Daemon
		GetMode() StorageMode
	}

	// SpaceMon is an implementation of SpaceMon.
	spaceMon struct {
		sync.RWMutex

		storeHost   *StoreHost // TODO: use this to trigger turning into read only mode
		logger      bark.Logger
		m3Client    metrics.Client
		hostMetrics *load.HostMetrics

		stopCh chan struct{}
		path   string
		mode   StorageMode
	}
)

// NewSpaceMon returns an instance of SpaceMon.
func NewSpaceMon(store *StoreHost, m3Client metrics.Client, hostMetrics *load.HostMetrics, logger bark.Logger, path string) SpaceMon {

	return &spaceMon{
		storeHost:   store,
		logger:      logger,
		m3Client:    m3Client,
		hostMetrics: hostMetrics,
		path:        path,
		mode:        StorageModeReadWrite,
	}
}

// Start starts the monitoring
func (s *spaceMon) Start() {

	s.logger.Info("SpaceMon: started")
	s.stopCh = make(chan struct{})
	go s.pump()
}

// Stop stops the monitoring
func (s *spaceMon) Stop() {

	close(s.stopCh)
	s.logger.Info("SpaceMon: stopped")
}

// GetMode returns the read/write mode of storage
func (s *spaceMon) GetMode() StorageMode {

	s.RLock()
	defer s.RUnlock()

	return s.mode
}

func (s *spaceMon) pump() {

	var stat syscall.Statfs_t

	path := s.path

	if len(path) <= 0 {

		s.logger.Warn("SpaceMon: monitoring path is empty, trying working directory")

		cwd, err := os.Getwd()
		if err != nil {
			s.logger.Error("SpaceMon: os.Getwd() failed", err)
			return
		}

		path = cwd
	}

	log := s.logger.WithField(`path`, path)

	ticker := time.NewTicker(spaceMonInterval)
	defer ticker.Stop()

	for range ticker.C {

		select {
		case <-s.stopCh:
			return
		default:
			// continue below
		}

		// query available/total space
		err := syscall.Statfs(path, &stat)
		if err != nil {
			log.WithField(common.TagErr, err).Error("SpaceMon: syscall.Statfs failed", path)
			continue
		}

		avail := stat.Bavail * uint64(stat.Bsize)
		total := stat.Blocks * uint64(stat.Bsize)

		if total <= 0 {
			log.Error(`SpaceMon: total space unavailable`)
			continue
		}

		availMiBs, totalMiBs := avail/megaBytes, total/megaBytes
		availPercent := 100.0 * float64(avail) / float64(total)

		s.hostMetrics.Set(load.HostMetricFreeDiskSpaceBytes, int64(avail))
		s.m3Client.UpdateGauge(metrics.SystemResourceScope, metrics.StorageDiskAvailableSpaceMB, int64(availMiBs))

		xlog := log.WithFields(bark.Fields{
			`avail`:   availMiBs,
			`total`:   totalMiBs,
			`percent`: availPercent,
		})

		s.Lock()
		defer s.Unlock()

		switch {
		case s.mode == StorageModeReadOnly:

			// disable read-only, if above resume-writes threshold
			if avail > resumeWritesThreshold {

				s.storeHost.EnableWrite()
				s.mode = StorageModeReadWrite

				xlog.Info("SpaceMon: disabling read-only")

			} else {

				xlog.Warn(`SpaceMon: continuing in read-only mode`)
			}

		case avail < alertThreshold: // enable read-only, if below alert-threshold

			xlog.Error("SpaceMon: available space less than alert-threshold")

			if s.mode != StorageModeReadOnly {
				s.mode = StorageModeReadOnly
				s.storeHost.DisableWrite()
			}

		case avail < warnThreshold: // warn, if below warn-threshold
			xlog.Warn("SpaceMon: available space less than warn-threshold")

		default:
			xlog.Debug("SpaceMon: monitoring")
		}
	}
}
