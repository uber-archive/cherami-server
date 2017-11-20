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

// Monitoring housekeeping will happen every 2 minutes
const storageMonitoringInterval = time.Duration(2 * time.Minute)

const (
	thresholdWarn         = 75 * gigaBytes
	thresholdReadOnly     = 50 * gigaBytes
	thresholdResumeWrites = 100 * gigaBytes
)

const (
	kiloBytes = 1024
	megaBytes = 1024 * kiloBytes
	gigaBytes = 1024 * megaBytes
	teraBytes = 1024 * gigaBytes
)

// StorageStatus defines the different storage status
type StorageStatus int32

// StorageMode defines the read write mode of the storage host
type StorageMode int32

const (
	// SMReadWrite allows both read and write
	SMReadWrite = iota
	// SMReadOnly allows read only
	SMReadOnly
)

type (
	// StorageMonitor keep monitoring disk usage, and log/alert/trigger necessary handling
	StorageMonitor interface {
		common.Daemon
		GetStorageMode() StorageMode
	}

	// StorageMonitor is an implementation of StorageMonitor.
	storageMonitor struct {
		sync.RWMutex

		storeHost *StoreHost // TODO: use this to trigger turning into read only mode

		logger      bark.Logger
		m3Client    metrics.Client
		hostMetrics *load.HostMetrics

		closeChannel chan struct{}

		monitoringTicker *time.Ticker

		monitoringPath string
		mode           StorageMode
	}
)

// NewStorageMonitor returns an instance of NewStorageMonitor.
func NewStorageMonitor(store *StoreHost, m3Client metrics.Client, hostMetrics *load.HostMetrics, logger bark.Logger, path string) StorageMonitor {
	return &storageMonitor{
		storeHost:      store,
		logger:         logger,
		m3Client:       m3Client,
		hostMetrics:    hostMetrics,
		monitoringPath: path,
		mode:           SMReadWrite,
	}
}

// Start starts the monitoring
func (s *storageMonitor) Start() {

	s.closeChannel = make(chan struct{})

	go func() {

		ticker := time.NewTicker(storageMonitoringInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				go s.checkStorage()
			case <-s.closeChannel:
				return
			}
		}
	}()

	s.logger.Info("StorageMonitor: started")
}

// Stop stops the monitoring
func (s *storageMonitor) Stop() {
	close(s.closeChannel)

	s.logger.Info("StorageMonitor: stopped")
}

// GetStorageMode returns the read/write mode of storage
func (s *storageMonitor) GetStorageMode() StorageMode {
	s.RLock()
	defer s.RUnlock()

	return s.mode
}

func (s *storageMonitor) checkStorage() {
	var stat syscall.Statfs_t

	path := s.monitoringPath

	if len(path) <= 0 {
		s.logger.Warn("StorageMonitor: monitoring path is empty, try working directory")
		wd, err := os.Getwd()
		if err != nil {
			s.logger.Error("StorageMonitor: os.Getwd() failed", err)
			return
		}
		path = wd
	}

	log := s.logger.WithField(`path`, path)

	err := syscall.Statfs(path, &stat)
	if err != nil {
		log.WithField(common.TagErr, err).Error("StorageMonitor: syscall.Statfs failed")
		return
	}

	avail := int64(stat.Bavail) * int64(stat.Bsize)
	total := int64(stat.Blocks) * int64(stat.Bsize)

	xlog := log.WithFields(bark.Fields{
		`availMiB`: avail / megaBytes,
		`totalMiB`: total / megaBytes,
	})

	if total <= 0 {
		xlog.Error(`StorageMonitor: total space unavailable`)
		return
	}

	s.hostMetrics.Set(load.HostMetricFreeDiskSpaceBytes, avail)
	s.m3Client.UpdateGauge(metrics.SystemResourceScope, metrics.StorageDiskAvailableSpaceMB, avail/megaBytes)

	s.Lock()
	defer s.Unlock()

	switch {
	case s.mode == SMReadOnly:

		// disable read-only, if above resume-writes threshold
		if avail > thresholdResumeWrites {
			xlog.Info("StorageMonitor: disabling read-only")
			s.storeHost.EnableWrite()
			s.mode = SMReadWrite
		} else {
			xlog.Warn("StorageMonitor: continuing read-only")
		}

	case avail < thresholdReadOnly: // enable read-only, if below readonly-threshold
		xlog.Error("StorageMonitor: available space less than readonly-threshold")

		if s.mode != SMReadOnly {
			s.mode = SMReadOnly
			s.storeHost.DisableWrite()
		}

	case avail < thresholdWarn: // warn, if below warn-threshold
		xlog.Warn("StorageMonitor: available space less than warn-threshold")

	default:
		xlog.Debug("StorageMonitor: monitoring")
	}
}
