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

// Monitoring housekeeping will happen every 2 minutes
const storageMonitoringInterval = time.Duration(2 * time.Minute)

const bytesPerMB = 1024 * 1024

const warningThreshold = 0.1        // 10%
const alertThreshold = 0.05         // 5%
const resumeWritableThreshold = 0.2 // 20%

// NewStorageMonitor returns an instance of NewStorageMonitor.
func NewStorageMonitor(store *StoreHost, m3Client metrics.Client, hostMetrics *load.HostMetrics, logger bark.Logger, path string) StorageMonitor {
	return &storageMonitor{
		storeHost:        store,
		logger:           logger,
		m3Client:         m3Client,
		hostMetrics:      hostMetrics,
		monitoringTicker: time.NewTicker(storageMonitoringInterval),
		monitoringPath:   path,
		mode:             SMReadWrite,
	}
}

// Start starts the monitoring
func (s *storageMonitor) Start() {
	s.logger.Info("StorageMonitor: started")

	s.closeChannel = make(chan struct{})

	go s.doHouseKeeping()
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

	if len(path) == 0 {
		s.logger.Warn("StorageMonitor: monitoring path is empty, try working directory")
		wd, err := os.Getwd()
		if err != nil {
			s.logger.Error("StorageMonitor: os.Getwd() failed", err)
			return
		}
		path = wd
	}

	err := syscall.Statfs(path, &stat)
	if err != nil {
		s.logger.Error("StorageMonitor: syscall.Statfs() failed", path, err)
		return
	}

	// Available blocks * size per block = available space in bytes
	availableMBs := stat.Bavail * uint64(stat.Bsize) / bytesPerMB
	totalMBs := stat.Blocks * uint64(stat.Bsize) / bytesPerMB

	if totalMBs <= 0 {
		s.logger.WithField(`filePath`, path).Error(`Monitoring disk space error: total MBs is not readable`)
		return
	}

	availablePcnt := float32(availableMBs) / float32(totalMBs)

	s.hostMetrics.Set(load.HostMetricFreeDiskSpaceBytes, int64(availableMBs)*bytesPerMB)
	s.m3Client.UpdateGauge(metrics.SystemResourceScope, metrics.StorageDiskAvailableSpacePcnt, int64(availablePcnt*1000))
	s.m3Client.UpdateGauge(metrics.SystemResourceScope, metrics.StorageDiskAvailableSpaceMB, int64(availableMBs))

	s.Lock()
	defer s.Unlock()

	if s.mode == SMReadOnly {
		// Whether we can get out of read only mode
		if availablePcnt > resumeWritableThreshold {
			s.storeHost.EnableWrite()
			s.mode = SMReadWrite
			s.logger.WithFields(bark.Fields{`filePath`: path, `availableMBs`: availableMBs, `totalMBs`: totalMBs, `availablePcnt`: availablePcnt}).Info(`Resumed read/write mode.`)
		} else {
			s.logger.WithFields(bark.Fields{`filePath`: path, `availableMBs`: availableMBs, `totalMBs`: totalMBs, `availablePcnt`: availablePcnt}).Warn(`In read only mode.`)
		}

		return
	}

	if availablePcnt < alertThreshold {
		s.logger.WithFields(bark.Fields{`filePath`: path, `availableMBs`: availableMBs, `totalMBs`: totalMBs, `availablePcnt`: availablePcnt}).Errorf(`Available disk space lower than alert threshold`)
		if s.mode != SMReadOnly {
			s.mode = SMReadOnly
			s.storeHost.DisableWrite()
		}
	} else if availablePcnt < warningThreshold {
		s.logger.WithFields(bark.Fields{`filePath`: path, `availableMBs`: availableMBs, `totalMBs`: totalMBs, `availablePcnt`: availablePcnt}).Warn(`Available disk space lower than warning threshold`)
	} else {
		s.logger.WithFields(bark.Fields{`filePath`: path, `availableMBs`: availableMBs, `totalMBs`: totalMBs, `availablePcnt`: availablePcnt}).Info(`Monitoring disk space`)
	}
}

func (s *storageMonitor) doHouseKeeping() {
	for {
		select {
		case <-s.monitoringTicker.C:
			go s.checkStorage()
		case <-s.closeChannel:
			return
		}
	}
}
