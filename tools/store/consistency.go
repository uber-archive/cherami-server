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

package store

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/codegangsta/cli"
	log "github.com/sirupsen/logrus"
	"github.com/tecbot/gorocksdb"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/storage/manyrocks"
	toolscommon "github.com/uber/cherami-server/tools/common"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

const (
	storeToolService = "store-tool"
	defaultPageSize  = 1000
)

// ConsistencyResult defines the result of consistency check
type ConsistencyResult int32

const (
	// ConsistencyResultGOOD means consistency check result is good
	ConsistencyResultGOOD = iota
	// ConsistencyResultWarning means consistency check result is not good
	ConsistencyResultWarning
	// ConsistencyResultError means consistency check result is very bad
	ConsistencyResultError
)

func consistencyResult2Str(result ConsistencyResult) string {
	switch result {
	case ConsistencyResultWarning:
		return "Warning"
	case ConsistencyResultError:
		return "Error"
	}

	return "Good"
}

// CleanupAction defines the clean up action of consistency check
type CleanupAction int32

const (
	// ActionNA means no need of any action
	ActionNA = iota
	// ActionNeedsAttention means needs some manual work to fix things
	ActionNeedsAttention
	// ActionRemoveFolder recommends to remove the folder
	ActionRemoveFolder
	// ActionRemoveMetadata recommends to remove the metadata record
	ActionRemoveMetadata
	// ActionRereplicate means there is a need to re-replicate the data
	ActionRereplicate
)

func cleanupAction2Str(action CleanupAction) string {
	switch action {
	case ActionNeedsAttention:
		return "NeedsAttention"
	case ActionRemoveFolder:
		return "RemoveFolder"
	case ActionRemoveMetadata:
		return "RemoveMetadata"
	}

	return "NA"
}

// StorageStatus defines the storage status
type StorageStatus int32

const (
	// StorageStatusNoData means there is nothing left, w/wo folder
	StorageStatusNoData = iota
	// StorageStatusLogsOnly means there is only logs left for the extent
	StorageStatusLogsOnly
	// StorageStatusCorrupted means there is data but it is corrupted and cannot be opened
	StorageStatusCorrupted
	// StorageStatusNormal means everything good
	StorageStatusNormal
)

func storageStatus2Str(status StorageStatus) string {
	switch status {
	case StorageStatusNoData:
		return "Empty"
	case StorageStatusCorrupted:
		return "Corrupted"
	case StorageStatusLogsOnly:
		return "LogsOnly"
	}

	return "Normal"
}

type extentConsistencyInfo struct {
	extentUUID        string
	metadataStatus    string
	storageStatus     StorageStatus
	consistencyResult ConsistencyResult
	consistencyAction CleanupAction
}

func getLocalIPv4Address(interfaceName string) (net.IP, error) {
	list, err := net.Interfaces()
	if err != nil {
		fmt.Fprintf(os.Stdout, "net.Interfaces() returns err: %v\n", err)
		return nil, err
	}

	for _, iface := range list {
		if iface.Name == interfaceName {
			addrs, err1 := iface.Addrs()
			if err1 != nil {
				fmt.Fprintf(os.Stdout, "net interface Addrs() returns err: %v\n", err1)
				return nil, err1
			}

			for _, addr := range addrs {
				var ip net.IP
				switch v := addr.(type) {
				case *net.IPNet:
					ip = v.IP
				case *net.IPAddr:
					ip = v.IP
				}
				if ip == nil || ip.IsLoopback() {
					continue
				}
				ip = ip.To4()
				if ip == nil {
					continue // not an ipv4 address
				}
				return ip, nil
			}
		}
	}

	return nil, fmt.Errorf("no address Found for %s", interfaceName)
}

func getStorageStatus(extentid string, mgr *manyrocks.ManyRocks, fullPath string, f os.FileInfo) StorageStatus {
	list, err := ioutil.ReadDir(fullPath)
	if err != nil || len(list) == 0 {
		return StorageStatusNoData
	}

	// Are there just LOG file? This is the scenarios that we try to access non-existent extents;
	countLock := 0
	countSST := 0
	countWAL := 0
	countLOG := 0
	for i := len(list) - 1; i >= 0; i-- {
		if list[i].IsDir() {
			continue
		}
		name := list[i].Name()
		if strings.HasPrefix(name, "LOG.") || strings.Compare(name, "LOG") == 0 {
			countLOG++
		} else if strings.HasSuffix(name, ".log") {
			countWAL++
		} else if strings.HasSuffix(name, ".sst") {
			countSST++
		} else if strings.Compare(name, "LOCK") == 0 {
			countLock++
		}
	}

	if countLock == 0 && countSST == 0 && countWAL == 0 && countLOG > 0 {
		return StorageStatusLogsOnly
	}

	// Try open the extent
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)
	defer opts.Destroy()

	db, err := gorocksdb.OpenDbForReadOnly(opts, fullPath, false)
	defer func() {
		if db != nil {
			db.Close()
		}
	}()

	if db == nil || err != nil {
		return StorageStatusCorrupted
	}

	return StorageStatusNormal
}

func convertMetadataStatus2Str(status shared.ExtentStatus) string {
	switch status {
	case shared.ExtentStatus_OPEN:
		return "open"
	case shared.ExtentStatus_DELETED:
		return "deleted"
	case shared.ExtentStatus_SEALED:
		return "sealed"
	case shared.ExtentStatus_CONSUMED:
		return "consumed"
	case shared.ExtentStatus_ARCHIVED:
		return "archived"
	}

	return "Unknown"
}

// CheckStoreConsistency checks the consistency inforamtion of the store
func CheckStoreConsistency(c *cli.Context) {
	if len(c.Args()) < 1 {
		toolscommon.ExitIfError(errors.New("not enough arguments"))
	}

	// Need to make sure this is the same host since we only allow consistency check per host level

	// check the root folder
	rootdir := string(c.String("rootdir"))

	if _, err := os.Stat(rootdir); err != nil {
		fmt.Fprintf(os.Stdout, "rootdir does not exist: %s\n", rootdir)
		toolscommon.ExitIfError(err)
	}

	if infoRoot, err := os.Lstat(rootdir); err != nil || !infoRoot.IsDir() {
		fmt.Fprintf(os.Stdout, "This is not a folder: %s\n", rootdir)
		toolscommon.ExitIfError(err)
	}

	list, err := ioutil.ReadDir(rootdir)
	if err != nil {
		fmt.Fprintf(os.Stdout, "ReadDir failed: %s\n", rootdir)
		toolscommon.ExitIfError(err)
	}

	storeHost := string(c.Args().Get(1))
	var storeIP net.IP
	if len(storeHost) == 0 {
		storeIP, err = getLocalIPv4Address("eth0")
		if err != nil {
			fmt.Fprintf(os.Stdout, "Failed to get host ipv4 address at eth0: %v\n", err)
			toolscommon.ExitIfError(err)
		}
		storeHost = storeIP.String()
	}

	storeHostAddr := fmt.Sprintf("%s:%s", storeHost, common.ServiceToPort[common.StoreServiceName])

	mClient := toolscommon.GetMClient(c, storeToolService)
	storeHostUUID, err := mClient.HostAddrToUUID(storeHostAddr)
	toolscommon.ExitIfError(err)

	extRes, err1 := mClient.ListStoreExtentsStats(&metadata.ListStoreExtentsStatsRequest{
		StoreUUID: common.StringPtr(storeHostUUID),
	})
	toolscommon.ExitIfError(err1)

	extentMetadataCache := make(map[string]*shared.ExtentStats)
	for _, stats := range extRes.GetExtentStatsList() {
		extent := stats.GetExtent()
		extentMetadataCache[extent.GetExtentUUID()] = stats

		// TODO: check if extent status is consistent with the replica status
	}

	mgr, err2 := manyrocks.New(&manyrocks.Opts{BaseDir: rootdir}, bark.NewLoggerFromLogrus(log.StandardLogger()))
	if err2 != nil {
		fmt.Fprintf(os.Stdout, "Cannot create a rocksdb manyrocks manager")
		toolscommon.ExitIfError(err2)
	}

	// Store existing extents map
	storeExtentsInfoMap := make(map[string]*extentConsistencyInfo)

	// Do a walk through all direct-sub folders
	for i := len(list) - 1; i >= 0; i-- {
		fullPath := filepath.Join(rootdir, list[i].Name())
		fInfo := list[i]

		if !fInfo.IsDir() {
			// Right now we will only check folders, warn about orphant files
			fmt.Fprintf(
				os.Stdout,
				"Warning, id:%s, not a folder, action:%s\n",
				fInfo.Name(),
				cleanupAction2Str(ActionNA))
			continue
		}

		extentid := fInfo.Name()

		storageStatus := getStorageStatus(extentid, mgr, fullPath, fInfo)

		consistencyInfo := &extentConsistencyInfo{
			extentUUID:        extentid,
			storageStatus:     storageStatus,
			consistencyResult: ConsistencyResultGOOD,
			consistencyAction: ActionNA,
		}

		extentStats, ok := extentMetadataCache[extentid]
		if !ok {
			// There is no metadata entry, but we do have orphant folder
			consistencyInfo.metadataStatus = "nonexist"
			consistencyInfo.consistencyResult = ConsistencyResultError
			consistencyInfo.consistencyAction = ActionRemoveFolder
		} else {
			switch extentStats.GetStatus() {
			case shared.ExtentStatus_DELETED:
				// In this case, the local folder/data should have been deleted
				consistencyInfo.metadataStatus = "deleted"
				consistencyInfo.consistencyResult = ConsistencyResultError
				consistencyInfo.consistencyAction = ActionRemoveFolder
			case shared.ExtentStatus_OPEN:
				consistencyInfo.metadataStatus = "open"
				if storageStatus == StorageStatusNoData || storageStatus == StorageStatusLogsOnly {
					// Could be the race condition and need to look into manually
					consistencyInfo.consistencyResult = ConsistencyResultWarning
					consistencyInfo.consistencyAction = ActionNeedsAttention
				} else if storageStatus == StorageStatusCorrupted {
					consistencyInfo.consistencyResult = ConsistencyResultError
					consistencyInfo.consistencyAction = ActionRereplicate
				} else {
					// Good default case
				}
			case shared.ExtentStatus_SEALED:
				fallthrough
			case shared.ExtentStatus_CONSUMED:
				fallthrough
			case shared.ExtentStatus_ARCHIVED:
				consistencyInfo.metadataStatus = convertMetadataStatus2Str(extentStats.GetStatus())

				if storageStatus != StorageStatusNormal {
					consistencyInfo.consistencyResult = ConsistencyResultError
					consistencyInfo.consistencyAction = ActionRereplicate
				}
			}
		}

		storeExtentsInfoMap[extentid] = consistencyInfo

		fmt.Fprintf(
			os.Stdout,
			"%s, id:%s, size:%d, createat:%v storage:%s metadata:%s, action:%s\n",
			consistencyResult2Str(consistencyInfo.consistencyResult),
			extentid,
			fInfo.Size(),
			fInfo.ModTime(),
			storageStatus2Str(consistencyInfo.storageStatus),
			consistencyInfo.metadataStatus,
			cleanupAction2Str(consistencyInfo.consistencyAction))
	}

	// Is there any extent that exist in metadata but not in local storage
	for extentID, extentStats := range extentMetadataCache {
		if _, ok := storeExtentsInfoMap[extentID]; ok {
			continue
		}

		if extentStats.GetStatus() == shared.ExtentStatus_DELETED {
			// This is the expected good case
			continue
		}

		var result ConsistencyResult
		var action CleanupAction
		if extentStats.GetStatus() == shared.ExtentStatus_OPEN {
			// metadata open status, while there is no folder yet, this is most likely
			// race condition case where the storage has not been allocated yet
			result = ConsistencyResultWarning
			action = ActionNeedsAttention
		} else {
			result = ConsistencyResultError
			action = ActionRemoveMetadata
		}

		fmt.Fprintf(
			os.Stdout,
			"%s, id:%s, storage:%s, metadata:%s, action:%s\n",
			consistencyResult2Str(result),
			extentID,
			storageStatus2Str(StorageStatusNoData),
			extentStats.GetStatus(),
			cleanupAction2Str(action))
	}
}
