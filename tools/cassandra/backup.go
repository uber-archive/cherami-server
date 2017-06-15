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

package cassandra

import (
	"compress/gzip"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	log "github.com/sirupsen/logrus"

	"github.com/uber/cherami-server/tools/awscloud"
)

// BackupTask runs a single run
// of backup for cassandra
type BackupTask struct {
	taskBase
	hostname        string
	keyspace        string
	rootDirPath     string
	schemaBackupDir string
	deployment      string
	s3BucketName    string
	name            string // backup will be created with this name
	quitC           chan struct{}
	stoppedC        chan struct{}
	nodeTool        NodeTool
	s3              awscloud.SecureS3Client
}

const dirPerm = os.ModeDir | os.FileMode(0755)
const filePerm = os.FileMode(0644)

type taskBase struct{}

const (
	schemaFileName = "schema.txt" // File name used to store captured schema
	peersFileName  = "peers.json" // File name used to store captured cassandra peers
)

func (task *taskBase) mkFileName(name string, keyspace string, suffix string) string {
	return name + "_" + keyspace + "_" + suffix
}

// s3KeyPrefix constructs the key prefix for backup objects
// looks like - deployment/cassandra/backups/cherami/cherami-server/20160628
func (task *taskBase) s3KeyPrefix(hostname string, name string, keyspace string, deployment string) string {
	return deployment + "/cassandra/backups/" + keyspace + "/" + hostname + "/" + name
}

// NewBackupTask creates and returns a task that
// captures a snapshot from cassandra and uploads
// the artifacts to S3. The snapshot will be captured
// with the given snapshotName the artifacts will be
// stored in S3 under the same name. Following is the
// key naming used in S3:
//
//   bucket:cherami
//          - deployment-name
//               - cassandra/backups/keyspace
//                  - metadata-servername
//                     - snapshotName
//                           snapshotName_keyspace_schema.txt
//                           snapshotName_keyspace_peers.json
//                           table1-hexstring
//                                - file1.db
//                                - manifest.json
//                           table2-hexstring
//                                .....
func NewBackupTask(config *BackupToolConfig, snapshotName string, hostname string, secret *Secret) (*BackupTask, error) {
	aesKey, err := hex.DecodeString(secret.Snapshot.AESMasterKey)
	if err != nil {
		return nil, err
	}
	cp := awscloud.NewCredProvider(secret.AWSAccessKeyID, secret.AWSSecretKey)
	s3 := awscloud.NewSecureS3Client(config.AWS.Region, cp, aesKey)

	addr, err := findEth0Addr()
	if err != nil {
		addr = "127.0.0.1"
	}

	nodeTool := NewNodeTool(addr)

	return &BackupTask{
		s3:              s3,
		nodeTool:        nodeTool,
		hostname:        hostname,
		deployment:      config.Deployment,
		schemaBackupDir: config.Backup.SchemaBackupDir,
		rootDirPath:     config.RootDirPath,
		s3BucketName:    config.AWS.S3Bucket,
		keyspace:        config.Backup.Keyspace,
		name:            snapshotName,
		quitC:           make(chan struct{}),
		stoppedC:        make(chan struct{}),
	}, nil
}

// Run runs the backup task
func (task *BackupTask) Run() error {

	objMap, ok := task.isSnapshotOnS3()
	if ok {
		// nothing to do, snapshot already present
		log.Infof("Snapshot already on S3, keyspace=%v,name=%v", task.keyspace, task.name)
		return nil
	}

	var err error

	if !task.isSnapshotOnDisk() {
		err = task.takeSnapshot()
		if err != nil {
			return err
		}
	}

	if isQuit(task.quitC) {
		goto done
	}

	err = task.uploadSnapshot(objMap)
	if err != nil {
		return err
	}

	if isQuit(task.quitC) {
		goto done
	}

	task.writeManifestFile()

	log.Infof("BackupTask succeeded, keyspace=%v,name=%v", task.keyspace, task.name)

done:
	close(task.stoppedC)
	return nil
}

// Stop sends a stop signal and waits
// until timeout for the task to quit
func (task *BackupTask) Stop(d time.Duration) bool {
	close(task.quitC)
	select {
	case <-task.stoppedC:
		return true
	case <-time.After(d):
		return false
	}
}

func (task *BackupTask) uploadSnapshot(objMap map[string]int64) error {

	keyToPath, err := task.findSnapshotFiles()
	if err != nil {
		return newTaskError("Can't find all snapshot files, err=%v", err)
	}

	if len(keyToPath) < 2 {
		log.Errorf("Can't find all snapshot files, keyspace=%v,snapshot=%v", task.keyspace, task.name)
		return newTaskError("Cannot find snapshot files after snapshot", nil)
	}

	log.Infof("Preparing for upload")

	for key, path := range keyToPath {

		s3Key := task.s3KeyPrefix(task.hostname, task.name, task.keyspace, task.deployment) + "/" + key

		if _, ok := objMap[s3Key]; ok {
			continue // already uploaded
		}

		e := task.uploadFileWithRetry(s3Key, path)
		if e != nil {
			log.Errorf("Giving up snapshot upload, s3 upload error; keyspace=%v, snapshot=%v, err=%v", task.keyspace, task.name, err)
			return newTaskError("s3 upload error", err)
		}
	}

	return nil
}

// uploads file with exponential backoff
func (task *BackupTask) uploadFileWithRetry(key string, path string) error {

	var backoff = time.Second
	var expiry = time.Now().UnixNano() + int64(20*time.Minute)

	log.Infof("Uploading file=%v to s3key=%v", path, key)

	for !isQuit(task.quitC) {

		f, err := os.Open(path)
		if err != nil {
			return err
		}

		reader, writer := io.Pipe()
		go func() {
			gw := gzip.NewWriter(writer)
			io.Copy(gw, f)
			f.Close()
			gw.Close()
			writer.Close()
		}()

		err = task.s3.Put(task.s3BucketName, key, reader)

		f.Close()

		if err == nil {
			break
		}

		reqErr, ok := toAWSReqErr(err)
		if !ok || reqErr.StatusCode() != http.StatusServiceUnavailable {
			return err // non-retryable error
		}

		if time.Now().UnixNano()+int64(backoff) > expiry {
			return err // max attempts exceeded
		}

		log.Infof("Error uploading file to S3, will retry, file=%v, err=%v", path, err)

		sleep(task.quitC, backoff)

		backoff = time.Duration(2 * int64(backoff))
	}

	return nil
}

func (task *BackupTask) findSnapshotFiles() (map[string]string, error) {

	var name = task.name
	var keyspace = task.keyspace

	var keyToPath = make(map[string]string)
	var tableRegex = "([[:word:]-]+-[[:xdigit:]]+)"
	var keyspaceDir = task.rootDirPath + "/data/data/" + task.keyspace

	dirRegex := regexp.MustCompile(keyspaceDir + "/" + tableRegex + "/snapshots/" + name + "/(.*$)")

	// Cassandra stores the snapshots using the following dir structure
	//
	//  cassandraRoot/data/data/keyspaceName
	//           tableName1-hexstring/snapshots/snapshotName
	// 			 tableName2-hexstring/snapshots/snapshotName
	//
	// Code below does a regex match with subexp being the table-hexstring
	filepath.Walk(keyspaceDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		match := dirRegex.FindStringSubmatch(path)
		if match != nil && len(match) == 3 {
			key := match[1] + "/" + match[2]
			keyToPath[key] = path
		}
		return nil
	})

	// also include snapshot metadata files
	var dirPath = task.schemaBackupDir
	var keys = []string{task.mkFileName(name, keyspace, schemaFileName), task.mkFileName(name, keyspace, peersFileName)}

	for _, key := range keys {
		var fpath = dirPath + "/" + key
		_, err := os.Stat(fpath)
		if err != nil {
			return nil, newTaskError("Cannot find file:"+fpath, err)
		}
		keyToPath[key] = fpath
	}

	return keyToPath, nil
}

func (task *BackupTask) takeSnapshot() error {

	task.deleteOldSnapshots()

	dirPath := task.schemaBackupDir
	_, err := os.Stat(dirPath)
	if err != nil {
		err = os.Mkdir(dirPath, dirPerm)
		if err != nil {
			return newTaskError("Failed to create backups dir", err)
		}
	}

	schema, err := task.nodeTool.DescKeyspace(task.keyspace)
	if err != nil {
		return newTaskError("Failed to desc keyspace", err)
	}

	fpath := dirPath + "/" + task.mkFileName(task.name, task.keyspace, schemaFileName)
	err = ioutil.WriteFile(fpath, []byte(schema), filePerm)
	if err != nil {
		return newTaskError("Error creating schema file", err)
	}

	peers, err := task.nodeTool.GetPeers()
	if err != nil {
		return newTaskError("Failed to discover peers", err)
	}

	blob, err := json.Marshal(peers)
	if err != nil {
		return newTaskError("Error encoding peers into json", err)
	}

	fpath = dirPath + "/" + task.mkFileName(task.name, task.keyspace, peersFileName)
	err = ioutil.WriteFile(fpath, blob, filePerm)
	if err != nil {
		return newTaskError("Error creating peers json", err)
	}

	err = task.nodeTool.CreateSnapshot(task.name, task.keyspace)
	if err != nil {
		return newTaskError("Failed to create snapshot", err)
	}

	return nil
}

func (task *BackupTask) deleteOldSnapshots() {
	log.Info("Deleting old snapshots")
	err := task.nodeTool.ClearAllSnapshots()
	if err != nil {
		log.WithField("error", err).Error("Deleting old snapshots failed")
	}
	os.RemoveAll(task.schemaBackupDir)
}

func (task *BackupTask) writeManifestFile() {
	path := task.schemaBackupDir + "/" + task.name + "_" + task.keyspace + ".manifest"
	ioutil.WriteFile(path, []byte("done"), filePerm)
}

func (task *BackupTask) writeFile(dirPath string, fname string, data string) error {
	fpath := dirPath + "/" + fname
	f, err := os.OpenFile(fpath, os.O_WRONLY, filePerm)
	if err != nil {
		return err
	}
	_, err = f.WriteString(data)
	if err != nil {
		f.Close()
		os.Remove(fpath)
		return err
	}
	f.Close()
	return nil
}

func toAWSReqErr(err error) (awserr.RequestFailure, bool) {
	if _, ok := err.(awserr.Error); ok {
		reqErr, ok1 := err.(awserr.RequestFailure)
		return reqErr, ok1
	}
	return nil, false
}

func (task *BackupTask) isSnapshotOnDisk() bool {
	path := task.schemaBackupDir + "/" + task.name + "_" + task.keyspace + ".manifest"
	_, err := os.Stat(path)
	if err != nil {
		return false
	}
	return true
}

func (task *BackupTask) isSnapshotOnS3() (map[string]int64, bool) {

	s3KeyPrefix := task.s3KeyPrefix(task.hostname, task.name, task.keyspace, task.deployment)

	// example prod/cassandra/backups/20160623
	objMap, err := task.s3.List(task.s3BucketName, s3KeyPrefix)
	if err != nil {
		log.Errorf("Error listing S3 bucket, prefix=%v, err=%v", s3KeyPrefix, err)
		return nil, false
	}

	// this is the last file we write, if this
	// exists, then the whole snapshot was successfully
	// uploaded
	peersKey := s3KeyPrefix + "/" + task.mkFileName(task.name, task.keyspace, peersFileName)
	_, ok := objMap[peersKey]
	return objMap, ok
}
