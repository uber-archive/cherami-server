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
	"io"
	"os"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/uber/cherami-server/tools/awscloud"
)

// RestoreTask represents a task that
// restores a backup snapshot from S3
// to disk
type RestoreTask struct {
	taskBase
	snapshotHostname string
	snapshotKey      string
	keyspace         string
	deployment       string
	outputDirPath    string
	s3BucketName     string
	s3               awscloud.SecureS3Client
}

// NewRestoreTask returns a new instance of task
// that fetches a cassandra snapshot from s3 and
// stores it onto a given output dir.
func NewRestoreTask(cfg *BackupToolConfig, secret *Secret) (*RestoreTask, error) {
	aesKey, err := hex.DecodeString(secret.Snapshot.AESMasterKey)
	if err != nil {
		return nil, err
	}
	cp := awscloud.NewCredProvider(secret.AWSAccessKeyID, secret.AWSSecretKey)
	s3 := awscloud.NewSecureS3Client(cfg.AWS.Region, cp, aesKey)
	return &RestoreTask{
		s3:               s3,
		snapshotHostname: cfg.Restore.SnapshotHostname,
		snapshotKey:      cfg.Restore.SnapshotDate,
		keyspace:         cfg.Restore.Keyspace,
		deployment:       cfg.Deployment,
		outputDirPath:    cfg.Restore.OutputDirPath,
		s3BucketName:     cfg.AWS.S3Bucket,
	}, nil
}

// Run runs the restore task
func (task *RestoreTask) Run() error {
	pathToKeys, ok := task.getFilePathsToS3Keys()
	if !ok {
		return newTaskError("Snapshot not found on S3", nil)
	}

	return task.downloadFiles(pathToKeys)
}

func (task *RestoreTask) mkDir(path string) error {
	_, err := os.Stat(path)
	if err != nil {
		err = os.MkdirAll(path, dirPerm)
		if err != nil {
			log.Errorf("Failed to create directory, dir=%v,err=%v", path, err)
			return err
		}
	}
	return nil
}

func (task *RestoreTask) downloadFiles(pathToKeys map[string]string) error {

	err := task.mkDir(task.outputDirPath)
	if err != nil {
		return nil
	}

	for path, key := range pathToKeys {

		task.mkDir(filepath.Dir(path))

		err = task.downloadFile(path, key)
		if err != nil {
			log.Errorf("Error downloading file, key=%v, path=%v, err=%v", key, path, err)
			return err
		}
	}

	return nil
}

func (task *RestoreTask) downloadFile(path string, s3Key string) error {

	log.Infof("Downloading s3key=%v to path=%v", s3Key, path)

	reader, err := task.s3.Get(task.s3BucketName, s3Key)
	if err != nil {
		return err
	}

	gzipIn, err := gzip.NewReader(reader)
	if err != nil {
		return err
	}

	outFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, filePerm)
	if err != nil {
		gzipIn.Close()
		log.Errorf("Error opening output file, path=%v, err=%v", path, err)
		return err
	}

	_, err = io.Copy(outFile, gzipIn)
	if err != nil {
		gzipIn.Close()
		outFile.Close()
		log.Errorf("Error downloading gzipped file, err=%v", err)
		return err
	}

	gzipIn.Close()
	outFile.Close()

	return nil
}

func (task *RestoreTask) getFilePathsToS3Keys() (map[string]string, bool) {

	name := task.snapshotKey
	s3Prefix := task.s3KeyPrefix(task.snapshotHostname, name, task.keyspace, task.deployment)

	objMap, err := task.s3.List(task.s3BucketName, s3Prefix)
	if err != nil {
		log.Errorf("Error listing S3 bucket, prefix=%v, err=%v", s3Prefix, err)
		return nil, false
	}

	// this is the last file we write, if this
	// exists, then the whole snapshot was successfully
	// uploaded
	peersKey := s3Prefix + "/" + task.mkFileName(name, task.keyspace, peersFileName)
	_, ok := objMap[peersKey]
	if !ok {
		return nil, false
	}

	var result = make(map[string]string)

	for k := range objMap {
		tokens := strings.Split(k, s3Prefix)
		if len(tokens) != 2 || tokens[1] == "/" {
			continue // should never happen
		}
		result[task.outputDirPath+tokens[1]] = k
	}
	return result, ok
}
