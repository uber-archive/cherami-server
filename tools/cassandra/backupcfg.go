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
	"github.com/uber/cherami-server/common/configure"
)

type (
	// BackupToolConfig is the configuration used for cassandra-backup tool
	BackupToolConfig struct {
		Deployment  string                     `yaml:"Deployment"`
		RootDirPath string                     `yaml:"RootDirPath"`
		AWS         AWSConfig                  `yaml:"AWS"`
		Logging     configure.LogConfiguration `yaml:"Logging"`
		Backup      BackupConfig               `yaml:"Backup"`
		Restore     RestoreConfig              `yaml:"Restore"`
	}

	// BackupConfig has all the backup specific configurations
	BackupConfig struct {
		Schedule        string `yaml:"Schedule"`
		HourOfDay       int    `yaml:"HourOfDay"`
		Keyspace        string `yaml:"Keyspace"`
		SchemaBackupDir string `yaml:"SchemaBackupDir"`
	}

	// RestoreConfig has snapshot restore specific configuration
	RestoreConfig struct {
		SnapshotDate     string `yaml:"SnapshotDate"`
		SnapshotHostname string `yaml:"SnapshotHostname"`
		OutputDirPath    string `yaml:"OutputDirPath"`
		Keyspace         string `yaml:"Keyspace"`
	}

	// AWSConfig contains aws specific configuratoin
	AWSConfig struct {
		Region         string `yaml:"Region"`
		S3Bucket       string `yaml:"S3Bucket"`
		SecretFilePath string `yaml:"SecretFilePath"`
	}

	// Secret represents the credentials that come from langley
	Secret struct {
		AWSAccessKeyID string        `yaml:"aws_access_key_id"`
		AWSSecretKey   string        `yaml:"aws_secret_access_key"`
		Snapshot       SnapshotCreds `yaml:"snapshot"`
	}

	// SnapshotCreds are the secrets needed to encrypt
	// snapshot data
	SnapshotCreds struct {
		AESMasterKey string `yaml:"aesMasterKey"`
	}
)

// LoadSecret loads the secret yaml from the given path
func LoadSecret(path string) (*Secret, error) {
	var secret Secret
	err := configure.NewCommonConfigure().LoadFile(&secret, path)
	if err != nil {
		return nil, err
	}
	return &secret, nil
}

// ValidateBackupConfig validates the given config
func ValidateBackupConfig(cfg *BackupToolConfig) error {
	if err := validateAWSConfig(&cfg.AWS); err != nil {
		return err
	}
	if len(cfg.Backup.Keyspace) < 1 {
		return newConfigError("Missing backup.Keyspace", nil)
	}
	if len(cfg.Backup.SchemaBackupDir) < 1 {
		return newConfigError("Missing backup.SchemaBackupDir", nil)
	}
	return nil
}

// ValidateRestoreConfig validate the given restore config
func ValidateRestoreConfig(cfg *BackupToolConfig) error {
	if err := validateAWSConfig(&cfg.AWS); err != nil {
		return err
	}
	if len(cfg.Restore.SnapshotDate) < 1 {
		return newConfigError("Missing restore.SnapshotDate", nil)
	}
	if len(cfg.Restore.OutputDirPath) < 1 {
		return newConfigError("Missing restore.OutputDirPath", nil)
	}
	if len(cfg.Restore.Keyspace) < 1 {
		return newConfigError("Missing restore.Keyspace", nil)
	}
	if len(cfg.Restore.SnapshotHostname) < 1 {
		return newConfigError("Missing restore.SnapshotHostname", nil)
	}
	return nil
}

func validateAWSConfig(aws *AWSConfig) error {
	if len(aws.Region) < 1 {
		return newConfigError("Missing aws region", nil)
	}
	if len(aws.SecretFilePath) < 1 {
		return newConfigError("Missing aws secret file path", nil)
	}
	if len(aws.S3Bucket) < 1 {
		return newConfigError("Missing aws s3 bucket name", nil)
	}
	return nil
}
