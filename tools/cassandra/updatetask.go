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
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
)

type (
	// SchemaUpdateTask represents a task that
	// takes a config for schema upgrade and in
	// turn, runs the upgrade
	SchemaUpdateTask struct {
		config    *SchemaUpdaterConfig
		cqlClient CQLClient
	}
	// Manifest represents the json deserialized
	// object for the schema update manifest
	Manifest struct {
		CurrVersion          int64
		MinCompatibleVersion int64
		Description          string
		SchemaUpdateCqlFiles []string
		md5                  string
	}
)

var (
	whitelistedCQLPrefixes = [2]string{"CREATE", "ALTER"}
)

// NewSchemaUpdateTask creates and returns a new SchemaUpdateTask
func NewSchemaUpdateTask(config *SchemaUpdaterConfig) (*SchemaUpdateTask, error) {
	cqlClient, err := newCQLClient(config)
	if err != nil {
		return nil, err
	}
	return &SchemaUpdateTask{
		config:    config,
		cqlClient: cqlClient,
	}, nil
}

// Run runs the task
func (task *SchemaUpdateTask) Run() error {

	config := task.config

	manifest, err := unmarshalManifest(config.ManifestFilePath)
	if err != nil {
		log.Errorf("Error unmarshaling manifest json, file=%v, err=%v", config.ManifestFilePath, err)
		return err
	}

	cqlStmts, err := parseCQLStmts(config.ManifestFilePath, manifest)
	if err != nil {
		log.Errorf("Error parsing CQL statements, err=%v", err)
		return err
	}

	err = validateCQLStmts(cqlStmts)
	if err != nil {
		log.Errorf("CQL validation failed, err=%v", err)
		return err
	}

	oldVer, err := task.checkVersionUpgradeIsSane(manifest)
	if err != nil {
		log.Errorf("Invalid version upgrade, err=%v", err)
		return err
	}

	err = task.execStmts(cqlStmts)
	if err != nil {
		log.Errorf("Schema update failed, err=%v", err)
		return err
	}

	if config.IsDryRun {
		return nil
	}

	err = task.cqlClient.UpdateSchemaVersion(manifest.CurrVersion, manifest.MinCompatibleVersion)
	if err != nil {
		log.Errorf("Failed to update schema_version table, err=%v", err)
		return err
	}

	err = task.cqlClient.WriteSchemaUpdateLog(oldVer, manifest.CurrVersion, manifest.md5, manifest.Description)
	if err != nil {
		log.Errorf("Failed to add entry to schema_update_history, err=%v\n", err.Error())
		return err
	}

	log.Infof("Schema update success, oldVersion=%v, newVersion=%v", oldVer, manifest.CurrVersion)
	return nil
}

func (task *SchemaUpdateTask) checkVersionUpgradeIsSane(manifest *Manifest) (int64, error) {
	config := task.config
	oldVer, err := task.cqlClient.ReadSchemaVersion()
	if err != nil {
		return 0, fmt.Errorf("Error reading schema version, err=%v", err)
	}
	if config.SkipVersionCheck {
		return oldVer, nil
	}
	if manifest.CurrVersion == oldVer || manifest.CurrVersion == oldVer+1 {
		return oldVer, nil
	}
	return 0, fmt.Errorf("Can't upgrade from version %v to version %v, updates must be one-up", oldVer, manifest.CurrVersion)
}

func (task *SchemaUpdateTask) execStmts(stmts []string) error {
	for _, stmt := range stmts {
		var err error
		if !task.config.IsDryRun {
			err = task.cqlClient.Exec(stmt)
		}
		if err != nil {
			log.Errorf("%v FAILED", stmt)
			log.Errorf("err=%v", err)
			return err
		}
		log.Infof("%v SUCCESS", stmt)
	}
	return nil
}

func parseCQLStmts(manifestFilePath string, manifest *Manifest) ([]string, error) {
	dir := filepath.Dir(manifestFilePath)
	result := make([]string, 0, 4)
	for _, file := range manifest.SchemaUpdateCqlFiles {
		path := dir + "/" + file
		stmts, err := ParseCQLFile(path)
		if err != nil {
			return nil, fmt.Errorf("Error parsing file %v, err=%v", path, err)
		}
		result = append(result, stmts...)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("Found 0 statements to execute upgrade")
	}
	return result, nil
}

func validateCQLStmts(stmts []string) error {
	for _, stmt := range stmts {
		valid := false
		for _, prefix := range whitelistedCQLPrefixes {
			if strings.HasPrefix(stmt, prefix) {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("CQL prefix not in whitelist, stmt=%v", stmt)
		}
	}
	return nil
}

func unmarshalManifest(filePath string) (*Manifest, error) {

	jsonStr, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	jsonBlob := []byte(jsonStr)

	var manifest Manifest
	err = json.Unmarshal(jsonBlob, &manifest)
	if err != nil {
		return nil, err
	}

	if manifest.CurrVersion == 0 {
		return nil, fmt.Errorf("Manifest missing CurrVersion")
	}

	if manifest.MinCompatibleVersion == 0 {
		return nil, fmt.Errorf("Manifest missing MinCompatibleVersion")
	}

	if len(manifest.SchemaUpdateCqlFiles) == 0 {
		return nil, fmt.Errorf("Manifest missing SchemaUpdateCqlFiles")
	}

	md5Bytes := md5.Sum(jsonBlob)
	manifest.md5 = hex.EncodeToString(md5Bytes[:])

	return &manifest, nil
}
