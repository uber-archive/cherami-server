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
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
)

type (
	SchemaUpdateTestSuite struct {
		suite.Suite
	}
)

func TestSchemaUpdateTestSuite(t *testing.T) {
	suite.Run(t, new(SchemaUpdateTestSuite))
}

func (s *SchemaUpdateTestSuite) TestBasic() {
	assert := s.Require()
	manifestDir, err := ioutil.TempDir("", "schemaUpdateTest")
	assert.Nil(err, "Failed to create manifest dir")

	cqlFileContent := generateTestCQLFileContent()

	cqlFile, err := ioutil.TempFile(manifestDir, "testCQL")
	assert.Nil(err, "Failed to create temp cql file")

	cqlFile.WriteString(cqlFileContent)
	assert.Nil(err, "Failed to write to temp cql file")

	manifestFileContent := `{
		"CurrVersion": 1,
		"MinCompatibleVersion": 1,
		"Description": "test",
		"SchemaUpdateCqlFiles": ["` + filepath.Base(cqlFile.Name()) + `"]
	}`

	manifestFile, err := ioutil.TempFile(manifestDir, "testManifest")
	assert.Nil(err, "Failed to create temp manifest file")

	_, err = manifestFile.WriteString(manifestFileContent)
	assert.Nil(err, "Failed to write to temp manifest file")

	cfg := &SchemaUpdaterConfig{
		HostsCsv:         "127.0.0.1",
		Keyspace:         "cherami",
		ManifestFilePath: manifestFile.Name(),
	}

	cqlClient := newMockCQLClient(0, 0)
	updateTask := &SchemaUpdateTask{
		config:    cfg,
		cqlClient: cqlClient,
	}

	err = updateTask.Run()

	os.Remove(cqlFile.Name())
	os.Remove(manifestFile.Name())
	os.Remove(manifestDir)

	assert.Nil(err, "UpdateTask failed to update schema")
	assert.Equal(int64(1), cqlClient.currVersion, "Wrong version after updating schema")
	assert.Equal(int64(1), cqlClient.minVersion, "Wrong min version after updating schema")
	assert.Equal(1, len(cqlClient.updateLogs), "Wrong update_history entries after schema update")
	assert.Equal(int64(0), cqlClient.updateLogs[0].oldVersion, "Wrong old version in update log")
	assert.Equal(int64(1), cqlClient.updateLogs[0].newVersion, "Wrong new version in update log")
}

func (s *SchemaUpdateTestSuite) TestWhitelist() {
	assert := s.Require()
	manifestDir, err := ioutil.TempDir("", "schemaUpdateTest")
	assert.Nil(err, "Failed to create manifest dir")

	cqlFileContent := `DROP TABLE foo;`
	cqlFile, err := ioutil.TempFile(manifestDir, "testCQL")
	assert.Nil(err, "Failed to create temp cql file")

	cqlFile.WriteString(cqlFileContent)
	assert.Nil(err, "Failed to write to temp cql file")

	manifestFileContent := `{
		"CurrVersion": 1,
		"MinCompatibleVersion": 1,
		"Description": "test",
		"SchemaUpdateCqlFiles": ["` + filepath.Base(cqlFile.Name()) + `"]
	}`

	manifestFile, err := ioutil.TempFile(manifestDir, "testManifest")
	assert.Nil(err, "Failed to create temp manifest file")
	_, err = manifestFile.WriteString(manifestFileContent)
	assert.Nil(err, "Failed to write to temp manifest file")

	cfg := &SchemaUpdaterConfig{
		HostsCsv:         "127.0.0.1",
		Keyspace:         "cherami",
		ManifestFilePath: manifestFile.Name(),
	}

	cqlClient := newMockCQLClient(0, 0)
	updateTask := &SchemaUpdateTask{
		config:    cfg,
		cqlClient: cqlClient,
	}

	err = updateTask.Run()

	os.Remove(cqlFile.Name())
	os.Remove(manifestFile.Name())
	os.Remove(manifestDir)

	s.NotNil(err, "Schema update expected to fail, when CQL file contains DROP cmds")
}

func (s *SchemaUpdateTestSuite) TestMultiVersionJumpFails() {
	assert := s.Require()
	manifestDir, err := ioutil.TempDir("", "schemaUpdateTest")
	assert.Nil(err, "Failed to create manifest dir")

	cqlFileContent := generateTestCQLFileContent()
	cqlFile, err := ioutil.TempFile(manifestDir, "testCQL")
	assert.Nil(err, "Failed to create temp cql file")

	cqlFile.WriteString(cqlFileContent)
	assert.Nil(err, "Failed to write to temp cql file")

	manifestFileContent := `{
		"CurrVersion": 5,
		"MinCompatibleVersion": 2,
		"Description": "test",
		"SchemaUpdateCqlFiles": ["` + filepath.Base(cqlFile.Name()) + `"]
	}`

	manifestFile, err := ioutil.TempFile(manifestDir, "testManifest")
	assert.Nil(err, "Failed to create temp manifest file")
	_, err = manifestFile.WriteString(manifestFileContent)
	assert.Nil(err, "Failed to write to temp manifest file")

	cfg := &SchemaUpdaterConfig{
		HostsCsv:         "127.0.0.1",
		Keyspace:         "cherami",
		ManifestFilePath: manifestFile.Name(),
	}

	cqlClient := newMockCQLClient(7, 2)
	updateTask := &SchemaUpdateTask{
		config:    cfg,
		cqlClient: cqlClient,
	}

	err = updateTask.Run()
	os.Remove(cqlFile.Name())
	os.Remove(manifestFile.Name())
	os.Remove(manifestDir)
	s.NotNil(err, "Schema update expected to fail, when trying to jump multiple versions")
}

type updateLog struct {
	oldVersion int64
	newVersion int64
}

type mockCQLClient struct {
	currVersion int64
	minVersion  int64
	updateLogs  []*updateLog
}

func newMockCQLClient(currVersion int64, minVersion int64) *mockCQLClient {
	return &mockCQLClient{
		currVersion: currVersion,
		minVersion:  minVersion,
		updateLogs:  make([]*updateLog, 0, 1),
	}
}

func (mock *mockCQLClient) Exec(stmt string) error {
	return nil
}

func (mock *mockCQLClient) ReadSchemaVersion() (int64, error) {
	return mock.currVersion, nil
}

func (mock *mockCQLClient) UpdateSchemaVersion(newVersion int64, minCompatibleVersion int64) error {
	mock.currVersion = newVersion
	mock.minVersion = minCompatibleVersion
	return nil
}

func (mock *mockCQLClient) WriteSchemaUpdateLog(oldVersion int64, newVersion int64, manifestMD5 string, desc string) error {
	log := &updateLog{
		oldVersion: oldVersion,
		newVersion: newVersion,
	}
	logs := make([]*updateLog, 0, len(mock.updateLogs)+1)
	logs = append(logs, log)
	logs = append(logs, mock.updateLogs...)
	mock.updateLogs = logs
	return nil
}

func generateTestCQLFileContent() string {
	return `
-- test cql file

CREATE TYPE bar_type (
  extent_uuid uuid,
  store_uuid uuid,
  destination_uuid uuid,
  status int, -- ExtentReplicaStatus enum
  -- begin = the first message in the extent that could ever be consumed. It may or may not be 'available' on a timer queue.
  -- Retention will change this from the initial values (i.e. zero). We have no 'rate' for this, as we expect the value to change in discrete jumps
  begin_address bigint,
  begin_sequence bigint,
  -- available = a consumer group could consume this message now; same as 'last' for append only, 
  -- but indicates the position of the 'now' cursor for timer queues
  available_address bigint,
  available_sequence bigint,
  available_sequence_rate double,
  -- last = the message that will be delivered last
  last_address bigint,
  last_sequence bigint,
  last_sequence_rate double,
  -- == --
  created_time timestamp, -- time at which the extent was created; distinct from when the first message was added
  begin_time timestamp, -- begin time is the time when the first message of a timer queue becomes available
  end_time timestamp, -- end time is the time that the last message becomes available
  begin_enqueue_time timestamp, -- begin enqueue time is the time that the first message was written (enqueued) to the extent
  last_enqueue_time timestamp, -- last enqueue time is the time that the last message was written (enqueued) to the extent
  -- size in bytes is the amount of hard disk space that would be recovered by deleting this extent
  size_in_bytes bigint,
  size_in_bytes_rate double
);


CREATE TABLE foo (
  foo_uuid uuid,
  status int, --  enum
  bars frozen<bar_type>,
  PRIMARY KEY (foo_uuid)
);

CREATE INDEX ON foo (status);

`
}
