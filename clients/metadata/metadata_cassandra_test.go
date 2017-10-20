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

package metadata

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/set"
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"

	log "github.com/sirupsen/logrus"
)

type CassandraSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	seqNum              int32
	tableNames          []string
	typeNames           []string
	suite.Suite
	TestCluster
}

const testPageSize = 2

func TestCassandraSuite(t *testing.T) {
	s := new(CassandraSuite)
	suite.Run(t, s)
}

func (s *CassandraSuite) SetupSuite() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.TestCluster.SetupTestCluster()
	s.seqNum = rand.Int31n(10000)
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
	s.initSchema()
}

func (s *CassandraSuite) TearDownSuite() {
	s.TestCluster.TearDownTestCluster()
}

func (s *CassandraSuite) generateName(prefix string) string {
	seq := int(atomic.AddInt32(&s.seqNum, 1))
	return strings.Join([]string{prefix, strconv.Itoa(seq)}, "_")
}

// initSchema does a desc keyspace and discovers the
// table / type names. This is to fuzz the
// table / types during test
func (s *CassandraSuite) initSchema() {

	targets := []string{"type", "table"}
	queriesCass22 := []string{
		fmt.Sprintf("select type_name from system.schema_usertypes where keyspace_name='%s'", s.keyspace),
		fmt.Sprintf("select columnfamily_name from system.schema_columnfamilies where keyspace_name='%s'", s.keyspace),
	}

	for i, target := range targets {
		var names []string
		var qry string
		var name string
		qry = queriesCass22[i]
		iter := s.TestCluster.session.Query(qry).Iter()
		for iter.Scan(&name) {
			names = append(names, name)
		}
		err := iter.Close()
		if err != nil {
			// If we get an error, make sure if this is the latest
			// cassandra version, which has different system
			// keyspaces.
			// https://docs.datastax.com/en/cql/3.3/cql/cql_using/useQuerySystem.html
			queriesCass3 := []string{
				fmt.Sprintf("select type_name from system_schema.types where keyspace_name='%s'", s.keyspace),
				fmt.Sprintf("select table_name from system_schema.tables where keyspace_name='%s'", s.keyspace),
			}
			names = []string{}
			qry = queriesCass3[i]
			iter = s.TestCluster.session.Query(qry).Iter()
			name = ""
			for iter.Scan(&name) {
				names = append(names, name)
			}
			err = iter.Close()
		}
		s.Nil(err, "Failed to close iterator")
		if target == "type" {
			s.typeNames = names
		} else if target == "table" {
			s.tableNames = names
		}
	}
}

// utilAlter is a utility routine to either alter type or table
func (s *CassandraSuite) utilAlter(target string, targetNames []string) error {
	for _, name := range targetNames {
		alterQry := fmt.Sprintf("ALTER %s %s.%s ADD %s text", target, s.keyspace, name, s.generateName("column"))
		if err := s.TestCluster.session.Query(alterQry).Exec(); err != nil {
			fmt.Println(alterQry, err.Error())
			return err
		}
	}

	return nil
}

// Alter will alter all tables and types to test upgrade resiliency
func (s *CassandraSuite) Alter() error {
	// First alter types
	err := s.utilAlter("type", s.typeNames)
	if err != nil {
		return err
	}
	// Now alter tables
	return s.utilAlter("table", s.tableNames)
}

func createDestination(s *CassandraSuite, path string, dlqDestination bool) (*shared.DestinationDescription, error) {
	createReq := &shared.CreateDestinationRequest{
		Path: common.StringPtr(path),
		Type: common.InternalDestinationTypePtr(shared.DestinationType_PLAIN),
		ConsumedMessagesRetention:   common.Int32Ptr(1800),
		UnconsumedMessagesRetention: common.Int32Ptr(3600),
		OwnerEmail:                  common.StringPtr(destinationOwnerEmail),
		ChecksumOption:              common.InternalChecksumOptionPtr(0),
	}

	// DLQ has a consumer group UUID; when the corresponding consumer group is created, this ID is automatically used
	if dlqDestination {
		createReq.DLQConsumerGroupUUID = common.StringPtr(uuid.New())
	}

	return s.client.CreateDestination(nil, createReq)
}

var destinationOwnerEmail = "lhc@uber.com"
var testKafkaCluster = `kafkaCluster`
var testKafkaTopics = []string{`a`, `bb`}

func (s *CassandraSuite) TestDestinationCRUD() {
	// Create
	var destination *shared.DestinationDescription
	var destinationAlter *shared.DestinationDescription

	now := int64(common.Now())
	now -= now % int64(time.Millisecond) // Cassandra timestamps are less precise, so we should limit our precision for this test

	cDest := func(path string, dlq bool) *shared.DestinationDescription {
		zoneConfig := &shared.DestinationZoneConfig{
			Zone:                   common.StringPtr(`test`),
			AllowConsume:           common.BoolPtr(true),
			RemoteExtentReplicaNum: common.Int32Ptr(2),
		}
		createDestination := &shared.CreateDestinationRequest{
			Path: common.StringPtr(path),
			Type: common.InternalDestinationTypePtr(shared.DestinationType_PLAIN),
			ConsumedMessagesRetention:   common.Int32Ptr(1800),
			UnconsumedMessagesRetention: common.Int32Ptr(3600),
			OwnerEmail:                  common.StringPtr(destinationOwnerEmail),
			ChecksumOption:              common.InternalChecksumOptionPtr(0),
			IsMultiZone:                 common.BoolPtr(true),
			ZoneConfigs:                 []*shared.DestinationZoneConfig{zoneConfig},
			KafkaCluster:                common.StringPtr(testKafkaCluster),
			KafkaTopics:                 testKafkaTopics,
		}

		if dlq {
			createDestination.DLQConsumerGroupUUID = common.StringPtr(`1e486dc5-409d-48ed-af25-664ef58ce400`)
		}

		destination, err := s.client.CreateDestination(nil, createDestination)
		s.Nil(err)
		s.Equal(createDestination.GetPath(), destination.GetPath())
		s.Equal(createDestination.GetType(), destination.GetType())
		s.Equal(createDestination.GetConsumedMessagesRetention(), destination.GetConsumedMessagesRetention())
		s.Equal(createDestination.GetUnconsumedMessagesRetention(), destination.GetUnconsumedMessagesRetention())
		s.Equal(createDestination.GetOwnerEmail(), destination.GetOwnerEmail())
		s.Equal(createDestination.GetChecksumOption(), destination.GetChecksumOption())
		s.Equal(createDestination.GetIsMultiZone(), destination.GetIsMultiZone())
		s.Equal(len(createDestination.ZoneConfigs), len(destination.ZoneConfigs))
		s.Equal(zoneConfig.GetZone(), destination.GetZoneConfigs()[0].GetZone())
		s.Equal(zoneConfig.GetAllowConsume(), destination.GetZoneConfigs()[0].GetAllowConsume())
		s.Equal(zoneConfig.GetAllowPublish(), destination.GetZoneConfigs()[0].GetAllowPublish())
		s.Equal(zoneConfig.GetRemoteExtentReplicaNum(), destination.GetZoneConfigs()[0].GetRemoteExtentReplicaNum())
		s.Equal(createDestination.GetDLQConsumerGroupUUID(), destination.GetDLQConsumerGroupUUID())
		s.Equal(createDestination.KafkaCluster, destination.KafkaCluster)
		s.assertStringUnorderedArrayEqual(createDestination.KafkaTopics, destination.KafkaTopics, `KafkaTopics should match`)

		// Duplicated Create
		_, err = s.client.CreateDestination(nil, createDestination)
		s.NotNil(err)
		s.IsType(new(shared.EntityAlreadyExistsError), err)

		return destination
	}

	destination = cDest(s.generateName("foo/bar"), false)

	// Pass 0 is normal; pass 1 has ALTER in effect
	for pass := 0; pass < 2; pass++ {

		// Read
		// By UUID
		getDestination := &shared.ReadDestinationRequest{
			DestinationUUID: common.StringPtr(destination.GetDestinationUUID()),
		}
		loadedDestination, err := s.client.ReadDestination(nil, getDestination)
		s.Nil(err)
		s.Equal(destination.GetDestinationUUID(), loadedDestination.GetDestinationUUID())
		s.Equal(destination.GetPath(), loadedDestination.GetPath())

		s.Equal(destination.GetType(), loadedDestination.GetType())
		s.Equal(destination.GetConsumedMessagesRetention(), loadedDestination.GetConsumedMessagesRetention())
		s.Equal(destination.GetUnconsumedMessagesRetention(), loadedDestination.GetUnconsumedMessagesRetention())
		s.Equal(destination.GetStatus(), loadedDestination.GetStatus())
		s.Equal(destination.GetOwnerEmail(), loadedDestination.GetOwnerEmail())
		s.Equal(destination.GetChecksumOption(), loadedDestination.GetChecksumOption())
		s.Equal(destination.GetIsMultiZone(), loadedDestination.GetIsMultiZone())
		s.Equal(len(destination.GetZoneConfigs()), len(loadedDestination.GetZoneConfigs()))
		s.Equal(destination.GetZoneConfigs()[0].GetZone(), loadedDestination.GetZoneConfigs()[0].GetZone())
		s.Equal(destination.GetZoneConfigs()[0].GetAllowConsume(), loadedDestination.GetZoneConfigs()[0].GetAllowConsume())
		s.Equal(destination.GetZoneConfigs()[0].GetAllowPublish(), loadedDestination.GetZoneConfigs()[0].GetAllowPublish())
		s.Equal(destination.GetZoneConfigs()[0].GetRemoteExtentReplicaNum(), loadedDestination.GetZoneConfigs()[0].GetRemoteExtentReplicaNum())
		s.Equal(destination.GetDLQConsumerGroupUUID(), loadedDestination.GetDLQConsumerGroupUUID())
		s.Equal(destination.GetDLQPurgeBefore(), loadedDestination.GetDLQPurgeBefore())
		s.Equal(destination.GetDLQMergeBefore(), loadedDestination.GetDLQMergeBefore())
		s.Equal(destination.KafkaCluster, loadedDestination.KafkaCluster)
		s.assertStringUnorderedArrayEqual(destination.KafkaTopics, loadedDestination.KafkaTopics, `KafkaTopics should match`)

		// By Path
		getDestination = &shared.ReadDestinationRequest{
			Path: common.StringPtr(destination.GetPath()),
		}
		loadedDestination, err = s.client.ReadDestination(nil, getDestination)
		s.Nil(err)
		s.Equal(destination.GetDestinationUUID(), loadedDestination.GetDestinationUUID())
		s.Equal(destination.GetPath(), loadedDestination.GetPath())
		s.Equal(destination.GetType(), loadedDestination.GetType())
		s.Equal(destination.GetConsumedMessagesRetention(), loadedDestination.GetConsumedMessagesRetention())
		s.Equal(destination.GetUnconsumedMessagesRetention(), loadedDestination.GetUnconsumedMessagesRetention())
		s.Equal(destination.GetStatus(), loadedDestination.GetStatus())
		s.Equal(destination.GetOwnerEmail(), loadedDestination.GetOwnerEmail())
		s.Equal(destination.GetChecksumOption(), loadedDestination.GetChecksumOption())
		s.Equal(destination.GetIsMultiZone(), loadedDestination.GetIsMultiZone())
		s.Equal(len(destination.GetZoneConfigs()), len(loadedDestination.GetZoneConfigs()))
		s.Equal(destination.GetZoneConfigs()[0].GetZone(), loadedDestination.GetZoneConfigs()[0].GetZone())
		s.Equal(destination.GetZoneConfigs()[0].GetAllowConsume(), loadedDestination.GetZoneConfigs()[0].GetAllowConsume())
		s.Equal(destination.GetZoneConfigs()[0].GetAllowPublish(), loadedDestination.GetZoneConfigs()[0].GetAllowPublish())
		s.Equal(destination.GetZoneConfigs()[0].GetRemoteExtentReplicaNum(), loadedDestination.GetZoneConfigs()[0].GetRemoteExtentReplicaNum())
		s.Equal(``, loadedDestination.GetDLQConsumerGroupUUID()) //
		s.Equal(int64(0), loadedDestination.GetDLQPurgeBefore()) // DLQ destinations are not visible as a 'by path'
		s.Equal(int64(0), loadedDestination.GetDLQMergeBefore()) //
		s.Equal(destination.KafkaCluster, loadedDestination.KafkaCluster)
		s.assertStringUnorderedArrayEqual(destination.KafkaTopics, loadedDestination.KafkaTopics, `KafkaTopics should match`)

		// Update
		updateDestination := &shared.UpdateDestinationRequest{
			DestinationUUID: common.StringPtr(destination.GetDestinationUUID()),
			Status:          common.InternalDestinationStatusPtr(shared.DestinationStatus_RECEIVEONLY),
			ConsumedMessagesRetention:   common.Int32Ptr(30),
			UnconsumedMessagesRetention: common.Int32Ptr(60),
			OwnerEmail:                  common.StringPtr("lhc@uber.com")}
		updatedDestination, err := s.client.UpdateDestination(nil, updateDestination)
		s.Nil(err)
		s.Equal(destination.GetDestinationUUID(), updatedDestination.GetDestinationUUID())
		s.Equal(destination.GetPath(), updatedDestination.GetPath())
		s.Equal(destination.GetType(), updatedDestination.GetType())
		s.Equal(updateDestination.GetConsumedMessagesRetention(), updatedDestination.GetConsumedMessagesRetention())
		s.Equal(updateDestination.GetUnconsumedMessagesRetention(), updatedDestination.GetUnconsumedMessagesRetention())
		s.Equal(updateDestination.GetStatus(), updatedDestination.GetStatus())
		s.Equal(updateDestination.GetOwnerEmail(), updatedDestination.GetOwnerEmail())
		s.Equal(updateDestination.GetChecksumOption(), updatedDestination.GetChecksumOption())

		getDestination = &shared.ReadDestinationRequest{
			DestinationUUID: common.StringPtr(destination.GetDestinationUUID()),
		}
		loadedDestination, err = s.client.ReadDestination(nil, getDestination)
		s.Nil(err)
		s.Equal(updatedDestination.GetDestinationUUID(), loadedDestination.GetDestinationUUID())
		s.Equal(updatedDestination.GetPath(), loadedDestination.GetPath())
		s.Equal(updatedDestination.GetType(), loadedDestination.GetType())
		s.Equal(updatedDestination.GetConsumedMessagesRetention(), loadedDestination.GetConsumedMessagesRetention())
		s.Equal(updatedDestination.GetUnconsumedMessagesRetention(), loadedDestination.GetUnconsumedMessagesRetention())
		s.Equal(updatedDestination.GetStatus(), loadedDestination.GetStatus())
		s.Equal(updatedDestination.GetOwnerEmail(), loadedDestination.GetOwnerEmail())
		s.Equal(updatedDestination.GetChecksumOption(), loadedDestination.GetChecksumOption())
		s.Equal(updatedDestination.GetIsMultiZone(), loadedDestination.GetIsMultiZone())
		s.Equal(len(updatedDestination.GetZoneConfigs()), len(loadedDestination.GetZoneConfigs()))
		s.Equal(updatedDestination.GetZoneConfigs()[0].GetZone(), loadedDestination.GetZoneConfigs()[0].GetZone())
		s.Equal(updatedDestination.GetZoneConfigs()[0].GetAllowConsume(), loadedDestination.GetZoneConfigs()[0].GetAllowConsume())
		s.Equal(updatedDestination.GetZoneConfigs()[0].GetAllowPublish(), loadedDestination.GetZoneConfigs()[0].GetAllowPublish())
		s.Equal(updatedDestination.GetZoneConfigs()[0].GetRemoteExtentReplicaNum(), loadedDestination.GetZoneConfigs()[0].GetRemoteExtentReplicaNum())
		s.Equal(updatedDestination.GetDLQConsumerGroupUUID(), loadedDestination.GetDLQConsumerGroupUUID())
		s.Equal(updatedDestination.GetDLQPurgeBefore(), loadedDestination.GetDLQPurgeBefore())
		s.Equal(updatedDestination.GetDLQMergeBefore(), loadedDestination.GetDLQMergeBefore())
		s.Equal(destination.KafkaCluster, loadedDestination.KafkaCluster)
		s.assertStringUnorderedArrayEqual(destination.KafkaTopics, loadedDestination.KafkaTopics, `KafkaTopics should match`)

		destination.ConsumedMessagesRetention = updateDestination.ConsumedMessagesRetention
		destination.UnconsumedMessagesRetention = updateDestination.UnconsumedMessagesRetention
		destination.Status = updateDestination.Status

		// Merge/Purge
		updateDestinationDLQCursors := &m.UpdateDestinationDLQCursorsRequest{
			DestinationUUID: common.StringPtr(destination.GetDestinationUUID()),
			DLQPurgeBefore:  common.Int64Ptr(now),
			DLQMergeBefore:  nil,
		}

		for cursorsPass := 0; cursorsPass < 2; cursorsPass++ {
			updatedDestination, err = s.client.UpdateDestinationDLQCursors(nil, updateDestinationDLQCursors)
			s.Nil(err)
			s.Equal(destination.GetDestinationUUID(), updatedDestination.GetDestinationUUID())
			s.Equal(destination.GetPath(), updatedDestination.GetPath())
			s.Equal(destination.GetType(), updatedDestination.GetType())
			if updateDestinationDLQCursors.DLQPurgeBefore != nil {
				s.Equal(updateDestinationDLQCursors.GetDLQPurgeBefore(), updatedDestination.GetDLQPurgeBefore())
			} else {
				s.Equal(updateDestinationDLQCursors.GetDLQMergeBefore(), updatedDestination.GetDLQMergeBefore())
			}

			getDestination = &shared.ReadDestinationRequest{
				DestinationUUID: common.StringPtr(destination.GetDestinationUUID()),
			}
			loadedDestination, err = s.client.ReadDestination(nil, getDestination)
			s.Nil(err)
			s.Equal(updatedDestination.GetDestinationUUID(), loadedDestination.GetDestinationUUID())
			s.Equal(updatedDestination.GetPath(), loadedDestination.GetPath())
			s.Equal(updatedDestination.GetType(), loadedDestination.GetType())
			s.Equal(updatedDestination.GetConsumedMessagesRetention(), loadedDestination.GetConsumedMessagesRetention())
			s.Equal(updatedDestination.GetUnconsumedMessagesRetention(), loadedDestination.GetUnconsumedMessagesRetention())
			s.Equal(updatedDestination.GetStatus(), loadedDestination.GetStatus())
			s.Equal(updatedDestination.GetOwnerEmail(), loadedDestination.GetOwnerEmail())
			s.Equal(updatedDestination.GetChecksumOption(), loadedDestination.GetChecksumOption())
			s.Equal(updatedDestination.GetIsMultiZone(), loadedDestination.GetIsMultiZone())
			s.Equal(len(updatedDestination.GetZoneConfigs()), len(loadedDestination.GetZoneConfigs()))
			s.Equal(updatedDestination.GetZoneConfigs()[0].GetZone(), loadedDestination.GetZoneConfigs()[0].GetZone())
			s.Equal(updatedDestination.GetZoneConfigs()[0].GetAllowConsume(), loadedDestination.GetZoneConfigs()[0].GetAllowConsume())
			s.Equal(updatedDestination.GetZoneConfigs()[0].GetAllowPublish(), loadedDestination.GetZoneConfigs()[0].GetAllowPublish())
			s.Equal(updatedDestination.GetZoneConfigs()[0].GetRemoteExtentReplicaNum(), loadedDestination.GetZoneConfigs()[0].GetRemoteExtentReplicaNum())
			s.Equal(updatedDestination.GetDLQConsumerGroupUUID(), loadedDestination.GetDLQConsumerGroupUUID())
			s.Equal(updatedDestination.GetDLQPurgeBefore(), loadedDestination.GetDLQPurgeBefore())
			s.Equal(updatedDestination.GetDLQMergeBefore(), loadedDestination.GetDLQMergeBefore())
			s.Equal(updatedDestination.KafkaCluster, loadedDestination.KafkaCluster)
			s.assertStringUnorderedArrayEqual(updatedDestination.KafkaTopics, loadedDestination.KafkaTopics, `KafkaTopics should match`)

			// Alter the update for the 2nd pass
			updateDestinationDLQCursors.DLQMergeBefore = common.Int64Ptr(now + int64(time.Hour))
			updateDestinationDLQCursors.DLQPurgeBefore = nil

			// Update the original destination with new values
			destination.DLQPurgeBefore = updatedDestination.DLQPurgeBefore
			destination.DLQMergeBefore = updatedDestination.DLQMergeBefore
		}

		// ALTER TABLE test
		if pass == 0 {
			s.Nil(s.Alter(), "ALTER table failed")
			destinationAlter = cDest(s.generateName(`foo2/bar2`), true)
		}
	}

	// Delete
	dDest := func(dest *shared.DestinationDescription) {
		deleteDestination := &shared.DeleteDestinationRequest{Path: common.StringPtr(dest.GetPath())}
		err := s.client.DeleteDestination(nil, deleteDestination)
		s.Nil(err)
		// Read by UUID might return deleted destination with DELETED status
		getDestination := &shared.ReadDestinationRequest{
			DestinationUUID: common.StringPtr(dest.GetDestinationUUID()),
		}
		deletedDestination, err := s.client.ReadDestination(nil, getDestination)
		s.Nil(err)
		s.True(deletedDestination.GetStatus() == shared.DestinationStatus_DELETING, fmt.Sprintf("%v", deletedDestination))
		// Read by path shouldn't return deleted destination
		getDestination = &shared.ReadDestinationRequest{
			Path: common.StringPtr(dest.GetPath()),
		}
		deletedDestination, err = s.client.ReadDestination(nil, getDestination)
		s.Error(err)
		s.IsType(&shared.EntityNotExistsError{}, err)
		s.Nil(deletedDestination)

		dReq := &m.DeleteDestinationUUIDRequest{UUID: common.StringPtr(dest.GetDestinationUUID())}
		err = s.client.DeleteDestinationUUID(nil, dReq)
		s.Nil(err)

		// Read by UUID might return deleted destination with DELETED status
		getDestination = &shared.ReadDestinationRequest{
			DestinationUUID: common.StringPtr(dest.GetDestinationUUID()),
		}
		deletedDestination, err = s.client.ReadDestination(nil, getDestination)
		s.True(deletedDestination == nil ||
			deletedDestination.GetStatus() == shared.DestinationStatus_DELETED, fmt.Sprintf("%v", deletedDestination))
	}

	dDest(destination)
	dDest(destinationAlter)
}

func (s *CassandraSuite) TestListDestinations() {
	zoneConfig1 := &shared.DestinationZoneConfig{
		Zone:                   common.StringPtr(`test1`),
		AllowConsume:           common.BoolPtr(true),
		RemoteExtentReplicaNum: common.Int32Ptr(1),
	}
	zoneConfig2 := &shared.DestinationZoneConfig{
		Zone:                   common.StringPtr(`test2`),
		AllowPublish:           common.BoolPtr(true),
		RemoteExtentReplicaNum: common.Int32Ptr(2),
	}

	count := 20
	// Create baz-# destinations
	for i := 0; i < count; i++ {
		createDestination := &shared.CreateDestinationRequest{
			Path: common.StringPtr(s.generateName(fmt.Sprintf("foolist/baz-%v", i))),
			Type: common.InternalDestinationTypePtr(shared.DestinationType_PLAIN),
			ConsumedMessagesRetention:   common.Int32Ptr(50),
			UnconsumedMessagesRetention: common.Int32Ptr(100),
			OwnerEmail:                  common.StringPtr(destinationOwnerEmail),
			ChecksumOption:              common.InternalChecksumOptionPtr(0),
			IsMultiZone:                 common.BoolPtr(false),
			ZoneConfigs:                 []*shared.DestinationZoneConfig{zoneConfig1},
		}
		_, err := s.client.CreateDestination(nil, createDestination)
		s.Nil(err)
	}
	// Create bar-# destinations
	for i := 0; i < count; i++ {
		createDestination := &shared.CreateDestinationRequest{
			Path: common.StringPtr(s.generateName(fmt.Sprintf("foolist/bar-%v", i))),
			Type: common.InternalDestinationTypePtr(shared.DestinationType_PLAIN),
			ConsumedMessagesRetention:   common.Int32Ptr(800),
			UnconsumedMessagesRetention: common.Int32Ptr(1600),
			OwnerEmail:                  common.StringPtr(destinationOwnerEmail),
			ChecksumOption:              common.InternalChecksumOptionPtr(0),
			IsMultiZone:                 common.BoolPtr(true),
			ZoneConfigs:                 []*shared.DestinationZoneConfig{zoneConfig2},
		}

		if i == count/2 {
			s.Nil(s.Alter(), "ALTER table failed")
		}

		_, err := s.client.CreateDestination(nil, createDestination)
		s.Nil(err)
	}
	// ListDestinations
	listDestinations := &shared.ListDestinationsRequest{
		Prefix: common.StringPtr("foolist/bar"),
		Limit:  common.Int64Ptr(testPageSize),
	}
	var result []*shared.DestinationDescription
	for {
		listResult, err := s.client.ListDestinations(nil, listDestinations)
		s.Nil(err)
		result = append(result, listResult.GetDestinations()...)
		if len(listResult.GetNextPageToken()) == 0 {
			break
		} else {
			listDestinations.PageToken = listResult.GetNextPageToken()
		}
	}
	s.Equal(count, len(result))
	for _, dest := range result {
		s.True(strings.HasPrefix(dest.GetPath(), "foolist/bar"))

		s.Equal(shared.DestinationType_PLAIN, dest.GetType())
		s.Equal(int32(800), dest.GetConsumedMessagesRetention())
		s.Equal(int32(1600), dest.GetUnconsumedMessagesRetention())
		s.Equal(shared.DestinationStatus_ENABLED, dest.GetStatus())
		s.Equal(true, dest.GetIsMultiZone())
		s.Equal(1, len(dest.GetZoneConfigs()))
		s.Equal(zoneConfig2.GetZone(), dest.GetZoneConfigs()[0].GetZone())
		s.Equal(zoneConfig2.GetAllowConsume(), dest.GetZoneConfigs()[0].GetAllowConsume())
		s.Equal(zoneConfig2.GetRemoteExtentReplicaNum(), dest.GetZoneConfigs()[0].GetRemoteExtentReplicaNum())
	}

	// list multi zone only
	listDestinations = &shared.ListDestinationsRequest{
		Prefix:        common.StringPtr("foolist"),
		MultiZoneOnly: common.BoolPtr(true),
		Limit:         common.Int64Ptr(testPageSize),
	}
	result = nil
	for {
		listResult, err := s.client.ListDestinations(nil, listDestinations)
		s.Nil(err)
		result = append(result, listResult.GetDestinations()...)
		if len(listResult.GetNextPageToken()) == 0 {
			break
		} else {
			listDestinations.PageToken = listResult.GetNextPageToken()
		}
	}
	s.Equal(count, len(result))
	for _, dest := range result {
		s.True(strings.HasPrefix(dest.GetPath(), "foolist/bar"))

		s.Equal(shared.DestinationType_PLAIN, dest.GetType())
		s.Equal(int32(800), dest.GetConsumedMessagesRetention())
		s.Equal(int32(1600), dest.GetUnconsumedMessagesRetention())
		s.Equal(shared.DestinationStatus_ENABLED, dest.GetStatus())
		s.Equal(true, dest.GetIsMultiZone())
		s.Equal(1, len(dest.GetZoneConfigs()))
		s.Equal(zoneConfig2.GetZone(), dest.GetZoneConfigs()[0].GetZone())
		s.Equal(zoneConfig2.GetAllowConsume(), dest.GetZoneConfigs()[0].GetAllowConsume())
		s.Equal(zoneConfig2.GetRemoteExtentReplicaNum(), dest.GetZoneConfigs()[0].GetRemoteExtentReplicaNum())
	}

	// Unexisting prefix should return empty list
	listDestinations = &shared.ListDestinationsRequest{
		Prefix: common.StringPtr("moo/"),
		Limit:  common.Int64Ptr(testPageSize),
	}
	listResult, err := s.client.ListDestinations(nil, listDestinations)
	s.Nil(err)
	s.Equal(0, len(listResult.GetDestinations()))
}

func (s *CassandraSuite) TestListDestinationsByUUID() {
	destPrefix := "fool/bar"
	count := 20
	zoneConfig := &shared.DestinationZoneConfig{
		Zone:                   common.StringPtr(`test1`),
		AllowConsume:           common.BoolPtr(true),
		RemoteExtentReplicaNum: common.Int32Ptr(1),
	}

	// Create destinations (half will be multi_zone dest)
	for i := 0; i < count; i++ {
		zoneConfigs := []*shared.DestinationZoneConfig{zoneConfig}
		isMultiZone := true
		if i%2 == 0 {
			zoneConfigs = []*shared.DestinationZoneConfig{}
			isMultiZone = false
		}

		createDestination := &shared.CreateDestinationRequest{
			Path: common.StringPtr(s.generateName(fmt.Sprintf("%v-%v", destPrefix, i))),
			Type: common.InternalDestinationTypePtr(shared.DestinationType_PLAIN),
			ConsumedMessagesRetention:   common.Int32Ptr(800),
			UnconsumedMessagesRetention: common.Int32Ptr(1600),
			OwnerEmail:                  common.StringPtr(destinationOwnerEmail),
			ChecksumOption:              common.InternalChecksumOptionPtr(0),
			IsMultiZone:                 common.BoolPtr(isMultiZone),
			ZoneConfigs:                 zoneConfigs,
		}

		if i == count/2 {
			s.Nil(s.Alter(), "ALTER table failed")
		}

		_, err := s.client.CreateDestination(nil, createDestination)
		s.Nil(err)
	}

	// ListDestinationsByUUID
	listDestinations := &shared.ListDestinationsByUUIDRequest{
		Limit: common.Int64Ptr(testPageSize),
		ValidateAgainstPathTable: common.BoolPtr(true),
	}
	var result []*shared.DestinationDescription
	for {
		listResult, err := s.client.ListDestinationsByUUID(nil, listDestinations)
		s.Nil(err)
		for _, d := range listResult.GetDestinations() {
			if strings.HasPrefix(d.GetPath(), destPrefix) {
				result = append(result, d)
			}
		}
		if len(listResult.GetNextPageToken()) == 0 {
			break
		} else {
			listDestinations.PageToken = listResult.GetNextPageToken()
		}
	}
	s.Equal(count, len(result))
	for _, dest := range result {
		s.Equal(shared.DestinationType_PLAIN, dest.GetType())
		s.Equal(int32(800), dest.GetConsumedMessagesRetention())
		s.Equal(int32(1600), dest.GetUnconsumedMessagesRetention())
		s.Equal(shared.DestinationStatus_ENABLED, dest.GetStatus())
	}

	// list multi zone only
	listDestinations = &shared.ListDestinationsByUUIDRequest{
		MultiZoneOnly:            common.BoolPtr(true),
		ValidateAgainstPathTable: common.BoolPtr(true),
		Limit: common.Int64Ptr(testPageSize),
	}
	result = nil
	for {
		listResult, err := s.client.ListDestinationsByUUID(nil, listDestinations)
		s.Nil(err)
		for _, d := range listResult.GetDestinations() {
			if strings.HasPrefix(d.GetPath(), destPrefix) {
				result = append(result, d)
			}
		}
		if len(listResult.GetNextPageToken()) == 0 {
			break
		} else {
			listDestinations.PageToken = listResult.GetNextPageToken()
		}
	}
	s.Equal(count/2, len(result))
	for _, dest := range result {
		s.Equal(shared.DestinationType_PLAIN, dest.GetType())
		s.Equal(int32(800), dest.GetConsumedMessagesRetention())
		s.Equal(int32(1600), dest.GetUnconsumedMessagesRetention())
		s.Equal(shared.DestinationStatus_ENABLED, dest.GetStatus())
		s.Equal(true, dest.GetIsMultiZone())
		s.Equal(1, len(dest.GetZoneConfigs()))
		s.Equal(zoneConfig.GetZone(), dest.GetZoneConfigs()[0].GetZone())
		s.Equal(zoneConfig.GetAllowConsume(), dest.GetZoneConfigs()[0].GetAllowConsume())
		s.Equal(zoneConfig.GetRemoteExtentReplicaNum(), dest.GetZoneConfigs()[0].GetRemoteExtentReplicaNum())
	}
}

func (s *CassandraSuite) TestExtentCRU() {
	// Create
	var destinations [3]*shared.DestinationDescription
	var extents [3]*shared.ExtentStats

	cDest := func(path string) *shared.DestinationDescription {
		createDestination := &shared.CreateDestinationRequest{
			Path: common.StringPtr(path),
			Type: common.InternalDestinationTypePtr(shared.DestinationType_PLAIN),
			ConsumedMessagesRetention:   common.Int32Ptr(1800),
			UnconsumedMessagesRetention: common.Int32Ptr(3600),
			OwnerEmail:                  common.StringPtr(destinationOwnerEmail),
			ChecksumOption:              common.InternalChecksumOptionPtr(0),
		}
		destination, err := s.client.CreateDestination(nil, createDestination)
		s.Nil(err)
		return destination
	}

	cExtent := func(dest *shared.DestinationDescription) *shared.ExtentStats {
		extentUUID := uuid.New()
		storeIds := []string{uuid.New(), uuid.New(), uuid.New()}
		extent := &shared.Extent{
			ExtentUUID:               common.StringPtr(extentUUID),
			DestinationUUID:          common.StringPtr(dest.GetDestinationUUID()),
			StoreUUIDs:               storeIds,
			InputHostUUID:            common.StringPtr(uuid.New()),
			RemoteExtentPrimaryStore: common.StringPtr(uuid.New()),
		}
		createExtent := &shared.CreateExtentRequest{Extent: extent}
		t0 := time.Now().UnixNano() / int64(time.Millisecond)
		createExtentResult, err := s.client.CreateExtent(nil, createExtent)
		tX := time.Now().UnixNano() / int64(time.Millisecond)

		s.Nil(err)
		s.True(reflect.DeepEqual(extent, createExtentResult.GetExtentStats().GetExtent()))
		s.True(createExtentResult.GetExtentStats().GetStatusUpdatedTimeMillis() >= t0)
		s.True(createExtentResult.GetExtentStats().GetStatusUpdatedTimeMillis() <= tX)
		return createExtentResult.GetExtentStats()
	}

	// Can't test all original/altered destination/extent combinations
	// Pass 0: original destination, original extent
	// Pass 1: original destination, altered extent
	// Pass 2: altered destination, altered extent
	// altered destination, original extent is possible (i.e. update destination after both code and schema are altered), but difficult to test

	for pass := 0; pass < 3; pass++ {
		switch pass {
		case 0:
			destinations[pass] = cDest(s.generateName(`original/original`))
		case 1:
			destinations[pass] = cDest(s.generateName(`original/altered`))
		case 2:
			destinations[pass] = cDest(s.generateName(`altered/altered`))
		}

		switch pass {
		case 0:
			extents[pass] = cExtent(destinations[pass])
		case 1:
			s.Nil(s.Alter(), "ALTER table failed")
			extents[pass] = cExtent(destinations[pass])
		case 2:
			extents[pass] = cExtent(destinations[pass])
		}
	}

	for pass := 0; pass < 3; pass++ {
		extentStatsOrig := extents[pass]
		extent := extentStatsOrig.GetExtent()

		readExtentStats := &m.ReadExtentStatsRequest{DestinationUUID: extent.DestinationUUID, ExtentUUID: extent.ExtentUUID}
		extentStats, err := s.client.ReadExtentStats(nil, readExtentStats)
		s.Nil(err)
		s.NotNil(extentStats)

		s.Equal(extentStatsOrig.GetArchivalLocation(), extentStats.GetExtentStats().GetArchivalLocation())
		s.Equal(extentStatsOrig.GetCreatedTimeMillis(), extentStats.GetExtentStats().GetCreatedTimeMillis())
		s.Equal(extentStatsOrig.GetStatus(), extentStats.GetExtentStats().GetStatus())
		s.Equal(extentStatsOrig.GetStatusUpdatedTimeMillis(), extentStats.GetExtentStats().GetStatusUpdatedTimeMillis())

		updateExtent := &m.UpdateExtentStatsRequest{
			DestinationUUID:          common.StringPtr(extent.GetDestinationUUID()),
			ExtentUUID:               common.StringPtr(extent.GetExtentUUID()),
			Status:                   common.MetadataExtentStatusPtr(shared.ExtentStatus_ARCHIVED),
			ArchivalLocation:         common.StringPtr("S3:foo/bar"),
			RemoteExtentPrimaryStore: common.StringPtr(uuid.New()),
		}

		t0 := time.Now().UnixNano() / int64(time.Millisecond)
		updateResult, err := s.client.UpdateExtentStats(nil, updateExtent)
		tX := time.Now().UnixNano() / int64(time.Millisecond)

		s.Nil(err)
		s.Equal(updateExtent.Status, updateResult.ExtentStats.Status)
		s.Equal(updateExtent.GetArchivalLocation(), updateResult.GetExtentStats().GetArchivalLocation())
		s.Equal(updateExtent.GetRemoteExtentPrimaryStore(), updateResult.GetExtentStats().GetExtent().GetRemoteExtentPrimaryStore())

		readExtentStats = &m.ReadExtentStatsRequest{DestinationUUID: extent.DestinationUUID, ExtentUUID: extent.ExtentUUID}
		time.Sleep(1 * time.Second)
		extentStats, err = s.client.ReadExtentStats(nil, readExtentStats)
		s.Nil(err)
		s.NotNil(extentStats)
		s.Equal(shared.ExtentStatus_ARCHIVED, extentStats.GetExtentStats().GetStatus())
		s.Equal(updateExtent.GetArchivalLocation(), extentStats.GetExtentStats().GetArchivalLocation())
		s.Equal(updateExtent.GetRemoteExtentPrimaryStore(), extentStats.GetExtentStats().GetExtent().GetRemoteExtentPrimaryStore())
		s.True(extentStats.GetExtentStats().GetStatusUpdatedTimeMillis() >= t0)
		s.True(extentStats.GetExtentStats().GetStatusUpdatedTimeMillis() <= tX)
	}
}

func (s *CassandraSuite) TestCreateExtentWithCgVisibility() {
	extentUUID := uuid.New()
	destUUID := uuid.New()
	cgUUID := uuid.New()
	storeIds := []string{uuid.New(), uuid.New(), uuid.New()}
	extent := &shared.Extent{
		ExtentUUID:      common.StringPtr(extentUUID),
		DestinationUUID: common.StringPtr(destUUID),
		StoreUUIDs:      storeIds,
		InputHostUUID:   common.StringPtr(uuid.New()),
	}
	createRequest := &shared.CreateExtentRequest{
		Extent:                  extent,
		ConsumerGroupVisibility: common.StringPtr(cgUUID),
	}
	_, err := s.client.CreateExtent(nil, createRequest)
	s.Nil(err)

	readExtentStats := &m.ReadExtentStatsRequest{
		DestinationUUID: common.StringPtr(destUUID),
		ExtentUUID:      common.StringPtr(extentUUID),
	}
	extentStats, err := s.client.ReadExtentStats(nil, readExtentStats)
	s.Nil(err)
	s.NotNil(extentStats)
	s.Equal(shared.ExtentStatus_OPEN, extentStats.GetExtentStats().GetStatus())
	s.Equal(cgUUID, extentStats.GetExtentStats().GetConsumerGroupVisibility())
}

func (s *CassandraSuite) TestUpdateStoreExtentReplicaStats() {
	extentUUID := uuid.New()
	destUUID := uuid.New()
	storeIds := []string{uuid.New(), uuid.New(), uuid.New()}
	extent := &shared.Extent{
		ExtentUUID:      common.StringPtr(extentUUID),
		DestinationUUID: common.StringPtr(destUUID),
		StoreUUIDs:      storeIds,
		InputHostUUID:   common.StringPtr(uuid.New()),
	}
	createRequest := &shared.CreateExtentRequest{Extent: extent}
	_, err := s.client.CreateExtent(nil, createRequest)
	s.Nil(err)

	beginEnqTime := time.Now().Add(-time.Hour)
	lastEnqTime := time.Now().Add(-time.Second)

	beginTime := time.Now().Add(-time.Minute)
	endTime := time.Now().Add(time.Millisecond)

	var beginSeq, lastSeq, availSeq int64 = 0x123, 0x456789, 0x345678
	var lastSeqRate, availSeqRate float64 = 23.45, 67.89
	var beginAddr, lastAddr int64 = 0x123456789ABCDE, 0xABCDEF012345678
	var sizeInBytes int64 = 0x456123789
	var sizeInBytesRate = 987.67

	stats1 := &shared.ExtentReplicaStats{
		ExtentUUID:            common.StringPtr(extentUUID),
		StoreUUID:             common.StringPtr(storeIds[1]),
		BeginAddress:          common.Int64Ptr(beginAddr),
		LastAddress:           common.Int64Ptr(lastAddr),
		BeginSequence:         common.Int64Ptr(beginSeq),
		LastSequence:          common.Int64Ptr(lastSeq),
		LastSequenceRate:      common.Float64Ptr(lastSeqRate),
		AvailableSequence:     common.Int64Ptr(availSeq),
		AvailableSequenceRate: common.Float64Ptr(availSeqRate),
		BeginEnqueueTimeUtc:   common.Int64Ptr(beginEnqTime.UnixNano()),
		LastEnqueueTimeUtc:    common.Int64Ptr(lastEnqTime.UnixNano()),
		SizeInBytes:           common.Int64Ptr(sizeInBytes),
		SizeInBytesRate:       common.Float64Ptr(sizeInBytesRate),
		Status:                common.MetadataExtentReplicaStatusPtr(shared.ExtentReplicaStatus_SEALED),
		BeginTime:             common.Int64Ptr(beginTime.UnixNano()),
		EndTime:               common.Int64Ptr(endTime.UnixNano()),
	}

	// update replica stats
	updateRequest := &m.UpdateStoreExtentReplicaStatsRequest{
		StoreUUID:    common.StringPtr(storeIds[1]),
		ExtentUUID:   common.StringPtr(extentUUID),
		ReplicaStats: []*shared.ExtentReplicaStats{stats1},
	}
	err = s.client.UpdateStoreExtentReplicaStats(nil, updateRequest)
	s.Nil(err)

	readReq := &m.ReadStoreExtentReplicaStatsRequest{
		StoreUUID:  common.StringPtr(storeIds[1]),
		ExtentUUID: common.StringPtr(extent.GetExtentUUID()),
	}

	result, err := s.client.ReadStoreExtentReplicaStats(nil, readReq)
	s.Nil(err, "Reading store extent stats failed")
	s.NotNil(result, "ReadStoreExtentReplicaStats returned nil")

	stats := result.GetExtent().GetReplicaStats()

	s.Equal(stats1.GetExtentUUID(), stats[0].GetExtentUUID())
	s.Equal(stats1.GetStoreUUID(), stats[0].GetStoreUUID())
	s.Equal(stats1.GetBeginAddress(), stats[0].GetBeginAddress())
	s.Equal(stats1.GetLastAddress(), stats[0].GetLastAddress())
	s.Equal(stats1.GetBeginSequence(), stats[0].GetBeginSequence())
	s.Equal(stats1.GetLastSequence(), stats[0].GetLastSequence())
	s.Equal(stats1.GetLastSequenceRate(), stats[0].GetLastSequenceRate())
	s.Equal(stats1.GetAvailableSequence(), stats[0].GetAvailableSequence())
	s.Equal(stats1.GetAvailableSequenceRate(), stats[0].GetAvailableSequenceRate())
	s.Equal(stats1.GetSizeInBytes(), stats[0].GetSizeInBytes())
	s.Equal(stats1.GetSizeInBytesRate(), stats[0].GetSizeInBytesRate())
	s.Equal(stats1.GetStatus(), stats[0].GetStatus())

	// after gocql marshals and unmarshals the timestamps, the values are off by a little, but not much!
	timeDifference := func(t0, t1 int64) time.Duration {
		if t0 > t1 {
			return time.Unix(0, t0).Sub(time.Unix(0, t1))
		}
		return time.Unix(0, t1).Sub(time.Unix(0, t0))
	}
	s.True(timeDifference(stats1.GetBeginEnqueueTimeUtc(), stats[0].GetBeginEnqueueTimeUtc()) < time.Millisecond)
	s.True(timeDifference(stats1.GetLastEnqueueTimeUtc(), stats[0].GetLastEnqueueTimeUtc()) < time.Millisecond)
	s.True(timeDifference(stats1.GetBeginTime(), stats[0].GetBeginTime()) < time.Millisecond)
	s.True(timeDifference(stats1.GetEndTime(), stats[0].GetEndTime()) < time.Millisecond)
}

func (s *CassandraSuite) TestReplicationStatus() {
	extentUUID := uuid.New()
	destUUID := uuid.New()
	storeIds := []string{uuid.New(), uuid.New(), uuid.New()}
	extent := &shared.Extent{
		ExtentUUID:      common.StringPtr(extentUUID),
		DestinationUUID: common.StringPtr(destUUID),
		StoreUUIDs:      storeIds,
		InputHostUUID:   common.StringPtr(uuid.New()),
	}
	createRequest := &shared.CreateExtentRequest{Extent: extent}
	_, err := s.client.CreateExtent(nil, createRequest)
	s.Nil(err)

	// read store extent with filtering
	readRequest := &m.ListStoreExtentsStatsRequest{
		StoreUUID:         common.StringPtr(storeIds[0]),
		ReplicationStatus: common.InternalExtentReplicaReplicationStatusTypePtr(shared.ExtentReplicaReplicationStatus_INVALID),
	}
	readResult, err := s.client.ListStoreExtentsStats(nil, readRequest)
	s.Nil(err)
	s.Equal(1, len(readResult.GetExtentStatsList()))

	// update replication status
	updateRequest := &m.UpdateStoreExtentReplicaStatsRequest{
		StoreUUID:         common.StringPtr(storeIds[0]),
		ExtentUUID:        common.StringPtr(extentUUID),
		ReplicationStatus: common.InternalExtentReplicaReplicationStatusTypePtr(shared.ExtentReplicaReplicationStatus_DONE),
	}
	err = s.client.UpdateStoreExtentReplicaStats(nil, updateRequest)
	s.Nil(err)

	// read again with filtering, expect no result is returned
	readResult, err = s.client.ListStoreExtentsStats(nil, readRequest)
	s.Nil(err)
	s.Equal(0, len(readResult.GetExtentStatsList()))
}

func (s *CassandraSuite) TestStoreExtentTimestamps() {
	extentUUID := uuid.New()
	destUUID := uuid.New()
	storeIds := []string{uuid.New(), uuid.New(), uuid.New()}
	extent := &shared.Extent{
		ExtentUUID:      common.StringPtr(extentUUID),
		DestinationUUID: common.StringPtr(destUUID),
		StoreUUIDs:      storeIds,
		InputHostUUID:   common.StringPtr(uuid.New()),
	}
	createRequest := &shared.CreateExtentRequest{Extent: extent}
	_, err := s.client.CreateExtent(nil, createRequest)
	s.Nil(err)

	// read store extent with filtering
	readRequest := &m.ListStoreExtentsStatsRequest{
		StoreUUID:         common.StringPtr(storeIds[0]),
		ReplicationStatus: common.InternalExtentReplicaReplicationStatusTypePtr(shared.ExtentReplicaReplicationStatus_INVALID),
	}
	readResult, err := s.client.ListStoreExtentsStats(nil, readRequest)
	s.Nil(err)
	s.Equal(1, len(readResult.GetExtentStatsList()))

	// update replication status
	updateRequest := &m.UpdateStoreExtentReplicaStatsRequest{
		StoreUUID:         common.StringPtr(storeIds[0]),
		ExtentUUID:        common.StringPtr(extentUUID),
		ReplicationStatus: common.InternalExtentReplicaReplicationStatusTypePtr(shared.ExtentReplicaReplicationStatus_DONE),
	}
	err = s.client.UpdateStoreExtentReplicaStats(nil, updateRequest)
	s.Nil(err)

	// read again with filtering, expect no result is returned
	readResult, err = s.client.ListStoreExtentsStats(nil, readRequest)
	s.Nil(err)
	s.Equal(0, len(readResult.GetExtentStatsList()))
}

func (s *CassandraSuite) TestReplicationStatus_Remote() {
	extentUUID := uuid.New()
	destUUID := uuid.New()
	storeIds := []string{uuid.New(), uuid.New(), uuid.New()}
	extent := &shared.Extent{
		ExtentUUID:      common.StringPtr(extentUUID),
		DestinationUUID: common.StringPtr(destUUID),
		StoreUUIDs:      storeIds,
		InputHostUUID:   common.StringPtr(uuid.New()),
		OriginZone:      common.StringPtr(`zone1`),
	}
	createRequest := &shared.CreateExtentRequest{Extent: extent}
	_, err := s.client.CreateExtent(nil, createRequest)
	s.Nil(err)

	// read store extent with filtering (ExtentReplicaReplicationStatus_PENDING)
	readRequest := &m.ListStoreExtentsStatsRequest{
		StoreUUID:         common.StringPtr(storeIds[0]),
		ReplicationStatus: common.InternalExtentReplicaReplicationStatusTypePtr(shared.ExtentReplicaReplicationStatus_PENDING),
	}
	readResult, err := s.client.ListStoreExtentsStats(nil, readRequest)
	s.Nil(err)
	s.Equal(1, len(readResult.GetExtentStatsList()))

	// update replication status
	updateRequest := &m.UpdateStoreExtentReplicaStatsRequest{
		StoreUUID:         common.StringPtr(storeIds[0]),
		ExtentUUID:        common.StringPtr(extentUUID),
		ReplicationStatus: common.InternalExtentReplicaReplicationStatusTypePtr(shared.ExtentReplicaReplicationStatus_DONE),
	}
	err = s.client.UpdateStoreExtentReplicaStats(nil, updateRequest)
	s.Nil(err)

	// read again with filtering, expect no result is returned
	readResult, err = s.client.ListStoreExtentsStats(nil, readRequest)
	s.Nil(err)
	s.Equal(0, len(readResult.GetExtentStatsList()))
}

func (s *CassandraSuite) TestMoveExtent() {
	var err error
	var (
		normal   = 0
		dlq      = 1
		moved    = 1
		static   = 0
		destPath = s.generateName(`/foo/bar`)
		cgPath   = s.generateName(`/foo.bar/consumer`)
	)
	assert := s.Require()

	var destinations [2]*shared.DestinationDescription
	var extents [2]*shared.ExtentStats
	var extentReplicaStats [2][]*shared.ExtentReplicaStats

	destinations[normal], err = createDestination(s, destPath, false)
	assert.Nil(err, "CreateDestination failed")

	createReq := &shared.CreateConsumerGroupRequest{
		DestinationPath:          common.StringPtr(destPath),
		ConsumerGroupName:        common.StringPtr(cgPath),
		StartFrom:                common.Int64Ptr(30),
		LockTimeoutSeconds:       common.Int32Ptr(10),
		MaxDeliveryCount:         common.Int32Ptr(5),
		SkipOlderMessagesSeconds: common.Int32Ptr(60),
		DelaySeconds:             common.Int32Ptr(60),
		OwnerEmail:               common.StringPtr("consumer_test@uber.com"),
	}

	gotCG, err := s.client.CreateConsumerGroup(nil, createReq)
	assert.Nil(err, "CreateConsumerGroup failed")
	assert.Equal(shared.ConsumerGroupStatus_ENABLED, gotCG.GetStatus(), "Wrong CG status")

	readDlqDstReq := &shared.ReadDestinationRequest{
		Path: common.StringPtr(gotCG.GetDeadLetterQueueDestinationUUID()),
	}
	destinations[dlq], err = s.client.ReadDestination(nil, readDlqDstReq)
	assert.Nil(err, "ReadDestination failed for DLQ")

	cExtent := func(dest *shared.DestinationDescription) *shared.ExtentStats {
		extentUUID := uuid.New()
		storeIds := []string{uuid.New(), uuid.New(), uuid.New()}
		extent := &shared.Extent{
			ExtentUUID:      common.StringPtr(extentUUID),
			DestinationUUID: common.StringPtr(dest.GetDestinationUUID()),
			StoreUUIDs:      storeIds,
			InputHostUUID:   common.StringPtr(uuid.New()),
		}
		createExtent := &shared.CreateExtentRequest{Extent: extent}
		createExtentResult, err := s.client.CreateExtent(nil, createExtent)
		s.Nil(err)
		s.True(reflect.DeepEqual(extent, createExtentResult.GetExtentStats().GetExtent()))
		return createExtentResult.GetExtentStats()
	}

	for i := 0; i < 2; i++ {
		extents[i] = cExtent(destinations[dlq])

		// Extent must be sealed to be moved
		sealReq := m.NewSealExtentRequest()
		sealReq.DestinationUUID = extents[i].GetExtent().DestinationUUID
		sealReq.ExtentUUID = extents[i].GetExtent().ExtentUUID

		err = s.client.SealExtent(nil, sealReq)
		s.Nil(err)
		extents[i].Status = common.MetadataExtentStatusPtr(shared.ExtentStatus_SEALED)

		readExtentStats := &m.ReadExtentStatsRequest{
			DestinationUUID: extents[i].GetExtent().DestinationUUID,
			ExtentUUID:      extents[i].GetExtent().ExtentUUID}
		extentStats, err := s.client.ReadExtentStats(nil, readExtentStats)
		s.Nil(err)
		s.NotNil(extentStats)

		s.Equal(extents[i].GetArchivalLocation(), extentStats.GetExtentStats().GetArchivalLocation())
		s.Equal(extents[i].GetCreatedTimeMillis(), extentStats.GetExtentStats().GetCreatedTimeMillis())
		s.Equal(extents[i].GetStatus(), extentStats.GetExtentStats().GetStatus())
		s.Equal(extents[i].GetConsumerGroupVisibility(), extentStats.GetExtentStats().GetConsumerGroupVisibility())

		stores := extentStats.GetExtentStats().GetExtent().GetStoreUUIDs()
		s.NotEqual(0, len(stores), `We should have some assigned stores`)

		// Assign some non-zero values on the store extent replica stats
		rsersReq := m.NewReadStoreExtentReplicaStatsRequest()
		rsersReq.ExtentUUID = extents[i].GetExtent().ExtentUUID
		usersReq := m.NewUpdateStoreExtentReplicaStatsRequest()
		usersReq.ExtentUUID = extents[i].GetExtent().ExtentUUID

		for _, st := range stores {

			rsersReq.StoreUUID = common.StringPtr(st)

			sers, err2 := s.client.ReadStoreExtentReplicaStats(nil, rsersReq)
			s.NoError(err2)
			s.NotNil(sers)

			usersReq.ReplicaStats = sers.GetExtent().GetReplicaStats()
			usersReq.ReplicaStats[0].AvailableSequence = common.Int64Ptr(42)
			usersReq.ReplicaStats[0].BeginSequence = common.Int64Ptr(21)
			usersReq.ReplicaStats[0].LastSequence = common.Int64Ptr(84)
			err2 = s.client.UpdateStoreExtentReplicaStats(nil, usersReq)
			s.NoError(err2)
			extentReplicaStats[i] = append(extentReplicaStats[i], usersReq.ReplicaStats[0])
		}
	}

	mReq := m.NewMoveExtentRequest()
	mReq.DestinationUUID = common.StringPtr(destinations[dlq].GetDestinationUUID())
	mReq.ExtentUUID = common.StringPtr(extents[moved].GetExtent().GetExtentUUID())
	mReq.NewDestinationUUID_ = common.StringPtr(destinations[normal].GetDestinationUUID())
	mReq.ConsumerGroupVisibilityUUID = common.StringPtr(gotCG.GetConsumerGroupUUID())

	err = s.client.MoveExtent(nil, mReq)
	s.Nil(err)

	for i := 0; i < 2; i++ {
		readExtentStats := m.NewReadExtentStatsRequest()
		readReplicaStats := m.NewReadStoreExtentReplicaStatsRequest()
		readExtentStats.ExtentUUID = extents[i].GetExtent().ExtentUUID
		readReplicaStats.ExtentUUID = extents[i].GetExtent().ExtentUUID

		// Set the final destination for each extent
		switch i {
		case moved:
			readExtentStats.DestinationUUID = common.StringPtr(destinations[normal].GetDestinationUUID())
		case static:
			readExtentStats.DestinationUUID = common.StringPtr(destinations[dlq].GetDestinationUUID())
		}

		extentStats, err2 := s.client.ReadExtentStats(nil, readExtentStats)
		s.Nil(err2, `Final destination should succeed for both extents`)
		s.NotNil(extentStats)

		s.Equal(extents[i].GetArchivalLocation(), extentStats.GetExtentStats().GetArchivalLocation())
		s.InDelta(extents[i].GetCreatedTimeMillis(), extentStats.GetExtentStats().GetCreatedTimeMillis(), 100.0)
		s.Equal(extents[i].GetStatus(), extentStats.GetExtentStats().GetStatus(), `Status should not change`)

		stores := extentStats.GetExtentStats().GetExtent().GetStoreUUIDs()

		switch i {
		case moved:
			s.Equal(destinations[normal].GetDestinationUUID(), extentStats.GetExtentStats().GetExtent().GetDestinationUUID(), `Moved extent should have new destUUID everywhere`)
		case static:
			s.Equal(destinations[dlq].GetDestinationUUID(), extentStats.GetExtentStats().GetExtent().GetDestinationUUID(), `Static extent should have original destination UUID`)
		}

		// Set the opposite destination for both extents, dlq for moved; normal for static
		switch i {
		case moved:
			s.Equal(gotCG.GetConsumerGroupUUID(), extentStats.GetExtentStats().GetConsumerGroupVisibility(), `%v`, i)
			readExtentStats.DestinationUUID = common.StringPtr(destinations[dlq].GetDestinationUUID())
		case static:
			s.Equal(extents[i].GetConsumerGroupVisibility(), extentStats.GetExtentStats().GetConsumerGroupVisibility(), `%v`, i)
			readExtentStats.DestinationUUID = common.StringPtr(destinations[normal].GetDestinationUUID())
		}

		extentStats, err2 = s.client.ReadExtentStats(nil, readExtentStats)

		switch i {
		case moved:
			s.NoError(err2, `extent should exist, but be marked deleted`)
			s.NotNil(extentStats)
			s.Equal(shared.ExtentStatus_DELETED, extentStats.GetExtentStats().GetStatus())
			s.Nil(extentStats.GetExtentStats().ConsumerGroupVisibility)
		case static:
			s.Error(err2, `read should fail, extent was not moved`)
			s.IsType(&shared.InternalServiceError{}, err2)
			s.Nil(extentStats)
		}

		for _, st := range stores {
			readReplicaStats.StoreUUID = common.StringPtr(st)
			ers, err3 := s.client.ReadStoreExtentReplicaStats(nil, readReplicaStats)
			s.NoError(err3, `Extent replica stats should exist for all extents, store %v`, st)
			s.NotNil(ers, `%d %v`, i, st)
			s.Equal(shared.ExtentStatus_SEALED, ers.GetExtent().GetStatus(), `%d %v`, i, st)
			s.Equal(shared.ExtentReplicaStatus_OPEN, ers.GetExtent().GetReplicaStats()[0].GetStatus(), `%d %v`, i, st) // T476127 -- Should be sealed

			match := false
		extentReplicaStatsLoop:
			for _, x := range extentReplicaStats[i] {
				if x.GetStoreUUID() == ers.GetExtent().GetReplicaStats()[0].GetStoreUUID() {
					s.assertReplicaStatsEqual(x, ers.GetExtent().GetReplicaStats()[0], `MoveExtent shouldn't change replica stats %v %v`, i, st)
					match = true
					break extentReplicaStatsLoop
				}
			}
			s.True(match)
		}
	}

	// Test cgVisibility update

	// Set CGVisibility to nil (currently not-nil)
	readExtentStats := m.NewReadExtentStatsRequest()
	readExtentStats.DestinationUUID = common.StringPtr(mReq.GetNewDestinationUUID_())
	readExtentStats.ExtentUUID = common.StringPtr(mReq.GetExtentUUID())
	extentStats, err := s.client.ReadExtentStats(nil, readExtentStats)
	s.Nil(err)
	s.NotNil(extentStats)

	mReq.DestinationUUID = common.StringPtr(mReq.GetNewDestinationUUID_())
	mReq.ConsumerGroupVisibilityUUID = nil
	err = s.client.MoveExtent(nil, mReq)
	s.Nil(err)

	extentStats2, err := s.client.ReadExtentStats(nil, readExtentStats)
	s.Nil(err)
	s.NotNil(extentStats2)

	s.Equal(extentStats.GetExtentStats().GetArchivalLocation(), extentStats2.GetExtentStats().GetArchivalLocation())
	s.Equal(extentStats.GetExtentStats().GetCreatedTimeMillis(), extentStats2.GetExtentStats().GetCreatedTimeMillis())
	s.Equal(extentStats.GetExtentStats().GetStatus(), extentStats2.GetExtentStats().GetStatus())
	s.assertReplicaStatsArrayEqual(extentStats.GetExtentStats().GetReplicaStats(), extentStats2.GetExtentStats().GetReplicaStats())
	s.True(reflect.DeepEqual(extentStats.GetExtentStats().GetExtent(), extentStats2.GetExtentStats().GetExtent()))
	s.Nil(extentStats2.GetExtentStats().ConsumerGroupVisibility)

	// Set CGVisibility to not-nil (currently nil)
	mReq.ConsumerGroupVisibilityUUID = common.StringPtr(uuid.New())
	err = s.client.MoveExtent(nil, mReq)
	s.Nil(err)

	extentStats2, err = s.client.ReadExtentStats(nil, readExtentStats)
	s.Nil(err)
	s.NotNil(extentStats2)

	s.Equal(extentStats.GetExtentStats().GetArchivalLocation(), extentStats2.GetExtentStats().GetArchivalLocation())
	s.Equal(extentStats.GetExtentStats().GetCreatedTimeMillis(), extentStats2.GetExtentStats().GetCreatedTimeMillis())
	s.Equal(extentStats.GetExtentStats().GetStatus(), extentStats2.GetExtentStats().GetStatus())
	s.assertReplicaStatsArrayEqual(extentStats.GetExtentStats().GetReplicaStats(), extentStats2.GetExtentStats().GetReplicaStats())
	s.True(reflect.DeepEqual(extentStats.GetExtentStats().GetExtent(), extentStats2.GetExtentStats().GetExtent()))
	s.Equal(mReq.GetConsumerGroupVisibilityUUID(), extentStats2.GetExtentStats().GetConsumerGroupVisibility(), "%v", mReq.GetExtentUUID())
}

func (s *CassandraSuite) assertStringUnorderedArrayEqual(a, b []string, msgAndArgs ...interface{}) {
	s.NotNil(a, msgAndArgs)
	s.NotNil(b, msgAndArgs)
	s.Equal(len(a), len(b), msgAndArgs)

	for _, A := range a {
		match := false
		for _, B := range b {
			if A == B {
				match = true
			}
		}
		s.True(match, msgAndArgs)
	}
}

func (s *CassandraSuite) assertReplicaStatsArrayEqual(a, b []*shared.ExtentReplicaStats, msgAndArgs ...interface{}) {
	s.NotNil(a, msgAndArgs)
	s.NotNil(b, msgAndArgs)
	s.Equal(len(a), len(b), msgAndArgs)

	for _, A := range a {
		match := false
	inner:
		for _, B := range b {
			if A.GetStoreUUID() == B.GetStoreUUID() {
				match = true
				s.assertReplicaStatsEqual(A, B, msgAndArgs)
				break inner
			}
		}
		s.True(match, msgAndArgs)
	}
}

func (s *CassandraSuite) assertReplicaStatsEqual(a, b *shared.ExtentReplicaStats, msgAndArgs ...interface{}) {
	s.NotNil(a, msgAndArgs)
	s.NotNil(b, msgAndArgs)

	// This prevents [1:] from giving slice out of bounds, below
	if len(msgAndArgs) < 2 {
		msgAndArgs = append([]interface{}{}, `%v %v`, msgAndArgs)
	}

	s.Equal(a.GetAvailableAddress(), b.GetAvailableAddress(), msgAndArgs[0], msgAndArgs[1:], `.GetAvailableAddress() not equal`)
	s.Equal(a.GetAvailableSequence(), b.GetAvailableSequence(), msgAndArgs[0], msgAndArgs[1:], `.GetAvailableSequence() not equal`)
	s.Equal(a.GetAvailableSequenceRate(), b.GetAvailableSequenceRate(), msgAndArgs[0], msgAndArgs[1:], `.GetAvailableSequenceRate() not equal`)
	s.Equal(a.GetBeginAddress(), b.GetBeginAddress(), msgAndArgs[0], msgAndArgs[1:], `.GetBeginAddress() not equal`)
	s.Equal(a.GetBeginEnqueueTimeUtc(), b.GetBeginEnqueueTimeUtc(), msgAndArgs[0], msgAndArgs[1:], `.GetBeginEnqueueTimeUtc() not equal`)
	s.Equal(a.GetBeginSequence(), b.GetBeginSequence(), msgAndArgs[0], msgAndArgs[1:], `.GetBeginSequence() not equal`)
	s.Equal(a.GetBeginTime(), b.GetBeginTime(), msgAndArgs[0], msgAndArgs[1:], `.GetBeginTime() not equal`)
	s.Equal(a.GetCreatedAt(), b.GetCreatedAt(), msgAndArgs[0], msgAndArgs[1:], `.GetCreatedAt() not equal`)
	s.Equal(a.GetEndTime(), b.GetEndTime(), msgAndArgs[0], msgAndArgs[1:], `.GetEndTime() not equal`)
	s.Equal(a.GetExtentUUID(), b.GetExtentUUID(), msgAndArgs[0], msgAndArgs[1:], `.GetExtentUUID() not equal`)
	s.Equal(a.GetLastAddress(), b.GetLastAddress(), msgAndArgs[0], msgAndArgs[1:], `.GetLastAddress() not equal`)
	s.Equal(a.GetLastEnqueueTimeUtc(), b.GetLastEnqueueTimeUtc(), msgAndArgs[0], msgAndArgs[1:], `.GetLastEnqueueTimeUtc() not equal`)
	s.Equal(a.GetLastSequence(), b.GetLastSequence(), msgAndArgs[0], msgAndArgs[1:], `.GetLastSequence() not equal`)
	s.Equal(a.GetLastSequenceRate(), b.GetLastSequenceRate(), msgAndArgs[0], msgAndArgs[1:], `.GetLastSequenceRate() not equal`)
	s.Equal(a.GetSizeInBytes(), b.GetSizeInBytes(), msgAndArgs[0], msgAndArgs[1:], `.GetSizeInBytes() not equal`)
	s.Equal(a.GetSizeInBytesRate(), b.GetSizeInBytesRate(), msgAndArgs[0], msgAndArgs[1:], `.GetSizeInBytesRate() not equal`)
	s.Equal(a.GetStatus(), b.GetStatus(), msgAndArgs[0], msgAndArgs[1:], `.GetStatus() not equal`)
	s.Equal(a.GetStoreUUID(), b.GetStoreUUID(), msgAndArgs[0], msgAndArgs[1:], `.GetStoreUUID() not equal`)
	s.InDelta(a.GetWriteTime(), b.GetWriteTime(), float64(time.Minute), msgAndArgs[0], msgAndArgs[1:], `.GetWriteTime() not equal`)
	a.WriteTime = b.WriteTime // Have to fudge this so that deep equal will work
	s.True(reflect.DeepEqual(a, b), msgAndArgs[0], msgAndArgs[1:], `Not deep equal; check for missing fields in assertReplicaStatsEqual`)
}

func (s *CassandraSuite) TestReadExtentByUUID() {
	// Create
	createDestination := &shared.CreateDestinationRequest{
		Path: common.StringPtr(s.generateName("readextentbyuuid/readextentbyuuid")),
		Type: common.InternalDestinationTypePtr(shared.DestinationType_PLAIN),
		ConsumedMessagesRetention:   common.Int32Ptr(1800),
		UnconsumedMessagesRetention: common.Int32Ptr(3600),
		OwnerEmail:                  common.StringPtr(destinationOwnerEmail),
		ChecksumOption:              common.InternalChecksumOptionPtr(0),
	}
	destination, err := s.client.CreateDestination(nil, createDestination)
	s.Nil(err)

	extentUUID := uuid.New()
	storeIds := []string{uuid.New(), uuid.New(), uuid.New()}
	extent := &shared.Extent{
		ExtentUUID:      common.StringPtr(extentUUID),
		DestinationUUID: common.StringPtr(destination.GetDestinationUUID()),
		StoreUUIDs:      storeIds,
		InputHostUUID:   common.StringPtr(uuid.New()),
	}
	createExtent := &shared.CreateExtentRequest{Extent: extent}
	createExtentResult, err := s.client.CreateExtent(nil, createExtent)
	s.Nil(err)
	s.True(reflect.DeepEqual(extent, createExtentResult.GetExtentStats().GetExtent()))
	s.InDelta(
		int64(common.Now())/int64(time.Millisecond),
		createExtentResult.GetExtentStats().GetCreatedTimeMillis(),
		float64(int64(time.Minute/time.Millisecond)), // Verify Create time was set properly, within a minute of 'now'
	)

	// Read
	readExtentStats := &m.ReadExtentStatsRequest{DestinationUUID: extent.DestinationUUID, ExtentUUID: extent.ExtentUUID}
	extentStats, err := s.client.ReadExtentStats(nil, readExtentStats)
	s.Nil(err)
	s.NotNil(extentStats)
	s.Equal(
		createExtentResult.GetExtentStats().GetCreatedTimeMillis(),
		extentStats.GetExtentStats().GetCreatedTimeMillis(),
	)

	readExtentStats = &m.ReadExtentStatsRequest{ExtentUUID: extent.ExtentUUID}
	extentStats, err = s.client.ReadExtentStats(nil, readExtentStats)
	s.Nil(err)
	s.NotNil(extentStats)
}

func (s *CassandraSuite) TestListExtents() {
	// Create
	// foo/bar 42 inputHost1, 40 inputHost2, 8 inputHost3
	// foo/baz 40 inputHost1, 50 inputHost2
	createDestination := &shared.CreateDestinationRequest{
		Path: common.StringPtr(s.generateName("foo/bar")),
		Type: common.InternalDestinationTypePtr(shared.DestinationType_PLAIN),
		ConsumedMessagesRetention:   common.Int32Ptr(1800),
		UnconsumedMessagesRetention: common.Int32Ptr(3600),
		OwnerEmail:                  common.StringPtr(destinationOwnerEmail),
		ChecksumOption:              common.InternalChecksumOptionPtr(0),
	}
	destination1, err := s.client.CreateDestination(nil, createDestination)
	s.Nil(err)

	inputHost1 := uuid.New()
	inputHost2 := uuid.New()
	inputHost3 := uuid.New()
	storeIds := []string{uuid.New(), uuid.New(), uuid.New()}
	remoteZone := `zone1`

	cExtent := func(inho string, dest *shared.DestinationDescription, originZone string) {
		extentUUID := uuid.New()
		extent := &shared.Extent{
			ExtentUUID:      common.StringPtr(extentUUID),
			DestinationUUID: common.StringPtr(dest.GetDestinationUUID()),
			StoreUUIDs:      storeIds,
			InputHostUUID:   common.StringPtr(inho),
			OriginZone:      common.StringPtr(originZone),
		}
		createExtent := &shared.CreateExtentRequest{Extent: extent}
		_, err = s.client.CreateExtent(nil, createExtent)
		s.Nil(err)
	}

	for i := 0; i < 42; i++ {
		cExtent(inputHost1, destination1, remoteZone)
	}

	for i := 0; i < 20; i++ {
		cExtent(inputHost2, destination1, ``)
	}

	s.Nil(s.Alter(), "ALTER table failed")

	for i := 0; i < 20; i++ {
		cExtent(inputHost2, destination1, ``)
	}

	for i := 0; i < 8; i++ {
		cExtent(inputHost3, destination1, remoteZone)
	}

	createDestination = &shared.CreateDestinationRequest{
		Path: common.StringPtr(s.generateName("foo/baz")),
		Type: common.InternalDestinationTypePtr(shared.DestinationType_TIMER),
		ConsumedMessagesRetention:   common.Int32Ptr(150),
		UnconsumedMessagesRetention: common.Int32Ptr(300),
		OwnerEmail:                  common.StringPtr(destinationOwnerEmail),
		ChecksumOption:              common.InternalChecksumOptionPtr(0),
	}
	destination2, err := s.client.CreateDestination(nil, createDestination)
	s.Nil(err)

	for i := 0; i < 40; i++ {
		cExtent(inputHost1, destination2, ``)
	}

	for i := 0; i < 50; i++ {
		cExtent(inputHost2, destination2, remoteZone)
	}

	lExtSts := func(dest *shared.DestinationDescription, three bool) {
		var statsList []*shared.ExtentStats
		listExtentsStats := &shared.ListExtentsStatsRequest{
			DestinationUUID: dest.DestinationUUID,
			Limit:           common.Int64Ptr(testPageSize),
		}

		for {
			listExtentStatsResult, errf := s.client.ListExtentsStats(nil, listExtentsStats)
			s.Nil(errf)
			statsList = append(statsList, listExtentStatsResult.ExtentStatsList...)
			if len(listExtentStatsResult.GetNextPageToken()) == 0 {
				break
			} else {
				listExtentsStats.PageToken = listExtentStatsResult.GetNextPageToken()
			}
		}

		s.Equal(90, len(statsList))
		for _, stats := range statsList {
			s.Equal(dest.GetDestinationUUID(), stats.Extent.GetDestinationUUID())
			s.InDelta(
				int64(common.Now())/int64(time.Millisecond),
				stats.GetCreatedTimeMillis(),
				float64(int64(5*time.Minute/time.Millisecond)), // Verify Create time was set properly, within 5 minutes of 'now'
			)

			if three {
				s.True(stats.Extent.GetInputHostUUID() == inputHost1 || stats.Extent.GetInputHostUUID() == inputHost2 || stats.Extent.GetInputHostUUID() == inputHost3)
			} else {
				s.True(stats.Extent.GetInputHostUUID() == inputHost1 || stats.Extent.GetInputHostUUID() == inputHost2)
			}

			storesMap := make(map[string]struct{})
			for _, id := range storeIds {
				storesMap[id] = struct{}{}
			}

			for _, id := range stats.Extent.GetStoreUUIDs() {
				_, ok := storesMap[id]
				s.True(ok, "ListExtentsStats() returned invalid store host")
				delete(storesMap, id)
			}
		}

		// verify filtering on local extents can work
		statsList = nil
		listExtentsStats = &shared.ListExtentsStatsRequest{
			DestinationUUID:  dest.DestinationUUID,
			LocalExtentsOnly: common.BoolPtr(true),
			Limit:            common.Int64Ptr(testPageSize),
		}
		for {
			listExtentStatsResult, errf := s.client.ListExtentsStats(nil, listExtentsStats)
			s.Nil(errf)
			statsList = append(statsList, listExtentStatsResult.ExtentStatsList...)
			if len(listExtentStatsResult.GetNextPageToken()) == 0 {
				break
			} else {
				listExtentsStats.PageToken = listExtentStatsResult.GetNextPageToken()
			}
		}

		s.Equal(40, len(statsList))
		for _, stat := range statsList {
			s.Equal(stat.GetExtent().GetOriginZone(), ``)
		}
	}

	lDstExts := func(dest *shared.DestinationDescription, three bool) {
		req := &m.ListDestinationExtentsRequest{
			DestinationUUID: dest.DestinationUUID,
			Limit:           common.Int64Ptr(testPageSize),
		}

		var dstExtents []*m.DestinationExtent

		for {
			resp, errf := s.client.ListDestinationExtents(nil, req)
			s.Nil(errf)
			dstExtents = append(dstExtents, resp.GetExtents()...)

			if len(resp.GetNextPageToken()) == 0 {
				break
			} else {
				req.PageToken = resp.GetNextPageToken()
			}
		}

		s.Equal(90, len(dstExtents))
		for _, de := range dstExtents {
			s.InDelta(
				int64(common.Now())/int64(time.Millisecond),
				de.GetCreatedTimeMillis(),
				float64(int64(5*time.Minute/time.Millisecond)), // Verify Create time was set properly, within 5 minutes of 'now'
			)

			if three {
				s.True(de.GetInputHostUUID() == inputHost1 || de.GetInputHostUUID() == inputHost2 || de.GetInputHostUUID() == inputHost3)
			} else {
				s.True(de.GetInputHostUUID() == inputHost1 || de.GetInputHostUUID() == inputHost2)
			}

			storesMap := make(map[string]struct{})
			for _, id := range storeIds {
				storesMap[id] = struct{}{}
			}

			for _, id := range de.GetStoreUUIDs() {
				_, ok := storesMap[id]
				s.True(ok, "ListDestinationExtents() returned invalid store host")
				delete(storesMap, id)
			}
		}
	}

	lExtInSts := func(dest *shared.DestinationDescription, inho *string) {
		listExtentsStats := &m.ListInputHostExtentsStatsRequest{DestinationUUID: dest.DestinationUUID, InputHostUUID: inho, Status: common.MetadataExtentStatusPtr(shared.ExtentStatus_OPEN)}
		listExtentStatsResult, errff := s.client.ListInputHostExtentsStats(nil, listExtentsStats)
		s.Nil(errff)

		statsList := listExtentStatsResult.ExtentStatsList
		s.Equal(40, len(statsList))
		for _, stats := range statsList {
			s.InDelta(
				int64(common.Now())/int64(time.Millisecond),
				stats.GetCreatedTimeMillis(),
				float64(int64(5*time.Minute/time.Millisecond)), // Verify Create time was set properly, within 5 minutes of 'now'
			)

			s.Equal(dest.GetDestinationUUID(), stats.Extent.GetDestinationUUID())
			s.Equal(*inho, stats.Extent.GetInputHostUUID())
		}
	}

	lExtSts(destination2, false)
	lDstExts(destination2, false)
	lExtInSts(destination2, &inputHost1)
	lExtSts(destination1, true)
	lDstExts(destination1, true)
	lExtInSts(destination1, &inputHost2)

	// List store extents
	listStoreExtentsStats := &m.ListStoreExtentsStatsRequest{StoreUUID: common.StringPtr(storeIds[1])}
	listStoreExtentStatsResult, err := s.client.ListStoreExtentsStats(nil, listStoreExtentsStats)
	s.Nil(err)

	statsList := listStoreExtentStatsResult.ExtentStatsList
	s.Equal(180, len(statsList))
	for _, stats := range statsList {
		s.Equal(1, len(stats.GetReplicaStats()))
		s.Equal(storeIds[1], stats.ReplicaStats[0].GetStoreUUID())
	}
}

func (s *CassandraSuite) TestExtentReplicaStatsRUD() {
	// Create
	createDestination := &shared.CreateDestinationRequest{
		Path: common.StringPtr(s.generateName("foo/bar")),
		Type: common.InternalDestinationTypePtr(shared.DestinationType_PLAIN),
		ConsumedMessagesRetention:   common.Int32Ptr(1800),
		UnconsumedMessagesRetention: common.Int32Ptr(3600),
		OwnerEmail:                  common.StringPtr(destinationOwnerEmail),
		ChecksumOption:              common.InternalChecksumOptionPtr(0),
	}
	destination, err := s.client.CreateDestination(nil, createDestination)
	s.Nil(err)

	storeIds := []string{uuid.New(), uuid.New(), uuid.New()}
	cExt := func() *shared.Extent {
		extentUUID := uuid.New()
		extent := &shared.Extent{
			ExtentUUID:      common.StringPtr(extentUUID),
			DestinationUUID: common.StringPtr(destination.GetDestinationUUID()),
			StoreUUIDs:      storeIds,
			InputHostUUID:   common.StringPtr(uuid.New()),
		}
		createExtent := &shared.CreateExtentRequest{Extent: extent}
		_, err = s.client.CreateExtent(nil, createExtent)
		s.Nil(err)
		return extent
	}

	extent := cExt()

	for pass := 0; pass < 3; pass++ {

		stats1 := &shared.ExtentReplicaStats{
			ExtentUUID:            common.StringPtr(extent.GetExtentUUID()),
			StoreUUID:             common.StringPtr(storeIds[0]),
			CreatedAt:             common.Int64Ptr(22),
			BeginAddress:          common.Int64Ptr(13),
			LastAddress:           common.Int64Ptr(18),
			BeginSequence:         common.Int64Ptr(123),
			LastSequence:          common.Int64Ptr(1024),
			LastSequenceRate:      common.Float64Ptr(70.68),
			AvailableSequence:     common.Int64Ptr(1024),
			AvailableSequenceRate: common.Float64Ptr(70.68),
			BeginEnqueueTimeUtc:   common.Int64Ptr(1111),
			LastEnqueueTimeUtc:    common.Int64Ptr(234),
			SizeInBytes:           common.Int64Ptr(12),
			SizeInBytesRate:       common.Float64Ptr(1035.6),
			Status:                common.MetadataExtentReplicaStatusPtr(shared.ExtentReplicaStatus_MISSING),
			BeginTime:             common.Int64Ptr(34),
			EndTime:               common.Int64Ptr(43),
		}
		stats2 := &shared.ExtentReplicaStats{
			ExtentUUID:            common.StringPtr(extent.GetExtentUUID()),
			StoreUUID:             common.StringPtr(storeIds[0]),
			CreatedAt:             common.Int64Ptr(222),
			BeginAddress:          common.Int64Ptr(132),
			LastAddress:           common.Int64Ptr(182),
			BeginSequence:         common.Int64Ptr(1232),
			LastSequence:          common.Int64Ptr(10242),
			LastSequenceRate:      common.Float64Ptr(70.68),
			AvailableSequence:     common.Int64Ptr(10242),
			AvailableSequenceRate: common.Float64Ptr(70.68),
			BeginEnqueueTimeUtc:   common.Int64Ptr(11112),
			LastEnqueueTimeUtc:    common.Int64Ptr(2342),
			SizeInBytes:           common.Int64Ptr(122),
			SizeInBytesRate:       common.Float64Ptr(1035.6),
			Status:                common.MetadataExtentReplicaStatusPtr(shared.ExtentReplicaStatus_SEALED),
			BeginTime:             common.Int64Ptr(342),
			EndTime:               common.Int64Ptr(432),
		}
		updateStatsRequest := &m.UpdateExtentReplicaStatsRequest{
			DestinationUUID: common.StringPtr(extent.GetDestinationUUID()),
			ExtentUUID:      common.StringPtr(extent.GetExtentUUID()),
			InputHostUUID:   common.StringPtr(extent.GetInputHostUUID()),
			ReplicaStats:    []*shared.ExtentReplicaStats{stats1, stats2},
		}
		err = s.client.UpdateExtentReplicaStats(nil, updateStatsRequest)
		s.Nil(err)

		if pass == 0 {
			s.Nil(s.Alter(), "ALTER table failed")
		} else {
			storeIds[0] = uuid.New()
			extent = cExt()
		}
	}
}

func assertConsumerGroupsEqual(s *CassandraSuite, expected, got *shared.ConsumerGroupDescription) {
	s.Equal(expected.GetDestinationUUID(), got.GetDestinationUUID(), "Wrong destinationUUID")
	s.Equal(expected.GetConsumerGroupUUID(), got.GetConsumerGroupUUID(), "Wrong consumer group UUID")
	s.Equal(expected.GetStartFrom(), got.GetStartFrom(), "Wrong StartFromField")
	s.Equal(expected.GetLockTimeoutSeconds(), got.GetLockTimeoutSeconds(), "Wrong LockTimeoutSeconds")
	s.Equal(expected.GetMaxDeliveryCount(), got.GetMaxDeliveryCount(), "Wrong MaxDeliveryCount")
	s.Equal(expected.GetSkipOlderMessagesSeconds(), got.GetSkipOlderMessagesSeconds(), "Wrong SkipOlderMessagesSeconds")
	s.Equal(expected.GetDelaySeconds(), got.GetDelaySeconds(), "Wrong DelaySeconds")
	s.Equal(expected.GetOwnerEmail(), got.GetOwnerEmail(), "Wrong OwnerEmail")
	s.Equal(expected.GetDeadLetterQueueDestinationUUID(), got.GetDeadLetterQueueDestinationUUID(), "Wrong DeadLetterQueueDestinationUUID")
	s.Equal(expected.GetActiveZone(), got.GetActiveZone(), "Wrong ActiveZone")
	s.Equal(expected.GetOptions(), got.GetOptions(), "Wrong Options")
}

func (s *CassandraSuite) TestDeleteConsumerGroupDeletesDLQ() {

	assert := s.Require()

	dstPath := s.generateName("/foo/bar")
	dst, err := createDestination(s, dstPath, false)
	assert.Nil(err, "CreateDestination failed")

	cgName := s.generateName("/foo.bar/consumer")

	createReq := &shared.CreateConsumerGroupRequest{
		DestinationPath:          common.StringPtr(dstPath),
		ConsumerGroupName:        common.StringPtr(cgName),
		StartFrom:                common.Int64Ptr(30),
		LockTimeoutSeconds:       common.Int32Ptr(10),
		MaxDeliveryCount:         common.Int32Ptr(5),
		SkipOlderMessagesSeconds: common.Int32Ptr(60),
		DelaySeconds:             common.Int32Ptr(60),
		OwnerEmail:               common.StringPtr("consumer_test@uber.com"),
	}

	gotCG, err := s.client.CreateConsumerGroup(nil, createReq)
	assert.Nil(err, "CreateConsumerGroup failed")
	assert.Equal(shared.ConsumerGroupStatus_ENABLED, gotCG.GetStatus(), "Wrong CG status")

	dlqUUID := gotCG.GetDeadLetterQueueDestinationUUID()
	readDstReq := &shared.ReadDestinationRequest{
		Path: common.StringPtr(dlqUUID),
	}
	dlqDst, err := s.client.ReadDestination(nil, readDstReq)
	assert.Nil(err, "ReadDestination failed for DLQ")
	assert.Equal(shared.DestinationStatus_ENABLED, dlqDst.GetStatus(), "Wrong dlq destination status")

	deleteReq := &shared.DeleteConsumerGroupRequest{
		DestinationPath:   common.StringPtr(dst.GetPath()),
		ConsumerGroupName: common.StringPtr(gotCG.GetConsumerGroupName()),
	}
	err = s.client.DeleteConsumerGroup(nil, deleteReq)
	assert.Nil(err, "DeleteConsumerGroup failed")

	readCGReq := &shared.ReadConsumerGroupRequest{
		DestinationPath:   common.StringPtr(dst.GetPath()),
		ConsumerGroupName: common.StringPtr(gotCG.GetConsumerGroupName()),
	}
	_, err = s.client.ReadConsumerGroup(nil, readCGReq)
	assert.NotNil(err, "ReadConsumerGroup succeeded on a DELETED group")

	dlqDst, err = s.client.ReadDestination(nil, readDstReq)
	assert.Nil(err, "ReadDestination failed to return deleted DLQ")
	assert.NotNil(dlqDst, `ReadDestination returned nil dest description`)
	assert.True(dlqDst.GetStatus() == shared.DestinationStatus_DELETING ||
		dlqDst.GetStatus() == shared.DestinationStatus_DELETED,
		`DLQ should be deleted`)

	readDstReq = &shared.ReadDestinationRequest{
		DestinationUUID: common.StringPtr(dlqUUID),
	}
	dlqDst, err = s.client.ReadDestination(nil, readDstReq)
	assert.Nil(err, "ReadDestination failed for DLQ")
	assert.Equal(shared.DestinationStatus_DELETING, dlqDst.GetStatus(), "Wrong dlq destination status")
}

func (s *CassandraSuite) TestConsumerGroupCRUD() {

	assert := s.Require()

	dstPath := s.generateName("/foo/bar")
	dst, err := createDestination(s, dstPath, false)
	assert.Nil(err, "CreateDestination failed")

	zoneConfig := &shared.ConsumerGroupZoneConfig{
		Zone:    common.StringPtr("zone1"),
		Visible: common.BoolPtr(false),
	}

	options := make(map[string]string)
	options[common.FlagDisableNackThrottling] = "true"
	options[common.FlagEnableSmartRetry] = "false"

	cgName := s.generateName("/foo/bar_consumer")

	createReq := &shared.CreateConsumerGroupRequest{
		DestinationPath:          common.StringPtr(dstPath),
		ConsumerGroupName:        common.StringPtr(cgName),
		StartFrom:                common.Int64Ptr(30),
		LockTimeoutSeconds:       common.Int32Ptr(10),
		MaxDeliveryCount:         common.Int32Ptr(5),
		SkipOlderMessagesSeconds: common.Int32Ptr(60),
		DelaySeconds:             common.Int32Ptr(60),
		OwnerEmail:               common.StringPtr("consumer_test@uber.com"),
		IsMultiZone:              common.BoolPtr(true),
		ActiveZone:               common.StringPtr("zone1"),
		ZoneConfigs:              []*shared.ConsumerGroupZoneConfig{zoneConfig},
		Options:                  options,
	}

	expectedCG := &shared.ConsumerGroupDescription{
		DestinationUUID:          common.StringPtr(dst.GetDestinationUUID()),
		ConsumerGroupName:        common.StringPtr(createReq.GetConsumerGroupName()),
		StartFrom:                common.Int64Ptr(createReq.GetStartFrom()),
		Status:                   common.InternalConsumerGroupStatusPtr(shared.ConsumerGroupStatus_ENABLED),
		LockTimeoutSeconds:       common.Int32Ptr(createReq.GetLockTimeoutSeconds()),
		MaxDeliveryCount:         common.Int32Ptr(createReq.GetMaxDeliveryCount()),
		SkipOlderMessagesSeconds: common.Int32Ptr(createReq.GetSkipOlderMessagesSeconds()),
		DelaySeconds:             common.Int32Ptr(createReq.GetDelaySeconds()),
		OwnerEmail:               common.StringPtr(createReq.GetOwnerEmail()),
		IsMultiZone:              common.BoolPtr(createReq.GetIsMultiZone()),
		ActiveZone:               common.StringPtr(createReq.GetActiveZone()),
		ZoneConfigs:              createReq.GetZoneConfigs(),
		Options:                  options,
	}

	expectedCGOrig := new(shared.ConsumerGroupDescription)
	*expectedCGOrig = *expectedCG

	log.Debugf(`Create %v`, *createReq.ConsumerGroupName)
	gotCG, err := s.client.CreateConsumerGroup(nil, createReq)
	assert.Nil(err, "CreateConsumerGroup failed")

	expectedCG.ConsumerGroupUUID = common.StringPtr(gotCG.GetConsumerGroupUUID())
	expectedCG.DeadLetterQueueDestinationUUID = common.StringPtr(gotCG.GetDeadLetterQueueDestinationUUID())
	assertConsumerGroupsEqual(s, expectedCG, gotCG)

	// make sure the DLQConsumerGroupUUID from DLQ dest is correct
	dlqDestReq := &shared.ReadDestinationRequest{
		Path: common.StringPtr(gotCG.GetDeadLetterQueueDestinationUUID()),
	}
	dlqDest, err := s.client.ReadDestination(nil, dlqDestReq)
	assert.Nil(err, "Read Dlq destination failed")
	assert.Equal(dlqDest.GetDLQConsumerGroupUUID(), gotCG.GetConsumerGroupUUID())
	assert.Equal(dlqDest.GetConsumedMessagesRetention(), int32(defaultDLQConsumedRetention))
	assert.Equal(dlqDest.GetUnconsumedMessagesRetention(), int32(defaultDLQUnconsumedRetention))
	assert.Equal(dlqDest.GetIsMultiZone(), false)
	assert.Equal(dlqDest.GetOwnerEmail(), createReq.GetOwnerEmail())
	dlqName, err := common.GetDLQPathNameFromCGName(createReq.GetConsumerGroupName())
	assert.Nil(err, "GetDLQPathNameFromCGName failed")
	assert.Equal(dlqDest.GetPath(), dlqName)

	for pass := 0; pass < 3; pass++ {
		readReq := &shared.ReadConsumerGroupRequest{
			DestinationPath:   common.StringPtr(createReq.GetDestinationPath()),
			ConsumerGroupName: common.StringPtr(createReq.GetConsumerGroupName()),
		}

		gotCG = nil
		gotCG, err = s.client.ReadConsumerGroup(nil, readReq)
		assert.Nil(err, "ReadConsumerGroup failed")
		assertConsumerGroupsEqual(s, expectedCG, gotCG)

		readReq.DestinationPath = nil
		readReq.DestinationUUID = common.StringPtr(dst.GetDestinationUUID())
		gotCG, err = s.client.ReadConsumerGroup(nil, readReq)
		assert.Nil(err, "ReadConsumerGroup failed")
		assertConsumerGroupsEqual(s, expectedCG, gotCG)

		options[common.FlagDisableNackThrottling] = "false"
		options[common.FlagEnableSmartRetry] = "true"

		readReq.ConsumerGroupUUID = common.StringPtr(gotCG.GetConsumerGroupUUID())
		gotCG, err = s.client.ReadConsumerGroupByUUID(nil, readReq)
		assert.Nil(err, "ReadConsumerGroupByUUID failed")
		assert.Equal(expectedCG.GetConsumerGroupUUID(), gotCG.GetConsumerGroupUUID(), "ReadConsumerGroupByUUID return wrong CG Name")
		updateReq := &shared.UpdateConsumerGroupRequest{
			DestinationPath:          common.StringPtr(createReq.GetDestinationPath()),
			ConsumerGroupName:        common.StringPtr(createReq.GetConsumerGroupName()),
			Status:                   common.InternalConsumerGroupStatusPtr(shared.ConsumerGroupStatus_DISABLED),
			LockTimeoutSeconds:       common.Int32Ptr(99),
			MaxDeliveryCount:         common.Int32Ptr(99),
			SkipOlderMessagesSeconds: common.Int32Ptr(100),
			DelaySeconds:             common.Int32Ptr(100),
			OwnerEmail:               common.StringPtr("consumer_test@uber.com"),
			Options:                  options,
		}

		if pass%2 == 0 {
			updateReq.ActiveZone = common.StringPtr(`zone2`)
		} else {
			updateReq.ActiveZone = common.StringPtr(``)
		}

		expectedCG.Status = common.InternalConsumerGroupStatusPtr(shared.ConsumerGroupStatus_DISABLED)
		expectedCG.LockTimeoutSeconds = common.Int32Ptr(updateReq.GetLockTimeoutSeconds())
		expectedCG.MaxDeliveryCount = common.Int32Ptr(updateReq.GetMaxDeliveryCount())
		expectedCG.SkipOlderMessagesSeconds = common.Int32Ptr(updateReq.GetSkipOlderMessagesSeconds())
		expectedCG.DelaySeconds = common.Int32Ptr(updateReq.GetDelaySeconds())
		expectedCG.OwnerEmail = common.StringPtr(updateReq.GetOwnerEmail())
		expectedCG.ActiveZone = common.StringPtr(updateReq.GetActiveZone())
		expectedCG.Options = options

		gotCG = nil
		gotCG, err = s.client.UpdateConsumerGroup(nil, updateReq)
		assert.Nil(err, "UpdateConsumerGroup failed")
		assertConsumerGroupsEqual(s, expectedCG, gotCG)

		gotCG = nil
		gotCG, err = s.client.ReadConsumerGroup(nil, readReq)
		assert.Nil(err, "ReadConsumerGroup failed after UpdateConsumerGroup success")
		assertConsumerGroupsEqual(s, expectedCG, gotCG)

		if pass == 0 {
			s.Nil(s.Alter(), "ALTER table failed")
		} else {
			var deleteReq *shared.DeleteConsumerGroupRequest
			// test one form of API or other with 50% probability
			if rand.Intn(2) == 1 {
				deleteReq = &shared.DeleteConsumerGroupRequest{
					DestinationPath:   common.StringPtr(createReq.GetDestinationPath()),
					ConsumerGroupName: common.StringPtr(createReq.GetConsumerGroupName()),
				}
			} else {
				deleteReq = &shared.DeleteConsumerGroupRequest{
					DestinationUUID:   common.StringPtr(dst.GetDestinationUUID()),
					ConsumerGroupName: common.StringPtr(createReq.GetConsumerGroupName()),
				}
			}

			err = s.client.DeleteConsumerGroup(nil, deleteReq)
			assert.Nil(err, "DeleteConsumerGroup failed")

			_, err = s.client.UpdateConsumerGroup(nil, updateReq)
			assert.NotNil(err, "UpdateConsumerGroup succeeded on a DELETED group")

			_, err = s.client.ReadConsumerGroup(nil, readReq)
			assert.NotNil(err, "ReadConsumerGroup succeeded on a DELETED group")

			_, err = s.client.CreateConsumerGroup(nil, createReq)
			assert.Nil(err, "CreateConsumerGroup failed after a DeleteConsumerGroup")

			createReq.ConsumerGroupName = common.StringPtr(*createReq.ConsumerGroupName + string(rune(pass+'0')))

			gotCG, err = s.client.CreateConsumerGroup(nil, createReq)
			assert.Nil(err, "CreateConsumerGroup failed")

			expectedCGOrig.ConsumerGroupUUID = common.StringPtr(gotCG.GetConsumerGroupUUID())
			expectedCGOrig.DeadLetterQueueDestinationUUID = common.StringPtr(gotCG.GetDeadLetterQueueDestinationUUID())
			assertConsumerGroupsEqual(s, expectedCGOrig, gotCG)

			*expectedCG = *expectedCGOrig
		}
	}
}

func (s *CassandraSuite) TestCGCRUDOnPhantomDestination() {

	dstPath := s.generateName("/foo/bar2")
	cgName := s.generateName("foobar-consumer")

	createReq := &shared.CreateConsumerGroupRequest{
		DestinationPath:          common.StringPtr(dstPath),
		ConsumerGroupName:        common.StringPtr(cgName),
		StartFrom:                common.Int64Ptr(30),
		LockTimeoutSeconds:       common.Int32Ptr(10),
		MaxDeliveryCount:         common.Int32Ptr(5),
		SkipOlderMessagesSeconds: common.Int32Ptr(6),
		DelaySeconds:             common.Int32Ptr(6),
		OwnerEmail:               common.StringPtr("consumer_test@uber.com"),
	}

	assert := s.Require()

	_, err := s.client.CreateConsumerGroup(nil, createReq)
	assert.NotNil(err, "CreateConsumerGroup succeeded on non-existent destination")

	updateReq := &shared.UpdateConsumerGroupRequest{
		DestinationPath:          common.StringPtr(createReq.GetDestinationPath()),
		ConsumerGroupName:        common.StringPtr(createReq.GetConsumerGroupName()),
		Status:                   common.InternalConsumerGroupStatusPtr(shared.ConsumerGroupStatus_DISABLED),
		LockTimeoutSeconds:       common.Int32Ptr(99),
		MaxDeliveryCount:         common.Int32Ptr(99),
		SkipOlderMessagesSeconds: common.Int32Ptr(99),
		DelaySeconds:             common.Int32Ptr(99),
		OwnerEmail:               common.StringPtr("consumer_test@uber.com"),
	}
	_, err = s.client.UpdateConsumerGroup(nil, updateReq)
	assert.NotNil(err, "UpdateConsumerGroup succeeded on non-existent destination")

	readReq := &shared.ReadConsumerGroupRequest{
		DestinationPath:   common.StringPtr(createReq.GetDestinationPath()),
		ConsumerGroupName: common.StringPtr(createReq.GetConsumerGroupName()),
	}

	_, err = s.client.ReadConsumerGroup(nil, readReq)
	assert.NotNil(err, "ReadConsumerGroup succeeded on non-existent destination")

	deleteReq := &shared.DeleteConsumerGroupRequest{
		DestinationPath:   common.StringPtr(createReq.GetDestinationPath()),
		ConsumerGroupName: common.StringPtr(createReq.GetConsumerGroupName()),
	}

	err = s.client.DeleteConsumerGroup(nil, deleteReq)
	assert.NotNil(err, "DeleteConsumerGroup succeeded on non-existent destination")
}

func (s *CassandraSuite) TestReCreateConsumerGroup() {

	assert := s.Require()

	dstPath := s.generateName("/foo/bar")
	_, err := createDestination(s, dstPath, false)
	assert.Nil(err, "CreateDestination failed")

	cgName := s.generateName("foobar-consumer")

	createReq := &shared.CreateConsumerGroupRequest{
		DestinationPath:          common.StringPtr(dstPath),
		ConsumerGroupName:        common.StringPtr(cgName),
		StartFrom:                common.Int64Ptr(30),
		LockTimeoutSeconds:       common.Int32Ptr(10),
		MaxDeliveryCount:         common.Int32Ptr(5),
		SkipOlderMessagesSeconds: common.Int32Ptr(60),
		DelaySeconds:             common.Int32Ptr(60),
		OwnerEmail:               common.StringPtr("consumer_test@uber.com"),
	}

	_, err = s.client.CreateConsumerGroup(nil, createReq)
	assert.Nil(err, "CreateConsumerGroup failed")

	_, err = s.client.CreateConsumerGroup(nil, createReq)
	assert.NotNil(err, "Recreation of same ConsumerGroup must fail")
}

func (s *CassandraSuite) TestListConsumerGroups() {

	assert := s.Require()

	dstPath := s.generateName("/foo/bar")

	dstInfo, err := createDestination(s, dstPath, false)
	assert.Nil(err, "CreateDestination failed")

	dstUUID := dstInfo.GetDestinationUUID()

	listCGs := func(req *shared.ListConsumerGroupRequest) (ret set.Set) {

		ret = set.New(0)

		for {
			listRes, err := s.client.ListConsumerGroups(nil, req)
			assert.Nil(err, "ListConsumerGroups failed to return results, input=%v", req)

			for _, gotCG := range listRes.GetConsumerGroups() {
				ret.Insert(gotCG.GetConsumerGroupName())
				fmt.Printf("ListConsumerGroups: cg=%v status=%v\n", gotCG.GetConsumerGroupName(), gotCG.GetStatus())
			}

			if len(listRes.GetNextPageToken()) == 0 {
				break
			}

			req.PageToken = listRes.GetNextPageToken()
		}
		fmt.Printf("ListConsumerGroups: --\n")

		return
	}

	listCGsUUID := func(req *shared.ListConsumerGroupsUUIDRequest) (ret set.Set) {

		ret = set.New(0)

		for {
			listRes, err := s.client.ListConsumerGroupsUUID(nil, req)
			assert.Nil(err, "ListConsumerGroupsUUID failed to return results, input=%v", req)

			for _, gotCG := range listRes.GetConsumerGroups() {
				ret.Insert(gotCG.GetConsumerGroupName())
				fmt.Printf("ListConsumerGroupsUUID: cg=%v status=%v\n", gotCG.GetConsumerGroupName(), gotCG.GetStatus())
			}

			if len(listRes.GetNextPageToken()) == 0 {
				break
			}

			req.PageToken = listRes.GetNextPageToken()
		}
		fmt.Printf("ListConsumerGroupsUUID: --\n")

		return
	}

	assert.True(listCGs(&shared.ListConsumerGroupRequest{
		DestinationPath: common.StringPtr(dstPath),
		Limit:           common.Int64Ptr(testPageSize),
	}).Empty(), "Result should be empty when there are no matching groups")

	assert.True(listCGs(&shared.ListConsumerGroupRequest{
		DestinationUUID: common.StringPtr(dstUUID),
		Limit:           common.Int64Ptr(testPageSize),
	}).Empty(), "Result should be empty when there are no matching groups")

	assert.True(listCGsUUID(&shared.ListConsumerGroupsUUIDRequest{
		DestinationUUID: common.StringPtr(dstUUID),
		Limit:           common.Int64Ptr(testPageSize),
	}).Empty(), "Result should be empty when there are no matching groups")

	testName := ""
	cgSet := set.New(0)
	cgMap := make(map[string]string) // path to uuid

	for i := 0; i < 10; i++ {
		name := s.generateName(fmt.Sprintf("foobar-consumer-%v", i))
		var createReq *shared.CreateConsumerGroupRequest

		createReq = &shared.CreateConsumerGroupRequest{
			DestinationPath:          common.StringPtr(dstPath),
			ConsumerGroupName:        common.StringPtr(name),
			StartFrom:                common.Int64Ptr(30),
			LockTimeoutSeconds:       common.Int32Ptr(10),
			MaxDeliveryCount:         common.Int32Ptr(5),
			SkipOlderMessagesSeconds: common.Int32Ptr(60),
			DelaySeconds:             common.Int32Ptr(60),
			OwnerEmail:               common.StringPtr("consumer_test@uber.com"),
		}

		cgDesc, err := s.client.CreateConsumerGroup(nil, createReq)
		assert.Nil(err, "Failed to create consumer group")
		cgSet.Insert(name)
		cgMap[name] = cgDesc.GetConsumerGroupUUID()

		if i == 5 {
			s.Nil(s.Alter(), "ALTER table failed")
		}

		testName = name
	}

	testCGs := listCGs(&shared.ListConsumerGroupRequest{
		DestinationPath:   common.StringPtr(dstPath),
		ConsumerGroupName: common.StringPtr(testName),
		Limit:             common.Int64Ptr(testPageSize),
	})

	assert.Equal(1, testCGs.Count(), "ListConsumerGroups failed to return correct number of result")
	assert.True(testCGs.Contains(testName), "Wrong consumer group returned")

	assert.True(listCGs(&shared.ListConsumerGroupRequest{
		DestinationPath: common.StringPtr(dstPath),
		Limit:           common.Int64Ptr(testPageSize),
	}).Equals(cgSet), "ListConsumerGroups did not return all CGs")

	assert.True(listCGs(&shared.ListConsumerGroupRequest{
		DestinationUUID: common.StringPtr(dstUUID),
		Limit:           common.Int64Ptr(testPageSize),
	}).Equals(cgSet), "ListConsumerGroups did not return all CGs")

	assert.True(listCGsUUID(&shared.ListConsumerGroupsUUIDRequest{
		DestinationUUID: common.StringPtr(dstUUID),
		Limit:           common.Int64Ptr(testPageSize),
	}).Equals(cgSet), "ListConsumerGroupsUUID did not return all CGs")

	var activeCGs = set.New(0)
	var notYetDeletedCGs = set.New(0)

	for i, cg := range cgSet.Keys() {

		switch {
		case i < 3: // mark these CGs 'deleting'
			err = s.client.DeleteConsumerGroup(nil, &shared.DeleteConsumerGroupRequest{
				DestinationPath:   common.StringPtr(dstPath),
				ConsumerGroupName: common.StringPtr(cg),
			})
			assert.Nil(err, "Failed to delete consumer group")
			fmt.Printf("DeleteConsumerGroup: %v [%v]\n", cg, cgMap[cg])

			err = s.client.DeleteConsumerGroupUUID(nil, &m.DeleteConsumerGroupUUIDRequest{
				UUID: common.StringPtr(cgMap[cg]),
			})
			assert.Nil(err, "Failed to delete consumer group")
			fmt.Printf("DeleteConsumerGroupUUID: %v [%v]\n", cg, cgMap[cg])

		case i < 7: // mark these CGs 'deleted'
			err = s.client.DeleteConsumerGroup(nil, &shared.DeleteConsumerGroupRequest{
				DestinationPath:   common.StringPtr(dstPath),
				ConsumerGroupName: common.StringPtr(cg),
			})
			assert.Nil(err, "Failed to delete consumer group")
			notYetDeletedCGs.Insert(cg)
			fmt.Printf("DeleteConsumerGroup: %v [%v]\n", cg, cgMap[cg])

		default:
			activeCGs.Insert(cg)
			notYetDeletedCGs.Insert(cg)
			fmt.Printf("Active CG: %v [%v]\n", cg, cgMap[cg])
		}
	}

	assert.True(listCGs(&shared.ListConsumerGroupRequest{
		DestinationUUID: common.StringPtr(dstUUID),
		Limit:           common.Int64Ptr(testPageSize),
	}).Equals(activeCGs), "ListConsumerGroups did not return all CGs")

	assert.True(listCGsUUID(&shared.ListConsumerGroupsUUIDRequest{
		DestinationUUID: common.StringPtr(dstUUID),
		Limit:           common.Int64Ptr(testPageSize),
	}).Equals(notYetDeletedCGs), "ListConsumerGroupsUUID did not return all CGs")
}

func (s *CassandraSuite) TestListAllConsumerGroups() {

	assert := s.Require()

	dstPath := s.generateName("/foo/bar")
	dstInfo, err := createDestination(s, dstPath, false)
	assert.Nil(err, "CreateDestination failed")

	dstUUID := dstInfo.GetDestinationUUID()
	groupMap := make(map[string]string)

	options := make(map[string]string)
	options[common.FlagDisableNackThrottling] = "true"
	options[common.FlagEnableSmartRetry] = "false"

	for i := 0; i < 10; i++ {
		name := s.generateName(fmt.Sprintf("foobar-consumer-%v", i))
		var createReq *shared.CreateConsumerGroupRequest

		createReq = &shared.CreateConsumerGroupRequest{
			DestinationPath:          common.StringPtr(dstPath),
			ConsumerGroupName:        common.StringPtr(name),
			StartFrom:                common.Int64Ptr(30),
			LockTimeoutSeconds:       common.Int32Ptr(10),
			MaxDeliveryCount:         common.Int32Ptr(5),
			SkipOlderMessagesSeconds: common.Int32Ptr(60),
			DelaySeconds:             common.Int32Ptr(60),
			OwnerEmail:               common.StringPtr("consumer_test@uber.com"),
			Options:                  options,
		}

		_, err = s.client.CreateConsumerGroup(nil, createReq)
		assert.Nil(err, "Failed to create consumer group")
		groupMap[name] = dstUUID

		if i == 5 {
			s.Nil(s.Alter(), "ALTER table failed")
		}
	}

	listReq := &shared.ListConsumerGroupRequest{
		Limit: common.Int64Ptr(testPageSize),
	}

	for {
		listRes, err1 := s.client.ListAllConsumerGroups(nil, listReq)
		assert.Nil(err1, "ListAllConsumerGroups failed to return results")

		for _, gotCG := range listRes.GetConsumerGroups() {
			var destUUID string
			var ok bool
			if destUUID, ok = groupMap[gotCG.GetConsumerGroupName()]; ok {
				if destUUID == gotCG.GetDestinationUUID() {
					s.Equal(int64(30), gotCG.GetStartFrom())
					s.Equal(int32(10), gotCG.GetLockTimeoutSeconds())
					s.Equal(int32(5), gotCG.GetMaxDeliveryCount())
					s.Equal(int32(60), gotCG.GetSkipOlderMessagesSeconds())
					s.Equal(int32(60), gotCG.GetDelaySeconds())
					s.Equal(string("consumer_test@uber.com"), gotCG.GetOwnerEmail())
					s.Equal(options, gotCG.GetOptions())
					delete(groupMap, gotCG.GetConsumerGroupName())
				}
			}
		}
		if len(listRes.GetNextPageToken()) == 0 {
			break
		} else {
			listReq.PageToken = listRes.GetNextPageToken()
		}
	}

	if len(groupMap) > 0 {
		assert.Fail("ListConsumerGroups failed to return all groups")
	}
}
func (s *CassandraSuite) TestSameCGNameOnDifferentDestinations() {

	assert := s.Require()

	dstPath1 := s.generateName("/foo/bar1")
	dst1, err := createDestination(s, dstPath1, false)
	assert.Nil(err, "CreateDestination failed")

	dstPath2 := s.generateName("/foo/bar2")
	dst2, err := createDestination(s, dstPath2, false)
	assert.Nil(err, "CreateDestination failed")

	createReq := &shared.CreateConsumerGroupRequest{
		DestinationPath:          common.StringPtr(dst1.GetPath()),
		ConsumerGroupName:        common.StringPtr(s.generateName("foobar-consumer")),
		StartFrom:                common.Int64Ptr(30),
		LockTimeoutSeconds:       common.Int32Ptr(10),
		MaxDeliveryCount:         common.Int32Ptr(5),
		SkipOlderMessagesSeconds: common.Int32Ptr(60),
		DelaySeconds:             common.Int32Ptr(60),
		OwnerEmail:               common.StringPtr("consumer_test@uber.com"),
	}

	_, err = s.client.CreateConsumerGroup(nil, createReq)
	assert.Nil(err, "CreateConsumerGroup failed")

	createReq.DestinationPath = common.StringPtr(dst2.GetPath())
	_, err = s.client.CreateConsumerGroup(nil, createReq)
	assert.Nil(err, "Creation of same ConsumerGroup on multiple destinations failed")
}

func assertConsumerGroupExtentEqual(s *CassandraSuite, expected, got *shared.ConsumerGroupExtent) {
	s.Equal(expected.GetConsumerGroupUUID(), got.GetConsumerGroupUUID(), "Wrong consumer group uuid")
	s.Equal(expected.GetExtentUUID(), got.GetExtentUUID(), "Wrong extent uuid")
	s.Equal(expected.GetOutputHostUUID(), got.GetOutputHostUUID(), "Wrong out host uuid")
	s.Equal(expected.GetAckLevelOffset(), got.GetAckLevelOffset(), "Wrong AckLevelOffset")
	s.Equal(expected.GetStatus(), got.GetStatus(), "Wrong Status")
	s.Equal(expected.GetConnectedStoreUUID(), got.GetConnectedStoreUUID(), "Wrong connectedStoreUUID")
	s.Equal(expected.GetAckLevelSeqNo(), got.GetAckLevelSeqNo(), "Wrong ackLevelSeqNo")
	s.Equal(expected.GetAckLevelSeqNoRate(), got.GetAckLevelSeqNoRate(), "Wrong ackLevelSeqNoRate")
	s.Equal(expected.GetReadLevelOffset(), got.GetReadLevelOffset(), "Wrong readLevelOffset")
	s.Equal(expected.GetReadLevelSeqNo(), got.GetReadLevelSeqNo(), "Wrong readLevelSeqNo")
	s.Equal(expected.GetReadLevelSeqNoRate(), got.GetReadLevelSeqNoRate(), "Wrong readLevelSeqNoRate")

	sort.Strings(expected.GetStoreUUIDs())
	sort.Strings(got.GetStoreUUIDs())
	s.Equal(expected.GetStoreUUIDs(), got.GetStoreUUIDs(), "Wrong set of store hosts")
}

func (s *CassandraSuite) TestSetOutputHost() {

	assert := s.Require()

	req := &m.SetOutputHostRequest{
		DestinationUUID:   common.StringPtr(uuid.New()),
		ExtentUUID:        common.StringPtr(uuid.New()),
		ConsumerGroupUUID: common.StringPtr(uuid.New()),
		OutputHostUUID:    common.StringPtr(uuid.New()),
	}

	// test auto create, if not exist
	err := s.client.SetOutputHost(nil, req)
	assert.Nil(err, "SetOutputHost failed")

	readCgeReq := &m.ReadConsumerGroupExtentRequest{
		DestinationUUID:   common.StringPtr(req.GetDestinationUUID()),
		ExtentUUID:        common.StringPtr(req.GetExtentUUID()),
		ConsumerGroupUUID: common.StringPtr(req.GetConsumerGroupUUID()),
	}

	got, err := s.client.ReadConsumerGroupExtent(nil, readCgeReq)
	assert.NotNil(err, "SetOutputHost should not auto create consumer group extent")

	for pass := 0; pass < 2; pass++ {

		// test updating outputhost works
		// Now create the cge and retry the update
		createReq := &shared.CreateConsumerGroupExtentRequest{
			DestinationUUID:   common.StringPtr(req.GetDestinationUUID()),
			ExtentUUID:        common.StringPtr(req.GetExtentUUID()),
			ConsumerGroupUUID: common.StringPtr(req.GetConsumerGroupUUID()),
			OutputHostUUID:    common.StringPtr(req.GetOutputHostUUID()),
		}

		createReq.StoreUUIDs = make([]string, 0, 3)
		for i := 0; i < 3; i++ {
			createReq.StoreUUIDs = append(createReq.StoreUUIDs, uuid.New())
		}

		err = s.client.CreateConsumerGroupExtent(nil, createReq)
		assert.Nil(err, "CreateConsumerGroupExtent() call failed")

		expected := &shared.ConsumerGroupExtent{
			ConsumerGroupUUID: common.StringPtr(req.GetConsumerGroupUUID()),
			ExtentUUID:        common.StringPtr(req.GetExtentUUID()),
			OutputHostUUID:    common.StringPtr(req.GetOutputHostUUID()),
			Status:            common.MetadataConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_OPEN),
			StoreUUIDs:        createReq.StoreUUIDs,
		}

		got, err = s.client.ReadConsumerGroupExtent(nil, readCgeReq)
		assert.Nil(err, "SetOutputHost failed to update consumer group extent")
		assertConsumerGroupExtentEqual(s, expected, got.GetExtent())

		req.OutputHostUUID = common.StringPtr(uuid.New())
		expected.OutputHostUUID = common.StringPtr(req.GetOutputHostUUID())

		err = s.client.SetOutputHost(nil, req)
		assert.Nil(err, "SetOutputHost failed to update out host")

		got, err = s.client.ReadConsumerGroupExtent(nil, readCgeReq)
		assert.Nil(err, "SetOutputHost failed to update consumer group extent")
		assertConsumerGroupExtentEqual(s, expected, got.GetExtent())

		if pass == 0 {
			s.Nil(s.Alter(), "ALTER table failed")
		}
	}
}

func (s *CassandraSuite) TestReadConsumerGroupExtentsByExtUUID() {

	assert := s.Require()
	for pass := 0; pass < 2; pass++ {

		// test updating outputhost works
		// Now create the cge and retry the update
		destUUID := uuid.New()
		extUUID := uuid.New()
		cgUUID := uuid.New()
		outputUUID := uuid.New()
		createReq := &shared.CreateConsumerGroupExtentRequest{
			DestinationUUID:   common.StringPtr(destUUID),
			ExtentUUID:        common.StringPtr(extUUID),
			ConsumerGroupUUID: common.StringPtr(cgUUID),
			OutputHostUUID:    common.StringPtr(outputUUID),
		}

		createReq.StoreUUIDs = make([]string, 0, 3)
		for i := 0; i < 3; i++ {
			createReq.StoreUUIDs = append(createReq.StoreUUIDs, uuid.New())
		}

		err := s.client.CreateConsumerGroupExtent(nil, createReq)
		assert.Nil(err, "CreateConsumerGroupExtent() call failed")

		reqExt := &m.ReadConsumerGroupExtentsByExtUUIDRequest{
			ExtentUUID: common.StringPtr(extUUID),
			Limit:      common.Int64Ptr(500),
		}

		mResp, err1 := s.client.ReadConsumerGroupExtentsByExtUUID(nil, reqExt)
		assert.Nil(err1, "ReadConsumerGroupExtentsByExtUUID call failed")
		assert.Equal(1, len(mResp.GetCgExtents()), "ReadConsumerGroupExtentsByExtUUID return wrong size")
		got := mResp.GetCgExtents()[0]
		expected := &shared.ConsumerGroupExtent{
			ConsumerGroupUUID: common.StringPtr(cgUUID),
			ExtentUUID:        common.StringPtr(extUUID),
			OutputHostUUID:    common.StringPtr(outputUUID),
			Status:            common.MetadataConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_OPEN),
			StoreUUIDs:        createReq.StoreUUIDs,
		}
		assert.Equal(expected.GetConsumerGroupUUID(), got.GetConsumerGroupUUID(), "Wrong consumer group uuid")
		assert.Equal(expected.GetExtentUUID(), got.GetExtentUUID(), "Wrong extent uuid")
		assert.Equal(expected.GetOutputHostUUID(), got.GetOutputHostUUID(), "Wrong out host uuid")
		assert.Equal(expected.GetAckLevelOffset(), got.GetAckLevelOffset(), "Wrong AckLevelOffset")
		assert.Equal(expected.GetStatus(), got.GetStatus(), "Wrong Status")
		assert.Equal(expected.GetConnectedStoreUUID(), got.GetConnectedStoreUUID(), "Wrong connectedStoreUUID")
		assert.Equal(expected.GetAckLevelSeqNo(), got.GetAckLevelSeqNo(), "Wrong ackLevelSeqNo")
		assert.Equal(expected.GetAckLevelSeqNoRate(), got.GetAckLevelSeqNoRate(), "Wrong ackLevelSeqNoRate")
		assert.Equal(expected.GetReadLevelOffset(), got.GetReadLevelOffset(), "Wrong readLevelOffset")
		assert.Equal(expected.GetReadLevelSeqNo(), got.GetReadLevelSeqNo(), "Wrong readLevelSeqNo")
		assert.Equal(expected.GetReadLevelSeqNoRate(), got.GetReadLevelSeqNoRate(), "Wrong readLevelSeqNoRate")

	}
}
func (s *CassandraSuite) TestSetAckOffset() {

	assert := s.Require()

	req := &shared.SetAckOffsetRequest{
		ExtentUUID:         common.StringPtr(uuid.New()),
		ConsumerGroupUUID:  common.StringPtr(uuid.New()),
		OutputHostUUID:     common.StringPtr(uuid.New()),
		ConnectedStoreUUID: common.StringPtr(uuid.New()),
		Status:             common.CheramiConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_OPEN),
		AckLevelAddress:    common.Int64Ptr(1234),
		AckLevelSeqNo:      common.Int64Ptr(2345),
		AckLevelSeqNoRate:  common.Float64Ptr(34.56),
		ReadLevelAddress:   common.Int64Ptr(4567),
		ReadLevelSeqNo:     common.Int64Ptr(5678),
		ReadLevelSeqNoRate: common.Float64Ptr(67.89),
	}

	// test auto creation of cge works
	err := s.client.SetAckOffset(nil, req)
	assert.Nil(err, "SetAckOffset failed")

	readCgeReq := &m.ReadConsumerGroupExtentRequest{
		DestinationUUID:   common.StringPtr(uuid.New()),
		ExtentUUID:        common.StringPtr(req.GetExtentUUID()),
		ConsumerGroupUUID: common.StringPtr(req.GetConsumerGroupUUID()),
	}

	_, err = s.client.ReadConsumerGroupExtent(nil, readCgeReq)
	assert.NotNil(err, "SetAckOffset failed to auto-create consumer group extent")

	for pass := 0; pass < 2; pass++ {
		log.Debugf(`pass %d`, pass)
		// Now create the cge and retry the update
		createReq := &shared.CreateConsumerGroupExtentRequest{
			DestinationUUID:   common.StringPtr(uuid.New()),
			ExtentUUID:        common.StringPtr(req.GetExtentUUID()),
			ConsumerGroupUUID: common.StringPtr(req.GetConsumerGroupUUID()),
			OutputHostUUID:    common.StringPtr(req.GetOutputHostUUID()),
		}

		createReq.StoreUUIDs = make([]string, 0, 3)
		for i := 0; i < 3; i++ {
			createReq.StoreUUIDs = append(createReq.StoreUUIDs, uuid.New())
		}

		err = s.client.CreateConsumerGroupExtent(nil, createReq)
		assert.Nil(err, "CreateConsumerGroupExtent() call failed")

		expected := &shared.ConsumerGroupExtent{
			ConsumerGroupUUID:  common.StringPtr(req.GetConsumerGroupUUID()),
			ExtentUUID:         common.StringPtr(req.GetExtentUUID()),
			OutputHostUUID:     common.StringPtr(req.GetOutputHostUUID()),
			Status:             common.MetadataConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_OPEN),
			StoreUUIDs:         createReq.StoreUUIDs,
			ConnectedStoreUUID: common.StringPtr(req.GetConnectedStoreUUID()),
			AckLevelOffset:     common.Int64Ptr(req.GetAckLevelAddress()),
			AckLevelSeqNo:      common.Int64Ptr(req.GetAckLevelSeqNo()),
			AckLevelSeqNoRate:  common.Float64Ptr(req.GetAckLevelSeqNoRate()),
			ReadLevelOffset:    common.Int64Ptr(req.GetReadLevelAddress()),
			ReadLevelSeqNo:     common.Int64Ptr(req.GetReadLevelSeqNo()),
			ReadLevelSeqNoRate: common.Float64Ptr(req.GetReadLevelSeqNoRate()),
		}

		var got *m.ReadConsumerGroupExtentResult_
		got, err = s.client.ReadConsumerGroupExtent(nil, readCgeReq)
		assert.Nil(err, "SetAckOffset failed to update consumer group extent")
		assertConsumerGroupExtentEqual(s, expected, got.GetExtent())

		*req.AckLevelAddress = 1111
		*req.AckLevelSeqNo = 2222
		*req.AckLevelSeqNoRate = 33.33
		*req.ReadLevelAddress = 4444
		*req.ReadLevelSeqNo = 5555
		*req.ReadLevelSeqNoRate = 66.66

		expected = &shared.ConsumerGroupExtent{
			ConsumerGroupUUID:  common.StringPtr(req.GetConsumerGroupUUID()),
			ExtentUUID:         common.StringPtr(req.GetExtentUUID()),
			OutputHostUUID:     common.StringPtr(req.GetOutputHostUUID()),
			Status:             common.MetadataConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_OPEN),
			StoreUUIDs:         createReq.StoreUUIDs,
			ConnectedStoreUUID: common.StringPtr(req.GetConnectedStoreUUID()),
			AckLevelOffset:     common.Int64Ptr(req.GetAckLevelAddress()),
			AckLevelSeqNo:      common.Int64Ptr(req.GetAckLevelSeqNo()),
			AckLevelSeqNoRate:  common.Float64Ptr(req.GetAckLevelSeqNoRate()),
			ReadLevelOffset:    common.Int64Ptr(req.GetReadLevelAddress()),
			ReadLevelSeqNo:     common.Int64Ptr(req.GetReadLevelSeqNo()),
			ReadLevelSeqNoRate: common.Float64Ptr(req.GetReadLevelSeqNoRate()),
		}

		// test updating cge works
		err = s.client.SetAckOffset(nil, req)
		assert.Nil(err, "SetAckOffset failed")

		got, err = s.client.ReadConsumerGroupExtent(nil, readCgeReq)
		assert.Nil(err, "SetAckOffset failed to update consumer group extent")
		assertConsumerGroupExtentEqual(s, expected, got.GetExtent())

		if pass == 0 {
			s.Nil(s.Alter(), "ALTER table failed")
		}
	}
}

func (s *CassandraSuite) TestSetAckOffsetWithoutOutputAndStore() {
	assert := s.Require()

	destUUID := uuid.New()
	cgUUID := uuid.New()
	extUUID := uuid.New()
	outputHost := uuid.New()
	connectedStore := uuid.New()
	storeUUIDs := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		storeUUIDs = append(storeUUIDs, uuid.New())
	}

	// First create the cg extent
	createReq := &shared.CreateConsumerGroupExtentRequest{
		DestinationUUID:   common.StringPtr(destUUID),
		ExtentUUID:        common.StringPtr(extUUID),
		ConsumerGroupUUID: common.StringPtr(cgUUID),
		OutputHostUUID:    common.StringPtr(outputHost),
		StoreUUIDs:        storeUUIDs,
	}

	err := s.client.CreateConsumerGroupExtent(nil, createReq)
	assert.Nil(err, "CreateConsumerGroupExtent() call failed")

	// set ack level
	setAckReq := &shared.SetAckOffsetRequest{
		ExtentUUID:         common.StringPtr(extUUID),
		ConsumerGroupUUID:  common.StringPtr(cgUUID),
		OutputHostUUID:     common.StringPtr(outputHost),
		ConnectedStoreUUID: common.StringPtr(connectedStore),
		Status:             common.CheramiConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_OPEN),
		AckLevelAddress:    common.Int64Ptr(1234),
		AckLevelSeqNo:      common.Int64Ptr(2345),
		AckLevelSeqNoRate:  common.Float64Ptr(34.56),
		ReadLevelAddress:   common.Int64Ptr(4567),
		ReadLevelSeqNo:     common.Int64Ptr(5678),
		ReadLevelSeqNoRate: common.Float64Ptr(67.89),
	}

	err = s.client.SetAckOffset(nil, setAckReq)
	assert.Nil(err, "SetAckOffset failed")

	expected := &shared.ConsumerGroupExtent{
		ConsumerGroupUUID:  common.StringPtr(cgUUID),
		ExtentUUID:         common.StringPtr(extUUID),
		OutputHostUUID:     common.StringPtr(outputHost),
		Status:             common.MetadataConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_OPEN),
		StoreUUIDs:         storeUUIDs,
		ConnectedStoreUUID: common.StringPtr(connectedStore),
		AckLevelOffset:     common.Int64Ptr(setAckReq.GetAckLevelAddress()),
		AckLevelSeqNo:      common.Int64Ptr(setAckReq.GetAckLevelSeqNo()),
		AckLevelSeqNoRate:  common.Float64Ptr(setAckReq.GetAckLevelSeqNoRate()),
		ReadLevelOffset:    common.Int64Ptr(setAckReq.GetReadLevelAddress()),
		ReadLevelSeqNo:     common.Int64Ptr(setAckReq.GetReadLevelSeqNo()),
		ReadLevelSeqNoRate: common.Float64Ptr(setAckReq.GetReadLevelSeqNoRate()),
	}

	readReq := &m.ReadConsumerGroupExtentRequest{
		DestinationUUID:   common.StringPtr(destUUID),
		ExtentUUID:        common.StringPtr(extUUID),
		ConsumerGroupUUID: common.StringPtr(cgUUID),
	}
	var got *m.ReadConsumerGroupExtentResult_
	got, err = s.client.ReadConsumerGroupExtent(nil, readReq)
	assert.Nil(err, "SetAckOffset failed to update consumer group extent")
	assertConsumerGroupExtentEqual(s, expected, got.GetExtent())

	// set ack without output host and connected store
	*setAckReq.AckLevelAddress = 1111
	setAckReq.OutputHostUUID = nil
	setAckReq.ConnectedStoreUUID = nil
	err = s.client.SetAckOffset(nil, setAckReq)
	assert.Nil(err, "SetAckOffset failed")

	expected.AckLevelOffset = common.Int64Ptr(setAckReq.GetAckLevelAddress())
	got, err = s.client.ReadConsumerGroupExtent(nil, readReq)
	assert.Nil(err, "SetAckOffset failed to update consumer group extent")
	assertConsumerGroupExtentEqual(s, expected, got.GetExtent())

	// set ack again, still without output host and connected store, but only the status
	setAckReq.Status = common.MetadataConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_CONSUMED)
	err = s.client.SetAckOffset(nil, setAckReq)
	assert.Nil(err, "SetAckOffset failed")

	expected.Status = common.MetadataConsumerGroupExtentStatusPtr(shared.ConsumerGroupExtentStatus_CONSUMED)
	got, err = s.client.ReadConsumerGroupExtent(nil, readReq)
	assert.Nil(err, "SetAckOffset failed to update consumer group extent")
	assertConsumerGroupExtentEqual(s, expected, got.GetExtent())
}

func (s *CassandraSuite) TestGetConsumerGroupExtents() {

	assert := s.Require()

	req := &shared.CreateConsumerGroupExtentRequest{
		DestinationUUID:   common.StringPtr(uuid.New()),
		ConsumerGroupUUID: common.StringPtr(uuid.New()),
	}

	storesMap := make(map[string]struct{})
	req.StoreUUIDs = make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		id := uuid.New()
		req.StoreUUIDs = append(req.StoreUUIDs, id)
		storesMap[id] = struct{}{}
	}

	const nExtentsPerOutputHost = 10
	const nOutputHosts = 3

	extents := make(map[string]bool)
	var outputhosts [nOutputHosts]string

	for c := 0; c < nOutputHosts; c++ {
		outputhosts[c] = uuid.New()
		req.OutputHostUUID = common.StringPtr(outputhosts[c])
		for i := 0; i < nExtentsPerOutputHost; i++ {
			req.ExtentUUID = common.StringPtr(uuid.New())
			err := s.client.CreateConsumerGroupExtent(nil, req)
			assert.Nil(err, "SetOutputHost() call failed")
			extents[req.GetExtentUUID()] = true
		}

		if c == nOutputHosts/2 {
			s.Nil(s.Alter(), "ALTER table failed")
		}
	}

	readCGExts := func() {

		readReq := &shared.ReadConsumerGroupExtentsRequest{
			DestinationUUID:   common.StringPtr(req.GetDestinationUUID()),
			ConsumerGroupUUID: common.StringPtr(req.GetConsumerGroupUUID()),
			MaxResults:        common.Int32Ptr(nExtentsPerOutputHost),
		}

		validExts := make(map[string]bool)
		for item := range extents {
			validExts[item] = true
		}

		for i := 0; i < nOutputHosts; i++ {
			readReq.OutputHostUUID = common.StringPtr(outputhosts[i])
			readReq.PageToken = nil

			var cgExtents []*shared.ConsumerGroupExtent
			for {
				ans, err := s.client.ReadConsumerGroupExtents(nil, readReq)
				assert.Nil(err, "ReadConsumerGroupExtents failed")
				cgExtents = append(cgExtents, ans.Extents...)

				if len(ans.GetNextPageToken()) == 0 {
					break
				} else {
					readReq.PageToken = ans.GetNextPageToken()
				}
			}

			assert.Equal(nExtentsPerOutputHost, len(cgExtents), "Wrong number of extents for outputhost")
			for n := 0; n < nExtentsPerOutputHost; n++ {
				_, ok := validExts[cgExtents[n].GetExtentUUID()]
				assert.True(ok, "Unknown extent returned for out host")
				delete(validExts, cgExtents[n].GetExtentUUID())
				storesCopy := make(map[string]struct{})
				for id := range storesMap {
					storesCopy[id] = struct{}{}
				}
				for _, id := range cgExtents[n].GetStoreUUIDs() {
					_, ok := storesCopy[id]
					s.True(ok, "ReadConsumerGroupExtentsLite() returned unknown store host")
					delete(storesCopy, id)
				}
			}
		}

		assert.Equal(0, len(validExts), "Not all extents were returned by ReadConsumerGroupExtents")
		// try reading an unknown consumer group
		readReq.ConsumerGroupUUID = common.StringPtr(uuid.New())
		ans, _ := s.client.ReadConsumerGroupExtents(nil, readReq)
		assert.Equal(0, len(ans.Extents), "ReadConsumerGroupExtents must return no results, but it did")
	}

	readCGExtsLite := func() {

		readReq := &m.ReadConsumerGroupExtentsLiteRequest{
			DestinationUUID:   common.StringPtr(req.GetDestinationUUID()),
			ConsumerGroupUUID: common.StringPtr(req.GetConsumerGroupUUID()),
			MaxResults:        common.Int32Ptr(nExtentsPerOutputHost),
		}

		for i := 0; i < nOutputHosts; i++ {
			readReq.OutputHostUUID = common.StringPtr(outputhosts[i])
			readReq.PageToken = nil

			var cgExtents []*m.ConsumerGroupExtentLite
			for {
				ans, err := s.client.ReadConsumerGroupExtentsLite(nil, readReq)
				assert.Nil(err, "ReadConsumerGroupExtents failed")
				cgExtents = append(cgExtents, ans.Extents...)

				if len(ans.GetNextPageToken()) == 0 {
					break
				} else {
					readReq.PageToken = ans.GetNextPageToken()
				}
			}

			assert.Equal(nExtentsPerOutputHost, len(cgExtents), "Wrong number of extents for outputhost")
			for n := 0; n < nExtentsPerOutputHost; n++ {
				_, ok := extents[cgExtents[n].GetExtentUUID()]
				assert.True(ok, "Unknown extent returned for out host")
				delete(extents, cgExtents[n].GetExtentUUID())
				storesCopy := make(map[string]struct{})
				for id := range storesMap {
					storesCopy[id] = struct{}{}
				}
				for _, id := range cgExtents[n].GetStoreUUIDs() {
					_, ok := storesCopy[id]
					s.True(ok, "ReadConsumerGroupExtentsLite() returned unknown store host")
					delete(storesCopy, id)
				}
			}
		}

		assert.Equal(0, len(extents), "Not all extents were returned by ReadConsumerGroupExtentsLite")
		// try reading an unknown consumer group
		readReq.ConsumerGroupUUID = common.StringPtr(uuid.New())
		ans, _ := s.client.ReadConsumerGroupExtentsLite(nil, readReq)
		assert.Equal(0, len(ans.Extents), "ReadConsumerGroupExtentsLite must return no results, but it did")
	}

	readCGExts()
	readCGExtsLite()
}

func (s *CassandraSuite) TestUpdateConsumerGroupExtentStatus() {

	assert := s.Require()

	cReq := shared.NewCreateConsumerGroupExtentRequest()
	cReq.DestinationUUID = common.StringPtr(uuid.New())
	cReq.ExtentUUID = common.StringPtr(uuid.New())
	cReq.ConsumerGroupUUID = common.StringPtr(uuid.New())
	cReq.OutputHostUUID = common.StringPtr(uuid.New())
	cReq.StoreUUIDs = []string{uuid.New(), uuid.New(), uuid.New()}

	connectedStores := []*string{common.StringPtr(cReq.StoreUUIDs[0]), nil}

	for _, cs := range connectedStores {

		cReq.DestinationUUID = common.StringPtr(uuid.New())

		err := s.client.CreateConsumerGroupExtent(nil, cReq)
		assert.Nil(err, "Failed to create consumer group extent")

		setReq := &shared.SetAckOffsetRequest{
			ConsumerGroupUUID:  common.StringPtr(cReq.GetConsumerGroupUUID()),
			ExtentUUID:         common.StringPtr(cReq.GetExtentUUID()),
			OutputHostUUID:     common.StringPtr(cReq.GetOutputHostUUID()),
			ConnectedStoreUUID: cs,
		}

		err = s.client.SetAckOffset(nil, setReq)
		assert.Nil(err, "Failed to update consumer group extent")

		getReq := m.NewReadConsumerGroupExtentRequest()
		getReq.DestinationUUID = cReq.DestinationUUID
		getReq.ConsumerGroupUUID = cReq.ConsumerGroupUUID
		getReq.ExtentUUID = cReq.ExtentUUID

		cge, err := s.client.ReadConsumerGroupExtent(nil, getReq)
		assert.Nil(err, "Failed to read consumer group extent")
		assert.Equal(shared.ConsumerGroupExtentStatus_OPEN, cge.GetExtent().GetStatus(), "Wrong consumer group extent status")

		updateReq := shared.NewUpdateConsumerGroupExtentStatusRequest()
		updateReq.ConsumerGroupUUID = cReq.ConsumerGroupUUID
		updateReq.ExtentUUID = cReq.ExtentUUID

		statusChanges := []shared.ConsumerGroupExtentStatus{shared.ConsumerGroupExtentStatus_CONSUMED, shared.ConsumerGroupExtentStatus_DELETED}

		for _, status := range statusChanges {
			updateReq.Status = common.MetadataConsumerGroupExtentStatusPtr(status)
			err = s.client.UpdateConsumerGroupExtentStatus(nil, updateReq)
			assert.Nil(err, "Failed to update consumer group extent status")
			cge, err = s.client.ReadConsumerGroupExtent(nil, getReq)
			assert.Nil(err, "Failed to read consumer group extent")
			assert.Equal(status, cge.GetExtent().GetStatus(), "Wrong consumer group extent status")
			assert.Equal(cReq.GetOutputHostUUID(), cge.GetExtent().GetOutputHostUUID(), "Wrong output host uuid after status update")
		}
	}
}

func (s *CassandraSuite) TestUUIDToHostAddrMapping() {

	var nEntries = 25
	var uuidToHostAddr = make(map[string]string)

	for pass := 0; pass < 2; pass++ {

		for i := pass * nEntries; i < (pass+1)*nEntries; i++ {
			u := uuid.New()
			addr := "127.0." + strconv.Itoa(i) + ".1:8080"
			uuidToHostAddr[u] = addr
			req := &m.RegisterHostUUIDRequest{
				HostUUID:   common.StringPtr(u),
				HostAddr:   common.StringPtr(addr),
				HostName:   common.StringPtr("host-" + addr),
				TtlSeconds: common.Int64Ptr(60),
			}
			err := s.client.RegisterHostUUID(nil, req)
			s.Nil(err, "Failed to register host UUID")
		}

		for k, v := range uuidToHostAddr {
			addr, err := s.client.UUIDToHostAddr(nil, k)
			s.Nil(err, "UUIDToHostID lookup failed")
			s.Equal(v, addr, "Wrong hostid for uuid")
			u, err := s.client.HostAddrToUUID(nil, v)
			s.Nil(err, "HostAddrToUUID lookup failed")
			s.Equal(k, u, "Wrong uuid for host addr")
		}

		if pass == 0 {
			s.Nil(s.Alter(), "ALTER table failed")
		}
	}
}

func (s *CassandraSuite) TestListHosts() {

	var nEntries = 25
	var uuidToHostAddr = make(map[string]string)

	for pass := 0; pass < 2; pass++ {

		for i := pass * nEntries; i < (pass+1)*nEntries; i++ {
			u := uuid.New()
			addr := "127.0." + strconv.Itoa(i) + ".1:8080"
			uuidToHostAddr[u] = addr
			req := &m.RegisterHostUUIDRequest{
				HostUUID:   common.StringPtr(u),
				HostAddr:   common.StringPtr(addr),
				HostName:   common.StringPtr("listhosts-" + addr),
				TtlSeconds: common.Int64Ptr(60),
			}
			err := s.client.RegisterHostUUID(nil, req)
			s.Nil(err, "Failed to register host UUID")
		}
	}

	reqType := m.HostType_HOST
	listReq := &m.ListHostsRequest{
		HostType: &reqType,
		Limit:    common.Int64Ptr(100),
	}
	hostRes, err := s.client.ListHosts(nil, listReq)
	s.Nil(err, "ListHosts lookup failed")
	numHosts := 0
	for _, desc := range hostRes.GetHosts() {
		hostUUID := desc.GetHostUUID()
		if hostAddr, ok := uuidToHostAddr[hostUUID]; ok {
			numHosts++
			s.Equal(hostAddr, desc.GetHostAddr(), fmt.Sprintf("hostAddr is not same for hostUUID %v ", hostUUID))
		}

	}
	s.Equal(len(uuidToHostAddr), numHosts, "Wrong size for the host list")
}

func (s *CassandraSuite) TestUUIDResolver() {
	var nEntries = 25
	var uuidToHostAddr = make(map[string]string)

	for pass := 0; pass < 2; pass++ {
		for i := pass * nEntries; i < (pass+1)*nEntries; i++ {
			u := uuid.New()
			hostid := "127.0." + strconv.Itoa(i) + ".1:8080"
			uuidToHostAddr[u] = hostid
			req := &m.RegisterHostUUIDRequest{
				HostUUID:   common.StringPtr(u),
				HostAddr:   common.StringPtr(hostid),
				HostName:   common.StringPtr("host-" + hostid),
				TtlSeconds: common.Int64Ptr(60),
			}
			err := s.client.RegisterHostUUID(nil, req)
			s.Nil(err, "RegisterHostID failed")
		}

		resolver := common.NewUUIDResolver(s.client)

		for k, v := range uuidToHostAddr {
			addr, err := resolver.Lookup(k)
			s.Nil(err, "Resolver failed to resolve uuid")
			s.Equal(v, addr, "Wrong addr resolved for uuid")
			u, err := resolver.ReverseLookup(v)
			s.Nil(err, "Resolver failed to reverse resolve addr")
			s.Equal(k, u, "Wrong uuid resolved for addr")
		}

		// test cache lookup
		for k, v := range uuidToHostAddr {
			addr, err := resolver.Lookup(k)
			s.Nil(err, "Resolver failed to resolve uuid from cache")
			s.Equal(v, addr, "Wrong addr resolved for uuid")
			u, err := resolver.ReverseLookup(v)
			s.Nil(err, "Resolver failed to reverse resolve addr from cache")
			s.Equal(k, u, "Wrong uuid resolved for addr")
		}

		if pass == 0 {
			s.Nil(s.Alter(), "ALTER table failed")
		}
	}

}

func (s *CassandraSuite) TestDeleteExtent() {

	dstPath := s.generateName("/cherami/test")
	dst, err := createDestination(s, dstPath, false)
	s.Nil(err, "CreateDestination failed")

	cExtent := func() *shared.CreateExtentResult_ {
		extentUUID := uuid.New()
		storeIds := []string{uuid.New(), uuid.New(), uuid.New()}
		extent := &shared.Extent{
			ExtentUUID:      common.StringPtr(extentUUID),
			DestinationUUID: common.StringPtr(dst.GetDestinationUUID()),
			StoreUUIDs:      storeIds,
			InputHostUUID:   common.StringPtr(uuid.New()),
		}
		createExtent := &shared.CreateExtentRequest{Extent: extent}
		nowMillis := time.Now().UnixNano() / int64(time.Millisecond)
		cResp, err := s.client.CreateExtent(nil, createExtent)
		s.Nil(err, "Creation of extent failed")
		s.Equal(shared.ExtentStatus_OPEN, cResp.GetExtentStats().GetStatus(), "Wrong extent status")
		diffMillis := (nowMillis - cResp.GetExtentStats().GetCreatedTimeMillis()) / int64(time.Millisecond)
		s.True(diffMillis < 60000, "Extent created with wrong created time")
		return cResp
	}

	createResp := cExtent()
	extent := createResp.GetExtentStats().GetExtent()

	updateReq := &m.UpdateExtentStatsRequest{
		DestinationUUID: common.StringPtr(dst.GetDestinationUUID()),
		ExtentUUID:      common.StringPtr(extent.GetExtentUUID()),
		Status:          common.MetadataExtentStatusPtr(shared.ExtentStatus_DELETED),
	}

	t0 := time.Now().UnixNano() / int64(time.Millisecond)
	res, err := s.client.UpdateExtentStats(nil, updateReq)
	tX := time.Now().UnixNano() / int64(time.Millisecond)

	s.Nil(err, "Failed to delete extent")
	s.Equal(createResp.GetExtentStats().GetCreatedTimeMillis(), res.GetExtentStats().GetCreatedTimeMillis(), "Wrong created time")

	readReq := &m.ReadExtentStatsRequest{
		DestinationUUID: common.StringPtr(dst.GetDestinationUUID()),
		ExtentUUID:      common.StringPtr(extent.GetExtentUUID()),
	}

	resp, err := s.client.ReadExtentStats(nil, readReq)
	s.Nil(err, "ReadExtentStats failed")
	s.Equal(createResp.GetExtentStats().GetCreatedTimeMillis(), resp.GetExtentStats().GetCreatedTimeMillis(), "Wrong created time")
	log.Debugf("Got extent time %d", resp.GetExtentStats().GetCreatedTimeMillis())
	gotExtent := resp.GetExtentStats().GetExtent()
	s.Equal(extent.GetInputHostUUID(), gotExtent.GetInputHostUUID(), "Wrong input host")
	s.Equal(shared.ExtentStatus_DELETED, resp.GetExtentStats().GetStatus(), "Wrong extent status")
	s.True(resp.GetExtentStats().GetStatusUpdatedTimeMillis() >= t0, "Incorrect StatusUpdatedTime")
	s.True(resp.GetExtentStats().GetStatusUpdatedTimeMillis() <= tX, "Incorrect StatusUpdatedTime")
	s.Equal(len(extent.GetStoreUUIDs()), len(gotExtent.GetStoreUUIDs()), "Wrong number of store hosts")
	for _, store := range gotExtent.GetStoreUUIDs() {
		found := false
		for _, s := range extent.GetStoreUUIDs() {
			if strings.Compare(s, store) == 0 {
				found = true
				break
			}
		}
		s.True(found, "Wrong store host after extent update")
	}

}

func (s *CassandraSuite) TestSealExtent() {

	dstPath := s.generateName("/cherami/test")
	dst, err := createDestination(s, dstPath, false)
	s.Nil(err, "CreateDestination failed")

	cExtent := func() *shared.Extent {
		extentUUID := uuid.New()
		storeIds := []string{uuid.New(), uuid.New(), uuid.New()}
		extent := &shared.Extent{
			ExtentUUID:      common.StringPtr(extentUUID),
			DestinationUUID: common.StringPtr(dst.GetDestinationUUID()),
			StoreUUIDs:      storeIds,
			InputHostUUID:   common.StringPtr(uuid.New()),
		}

		createExtent := &shared.CreateExtentRequest{Extent: extent}
		cResp, err := s.client.CreateExtent(nil, createExtent)
		s.Nil(err, "Creation of extent failed")
		s.Equal(shared.ExtentStatus_OPEN, cResp.GetExtentStats().GetStatus(), "Wrong extent status")
		return extent
	}

	extent := cExtent()

	archivalLoc := "archivalLoc"

	for pass := 0; pass < 2; pass++ {

		updateReq := &m.UpdateExtentStatsRequest{
			DestinationUUID:  common.StringPtr(dst.GetDestinationUUID()),
			ExtentUUID:       common.StringPtr(extent.GetExtentUUID()),
			ArchivalLocation: common.StringPtr(archivalLoc),
		}

		_, err = s.client.UpdateExtentStats(nil, updateReq)
		s.Nil(err, "Failed to update archival location")

		sealReq := &m.SealExtentRequest{
			DestinationUUID: common.StringPtr(dst.GetDestinationUUID()),
			ExtentUUID:      common.StringPtr(extent.GetExtentUUID()),
		}

		err = s.client.SealExtent(nil, sealReq)
		s.Nil(err, "Failed to seal extent")

		err = s.client.SealExtent(nil, sealReq)
		s.Nil(err, "SealExtent() not expected fail on previously sealed extent")

		// make sure archival location and other fields are untouched
		readReq := &m.ReadExtentStatsRequest{
			DestinationUUID: common.StringPtr(dst.GetDestinationUUID()),
			ExtentUUID:      common.StringPtr(extent.GetExtentUUID()),
		}

		resp, err := s.client.ReadExtentStats(nil, readReq)
		s.Nil(err, "ReadExtentStats failed")
		gotExtent := resp.GetExtentStats().GetExtent()
		s.Equal(extent.GetInputHostUUID(), gotExtent.GetInputHostUUID(), "Wrong input host")
		s.Equal(archivalLoc, resp.GetExtentStats().GetArchivalLocation(), "Wrong archival location")
		s.Equal(shared.ExtentStatus_SEALED, resp.GetExtentStats().GetStatus(), "Wrong extent status")
		s.Equal(len(extent.GetStoreUUIDs()), len(gotExtent.GetStoreUUIDs()), "Wrong number of store hosts")

		for _, store := range gotExtent.GetStoreUUIDs() {
			found := false
			for _, s := range extent.GetStoreUUIDs() {
				if strings.Compare(s, store) == 0 {
					found = true
					break
				}
			}
			s.True(found, "Wrong store host after extent update")
		}

		if pass == 0 {
			s.Nil(s.Alter(), "ALTER table failed")
		} else if pass == 1 {
			extent = cExtent()
		} else {
			updateReq.Status = common.MetadataExtentStatusPtr(shared.ExtentStatus_CONSUMED)
			_, err = s.client.UpdateExtentStats(nil, updateReq)
			s.Nil(err, "UpdateExtentStats() failed")

			err = s.client.SealExtent(nil, sealReq)
			s.NotNil(err, "SealExtent() expected to fail on a consumed extent")
		}
	}
}

func (s *CassandraSuite) TestCreateAndReadStoreExtent() {

	dstPath := s.generateName("/cherami/testStore")
	dst, err := createDestination(s, dstPath, false)
	s.Nil(err, "CreateDestination failed")

	storeIds := []string{uuid.New(), uuid.New(), uuid.New()}
	cExtent := func() *shared.Extent {
		extentUUID := uuid.New()
		extent := &shared.Extent{
			ExtentUUID:      common.StringPtr(extentUUID),
			DestinationUUID: common.StringPtr(dst.GetDestinationUUID()),
			StoreUUIDs:      storeIds,
			InputHostUUID:   common.StringPtr(uuid.New()),
		}

		createExtent := &shared.CreateExtentRequest{Extent: extent}
		cResp, err := s.client.CreateExtent(nil, createExtent)
		s.Nil(err, "Creation of extent failed")
		s.Equal(shared.ExtentStatus_OPEN, cResp.GetExtentStats().GetStatus(), "Wrong extent status")
		return extent
	}

	extent := cExtent()
	mReq := &m.ReadStoreExtentReplicaStatsRequest{
		StoreUUID:  common.StringPtr(storeIds[1]),
		ExtentUUID: common.StringPtr(extent.GetExtentUUID()),
	}

	result, err := s.client.ReadStoreExtentReplicaStats(nil, mReq)
	s.Nil(err, "Reading store extent stats failed")
	s.NotNil(result, "ReadStoreExtentReplicaStats returned nil")
}
