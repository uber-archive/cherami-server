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
	"bytes"
	"fmt"
	"io/ioutil"
	golangLog "log"
	"math/rand"
	"os/exec"
	"strings"
	"time"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-server/common/configure"
	m "github.com/uber/cherami-thrift/.generated/go/metadata"
)

// this file contains all the utility routines

// DropKeyspace drops the given keyspace, if it exists
func DropKeyspace(s *gocql.Session, keyspace string) (err error) {
	err = s.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", keyspace)).Exec()
	if err != nil {
		log.WithField(common.TagErr, err).Error(`drop keyspace error`)
	}
	log.WithField(`keyspace`, keyspace).Info(`dropped namespace`)
	return
}

// CreateKeyspace creates the keyspace on this session with the given replicas
func CreateKeyspace(s *gocql.Session, keyspace string, replicas int, overwrite bool) (err error) {
	// if overwrite flag is set, drop the keyspace and create a new one
	if overwrite {
		err = s.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", keyspace)).Exec()
		if err != nil {
			log.WithField(common.TagErr, err).Error(`drop keyspace error`)
		}
	}
	err = s.Query(fmt.Sprintf(`CREATE KEYSPACE %s WITH replication = {
		'class' : 'SimpleStrategy', 'replication_factor' : %d}`, keyspace, replicas)).Exec()
	if err != nil {
		log.WithField(common.TagErr, err).Error(`create keyspace error`)
	}
	log.WithField(`keyspace`, keyspace).Info(`created namespace`)

	return
}

// LoadSchema loads the schema from the given .cql file on this keyspace using cqlsh
func LoadSchema(cqlshpath string, fileName string, keyspace string) (err error) {
	// Using cqlsh as I couldn't find a way to execute multiple commands through gocql.Session
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd := exec.Command(cqlshpath,
		"--username=cassandra",
		"--password=cassandra",
		fmt.Sprintf("--keyspace=%v", keyspace),
		fmt.Sprintf("--file=%v", fileName),
		`127.0.0.1`)
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err = cmd.Run()

	// CQLSH doesn't return non-zero for some errors
	if err != nil || strings.Contains(stderr.String(), `Errno`) {
		err = fmt.Errorf("LoadSchema %v returned %v. STDERR: %v", cmd.Path, err, stderr.String())
	}
	return
}

func newCluster(clusterHosts string) *gocql.ClusterConfig {
	var hosts []string
	for _, h := range strings.Split(clusterHosts, ",") {
		if host := strings.TrimSpace(h); len(host) > 0 {
			hosts = append(hosts, host)
		}
	}

	return gocql.NewCluster(hosts...)
}

// CreateKeyspaceNoSession is used to create a keyspace when we don't have a session
func CreateKeyspaceNoSession(
	clusterHosts string,
	port int,
	keyspace string,
	replicas int,
	overwrite bool,
	auth configure.Authentication,
) error {
	// open a session to the "system" keyspace just to create the new keyspace
	// TODO: Find out if we can do this "outside" of a session (cqlsh?)
	cluster := newCluster(clusterHosts)
	cluster.Port = port
	cluster.Consistency = gocql.One
	cluster.Keyspace = "system"
	cluster.Timeout = 40 * time.Second
	cluster.ProtoVersion = cassandraProtoVersion
	if auth.Enabled {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: auth.Username,
			Password: auth.Password,
		}
	}
	session, err := cluster.CreateSession()
	if err != nil {
		log.WithField(common.TagErr, err).Error(`CreateKeyspaceNoSession: unable to create session`)
		return err
	}

	// now create the keyspace
	return CreateKeyspace(session, keyspace, replicas, overwrite)
}

//
//  Testing Utilities
//

// TestCluster contains a testing Cassandra cluster and metadata client
type TestCluster struct {
	keyspace string
	cluster  *gocql.ClusterConfig
	session  *gocql.Session
	client   m.TChanMetadataService
}

func generateRandomKeyspace(n int) string {
	rand.Seed(time.Now().UnixNano())
	letterRunes := []rune("cherami")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// SetupTestCluster initializes the test cluster
func (s *TestCluster) SetupTestCluster() {
	golangLog.SetOutput(ioutil.Discard)

	ip := `127.0.0.1`
	port := 9042
	s.createCluster(ip, port, gocql.Consistency(1), generateRandomKeyspace(10))
	s.createKeyspace(1)
	s.loadSchema("schema/metadata.cql")

	auth := configure.Authentication{
		Enabled:  true,
		Username: "cassandra",
		Password: "cassandra",
	}

	var err error
	s.client, err = NewCassandraMetadataService(&configure.MetadataConfig{
		CassandraHosts: ip,
		Port:           port,
		Keyspace:       s.keyspace,
		Consistency:    "One",
		Authentication: auth,
	}, nil)

	if err != nil {
		log.Fatal(err)
	}
}

// TearDownTestCluster cleans up the test cluster
func (s *TestCluster) TearDownTestCluster() {
	s.dropKeyspace()
	s.session.Close()
}

func (s *TestCluster) createCluster(clusterHosts string, port int, cons gocql.Consistency, keyspace string) {
	s.cluster = newCluster(clusterHosts)
	s.cluster.Port = port
	s.cluster.Consistency = cons
	s.cluster.Keyspace = "system"
	s.cluster.Timeout = 40 * time.Second
	s.cluster.ProtoVersion = cassandraProtoVersion
	s.cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}
	var err error
	s.session, err = s.cluster.CreateSession()
	if err != nil {
		log.WithField(common.TagErr, err).Fatal(`createSession`)
	}
	s.keyspace = keyspace
}

func (s *TestCluster) dropKeyspace() {
	err := DropKeyspace(s.session, s.keyspace)
	if err != nil {
		log.Fatal(err)
	}
}

func (s *TestCluster) createKeyspace(replicas int) {
	err := CreateKeyspace(s.session, s.keyspace, replicas, true)
	if err != nil {
		log.Fatal(err)
	}

	s.cluster.Keyspace = s.keyspace
}

func (s *TestCluster) loadSchema(fileName string) {
	err := LoadSchema("/usr/local/bin/cqlsh", fileName, s.keyspace)
	if err != nil {
		err = LoadSchema("/usr/local/bin/cqlsh", "../../clients/metadata/"+fileName, s.keyspace)
	}

	if err != nil {
		log.Fatal(err)
	}
}

// GetClient returns the metadata client interface
func (s *TestCluster) GetClient() m.TChanMetadataService {
	return s.client
}
