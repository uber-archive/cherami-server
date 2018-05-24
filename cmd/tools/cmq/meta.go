package main

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/gocql/gocql"
	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"
)

type opts struct {
	Hosts       string
	Port        int
	Keyspace    string
	Consistency string
	Username    string
	Password    string
	Timeout     time.Duration
	Retries     int
	PageSize    int
}

type metadataClient struct {
	session     *gocql.Session
	consistency gocql.Consistency
	retries     int
	pageSize    int
}

const (
	protocolVersion = 4
	numConns        = 1
)

func newMetadataClient() (*metadataClient, error) {

	opts := getOpts(cliContext)

	cluster := gocql.NewCluster(opts.Hosts) // TODO: add support for multiple hosts

	cluster.Port = opts.Port
	cluster.Keyspace = opts.Keyspace

	if len(opts.Username) > 0 || len(opts.Password) > 0 {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: opts.Username,
			Password: opts.Password,
		}
	}

	cluster.Consistency = gocql.ParseConsistency(opts.Consistency)

	cluster.NumConns = numConns
	cluster.ProtoVersion = protocolVersion
	cluster.Timeout = opts.Timeout
	cluster.PageSize = opts.PageSize

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("CreateSession: %v", err)
	}

	return &metadataClient{
		session:     session,
		consistency: cluster.Consistency,
		retries:     opts.Retries,
		pageSize:    opts.PageSize,
	}, nil
}

func (m *metadataClient) QueryRow(cql string) (row map[string]interface{}, err error) {

	row = make(map[string]interface{})
	err = m.session.Query(cql).MapScan(row)
	return
}

func (m *metadataClient) Close() {
	m.session.Close()
}

type pagedScanIter struct {
	query *gocql.Query
	iter  *gocql.Iter
}

func (m *metadataClient) pagedQueryIter(cql string) *pagedScanIter {

	query := m.session.Query(cql).Consistency(m.consistency).PageSize(m.pageSize).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: m.retries})
	iter := query.Iter()

	if iter == nil {
		return nil
	}

	return &pagedScanIter{query: query, iter: iter}
}

func (it *pagedScanIter) scan(dest ...interface{}) bool {

	if !it.iter.Scan(dest...) {

		pageState := it.iter.PageState()

		if len(pageState) == 0 {
			return false
		}

		it.iter.Close()

		it.iter = it.query.PageState(pageState).Iter()
	}

	return true
}

func (it *pagedScanIter) close() error {

	err := it.iter.Close()

	if err != nil {
		fmt.Printf("pagedScanIter close error: %v\n", err)
	}

	return err
}

type filter struct {
	dest  string
	cg    string
	ext   string
	where map[string]string
}

func (m *metadataClient) Query(table string, f filter) (row map[string]interface{}, err error) {

	/*
		Query(
			"consumer_group_extents",
			select{"destination.path", "destination.uuid"},
			where{cg: cgUUID, ext: extUUID},
			filter{"destination.status": 5},
		)
	*/

	/*
		row = make(map[string]interface{})

		var where []string

		// err = m.session.Query(cql).MapScan(row)
	*/

	return
}

// ZoneConfig has config for cassandra for each zone
type ZoneConfig struct {
	Hosts    string `yaml:"hosts"` // TODO: add support for multiple hosts
	Port     int    `yaml:"port"`
	Keyspace string `yaml:"keyspace"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

func getOpts(c *cli.Context) *opts {

	var opts = &opts{
		Port:        9042,
		Consistency: "One",
		Keyspace:    "cherami",
	}

	if c.IsSet("zone") {

		if configYaml, err := ioutil.ReadFile(c.String("config")); err == nil {

			config := make(map[string]ZoneConfig)

			if err = yaml.Unmarshal(configYaml, &config); err != nil {

				fmt.Printf("error parsing yaml (%s): %v\n", c.String("config"), err)

			} else {

				if cfg, ok := config[c.String("zone")]; !ok {

					fmt.Printf("config not found for zone '%v'\n", c.String("zone"))

				} else {

					opts.Hosts = cfg.Hosts
					if cfg.Port != 0 {
						opts.Port = cfg.Port
					}
					opts.Keyspace = cfg.Keyspace
					opts.Username = cfg.Username
					opts.Password = cfg.Password
				}
			}
		}
	}

	if c.IsSet("hosts") {
		opts.Hosts = c.String("hosts")
	}

	if c.IsSet("port") {
		opts.Port = c.Int("port")
	}

	if c.IsSet("keyspace") {
		opts.Keyspace = c.String("keyspace")
	}

	if c.IsSet("consistency") {
		opts.Consistency = c.String("consistency")
	}

	if c.IsSet("username") {
		opts.Username = c.String("username")
	}

	if c.IsSet("password") {
		opts.Password = c.String("password")
	}

	if c.IsSet("timeout") {
		opts.Timeout = c.Duration("timeout")
	}

	if c.IsSet("retries") {
		opts.Retries = c.Int("retries")
	}

	if c.IsSet("pagesize") {
		opts.PageSize = c.Int("pagesize")
	}

	return opts
}
