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

package common

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/codegangsta/cli"
	ccli "github.com/uber/cherami-client-go/client/cherami"
	"github.com/uber/cherami-server/clients/inputhost"
	mcli "github.com/uber/cherami-server/clients/metadata"
	"github.com/uber/cherami-server/clients/outputhost"
	"github.com/uber/cherami-server/clients/storehost"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
	"github.com/uber/cherami-thrift/.generated/go/shared"
	"github.com/uber/cherami-thrift/.generated/go/store"
)

// GlobalOptions are options shared by most command line
type GlobalOptions struct {
	hyperbahn              bool
	hyperbahnBootstrapFile string
	env                    string
	frontendHost           string
	frontendPort           int
	timeoutSecs            int
}

const (
	// DefaultPageSize is the default page size for data base access for command line
	DefaultPageSize = 1000

	// UnknownUUID is the suffix added to a host when the host uuid does not exist in cassandra
	UnknownUUID = " UNKNOWN_HOST_UUID"
	// DestinationType is the name for entity type for destination in listEntityOps
	DestinationType = "DST"
	// ConsumerGroupType is the name for entity type for consumer group in listEntityOps
	ConsumerGroupType = "CG"
	// DefaultUnconsumedMessagesRetention is the default value for unconsumed messages retention
	DefaultUnconsumedMessagesRetention = 3 * 24 * 3600 // 3 days
	// DefaultConsumedMessagesRetention is the default value for consumed messages retention
	DefaultConsumedMessagesRetention = 1 * 24 * 3600 // 1 day
	// MinUnconsumedMessagesRetentionForMultiZoneDest is the minimum unconsumed retention allowed
	MinUnconsumedMessagesRetentionForMultiZoneDest = 3 * 24 * 3600
	// MinConsumedMessagesRetention is the minimum consumed retention
	MinConsumedMessagesRetention = 180
	// MaxConsumedMessagesRetention is the maximum consumed retention
	MaxConsumedMessagesRetention = 7 * 24 * 3600
	// MinUnconsumedMessagesRetention is the minimum unconsumed retention
	MinUnconsumedMessagesRetention = 180
	// MaxUnconsumedMessagesRetention is the maximum unconsumed retention
	MaxUnconsumedMessagesRetention = 7 * 24 * 3600
	// DefaultLockTimeoutSeconds is the default value for lock timeout seconds
	DefaultLockTimeoutSeconds = 60
	// MinLockTimeoutSeconds is the minimum lock timeout seconds
	MinLockTimeoutSeconds = 10
	// MaxLockTimeoutSeconds is the maximum lock timeout seconds
	MaxLockTimeoutSeconds = 3600
	// DefaultMaxDeliveryCount is the default value for max delivery count
	DefaultMaxDeliveryCount = 10
	// MinMaxDeliveryCount is the minimum value for max delivery count
	MinMaxDeliveryCount = 1
	// MaxMaxDeliveryCount is the maximum value for max delivery count
	MaxMaxDeliveryCount = 1000
	// DefaultSkipOlderMessageSeconds is the default value for skipping older message
	DefaultSkipOlderMessageSeconds = 0
	// MinSkipOlderMessageSeconds is the minimum value for skipping older message
	MinSkipOlderMessageSeconds = 1800
	// MaxSkipOlderMessageSeconds is the maximum value for skipping older message
	MaxSkipOlderMessageSeconds = 2 * 24 * 3600
	// DefaultDelayMessageSeconds is the default value for delaying message
	DefaultDelayMessageSeconds = 0
	// MinDelayMessageSeconds is the minimum value for delaying message
	MinDelayMessageSeconds = 0
	// MaxDelayMessageSeconds is the maximum value for delaying message
	MaxDelayMessageSeconds = 2 * 24 * 3600

	// Kafka prefix is a required prefix for all Kafka type destinations and consumer groups
	kafkaPrefix = `/kafka_`
)

const (
	strNotEnoughArgs                  = "Not enough arguments. Try \"-help\""
	strTooManyArgs                    = "Too many arguments. Try \"-help\""
	strNoChange                       = "Update must update something. Try \"-help\""
	strCGSpecIncorrectArgs            = "Incorrect consumer group specification. Use \"<cg_uuid>\" or \"<dest_path> <cg_name>\""
	strDestStatus                     = "Destination status must be \"enabled\", \"disabled\", \"sendonly\", or \"recvonly\""
	strCGStatus                       = "Consumer group status must be \"enabled\", or \"disabled\""
	strWrongDestZoneConfig            = "Format of destination zone config is wrong, should be \"ZoneName,AllowPublish,AllowConsume,ReplicaCount\". For example: \"zone1,true,true,3\""
	strWrongReplicaCount              = "Replica count must be within 1 to 3"
	strWrongZoneConfigCount           = "Multi zone destination or consumer group must have at least 2 zone configs"
	strWrongCgZoneConfig              = "Format of consumer group zone config is wrong, should be \"ZoneName,PreferedActiveZone\". For example: \"zone1,false\""
	strMultiZoneCgWithSingleZoneDest  = "Multi zone consumer group must be created with a multi zone destination"
	strMultiplePreferedActiveZone     = "At most one zone can be prefered active zone"
	strUnconsumedRetentionTooSmall    = "Unconsumed retention period for multi zone destination should be at least 3 days"
	strKafkaNaming                    = "Kafka destinations and consumer groups must begin with \"" + kafkaPrefix + "\""
	strKafkaNotEnoughArgs             = "Kafka destinations must specify the Kafka cluster and at least one topic"
	strInvalidConsumedRetention       = "Invalid consumed message retention %v (must be between %v and %v), please contact cherami team"
	strInvalidUnconsumedRetention     = "Invalid unconsumed message retention %v (must be between %v and %v), please contact cherami team"
	strInvalidLockTimeoutSeconds      = "Invalid lock timeout seconds %v (must be between %v and %v), please contact cherami team"
	strInvalidMaxDeliveryCount        = "Invalid max delivery count %v (must be between %v and %v), please contact cherami team"
	strInvalidSkipOlderMessageSeconds = "Invalid skip older message seconds %v (must be zero or between %v and %v), please contact cherami team"
	strInvalidDelayMessageSeconds     = "Invalid delay message seconds %v (must be between %v and %v), please contact cherami team"
)

var uuidRegex, _ = regexp.Compile(`^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$`)

// ExitIfError exit while err is not nil and print the calling stack also
func ExitIfError(err error) {
	const stacksEnv = `CHERAMI_SHOW_STACKS`
	envReminder := "\n-env=staging is now the default. Did you mean '-env=prod' ?\n"
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		fmt.Fprintln(os.Stderr, envReminder)
		if os.Getenv(stacksEnv) != `` {
			debug.PrintStack()
		} else {
			fmt.Fprintf(os.Stderr, "('export %s=1' to see stack traces)\n", stacksEnv)
		}
		os.Exit(1)
	}
}

// newGlobalOptionsFromCLIContext return GlobalOptions based on the cli.Context
func newGlobalOptionsFromCLIContext(c *cli.Context) *GlobalOptions {
	host, port, err := common.SplitHostPort(c.GlobalString("hostport"))
	ExitIfError(err)
	environment := c.GlobalString("env")
	if strings.HasPrefix(environment, `prod`) {
		environment = ``
	}
	return &GlobalOptions{
		hyperbahn:              c.GlobalBool("hyperbahn"),
		hyperbahnBootstrapFile: c.GlobalString("hyperbahn_bootstrap_file"),
		env:          environment,
		frontendHost: host,
		frontendPort: port,
		timeoutSecs:  c.GlobalInt("timeout"),
	}
}

// GetCClient return a cherami.Client
func GetCClient(c *cli.Context, serviceName string) ccli.Client {
	return GetCClientSecure(c, serviceName, nil)
}

// GetCClientSecure return a cherami.Client with security enabled
func GetCClientSecure(c *cli.Context, serviceName string, authProvider ccli.AuthProvider) ccli.Client {
	gOpts := newGlobalOptionsFromCLIContext(c)
	var cClient ccli.Client
	var err error
	cOpts := ccli.ClientOptions{
		Timeout:       time.Duration(gOpts.timeoutSecs) * time.Second,
		DeploymentStr: gOpts.env,
		AuthProvider:  authProvider,
	}

	if !(len(gOpts.frontendHost) > 0 || gOpts.frontendPort > 0) && gOpts.hyperbahn {
		cClient, err = ccli.NewHyperbahnClient(serviceName, gOpts.hyperbahnBootstrapFile, &cOpts)
	} else {
		cClient, err = ccli.NewClient(serviceName, gOpts.frontendHost, gOpts.frontendPort, &cOpts)
	}

	ExitIfError(err)
	return cClient
}

// GetMClient return a metadata.Client
func GetMClient(c *cli.Context, serviceName string) mcli.Client {
	gOpts := newGlobalOptionsFromCLIContext(c)
	var mClient mcli.Client
	var err error
	cOpts := ccli.ClientOptions{
		Timeout:       time.Duration(gOpts.timeoutSecs) * time.Second,
		DeploymentStr: gOpts.env,
	}

	if !(len(gOpts.frontendHost) > 0 || gOpts.frontendPort > 0) && gOpts.hyperbahn {
		mClient, err = mcli.NewHyperbahnClient(serviceName, gOpts.hyperbahnBootstrapFile, &cOpts)
	} else {
		mClient, err = mcli.NewClient(serviceName, gOpts.frontendHost, gOpts.frontendPort, &cOpts)
	}

	ExitIfError(err)
	return mClient
}

// Jsonify return the json string based on the obj
func Jsonify(obj thrift.TStruct) string {
	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTSimpleJSONProtocol(transport)
	obj.Write(protocol)
	protocol.Flush()
	transport.Flush()
	return transport.String()
}

func getChecksumOptionParam(optionStr string) cherami.ChecksumOption {
	checksumstr := strings.ToLower(optionStr)
	if checksumstr == "md5" {
		return cherami.ChecksumOption_MD5
	}

	return cherami.ChecksumOption_CRC32IEEE
}

// CreateDestination creates destination
func CreateDestination(c *cli.Context, cliHelper common.CliHelper, serviceName string) {
	CreateDestinationSecure(c, cliHelper, serviceName, nil)
}

// CreateDestinationSecure creates destination with security enabled
func CreateDestinationSecure(
	c *cli.Context,
	cliHelper common.CliHelper,
	serviceName string,
	authProvider ccli.AuthProvider,
) {
	cClient := GetCClientSecure(c, serviceName, authProvider)

	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	adminMode := c.GlobalBool("admin_mode")

	path := c.Args().First()
	kafkaCluster := c.String("kafka_cluster")
	kafkaTopics := c.StringSlice("kafka_topics")

	dType := cherami.DestinationType_PLAIN
	if c.String("type") == "timer" {
		dType = cherami.DestinationType_TIMER
	}
	if c.String("type") == "kafka" { // Kafka type, check Kafka params
		dType = cherami.DestinationType_KAFKA
		if !strings.HasPrefix(path, kafkaPrefix) {
			ExitIfError(errors.New(strKafkaNaming))
		}
		if kafkaCluster == `` || common.ContainsEmpty(kafkaTopics) || len(kafkaTopics) == 0 { // Server will also check this
			ExitIfError(errors.New(strKafkaNotEnoughArgs))
		}
	}

	consumedMessagesRetention := int32(c.Int("consumed_messages_retention"))
	if !adminMode && (consumedMessagesRetention < MinConsumedMessagesRetention || consumedMessagesRetention > MaxConsumedMessagesRetention) {
		ExitIfError(fmt.Errorf(strInvalidConsumedRetention, consumedMessagesRetention, MinConsumedMessagesRetention, MaxConsumedMessagesRetention))
	}

	unconsumedMessagesRetention := int32(c.Int("unconsumed_messages_retention"))
	if !adminMode && (unconsumedMessagesRetention < MinUnconsumedMessagesRetention || unconsumedMessagesRetention > MaxUnconsumedMessagesRetention) {
		ExitIfError(fmt.Errorf(strInvalidUnconsumedRetention, unconsumedMessagesRetention, MinUnconsumedMessagesRetention, MaxUnconsumedMessagesRetention))
	}

	checksumOption := getChecksumOptionParam(string(c.String("checksum_option")))
	ownerEmail := string(c.String("owner_email"))
	if len(ownerEmail) == 0 {
		cliHelper.GetDefaultOwnerEmail()
	}

	zoneConfigs := getDestZoneConfigs(c, cliHelper)
	// don't allow short unconsumed message retention for multi_zone destination
	// this is a prevention mechanism to prevent messages from being deleted in source zone in case there's some
	// issue with cross zone replication(for example, network down between zones)
	if len(zoneConfigs.Configs) > 1 && unconsumedMessagesRetention < MinUnconsumedMessagesRetentionForMultiZoneDest {
		ExitIfError(errors.New(strUnconsumedRetentionTooSmall))
	}

	desc, err := cClient.CreateDestination(&cherami.CreateDestinationRequest{
		Path: &path,
		Type: &dType,
		ConsumedMessagesRetention:   &consumedMessagesRetention,
		UnconsumedMessagesRetention: &unconsumedMessagesRetention,
		ChecksumOption:              checksumOption,
		OwnerEmail:                  &ownerEmail,
		IsMultiZone:                 common.BoolPtr(len(zoneConfigs.Configs) > 0),
		ZoneConfigs:                 &zoneConfigs,
		KafkaCluster:                &kafkaCluster,
		KafkaTopics:                 kafkaTopics,
	})

	ExitIfError(err)
	fmt.Printf("%v\n", Jsonify(desc))
}

func getDestZoneConfigs(c *cli.Context, cliHelper common.CliHelper) cherami.DestinationZoneConfigs {
	var zoneConfigs cherami.DestinationZoneConfigs
	configs := c.StringSlice("zone_config")
	for _, config := range configs {
		parts := strings.Split(config, `,`)
		if len(parts) != 4 {
			ExitIfError(errors.New(strWrongDestZoneConfig))
		}

		zone, err := cliHelper.GetCanonicalZone(parts[0])
		ExitIfError(err)

		allowPublish, err := strconv.ParseBool(parts[1])
		ExitIfError(err)
		allowConsume, err := strconv.ParseBool(parts[2])
		ExitIfError(err)
		replicaCount, err := strconv.ParseInt(parts[3], 10, 0)
		ExitIfError(err)

		if replicaCount < 1 || replicaCount > 3 {
			ExitIfError(errors.New(strWrongReplicaCount))
		}

		zoneConfigs.Configs = append(zoneConfigs.Configs, &cherami.DestinationZoneConfig{
			Zone:                   common.StringPtr(zone),
			AllowPublish:           common.BoolPtr(allowPublish),
			AllowConsume:           common.BoolPtr(allowConsume),
			RemoteExtentReplicaNum: common.Int32Ptr(int32(replicaCount)),
		})
	}

	if len(zoneConfigs.Configs) == 1 {
		ExitIfError(errors.New(strWrongZoneConfigCount))
	}
	return zoneConfigs
}

// UpdateDestination updates destination
func UpdateDestination(c *cli.Context, cliHelper common.CliHelper, serviceName string) {
	UpdateDestinationSecure(c, cliHelper, serviceName, nil)
}

// UpdateDestinationSecure updates destination with security enabled
func UpdateDestinationSecure(
	c *cli.Context,
	cliHelper common.CliHelper,
	serviceName string,
	authProvider ccli.AuthProvider,
) {
	mClient := GetMClient(c, serviceName)
	cClient := GetCClientSecure(c, serviceName, authProvider)

	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	path := c.Args().First()

	status := cherami.DestinationStatus_ENABLED
	switch c.String("status") {
	case "disabled":
		status = cherami.DestinationStatus_DISABLED
	case "sendonly":
		status = cherami.DestinationStatus_SENDONLY
	case "recvonly:":
		status = cherami.DestinationStatus_RECEIVEONLY
	case "enabled":
	default:
		if c.IsSet(`status`) {
			ExitIfError(errors.New(strDestStatus))
		}
	}

	setCount := 0

	request := &cherami.UpdateDestinationRequest{
		Path: &path,
		ConsumedMessagesRetention:   getIfSetInt32(c, `consumed_messages_retention`, &setCount),
		UnconsumedMessagesRetention: getIfSetInt32(c, `unconsumed_messages_retention`, &setCount),
		ZoneConfigs:                 getIfSetDestZoneConfig(c, &setCount, cliHelper),
	}

	// don't allow short unconsumed message retention for multi_zone destination
	// this is a prevention mechanism to prevent messages from being deleted in source zone in case there's some
	// issue with cross zone replication(for example, network down between zones)
	existingDesc, err := readDestinationFromMetadata(mClient, path)
	ExitIfError(err)
	if existingDesc.GetIsMultiZone() || (request.IsSetZoneConfigs() && len(request.GetZoneConfigs().GetConfigs()) > 0) {
		if c.IsSet(`unconsumed_messages_retention`) && int32(c.Int(`unconsumed_messages_retention`)) < MinUnconsumedMessagesRetentionForMultiZoneDest {
			ExitIfError(errors.New(strUnconsumedRetentionTooSmall))
		}
		if !c.IsSet(`unconsumed_messages_retention`) && existingDesc.GetUnconsumedMessagesRetention() < MinUnconsumedMessagesRetentionForMultiZoneDest {
			ExitIfError(errors.New(strUnconsumedRetentionTooSmall))
		}
	}

	if c.IsSet(`status`) {
		setCount++
		request.Status = &status
	}

	if c.IsSet(`checksum_option`) {
		checksumOption := getChecksumOptionParam(string(c.String("checksum_option")))
		request.ChecksumOption = &checksumOption
		setCount++
	}

	if setCount == 0 {
		ExitIfError(errors.New(strNoChange))
	}

	desc, err := cClient.UpdateDestination(request)

	ExitIfError(err)
	fmt.Printf("%v\n", Jsonify(desc))
}

// CreateConsumerGroup creates consumer group based on cli.Context
func CreateConsumerGroup(c *cli.Context, cliHelper common.CliHelper, serviceName string) {
	CreateConsumerGroupSecure(c, cliHelper, serviceName, nil)
}

// CreateConsumerGroupSecure creates consumer group based on cli.Context
func CreateConsumerGroupSecure(
	c *cli.Context,
	cliHelper common.CliHelper,
	serviceName string,
	authProvider ccli.AuthProvider,
) {
	mClient := GetMClient(c, serviceName)
	cClient := GetCClientSecure(c, serviceName, authProvider)

	if len(c.Args()) < 2 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	adminMode := c.GlobalBool("admin_mode")

	path := c.Args()[0]
	name := c.Args()[1]

	// For Kafka-type destinations, verify that the consumer group has the prefix, too
	if strings.HasPrefix(path, kafkaPrefix) && !strings.HasPrefix(name, kafkaPrefix) {
		ExitIfError(errors.New(strKafkaNaming))
	}

	// StartFrom is a 64-bit value with nano-second precision
	startTime := int64(c.Int("start_time")) * 1e9

	lockTimeout := int32(c.Int("lock_timeout_seconds"))
	if !adminMode && (lockTimeout < MinLockTimeoutSeconds || lockTimeout > MaxLockTimeoutSeconds) {
		ExitIfError(fmt.Errorf(strInvalidLockTimeoutSeconds, lockTimeout, MinLockTimeoutSeconds, MaxLockTimeoutSeconds))
	}

	maxDelivery := int32(c.Int("max_delivery_count"))
	if !adminMode && (maxDelivery < MinMaxDeliveryCount || maxDelivery > MaxMaxDeliveryCount) {
		ExitIfError(fmt.Errorf(strInvalidMaxDeliveryCount, maxDelivery, MinMaxDeliveryCount, MaxMaxDeliveryCount))
	}

	skipOlder := int32(c.Int("skip_older_messages_in_seconds"))
	if !adminMode && skipOlder != 0 && (skipOlder < MinSkipOlderMessageSeconds || skipOlder > MaxSkipOlderMessageSeconds) {
		ExitIfError(fmt.Errorf(strInvalidSkipOlderMessageSeconds, skipOlder, MinSkipOlderMessageSeconds, MaxSkipOlderMessageSeconds))
	}

	delay := int32(c.Int("delay_seconds"))
	if !adminMode && (delay < MinDelayMessageSeconds || delay > MaxDelayMessageSeconds) {
		ExitIfError(fmt.Errorf(strInvalidDelayMessageSeconds, delay, MinDelayMessageSeconds, MaxDelayMessageSeconds))
	}

	ownerEmail := string(c.String("owner_email"))

	// Override default startFrom for DLQ consumer groups
	if common.UUIDRegex.MatchString(path) && int64(c.Int("start_time")) > time.Now().Unix()-60 {
		fmt.Printf(`Start_time defaulted to zero, while creating DLQ consumer group`)
		startTime = 0
	}

	if len(ownerEmail) == 0 {
		ownerEmail = cliHelper.GetDefaultOwnerEmail()
	}

	zoneConfigs := getCgZoneConfigs(c, mClient, cliHelper, path)
	isMultiZone := len(zoneConfigs.GetConfigs()) > 0

	options := make(map[string]string)

	if c.Bool(common.FlagDisableNackThrottling) {
		options[common.FlagDisableNackThrottling] = "true"
	}

	if c.Bool(common.FlagEnableSmartRetry) {
		options[common.FlagEnableSmartRetry] = "true"
	}

	if c.Bool(common.FlagEnableQueueDepthTabulation) {
		options[common.FlagEnableQueueDepthTabulation] = "true"
	}

	desc, err := cClient.CreateConsumerGroup(&cherami.CreateConsumerGroupRequest{
		DestinationPath:            &path,
		ConsumerGroupName:          &name,
		StartFrom:                  &startTime,
		LockTimeoutInSeconds:       &lockTimeout,
		MaxDeliveryCount:           &maxDelivery,
		SkipOlderMessagesInSeconds: &skipOlder,
		DelaySeconds:               &delay,
		OwnerEmail:                 &ownerEmail,
		IsMultiZone:                &isMultiZone,
		ZoneConfigs:                &zoneConfigs,
		Options:                    options,
	})

	ExitIfError(err)
	fmt.Printf("%v\n", Jsonify(desc))
}

func getCgZoneConfigs(c *cli.Context, mClient mcli.Client, cliHelper common.CliHelper, destinationPath string) cherami.ConsumerGroupZoneConfigs {
	var zoneConfigs cherami.ConsumerGroupZoneConfigs
	configs := c.StringSlice("zone_config")
	var preferedActiveZone string
	for _, config := range configs {
		parts := strings.Split(config, `,`)
		if len(parts) != 2 {
			ExitIfError(errors.New(strWrongCgZoneConfig))
		}

		zone, err := cliHelper.GetCanonicalZone(parts[0])
		ExitIfError(err)

		isPreferedActiveZone, err := strconv.ParseBool(parts[1])
		ExitIfError(err)

		if isPreferedActiveZone {
			if len(preferedActiveZone) > 0 {
				ExitIfError(errors.New(strMultiplePreferedActiveZone))
			} else {
				preferedActiveZone = zone
			}
		}

		zoneConfigs.Configs = append(zoneConfigs.Configs, &cherami.ConsumerGroupZoneConfig{
			Zone:    common.StringPtr(zone),
			Visible: common.BoolPtr(true),
		})
	}

	if len(zoneConfigs.Configs) == 1 {
		ExitIfError(errors.New(strWrongZoneConfigCount))
	}

	if len(zoneConfigs.Configs) > 1 {
		dest, err := readDestinationFromMetadata(mClient, destinationPath)
		ExitIfError(err)

		if !dest.GetIsMultiZone() {
			ExitIfError(errors.New(strMultiZoneCgWithSingleZoneDest))
		}

		zoneConfigs.ActiveZone = common.StringPtr(preferedActiveZone)
	}
	return zoneConfigs
}

func getCgFromMedatada(mClient mcli.Client, path string, name string) *shared.ConsumerGroupDescription {
	cg, err := mClient.ReadConsumerGroup(&shared.ReadConsumerGroupRequest{
		DestinationPath:   common.StringPtr(path),
		ConsumerGroupName: common.StringPtr(name),
	})
	ExitIfError(err)

	return cg
}

// UpdateConsumerGroup updates the consumer group
func UpdateConsumerGroup(c *cli.Context, cliHelper common.CliHelper, serviceName string) {
	UpdateConsumerGroupSecure(c, cliHelper, serviceName, nil)
}

// UpdateConsumerGroupSecure updates the consumer group with security enabled
func UpdateConsumerGroupSecure(
	c *cli.Context,
	cliHelper common.CliHelper,
	serviceName string,
	authProvider ccli.AuthProvider,
) {
	mClient := GetMClient(c, serviceName)
	cClient := GetCClientSecure(c, serviceName, authProvider)

	var path, name string

	switch {
	case len(c.Args()) < 1:
		ExitIfError(errors.New(strNotEnoughArgs))

	case len(c.Args()) == 1: // assume the arg is a uuid

		respCG, err := mClient.ReadConsumerGroupByUUID(&shared.ReadConsumerGroupRequest{
			ConsumerGroupUUID: &c.Args()[0],
		})
		ExitIfError(err)

		respDest, err := mClient.ReadDestination(&shared.ReadDestinationRequest{
			DestinationUUID: respCG.DestinationUUID,
		})
		ExitIfError(err)

		path, name = respDest.GetPath(), respCG.GetConsumerGroupName()

	default:
		path, name = c.Args()[0], c.Args()[1]
	}

	status := cherami.ConsumerGroupStatus_ENABLED
	if c.String("status") == "disabled" {
		status = cherami.ConsumerGroupStatus_DISABLED
	} else if c.String("status") != "enabled" {
		if c.IsSet(`status`) {
			ExitIfError(errors.New(strCGStatus))
		}
	}

	setCount := 0

	uReq := &cherami.UpdateConsumerGroupRequest{
		DestinationPath:            &path,
		ConsumerGroupName:          &name,
		LockTimeoutInSeconds:       getIfSetInt32(c, `lock_timeout_seconds`, &setCount),
		MaxDeliveryCount:           getIfSetInt32(c, `max_delivery_count`, &setCount),
		SkipOlderMessagesInSeconds: getIfSetInt32(c, `skip_older_messages_in_seconds`, &setCount),
		DelaySeconds:               getIfSetInt32(c, `delay_seconds`, &setCount),
		OwnerEmail:                 getIfSetString(c, `owner_email`, &setCount),
		ActiveZone:                 getIfSetString(c, `active_zone`, &setCount),
		ZoneConfigs:                getIfSetCgZoneConfig(c, mClient, cliHelper, path, &setCount),
		Options:                    getIfSetOptions(c, mClient, path, name, &setCount),
	}

	if c.IsSet(`status`) {
		setCount++
		uReq.Status = &status
	}

	if setCount == 0 {
		ExitIfError(errors.New(strNoChange))
	}

	desc, err := cClient.UpdateConsumerGroup(uReq)
	ExitIfError(err)
	fmt.Printf("%v\n", Jsonify(desc))
}

// UnloadConsumerGroup unloads the CG based on cli.Context
func UnloadConsumerGroup(c *cli.Context, mClient mcli.Client) {
	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	hostPort := c.Args()[0]
	cgUUID := c.String("cg_uuid")

	if !uuidRegex.MatchString(cgUUID) {
		ExitIfError(errors.New("specify a valid cg UUID"))
	}

	// generate a random instance id to be used to create a client tchannel
	instanceID := rand.Intn(50000)
	outputClient, err := outputhost.NewClient(instanceID, hostPort)
	ExitIfError(err)
	defer outputClient.Close()

	cgUnloadReq := admin.NewUnloadConsumerGroupsRequest()
	cgUnloadReq.CgUUIDs = []string{cgUUID}

	err = outputClient.UnloadConsumerGroups(cgUnloadReq)
	ExitIfError(err)
}

// ListAllConsumerGroups lists all loaded CGs in memory of the outputhost
func ListAllConsumerGroups(c *cli.Context) {
	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	hostPort := c.Args()[0]

	// generate a random instance id to be used to create a client tchannel
	instanceID := rand.Intn(50000)
	outputClient, err := outputhost.NewClient(instanceID, hostPort)
	ExitIfError(err)
	defer outputClient.Close()

	listCgResult, err := outputClient.ListLoadedConsumerGroups()
	ExitIfError(err)

	if listCgResult != nil {
		for _, cg := range listCgResult.Cgs {
			fmt.Printf("%v\n", Jsonify(cg))
		}
	}
}

// GetConsumerGroupState unloads the CG based on cli.Context
func GetConsumerGroupState(c *cli.Context) {
	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	hostPort := c.Args()[0]
	cgUUID := c.String("cg_uuid")

	if !uuidRegex.MatchString(cgUUID) {
		ExitIfError(errors.New("specify a valid cg UUID"))
	}

	// generate a random instance id to be used to create a client tchannel
	instanceID := rand.Intn(50000)
	outputClient, err := outputhost.NewClient(instanceID, hostPort)
	ExitIfError(err)
	defer outputClient.Close()

	cgStateReq := admin.NewReadConsumerGroupStateRequest()
	cgStateReq.CgUUIDs = []string{cgUUID}

	readcgStateRes, err1 := outputClient.ReadCgState(cgStateReq)
	ExitIfError(err1)

	fmt.Printf("sessionID: %v\n", readcgStateRes.GetSessionID())

	for _, cg := range readcgStateRes.CgState {
		printCgState(cg)
		for _, ext := range cg.CgExtents {
			fmt.Printf("\t%v\n", Jsonify(ext))
		}
	}
}

type cgStateJSONOutput struct {
	CgUUID             string `json:"cgUUID"`
	NumOutstandingMsgs int32  `json:"numOutstandingMsgs"`
	MsgChSize          int64  `json:"msgChSize"`
	MsgCacheChSize     int64  `json:"msgCacheChSize"`
	NumConnections     int64  `json:"numConnections"`
}

func printCgState(cgState *admin.ConsumerGroupState) {
	output := &cgStateJSONOutput{
		CgUUID:             cgState.GetCgUUID(),
		NumOutstandingMsgs: cgState.GetNumOutstandingMsgs(),
		MsgChSize:          cgState.GetMsgChSize(),
		MsgCacheChSize:     cgState.GetMsgCacheChSize(),
		NumConnections:     cgState.GetNumConnections(),
	}
	outputStr, _ := json.Marshal(output)
	fmt.Fprintln(os.Stdout, string(outputStr))
}

// UnloadDestination unloads the Destination based on cli.Context
func UnloadDestination(c *cli.Context, mClient mcli.Client) {
	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	hostPort := c.Args()[0]
	destUUID := c.String("dest_uuid")

	if !uuidRegex.MatchString(destUUID) {
		ExitIfError(errors.New("specify a valid dest UUID"))
	}

	// generate a random instance id to be used to create a client tchannel
	instanceID := rand.Intn(50000)
	inputClient, err := inputhost.NewClient(instanceID, hostPort)
	ExitIfError(err)
	defer inputClient.Close()

	destUnloadReq := admin.NewUnloadDestinationsRequest()
	destUnloadReq.DestUUIDs = []string{destUUID}

	err = inputClient.UnloadDestinations(destUnloadReq)
	ExitIfError(err)
}

// ListAllLoadedDestinations lists all loaded Destinations in memory of the inputhost
func ListAllLoadedDestinations(c *cli.Context) {
	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	hostPort := c.Args()[0]

	// generate a random instance id to be used to create a client tchannel
	instanceID := rand.Intn(50000)
	inputClient, err := inputhost.NewClient(instanceID, hostPort)
	ExitIfError(err)
	defer inputClient.Close()

	listDestResult, err := inputClient.ListLoadedDestinations()
	ExitIfError(err)

	if listDestResult != nil {
		for _, dest := range listDestResult.Dests {
			fmt.Printf("%v\n", Jsonify(dest))
		}
	}
}

// GetDestinationState unloads the Destination based on cli.Context
func GetDestinationState(c *cli.Context) {
	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	hostPort := c.Args()[0]
	destUUID := c.String("dest_uuid")

	if !uuidRegex.MatchString(destUUID) {
		ExitIfError(errors.New("specify a valid dest UUID"))
	}

	// generate a random instance id to be used to create a client tchannel
	instanceID := rand.Intn(50000)
	inputClient, err := inputhost.NewClient(instanceID, hostPort)
	ExitIfError(err)
	defer inputClient.Close()

	destStateReq := admin.NewReadDestinationStateRequest()
	destStateReq.DestUUIDs = []string{destUUID}

	readdestStateRes, err1 := inputClient.ReadDestState(destStateReq)
	ExitIfError(err1)

	fmt.Printf("inputhostUUID: %v\n", readdestStateRes.GetInputHostUUID())

	for _, dest := range readdestStateRes.DestState {
		printDestState(dest)
		for _, ext := range dest.DestExtents {
			fmt.Printf("\t%v\n", Jsonify(ext))
		}
	}
}

type destStateJSONOutput struct {
	DestUUID       string `json:"destUUID"`
	MsgsChSize     int64  `json:"msgsChSize"`
	NumConnections int64  `json:"numConnections"`
	NumMsgsIn      int64  `json:"numMsgsIn"`
	NumSentAcks    int64  `json:"numSentAcks"`
	NumSentNacks   int64  `json:"numSentNacks"`
	NumThrottled   int64  `json:"numThrottled"`
	NumFailed      int64  `json:"numFailed"`
}

func printDestState(destState *admin.DestinationState) {
	input := &destStateJSONOutput{
		DestUUID:       destState.GetDestUUID(),
		MsgsChSize:     destState.GetMsgsChSize(),
		NumConnections: destState.GetNumConnections(),
		NumMsgsIn:      destState.GetNumMsgsIn(),
		NumSentAcks:    destState.GetNumSentAcks(),
		NumSentNacks:   destState.GetNumSentNacks(),
		NumThrottled:   destState.GetNumThrottled(),
		NumFailed:      destState.GetNumFailed(),
	}
	inputStr, _ := json.Marshal(input)
	fmt.Fprintln(os.Stdout, string(inputStr))
}

type destDescJSONOutputFields struct {
	Path                        string                          `json:"path"`
	UUID                        string                          `json:"uuid"`
	Status                      shared.DestinationStatus        `json:"status"`
	Type                        shared.DestinationType          `json:"type"`
	ChecksumOption              string                          `json:"checksum_option"`
	OwnerEmail                  string                          `json:"owner_email"`
	ConsumedMessagesRetention   int32                           `json:"consumed_messages_retention"`
	UnconsumedMessagesRetention int32                           `json:"unconsumed_messages_retention"`
	DLQCGUUID                   string                          `json:"dlq_cg_uuid"`
	DLQPurgeBefore              int64                           `json:"dlq_purge_before"`
	DLQMergeBefore              int64                           `json:"dlq_merge_before"`
	IsMultiZone                 bool                            `json:"is_multi_zone"`
	ZoneConfigs                 []*shared.DestinationZoneConfig `json:"zone_configs"`
	KafkaCluster                string                          `json:"kafka_cluster"`
	KafkaTopics                 []string                        `json:"kafka_topics"`
}

func printDest(dest *shared.DestinationDescription) {
	output := &destDescJSONOutputFields{
		Path:                        dest.GetPath(),
		UUID:                        dest.GetDestinationUUID(),
		Status:                      dest.GetStatus(),
		Type:                        dest.GetType(),
		OwnerEmail:                  dest.GetOwnerEmail(),
		ConsumedMessagesRetention:   dest.GetConsumedMessagesRetention(),
		UnconsumedMessagesRetention: dest.GetUnconsumedMessagesRetention(),
		DLQCGUUID:                   dest.GetDLQConsumerGroupUUID(),
		DLQPurgeBefore:              dest.GetDLQPurgeBefore(),
		DLQMergeBefore:              dest.GetDLQMergeBefore(),
		IsMultiZone:                 dest.GetIsMultiZone(),
		ZoneConfigs:                 dest.GetZoneConfigs(),
		KafkaCluster:                dest.GetKafkaCluster(),
		KafkaTopics:                 dest.GetKafkaTopics(),
	}

	switch dest.GetChecksumOption() {
	case shared.ChecksumOption_CRC32IEEE:
		output.ChecksumOption = "CRC32_IEEE"
	case shared.ChecksumOption_MD5:
		output.ChecksumOption = "MD5"
	}

	outputStr, _ := json.Marshal(output)
	fmt.Fprintln(os.Stdout, string(outputStr))
}

// ReadDestination return the detail for dest, and also consumer group for this dest
func ReadDestination(c *cli.Context, serviceName string) {
	mClient := GetMClient(c, serviceName)

	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	path := c.Args().First()
	showCG := c.Bool("showcg")
	desc, err := readDestinationFromMetadata(mClient, path)
	ExitIfError(err)
	printDest(desc)

	// only show cg info if showCG flag is true
	if showCG {
		// read all the consumer group for this destination, including deleted ones
		destUUID := desc.GetDestinationUUID()
		req := &shared.ListConsumerGroupRequest{
			Limit: common.Int64Ptr(DefaultPageSize),
		}
		var cgsInfo = make([]*shared.ConsumerGroupDescription, 0)
		for {
			resp, err1 := mClient.ListAllConsumerGroups(req)
			ExitIfError(err1)

			for _, cg := range resp.GetConsumerGroups() {
				if destUUID == cg.GetDestinationUUID() {
					cgsInfo = append(cgsInfo, cg)
				}
			}

			if len(resp.GetConsumerGroups()) < DefaultPageSize {
				break
			} else {
				req.PageToken = resp.NextPageToken
			}
		}
		// print out all the consumer groups for this destination
		for _, cg := range cgsInfo {
			printCG(cg)
		}
	}
}

// path passed from tools might contain a password for some hacky security check, however the ReadDestination
// metadata call doesn't handle that. This helper function strips the password if there's any.
// TODO: we should remove this once the hacky security check is removed
func readDestinationFromMetadata(mClient mcli.Client, path string) (*shared.DestinationDescription, error) {
	return mClient.ReadDestination(&shared.ReadDestinationRequest{
		Path: common.StringPtr(strings.Split(path, `+`)[0]),
	})
}

// ReadDlq return the info for dlq dest and related consumer group
func ReadDlq(c *cli.Context, serviceName string) {
	mClient := GetMClient(c, serviceName)

	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	dlqUUID := c.Args().First()
	desc, err0 := readDestinationFromMetadata(mClient, dlqUUID)

	ExitIfError(err0)
	printDest(desc)

	cgUUID := desc.GetDLQConsumerGroupUUID()
	if len(cgUUID) == 0 {
		ExitIfError(errors.New("no dlqConsumerGroupUUID for this destination. Please ensure it is a dlq destination"))
	}

	req := &shared.ReadConsumerGroupRequest{
		ConsumerGroupUUID: &cgUUID,
	}

	resp, err := mClient.ReadConsumerGroupByUUID(req)
	ExitIfError(err)
	printCG(resp)
}

// ReadCgBacklog reads the CG back log
func ReadCgBacklog(c *cli.Context, serviceName string) {
	cClient := GetCClient(c, serviceName)

	var cg, dst string
	var dstPtr *string
	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	} else if len(c.Args()) > 2 {
		ExitIfError(errors.New(strTooManyArgs))
	}

	switch len(c.Args()) {
	case 1:
		cg = c.Args()[0]
	default:
		cg = c.Args()[1]
		dst = c.Args()[0]
	}

	if dst != `` {
		dstPtr = &dst
	}

	backlog, err := cClient.GetQueueDepthInfo(&cherami.GetQueueDepthInfoRequest{
		ConsumerGroupName: &cg,
		DestinationPath:   dstPtr,
	})
	if err != nil {
		fmt.Printf("Cannot get backlog. Please check the dashboard.")
		os.Exit(1)
	}

	fmt.Println(backlog.GetValue())
}

// DeleteDestination deletes the destination
func DeleteDestination(c *cli.Context, serviceName string) {
	DeleteDestinationSecure(c, serviceName, nil)
}

// DeleteDestinationSecure deletes the destination with security enabled
func DeleteDestinationSecure(c *cli.Context, serviceName string, authProvider ccli.AuthProvider) {
	cClient := GetCClientSecure(c, serviceName, authProvider)

	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}
	path := c.Args().First()
	err := cClient.DeleteDestination(&cherami.DeleteDestinationRequest{
		Path: &path,
	})

	ExitIfError(err)
}

// DeleteConsumerGroup deletes the consumer group
func DeleteConsumerGroup(c *cli.Context, serviceName string) {
	DeleteConsumerGroupSecure(c, serviceName, nil)
}

// DeleteConsumerGroupSecure deletes the consumer group with security enabled
func DeleteConsumerGroupSecure(c *cli.Context, serviceName string, authProvider ccli.AuthProvider) {
	cClient := GetCClientSecure(c, serviceName, authProvider)

	if len(c.Args()) < 2 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}
	path := c.Args()[0]
	name := c.Args()[1]

	err := cClient.DeleteConsumerGroup(&cherami.DeleteConsumerGroupRequest{
		DestinationPath:   &path,
		ConsumerGroupName: &name,
	})

	ExitIfError(err)
}

type cgJSONOutputFields struct {
	CGName                   string                            `json:"consumer_group_name"`
	DestUUID                 string                            `json:"destination_uuid"`
	CGUUID                   string                            `json:"consumer_group_uuid"`
	Status                   shared.ConsumerGroupStatus        `json:"consumer_group_status"`
	StartFrom                int64                             `json:"startFrom"`
	LockTimeoutSeconds       int32                             `json:"lock_timeout_seconds"`
	MaxDeliveryCount         int32                             `json:"max_delivery_count"`
	SkipOlderMessagesSeconds int32                             `json:"skip_older_msg_seconds"`
	DelaySeconds             int32                             `json:"delay_seconds"`
	CGEmail                  string                            `json:"owner_email"`
	CGDlq                    string                            `json:"dlqUUID"`
	IsMultiZone              bool                              `json:"is_multi_zone"`
	ZoneConfigs              []*shared.ConsumerGroupZoneConfig `json:"zone_Configs"`
	ActiveZone               string                            `json:"active_zone"`
	Options                  map[string]string                 `json:"options"`
}

func printCG(cg *shared.ConsumerGroupDescription) {
	output := &cgJSONOutputFields{
		CGName:                   cg.GetConsumerGroupName(),
		DestUUID:                 cg.GetDestinationUUID(),
		CGUUID:                   cg.GetConsumerGroupUUID(),
		Status:                   cg.GetStatus(),
		StartFrom:                cg.GetStartFrom(),
		LockTimeoutSeconds:       cg.GetLockTimeoutSeconds(),
		MaxDeliveryCount:         cg.GetMaxDeliveryCount(),
		SkipOlderMessagesSeconds: cg.GetSkipOlderMessagesSeconds(),
		DelaySeconds:             cg.GetDelaySeconds(),
		CGEmail:                  cg.GetOwnerEmail(),
		CGDlq:                    cg.GetDeadLetterQueueDestinationUUID(),
		IsMultiZone:              cg.GetIsMultiZone(),
		ZoneConfigs:              cg.GetZoneConfigs(),
		ActiveZone:               cg.GetActiveZone(),
		Options:                  cg.GetOptions(),
	}
	outputStr, _ := json.Marshal(output)
	fmt.Fprintln(os.Stdout, string(outputStr))
}

// ReadConsumerGroup return the consumer group information
func ReadConsumerGroup(c *cli.Context, serviceName string) {
	mClient := GetMClient(c, serviceName)

	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strCGSpecIncorrectArgs))
	}

	if len(c.Args()) == 2 {
		path := c.Args()[0]
		name := c.Args()[1]

		cgDesc, err := mClient.ReadConsumerGroup(&shared.ReadConsumerGroupRequest{
			DestinationPath:   &path,
			ConsumerGroupName: &name,
		})

		ExitIfError(err)
		printCG(cgDesc)
		return
	}
	if len(c.Args()) == 1 {
		cgUUID := c.Args()[0]

		cgDesc, err := mClient.ReadConsumerGroupByUUID(&shared.ReadConsumerGroupRequest{
			ConsumerGroupUUID: common.StringPtr(cgUUID)})
		ExitIfError(err)
		printCG(cgDesc)
	}
}

// MergeDLQForConsumerGroup return the consumer group information
func MergeDLQForConsumerGroup(c *cli.Context, serviceName string) {
	cClient := GetCClient(c, serviceName)

	var err error
	switch len(c.Args()) {
	default:
		err = errors.New(strCGSpecIncorrectArgs)
	case 2:
		path := c.Args()[0]
		name := c.Args()[1]

		err = cClient.MergeDLQForConsumerGroup(&cherami.MergeDLQForConsumerGroupRequest{
			DestinationPath:   common.StringPtr(path),
			ConsumerGroupName: common.StringPtr(name),
		})
	case 1:
		cgUUID := c.Args()[0]
		err = cClient.MergeDLQForConsumerGroup(&cherami.MergeDLQForConsumerGroupRequest{
			ConsumerGroupName: common.StringPtr(cgUUID),
		})
	}
	ExitIfError(err)
}

// PurgeDLQForConsumerGroup return the consumer group information
func PurgeDLQForConsumerGroup(c *cli.Context, serviceName string) {
	cClient := GetCClient(c, serviceName)

	var err error
	switch len(c.Args()) {
	default:
		err = errors.New(strCGSpecIncorrectArgs)
	case 2:
		path := c.Args()[0]
		name := c.Args()[1]

		err = cClient.PurgeDLQForConsumerGroup(&cherami.PurgeDLQForConsumerGroupRequest{
			DestinationPath:   common.StringPtr(path),
			ConsumerGroupName: common.StringPtr(name),
		})
	case 1:
		cgUUID := c.Args()[0]
		err = cClient.PurgeDLQForConsumerGroup(&cherami.PurgeDLQForConsumerGroupRequest{
			ConsumerGroupName: common.StringPtr(cgUUID),
		})
	}
	ExitIfError(err)
}

type destJSONOutputFields struct {
	DestinationName             string                          `json:"destination_name"`
	DestinationUUID             string                          `json:"destination_uuid"`
	Status                      shared.DestinationStatus        `json:"status"`
	Type                        shared.DestinationType          `json:"type"`
	OwnerEmail                  string                          `json:"owner_email"`
	TotalExts                   int                             `json:"total_ext"`
	OpenExts                    int                             `json:"open"`
	SealedExts                  int                             `json:"sealed"`
	ConsumedExts                int                             `json:"consumed"`
	DeletedExts                 int                             `json:"Deleted"`
	ConsumedMessagesRetention   int32                           `json:"consumed_messages_retention"`
	UnconsumedMessagesRetention int32                           `json:"unconsumed_messages_retention"`
	IsMultiZone                 bool                            `json:"is_multi_zone"`
	ZoneConfigs                 []*shared.DestinationZoneConfig `json:"zone_configs"`
}

func matchDestStatus(status string, wantStatus shared.DestinationStatus) bool {

	switch status {
	case "enabled":
		return wantStatus == shared.DestinationStatus_ENABLED
	case "disabled":
		return wantStatus == shared.DestinationStatus_DISABLED
	case "sendonly":
		return wantStatus == shared.DestinationStatus_SENDONLY
	case "recvonly":
		return wantStatus == shared.DestinationStatus_RECEIVEONLY
	default:
		ExitIfError(errors.New("Please use status among: enabled | disabled | sendonly | recvonly"))
	}
	return false
}

// ReadMessage implement for show msg command line
func ReadMessage(c *cli.Context, serviceName string) {
	mClient := GetMClient(c, serviceName)

	if len(c.Args()) < 2 {
		ExitIfError(errors.New("not enough arguments, need to specify both extent uuid and message address"))
	}

	uuidStr := c.Args()[0]
	descExtent, err := mClient.ReadExtentStats(&metadata.ReadExtentStatsRequest{
		ExtentUUID: &uuidStr,
	})
	ExitIfError(err)
	extentStats := descExtent.GetExtentStats()
	extent := extentStats.GetExtent()

	addressStr := c.Args()[1]
	address, err1 := strconv.ParseInt(addressStr, 16, 64)
	ExitIfError(err1)

	storeHostUUIDs := extent.GetStoreUUIDs()
	for _, storeUUID := range storeHostUUIDs {
		storeHostAddr, err2 := mClient.UUIDToHostAddr(storeUUID)
		if err2 != nil {
			storeHostAddr = storeUUID + UnknownUUID
		}

		sClient, err3 := storehost.NewClient(storeUUID, storeHostAddr)
		ExitIfError(err3)
		defer sClient.Close()

		req := store.NewReadMessagesRequest()
		req.ExtentUUID = common.StringPtr(string(uuidStr))
		req.StartAddress = common.Int64Ptr(address)
		req.StartAddressInclusive = common.BoolPtr(true)
		req.NumMessages = common.Int32Ptr(1)

		// read messages from replica
		resp, err4 := sClient.ReadMessages(req)
		ExitIfError(err4)

		// print out msg from all store hosts
		readMessage := resp.GetMessages()[0].GetMessage()
		message := readMessage.GetMessage()

		ipAndPort := strings.Split(storeHostAddr, ":")

		output := &messageJSONOutputFields{
			StoreAddr:      ipAndPort[0],
			StoreUUID:      storeUUID,
			MessageAddress: readMessage.GetAddress(),
			SequenceNumber: message.GetSequenceNumber(),
			EnqueueTimeUtc: time.Unix(0, message.GetEnqueueTimeUtc()*1000000),
		}

		outputStr, _ := json.Marshal(output)
		fmt.Fprintln(os.Stdout, string(outputStr))
		fmt.Fprintf(os.Stdout, "%v\n", message.GetPayload())
	}
}

type messageJSONOutputFields struct {
	StoreAddr      string    `json:"storehost_addr"`
	StoreUUID      string    `json:"storehost_uuid"`
	MessageAddress int64     `json:"address"`
	SequenceNumber int64     `json:"sequenceNumber,omitempty"`
	EnqueueTimeUtc time.Time `json:"enqueueTimeUtc,omitempty"`
}

// ListDestinations return destinations based on the Cli.Context
func ListDestinations(c *cli.Context, serviceName string) {
	mClient := GetMClient(c, serviceName)

	prefix := string(c.String("prefix"))
	included := string(c.String("include"))
	excluded := string(c.String("exclude"))
	destStatus := string(c.String("status"))

	req := &shared.ListDestinationsRequest{
		Prefix: &prefix,
		Limit:  common.Int64Ptr(DefaultPageSize),
	}

	inReg, errI := regexp.Compile(included)
	ExitIfError(errI)
	exReg, errE := regexp.Compile(excluded)
	ExitIfError(errE)

	var destsInfo = make(map[shared.DestinationStatus][]*destJSONOutputFields, 0)
	for {
		resp, err := mClient.ListDestinations(req)
		ExitIfError(err)

		for _, desc := range resp.GetDestinations() {

			if len(included) > 0 && !inReg.MatchString(desc.GetPath()) {
				continue
			}
			if len(excluded) > 0 && exReg.MatchString(desc.GetPath()) {
				continue
			}
			nTotal := 0
			nOpen := 0
			nSealed := 0
			nConsumed := 0
			nDeleted := 0
			listExtentsStats := &shared.ListExtentsStatsRequest{
				DestinationUUID: desc.DestinationUUID,
				Limit:           common.Int64Ptr(DefaultPageSize),
			}

			for {
				listExtentStatsResult, err1 := mClient.ListExtentsStats(listExtentsStats)
				ExitIfError(err1)

				for _, stats := range listExtentStatsResult.ExtentStatsList {
					//extent := stats.GetExtent()
					nTotal++
					if stats.GetStatus() == shared.ExtentStatus_OPEN {
						nOpen++
					} else if stats.GetStatus() == shared.ExtentStatus_SEALED {
						nSealed++
					} else if stats.GetStatus() == shared.ExtentStatus_CONSUMED {
						nConsumed++
					} else if stats.GetStatus() == shared.ExtentStatus_DELETED {
						nDeleted++
					}
				}

				if len(listExtentStatsResult.GetNextPageToken()) == 0 {
					break
				} else {
					listExtentsStats.PageToken = listExtentStatsResult.GetNextPageToken()
				}
			}

			// if -status option not provide, show all the destination path
			status := desc.GetStatus()
			if len(destStatus) == 0 || matchDestStatus(destStatus, status) {
				outputDest := &destJSONOutputFields{
					DestinationName: desc.GetPath(),
					DestinationUUID: desc.GetDestinationUUID(),
					Status:          status,
					Type:            desc.GetType(),
					OwnerEmail:      desc.GetOwnerEmail(),
					TotalExts:       nTotal,

					OpenExts:                    nOpen,
					SealedExts:                  nSealed,
					ConsumedExts:                nConsumed,
					DeletedExts:                 nDeleted,
					ConsumedMessagesRetention:   desc.GetConsumedMessagesRetention(),
					UnconsumedMessagesRetention: desc.GetUnconsumedMessagesRetention(),
					IsMultiZone:                 desc.GetIsMultiZone(),
					ZoneConfigs:                 desc.GetZoneConfigs(),
				}
				destsInfo[status] = append(destsInfo[status], outputDest)
			}
		}

		if len(resp.GetNextPageToken()) == 0 {
			break
		} else {
			req.PageToken = resp.GetNextPageToken()
		}
	}
	// print output here based on the destination status
	for _, dests := range destsInfo {
		for _, dest := range dests {
			outputStr, _ := json.Marshal(dest)
			fmt.Fprintln(os.Stdout, string(outputStr))
		}
	}
}

// ListConsumerGroups return the consumer groups based on the destination provided
func ListConsumerGroups(c *cli.Context, serviceName string) {
	cClient := GetCClient(c, serviceName)

	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}
	path := c.Args()[0]
	var name string
	if len(c.Args()) > 1 {
		name = c.Args()[1]
	}

	req := &cherami.ListConsumerGroupRequest{
		DestinationPath:   &path,
		ConsumerGroupName: &name,
		Limit:             common.Int64Ptr(DefaultPageSize),
	}

	for {
		resp, err := cClient.ListConsumerGroups(req)
		ExitIfError(err)

		for _, desc := range resp.GetConsumerGroups() {
			fmt.Printf("%s %s %s\n", desc.GetDestinationPath(), desc.GetConsumerGroupName(), desc.GetConsumerGroupUUID())
		}

		if len(resp.GetNextPageToken()) == 0 {
			break
		} else {
			req.PageToken = resp.GetNextPageToken()
		}
	}
}

// Publish start to pusblish to the destination provided
func Publish(c *cli.Context, serviceName string) {
	cClient := GetCClient(c, serviceName)

	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}
	path := c.Args().First()

	publisher := cClient.CreatePublisher(&ccli.CreatePublisherRequest{
		Path: path,
	})

	err := publisher.Open()
	ExitIfError(err)

	receiptCh := make(chan *ccli.PublisherReceipt)
	receiptWg := sync.WaitGroup{}
	go func() {
		for receipt := range receiptCh {
			if receipt.Error != nil {
				fmt.Fprintf(os.Stdout, "Error for publish ID %s is %s\n", receipt.ID, receipt.Error.Error())
			} else {
				fmt.Fprintf(os.Stdout, "Receipt for publish ID %s is %s\n", receipt.ID, receipt.Receipt)
			}
			receiptWg.Done()
		}
	}()

	fmt.Fprintf(os.Stdout, "Enter messages to publish, one per line. Ctrl-D to finish.\n")

	var readErr error
	var line []byte
	var id string
	bio := bufio.NewReader(os.Stdin)

	for readErr == nil {

		line, readErr = bio.ReadBytes('\n')
		if len(line) == 0 { // When readErr != nil, line can still be non-empty
			break
		}

		// Increment the WaitGroup before the publish to make sure that
		// we'll never call Done on the WaitGroup before we've called Add.
		receiptWg.Add(1)
		id, err = publisher.PublishAsync(&ccli.PublisherMessage{
			Data: line,
		}, receiptCh)

		if err != nil {
			// If the publish errored, we never actually sent a receipt down the channel.
			receiptWg.Done()
			fmt.Fprintf(os.Stderr, "%v\n", err)
			break
		}

		fmt.Fprintf(os.Stdout, "Local publish ID: %s\n", id)
	}

	if !common.AwaitWaitGroup(&receiptWg, time.Minute) {
		fmt.Fprintf(os.Stderr, "Timed out waiting for receipt.\n")
	}
	publisher.Close()
	close(receiptCh)
}

type kafkaMessageJSON struct {
	EnqueueTime string
	Key         string
	Topic       string
	Partition   int
	Offset      int64
	Msg         string
}

// Consume start to consume from the destination
func Consume(c *cli.Context, serviceName string) {
	cClient := GetCClient(c, serviceName)

	var err error
	if len(c.Args()) < 2 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}
	gOpts := newGlobalOptionsFromCLIContext(c)
	path := c.Args()[0]
	name := c.Args()[1]

	consumer := cClient.CreateConsumer(&ccli.CreateConsumerRequest{
		Path:              path,
		ConsumerGroupName: name,
		ConsumerName:      "",
		PrefetchCount:     c.Int("prefetch_count"),
		Options: &ccli.ClientOptions{
			Timeout:       time.Duration(gOpts.timeoutSecs) * time.Second,
			DeploymentStr: gOpts.env,
		},
	})

	autoAck := c.BoolT("autoack")
	ch := make(chan ccli.Delivery, common.MaxInt(c.Int("prefetch_count")*2, 1))
	ch, err = consumer.Open(ch)
	ExitIfError(err)

	if !autoAck {
		// read ack tokens from Stdin
		go func() {
			var line []byte
			bio := bufio.NewReader(os.Stdin)
			for err == nil {
				line, err = bio.ReadBytes('\n')
				if len(line) > 0 {
					consumer.AckDelivery(string(line))
				}
			}
		}()
	}

	var headerOnce sync.Once
	for delivery := range ch {
		msg := delivery.GetMessage()
		if _, ok := msg.GetPayload().GetUserContext()[`topic`]; ok { // If this is a Kafka-for-Cherami message, print in a JSON blob
			m := msg.GetPayload().GetUserContext()
			var b []byte
			p, _ := strconv.Atoi(m[`partition`])
			o, _ := strconv.ParseInt(m[`offset`], 10, 63)
			b, err = json.Marshal(kafkaMessageJSON{
				Msg:         string(msg.GetPayload().GetData()),
				EnqueueTime: time.Unix(0, msg.GetEnqueueTimeUtc()).Format(time.StampMilli),
				Key:         m[`key`],
				Topic:       m[`topic`],
				Partition:   p,
				Offset:      o,
			})
			if err != nil {
				panic(err)
			}
			if err == nil {
				_, err = fmt.Fprintf(os.Stdout, "%s\n", string(b))
			}
		} else {
			headerOnce.Do(func() {
				fmt.Fprintf(os.Stdout, "%s, %s\n", `Enqueue Time (nanoseconds)`, `Message Data`)
			})
			_, err = fmt.Fprintf(os.Stdout, "%v, %s\n", msg.GetEnqueueTimeUtc(), string(msg.GetPayload().GetData()))
		}
		if autoAck {
			delivery.Ack()
		} else {
			fmt.Fprintf(os.Stdout, "%s\n", delivery.GetDeliveryToken())
		}

		if err != nil {
			if e, ok := err.(*os.PathError); ok {
				if ee, ok1 := e.Err.(syscall.Errno); ok1 && ee == syscall.EPIPE {
					return
				}
			}

			fmt.Fprintf(os.Stderr, "%T %v\n", err, err)
			return
		}
	}
}

func getIfSetInt32(c *cli.Context, p string, setCount *int) (r *int32) {
	if c.IsSet(p) {
		v := int32(c.Int(p))
		*setCount++
		return &v
	}
	return
}

func getIfSetString(c *cli.Context, p string, setCount *int) (r *string) {
	if c.IsSet(p) {
		v := string(c.String(p))
		*setCount++
		return &v
	}
	return
}

func getIfSetDestZoneConfig(c *cli.Context, setCount *int, cliHelper common.CliHelper) (zc *cherami.DestinationZoneConfigs) {
	zoneConfig := getDestZoneConfigs(c, cliHelper)
	if len(zoneConfig.GetConfigs()) > 0 {
		*setCount++
		return &zoneConfig
	}
	return
}

func getIfSetCgZoneConfig(c *cli.Context, mClient mcli.Client, cliHelper common.CliHelper, destinationPath string, setCount *int) (zc *cherami.ConsumerGroupZoneConfigs) {
	zoneConfig := getCgZoneConfigs(c, mClient, cliHelper, destinationPath)
	if len(zoneConfig.GetConfigs()) > 0 {
		*setCount++
		return &zoneConfig
	}
	return
}

func checkOptionChange(c *cli.Context, options map[string]string, flag string) {
	if c.IsSet(flag) {
		if c.Bool(flag) {
			options[flag] = "true"
		} else {
			options[flag] = "false"
		}
	}

}

func getIfSetOptions(c *cli.Context, mClient mcli.Client, path string, name string, setCount *int) (options map[string]string) {
	if c.IsSet(common.FlagDisableNackThrottling) ||
		c.IsSet(common.FlagEnableSmartRetry) ||
		c.IsSet(common.FlagEnableQueueDepthTabulation) {

		cg := getCgFromMedatada(mClient, path, name)
		options := cg.GetOptions()

		checkOptionChange(c, options, common.FlagDisableNackThrottling)
		checkOptionChange(c, options, common.FlagEnableSmartRetry)
		checkOptionChange(c, options, common.FlagEnableQueueDepthTabulation)

		*setCount++
		return options
	}
	return
}

type storeClientCache struct {
	mClient mcli.Client
	cache   map[string]*storehost.StoreClientImpl
}

func newStoreClientCache(mClient mcli.Client) *storeClientCache {
	return &storeClientCache{
		mClient: mClient,
		cache:   make(map[string]*storehost.StoreClientImpl),
	}
}

func (t *storeClientCache) get(storeUUID string) (*storehost.StoreClientImpl, error) {

	client, ok := t.cache[storeUUID]

	if !ok {
		hostAddr, err := t.mClient.UUIDToHostAddr(storeUUID)
		if err != nil {
			hostAddr = storeUUID + UnknownUUID
		}

		client, err = storehost.NewClient(storeUUID, hostAddr)

		if err != nil {
			return nil, err
		}

		t.cache[storeUUID] = client
	}

	return client, nil
}

func (t *storeClientCache) close() {
	for _, client := range t.cache {
		client.Close()
	}
}

type sealcheckJSONOutputFields struct {
	DestinationUUID string `json:"destination_uuid"`
	ExtentUUID      string `json:"extent_uuid"`
	StoreUUID       string `json:"store_uuid"`
	IsSealed        bool   `json:"is_sealed"`
	IsMissing       bool   `json:"is_missing"`
	IsEmpty         bool   `json:"is_empty"`
}

// SealConsistencyCheck implement for show msg command line
func SealConsistencyCheck(c *cli.Context, mClient mcli.Client) {

	prefix := c.String("prefix")
	seal := c.Bool("seal")
	verbose := c.Bool("verbose")
	veryVerbose := c.Bool("veryverbose")
	dlq := c.Bool("dlq")

	storeClients := newStoreClientCache(mClient)
	defer storeClients.close()

	const (
		seqNumBits       = 26
		seqNumBitmask    = (int64(1) << seqNumBits) - 1
		timestampBitmask = math.MaxInt64 &^ seqNumBitmask
		sealKeyTimestamp = math.MaxInt64 & timestampBitmask
	)

	checkDests := func(destUUIDs []string) {

		for _, destUUID := range destUUIDs {

			// find all sealed extents for the destination that are "local" -- ie, we skip
			// extents that belong to a multi-zone destination, but are not in the "origin"
			// zone, because they could potentially be still being replicated.
			listExtentsStats := &shared.ListExtentsStatsRequest{
				DestinationUUID:  common.StringPtr(string(destUUID)),
				Status:           shared.ExtentStatusPtr(shared.ExtentStatus_SEALED),
				LocalExtentsOnly: common.BoolPtr(true), // FIXME: make arg
				Limit:            common.Int64Ptr(DefaultPageSize),
			}

		iterate_listextents_pages:
			for {
				if veryVerbose {
					fmt.Printf("querying metadata: ListExtentsStats(dest=%v status=%v LocalOnly=%v Limit=%v)\n",
						destUUID, shared.ExtentStatus_SEALED, true, DefaultPageSize)
				}

				listExtentStatsResult, err1 := mClient.ListExtentsStats(listExtentsStats)

				if err1 != nil {
					fmt.Fprintf(os.Stderr, "ListExtentsStats(dest=%v) error: %v\n", destUUID, err1)
					break iterate_listextents_pages
				}

				for _, stats := range listExtentStatsResult.ExtentStatsList {

					extent := stats.GetExtent()
					extentUUID := extent.GetExtentUUID()
					storeUUIDs := extent.GetStoreUUIDs()

				iterate_stores:
					for _, storeUUID := range storeUUIDs {

						storeClient, err1 := storeClients.get(storeUUID)

						if err1 != nil {
							fmt.Fprintf(os.Stderr, "error getting store client (store=%v): %v\n", storeUUID, err1)
							continue iterate_stores
						}

						req := store.NewGetAddressFromTimestampRequest()
						req.ExtentUUID = common.StringPtr(string(extentUUID))
						req.Timestamp = common.Int64Ptr(sealKeyTimestamp)

						// query storage to find address of the message with the given timestamp
						resp, err1 := storeClient.GetAddressFromTimestamp(req)

						var extentNotFound bool

						if err1 != nil {
							_, extentNotFound = err1.(*store.ExtentNotFoundError)

							if !extentNotFound {
								fmt.Fprintf(os.Stderr, "dest=%v extent=%v store=%v: GetAddressFromTimestamp error: %v\n",
									destUUID, extentUUID, storeUUID, err1)
								continue iterate_stores
							}
						}

						switch {
						case extentNotFound || !resp.GetSealed(): // handle un-sealed extent

							output := &sealcheckJSONOutputFields{
								DestinationUUID: destUUID,
								ExtentUUID:      extentUUID,
								StoreUUID:       storeUUID,
								IsSealed:        false,
								IsMissing:       extentNotFound,
								IsEmpty:         extentNotFound || resp.GetAddress() == store.ADDR_BEGIN,
							}

							// now seal the extent
							if seal {

								if verbose {
									fmt.Printf("sealing extent on replica: %v %v %v\n", destUUID, extentUUID, storeUUID)
								}

								req := store.NewSealExtentRequest()
								req.ExtentUUID = common.StringPtr(string(extentUUID))
								req.SequenceNumber = nil // seal at 'unspecified' seqnum

								// seal the extent on the store
								err2 := storeClient.SealExtent(req)

								if err2 != nil {
									fmt.Fprintf(os.Stderr, "dest=%v extent=%v store=%v: GetAddressFromTimestamp error: %v\n",
										destUUID, extentUUID, storeUUID, err1)
									continue iterate_stores
								}

								output.IsSealed = true
							}

							outputStr, _ := json.Marshal(output)
							fmt.Fprintln(os.Stdout, string(outputStr))

						default:

							if verbose {

								output := &sealcheckJSONOutputFields{
									DestinationUUID: destUUID,
									ExtentUUID:      extentUUID,
									StoreUUID:       storeUUID,
									IsSealed:        resp.GetSealed(),
									IsMissing:       extentNotFound,
									IsEmpty:         extentNotFound || resp.GetAddress() == store.ADDR_BEGIN,
								}

								outputStr, _ := json.Marshal(output)
								fmt.Fprintln(os.Stdout, string(outputStr))
							}
						}
					}

				}

				if len(listExtentStatsResult.GetNextPageToken()) == 0 {
					break iterate_listextents_pages
				}

				listExtentsStats.PageToken = listExtentStatsResult.GetNextPageToken()
			}
		}
	}

	var getDlqs = func(destUUID string) (dlqs []string) {

		req := &shared.ListConsumerGroupRequest{
			DestinationPath: common.StringPtr(destUUID),
			Limit:           common.Int64Ptr(DefaultPageSize),
		}

		for {
			resp, err := mClient.ListConsumerGroups(req)
			ExitIfError(err)

			for _, cg := range resp.GetConsumerGroups() {

				if cg.IsSetDeadLetterQueueDestinationUUID() {
					dlqs = append(dlqs, cg.GetDeadLetterQueueDestinationUUID())
				}
			}

			if len(resp.GetNextPageToken()) == 0 {
				return
			}

			req.PageToken = resp.GetNextPageToken()
		}
	}

	var getAllDlqs = func() (dlqs map[string][]string) {

		dlqs = make(map[string][]string)

		if veryVerbose {
			fmt.Printf("querying metadata: ListAllConsumerGroups()\n")
		}

		req := &shared.ListConsumerGroupRequest{
			Limit: common.Int64Ptr(DefaultPageSize),
		}

		var nDlqs int

		for {
			resp, err1 := mClient.ListAllConsumerGroups(req)
			ExitIfError(err1)

			for _, cg := range resp.GetConsumerGroups() {

				if cg.IsSetDeadLetterQueueDestinationUUID() {

					destUUID := cg.GetDestinationUUID()
					dlqs[destUUID] = append(dlqs[destUUID], cg.GetDeadLetterQueueDestinationUUID())
					nDlqs++
				}
			}

			if len(resp.GetNextPageToken()) == 0 {

				if veryVerbose {
					fmt.Printf("found %d DLQ destinations for %d destinations\n", nDlqs, len(dlqs))
				}

				return
			}

			req.PageToken = resp.NextPageToken
		}
	}

	if len(c.Args()) > 0 {

		desc, err := mClient.ReadDestination(&shared.ReadDestinationRequest{
			Path: common.StringPtr(c.Args()[0]),
		})

		ExitIfError(err)

		var destUUIDs []string
		destUUIDs = append(destUUIDs, desc.GetDestinationUUID())

		if dlq { // check DLQ destinations
			destUUIDs = append(destUUIDs, getDlqs(desc.GetDestinationUUID())...)
		}

		checkDests(destUUIDs)

	} else {

		var dlqs map[string][]string

		if dlq { // check DLQ destinations
			dlqs = getAllDlqs()
		}

		reqListDest := &shared.ListDestinationsRequest{
			Prefix: common.StringPtr(prefix),
			Limit:  common.Int64Ptr(DefaultPageSize),
		}

	iterate_listdestinations_pages:
		for {
			if veryVerbose {
				fmt.Printf("querying metadata: ListDestinations(prefix=\"%s\")\n", prefix)
			}

			respListDest, err := mClient.ListDestinations(reqListDest)

			if err != nil {
				fmt.Fprintf(os.Stderr, "ListDestinations error: %v\n", err)
				break iterate_listdestinations_pages
			}

			for _, desc := range respListDest.GetDestinations() {

				destUUIDs := []string{desc.GetDestinationUUID()}

				if dlq { // check DLQ destinations
					destUUIDs = append(destUUIDs, dlqs[destUUIDs[0]]...)
				}

				checkDests(destUUIDs)
			}

			if len(respListDest.GetNextPageToken()) == 0 {
				break iterate_listdestinations_pages
			}

			reqListDest.PageToken = respListDest.GetNextPageToken()
		}
	}
}

type sealextentJSONOutputFields struct {
	StoreUUID  string `json:"store_uuid"`
	ExtentUUID string `json:"extent_uuid"`
	IsSealed   bool   `json:"is_sealed"`
}

// StoreSealExtent sends a SealExtent request for an extent on a storehost
func StoreSealExtent(c *cli.Context, mClient mcli.Client) {

	if len(c.Args()) < 2 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	storeUUID, extentUUID := c.Args()[0], c.Args()[1]

	hostAddr, err := mClient.UUIDToHostAddr(storeUUID)
	if err != nil {
		hostAddr = storeUUID + UnknownUUID
	}

	storeClient, err := storehost.NewClient(storeUUID, hostAddr)

	if err != nil {
		fmt.Fprintf(os.Stderr, "error resolving host (%v): %v\n", storeUUID, err)
		return
	}

	req := store.NewSealExtentRequest()
	req.ExtentUUID = common.StringPtr(string(extentUUID))
	req.SequenceNumber = nil // seal at 'unspecified' seqnum

	// send a seal-extent request to store
	err = storeClient.SealExtent(req)

	if err != nil {
		fmt.Fprintf(os.Stderr, "SealExtent error: %v\n", err)
		return
	}

	output := &sealextentJSONOutputFields{
		StoreUUID:  storeUUID,
		ExtentUUID: extentUUID,
		IsSealed:   true,
	}

	outputStr, _ := json.Marshal(output)
	fmt.Fprintln(os.Stdout, string(outputStr))
}

type isextentsealedJSONOutputFields struct {
	StoreUUID  string `json:"store_uuid"`
	ExtentUUID string `json:"extent_uuid"`
	IsSealed   bool   `json:"is_sealed"`
	IsMissing  bool   `json:"is_missing"`
}

// StoreIsExtentSealed checks if an extent is sealed on the specified store
func StoreIsExtentSealed(c *cli.Context, mClient mcli.Client) {

	const (
		seqNumBits       = 26
		seqNumBitmask    = (int64(1) << seqNumBits) - 1
		timestampBitmask = math.MaxInt64 &^ seqNumBitmask
		sealKeyTimestamp = math.MaxInt64 & timestampBitmask
	)

	if len(c.Args()) < 2 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	storeUUID, extentUUID := c.Args()[0], c.Args()[1]

	hostAddr, err := mClient.UUIDToHostAddr(storeUUID)
	if err != nil {
		hostAddr = storeUUID + UnknownUUID
	}

	storeClient, err := storehost.NewClient(storeUUID, hostAddr)

	if err != nil {
		fmt.Fprintf(os.Stderr, "error resolving host (%v): %v\n", storeUUID, err)
		return
	}

	req := store.NewGetAddressFromTimestampRequest()
	req.ExtentUUID = common.StringPtr(string(extentUUID))
	req.Timestamp = common.Int64Ptr(sealKeyTimestamp)

	// query storage to find address of the message with the given timestamp
	resp, err := storeClient.GetAddressFromTimestamp(req)

	var extentNotFound bool

	if err != nil {

		_, extentNotFound = err.(*store.ExtentNotFoundError)

		if !extentNotFound {
			fmt.Fprintf(os.Stderr, "GetAddressFromTimestamp error: %v\n", err)
			return
		}
	}

	output := &isextentsealedJSONOutputFields{
		StoreUUID:  storeUUID,
		ExtentUUID: extentUUID,
		IsMissing:  extentNotFound,
		IsSealed:   (err == nil) && resp.GetSealed(),
	}

	outputStr, _ := json.Marshal(output)
	fmt.Fprintln(os.Stdout, string(outputStr))
}

// StoreGetAddressFromTimestamp sends a GetAddressFromTimestamp request to the specified store
func StoreGetAddressFromTimestamp(c *cli.Context, mClient mcli.Client) {

	// TODO //
}

type purgeJSONOutputFields struct {
	StoreUUID  string `json:"store_uuid"`
	ExtentUUID string `json:"extent_uuid"`
	Address    int64  `json:"first_available_address"`
}

// StorePurgeMessages sends a purge command for an extent to the specified store.
func StorePurgeMessages(c *cli.Context, mClient mcli.Client) {

	if len(c.Args()) < 2 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	storeUUID, extentUUID := c.Args()[0], c.Args()[1]

	deleteExtent := c.Bool("entirely")
	purgeAddr := c.Int64("address")

	if !deleteExtent && purgeAddr == 0 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	if deleteExtent {
		purgeAddr = store.ADDR_SEAL
	}

	hostAddr, err := mClient.UUIDToHostAddr(storeUUID)
	if err != nil {
		hostAddr = storeUUID + UnknownUUID
	}

	storeClient, err := storehost.NewClient(storeUUID, hostAddr)

	if err != nil {
		fmt.Fprintf(os.Stderr, "error resolving host (%v): %v\n", storeUUID, err)
		return
	}

	req := store.NewPurgeMessagesRequest()
	req.ExtentUUID = common.StringPtr(string(extentUUID))
	req.Address = common.Int64Ptr(purgeAddr)

	// send a purge-messages request
	resp, err := storeClient.PurgeMessages(req)

	if err != nil {
		fmt.Fprintf(os.Stderr, "PurgeMessages error: %v\n", err)
		return
	}

	output := &purgeJSONOutputFields{
		StoreUUID:  storeUUID,
		ExtentUUID: extentUUID,
		Address:    resp.GetAddress(),
	}

	outputStr, _ := json.Marshal(output)
	fmt.Fprintln(os.Stdout, string(outputStr))
}

type listextentsJSONOutputFields struct {
	StoreUUID    string `json:"store_uuid"`
	ExtentUUID   string `json:"extent_uuid"`
	Size         int64  `json:"size"`
	Modified     int64  `json:"modified_time"`
	ModifiedTime string `json:"modified_time_string"`
}

// StoreListExtents sends a request to the specified store to get a list of extents
func StoreListExtents(c *cli.Context, mClient mcli.Client) {

	if len(c.Args()) < 1 {
		ExitIfError(errors.New(strNotEnoughArgs))
	}

	storeUUID := c.Args()[0]

	hostAddr, err := mClient.UUIDToHostAddr(storeUUID)
	if err != nil {
		hostAddr = storeUUID + UnknownUUID
	}

	storeClient, err := storehost.NewClient(storeUUID, hostAddr)

	if err != nil {
		fmt.Fprintf(os.Stderr, "error resolving host (%v): %v\n", storeUUID, err)
		return
	}

	// send a list-extents request
	resp, err := storeClient.ListExtents()

	if err != nil {
		fmt.Fprintf(os.Stderr, "ListExtents error: %v\n", err)
		return
	}

	for _, x := range resp.GetExtents() {

		output := &listextentsJSONOutputFields{
			StoreUUID:  storeUUID,
			ExtentUUID: x.GetExtentUUID(),
			// DestinationUUID: x.GetDestinationUUID(), // TODO: currently unavailable
		}

		// size/modified time info may not be available
		if x.IsSetSize() {

			output.Size = x.GetSize()

			mTime := x.GetModifiedTime()
			output.Modified = mTime
			output.ModifiedTime = time.Unix(0, mTime).String()
		}

		outputStr, _ := json.Marshal(output)
		fmt.Fprintln(os.Stdout, string(outputStr))
	}
}
