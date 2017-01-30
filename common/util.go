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
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common/configure"
	"github.com/uber/cherami-server/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/ringpop-go"
	"github.com/uber/ringpop-go/discovery/statichosts"
	"github.com/uber/ringpop-go/swim"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/hyperbahn"
	"github.com/uber/tchannel-go/thrift"
)

var rpMutex sync.RWMutex // protect ringpop objects
var ringpopBootstrapFile string

const hyperbahnPort int16 = 21300
const rpAppNamePrefix string = "cherami"
const maxRpJoinTimeout = 30 * time.Second
const maxRateExtrapolationTime = 120.0

// MaxDuration is maximum time duration
const MaxDuration time.Duration = 1<<62 - 1

var ports = []int{4240, 4253, 4254, 5425, 4922}

const (
	inputHostAdminChannelName  = "inputhost-admin-client"
	outputHostAdminChannelName = "outputhost-admin-client"
	storeHostClientChannelName = "storehost-client"
)

// some default values for the limits
// TODO: this will be moved behind a separate "limits" interface
const (
	// MaxHostOverallConn is the maximam overall connection limit for this host
	// TODO: Need to figure out the suitable values
	MaxHostOverallConn = 100000
	// HostOverallConnLimit is the overall connection limit for this host
	HostOverallConnLimit = 10000

	// MaxHostPerSecondConn is the maximam per second  rate limit for this host
	// TODO: Need to figure out the suitable values
	MaxHostPerSecondConn = 10000
	// HostPerSecondConnLimit is the per second rate limit for this host
	HostPerSecondConnLimit = 1000

	//MaxHostPerConnMsgsLimitPerSecond is the maximam for per connection messages limit
	// TODO: Need to figure out the suitable values
	MaxHostPerConnMsgsLimitPerSecond = 800000
	// HostPerConnMsgsLimitPerSecond is the per connection messages limit
	HostPerConnMsgsLimitPerSecond = 80000

	//MaxHostPerExtentMsgsLimitPerSecond is the maximam for per extent messages limit
	// TODO: Need to figure out the suitable values
	MaxHostPerExtentMsgsLimitPerSecond = 200000
	// HostPerExtentMsgsLimitPerSecond is the per extent messages limit
	HostPerExtentMsgsLimitPerSecond = 20000

	// MaxHostMaxConnPerDestination is the maximam for max connections per destination
	// TODO: Need to figure out the suitable values
	MaxHostMaxConnPerDestination = 10000
	// HostMaxConnPerDestination is the max connections per destination
	HostMaxConnPerDestination = 1000
)

// Utlity routines for ringpop..
func buildRingpopHosts(ipaddr string, port int) []string {
	var ringpopHosts []string
	ringpopHosts = append(ringpopHosts, fmt.Sprintf("%s:%d", ipaddr, port))
	for _, indPort := range ports {
		if indPort != port {
			log.Infof("appending port: %v", indPort)
			ringpopHosts = append(ringpopHosts, fmt.Sprintf("%s:%d", ipaddr, indPort))
		}
	}
	return ringpopHosts
}

// CreateRingpop instantiates the ringpop for the provided channel and host,
func CreateRingpop(service string, ch *tchannel.Channel, ipaddr string, port int) *(ringpop.Ringpop) {
	rp, _ := ringpop.New(fmt.Sprintf("%s", rpAppNamePrefix), ringpop.Channel(ch), ringpop.Identity(fmt.Sprintf("%s:%d", ipaddr, port)))

	return rp
}

// GetDefaultLogger is a utility routine to get the default logger
func GetDefaultLogger() bark.Logger {
	return bark.NewLoggerFromLogrus(log.StandardLogger())
}

// BootstrapRingpop tries to bootstrap the given ringpop instance using the hosts list
func BootstrapRingpop(rp *ringpop.Ringpop, ipaddr string, port int, cfg configure.CommonServiceConfig) error {
	lcLg := GetDefaultLogger()
	if rp == nil {
		// No ringpop to bootstrap
		lcLg.WithField(TagHostIP, FmtHostIP(ipaddr)).Error("no ringpop to bootstrap")
		return errors.New("cannot bootstrap a nil ringpop")
	}

	var rpHosts []string

	if len(cfg.GetRingHosts()) > 0 {
		// Trim the trailing comma in rpHosts
		// For example: If rpHosts = "10.x.y.z,", we should trim
		// it to say, rpHostsTrimmed = "10.x.y.z"
		rpHostsTrimmed := strings.TrimRight(cfg.GetRingHosts(), ",")
		rpHosts = strings.Split(rpHostsTrimmed, ",")
	}

	if len(rpHosts) == 0 {
		rpHosts = buildRingpopHosts(ipaddr, port)
	}

	lcLg.WithFields(bark.Fields{TagHostIP: FmtHostIP(ipaddr), `RingHosts`: rpHosts}).
		Debug("RingHosts")

	bOptions := new(swim.BootstrapOptions)
	bOptions.DiscoverProvider = statichosts.New(rpHosts...)
	bOptions.MaxJoinDuration = maxRpJoinTimeout
	bOptions.JoinSize = 1 // this ensures the first guy comes up quickly

	_, err := rp.Bootstrap(bOptions)
	return err
}

func getHyperbahnInitialNodes(bootstrapFile string) []string {
	ip, _ := tchannel.ListenIP()
	ret := []string{fmt.Sprintf("%s:%d", ip.String(), hyperbahnPort)}

	if len(bootstrapFile) < 1 {
		return ret
	}

	blob, err := ioutil.ReadFile(bootstrapFile)
	if err != nil {
		return ret
	}

	err = json.Unmarshal(blob, &ret)
	if err != nil {
		return ret
	}

	return ret
}

func isPersistentService(name string) bool {
	return (strings.Compare(name, StoreServiceName) == 0)
}

// SetupServerConfig reads on-disk config (in config/)
func SetupServerConfig(configurator configure.Configure) configure.CommonAppConfig {
	return configurator.SetupServerConfig()
}

// CreateHyperbahnClient returns a hyperbahn client
func CreateHyperbahnClient(ch *tchannel.Channel, bootstrapFile string) *hyperbahn.Client {
	initialNodes := getHyperbahnInitialNodes(bootstrapFile)
	config := hyperbahn.Configuration{InitialNodes: initialNodes}
	if len(config.InitialNodes) == 0 {
		log.Fatal("No Hyperbahn nodes to connect to.")
	}
	hClient, _ := hyperbahn.NewClient(ch, config, nil)
	return hClient
}

// ServiceLoop runs the http admin endpoints. This is a blocking call.
func ServiceLoop(port int, cfg configure.CommonAppConfig, service *Service) {
	httpHandlers := NewHTTPHandler(cfg, service)
	mux := http.NewServeMux()
	httpHandlers.Register(mux)

	listenAddress := `127.0.0.1`
	if service.cfg.GetListenAddress().IsLoopback() { // If we have a particular loopback listen address, override the default
		listenAddress = service.cfg.GetListenAddress().String()
	}
	log.Info(fmt.Sprintf("Diagnostic http endpoint listening on %s:%d", listenAddress, port))
	log.Panic(http.ListenAndServe(fmt.Sprintf("%s:%d", listenAddress, port), mux))
}

// WSStart register websocket handlers and spin up websocket server.
// This is not a blocking call
func WSStart(listenAddress string, port int, wsservice WSService) {
	mux := wsservice.RegisterWSHandler()
	go func() {
		log.Info(fmt.Sprintf("WebSocket listening %s:%d", listenAddress, port))
		log.Panic(http.ListenAndServe(fmt.Sprintf("%v:%d", listenAddress, port), mux))
	}()
}

// SplitHostPort takes a x.x.x.x:yyyy string and split it into host and ports
func SplitHostPort(hostPort string) (string, int, error) {
	if len(hostPort) == 0 {
		return "", 0, nil
	}

	parts := strings.Split(hostPort, ":")
	port, err := strconv.Atoi(parts[1])
	return parts[0], port, err
}

var guidRegex = regexp.MustCompile(`([[:xdigit:]]{8})-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}`)

// ShortenGUIDString takes a string with one or more GUIDs and elides them to make it more human readable. It turns
// "354754bd-b73e-4d20-8021-ab93a3d145c0:67af70c5-f45e-4b3d-9d20-6758195e2ff4:3:2" into "354754bd:67af70c5:3:2"
func ShortenGUIDString(s string) string {
	return guidRegex.ReplaceAllString(s, "$1")
}

// UUIDToUint16 uses the UUID and returns a uint16 out of it
func UUIDToUint16(s string) uint16 {
	if !guidRegex.MatchString(s) {
		log.Fatalf("Input is not an UUID, %v", s)
	}
	return uint16(UUIDHashCode(s))
}

// ConditionFunc represents an expression that evaluates to
// true on when some condition is satisfied and false otherwise
type ConditionFunc func() bool

// SpinWaitOnCondition busy waits for a given condition to be true until the timeout
// Returns true if the condition was satisfied, false on timeout
func SpinWaitOnCondition(condition ConditionFunc, timeout time.Duration) bool {

	timeoutCh := time.After(timeout)

	for !condition() {
		select {
		case <-timeoutCh:
			return false
		default:
			time.Sleep(time.Millisecond * 5)
		}
	}

	return true
}

// AwaitWaitGroup calls Wait on the given wait
// Returns true if the Wait() call succeeded before the timeout
// Returns false if the Wait() did not return before the timeout
func AwaitWaitGroup(wg *sync.WaitGroup, timeout time.Duration) bool {

	doneC := make(chan struct{})

	go func() {
		wg.Wait()
		close(doneC)
	}()

	select {
	case <-doneC:
		return true
	case <-time.After(timeout):
		return false
	}
}

// RWLockReadAndConditionalWrite implements the RWLock Read+Read&Conditional-Write pattern.
// m is the RWMutex covering a shared resource
// readFn is a function that returns a true if a write on the shared resource is required.
// writeFn is a function that updates the shared resource.
// The result of the read/write can be returned by capturing return variables in your provided functions
func RWLockReadAndConditionalWrite(m *sync.RWMutex, readFn func() bool, writeFn func()) {
	m.RLock()
	writeRequired := readFn()
	m.RUnlock()
	if writeRequired {
		m.Lock()
		writeRequired = readFn()
		if writeRequired {
			writeFn()
		}
		m.Unlock()
	}
	return
}

// CreateInputHostAdminClient creates and returns tchannel client
// for the input host admin API
func CreateInputHostAdminClient(ch *tchannel.Channel, hostPort string) (admin.TChanInputHostAdmin, error) {
	tClient := thrift.NewClient(ch, InputServiceName, &thrift.ClientOptions{
		HostPort: hostPort,
	})
	client := admin.NewTChanInputHostAdminClient(tClient)
	return client, nil
}

// CreateOutputHostAdminClient creates and returns tchannel client
// for the output host admin API
func CreateOutputHostAdminClient(ch *tchannel.Channel, hostPort string) (admin.TChanOutputHostAdmin, error) {
	tClient := thrift.NewClient(ch, OutputServiceName, &thrift.ClientOptions{
		HostPort: hostPort,
	})
	client := admin.NewTChanOutputHostAdminClient(tClient)
	return client, nil
}

// IsRetryableTChanErr returns true if the given tchannel
// error is a retryable error.
func IsRetryableTChanErr(err error) bool {
	return (err == tchannel.ErrTimeout ||
		err == tchannel.ErrServerBusy ||
		err == tchannel.ErrRequestCancelled)
}

// GetTagsFromPath function return the tags name for path  based on directory path name passed
// Usually pass the Consumer group name or a destination path name to get a tag name
func GetTagsFromPath(path string) (string, error) {
	if PathRegex.MatchString(path) {
		// Get the  DLQ "path" is the consumer group name plus .dlq
		tagPath := strings.Replace(path, "/", "_", -1)
		return tagPath, nil
	}
	return "", fmt.Errorf("Invalid path name: %v", path)
}

// GetDirectoryName function gives the directory name given a path used for destination or consumer groups
func GetDirectoryName(path string) (string, error) {
	parts := strings.Split(path, "/")
	if len(parts) < 3 {
		return "", fmt.Errorf("Invalid path: %v", path)
	}

	return parts[1], nil
}

// GetDLQPathNameFromCGName function return the DLQ destination name based on the consumer group passed
// Usually pass the Consumer group name to get a DLQ path name
// DEVNOTE: DO NOT QUERY A DLQ DESTINATION BY THIS NAME. This name is for reporting purposes only.
// All destination APIs support passing the DLQ UUID as the path.
func GetDLQPathNameFromCGName(CGName string) (string, error) {
	if PathRegex.MatchString(CGName) {
		// Get the  DLQ "path" is the consumer group name plus .dlq
		dlqPath := fmt.Sprintf("%s.dlq", CGName)
		return dlqPath, nil
	}
	return "", fmt.Errorf("Invalid Consumer group name: %v", CGName)
}

// GetDateTag returns the current date used for tagging daily metric
func GetDateTag() string {
	return time.Now().Format("2006-01-02")
}

// GetConnectionKey is used to create a key used by connections for looking up connections
func GetConnectionKey(host *cherami.HostAddress) string {
	return fmt.Sprintf("%v:%d", host.GetHost(), host.GetPort())
}

// GetRandInt64 is used to get a 64 bit random number between min and max
func GetRandInt64(min int64, max int64) int64 {
	// we need to get a random number between min and max
	return min + rand.Int63n(max-min)
}

// UUIDHashCode is a hash function for hashing string uuid
// if the uuid is malformed, then the hash function always
// returns 0 as the hash value
func UUIDHashCode(key string) uint32 {
	if len(key) != UUIDStringLength {
		return 0
	}
	// Use the first 4 bytes of the uuid as the hash
	b, err := hex.DecodeString(key[:8])
	if err != nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}

// SequenceNumber is an int64 number represents the sequence of messages in Extent
type SequenceNumber int64

// UnixNanoTime is Unix time as nanoseconds since Jan 1st, 1970, 00:00 GMT
type UnixNanoTime int64

// Seconds is time as seconds, either relative or absolute since the epoch
type Seconds float64

// ToSeconds turns a relative or absolute UnixNanoTime to float Seconds
func (u UnixNanoTime) ToSeconds() Seconds {
	return Seconds(float64(u) / float64(1e9))
}

// ToSecondsFmt turns a relative or absolute UnixNanoTime to float Seconds, and returns 'never' if the input is zero
func (u UnixNanoTime) ToSecondsFmt() string {
	s := u.ToSeconds()
	if s > 0.99*Now().ToSeconds() { // If we probably calculated a relative time now - zero
		return `never`
	}
	return fmt.Sprintf(`%v`, s)
}

// DurationToSeconds converts a time.Duration to Seconds
func DurationToSeconds(t time.Duration) Seconds {
	return Seconds(float64(int64(t)) / float64(int64(time.Second)))
}

// Now is the version to return UnixNanoTime
func Now() UnixNanoTime {
	return UnixNanoTime(time.Now().UnixNano())
}

// ExtrapolateDifference calculates the extrapolated difference in two observed value with rates, at some arbitrary time. It is assumed that A > B, so if B is extrapolated to be greater than A, the difference will be presumed to be zero.
func ExtrapolateDifference(observedA, observedB SequenceNumber, observedARate, observedBRate float64, observedATime, observedBTime, extrapolatedTime UnixNanoTime, maxExtrapolationTime Seconds) (extrapolated int64) {
	if extrapolatedTime == 0 {
		if observedA < observedB {
			return 0
		}
		return int64(observedA - observedB)
	}

	extrapolatedA := ExtrapolateValue(observedA, observedARate, observedATime, extrapolatedTime, maxExtrapolationTime)
	extrapolatedB := ExtrapolateValue(observedB, observedBRate, observedBTime, extrapolatedTime, maxExtrapolationTime)

	// Force a positive value
	if extrapolatedA < extrapolatedB {
		extrapolatedA = extrapolatedB
	}

	return int64(extrapolatedA - extrapolatedB)
}

// ExtrapolateValue extrapolates a value based on an observed value and rate at a given time
func ExtrapolateValue(observed SequenceNumber, observedRate float64, observedTime, extrapolatedTime UnixNanoTime, maxExtrapolationTime Seconds) (extrapolated SequenceNumber) {
	if observedTime == 0 || extrapolatedTime == 0 {
		return observed
	}

	deltaT := float64((extrapolatedTime - observedTime).ToSeconds())
	deltaV := float64(observedRate * deltaT)

	if math.Abs(float64(deltaT)) > float64(maxExtrapolationTime) {
		return observed
	}

	return observed + SequenceNumber(math.Floor(deltaV))
}

// CalculateRate does a simple rate calculation
func CalculateRate(last, curr SequenceNumber, lastTime, currTime UnixNanoTime) float64 {
	if lastTime == 0 || currTime == 0 {
		return float64(0)
	}

	deltaV := float64(curr - last)
	deltaT := (currTime - lastTime).ToSeconds()

	if math.Abs(float64(deltaT)) > maxRateExtrapolationTime {
		return float64(0)
	}

	return float64(deltaV / float64(deltaT))
}

// RandomBytes generates random bytes of given size
func RandomBytes(size int) []byte {
	var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	b := make([]byte, size)
	for i := range b {
		b[i] = letters[rand.Int63()%int64(len(letters))]
	}

	return b
}

// WaitTimeout waits for given func until timeout (return true if timeout)
func WaitTimeout(timeout time.Duration, fn func()) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		fn()
	}()
	select {
	case <-c:
		return false
	case <-time.After(timeout):
		return true
	}
}

// GeometricRollingAverage is the value of a geometrically diminishing rolling average
type GeometricRollingAverage float64

// SetGeometricRollingAverage adds a value to the geometric rolling average
func (avg *GeometricRollingAverage) SetGeometricRollingAverage(val float64) {
	const rollingAverageFalloff = 100
	*avg -= *avg / GeometricRollingAverage(rollingAverageFalloff)
	*avg += GeometricRollingAverage(val) / GeometricRollingAverage(rollingAverageFalloff)
}

// GetGeometricRollingAverage returns the result of the geometric rolling average
func (avg *GeometricRollingAverage) GetGeometricRollingAverage() float64 {
	return float64(*avg)
}

//NewMetricReporterWithHostname create statsd/simple reporter based on config
func NewMetricReporterWithHostname(cfg configure.CommonServiceConfig) metrics.Reporter {
	hostName, e := os.Hostname()
	lcLg := GetDefaultLogger()
	if e != nil {
		lcLg.WithFields(bark.Fields{TagErr: e}).Fatal("Error getting hostname")
	}

	mCfg := cfg.GetMetricsConfig().(*configure.MetricsConfig)

	// if we don't have any config object, just instantiate a simple reporter
	if mCfg == nil || mCfg.Statsd == nil || len(mCfg.Statsd.HostPort) == 0 {
		reporter := metrics.NewSimpleReporter(map[string]string{
			metrics.HostnameTagName: hostName,
		})
		return reporter
	}

	reporter := metrics.NewStatsdReporter(mCfg.Statsd.HostPort, mCfg.Statsd.Prefix,
		mCfg.Statsd.FlushInterval, mCfg.Statsd.FlushBytes, map[string]string{
			metrics.HostnameTagName: hostName,
		})
	return reporter
}

//NewTestMetricsReporter creates a test reporter that allows registration of handler functions
func NewTestMetricsReporter() metrics.Reporter {
	hostName, e := os.Hostname()
	lcLg := GetDefaultLogger()
	if e != nil {
		lcLg.WithFields(bark.Fields{TagErr: e}).Fatal("Error getting hostname")
	}

	reporter := metrics.NewTestReporter(map[string]string{
		metrics.HostnameTagName: hostName,
	})
	return reporter
}

//GetLocalClusterInfo gets the zone and tenancy from the given deployment
func GetLocalClusterInfo(deployment string) (zone string, tenancy string) {
	parts := strings.Split(deployment, "_")
	if len(parts) != 2 {
		return deployment, TenancyProd
	}
	return parts[1], parts[0]
}

// IsRemoteZoneExtent returns whether the extent is a remote zone extent
func IsRemoteZoneExtent(extentOriginZone string, localZone string) bool {
	return len(extentOriginZone) > 0 && !strings.EqualFold(extentOriginZone, localZone)
}

// GetEnvVariableFromHostPort gets the environment variable corresponding to this
// host port.
// XXX: this can be removed once we move to ringpop labels and exchange websocket
// port as part of the ringpop metadata
func GetEnvVariableFromHostPort(hostPort string) (envVar string) {
	r := strings.NewReplacer(".", "_", ":", "_")
	return fmt.Sprintf("IP_%v", r.Replace(hostPort))
}

type cliHelper struct {
	defaultOwnerEmail string
	cZones            map[string]string
}

// GetDefaultOwnerEmail is the implementation of the corresponding method
func (r *cliHelper) GetDefaultOwnerEmail() string {
	return r.defaultOwnerEmail
}

// GetCanonicalZone is the implementation of the corresponding method
func (r *cliHelper) GetCanonicalZone(zone string) (cZone string, err error) {
	var ok bool
	if len(zone) == 0 {
		return "", errors.New("Invalid Zone Name")
	}

	// If canonical zone list is empty, then any zone is valid
	if len(r.cZones) == 0 {
		return zone, nil
	}

	if cZone, ok = r.cZones[zone]; !ok {
		return "", errors.New("Invalid Zone Name")
	}

	return
}

// SetDefaultOwnerEmail is the implementation of the corresponding method
func (r *cliHelper) SetDefaultOwnerEmail(oe string) {
	r.defaultOwnerEmail = oe
}

// SetCanonicalZones is the implementation of the corresponding method
func (r *cliHelper) SetCanonicalZones(cZones map[string]string) {
	for k, v := range cZones {
		r.cZones[k] = v
	}
}

// NewCliHelper is used to create an uber specific CliHelper
func NewCliHelper() CliHelper {
	return &cliHelper{
		defaultOwnerEmail: "cherami@cli",
		cZones:            make(map[string]string),
	}
}

var overrideValueByPrefixLogMapLock sync.RWMutex
var overrideValueByPrefixLogMap = make(map[string]struct{})

// OverrideValueByPrefix takes a list of override rules in the form 'prefix=val' and a given string, and determines the most specific rule
// that applies to the given string. It then replaces the given default value with the override value. logFn is a logging closure that
// allows lazy instatiation of a logger interface to log error conditions and override status. valName is used for logging purposes, to
// differentiate multiple instantiations in the same context
//
// As an example, you could override a parameter, like the number of desired extents, according to various destination paths.
// We could try to have 8 extents by default, and give destinations beginning with /test only 1, and give a particular destination
// specifically a higher amount. To achieve this, we could configure the overrides like this:
//
// overrides := {`=8`, `/test=1`, `/JobPlatform/TripEvents$=16`}
//
func OverrideValueByPrefix(logFn func() bark.Logger, path string, overrides []string, defaultVal int64, valName string) int64 {
	// Terminate the path with a dollarsign, so that we can do something like this:
	//
	// This rule : /foo/bar$=X
	// Gives:
	// /foo/bar     = X
	// /foo/bar_baz = default
	// /foo/quz     = default
	//
	// This rule : /foo/bar=X
	// Gives:
	// /foo/bar     = X
	// /foo/bar_baz = X
	// /foo/quz     = default
	//
	path += `$`

	var longestMatchValue int64
	var longestMatchKey string
	var hasMatch bool
	var err error

moreOverrides:
	for _, ovrd := range overrides {
		split := strings.Split(ovrd, `=`)
		if len(split) != 2 {
			logFn().WithFields(bark.Fields{`rule`: ovrd, `valName`: valName}).Error(`Invalid override rule, couldn't split`)
			continue moreOverrides
		}
		if strings.HasPrefix(path, split[0]) {
			if len(split[0]) > len(longestMatchKey) || (split[0] == `` && !hasMatch) { // Match for a longer key, or just the empty default
				var lmv int
				lmv, err = strconv.Atoi(split[1])
				if err != nil {
					logFn().WithFields(bark.Fields{`rule`: ovrd, `valName`: valName, TagErr: err}).Error(`Invalid override rule, couldn't covert value`)
					continue moreOverrides
				}
				longestMatchKey = split[0]
				longestMatchValue = int64(lmv)
				hasMatch = true
			}
		}
	}

	if hasMatch {
		if len(longestMatchKey) > 1 { // Don't log for very short prefixes, e.g. '/', ''
			// Log just once for this particular rule; if the value or rule changes, log again
			logMapKey := valName + path + longestMatchKey + strconv.Itoa(int(longestMatchValue))

			readFn := func() bool {
				_, logPreviouslyEmitted := overrideValueByPrefixLogMap[logMapKey]
				return !logPreviouslyEmitted
			}
			writeFn := func() {
				if _, logPreviouslyEmitted2 := overrideValueByPrefixLogMap[logMapKey]; logPreviouslyEmitted2 {
					return
				}
				overrideValueByPrefixLogMap[logMapKey] = struct{}{}
				logFn().WithFields(bark.Fields{
					`valName`:  valName,
					`rule`:     longestMatchKey,
					`path`:     path,
					`override`: longestMatchValue}).Info(`Overrided value`)
			}
			RWLockReadAndConditionalWrite(&overrideValueByPrefixLogMapLock, readFn, writeFn)
		}
		return longestMatchValue
	}

	return defaultVal
}
