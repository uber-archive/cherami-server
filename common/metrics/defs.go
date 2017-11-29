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

package metrics

// types used/defined by the package
type (
	// MetricName is the name of the metric
	MetricName string

	// MetricType is the type of the metric
	MetricType int

	// metricDefinition contains the definition for a metric
	metricDefinition struct {
		metricType int    // metric type
		metricName string // metric name
	}

	// scopeDefinition holds the tag definitions for a scope
	scopeDefinition struct {
		operation string            // 'operation' tag for scope
		tags      map[string]string // additional tags for scope
	}

	// ServiceIdx is an index that uniquely identifies the service
	ServiceIdx int
)

// MetricTypes which are supported
const (
	Counter = iota
	Timer
	Gauge
)

// Service names for all services that emit Metrics
const (
	Common ServiceIdx = iota
	Frontend
	Controller
	Inputhost
	Outputhost
	Storage
	Replicator
)

// Common tags for all services
const (
	OperationTagName     = "operation"
	DestinationTagName   = "destination"
	ConsumerGroupTagName = "consumerGroup"
	HostnameTagName      = "hostname"
)

// Cherami's metrics are tagged, but if we need to flatten
// it (e.g. for statsd), we order tags by: Operation,
// Destination, ConsumerGroup, Hostname
var tagsFlattenOrder = []string{
	OperationTagName,
	DestinationTagName,
	ConsumerGroupTagName,
	HostnameTagName,
}

// This package should hold all the metrics and tags for cherami
const (
	UnknownDirectoryTagValue = "Unknown"
)

// Common service base metrics
const (
	RestartCount         = "restarts"
	NumGoRoutinesGauge   = "num-goroutines"
	GoMaxProcsGauge      = "gomaxprocs"
	MemoryAllocatedGauge = "memory.allocated"
	MemoryHeapGauge      = "memory.heap"
	MemoryHeapIdleGauge  = "memory.heapidle"
	MemoryHeapInuseGauge = "memory.heapinuse"
	MemoryStackGauge     = "memory.stack"
	NumGCCounter         = "memory.num-gc"
	GcPauseMsTimer       = "memory.gc-pause-ms"
)

// ServiceMetrics are types for common service base metrics
var ServiceMetrics = map[MetricName]MetricType{
	RestartCount: Counter,
}

// GoRuntimeMetrics represent the runtime stats from go runtime
var GoRuntimeMetrics = map[MetricName]MetricType{
	NumGoRoutinesGauge:   Gauge,
	GoMaxProcsGauge:      Gauge,
	MemoryAllocatedGauge: Gauge,
	MemoryHeapGauge:      Gauge,
	MemoryHeapIdleGauge:  Gauge,
	MemoryHeapInuseGauge: Gauge,
	MemoryStackGauge:     Gauge,
	NumGCCounter:         Counter,
	GcPauseMsTimer:       Timer,
}

// Scope enum
const (
	// -- Operation scopes for Metadata (common) --

	// MetadataListEntityOpsScope defines scope for an operation on metadata
	MetadataListEntityOpsScope = iota
	// MetadataHostAddrToUUIDScope defines scope for an operation on metadata
	MetadataHostAddrToUUIDScope
	// MetadataListAllConsumerGroupsScope defines scope for an operation on metadata
	MetadataListAllConsumerGroupsScope
	// MetadataListConsumerGroupsScope defines scope for an operation on metadata
	MetadataListConsumerGroupsScope
	// MetadataListConsumerGroupsUUIDScope defines scope for an operation on metadata
	MetadataListConsumerGroupsUUIDScope
	// MetadataListDestinationsScope defines scope for an operation on metadata
	MetadataListDestinationsScope
	// MetadataListDestinationsByUUIDScope defines scope for an operation on metadata
	MetadataListDestinationsByUUIDScope
	// MetadataListDestinationExtentsScope defines the scope for an operation on metadata
	MetadataListDestinationExtentsScope
	// MetadataListExtentsStatsScope defines scope for an operation on metadata
	MetadataListExtentsStatsScope
	// MetadataListHostsScope defines scope for an operation on metadata
	MetadataListHostsScope
	// MetadataListInputHostExtentsStatsScope defines scope for an operation on metadata
	MetadataListInputHostExtentsStatsScope
	// MetadataListStoreExtentsStatsScope defines scope for an operation on metadata
	MetadataListStoreExtentsStatsScope
	// MetadataReadConsumerGroupScope defines scope for an operation on metadata
	MetadataReadConsumerGroupScope
	// MetadataReadConsumerGroupByUUIDScope defines scope for an operation on metadata
	MetadataReadConsumerGroupByUUIDScope
	// MetadataReadConsumerGroupExtentScope defines scope for an operation on metadata
	MetadataReadConsumerGroupExtentScope
	// MetadataReadConsumerGroupExtentsScope defines scope for an operation on metadata
	MetadataReadConsumerGroupExtentsScope
	// MetadataReadConsumerGroupExtentsLiteScope defines scope for an operation on metadata
	MetadataReadConsumerGroupExtentsLiteScope
	// MetadataReadConsumerGroupExtentsByExtUUIDScope defines scope for an operation on metadata
	MetadataReadConsumerGroupExtentsByExtUUIDScope
	// MetadataReadDestinationScope defines scope for an operation on metadata
	MetadataReadDestinationScope
	// MetadataReadExtentStatsScope defines scope for an operation on metadata
	MetadataReadExtentStatsScope
	// MetadataUUIDToHostAddrScope defines scope for an operation on metadata
	MetadataUUIDToHostAddrScope
	// MetadataUpdateServiceConfigScope defines scope for an operation on metadata
	MetadataUpdateServiceConfigScope
	// MetadataCreateConsumerGroupScope defines scope for an operation on metadata
	MetadataCreateConsumerGroupScope
	// MetadataCreateConsumerGroupUUIDScope defines scope for an operation on metadata
	MetadataCreateConsumerGroupUUIDScope
	// MetadataCreateConsumerGroupExtentScope defines scope for an operation on metadata
	MetadataCreateConsumerGroupExtentScope
	// MetadataCreateDestinationScope defines scope for an operation on metadata
	MetadataCreateDestinationScope
	// MetadataCreateDestinationUUIDScope defines scope for an operation on metadata
	MetadataCreateDestinationUUIDScope
	// MetadataCreateExtentScope defines scope for an operation on metadata
	MetadataCreateExtentScope
	// MetadataCreateHostInfoScope defines scope for an operation on metadata
	MetadataCreateHostInfoScope
	// MetadataCreateServiceConfigScope defines scope for an operation on metadata
	MetadataCreateServiceConfigScope
	// MetadataDeleteConsumerGroupScope defines scope for an operation on metadata
	MetadataDeleteConsumerGroupScope
	// MetadataDeleteConsumerGroupUUIDScope defines scope for an operation on metadata
	MetadataDeleteConsumerGroupUUIDScope
	// MetadataDeleteDestinationScope defines scope for an operation on metadata
	MetadataDeleteDestinationScope
	// MetadataDeleteDestinationUUIDScope defines scope for an operation on metadata
	MetadataDeleteDestinationUUIDScope
	// MetadataDeleteHostInfoScope defines scope for an operation on metadata
	MetadataDeleteHostInfoScope
	// MetadataDeleteServiceConfigScope defines scope for an operation on metadata
	MetadataDeleteServiceConfigScope
	// MetadataMoveExtentScope defines scope for an operation on metadata
	MetadataMoveExtentScope
	// MetadataReadHostInfoScope defines scope for an operation on metadata
	MetadataReadHostInfoScope
	// MetadataReadServiceConfigScope defines scope for an operation on metadata
	MetadataReadServiceConfigScope
	// MetadataReadStoreExtentReplicaStatsScope defines scope for an operation on metadata
	MetadataReadStoreExtentReplicaStatsScope
	// MetadataRegisterHostUUIDScope defines scope for an operation on metadata
	MetadataRegisterHostUUIDScope
	// MetadataSealExtentScope defines scope for an operation on metadata
	MetadataSealExtentScope
	// MetadataSetAckOffsetScope defines scope for an operation on metadata
	MetadataSetAckOffsetScope
	// MetadataSetOutputHostScope defines scope for an operation on metadata
	MetadataSetOutputHostScope
	// MetadataUpdateConsumerGroupScope defines scope for an operation on metadata
	MetadataUpdateConsumerGroupScope
	// MetadataUpdateConsumerGroupExtentStatusScope defines scope for an operation on metadata
	MetadataUpdateConsumerGroupExtentStatusScope
	// MetadataUpdateDestinationScope defines scope for an operation on metadata
	MetadataUpdateDestinationScope
	// MetadataUpdateDestinationDLQCursorsScope defines scope for an operation on metadata
	MetadataUpdateDestinationDLQCursorsScope
	// MetadataUpdateExtentReplicaStatsScope defines scope for an operation on metadata
	MetadataUpdateExtentReplicaStatsScope
	// MetadataUpdateExtentStatsScope defines scope for an operation on metadata
	MetadataUpdateExtentStatsScope
	// MetadataUpdateHostInfoScope defines scope for an operation on metadata
	MetadataUpdateHostInfoScope
	// MetadataUpdateStoreExtentReplicaStatsScope defines scope for an operation on metadata
	MetadataUpdateStoreExtentReplicaStatsScope

	// -- Operation scopes for ZoneFailoverManager (common) --
	ZoneFailoverMgrScope

	// -- Operation scopes for InputHost --

	// OpenPublisherStreamScope  represents  OpenPublisherStream API
	OpenPublisherStreamScope
	// DestinationsUpdatedScope represents DestinationsUpdated API
	DestinationsUpdatedScope
	// PubConnectionStreamScope  represents Streaming Message received by inputhost
	PubConnectionStreamScope
	// PutMessageBatchInputHostScope represents PutMessageBatch API
	PutMessageBatchInputHostScope
	//PubConnectionScope  represents Streaming Message received by inputhost
	PubConnectionScope
	//ReplicaConnectionScope represents inputhost's replica connection stream
	ReplicaConnectionScope
	//PutMessageBatchInputHostDestScope represent API PutMessageBatch for per destination
	PutMessageBatchInputHostDestScope
	// UnloadDestinationsScope represents UnloadDestinations API
	UnloadDestinationsScope
	// ListLoadedDestinationsScope represents ListLoadedDestinations API
	ListLoadedDestinationsScope
	// ReadDestStateScope represents ReadDestState API
	ReadDestStateScope
	// DrainExtentsScope represents DrainExtentsScope API
	DrainExtentsScope

	// -- Operation scopes for OutputHost --

	//OpenConsumerStreamScope  represents  OpenConsumerStream API
	OpenConsumerStreamScope
	//AckMessagesScope represents AckMessages API
	AckMessagesScope
	// ConsumerGroupsUpdatedScope represents ConsumerGroupsUpdated API
	ConsumerGroupsUpdatedScope
	// ConsConnectionStreamScope  represents  Streamming Message sent by outputhost
	ConsConnectionStreamScope
	// ReceiveMessageBatchOutputHostScope represents ReceiveMessageBatch API
	ReceiveMessageBatchOutputHostScope
	// UnloadConsumerGroupsScope represents UnloadConsumerGroups API
	UnloadConsumerGroupsScope
	// ConsConnectionScope  represents  Streamming Message sent by outputhost
	ConsConnectionScope
	// ReceiveMessageBatchOutputHostCGScope represents API ReceiveMessageBatch for per destination
	ReceiveMessageBatchOutputHostCGScope

	// -- Operation scopes for ControllerHost --

	// GetInputHostsScope represents GetInputHost API
	GetInputHostsScope
	// GetOutputHostsScope represents GetOutputHost API
	GetOutputHostsScope
	// ReportNodeMetricScope represents ReportNodeMetric API
	ReportNodeMetricScope
	// ReportDestinationMetricScope represents ReportDestinationMetric API
	ReportDestinationMetricScope
	// ReportDestinationExtentMetricScope represents ReportDestinationMetric API
	ReportDestinationExtentMetricScope
	// ReportConsumerGroupMetricScope represents ReportConsumerGroupMetric API
	ReportConsumerGroupMetricScope
	// ReportConsumerGroupExtentMetricScope represents ReportConsumerGroupExtentMetric API
	ReportConsumerGroupExtentMetricScope
	// ReportStoreExtentMetricScope represents ReportStoreExtentMetric API
	ReportStoreExtentMetricScope
	// RefreshInputHostsForDstScope represents the internal
	// API for handling GetInputHosts
	RefreshInputHostsForDstScope
	// RefreshOutputHostsForConsGroupScope represents the internal
	// API for handling GetOutputHosts
	RefreshOutputHostsForConsGroupScope
	// EventPipelineScope represents the EventPipeline operation
	EventPipelineScope
	// ExtentsUnreachableScope represents ExtentsUnreachable API
	ExtentsUnreachableScope
	// ExtentCreatedEventScope represents ExtentCreatedEvent
	ExtentCreatedEventScope
	// ConsGroupUpdatedEventScope represents event handler
	ConsGroupUpdatedEventScope
	// ExtentDownEventScope represents event handler
	ExtentDownEventScope
	// InputNotifyEventScope represents event handler
	InputNotifyEventScope
	// OutputNotifyEventScope represents event handler
	OutputNotifyEventScope
	// InputFailedEventScope represents event handler
	InputFailedEventScope
	// StoreFailedEventScope represents event handler
	StoreFailedEventScope
	// RemoteExtentPrimaryStoreDownEventScope represents event handler
	RemoteExtentPrimaryStoreDownEventScope
	// StoreExtentStatusOutOfSyncEventScope represents an event handler
	StoreExtentStatusOutOfSyncEventScope
	// StartReplicationForRemoteZoneExtentScope represents event handler
	StartReplicationForRemoteZoneExtentScope
	// ExtentMonitorScope represents the extent monitor daemon
	ExtentMonitorScope
	// RetentionMgrScope represents the retention manager
	RetentionMgrScope
	// DLQOperationScope represents the dlqMonitor
	DLQOperationScope
	// ControllerCreateDestinationScope represents controller CreateDestination API
	ControllerCreateDestinationScope
	// ControllerUpdateDestinationScope represents controller UpdateDestination API
	ControllerUpdateDestinationScope
	// ControllerDeleteDestinationScope represents controller DeleteDestination API
	ControllerDeleteDestinationScope
	// ControllerCreateConsumerGroupScope represents controller CreateConsumerGroup API
	ControllerCreateConsumerGroupScope
	// ControllerUpdateConsumerGroupScope represents controller UpdateConsumerGroup API
	ControllerUpdateConsumerGroupScope
	// ControllerDeleteConsumerGroupScope represents controller DeleteConsumerGroup API
	ControllerDeleteConsumerGroupScope
	// ControllerCreateRemoteZoneExtentScope represents controller CreateRemoteZoneExtent API
	ControllerCreateRemoteZoneExtentScope
	// ControllerCreateRemoteZoneCgExtentScope represents controller CreateRemoteZoneConsumerGroupExtent API
	ControllerCreateRemoteZoneCgExtentScope
	// QueueDepthBacklogCGScope represents metrics within queuedepth per consumer group
	QueueDepthBacklogCGScope

	// -- Operation scopes for FrontendHost --

	// CreateDestinationScope  represents CreateDestination API in frontend
	CreateDestinationScope
	// ReadDestinationScope represents ReadDestination API in frontend
	ReadDestinationScope
	// UpdateDestinationScope represents  UpdateDestination API in frontend
	UpdateDestinationScope
	// DeleteDestinationScope represents DeleteDestination API in frontend
	DeleteDestinationScope
	// ListDestinationsScope ListDestinations API in frontend
	ListDestinationsScope

	// CreateConsumerGroupScope represents CreateConsumerGroup API in frontend
	CreateConsumerGroupScope
	// ReadConsumerGroupScope represents ReadConsumerGroup API in frontend
	ReadConsumerGroupScope
	// UpdateConsumerGroupScope represents UpdateConsumerGroup API in frontend
	UpdateConsumerGroupScope
	// DeleteConsumerGroupScope represents DeleteConsumerGroup API in frontend
	DeleteConsumerGroupScope
	// ListConsumerGroupsScope represents ListConsumerGroups API in frontend
	ListConsumerGroupsScope

	// ReadDestinationHostsScope represents ReadDestinationHosts API in frontend
	ReadDestinationHostsScope
	// ReadConsumerGroupHostsScope represents ReadConsumerGroupHosts API in frontend
	ReadConsumerGroupHostsScope
	// PutMessageBatchScope represents PutMessageBatch API in frontend
	PutMessageBatchScope
	// ReceiveMessageBatchScope  represents ReseiveMessageBatch API in frontend
	ReceiveMessageBatchScope
	// CompleteMessageBatchScope represents CompleteMessageBatch API in frontend
	CompleteMessageBatchScope

	// PurgeDLQForConsumerGroupScope represents PurgeDLQForConsumerGroup API in frontend
	PurgeDLQForConsumerGroupScope
	// MergeDLQForConsumerGroupScope represents MergeDLQForConsumerGroup API in frontend
	MergeDLQForConsumerGroupScope
	// GetQueueDepthInfoScope
	GetQueueDepthInfoScope

	// -- Operation scopes for StoreHost --

	// OpenAppendStreamScope  represents  OpenAppendStream API
	OpenAppendStreamScope
	// OpenReadStreamScope  represents  OpenReadStream API
	OpenReadStreamScope
	// GetAddressFromTimestampScope  represents StGetAddressFromTimestamp API
	GetAddressFromTimestampScope
	// GetExtentInfoScope  represents GetExtentInfo API
	GetExtentInfoScope
	// SealExtentScope  represents SealExtent API
	SealExtentScope
	// PurgeMessagesScope represents PurgeMessagesScope API
	PurgeMessagesScope
	// InConnScope represents related metrics for inConn in storage
	InConnScope
	// OutConnScope represents related metrics for OutConn in storage
	OutConnScope
	// ExtentManagerScope represents related metrics for ExtentManager in storage
	ExtentManagerScope
	// SystemResourceScope represenets related metrics for system resource in storage
	SystemResourceScope
	// ReplicateExtentScope represents related metrics for ReplicateExtent API
	ReplicateExtentScope
	// ListExtentsScope represents metrics for ListExtents API
	ListExtentsScope

	// -- Operation scopes for Replicator --

	// OpenReplicationRemoteReadScope represents OpenReplicationRemoteRead API
	OpenReplicationRemoteReadScope
	// OpenReplicationReadScope represents OpenReplicationRead API
	OpenReplicationReadScope
	// OpenReplicationRemoteReadPerDestScope represents OpenReplicationRemoteRead API (per destination)
	OpenReplicationRemoteReadPerDestScope
	// OpenReplicationReadPerDestScope represents OpenReplicationRead API (per destination)
	OpenReplicationReadPerDestScope
	// ReplicatorCreateDestUUIDScope represents replicator CreateDestinationUUID API
	ReplicatorCreateDestUUIDScope
	// ReplicatorCreateRmtDestUUIDScope represents replicator CreateRemoteDestinationUUID API
	ReplicatorCreateRmtDestUUIDScope
	// ReplicatorUpdateDestScope represents replicator UpdateDestination API
	ReplicatorUpdateDestScope
	// ReplicatorUpdateRmtDestScope represents replicator UpdateRemoteDestination API
	ReplicatorUpdateRmtDestScope
	// ReplicatorDeleteDestScope represents replicator DeleteDestination API
	ReplicatorDeleteDestScope
	// ReplicatorDeleteRmtDestScope represents replicator DeleteRemoteDestination API
	ReplicatorDeleteRmtDestScope
	// ReplicatorCreateCgUUIDScope represents replicator CreateConsumerGroupUUID API
	ReplicatorCreateCgUUIDScope
	// ReplicatorCreateRmtCgUUIDScope represents replicator CreateRemoteConsumerGroupUUID API
	ReplicatorCreateRmtCgUUIDScope
	// ReplicatorUpdateCgScope represents replicator UpdateConsumerGroup API
	ReplicatorUpdateCgScope
	// ReplicatorUpdateRmtCgScope represents replicator UpdateRemoteConsumerGroup API
	ReplicatorUpdateRmtCgScope
	// ReplicatorDeleteCgScope represents replicator DeleteConsumerGroup API
	ReplicatorDeleteCgScope
	// ReplicatorDeleteRmtCgScope represents replicator DeleteRemoteConsumerGroup API
	ReplicatorDeleteRmtCgScope
	// ReplicatorCreateExtentScope represents replicator CreateExtent API
	ReplicatorCreateExtentScope
	// ReplicatorCreateRmtExtentScope represents replicator CreateRemoteExtent API
	ReplicatorCreateRmtExtentScope
	// ReplicatorCreateCgExtentScope represents replicator CreateConsumerGroupExtent API
	ReplicatorCreateCgExtentScope
	// ReplicatorCreateRmtCgExtentScope represents replicator CreateRemoteConsumerGroupExtent API
	ReplicatorCreateRmtCgExtentScope
	// ReplicatorSetAckOffsetScope represents replicator SetAckOffset API
	ReplicatorSetAckOffsetScope
	// ReplicatorSetAckOffsetInRemoteScope represents replicator SetAckOffsetInRemote API
	ReplicatorSetAckOffsetInRemoteScope
	// ReplicatorReadDestinationInRemoteZoneScope represents replicator ReadDestinationInRemoteZone API
	ReplicatorReadDestinationInRemoteZoneScope
	// ReplicatorReadCgInRemoteZoneScope represents replicator ReadConsumerGroupInRemoteZone API
	ReplicatorReadCgInRemoteZoneScope
	// ReplicatorHostUpdaterScope represents the host updater scope
	ReplicatorHostUpdaterScope
	// ReplicatorReconcileScope represents replicator's reconcile process
	ReplicatorReconcileScope
)

var scopeDefs = map[ServiceIdx]map[int]scopeDefinition{

	// Common operation tag values (shared by all services)
	Common: {
		// Metadata operation tag values as seen by the Metrics backend
		MetadataListEntityOpsScope:                     {operation: "MetadataListEntityOps"},
		MetadataHostAddrToUUIDScope:                    {operation: "MetadataHostAddrToUUID"},
		MetadataListAllConsumerGroupsScope:             {operation: "MetadataListAllConsumerGroups"},
		MetadataListConsumerGroupsScope:                {operation: "MetadataListConsumerGroups"},
		MetadataListConsumerGroupsUUIDScope:            {operation: "MetadataListConsumerGroupsUUID"},
		MetadataListDestinationsScope:                  {operation: "MetadataListDestinations"},
		MetadataListDestinationsByUUIDScope:            {operation: "MetadataListDestinationsByUUID"},
		MetadataListDestinationExtentsScope:            {operation: "MetadataListDestinationExtents"},
		MetadataListExtentsStatsScope:                  {operation: "MetadataListExtentsStats"},
		MetadataListHostsScope:                         {operation: "MetadataListHosts"},
		MetadataListInputHostExtentsStatsScope:         {operation: "MetadataListInputHostExtentsStats"},
		MetadataListStoreExtentsStatsScope:             {operation: "MetadataListStoreExtentsStats"},
		MetadataReadConsumerGroupScope:                 {operation: "MetadataReadConsumerGroup"},
		MetadataReadConsumerGroupByUUIDScope:           {operation: "MetadataReadConsumerGroupByUUID"},
		MetadataReadConsumerGroupExtentScope:           {operation: "MetadataReadConsumerGroupExtent"},
		MetadataReadConsumerGroupExtentsScope:          {operation: "MetadataReadConsumerGroupExtents"},
		MetadataReadConsumerGroupExtentsLiteScope:      {operation: "MetadataReadConsumerGroupExtentsLite"},
		MetadataReadConsumerGroupExtentsByExtUUIDScope: {operation: "MetadataReadConsumerGroupExtentsByExtUUID"},
		MetadataReadDestinationScope:                   {operation: "MetadataReadDestination"},
		MetadataReadExtentStatsScope:                   {operation: "MetadataReadExtentStats"},
		MetadataUUIDToHostAddrScope:                    {operation: "MetadataUUIDToHostAddr"},
		MetadataUpdateServiceConfigScope:               {operation: "MetadataUpdateServiceConfig"},
		MetadataCreateConsumerGroupScope:               {operation: "MetadataCreateConsumerGroup"},
		MetadataCreateConsumerGroupUUIDScope:           {operation: "MetadataCreateConsumerGroupUUID"},
		MetadataCreateConsumerGroupExtentScope:         {operation: "MetadataCreateConsumerGroupExtent"},
		MetadataCreateDestinationScope:                 {operation: "MetadataCreateDestination"},
		MetadataCreateDestinationUUIDScope:             {operation: "MetadataCreateDestinationUUID"},
		MetadataCreateExtentScope:                      {operation: "MetadataCreateExtent"},
		MetadataCreateHostInfoScope:                    {operation: "MetadataCreateHostInfo"},
		MetadataCreateServiceConfigScope:               {operation: "MetadataCreateServiceConfig"},
		MetadataDeleteConsumerGroupScope:               {operation: "MetadataDeleteConsumerGroup"},
		MetadataDeleteConsumerGroupUUIDScope:           {operation: "MetadataDeleteConsumerGroupUUID"},
		MetadataDeleteDestinationScope:                 {operation: "MetadataDeleteDestination"},
		MetadataDeleteDestinationUUIDScope:             {operation: "MetadataDeleteDestinationUUID"},
		MetadataDeleteHostInfoScope:                    {operation: "MetadataDeleteHostInfo"},
		MetadataDeleteServiceConfigScope:               {operation: "MetadataDeleteServiceConfig"},
		MetadataMoveExtentScope:                        {operation: "MetadataMoveExtent"},
		MetadataReadHostInfoScope:                      {operation: "MetadataReadHostInfo"},
		MetadataReadServiceConfigScope:                 {operation: "MetadataReadServiceConfig"},
		MetadataReadStoreExtentReplicaStatsScope:       {operation: "MetadataReadStoreExtentReplicaStats"},
		MetadataRegisterHostUUIDScope:                  {operation: "MetadataRegisterHostUUID"},
		MetadataSealExtentScope:                        {operation: "MetadataSealExtent"},
		MetadataSetAckOffsetScope:                      {operation: "MetadataSetAckOffset"},
		MetadataSetOutputHostScope:                     {operation: "MetadataSetOutputHost"},
		MetadataUpdateConsumerGroupScope:               {operation: "MetadataUpdateConsumerGroup"},
		MetadataUpdateConsumerGroupExtentStatusScope:   {operation: "MetadataUpdateConsumerGroupExtentStatus"},
		MetadataUpdateDestinationScope:                 {operation: "MetadataUpdateDestination"},
		MetadataUpdateDestinationDLQCursorsScope:       {operation: "MetadataUpdateDestinationDLQCursors"},
		MetadataUpdateExtentReplicaStatsScope:          {operation: "MetadataUpdateExtentReplicaStats"},
		MetadataUpdateExtentStatsScope:                 {operation: "MetadataUpdateExtentStats"},
		MetadataUpdateHostInfoScope:                    {operation: "MetadataUpdateHostInfo"},
		MetadataUpdateStoreExtentReplicaStatsScope:     {operation: "MetadataUpdateStoreExtentReplicaStats"},

		ZoneFailoverMgrScope: {operation: "ZoneFailoverMgr"},
	},

	// Frontend operation tag values as seen by the Metrics backend
	Frontend: {
		CreateDestinationScope:        {operation: "CreateDestination"},
		ReadDestinationScope:          {operation: "ReadDestination"},
		UpdateDestinationScope:        {operation: "UpdateDestination"},
		DeleteDestinationScope:        {operation: "DeleteDestination"},
		ListDestinationsScope:         {operation: "ListDestinations"},
		CreateConsumerGroupScope:      {operation: "CreateConsumerGroup"},
		ReadConsumerGroupScope:        {operation: "ReadConsmerGroup"},
		UpdateConsumerGroupScope:      {operation: "UpdateConsumerGroup"},
		DeleteConsumerGroupScope:      {operation: "DeleteConsumerGroup"},
		ListConsumerGroupsScope:       {operation: "ListConsumerGroups"},
		ReadDestinationHostsScope:     {operation: "ReadDestinationHosts"},
		ReadConsumerGroupHostsScope:   {operation: "ReadConsumerGroupHosts"},
		PutMessageBatchScope:          {operation: "PutMessageBatch"},
		ReceiveMessageBatchScope:      {operation: "ReceiveMessageBatch"},
		CompleteMessageBatchScope:     {operation: "CompleteMessageBatch"},
		PurgeDLQForConsumerGroupScope: {operation: "PurgeDLQForConsumerGroup"},
		MergeDLQForConsumerGroupScope: {operation: "MergeDLQForConsumerGroup"},
		GetQueueDepthInfoScope:        {operation: "GetQueueDepthInfo"},
	},

	// Inputhost operation tag values as seen by the Metrics backend
	Inputhost: {
		OpenPublisherStreamScope:      {operation: "OpenPublisherStream"},
		DestinationsUpdatedScope:      {operation: "DestinationsUpdated"},
		PubConnectionStreamScope:      {operation: "PubConnection"},
		PutMessageBatchInputHostScope: {operation: "PutMessageBatchInputHost"},
		UnloadDestinationsScope:       {operation: "UnloadDestinations"},
		ListLoadedDestinationsScope:   {operation: "ListLoadedDestinations"},
		ReadDestStateScope:            {operation: "ReadDestState"},
		DrainExtentsScope:             {operation: "DrainExtentsScope"},
	},

	// Outputhost operation tag values as seen by the Metrics backend
	Outputhost: {
		OpenConsumerStreamScope:            {operation: "OpenConsumerStream"},
		AckMessagesScope:                   {operation: "AckMessage"},
		ConsumerGroupsUpdatedScope:         {operation: "ConsumerGroupsUpdated"},
		ConsConnectionStreamScope:          {operation: "ConsConnection"},
		ReceiveMessageBatchOutputHostScope: {operation: "ReceiveMessageBatchOutputHost"},
		UnloadConsumerGroupsScope:          {operation: "UnloadConsumerGroups"},
	},

	// Storage operation tag values as seen by the Metrics backend
	Storage: {
		OpenAppendStreamScope:        {operation: "OpenAppendStream"},
		OpenReadStreamScope:          {operation: "OpenReadStream"},
		GetAddressFromTimestampScope: {operation: "GetAddressFromTimestamp"},
		GetExtentInfoScope:           {operation: "GetExtentInfo"},
		SealExtentScope:              {operation: "SealExtent"},
		PurgeMessagesScope:           {operation: "PurgeMessages"},
		InConnScope:                  {operation: "InConn"},
		OutConnScope:                 {operation: "OutConn"},
		ExtentManagerScope:           {operation: "ExtentInfo"},
		SystemResourceScope:          {operation: "GetSystemResourceInfo"},
		ReplicateExtentScope:         {operation: "ReplicateExtent"},
		ListExtentsScope:             {operation: "ListExtents"},
	},

	// Replicator operation tag values as seen by the Metrics backend
	Replicator: {
		OpenReplicationRemoteReadScope:             {operation: "OpenReplicationRemoteReadStream"},
		OpenReplicationReadScope:                   {operation: "OpenReplicationReadStream"},
		ReplicatorCreateDestUUIDScope:              {operation: "ReplicatorCreateDestinationUUID"},
		ReplicatorCreateRmtDestUUIDScope:           {operation: "ReplicatorCreateRemoteDestinationUUID"},
		ReplicatorUpdateDestScope:                  {operation: "ReplicatorUpdateDestination"},
		ReplicatorUpdateRmtDestScope:               {operation: "ReplicatorUpdateRemoteDestination"},
		ReplicatorDeleteDestScope:                  {operation: "ReplicatorDeleteDestination"},
		ReplicatorDeleteRmtDestScope:               {operation: "ReplicatorDeleteRemoteDestination"},
		ReplicatorCreateCgUUIDScope:                {operation: "ReplicatorCreateConsumerGroupUUID"},
		ReplicatorCreateRmtCgUUIDScope:             {operation: "ReplicatorCreateRemoteConsumerGroupUUID"},
		ReplicatorUpdateCgScope:                    {operation: "ReplicatorUpdateConsumerGroup"},
		ReplicatorUpdateRmtCgScope:                 {operation: "ReplicatorUpdateRemoteConsumerGroup"},
		ReplicatorDeleteCgScope:                    {operation: "ReplicatorDeleteConsumerGroup"},
		ReplicatorDeleteRmtCgScope:                 {operation: "ReplicatorDeleteRemoteConsumerGroup"},
		ReplicatorCreateExtentScope:                {operation: "ReplicatorCreateExtent"},
		ReplicatorCreateRmtExtentScope:             {operation: "ReplicatorCreateRemoteExtent"},
		ReplicatorCreateCgExtentScope:              {operation: "ReplicatorCreateConsumerGroupExtent"},
		ReplicatorCreateRmtCgExtentScope:           {operation: "ReplicatorCreateRemoteConsumerGroupExtent"},
		ReplicatorSetAckOffsetScope:                {operation: "SetAckOffset"},
		ReplicatorSetAckOffsetInRemoteScope:        {operation: "SetAckOffsetInRemote"},
		ReplicatorReadDestinationInRemoteZoneScope: {operation: "ReadDestinationInRemoteZone"},
		ReplicatorReadCgInRemoteZoneScope:          {operation: "ReadConsumerGroupInRemoteZone"},
		ReplicatorHostUpdaterScope:                 {operation: "ReplicatorHostUpdater"},
		ReplicatorReconcileScope:                   {operation: "ReplicatorReconcile"},
	},

	// Controller operation tag values as seen by the Metrics backend
	Controller: {
		GetInputHostsScope:                       {operation: "GetInputHosts"},
		GetOutputHostsScope:                      {operation: "GetOutputHosts"},
		ReportNodeMetricScope:                    {operation: "ReportNodeMetric"},
		ReportDestinationMetricScope:             {operation: "ReportDestinationMetric"},
		ReportDestinationExtentMetricScope:       {operation: "ReportDestinatoinExtentMetric"},
		ReportConsumerGroupMetricScope:           {operation: "ReportConsumerGroupMetric"},
		ReportConsumerGroupExtentMetricScope:     {operation: "ReportConsumerGroupExtentMetric"},
		ReportStoreExtentMetricScope:             {operation: "ReportStoreExtentMetric"},
		RefreshInputHostsForDstScope:             {operation: "RefreshInputHostsForDst"},
		RefreshOutputHostsForConsGroupScope:      {operation: "RefreshOutputHostsForConsGroup"},
		EventPipelineScope:                       {operation: "EventPipeline"},
		ExtentsUnreachableScope:                  {operation: "ExtentsUnreachable"},
		ExtentCreatedEventScope:                  {operation: "ExtentCreatedEvent"},
		ConsGroupUpdatedEventScope:               {operation: "ConsGroupUpdatedEvent"},
		ExtentDownEventScope:                     {operation: "ExtentDownEvent"},
		InputNotifyEventScope:                    {operation: "InputNotifyEvent"},
		OutputNotifyEventScope:                   {operation: "OutputNotifyEvent"},
		InputFailedEventScope:                    {operation: "InputFailedEvent"},
		StoreFailedEventScope:                    {operation: "StoreFailedEvent"},
		RemoteExtentPrimaryStoreDownEventScope:   {operation: "RemoteExtentPrimaryStoreDownEvent"},
		StoreExtentStatusOutOfSyncEventScope:     {operation: "StoreExtentStatusOutOfSyncEvent"},
		StartReplicationForRemoteZoneExtentScope: {operation: "StartReplicationForRemoteZoneExtent"},
		QueueDepthBacklogCGScope:                 {operation: "QueueDepthBacklog"},
		ExtentMonitorScope:                       {operation: "ExtentMonitor"},
		RetentionMgrScope:                        {operation: "RetentionMgr"},
		DLQOperationScope:                        {operation: "DLQOperation"},
		ControllerCreateDestinationScope:         {operation: "CreateDestination"},
		ControllerUpdateDestinationScope:         {operation: "UpdateDestination"},
		ControllerDeleteDestinationScope:         {operation: "DeleteDestination"},
		ControllerCreateConsumerGroupScope:       {operation: "CreateConsumerGroup"},
		ControllerUpdateConsumerGroupScope:       {operation: "UpdateConsumerGroup"},
		ControllerDeleteConsumerGroupScope:       {operation: "DeleteConsumerGroup"},
		ControllerCreateRemoteZoneExtentScope:    {operation: "CreateRemoteZoneExtent"},
		ControllerCreateRemoteZoneCgExtentScope:  {operation: "CreateRemoteZoneConsumerGroupExtent"},
	},
}

var dynamicScopeDefs = map[ServiceIdx]map[int]scopeDefinition{

	// Inputhost Scope Names
	Inputhost: {
		PubConnectionScope:                {operation: "PubConnection"},
		PutMessageBatchInputHostDestScope: {operation: "PutMessageBatchInputHost"},
		ReplicaConnectionScope:            {operation: "ReplicaConnection"},
	},

	// Outputhost Scope Names
	Outputhost: {
		ConsConnectionScope:                  {operation: "ConsConnection"},
		ReceiveMessageBatchOutputHostCGScope: {operation: "ReceiveMessageBatchInputHost"},
	},

	// Controller Scope Names
	Controller: {
		QueueDepthBacklogCGScope: {operation: "QueueDepthBacklog"},
	},

	Replicator: {
		OpenReplicationRemoteReadPerDestScope: {operation: "OpenReplicationRemoteReadStreamPerDest"},
		OpenReplicationReadPerDestScope:       {operation: "OpenReplicationReadStreamPerDest"},
	},
}

// Metric enum
const (

	// -- Common metrics -- //

	// MetadataRequests is the count of requests to metadata
	MetadataRequests = iota
	// MetadataFailures is the count of requests to metadata that have failed
	MetadataFailures
	// MetadataLatency is the latency of requests to metadata
	MetadataLatency

	// ZoneFailoverMgrRunSuccess indicates a success run
	ZoneFailoverMgrRunSuccess
	// ZoneFailoverMgrRunFailToDetectActiveZone indicates failure to detect active zone
	ZoneFailoverMgrRunFailToDetectActiveZone
	// ZoneFailoverMgrRunFailToWriteMetadata indicates failure to write metadata
	ZoneFailoverMgrRunFailToWriteMetadata
	// ZoneFailoverMgrRunFailToWriteMetadata indicates a failed run
	ZoneFailoverMgrRunFailure

	// --Inputhost metrics -- //

	// InputhostRequests indicates the request count for inputhost
	InputhostRequests
	// InputhostFailures indicates failure count for inputhost
	InputhostFailures
	// InputhostMessageReceived records the count of messages received
	InputhostMessageReceived
	// InputhostMessageFailures records the count of messages received failures
	InputhostMessageFailures
	//InputhostReconfClientRequests indicates the request count for reconfige clients
	InputhostReconfClientRequests
	//InputhostMessageLimitThrottled indicates the request has been throttled due to our rate limit
	InputhostMessageLimitThrottled
	//InputhostMessageChannelFullThrottled indicates the request has been throttled due to the channel being full
	InputhostMessageChannelFullThrottled
	// InputhostUserFailures indicates this is a user failure (~HTTP 4xx)
	InputhostUserFailures
	// InputhostInternalFailures indicates this is an internal failure (HTTP 5xx)
	InputhostInternalFailures
	// InputhostMessageUserFailures records the count of messages received failures
	InputhostMessageUserFailures
	// InputhostMessageInternalFailures records the count of messages received failures
	InputhostMessageInternalFailures
	// InputhostGauges is the gauges for inputhost
	InputhostGauges
	// InputhostPubConnection is the gauges for active pubconnection
	InputhostPubConnection
	// InputhostLatencyTimer represents time taken by an operation
	InputhostLatencyTimer
	// InputhostWriteMessageLatency is the latency from receiving a message from stream to returning ack back to stream
	InputhostWriteMessageLatency
	// InputhostWriteMessageBeforeAckLatency is the latency from receiving a message from stream to getting ack from replicas
	// the only difference with InputhostWriteMessageLatency is this metrics excludes the latency for writing ack back to the stream
	InputhostWriteMessageBeforeAckLatency
	// InputhostDestMessageReceived  indicates prefix name for destinations request counter
	// append the destination path will be the actual name for the counter.
	// each destination has a unique name tag
	InputhostDestMessageReceived
	// InputhostDestMessageFailures indicates prefix nmae of destinations failure counter
	// append the destination path will be the actual name for the counter.
	// each destination has a unique name tag
	InputhostDestMessageFailures
	// InputhostDestMessageLimitThrottled is used to indicate that this particular destination
	// is throttled due to token bucket rate limit
	InputhostDestMessageLimitThrottled
	// InputhostDestMessageChannelFullThrottled is used to indicate that this particular destination
	// is throttled due to the channel being full
	InputhostDestMessageChannelFullThrottled
	// InputhostDestMessageUserFailures indicates prefix nmae of destinations failure counter
	// append the destination path will be the actual name for the counter.
	// each destination has a unique name tag
	InputhostDestMessageUserFailures
	// InputhostDestMessageInternalFailures indicates prefix nmae of destinations failure counter
	// append the destination path will be the actual name for the counter.
	// each destination has a unique name tag
	InputhostDestMessageInternalFailures
	// InputhostDestWriteMessageLatency is the latency from receiving a message from stream to returning ack back to stream
	InputhostDestWriteMessageLatency
	// InputhostDestWriteMessageBeforeAckLatency is the latency from receiving a message from stream to getting ack from replicas
	// the only difference with InputhostDestWriteMessageLatency is this metrics excludes the latency for returning ack back to the stream
	InputhostDestWriteMessageBeforeAckLatency
	// InputhostDestPubConnection is the gauge of active connections per destination
	InputhostDestPubConnection
	// InputhostMessageReceivedBytes tracks the total incoming messages in bytes
	InputhostDestMessageReceivedBytes
	// InputhostMessageSentBytes tracks the total outgoing messages (to replica) in bytes
	InputhostDestMessageSentBytes

	// -- Outputhost metrics -- //

	// OutputhostRequests indicates the request count for outputhost
	OutputhostRequests
	// OutputhostFailures indicates failure count for outputhost
	OutputhostFailures
	// OutputhostLongPollingTimeOut indicates time out for long polling
	OutputhostLongPollingTimeOut
	// OutputhostReceiveMsgBatchWriteToMsgCacheTimeout indicates time out for ReceiveMsgBatch to write to msg cache
	OutputhostReceiveMsgBatchWriteToMsgCacheTimeout
	// OutputhostMessageSent records the count of messages sent
	OutputhostMessageSent
	// OutputhostMessageFailures records the count of messages sent failures
	OutputhostMessageFailures
	// OutputhostCreditsReceived indicates the count of the credits
	OutputhostCreditsReceived
	// OutputhostDLQMessageRequests records the count of DLQ messages requests
	OutputhostDLQMessageRequests
	// OutputhostDLQMessageFailures records the count of DLQ messages sent failures
	OutputhostDLQMessageFailures
	// OutputhostMessageRedelivered records the count of messages redeliverd
	OutputhostMessageRedelivered
	// OutputhostMessageSentAck records the count of ack messages
	OutputhostMessageSentAck
	// OutputhostMessageSentNAck records the count of nack messages
	OutputhostMessageSentNAck
	//OutputhostMessageAckFailures recores the count of messages ack failures
	OutputhostMessageAckFailures
	//OutputhostMessageNackFailures recores the count of messages nack failures
	OutputhostMessageNackFailures
	// OutputhostMessageNoAckManager records the count of acks which came where there was no Ack Manager
	OutputhostMessageNoAckManager
	// OutputhostMessageDiffSession records the count of acks which came for a different session
	OutputhostMessageDiffSession
	// OutputhostMessageAckManagerError records the count of errors from the ack manager
	OutputhostMessageAckManagerError
	// OutputhostUserFailures indicates this is a user failure (~HTTP 4xx)
	OutputhostUserFailures
	// OutputhostInternalFailures indicates this is an internal failure (HTTP 5xx)
	OutputhostInternalFailures
	// OutputhostConsConnection is the number of active connections
	OutputhostConsConnection
	// OutputhostCreditsAccumulated is a gauge to record credits that are accumulated locally
	OutputhostCreditsAccumulated
	// OutputhostLatencyTimer represents time taken by an operation
	OutputhostLatencyTimer
	// OutputhostCGMessageSent records the count of messages sent per consumer group
	OutputhostCGMessageSent
	// OutputhostCGMessageSentBytes records the total size of messages sent per consumer-group
	OutputhostCGMessageSentBytes
	// OutputhostCGMessageReceivedBytes records the total size of message received from replica per CG
	OutputhostCGMessageReceivedBytes
	// OutputhostCGMessageFailures records the count of messages sent failures per consumer group
	OutputhostCGMessageFailures
	// OutputhostCGCreditsReceived indicates the count of the credits per consumer group
	OutputhostCGCreditsReceived
	// OutputhostCGDLQMessageRequests records the count of DLQ messages requests per consumer group
	OutputhostCGDLQMessageRequests
	// OutputhostCGDLQMessageFailures records the count of DLQ messages sent failures per consumer group
	OutputhostCGDLQMessageFailures
	// OutputhostCGMessageRedelivered records the count of messages redelivered
	OutputhostCGMessageRedelivered
	// OutputhostCGMessageSentAck records the count of ack messages
	OutputhostCGMessageSentAck
	// OutputhostCGMessageSentNAck records the count of nack messages
	OutputhostCGMessageSentNAck
	// OutputhostCGMessagesThrottled records the count of messages throttled
	OutputhostCGMessagesThrottled
	// OutputhostCGAckMgrSeqNotFound is the gauge to track acks whose seq number is not found
	OutputhostCGAckMgrSeqNotFound
	// OutputhostCGReplicaReconnect is a counter that tracks replica reconnections
	OutputhostCGReplicaReconnect
	// OutputhostCGMessageSentLatency is the latency to send a message
	OutputhostCGMessageSentLatency
	//OutputhostCGMessageCacheSize is the cashe size of consumer group message
	OutputhostCGMessageCacheSize
	// OutputhostCGConsConnection is the number of active connections per consumer group
	OutputhostCGConsConnection
	// OutputhostCGOutstandingDeliveries is the number of outstanding messages for a single consumer group
	OutputhostCGOutstandingDeliveries
	// OutputhostCGHealthState is the most recent health state for the consumer group
	OutputhostCGHealthState
	// OutputhostCGNumExtents is the number of active extents for this CG
	OutputhostCGNumExtents
	// OutputhostCGAckMgrSize is a gauge for the size of the ack manager
	OutputhostCGAckMgrSize
	// OutputhostCGAckMgrLevelUpdate is a gauge to track the last time ack level moved
	OutputhostCGAckMgrLevelUpdate
	// OutputhostCGAckMgrConsumed is a gauge for the extents marked as consumed
	OutputhostCGAckMgrConsumed
	// OutputhostCGAckMgrResetMsg is the gauge to track the resets to the ackMgrMap
	OutputhostCGAckMgrResetMsg
	// OutputhostCGAckMgrResetMsgError is the gauge to track errors on reset to the ackMgrMap
	OutputhostCGAckMgrResetMsgError
	// OutputhostCGSkippedMessages is the gauge to track skipped messages
	OutputhostCGSkippedMessages
	// OutputhostCGCreditsAccumulated is a gauge to record credits that are accumulated locally per consumer group
	OutputhostCGCreditsAccumulated
	// OutputhostCGKafkaIncomingBytes corresponds to Kafka's (sarama's) incoming-byte-rate metric
	OutputhostCGKafkaIncomingBytes
	// OutputhostCGKafkaOutgoingBytes corresponds to Kafka's (sarama's) outgoing-byte-rate metric
	OutputhostCGKafkaOutgoingBytes
	// OutputhostCGKafkaRequestSent corresponds to Kafka's (sarama's) request-rate metric
	OutputhostCGKafkaRequestSent
	// OutputhostCGKafkaRequestSize corresponds to Kafka's (sarama's) request-size metric
	OutputhostCGKafkaRequestSize
	// OutputhostCGKafkaRequestLatency corresponds to Kafka's (sarama's) request-latency metric
	OutputhostCGKafkaRequestLatency
	// OutputhostCGKafkaResponseReceived corresponds to Kafka's (sarama's) response-rate metric
	OutputhostCGKafkaResponseReceived
	// OutputhostCGKafkaResponseSize corresponds to Kafka's (sarama's) response-size metric
	OutputhostCGKafkaResponseSize

	// -- Frontend metrics -- //

	// FrontendRequests indicates the request count for frontend
	FrontendRequests
	// FrontendFailures indicates failure count for frontend
	FrontendFailures
	// FrontendEntityNotExist indicates entityNotExist Error for frontend
	FrontendEntityNotExist
	// FrontendUserFailures indicates this is a user failure (~HTTP 4xx)
	FrontendUserFailures
	// FrontendInternalFailures indicates this is an internal failure (HTTP 5xx)
	FrontendInternalFailures
	// FrontendLatencyTimer represents time taken by an operation
	FrontendLatencyTimer

	// -- Storehost metrics -- //

	// StorageRequests is the count of requests
	StorageRequests
	// StorageFailures is the count of requests that have failed
	StorageFailures
	// StorageStoreFailures is the count of requests that have failed due to storage errors
	StorageStoreFailures
	// StorageMessageReceived records the count of messages received
	StorageMessageReceived
	// StorageMessageSent records the count of messages sent
	StorageMessageSent
	// WatermarksReceived records the count of fully replicated watermarks received
	WatermarksReceived
	// StorageOpenExtents is the number of active extents
	StorageOpenExtents
	// StorageWriteStreams is the number of active write streams
	StorageWriteStreams
	// StorageReadStreams is the number of active read streams
	StorageReadStreams
	// StorageInMsgChanDepth is the depth of the msg-chan buffer during writes
	StorageInMsgChanDepth
	// StorageInAckChanDepth is the depth of the ack-chan buffer during writes
	StorageInAckChanDepth
	// StorageOutMsgChanDepth is the depth of the msg-chan buffer during reads
	StorageOutMsgChanDepth
	// StorageDiskAvailableSpaceMB is the available disk space in MB
	StorageDiskAvailableSpaceMB
	// StorageDiskAvailableSpacePcnt is the available disk space percentage
	StorageDiskAvailableSpacePcnt

	// StorageLatencyTimer is the latency for every (non-streaming) request
	StorageLatencyTimer
	// StorageWriteStoreLatency is the latency to write message to store
	StorageWriteStoreLatency
	// StorageWriteMessageLatency is the latency from receiving a message from stream(input) to returning ack back to stream
	StorageWriteMessageLatency
	// StorageWriteMessageBeforeAckLatency is the latency from receiving a message from stream(input) to getting ack from store
	// the only difference with StorageWriteMessageLatency is this metrics excludes the latency for returning ack back to the stream
	StorageWriteMessageBeforeAckLatency
	// StorageReadStoreLatency is the latency to read message from store
	StorageReadStoreLatency
	// StorageReadMessageLatency is the latency to read and send out a message
	StorageReadMessageLatency
	// StorageInWriteTChannelLatency is the latency to write ack to tchannel in stream
	StorageInWriteTChannelLatency
	// StorageInFlushTChannelLatency is the latency to flush ack to tchannel in stream
	StorageInFlushTChannelLatency
	// StorageOutWriteTChannelLatency is the latench to write msg to tchannel out stream
	StorageOutWriteTChannelLatency
	// StorageOutFlushTChannelLatency is the latency to flush msg to tchannel out stream
	StorageOutFlushTChannelLatency
	// StorageReplicationJobMaxConsecutiveFailures is the max number of consecutive failures for any replication job
	StorageReplicationJobMaxConsecutiveFailures
	// StorageReplicationJobCurrentFailures is the number of failed job in current run
	StorageReplicationJobCurrentFailures
	// StorageReplicationJobCurrentSuccess is the number of success job in current run
	StorageReplicationJobCurrentSuccess
	// StorageReplicationJobRun indicates the replication job runs
	StorageReplicationJobRun

	// -- Controller metrics -- //

	// ControllerErrMetadataReadCounter indicates an error from metadata
	ControllerErrMetadataReadCounter
	// ControllerErrMetadataUpdateCounter indicates failure to update metadata
	ControllerErrMetadataUpdateCounter
	// ControllerErrMetadataEntityNotFoundCounter indicates non-existent entity
	ControllerErrMetadataEntityNotFoundCounter
	// ControllerErrTryLockCounter indicates failure to acquire try lock
	ControllerErrTryLockCounter
	// ControllerErrCreateExtentCounter indicates failure to create extent
	ControllerErrCreateExtentCounter
	// ControllerErrPickInHostCounter indicates failure to pick input host for extent
	ControllerErrPickInHostCounter
	// ControllerErrPickOutHostCounter indicates failure to pick output host for extent
	ControllerErrPickOutHostCounter
	// ControllerErrCallReplicatorCounter indicates failure to call replicator
	ControllerErrCallReplicatorCounter

	// ControllerErrPickStoreHostCounter indicates failure to pick store host for extent
	ControllerErrPickStoreHostCounter
	// ControllerErrResolveUUIDCounter indicates failure to resolve UUID to ip:port
	ControllerErrResolveUUIDCounter
	// ControllerErrCreateTChanClientCounter indicates failure to create tchannel client
	ControllerErrCreateTChanClientCounter
	// ControllerErrNoHealthyStoreCounter indicates unavailability of store hosts
	ControllerErrNoHealthyStoreCounter
	// ControllerErrSealFailed indicates failure of seal operation
	ControllerErrSealFailed
	// ControllerErrTooManyOpenCGExtents indicates too many open extents for some CG
	ControllerErrTooManyOpenCGExtents
	// ControllerErrNoRetryWorkers indicates that an event cannot be retried
	// because all of the retry workers are busy
	ControllerErrNoRetryWorkers
	// ControllerErrBadRequestCounter indicates a malformed request
	ControllerErrBadRequestCounter
	// ControllerErrBadEntityCounter indicates either an entity not exists or disabled error
	ControllerErrBadEntityCounter
	// ControllerErrDrainFailed indicates that a drain command issued to input failed
	ControllerErrDrainFailed

	// ControllerEventsDropped indicates an event drop due to queue full
	ControllerEventsDropped
	// ControllerRequests indicates the request count
	ControllerRequests
	// ControllerFailures indicates failure count
	ControllerFailures
	// ControllerRetries represents retry attempts on an operation
	ControllerRetries

	// ControllerRetriesExceeded indicates max retries being exceeded on an operation
	ControllerRetriesExceeded
	//ControllerRateLimited indicates count of the times that throttle kicked in
	ControllerRateLimited

	// ControllerRetentionJobStartCounter indicates the started retention job count
	ControllerRetentionJobStartCounter
	// ControllerRetentionJobFailedCounter indicates the failed retention job count
	ControllerRetentionJobFailedCounter
	// ControllerRetentionJobCompletedCounter indicates the completed retention job count
	ControllerRetentionJobCompletedCounter
	// ControllerRetentionJobCancelledCounter indicates the cancelled retention job count
	ControllerRetentionJobCancelledCounter
	// ControllerGetAddressFailedCounter indicates the failed getAddress calls
	ControllerGetAddressFailedCounter
	// ControllerGetAddressCompletedCounter indicates the completed getAddress calls
	ControllerGetAddressCompletedCounter
	// ControllerPurgeMessagesRequestCounter indicates the purge messages request count
	ControllerPurgeMessagesRequestCounter

	// ControllerLatencyTimer represents time taken by an operation
	ControllerLatencyTimer
	// ControllerRetentionJobDuration is the time spent to finish retention on an extent
	ControllerRetentionJobDuration
	// ControllerGetAddressLatency is the latency of getAddressFromTS
	ControllerGetAddressLatency
	// ControllerPurgeMessagesLatency is the letency of purge messages request
	ControllerPurgeMessagesLatency

	// ControllerCGBacklogAvailable is the numbers for availbale back log
	ControllerCGBacklogAvailable
	// ControllerCGBacklogUnavailable is the numbers for unavailbale back log
	ControllerCGBacklogUnavailable
	// ControllerCGBacklogInflight is the numbers for inflight back log
	ControllerCGBacklogInflight
	// ControllerCGBacklogDLQ is the numbers for DLQ back log
	ControllerCGBacklogDLQ
	// ControllerCGBacklogProgress is an indication of progress made on the backlog
	ControllerCGBacklogProgress
	// ControllerNumOpenExtents represents the count of open extents
	ControllerNumOpenExtents
	// ControllerNumSealedExtents represents the count of sealed extents
	ControllerNumSealedExtents
	// ControllerNumConsumedExtents represents the count of consumed extents
	ControllerNumConsumedExtents
	// ControllerNumOpenDLQExtents represents the count of open dlq extents
	ControllerNumOpenDLQExtents
	// ControllerNumSealedDLQExtents represents the count of sealed dlq extents
	ControllerNumSealedDLQExtents
	// ControllerNumConsumedDLQExtents represents the count of consumed dlq extents
	ControllerNumConsumedDLQExtents
	// ControllerNumOpenCGExtents represents the count of open cg extents
	ControllerNumOpenCGExtents
	// ControllerNumConsumedCGExtents represents the count of consumed cg extents
	ControllerNumConsumedCGExtents
	// ControllerNoActiveZone indicates there's no active zone from dynamic config
	ControllerNoActiveZone

	// -- Replicator metrics -- //

	// ReplicatorCreateInStreamFailure indicates failure when creating in stream
	ReplicatorCreateInStreamFailure
	// ReplicatorCreateOutStreamFailure indicates failure when creating out stream
	ReplicatorCreateOutStreamFailure
	// ReplicatorRequests indicates non-messaging request count for replicator
	ReplicatorRequests
	// ReplicatorFailures indicates non-messaging failure count for replicator
	ReplicatorFailures
	// ReplicatorBadRequest indicates bad request
	ReplicatorBadRequest

	// ReplicatorInConnCreditsReceived indicates how many credits InConn received
	ReplicatorInConnCreditsReceived
	// ReplicatorInConnMsgWritten indicates how many messages InConn writes to client
	ReplicatorInConnMsgWritten
	// ReplicatorInConnPerDestMsgWritten indicates how many messages InConn writes to client per destination
	ReplicatorInConnPerDestMsgWritten
	// ReplicatorInConnPerDestMsgLatency indicates the per destination replication latency
	ReplicatorInConnPerDestMsgLatency
	// ReplicatorOutConnCreditsSent indicates how many credits OutConn sent
	ReplicatorOutConnCreditsSent
	// ReplicatorOutConnMsgRead indicates how many messages OutConn read
	ReplicatorOutConnMsgRead

	// ReplicatorReconcileDestRun indicates the reconcile fails
	ReplicatorReconcileFail
	// ReplicatorReconcileDestRun indicates the reconcile for dest runs
	ReplicatorReconcileDestRun
	// ReplicatorReconcileDestFail indicates the reconcile for dest fails
	ReplicatorReconcileDestFail
	// ReplicatorReconcileDestFoundMissing indicates the reconcile for dest found a missing dest
	ReplicatorReconcileDestFoundMissing
	// ReplicatorReconcileCgRun indicates the reconcile for cg runs
	ReplicatorReconcileCgRun
	// ReplicatorReconcileCgFail indicates the reconcile for cg fails
	ReplicatorReconcileCgFail
	// ReplicatorReconcileCgFoundMissing indicates the reconcile for cg found a missing cg
	ReplicatorReconcileCgFoundMissing
	// ReplicatorReconcileCgFoundUpdated indicates the reconcile for cg found a updated cg
	ReplicatorReconcileCgFoundUpdated
	// ReplicatorReconcileDestExtentRun indicates the reconcile for dest extent runs
	ReplicatorReconcileDestExtentRun
	// ReplicatorReconcileDestExtentFail indicates the reconcile for dest extent fails
	ReplicatorReconcileDestExtentFail
	// ReplicatorReconcileDestExtentFoundMissing indicates the reconcile for dest extent found a missing dest extent
	ReplicatorReconcileDestExtentFoundMissing
	// ReplicatorReconcileDestExtentRemoteConsumedLocalMissing indicates the reconcile for dest extent found a dest extent that is consumed on remote side and local is missing
	ReplicatorReconcileDestExtentRemoteConsumedLocalMissing
	// ReplicatorReconcileDestExtentRemoteDeleted indicates the reconcile for dest extent found a dest extent that is deleted on remote side and local is missing
	ReplicatorReconcileDestExtentRemoteDeletedLocalMissing
	// ReplicatorReconcileDestExtentRemoteDeletedLocalNot indicates the reconcile for dest extent found an inconsistent extent status(remote is deleted, local is not)
	ReplicatorReconcileDestExtentRemoteDeletedLocalNot
	// ReplicatorReconcileDestExtentSuspectMissingExtents indicates the length of the suspect missing extent list
	ReplicatorReconcileDestExtentSuspectMissingExtents
	// ReplicatorReconcileCgExtentRun indicates the reconcile for cg extent runs
	ReplicatorReconcileCgExtentRun
	// ReplicatorReconcileCgExtentFail indicates the reconcile for cg extent fails
	ReplicatorReconcileCgExtentFail
	// ReplicatorReconcileCgExtentFoundMissing indicates the reconcile for cg extent found a missing cg extent
	ReplicatorReconcileCgExtentFoundMissing
	// ReplicatorReconcileCgExtentRemoteConsumedLocalMissing indicates the reconcile for cg extent found a cg extent that is consumed on remote side and local is missing
	ReplicatorReconcileCgExtentRemoteConsumedLocalMissing
	// ReplicatorReconcileCgExtentRemoteDeletedLocalMissing indicates the reconcile for cg extent found a cg extent that is deleted on remote side and local is missing
	ReplicatorReconcileCgExtentRemoteDeletedLocalMissing
	// ReplicatorInvalidHostUpdates indicates an invalid update received for hosts update
	ReplicatorInvalidHostsUpdate
	// ReplicatorHostUpdated indicates a success hosts update
	ReplicatorHostUpdated

	numMetrics
)

// var metricDefs = [NumServices][]map[int]metricDefinition{
var metricDefs = map[ServiceIdx]map[int]metricDefinition{

	// definition for Common metrics (for all services)
	Common: {
		MetadataRequests:                         {Counter, "metadata.requests"},
		MetadataFailures:                         {Counter, "metadata.failures"},
		MetadataLatency:                          {Timer, "metadata.latency"},
		ZoneFailoverMgrRunSuccess:                {Gauge, "ZoneFailoverMgr.run-success"},
		ZoneFailoverMgrRunFailToDetectActiveZone: {Gauge, "ZoneFailoverMgr.run-fail-detect-active-zone"},
		ZoneFailoverMgrRunFailToWriteMetadata:    {Gauge, "ZoneFailoverMgr.run-fail-write-metadata"},
		ZoneFailoverMgrRunFailure:                {Gauge, "ZoneFailoverMgr.run-failure"},
	},

	// definitions for Inputhost metrics
	Inputhost: {
		InputhostRequests:                     {Counter, "inputhost.requests"},
		InputhostFailures:                     {Counter, "inputhost.errors"},
		InputhostMessageReceived:              {Counter, "inputhost.message.received"},
		InputhostMessageFailures:              {Counter, "inputhost.message.errors"},
		InputhostReconfClientRequests:         {Counter, "inputhost.reconfigure.client.request"},
		InputhostMessageLimitThrottled:        {Counter, "inputhost.message.limit.throttled"},
		InputhostMessageChannelFullThrottled:  {Counter, "inputhost.message.channel.throttled"},
		InputhostUserFailures:                 {Counter, "inputhost.user-errors"},
		InputhostInternalFailures:             {Counter, "inputhost.internal-errors"},
		InputhostMessageUserFailures:          {Counter, "inputhost.message.user-errors"},
		InputhostMessageInternalFailures:      {Counter, "inputhost.message.internal-errors"},
		InputhostPubConnection:                {Gauge, "inputhost.pubconnection"},
		InputhostLatencyTimer:                 {Timer, "inputhost.latency"},
		InputhostWriteMessageLatency:          {Timer, "inputhost.message.write-latency"},
		InputhostWriteMessageBeforeAckLatency: {Timer, "inputhost.message.write-latency-before-ack"},
	},

	// definitions for Outputhost metrics
	Outputhost: {
		OutputhostRequests:                              {Counter, "outputhost.requests"},
		OutputhostFailures:                              {Counter, "outputhost.errors"},
		OutputhostLongPollingTimeOut:                    {Counter, "outputhost.timeout-longpoll"},
		OutputhostReceiveMsgBatchWriteToMsgCacheTimeout: {Counter, "outputhost.timeout-receive-msg-batch-write-to-msg-cache"},
		OutputhostMessageSent:                           {Counter, "outputhost.message.sent"},
		OutputhostMessageFailures:                       {Counter, "outputhost.message.errors"},
		OutputhostCreditsReceived:                       {Counter, "outputhost.credit-received"},
		OutputhostDLQMessageRequests:                    {Counter, "outputhost.message.sent-dlq"},
		OutputhostDLQMessageFailures:                    {Counter, "outputhost.message.errors-dlq"},
		OutputhostMessageRedelivered:                    {Counter, "outputhost.message.redelivered"},
		OutputhostMessageSentAck:                        {Counter, "outputhost.message.sent-ack"},
		OutputhostMessageSentNAck:                       {Counter, "outputhost.message.sent-nack"},
		OutputhostMessageAckFailures:                    {Counter, "outputhost.message.errors-ack"},
		OutputhostMessageNackFailures:                   {Counter, "outputhost.message.errors-nack"},
		OutputhostMessageNoAckManager:                   {Counter, "outputhost.message.no-ackmgr"},
		OutputhostMessageDiffSession:                    {Counter, "outputhost.message.diff-session"},
		OutputhostMessageAckManagerError:                {Counter, "outputhost.message.errors-ackmgr"},
		OutputhostUserFailures:                          {Counter, "outputhost.user-errors"},
		OutputhostInternalFailures:                      {Counter, "outputhost.internal-errors"},
		OutputhostConsConnection:                        {Gauge, "outputhost.consconnection"},
		OutputhostCreditsAccumulated:                    {Gauge, "outputhost.credit-accumulated"},
		OutputhostLatencyTimer:                          {Timer, "outputhost.latency"},
	},

	// definitions for Frontend metrics
	Frontend: {
		FrontendRequests:         {Counter, "frontend.requests"},
		FrontendFailures:         {Counter, "frontend.errors"},
		FrontendEntityNotExist:   {Counter, "frontend.errors.entitynotexist"},
		FrontendUserFailures:     {Counter, "frontend.user-errors"},
		FrontendInternalFailures: {Counter, "frontend.internal-errors"},
		FrontendLatencyTimer:     {Timer, "frontend.latency"},
	},

	// definitions for Storehost metrics
	Storage: {
		StorageRequests:                             {Counter, "storage.requests"},
		StorageFailures:                             {Counter, "storage.errors"},
		StorageStoreFailures:                        {Counter, "storage.store-error"},
		StorageMessageReceived:                      {Counter, "storage.message.received"},
		StorageMessageSent:                          {Counter, "storage.message.sent"},
		WatermarksReceived:                          {Counter, "storage.watermarks"},
		StorageOpenExtents:                          {Gauge, "storage.open-extents"},
		StorageWriteStreams:                         {Gauge, "storage.write.streams"},
		StorageReadStreams:                          {Gauge, "storage.read.streams"},
		StorageInMsgChanDepth:                       {Gauge, "storage.in.msgchan-depth"},
		StorageInAckChanDepth:                       {Gauge, "storage.in.ackchan-depth"},
		StorageOutMsgChanDepth:                      {Gauge, "storage.out.msgchan-depth"},
		StorageDiskAvailableSpaceMB:                 {Gauge, "storage.disk.availablespace.mb"},
		StorageDiskAvailableSpacePcnt:               {Gauge, "storage.disk.availablespace.pcnt"},
		StorageLatencyTimer:                         {Timer, "storage.latency"},
		StorageWriteStoreLatency:                    {Timer, "storage.write.store-latency"},
		StorageWriteMessageLatency:                  {Timer, "storage.write.message-latency"},
		StorageWriteMessageBeforeAckLatency:         {Timer, "storage.write.message-latency-before-ack"},
		StorageReadStoreLatency:                     {Timer, "storage.read.store-latency"},
		StorageReadMessageLatency:                   {Timer, "storage.read.message-latency"},
		StorageInWriteTChannelLatency:               {Timer, "storage.in.write-tchannel-latency"},
		StorageInFlushTChannelLatency:               {Timer, "storage.in.flush-tchannel-latency"},
		StorageOutWriteTChannelLatency:              {Timer, "storage.out.write-tchannel-latency"},
		StorageOutFlushTChannelLatency:              {Timer, "storage.out.flush-tchannel-latency"},
		StorageReplicationJobMaxConsecutiveFailures: {Gauge, "storage.replication-job.max-consecutive-failures"},
		StorageReplicationJobCurrentFailures:        {Gauge, "storage.replication-job.current-failures"},
		StorageReplicationJobCurrentSuccess:         {Gauge, "storage.replication-job.current-success"},
		StorageReplicationJobRun:                    {Gauge, "storage.replication-job.run"},
	},

	// definitions for Controller metrics
	Controller: {
		ControllerErrMetadataReadCounter:           {Counter, "controller.errors.metadata-read"},
		ControllerErrMetadataUpdateCounter:         {Counter, "controller.errors.metadata-update"},
		ControllerErrMetadataEntityNotFoundCounter: {Counter, "controller.errors.metadata-entitynotfound"},
		ControllerErrTryLockCounter:                {Counter, "controller.errors.try-lock"},
		ControllerErrCreateExtentCounter:           {Counter, "controller.errors.create-extent"},
		ControllerErrPickInHostCounter:             {Counter, "controller.errors.pick-inputhost"},
		ControllerErrPickOutHostCounter:            {Counter, "controller.errors.pick-outputhost"},
		ControllerErrCallReplicatorCounter:         {Counter, "controller.errors.call-replicator"},
		ControllerErrPickStoreHostCounter:          {Counter, "controller.errors.pick-storehost"},
		ControllerErrResolveUUIDCounter:            {Counter, "controller.errors.resolve-uuid"},
		ControllerErrCreateTChanClientCounter:      {Counter, "controller.errors.create-tchannel-client"},
		ControllerErrNoHealthyStoreCounter:         {Counter, "controller.errors.no-healthy-store"},
		ControllerErrSealFailed:                    {Counter, "controller.errors.seal-failed"},
		ControllerErrTooManyOpenCGExtents:          {Counter, "controller.errors.too-many-open-cgextents"},
		ControllerErrNoRetryWorkers:                {Counter, "controller.errors.no-retry-workers"},
		ControllerErrBadRequestCounter:             {Counter, "controller.errors.bad-requests"},
		ControllerErrBadEntityCounter:              {Counter, "controller.errors.bad-entity"},
		ControllerErrDrainFailed:                   {Counter, "controller.errors.drain-failed"},
		ControllerEventsDropped:                    {Counter, "controller.events-dropped"},
		ControllerRequests:                         {Counter, "controller.requests"},
		ControllerFailures:                         {Counter, "controller.errors"},
		ControllerRetries:                          {Counter, "controller.retries"},
		ControllerRetriesExceeded:                  {Counter, "controller.retries-exceeded"},
		ControllerRateLimited:                      {Counter, "controller.rate-limited"},
		ControllerRetentionJobStartCounter:         {Counter, "controller.retentionmgr.job.started"},
		ControllerRetentionJobFailedCounter:        {Counter, "controller.retentionmgr.job.failed"},
		ControllerRetentionJobCompletedCounter:     {Counter, "controller.retentionmgr.job.completed"},
		ControllerRetentionJobCancelledCounter:     {Counter, "controller.retentionmgr.job.cancelled"},
		ControllerGetAddressFailedCounter:          {Counter, "controller.retentionmgr.getaddress.failed"},
		ControllerGetAddressCompletedCounter:       {Counter, "controller.retentionmgr.getaddress.completed"},
		ControllerPurgeMessagesRequestCounter:      {Counter, "controller.retentionmgr.purgemessagesrequest"},
		ControllerNumOpenExtents:                   {Counter, "controller.dstextents.open"},
		ControllerNumSealedExtents:                 {Counter, "controller.dstextents.sealed"},
		ControllerNumConsumedExtents:               {Counter, "controller.dstextents.consumed"},
		ControllerNumOpenDLQExtents:                {Counter, "controller.dstextents.dlq.open"},
		ControllerNumSealedDLQExtents:              {Counter, "controller.dstextents.dlq.sealed"},
		ControllerNumConsumedDLQExtents:            {Counter, "controller.dstextents.dlq.consumed"},
		ControllerNumOpenCGExtents:                 {Counter, "controller.cgextents.open"},
		ControllerNumConsumedCGExtents:             {Counter, "controller.cgextents.consumed"},
		ControllerLatencyTimer:                     {Timer, "controller.latency"},
		ControllerRetentionJobDuration:             {Timer, "controller.retentionmgr.jobduration"},
		ControllerGetAddressLatency:                {Timer, "controller.retentionmgr.getaddresslatency"},
		ControllerPurgeMessagesLatency:             {Timer, "controller.retentionmgr.purgemessageslatency"},
		ControllerNoActiveZone:                     {Gauge, "controller.no-active-zone"},
	},

	// definitions for Replicator metrics
	Replicator: {
		ReplicatorCreateInStreamFailure:                         {Counter, "replicator.create-in-stream.failure"},
		ReplicatorCreateOutStreamFailure:                        {Counter, "replicator.create-out-stream.failure"},
		ReplicatorRequests:                                      {Counter, "replicator.requests"},
		ReplicatorFailures:                                      {Counter, "replicator.errors"},
		ReplicatorBadRequest:                                    {Counter, "replicator.requests.bad"},
		ReplicatorInConnCreditsReceived:                         {Counter, "replicator.inconn.creditsreceived"},
		ReplicatorInConnMsgWritten:                              {Counter, "replicator.inconn.msgwritten"},
		ReplicatorOutConnCreditsSent:                            {Counter, "replicator.outconn.creditssent"},
		ReplicatorOutConnMsgRead:                                {Counter, "replicator.outconn.msgread"},
		ReplicatorReconcileFail:                                 {Gauge, "replicator.reconcile.fail"},
		ReplicatorReconcileDestRun:                              {Gauge, "replicator.reconcile.dest.run"},
		ReplicatorReconcileDestFail:                             {Gauge, "replicator.reconcile.dest.fail"},
		ReplicatorReconcileDestFoundMissing:                     {Gauge, "replicator.reconcile.dest.foundmissing"},
		ReplicatorReconcileCgRun:                                {Gauge, "replicator.reconcile.cg.run"},
		ReplicatorReconcileCgFail:                               {Gauge, "replicator.reconcile.cg.fail"},
		ReplicatorReconcileCgFoundMissing:                       {Gauge, "replicator.reconcile.cg.foundmissing"},
		ReplicatorReconcileCgFoundUpdated:                       {Gauge, "replicator.reconcile.cg.foundupdated"},
		ReplicatorReconcileDestExtentRun:                        {Gauge, "replicator.reconcile.destextent.run"},
		ReplicatorReconcileDestExtentFail:                       {Gauge, "replicator.reconcile.destextent.fail"},
		ReplicatorReconcileDestExtentFoundMissing:               {Gauge, "replicator.reconcile.destextent.foundmissing"},
		ReplicatorReconcileDestExtentRemoteConsumedLocalMissing: {Gauge, "replicator.reconcile.destextent.remote-consumed-local-missing"},
		ReplicatorReconcileDestExtentRemoteDeletedLocalMissing:  {Gauge, "replicator.reconcile.destextent.remote-deleted-local-missing"},
		ReplicatorReconcileDestExtentRemoteDeletedLocalNot:      {Gauge, "replicator.reconcile.destextent.remote-deleted-local-not"},
		ReplicatorReconcileDestExtentSuspectMissingExtents:      {Gauge, "replicator.reconcile.destextent.suspect-missing-extent"},
		ReplicatorReconcileCgExtentRun:                          {Gauge, "replicator.reconcile.cgextent.run"},
		ReplicatorReconcileCgExtentFail:                         {Gauge, "replicator.reconcile.cgextent.fail"},
		ReplicatorReconcileCgExtentFoundMissing:                 {Gauge, "replicator.reconcile.cgextent.foundmissing"},
		ReplicatorReconcileCgExtentRemoteConsumedLocalMissing:   {Gauge, "replicator.reconcile.cgextent.remote-consumed-local-missing"},
		ReplicatorReconcileCgExtentRemoteDeletedLocalMissing:    {Gauge, "replicator.reconcile.cgextent.remote-deleted-local-missing"},
		ReplicatorInvalidHostsUpdate:                            {Counter, "replicator.hostupdater.invalid"},
		ReplicatorHostUpdated:                                   {Counter, "replicator.hostupdater.success"},
	},
}

var dynamicMetricDefs = map[ServiceIdx]map[int]metricDefinition{
	// definitions for Inputhost metrics
	Inputhost: {
		InputhostDestMessageReceived:              {Counter, "inputhost.message.received.dest"},
		InputhostDestMessageReceivedBytes:         {Counter, "inputhost.message.received.bytes.dest"},
		InputhostDestMessageSentBytes:             {Counter, "inputhost.message.sent.bytes.dest"},
		InputhostDestMessageFailures:              {Counter, "inputhost.message.errors.dest"},
		InputhostDestMessageLimitThrottled:        {Counter, "inputhost.message.limit.throttled.dest"},
		InputhostDestMessageChannelFullThrottled:  {Counter, "inputhost.message.channel.throttled.dest"},
		InputhostDestMessageUserFailures:          {Counter, "inputhost.message.user-errors.dest"},
		InputhostDestMessageInternalFailures:      {Counter, "inputhost.message.internal-errors.dest"},
		InputhostDestWriteMessageLatency:          {Timer, "inputhost.message.write-latency.dest"},
		InputhostDestWriteMessageBeforeAckLatency: {Timer, "inputhost.message.write-latency-before-ack.dest"},
		InputhostDestPubConnection:                {Gauge, "inputhost.pubconnection.dest"},
	},

	// definitions for Outputhost metrics
	Outputhost: {
		OutputhostCGMessageSent:           {Counter, "outputhost.message.sent.cg"},
		OutputhostCGMessageSentBytes:      {Counter, "outputhost.message.sent.bytes.cg"},
		OutputhostCGMessageReceivedBytes:  {Counter, "outputhost.message.received.bytes.cg"},
		OutputhostCGMessageFailures:       {Counter, "outputhost.message.errors.cg"},
		OutputhostCGCreditsReceived:       {Counter, "outputhost.credit-received.cg"},
		OutputhostCGDLQMessageRequests:    {Counter, "outputhost.message.sent-dlq.cg"},
		OutputhostCGDLQMessageFailures:    {Counter, "outputhost.message.errors-dlq.cg"},
		OutputhostCGMessageRedelivered:    {Counter, "outputhost.message.redelivered.cg"},
		OutputhostCGMessageSentAck:        {Counter, "outputhost.message.sent-ack.cg"},
		OutputhostCGMessageSentNAck:       {Counter, "outputhost.message.sent-nack.cg"},
		OutputhostCGMessagesThrottled:     {Counter, "outputhost.message.throttled"},
		OutputhostCGAckMgrSeqNotFound:     {Counter, "outputhost.ackmgr.seq-not-found.cg"},
		OutputhostCGReplicaReconnect:      {Counter, "outputhost.replica-reconnect.cg"},
		OutputhostCGMessageSentLatency:    {Timer, "outputhost.message.sent-latency.cg"},
		OutputhostCGMessageCacheSize:      {Gauge, "outputhost.message.cache.size.cg"},
		OutputhostCGConsConnection:        {Gauge, "outputhost.consconnection.cg"},
		OutputhostCGOutstandingDeliveries: {Gauge, "outputhost.outstandingdeliveries.cg"},
		OutputhostCGHealthState:           {Gauge, "outputhost.healthstate.cg"},
		OutputhostCGNumExtents:            {Gauge, "outputhost.numextents.cg"},
		OutputhostCGAckMgrSize:            {Gauge, "outputhost.ackmgr.size.cg"},
		OutputhostCGAckMgrLevelUpdate:     {Gauge, "outputhost.ackmgr.level.updated.cg"},
		OutputhostCGAckMgrConsumed:        {Gauge, "outputhost.ackmgr.consumed.cg"},
		OutputhostCGAckMgrResetMsg:        {Gauge, "outputhost.ackmgr.reset.message.cg"},
		OutputhostCGAckMgrResetMsgError:   {Gauge, "outputhost.ackmgr.reset.message.error.cg"},
		OutputhostCGSkippedMessages:       {Gauge, "outputhost.skipped.messages.cg"},
		OutputhostCGCreditsAccumulated:    {Gauge, "outputhost.credit-accumulated.cg"},

		// Kafka "broker-related metrics"
		OutputhostCGKafkaIncomingBytes:    {Counter, "outputhost.kafka.received.bytes.cg"},
		OutputhostCGKafkaOutgoingBytes:    {Counter, "outputhost.kafka.sent.bytes.cg"},
		OutputhostCGKafkaRequestSent:      {Counter, "outputhost.kafka.request.cg"},
		OutputhostCGKafkaRequestSize:      {Timer, "outputhost.kafka.request.size.cg"}, // Histograms are respresented in M3 as timers, per documentation
		OutputhostCGKafkaRequestLatency:   {Timer, "outputhost.kafka.request.latency.cg"},
		OutputhostCGKafkaResponseReceived: {Counter, "outputhost.kafka.response.received.cg"},
		OutputhostCGKafkaResponseSize:     {Timer, "outputhost.kafka.response.size.cg"},
	},

	// definitions for Controller metrics
	Controller: {
		ControllerCGBacklogAvailable:   {Gauge, "controller.backlog.available.cg"},
		ControllerCGBacklogUnavailable: {Gauge, "controller.backlog.unavailable.cg"},
		ControllerCGBacklogInflight:    {Gauge, "controller.backlog.inflight.cg"},
		ControllerCGBacklogDLQ:         {Gauge, "controller.backlog.DLQ.cg"},
		ControllerCGBacklogProgress:    {Gauge, "controller.backlog.progress.cg"},
	},
	Replicator: {
		ReplicatorInConnPerDestMsgWritten: {Counter, "replicator.inconn.perdestmsgwritten"},
		ReplicatorInConnPerDestMsgLatency: {Timer, "replicator.inconn.perdestmsglatency"},
	},
}

// ErrorClass is an enum to help with classifying SLA vs. non-SLA errors (SLA = "service level agreement")
type ErrorClass uint8

const (
	// NoError indicates that there is no error (error should be nil)
	NoError ErrorClass = iota
	// UserError indicates that this is NOT an SLA-reportable error
	UserError
	// InternalError indicates that this is an SLA-reportable error
	InternalError
)
