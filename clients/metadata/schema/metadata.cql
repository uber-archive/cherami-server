-- Copyright (c) 2016 Uber Technologies, Inc.

-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:

-- The above copyright notice and this permission notice shall be included in
-- all copies or substantial portions of the Software.

-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
-- THE SOFTWARE.

-- this cql contains the latest schema used in dev environment
-- it is primarily used for unit test and integration test
-- this file should not be used to setup prod schema

CREATE TYPE user_info (
   user_name text,
   user_email text
);

CREATE TABLE user_operations_by_entity_name (
   entity_name text,
   entity_uuid uuid,
   entity_type text,
   initiator frozen<user_info>,
   caller_service_name text,
   caller_host_name text,
   operation_type text,
   operation_time timestamp,
   operation_content text,
   PRIMARY KEY (entity_name, entity_type, operation_type, operation_time)
);

CREATE TABLE user_operations_by_entity_uuid (
   entity_name text,
   entity_uuid uuid,
   entity_type text,
   initiator frozen<user_info>,
   caller_service_name text,
   caller_host_name text,
   operation_type text,
   operation_time timestamp,
   operation_content text,
   PRIMARY KEY (entity_uuid, entity_type, operation_type, operation_time)
);

CREATE TYPE destination_zone_config (
    zone text,
    allow_publish boolean,
    allow_consume boolean,
    always_replicate_to boolean,
    remote_extent_replica_num int,
);

CREATE TABLE destination_schema (
   destination_uuid text,
   version int,
   type text,
   source text,
   data varchar,
   created_time timestamp,
   PRIMARY KEY (destination_uuid, version)
);

CREATE TYPE destination (
    uuid uuid,
    path text,
    type int,
    status int,
    consumed_messages_retention int,
    unconsumed_messages_retention int,
    owner_email text,
    checksum_option int,
    is_multi_zone boolean,
    zone_configs list<frozen<destination_zone_config>>,
    schema_version int,
    options map<text, text>,
);

CREATE TABLE destinations (
    uuid uuid PRIMARY KEY,
    is_multi_zone boolean,
    destination frozen<destination>,

    -- DLQ Destination metadata; N.B.: DLQ destinations don't exist in the destinations_by_path table --
    dlq_purge_before timestamp, -- Indicates that retention should delete messages before this timestamp in this destination; consumer groups should not read before this
    dlq_merge_before timestamp, -- Indicates that extent controller should merge messages/extents created before this timestamp to the consumer group. consumer groups should not read before this
    dlq_consumer_group uuid,    -- If this is a DLQ destination, the consumer group uuid that corresponds to this DLQ destination
    kafka_cluster text,
    kafka_topics set<text>
);

CREATE INDEX ON destinations (is_multi_zone);

CREATE TABLE destinations_by_path (
    directory_uuid uuid,
    path text,
    is_multi_zone boolean,
    destination frozen<destination>,
    kafka_cluster text,
    kafka_topics set<text>,
    PRIMARY KEY (directory_uuid, path)
);

CREATE INDEX ON destinations_by_path (is_multi_zone);

CREATE TYPE extent (
  uuid uuid,
  destination_uuid uuid,
  store_uuids set<uuid>,
  input_host_uuid uuid,
  status int, -- ExtentStatus enum
  archival_location text,
  origin_zone text,
  remote_extent_primary_store text, // the primary store for remote extent
);

CREATE TYPE extent_replica_stats (
  extent_uuid uuid,
  store_uuid uuid,
  destination_uuid uuid,
  status int, -- ExtentReplicaStatus enum
  store text, -- the type of store to use (ex: Rockstor, Chunky, ManyRocks, etc)
  store_version text, -- the version of the particular store
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

CREATE TABLE destination_extents (
  destination_uuid uuid,
  extent_uuid uuid,
  status int, -- ExtentStatus enum
  status_updated_time timestamp,
  created_time timestamp,
  origin_zone text,
  extent frozen<extent>,
  replica_stats map<uuid, frozen<extent_replica_stats>>,
  consumer_group_visibility uuid,
  PRIMARY KEY (destination_uuid, extent_uuid)
);

CREATE INDEX ON destination_extents (status);
CREATE INDEX ON destination_extents (created_time);
CREATE INDEX ON destination_extents (origin_zone);

CREATE TABLE input_host_extents (
  destination_uuid uuid,
  input_host_uuid uuid,
  extent_uuid uuid,
  status int, -- ExtentStatus enum
  created_time timestamp,
  extent frozen<extent>,
  replica_stats map<uuid, frozen<extent_replica_stats>>,
  PRIMARY KEY (input_host_uuid, destination_uuid, extent_uuid)
);

CREATE INDEX ON input_host_extents (status);
CREATE INDEX ON input_host_extents (created_time);

CREATE TABLE store_extents (
  store_uuid uuid,
  extent_uuid uuid,
  status int, -- ExtentStatus enum
  created_time timestamp,
  extent frozen<extent>,
  replica_stats frozen<extent_replica_stats>,
  replication_status int,
  PRIMARY KEY (store_uuid, extent_uuid)
);

CREATE INDEX ON store_extents (status);
CREATE INDEX ON store_extents (replication_status);
CREATE INDEX ON store_extents (created_time);

CREATE TYPE consumer_group_zone_config (
    zone text,
    visible boolean,
);

CREATE TYPE consumer_group (
  uuid uuid,
  destination_uuid uuid,
  name text,
  start_from bigint,
  status int, -- ConsumerGroupStatus enum
  lock_timeout_seconds int,
  max_delivery_count int,
  skip_older_messages_seconds int,
  delay_seconds int,
  dead_letter_queue_destination_uuid uuid,
  owner_email text,
  is_multi_zone boolean,
  active_zone text,
  zone_configs list<frozen<consumer_group_zone_config>>,
  options map<text, text>,
);

CREATE TABLE consumer_groups (
  uuid uuid PRIMARY KEY,
  destination_uuid uuid,
  is_multi_zone boolean,
  consumer_group frozen<consumer_group>,
);

CREATE INDEX ON consumer_groups (destination_uuid);
CREATE INDEX ON consumer_groups (is_multi_zone);

CREATE TABLE consumer_groups_by_name (
  destination_uuid uuid,
  name text,
  is_multi_zone boolean,
  consumer_group frozen<consumer_group>,
  PRIMARY KEY (destination_uuid, name)
);
CREATE INDEX ON consumer_groups_by_name (is_multi_zone);

CREATE TABLE consumer_group_extents (
  consumer_group_uuid uuid,
  extent_uuid uuid,
  output_host_uuid uuid,
  status int,                           -- ConsumerGroupExtentStatus
  ack_level_offset bigint,              -- Storehost address of the current ackLevel
  ack_level_sequence bigint,            -- Output host sequence number of the current acklevel
  ack_level_sequence_rate double,       -- Rate of change in ack_level_seq, 1/seconds
  received_level_offset bigint,         -- Storehost address of the last message received by the outputhost
  received_level_sequence bigint,       -- Sequence number of the last message received by the outputhost
  received_level_sequence_rate double,  -- Rate of change in received_level_seq, 1/seconds
  connected_store uuid,                 -- UUID of the store that the outputhost is currently connected to
  store_uuids set<uuid>,
  PRIMARY KEY (consumer_group_uuid, extent_uuid)
);

CREATE INDEX ON consumer_group_extents (status);

CREATE TABLE uuid_to_host_addr (
  uuid uuid PRIMARY KEY,
  hostaddr text,
  hostname text
);

CREATE TABLE host_addr_to_uuid (
  hostaddr text PRIMARY KEY,
  uuid uuid,
  hostname text
);

CREATE TABLE host_info (
  hostname text,
  key text,
  value text,
  PRIMARY KEY(hostname, key)
);

CREATE TABLE service_config (
  cluster text,           -- name of the cluster, ex - prod
  service_name text,      -- name of the service input/output/store/controller/frontend
  service_version text,   -- version of the service, custom string, `*` indicates don't care
  sku text,      
  hostname text,
  config_key text,
  config_value text,
  PRIMARY KEY(cluster, service_name, service_version, sku, hostname, config_key)
);

