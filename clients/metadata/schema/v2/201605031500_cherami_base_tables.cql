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

CREATE TYPE destination (
    uuid uuid,
    path text,
    type int,
    status int,
    consumed_messages_retention int,
    unconsumed_messages_retention int,
    owner_email text
);

CREATE TABLE destinations (
    uuid uuid PRIMARY KEY,
    destination frozen<destination>
);

CREATE TABLE destinations_by_path (
    directory_uuid uuid,
    path text, 
    destination frozen<destination>,
    PRIMARY KEY (directory_uuid, path)
);

CREATE TYPE extent (
  uuid uuid,
  destination_uuid uuid,
  store_uuids set<uuid>,
  input_host_uuid uuid,
  status int, -- ExtentStatus enum
  archival_location text
);

CREATE TYPE extent_replica_stats (
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

CREATE TABLE destination_extents (
  destination_uuid uuid,
  extent_uuid uuid,
  status int, -- ExtentStatus enum
  extent frozen<extent>,
  replica_stats map<uuid, frozen<extent_replica_stats>>,
  PRIMARY KEY (destination_uuid, extent_uuid)
);

CREATE INDEX ON destination_extents (status);

CREATE TABLE input_host_extents (
  destination_uuid uuid,
  input_host_uuid uuid,
  extent_uuid uuid,
  status int, -- ExtentStatus enum
  extent frozen<extent>,
  replica_stats map<uuid, frozen<extent_replica_stats>>,
  PRIMARY KEY (input_host_uuid, destination_uuid, extent_uuid)
);

CREATE INDEX ON input_host_extents (status);

CREATE TABLE store_extents (
  store_uuid uuid,
  extent_uuid uuid,
  status int, -- ExtentStatus enum
  extent frozen<extent>,
  replica_stats frozen<extent_replica_stats>,
  PRIMARY KEY (store_uuid, extent_uuid)
);

CREATE INDEX ON store_extents (status);

CREATE TYPE consumer_group (
  uuid uuid,
  destination_uuid uuid,
  name text,
  status int, -- ConsumerGroupStatus enum
  lock_timeout_seconds int,
  max_delivery_count int,
  skip_older_messages_seconds int,
  dead_letter_queue_destination_uuid uuid,
  owner_email text
);

CREATE TABLE consumer_groups (
  uuid uuid PRIMARY KEY,
  consumer_group frozen<consumer_group>,
);

CREATE TABLE consumer_groups_by_name (
  destination_uuid uuid,
  name text,
  consumer_group frozen<consumer_group>,
  PRIMARY KEY (destination_uuid, name)
);

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
