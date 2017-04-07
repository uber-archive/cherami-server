// Copyright (c) 2017 Uber Technologies, Inc.
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

package outputhost

import (
	"encoding/json"
	"sync"

	sc "github.com/bsm/sarama-cluster"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/metadata"
)

// kafkaCommitter is commits ackLevels to Cassandra through the TChanMetadataClient interface
type kafkaCommitter struct {
	connectedStoreUUID *string
	commitLevel        CommitterLevel
	readLevel          CommitterLevel
	finalLevel         CommitterLevel
	metaclient         metadata.TChanMetadataService
	*sc.OffsetStash
	*sc.Consumer
	KafkaOffsetMetadata
	metadataString string // JSON version of KafkaOffsetMetadata
	logger bark.Logger
}

// KafkaOffsetMetadata is a structure used for JSON encoding/decoding of the metadata stored for
// Kafka offsets committed by Cherami
type KafkaOffsetMetadata struct {
	// Version is the version of this structure
	Version uint

	// CGUUID is the internal Cherami consumer group UUID that committed this offset
	CGUUID string

	// OutputHostUUID is the UUID of the Cherami Outputhost that committed this offset
	OutputHostUUID string
}

const kafkaOffsetMetadataVersion = uint(0) // Current version of the KafkaOffsetMetadata

/*
 * Committer interface
 */

// SetCommitLevel just updates the next Cherami ack level that will be flushed
func (c *kafkaCommitter) SetCommitLevel(l CommitterLevel) {
	c.commitLevel = l
	tp, offset := kafkaAddresser.GetTopicPartitionOffset(l.address, c.getLogFn())
	if tp != nil {
		c.OffsetStash.MarkPartitionOffset(tp.Topic, tp.Partition, offset, c.metadataString)
	}
}

// SetReadLevel just updates the next Cherami read level that will be flushed
func (c *kafkaCommitter) SetReadLevel(l CommitterLevel) {
	c.readLevel = l
}

// SetFinalLevel just updates the last possible read level
func (c *kafkaCommitter) SetFinalLevel(l CommitterLevel) {
	c.finalLevel = l
}

// UnlockAndFlush pushes our commit and read levels to Cherami metadata, using SetAckOffset
func (c *kafkaCommitter) UnlockAndFlush(l sync.Locker) error {
	os := c.OffsetStash
	c.OffsetStash = sc.NewOffsetStash()
	l.Unlock() // MarkOffsets may take some time, so we unlock the thread that owns us
	c.MarkOffsets(os)
	return nil
}

// GetReadLevel returns the next readlevel that will be flushed
func (c *kafkaCommitter) GetReadLevel() (l CommitterLevel) {
	l = c.readLevel
	return
}

// GetCommitLevel returns the next commit level that will be flushed
func (c *kafkaCommitter) GetCommitLevel() (l CommitterLevel) {
	l = c.commitLevel
	return
}

/*
 * Setup & Utility
 */

// NewkafkaCommitter instantiates a kafkaCommitter
func NewkafkaCommitter(metaclient metadata.TChanMetadataService,
	outputHostUUID string,
	cgUUID string,
	extUUID string,
	connectedStoreUUID *string,
	logger bark.Logger) *kafkaCommitter {
	meta := KafkaOffsetMetadata{
		Version:        kafkaOffsetMetadataVersion,
		OutputHostUUID: outputHostUUID,
		CGUUID:         cgUUID,
	}

	metaJSON, _ := json.Marshal(meta)
	return &kafkaCommitter{
		metaclient:          metaclient,
		connectedStoreUUID:  connectedStoreUUID,
		OffsetStash:         sc.NewOffsetStash(),
		metadataString:      string(metaJSON),
		KafkaOffsetMetadata: meta,
		logger: logger,
	}
}

func (c *kafkaCommitter) getLogFn() func() bark.Logger {
	return func() bark.Logger {
		return c.logger.WithFields(bark.Fields{
			`module`:       `kafkaCommitter`,
			common.TagOut:  common.FmtOut(c.OutputHostUUID),
			common.TagCnsm: common.FmtCnsm(c.CGUUID),
		})
	}
}
