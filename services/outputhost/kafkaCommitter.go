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
	"time"

	sc "github.com/bsm/sarama-cluster"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
)

var outputHostStartTime = time.Now()

// KafkaCommitter is commits ackLevels to Cassandra through the TChanMetadataClient interface
type KafkaCommitter struct {
	connectedStoreUUID *string
	commitLevel        CommitterLevel
	readLevel          CommitterLevel
	finalLevel         CommitterLevel
	*sc.OffsetStash
	consumer **sc.Consumer
	KafkaOffsetMetadata
	metadataString string // JSON version of KafkaOffsetMetadata
	logger         bark.Logger
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

	// OutputHostStartTime is the time that the output host started
	OutputHostStartTime string

	// CommitterStartTime is the time that this committer was started
	CommitterStartTime string
}

const kafkaOffsetMetadataVersion = uint(0) // Current version of the KafkaOffsetMetadata

/*
 * Committer interface
 */

// SetCommitLevel just updates the next Cherami ack level that will be flushed
func (c *KafkaCommitter) SetCommitLevel(l CommitterLevel) {
	c.commitLevel = l
	tp, offset := kafkaAddresser.GetTopicPartitionOffset(l.address, c.getLogFn())
	if tp != nil {
		c.OffsetStash.MarkPartitionOffset(tp.Topic, tp.Partition, offset, c.metadataString)
	}
}

// SetReadLevel just updates the next Cherami read level that will be flushed
func (c *KafkaCommitter) SetReadLevel(l CommitterLevel) {
	c.readLevel = l
}

// SetFinalLevel just updates the last possible read level
func (c *KafkaCommitter) SetFinalLevel(l CommitterLevel) {
	c.finalLevel = l
}

// UnlockAndFlush pushes our commit and read levels to Cherami metadata, using SetAckOffset
func (c *KafkaCommitter) UnlockAndFlush(l sync.Locker) error {
	var err error
	os := c.OffsetStash
	c.OffsetStash = sc.NewOffsetStash()
	l.Unlock() // MarkOffsets may take some time, so we unlock the thread that owns us
	if *c.consumer != nil {
		c.logger.WithField(`offsets`, os.Offsets()).Debug(`Flushing Offsets`)
		(*c.consumer).MarkOffsets(os)
		err = (*c.consumer).CommitOffsets()
	}
	return err
}

// GetReadLevel returns the next readlevel that will be flushed
func (c *KafkaCommitter) GetReadLevel() (l CommitterLevel) {
	l = c.readLevel
	return
}

// GetCommitLevel returns the next commit level that will be flushed
func (c *KafkaCommitter) GetCommitLevel() (l CommitterLevel) {
	l = c.commitLevel
	return
}

/*
 * Setup & Utility
 */

// NewKafkaCommitter instantiates a kafkaCommitter
func NewKafkaCommitter(
	outputHostUUID string,
	cgUUID string,
	logger bark.Logger,
	client **sc.Consumer) *KafkaCommitter {
	now := time.Now()

	meta := KafkaOffsetMetadata{
		Version:             kafkaOffsetMetadataVersion,
		OutputHostUUID:      outputHostUUID,
		CGUUID:              cgUUID,
		OutputHostStartTime: outputHostStartTime.Format(time.RFC3339),
		CommitterStartTime:  now.Format(time.RFC3339),
	}

	metaJSON, _ := json.Marshal(meta)
	return &KafkaCommitter{
		OffsetStash:         sc.NewOffsetStash(),
		metadataString:      string(metaJSON),
		KafkaOffsetMetadata: meta,
		logger:              logger,
		consumer:            client,
	}
}

func (c *KafkaCommitter) getLogFn() func() bark.Logger {
	return func() bark.Logger {
		return c.logger.WithFields(bark.Fields{
			`module`:       `KafkaCommitter`,
			common.TagOut:  common.FmtOut(c.OutputHostUUID),
			common.TagCnsm: common.FmtCnsm(c.CGUUID),
		})
	}
}
