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
	"math"
	"sync"

	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/common"
)

// Kafka Addresser store address encoding scheme
//                    -=-=-
// t = topic+partition (map lookup)
// o = offset
//                     ttttttooooooooooooo
// math.MaxInt64    =  9223372036854775807 = Max store address
// divisor          =       10000000000000
// MaxTP            =  922336 <- Note that it's 1 less, which makes the max offset uniform
// MaxOffset        =        9999999999999
// MaxAddress       =  9223369999999999999
// MaxOffset10YrQPS =                31709 <-- The sustained QPS that would exhaust the max offset in 10 years
//                    -=-=-
// https://play.golang.org/p/VVMnGgHWSG

const (
	kafkaAddresserDivisor    = int64(1e13)
	kafkaAddresserMaxTP      = int64(math.MaxInt64/kafkaAddresserDivisor) - 1
	kafkaAddresserMaxOffset  = kafkaAddresserDivisor - 1
	kafkaAddresserMaxAddress = kafkaAddresserMaxTP*kafkaAddresserDivisor + kafkaAddresserMaxOffset
)

// TopicPartition represents a Kafka topic/partition pair
type TopicPartition struct {
	Topic     string
	Partition int32
}

// KafkaTopicPartitionAddresser translates topic/partition/offset to/from store address
type KafkaTopicPartitionAddresser struct {
	sync.RWMutex
	tp2i   map[TopicPartition]int64
	i2tp   map[int64]*TopicPartition
	nextTP int64
}

var kafkaAddresser = KafkaTopicPartitionAddresser{}

// GetStoreAddress converts a given topic, partition, and offset into a store address
func (k *KafkaTopicPartitionAddresser) GetStoreAddress(tp *TopicPartition, offset int64, logFn func() bark.Logger) (address storeHostAddress) {
	calc := func(tpInt int64, offset int64) storeHostAddress {
		return storeHostAddress((tpInt * kafkaAddresserDivisor) + offset)
	}

	if offset > kafkaAddresserMaxOffset {
		logFn().WithFields(bark.Fields{
			`module`:    `kafkaAddresser`,
			`topic`:     tp.Topic,
			`partition`: tp.Partition,
			`offset`:    offset,
		}).Error(`Offset out of Kafka addresser design range`)
	}

	// Pegging the offset will just mean that we don't commit
	// larger offsets to Kafka, but shouldn't cause an outage
	offset = common.MinInt64(offset, kafkaAddresserMaxOffset)

	k.RLock()
	if i, ok := k.tp2i[*tp]; ok {
		k.RUnlock()
		return calc(i, offset)
	}
	k.RUnlock()

	// New addresser assignment path
	k.Lock()
	defer k.Unlock() // Defer only on the unhappy (write) path

	// Check for race between RUnlock and Lock
	if i, ok := k.tp2i[*tp]; ok {
		return calc(i, offset)
	}

	// Check initialization
	if k.nextTP == 0 {
		k.tp2i = make(map[TopicPartition]int64)
		k.i2tp = make(map[int64]*TopicPartition)
	}

	// Assignment
	k.nextTP++
	k.i2tp[k.nextTP] = tp
	k.tp2i[*tp] = k.nextTP

	if k.nextTP > kafkaAddresserMaxTP {
		panic(`too many topic/partition assignments`)
	}

	logFn().WithFields(bark.Fields{
		`module`:    `kafkaAddresser`,
		`topic`:     tp.Topic,
		`partition`: tp.Partition,
		`offset`:    offset,
		`tpInt`:     k.nextTP,
	}).Info(`New assignment`)

	return calc(k.nextTP, offset)
}

// GetTopicPartitionOffset recovers the original topic, partition, and offset from a store address
func (k *KafkaTopicPartitionAddresser) GetTopicPartitionOffset(address storeHostAddress, logFn func() bark.Logger) (tp *TopicPartition, offset int64) {
	returnOffset := func(tp *TopicPartition, address storeHostAddress) (tpOut *TopicPartition, offset int64) {
		return tp, int64(address) % kafkaAddresserDivisor
	}

	recoverTP := func(address storeHostAddress) (offset int64) {
		return int64(address) / kafkaAddresserDivisor
	}

	k.RLock()
	if tp, ok := k.i2tp[recoverTP(address)]; ok {
		k.RUnlock()
		return returnOffset(tp, address)
	}
	k.RUnlock()

	tpInt := recoverTP(address)
	_, offset = returnOffset(&TopicPartition{}, address)

	logFn().WithFields(bark.Fields{
		`module`:            `kafkaAddresser`,
		`offset`:            offset,
		`topicPartitionInt`: tpInt,
	}).Error(`Unmapped store address received`)
	return nil, 0
}
