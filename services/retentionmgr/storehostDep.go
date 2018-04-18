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

package retentionmgr

import (
	"time"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/store"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"
)

type storehostDepImpl struct {
	clientFactory common.ClientFactory
	logger        bark.Logger
}

func newStorehostDep(clientFactory common.ClientFactory, log bark.Logger) storehostDep {
	return &storehostDepImpl{clientFactory: clientFactory, logger: log}
}

func (t *storehostDepImpl) GetAddressFromTimestamp(storeID storehostID, extID extentID, timestamp int64) (addr int64, sealed bool, err error) {

	// no-op GetAddressFromTimestamp calls to kafka phantom-stores
	if string(storeID) == common.KafkaPhantomExtentStorehost {
		return int64(store.ADDR_BEGIN), false, nil
	}

	s, _, err := t.clientFactory.GetThriftStoreClientUUID(string(storeID), string(storeID))

	if err != nil {
		return int64(store.ADDR_BEGIN), false, err
	}

	defer t.clientFactory.ReleaseThriftStoreClient(string(storeID))

	ctx, cancel := thrift.NewContext(2 * time.Second)
	defer cancel()

	log := t.logger.WithFields(bark.Fields{
		common.TagStor: storeID,
		common.TagExt:  common.FmtExt(string(extID)),
		`timestamp`:    timestamp,
	})

	req := store.NewGetAddressFromTimestampRequest()
	req.ExtentUUID = common.StringPtr(string(extID))
	req.Timestamp = common.Int64Ptr(timestamp)

	log.Debug("GetAddressFromTimestamp on replica")

	// query storage to find address of the message with the given timestamp
	resp, err := s.GetAddressFromTimestamp(ctx, req)

	if err != nil {
		log.WithField(common.TagErr, err).Debug("GetAddressFromTimestamp failed")
		return int64(store.ADDR_BEGIN), false, err
	}

	log.WithFields(bark.Fields{
		`address`: resp.GetAddress(),
		`sealed`:  resp.GetSealed(),
	}).Debug("GetAddressFromTimestamp done")

	return resp.GetAddress(), resp.GetSealed(), nil
}

func (t *storehostDepImpl) PurgeMessages(storeID storehostID, extID extentID, retentionAddr int64) (doneAddr int64, err error) {

	// no-op PurgeMessages calls to kafka phantom-stores
	if string(storeID) == common.KafkaPhantomExtentStorehost {
		return int64(store.ADDR_BEGIN), nil
	}

	s, _, err := t.clientFactory.GetThriftStoreClientUUID(string(storeID), string(storeID))

	if err != nil {
		return 0, err
	}

	defer t.clientFactory.ReleaseThriftStoreClient(string(storeID))

	ctx, cancel := thrift.NewContext(10 * time.Second)
	defer cancel()

	log := t.logger.WithFields(bark.Fields{
		common.TagStor:  storeID,
		common.TagExt:   common.FmtExt(string(extID)),
		`retentionAddr`: retentionAddr,
	})

	req := store.NewPurgeMessagesRequest()
	req.ExtentUUID = common.StringPtr(string(extID))
	req.Address = common.Int64Ptr(int64(retentionAddr))

	log.Debug("PurgeMessages on replica")

	// send command to storage to purge messages until retention address
	resp, err := s.PurgeMessages(ctx, req)

	if err != nil {
		log.WithField(common.TagErr, err).Debug("PurgeMessages failed")
		return 0, err // ignore errors; we will get to it the next time retention runs
	}

	log.WithField(`doneAddr`, resp.GetAddress()).Debug("PurgeMessages done")

	return resp.GetAddress(), nil
}
