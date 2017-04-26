package main

import (
	"math"

	"github.com/uber/cherami-server/storage"
	"github.com/uber/cherami-thrift/.generated/go/store"

	"github.com/apache/thrift/lib/go/thrift"
)

// -- decode message/address -- //
const (
	seqNumBits = 26

	invalidKey = math.MaxInt64

	seqNumBitmask    = (int64(1) << seqNumBits) - 1
	timestampBitmask = math.MaxInt64 &^ seqNumBitmask
	seqNumMax        = int64(math.MaxInt64-2) & seqNumBitmask

	seqNumUnspecifiedSeal = int64(math.MaxInt64 - 1)
)

func deconstructKey(key storage.Key) (visibilityTime int64, seqNum int64) {
	return int64(int64(key) & timestampBitmask), int64(int64(key) & seqNumBitmask)
}

func deconstructSealExtentKey(key storage.Key) (seqNum int64) {

	seqNum = int64(key) & seqNumBitmask

	// we use the special seqnum ('MaxInt64 - 1') when the extent has been sealed
	// at an "unspecified" seqnum; check for this case, and return appropriate value
	if seqNum == (seqNumBitmask - 1) {
		seqNum = seqNumUnspecifiedSeal
	}

	return
}

func isSealExtentKey(key storage.Key) bool {
	return key != storage.InvalidKey && (int64(key)&timestampBitmask) == timestampBitmask
}

func deserializeMessage(data []byte) (*store.AppendMessage, error) {
	msg := &store.AppendMessage{}
	deserializer := thrift.NewTDeserializer()
	if err := deserializer.Read(msg, data); err != nil {
		return nil, err
	}

	return msg, nil
}
