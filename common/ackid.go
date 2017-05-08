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
	"bytes"
	"encoding/base64"
	"fmt"
	"sync/atomic"
)

// CombinedID is the one which holds session, ackmgr and seqnum together
type CombinedID uint64

// AckID designates a consumer message to ack/nack
type AckID struct {
	MutatedID CombinedID
	Address   int64
}

const (
	sessionIDNumBits = 16 // 16 bits for the unique session ID
	ackMgrIDNumBits  = 16 // 16 bits for the unique ack managers within this host
	indexNumBits     = 32 // 32 bits for the index of the message within the ack manager
	maxBits          = 64

	sessionIDMask uint64 = 18446462598732840960 //0xffff000000000000
	ackMgrIDMask  uint64 = 281470681743360      //0x0000ffff00000000
	indexMask     uint64 = 4294967295           //0x00000000ffffffff
)

// ToString serializes AckID object into a base64 encoded string
// First 64 bits of the AckID is as follows:
// 16 bit - Session ID (constructed from the uuid)
// 16 bit - Monotonically increasing number to identify all unique ack managers within a host
// 32 bit - Sequence Number within the AckManager which is used to update the data structure within
//          the ack manager
// The reason for having the above fields in the ackID is as follows:
// sessionID - to make sure ack is to the same outputhost (let's say to prevent a bad client)
// ackID - to uniquely identify the ack managers within this outputhost
// index - to identify the data structure within the ackMgr
// Address - to make sure we validate the ack based on what we get from the store
// We need all the above to prevent collisions
func (a AckID) ToString() string {
	// TODO: have a pool for the bytes buffer
	var b bytes.Buffer
	fmt.Fprintln(&b, a.MutatedID, a.Address)
	return base64.StdEncoding.EncodeToString(b.Bytes())
}

// ConstructCombinedID constructs the combinedID from the session,
// ackmgr and the seq numbers based on the bit masks
func (a AckID) ConstructCombinedID(sessionID uint64, ackMgrID uint64, index uint64) CombinedID {
	return CombinedID(((sessionID << 48) & sessionIDMask) | ((ackMgrID << 32) & ackMgrIDMask) | (index & indexMask))
}

// DeconstructCombinedID deconstructs the combinedID
func (c CombinedID) DeconstructCombinedID() (uint16, uint16, uint32) {
	return uint16((uint64(c) & sessionIDMask) >> 48), uint16((uint64(c) & ackMgrIDMask) >> 32), uint32(uint64(c) & indexMask)
}

// AckIDFromString deserializes a string into the object.
func AckIDFromString(ackID string) (*AckID, error) {
	decoded, err := base64.StdEncoding.DecodeString(ackID)
	if err != nil {
		return nil, err
	}

	// get the decoded byte buffer into an ackID struct
	ret := AckID{}
	b := bytes.NewBuffer(decoded)
	_, err = fmt.Fscanln(b, &ret.MutatedID, &ret.Address)
	if err != nil {
		return nil, err
	}
	return &ret, nil
}

// ConstructAckID is a helper routine to construct the ackID from the given args
func ConstructAckID(sessionID uint16, ackMgrID uint16, index uint32, address int64) string {
	stAckID := AckID{Address: address}
	stAckID.MutatedID = stAckID.ConstructCombinedID(uint64(sessionID), uint64(ackMgrID), uint64(index))

	return stAckID.ToString()
}

type (
	// HostAckIDGenerator is the per host ackID generator for this host
	// Right now, it is a monotonically increasing uint32
	HostAckIDGenerator interface {
		// GetNextAckID is the routine which gets the next ackID
		GetNextAckID() uint32
	}

	hostAckIDGenImpl struct {
		currentAckID uint32
	}
)

// NewHostAckIDGenerator returns a HostAckIDGenerator object and starts from the given value
func NewHostAckIDGenerator(startFrom uint32) HostAckIDGenerator {
	return &hostAckIDGenImpl{
		currentAckID: startFrom,
	}
}

// GetNextAckID is the implementation of the corresponding method on the HostAckIDGenerator interface
func (h *hostAckIDGenImpl) GetNextAckID() uint32 {
	return atomic.AddUint32(&h.currentAckID, 1)
}
