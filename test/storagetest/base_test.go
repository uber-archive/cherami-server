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

package storagetest

import (
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cherami-server/storage"
)

type StorageSuite struct {
	suite.Suite
}

func TestStorageSuite(t *testing.T) {
	suite.Run(t, new(StorageSuite))
}

func (s *StorageSuite) SetupTest() {
	var err error
	s.NoError(err)
	log.SetOutput(os.Stdout)
	log.SetLevel(log.ErrorLevel)
}

func (s *StorageSuite) TearDownTest() {
}

type Keys []storage.Key

// implement methods to enable sort
func (keys Keys) Len() int {
	return len(keys)
}

func (keys Keys) Swap(i, j int) {
	keys[i], keys[j] = keys[j], keys[i]
}

func (keys Keys) Less(i, j int) bool {
	return keys[i] < keys[j]
}

func (s *StorageSuite) testStoreRandomKeys(storeMgr storage.StoreManager) {

	id := storage.ExtentUUID(uuid.NewRandom())

	ext, err := storeMgr.OpenExtent(id, storage.IncreasingKeys, nil, false)
	s.NoError(err)

	numMessages := 1000
	minSize := 8
	maxSize := 65536

	keys := make(Keys, numMessages+1)

	var putKey storage.Key

	for i := 0; i < numMessages; i++ {

		// FIXME: ensure keys are _unique_
		putKey = 1 + storage.Key(rand.Int63())

		// ignore 'invalid-key'
		if putKey == storage.InvalidKey {
			i--
			continue
		}

		r := rand.New(rand.NewSource(int64(putKey))) // seed on key

		valPut := make([]byte, minSize+r.Intn(maxSize-minSize+1))

		for j := range valPut {
			valPut[j] = byte(r.Intn(256))
		}

		// test Put
		addr, e := ext.Put(putKey, valPut)
		log.Debugf("Put(key=%016x, len=%d bytes): addr=%016x", putKey, len(valPut), addr)

		// validate return values of Put
		s.NoError(e, "Put should not return error")

		keys[i] = putKey
	}

	for _, key := range keys {
		log.Debugf("presort key=%x\n", key)
	}

	// put an InvalidKey as an end sentinel marker
	keys[numMessages] = storage.InvalidKey

	// sort keys, since this is an ordered key-value store
	sort.Sort(keys)

	for _, key := range keys {
		log.Debugf("sorted key=%x\n", key)
	}

	// -- test SeekFirst
	addrFirst, keyFirst, errFirst := ext.SeekFirst()
	log.Debugf("SeekFirst(): addr=%016x key=%016x err=%v", addrFirst, keyFirst, errFirst)

	// validate return values of SeekFirst
	s.Equal(nil, errFirst, "SeekFirst() should not fail")
	s.NotEqual(storage.MinAddr, addrFirst, "SeekFirst should not return MinAddr")
	s.NotEqual(storage.InvalidKey, keyFirst, "SeekFirst should not return InvalidKey")
	s.Equal(keys[0], keyFirst, "SeekFirst returned incorrect key")

	// -- test SeekLast
	addrLast, keyLast, errLast := ext.SeekLast()
	log.Debugf("SeekLast(): addr=%016x key=%016x err=%v", addrLast, keyLast, errLast)

	// validate return values of SeekLast
	s.Equal(nil, errLast, "SeekLast() should not fail")
	s.NotEqual(storage.MaxAddr, addrLast, "SeekLast should not return MaxAddr")
	s.NotEqual(storage.InvalidKey, keyLast, "SeekLast should not return InvalidKey")
	s.Equal(keys[numMessages-1], keyLast, "SeekLast returned incorrect key")

	// -- test SeekFloor(0)
	addrFloor, keyFloor, errFloor := ext.SeekFloor(0)
	log.Debugf("SeekFloor(key=%016x): addr=%016x key=%016x", 0, addrFloor, keyFloor)

	// validate return values of SeekFloor
	s.Equal(nil, errFloor, "SeekFloor(0) should not return error")
	s.Equal(storage.MinAddr, addrFloor, "SeekFloor(0) should return MinAddr")
	s.Equal(storage.InvalidKey, keyFloor, "SeekFloor(0) should return InvalidKey")

	// -- test SeekCeiling(0)
	addrCeil, keyCeil, errCeil := ext.SeekCeiling(0)
	log.Debugf("SeekCeiling(key=%016x): addr=%016x key=%016x", 0, addrCeil, keyCeil)

	// validate return values of SeekCeiling
	s.NoError(errCeil, "SeekCeiling(0) should not fail")
	s.Equal(keys[0], keyCeil, "SeekCeiling(0) returned incorrect address")
	s.NotEqual(storage.MinAddr, addrCeil, "SeekCeiling(0) should not MinAddr")

	// validate SeekCeiling(0) with SeekFirst
	s.Equal(addrFirst, addrCeil, "SeekFirst and SeekCeiling(0) should return same address")

	// start from SeekCeiling(0)
	addr := addrFirst

	for i, key := range keys {

		if key == storage.InvalidKey {
			break
		}

		nextKey := keys[i+1]

		// -- test Get
		keyGet, valGet, nextAddrGet, nextKeyGet, errGet := ext.Get(addr)
		log.Debugf("Get(addr=%016x): key=%016x val=%d bytes nextAddr=%016x nextKey=%016x", addr, keyGet, len(valGet), nextAddrGet, nextKeyGet)

		nextAddr := nextAddrGet

		// validate return values of Get
		s.Equal(key, keyGet)
		s.Equal(nextKey, nextKeyGet)
		s.NoError(errGet)

		// validate addresses are strictly increasing
		s.Equal(true, nextAddr > addr, "nextAddr should be greater than addr")

		corrupt := false

		r := rand.New(rand.NewSource(int64(key)))
		s.Equal(minSize+r.Intn(maxSize-minSize+1), len(valGet))

		for j := range valGet {
			if valGet[j] != byte(r.Intn(256)) {
				corrupt = true
				break
			}
		}

		s.False(corrupt, "data corrupted")

		// test Next
		nextAddrNext, nextKeyNext, errNext := ext.Next(addr)
		log.Debugf("Next(addr=%016x): nextAddr=%016x nextKey=%016x", addr, nextAddrNext, nextKeyNext)

		// validate return values of Next
		s.Equal(nextAddr, nextAddrNext, "Next() and Get() should return the same nextAddr")
		s.Equal(nextKey, nextKeyNext, "Next() returned incorrect nextKey")
		s.NoError(errNext)

		// -- test SeekFloor
		addrFloor, keyFloor, errFloor = ext.SeekFloor(key)
		log.Debugf("SeekFloor(key=%016x): addr=%016x key=%016x", key, addrFloor, keyFloor)

		// validate return values of SeekFloor
		s.Equal(addr, addrFloor, "SeekFloor returned incorrect adddr")
		s.Equal(key, keyFloor, "SeekFloor returned incorrect key")
		s.NoError(errFloor, "SeekFloor should not return error")

		// -- test SeekFloor
		addrFloor, keyFloor, errFloor = ext.SeekFloor(key + 3)
		log.Debugf("SeekFloor(key=%016x): addr=%016x key=%016x", key+3, addrFloor, keyFloor)

		// validate return values of SeekFloor
		s.Equal(addr, addrFloor, "SeekFloor(key+3) returned incorrect adddr")
		s.Equal(key, keyFloor, "SeekFloor(key+3) returned incorrect key")
		s.NoError(errFloor, "SeekFloor(key+3) should not return error")

		// -- test SeekCeiling
		addrCeil, keyCeil, errCeil = ext.SeekCeiling(key)
		log.Debugf("SeekCeiling(key=%016x): addr=%016x key=%016x", key, addrCeil, keyCeil)

		// validate return values of SeekCeiling
		s.Equal(addr, addrCeil, "SeekCeiling returned incorrect addr")
		s.Equal(key, keyCeil, "SeekCeiling returned incorrect key")
		s.NoError(errCeil, "SeekCeiling should not return error")

		// -- test SeekCeiling(key-3)
		addrCeil, keyCeil, errCeil = ext.SeekCeiling(key - 3)
		log.Debugf("SeekCeiling(key=%016x): addr=%016x key=%016x", key-3, addrCeil, keyCeil)

		// validate return values of SeekCeiling
		s.Equal(addr, addrCeil, "SeekCeiling(key-3) should return addr of key")
		s.Equal(key, keyCeil, "SeekCeiling(key-3) should return key")
		s.NoError(errCeil, "SeekCeiling(key-3) should not return error")

		// -- test GetKey
		keyGetKey, errGetKey := ext.GetKey(addr)
		log.Debugf("GetKey(addr=%016x): key=%016x", addr, keyGetKey)

		// validate return values of GetKey
		s.Equal(key, keyGetKey, "GetKey returned incorrect key")
		s.NoError(errGetKey, "GetKey should not return error")

		addr = nextAddr
	}

	/* // disabled for now .. will be fixed when GetMany uses 'endKey'
	// -- test GetMany

	addr, key, err := ext.SeekFirst()
	log.Debugf("SeekFirst(): addr=%016x key=%016x err=%v", addrFirst, keyFirst, errFirst)

	for i := 0; i < numMessages; {

	  // ask for random set of messages
	  numMsgs := rand.Intn(numMessages / 10)
	  numAddrMax := rand.Intn(numMessages / 10)

	  if numAddrMax > (numMessages - i) {
	    numAddrMax = numMessages - i
	  }

	  msgs, nextAddr, nextKey, e := ext.GetMany(addrs[i], int32(numMsgs), addrs[i+numAddrMax])
	  log.Debugf("GetMany(addr=%016x num=%d addrMax=%016x): msgs=%d nextAddr=%x nextKey=%x err=%v", addrs[i], numMsgs, addrs[i+numAddrMax], len(msgs), nextAddr, nextKey, e)

	  n := len(msgs) // number of messages returned

	  // validate the number of messages returned
	  if numAddrMax < numMsgs {
	    s.Equal(numAddrMax, n)
	  } else {
	    s.Equal(numMsgs, n)
	  }

	  // validate each message
	  for j := range msgs {

	    s.Equal(keys[addrs[i+j]], msgs[j].Key) // validate each key

	    // validate message content
	    corrupt := false

	    r := rand.New(rand.NewSource(int64(keys[addrs[i+j]])))
	    for k := range msgs[j].Value {
	      if msgs[j].Value[k] != byte(r.Intn(256)) {
	        corrupt = true
	        break
	      }
	    }

	    s.Equal(false, corrupt)
	  }

	  s.Equal(nextAddr, addrs[i+n])
	  s.Equal(nextKey, keys[addrs[i+n]])
	  s.NoError(e)

	  i += n
	}
	*/

	ext.Close()
}

func (s *StorageSuite) testStoreIncreasingKeys(storeMgr storage.StoreManager) {

	id := storage.ExtentUUID(uuid.NewRandom())

	ext, err := storeMgr.OpenExtent(id, storage.IncreasingKeys, nil, false)
	s.NoError(err)

	numMessages := 1000
	minSize := 8
	maxSize := 65536

	keys := make(Keys, numMessages+1)

	var putKey storage.Key

	for i := 0; i < numMessages; i++ {

		// generate keys that are jumping ahead randomly
		putKey += 10 + storage.Key(rand.Int63n(65526))

		r := rand.New(rand.NewSource(int64(putKey)))

		valPut := make([]byte, minSize+r.Intn(maxSize-minSize+1))

		for j := range valPut {
			valPut[j] = byte(r.Intn(256))
		}

		// test Put
		addr, e := ext.Put(putKey, valPut)
		log.Debugf("Put(key=%016x, len=%d bytes): addr=%016x", putKey, len(valPut), addr)

		// validate return values of Put
		s.NoError(e, "Put should not return error")

		keys[i] = putKey
	}

	ext.Sync()

	// put an InvalidKey as an end sentinel marker
	keys[numMessages] = storage.InvalidKey

	// -- test SeekFirst
	addrFirst, keyFirst, errFirst := ext.SeekFirst()
	log.Debugf("SeekFirst(): addr=%016x key=%016x err=%v", addrFirst, keyFirst, errFirst)

	// validate return values of SeekFirst
	s.Equal(nil, errFirst, "SeekFirst() should not fail")
	s.NotEqual(storage.MinAddr, addrFirst, "SeekFirst should not return MinAddr")
	s.NotEqual(storage.InvalidKey, keyFirst, "SeekFirst should not return InvalidKey")
	s.Equal(keys[0], keyFirst, "SeekFirst returned incorrect key")

	// -- test SeekLast
	addrLast, keyLast, errLast := ext.SeekLast()
	log.Debugf("SeekLast(): addr=%016x key=%016x err=%v", addrLast, keyLast, errLast)

	// validate return values of SeekLast
	s.Equal(nil, errLast, "SeekLast() should not fail")
	s.NotEqual(storage.MaxAddr, addrLast, "SeekLast should not return MaxAddr")
	s.NotEqual(storage.InvalidKey, keyLast, "SeekLast should not return InvalidKey")
	s.Equal(keys[numMessages-1], keyLast, "SeekLast returned incorrect key")

	// -- test SeekFloor(0)
	addrFloor, keyFloor, errFloor := ext.SeekFloor(0)
	log.Debugf("SeekFloor(key=%016x): addr=%016x key=%016x", 0, addrFloor, keyFloor)

	// validate return values of SeekFloor
	s.Equal(nil, errFloor, "SeekFloor(0) should not return error")
	s.Equal(storage.MinAddr, addrFloor, "SeekFloor(0) should return MinAddr")
	s.Equal(storage.InvalidKey, keyFloor, "SeekFloor(0) should return InvalidKey")

	// -- test SeekCeiling(0)
	addrCeil, keyCeil, errCeil := ext.SeekCeiling(0)
	log.Debugf("SeekCeiling(key=%016x): addr=%016x key=%016x", 0, addrCeil, keyCeil)

	// validate return values of SeekCeiling
	s.NoError(errCeil, "SeekCeiling(0) should not fail")
	s.Equal(keys[0], keyCeil, "SeekCeiling(0) returned incorrect address")
	s.NotEqual(storage.MinAddr, addrCeil, "SeekCeiling(0) should not MinAddr")

	// validate SeekCeiling(0) with SeekFirst
	s.Equal(addrFirst, addrCeil, "SeekFirst and SeekCeiling(0) should return same address")

	// start from SeekCeiling(0)
	addr := addrFirst

	for i, key := range keys {

		if key == storage.InvalidKey {
			break
		}

		nextKey := keys[i+1]

		// -- test Get
		keyGet, valGet, nextAddrGet, nextKeyGet, errGet := ext.Get(addr)
		log.Debugf("Get(addr=%016x): key=%016x val=%d bytes nextAddr=%016x nextKey=%016x", addr, keyGet, len(valGet), nextAddrGet, nextKeyGet)

		nextAddr := nextAddrGet

		// validate return values of Get
		s.Equal(key, keyGet, "Get returned incorrect key")
		s.Equal(nextKey, nextKeyGet, "Get returned incorrect nextKey")
		s.NoError(errGet, "Get should not return error")

		// validate addresses are strictly increasing
		s.True(nextAddr > addr, "nextAddr should be greater than addr")

		corrupt := false

		r := rand.New(rand.NewSource(int64(key)))
		s.Equal(minSize+r.Intn(maxSize-minSize+1), len(valGet))

		for j := range valGet {
			if valGet[j] != byte(r.Intn(256)) {
				corrupt = true
				break
			}
		}

		s.False(corrupt, "data corrupted")

		// test Next
		nextAddrNext, nextKeyNext, errNext := ext.Next(addr)
		log.Debugf("Next(addr=%016x): nextAddr=%016x nextKey=%016x", addr, nextAddrNext, nextKeyNext)

		// validate return values of Next
		s.Equal(nextAddr, nextAddrNext, "Next() and Get() should return the same nextAddr")
		s.Equal(nextKey, nextKeyNext, "Next() returned incorrect nextKey")
		s.NoError(errNext)

		// -- test SeekFloor
		addrFloor, keyFloor, errFloor = ext.SeekFloor(key)
		log.Debugf("SeekFloor(key=%016x): addr=%016x key=%016x", key, addrFloor, keyFloor)

		// validate return values of SeekFloor
		s.Equal(addr, addrFloor, "SeekFloor returned incorrect adddr")
		s.Equal(key, keyFloor, "SeekFloor returned incorrect key")
		s.NoError(errFloor, "SeekFloor should not return error")

		// -- test SeekFloor
		addrFloor, keyFloor, errFloor = ext.SeekFloor(key + 3)
		log.Debugf("SeekFloor(key=%016x): addr=%016x key=%016x", key+3, addrFloor, keyFloor)

		// validate return values of SeekFloor
		s.Equal(addr, addrFloor, "SeekFloor(key+3) returned incorrect adddr")
		s.Equal(key, keyFloor, "SeekFloor(key+3) returned incorrect key")
		s.NoError(errFloor, "SeekFloor(key+3) should not return error")

		// -- test SeekCeiling
		addrCeil, keyCeil, errCeil = ext.SeekCeiling(key)
		log.Debugf("SeekCeiling(key=%016x): addr=%016x key=%016x", key, addrCeil, keyCeil)

		// validate return values of SeekCeiling
		s.Equal(addr, addrCeil, "SeekCeiling returned incorrect addr")
		s.Equal(key, keyCeil, "SeekCeiling returned incorrect key")
		s.NoError(errCeil, "SeekCeiling should not return error")

		// -- test SeekCeiling(key-3)
		addrCeil, keyCeil, errCeil = ext.SeekCeiling(key - 3)
		log.Debugf("SeekCeiling(key=%016x): addr=%016x key=%016x", key-3, addrCeil, keyCeil)

		// validate return values of SeekCeiling
		s.Equal(addr, addrCeil, "SeekCeiling(key-3) should return addr of key")
		s.Equal(key, keyCeil, "SeekCeiling(key-3) should return key")
		s.NoError(errCeil, "SeekCeiling(key-3) should not return error")

		// -- test GetKey
		keyGetKey, errGetKey := ext.GetKey(addr)
		log.Debugf("GetKey(addr=%016x): key=%016x", addr, keyGetKey)

		// validate return values of GetKey
		s.Equal(key, keyGetKey, "GetKey returned incorrect key")
		s.NoError(errGetKey, "GetKey should not return error")

		addr = nextAddr
	}

	/* // disabled for now .. will be fixed when GetMany uses 'endKey'
	// -- test GetMany

	addr, key, err := ext.SeekFirst()
	log.Debugf("SeekFirst(): addr=%016x key=%016x err=%v", addrFirst, keyFirst, errFirst)

	for i := 0; i < numMessages; {

	  // ask for random set of messages
	  numMsgs := rand.Intn(numMessages / 10)
	  numAddrMax := rand.Intn(numMessages / 10)

	  if numAddrMax > (numMessages - i) {
	    numAddrMax = numMessages - i
	  }

	  msgs, nextAddr, nextKey, e := ext.GetMany(addrs[i], int32(numMsgs), addrs[i+numAddrMax])
	  log.Debugf("GetMany(addr=%016x num=%d addrMax=%016x): msgs=%d nextAddr=%x nextKey=%x err=%v", addrs[i], numMsgs, addrs[i+numAddrMax], len(msgs), nextAddr, nextKey, e)

	  n := len(msgs) // number of messages returned

	  // validate the number of messages returned
	  if numAddrMax < numMsgs {
	    s.Equal(numAddrMax, n)
	  } else {
	    s.Equal(numMsgs, n)
	  }

	  // validate each message
	  for j := range msgs {

	    s.Equal(keys[addrs[i+j]], msgs[j].Key) // validate each key

	    // validate message content
	    corrupt := false

	    r := rand.New(rand.NewSource(int64(keys[addrs[i+j]])))
	    for k := range msgs[j].Value {
	      if msgs[j].Value[k] != byte(r.Intn(256)) {
	        corrupt = true
	        break
	      }
	    }

	    s.Equal(false, corrupt)
	  }

	  s.Equal(nextAddr, addrs[i+n])
	  s.Equal(nextKey, keys[addrs[i+n]])
	  s.NoError(e)

	  i += n
	}
	*/

	ext.Close()
}
