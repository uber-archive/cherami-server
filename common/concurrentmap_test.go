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
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	ConcurrentMapSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}
	boolType bool
	Integer  int
)

func TestConcurrentMapSuite(t *testing.T) {
	suite.Run(t, new(ConcurrentMapSuite))
}

func (s *ConcurrentMapSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *ConcurrentMapSuite) TestGetAfterPut() {

	countMap := make(map[string]int)
	testMap := NewShardedConcurrentMap(1, UUIDHashCode)

	for i := 0; i < 1024; i++ {
		key := uuid.New()
		countMap[key] = 0
		testMap.Put(key, boolType(true))
	}

	for k := range countMap {
		v, ok := testMap.Get(k)
		boolValue := v.(boolType)
		s.True(ok, "Get after put failed")
		s.True(bool(boolValue), "Wrong value returned from map")
	}

	s.Equal(len(countMap), testMap.Size(), "Size() returned wrong value")

	it := testMap.Iter()
	for entry := range it.Entries() {
		countMap[entry.Key]++
	}
	it.Close()

	for _, v := range countMap {
		s.Equal(1, v, "Iterator test failed")
	}

	for k := range countMap {
		testMap.Remove(k)
	}

	s.Equal(0, testMap.Size(), "Map returned non-zero size after deleting all entries")
}

func (s *ConcurrentMapSuite) TestPutIfNotExist() {
	testMap := NewShardedConcurrentMap(1, UUIDHashCode)
	key := uuid.New()
	ok := testMap.PutIfNotExist(key, boolType(true))
	s.True(ok, "PutIfNotExist failed to insert item")
	ok = testMap.PutIfNotExist(key, boolType(true))
	s.False(ok, "PutIfNotExist invariant failed")
}

func (s *ConcurrentMapSuite) TestMapConcurrency() {
	nKeys := 1024
	keys := make([]string, nKeys)
	for i := 0; i < nKeys; i++ {
		keys[i] = uuid.New()
	}

	var total int32
	var startWG sync.WaitGroup
	var doneWG sync.WaitGroup
	testMap := NewShardedConcurrentMap(1024, UUIDHashCode)

	startWG.Add(1)

	for i := 0; i < 10; i++ {

		doneWG.Add(1)

		go func() {
			startWG.Wait()
			for n := 0; n < nKeys; n++ {
				val := Integer(rand.Int())
				if testMap.PutIfNotExist(keys[n], val) {
					atomic.AddInt32(&total, int32(val))
					_, ok := testMap.Get(keys[n])
					s.True(ok, "Concurrency Get test failed")
				}
			}
			doneWG.Done()
		}()
	}

	startWG.Done()
	doneWG.Wait()

	s.Equal(nKeys, testMap.Size(), "Wrong concurrent map size")

	var gotTotal int32
	for i := 0; i < nKeys; i++ {
		v, ok := testMap.Get(keys[i])
		s.True(ok, "Get failed to find previously inserted key")
		intVal := v.(Integer)
		gotTotal += int32(intVal)
	}

	s.Equal(total, gotTotal, "Concurrent put test failed, wrong sum of values inserted")
}
