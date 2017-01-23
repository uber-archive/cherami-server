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

package controllerhost

import (
	"sync"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cherami-server/common"
)

type (
	LockMgrSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}
)

func TestLockMgrSuite(t *testing.T) {
	suite.Run(t, new(LockMgrSuite))
}

func (s *LockMgrSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *LockMgrSuite) TestNonPowerOfTwo() {
	inputs := []int{3, 5, 7, 18, 24, 31}
	for _, input := range inputs {
		_, err := NewLockMgr(input, common.UUIDHashCode, common.GetDefaultLogger())
		s.NotNil(err)
	}
}

func (s *LockMgrSuite) TestLockUnlock() {
	lockMgr, err := NewLockMgr(16, common.UUIDHashCode, common.GetDefaultLogger())
	s.Nil(err, "Failed to create LockMgr")
	keys := make([]string, 10)
	for i := 0; i < len(keys); i++ {
		keys[i] = uuid.New()
		ok := lockMgr.TryLock(keys[i], 0)
		s.True(ok, "Failed to acquire lock")
		ok = lockMgr.TryLock(keys[i], 0)
		s.False(ok, "lock() must fail, but succeeded")
	}
	for i := 0; i < len(keys); i++ {
		lockMgr.Unlock(keys[i])
	}
	s.Equal(int64(0), lockMgr.Size(), "Wrong size at the end of test")
}

func (s *LockMgrSuite) TestConcurrentLockUnlock() {

	lockMgr, err := NewLockMgr(16, common.UUIDHashCode, common.GetDefaultLogger())
	s.Nil(err, "Failed to create LockMgr")

	keys := make([]string, 10)
	for i := 0; i < len(keys); i++ {
		keys[i] = uuid.New()
	}

	startWG := sync.WaitGroup{}
	doneWG := sync.WaitGroup{}
	startWG.Add(1)
	doneWG.Add(2)

	for j := 0; j < 2; j++ {

		go func() {

			startWG.Wait()

			for i := 0; i < len(keys); i++ {
				ok := lockMgr.TryLock(keys[i], time.Minute)
				s.True(ok, "Failed to acquire lock")
				ok = lockMgr.TryLock(keys[i], 0)
				s.False(ok, "lock() must fail, but succeeded")
			}

			for i := 0; i < len(keys); i++ {
				lockMgr.Unlock(keys[i])
			}

			doneWG.Done()

		}()
	}

	startWG.Done()
	succ := common.AwaitWaitGroup(&doneWG, time.Minute)
	s.True(succ, "Cocurrent lock()/unlock() test failed")
	s.Equal(int64(0), lockMgr.Size(), "Wrong size at the end of test")
}

func (s *LockMgrSuite) TestWaitQueue() {

	lockMgr, err := NewLockMgr(16, common.UUIDHashCode, common.GetDefaultLogger())
	s.Nil(err, "Failed to create LockMgr")

	startWG := sync.WaitGroup{}
	doneWG := sync.WaitGroup{}

	key := uuid.New()
	startWG.Add(1)

	for i := 0; i < 30; i++ {

		doneWG.Add(1)

		go func() {
			startWG.Wait()
			ok := lockMgr.TryLock(key, 5*time.Minute)
			s.True(ok, "tryLock() timed out")
			lockMgr.Unlock(key)
			doneWG.Done()
		}()
	}

	startWG.Done()
	succ := common.AwaitWaitGroup(&doneWG, 5*time.Minute)
	s.True(succ, "WaitQueue test failed")
	s.Equal(int64(0), lockMgr.Size(), "Wrong size at the end of test")
}

func (s *LockMgrSuite) TestTimeoutRace() {

	lockMgr, err := NewLockMgr(16, common.UUIDHashCode, common.GetDefaultLogger())
	s.Nil(err, "Failed to create LockMgr")

	key := uuid.New()
	ok := lockMgr.TryLock(key, time.Second)
	s.True(ok, "tryLock() failed unexpectedly")

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		lockMgr.TryLock(key, time.Second+40*time.Microsecond)
		wg.Done()
	}()

	<-time.After(time.Second)
	lockMgr.Unlock(key)
	wg.Wait()
}
