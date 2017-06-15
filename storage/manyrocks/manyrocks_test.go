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

package manyrocks

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber/cherami-server/storage"
)

type ManyRocksSuite struct {
	suite.Suite
}

func TestStorageSuite(t *testing.T) {
	suite.Run(t, new(ManyRocksSuite))
}

func (s *ManyRocksSuite) TestExtentOpenClose() {
	tmpTestDir, _ := ioutil.TempDir("", "manyrocks-test")
	mgr, err := New(&Opts{BaseDir: tmpTestDir}, bark.NewLoggerFromLogrus(log.New()))
	s.NoError(err)

	id := storage.ExtentUUID(uuid.NewRandom())
	path := fmt.Sprintf("%s/%v", tmpTestDir, id)

	// open with failIfNotExist == true
	ext, err := mgr.OpenExtent(id, storage.IncreasingKeys, nil, true)
	s.Nil(ext)
	s.Error(err)

	// verify no dir created
	_, err = os.Stat(path)
	s.Error(err)
	s.True(os.IsNotExist(err))

	// open with failIfNotExist == false -- should create the dir
	ext, err = mgr.OpenExtent(id, storage.IncreasingKeys, nil, false)
	s.NotNil(ext)
	s.NoError(err)

	// verify dir created
	_, err = os.Stat(path)
	s.NoError(err)

	// verify newly created extent is not in deleted extents map
	s.False(mgr.isExtentDeleted(id))

	ext.DeleteExtent()
	ext.Close()

	// verify dir deleted
	_, err = os.Stat(path)
	s.Error(err)
	s.True(os.IsNotExist(err))

	// verify deleted extent is tracked
	s.True(mgr.isExtentDeleted(id))
}
