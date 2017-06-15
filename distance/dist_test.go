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

package distance

import (
	"io/ioutil"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
)

const topology = `
distance: 1024
entities:
  - resource: foo1-dc
    type: dc
    distance: 4
    entities:
      - resource: foo1a-pod
        type: pod
        distance: 2
        entities:
          - resource: foo1a1-rack
            type: rack
            distance: 1
            entities:
              - resource: cherami01-foo1
                type: host
                entities:
                  - resource: 10.10.0.1
                  - resource: 10.10.0.2
          - resource: foo1a2-rack
            type: rack
            distance: 1
            entities:
              - resource: cherami02-foo1
                type: host
                entities:
                  - resource: 10.10.1.1
                  - resource: 10.10.1.2
  - resource: bar1-dc
    type: dc
    distance: 4
    entities:
      - resource: bar1a-pod
        type: pod
        distance: 2
        entities:
          - resource: bar1a1-rack
            type: rack
            distance: 1
            entities:
              - resource: cherami01-bar1
                type: host
                entities:
                  - resource: 10.1.1.1
                  - resource: 10.1.1.2
              - resource: cherami02-bar1
                type: host
                entities:
                  - resource: 10.1.2.1
                  - resource: 10.1.2.2
        overrides:
          - target: bar2-dc
            distance: 128
  - resource: bar2-dc
    type: dc
    distance: 4
    entities:
      - resource: bar2b-pod
        type: pod
        distance: 2
        entities:
          - resource: bar2b3-rack
            type: rack
            distance: 1
            entities:
              - resource: cherami03-bar2
                type: host
                entities:
                  - resource: 10.2.3.1
                  - resource: 10.2.3.2
        overrides:
          - target: bar1-dc
            distance: 256
`

type DistanceSuite struct {
	suite.Suite
	tmpFile *os.File
	distMap Map
}

func TestDistanceSuite(t *testing.T) {
	suite.Run(t, new(DistanceSuite))
}

func (s *DistanceSuite) SetupTest() {
	//log.Configure(&log.Configuration{Stdout: true}, true)

	f, err := ioutil.TempFile("", "topology")
	require.NoError(s.T(), err)
	s.tmpFile = f
	defer f.Close()

	_, err = f.Write([]byte(topology))
	require.NoError(s.T(), err)

	s.distMap, err = New(f.Name(), bark.NewLoggerFromLogrus(log.New()))
	require.NoError(s.T(), err)
}

func (s *DistanceSuite) TearDownTest() {
	defer os.RemoveAll(s.tmpFile.Name())
}

func (s *DistanceSuite) TestMissing() {
	m, e := New("MISSING_FILE", bark.NewLoggerFromLogrus(log.New()))
	assert.Error(s.T(), e)

	_, _, e = m.FindDistance("A", "B")
	assert.EqualValues(s.T(), errSource, e)

	_, _, e = s.distMap.FindDistance("10.1.1.1", "10.2.3.0")
	assert.EqualValues(s.T(), errTarget, e)

	_, _, e = s.distMap.FindDistance("10.1.1.0", "10.2.3.1")
	assert.EqualValues(s.T(), errSource, e)
}

func (s *DistanceSuite) TestFindDistance() {
	x, y, e := s.distMap.FindDistance("", "10.1.1.1")
	assert.NoError(s.T(), e)
	assert.EqualValues(s.T(), 0, x)
	assert.EqualValues(s.T(), 0, y)

	x, y, e = s.distMap.FindDistance("10.1.1.1", "10.1.1.2")
	assert.NoError(s.T(), e)
	assert.EqualValues(s.T(), 0, x)
	assert.EqualValues(s.T(), 0, y)

	x, y, e = s.distMap.FindDistance("10.1.1.1", "10.1.2.2")
	assert.NoError(s.T(), e)
	assert.EqualValues(s.T(), 1, x)
	assert.EqualValues(s.T(), 1, y)

	x, y, e = s.distMap.FindDistance("10.1.1.1", "10.2.3.2")
	assert.NoError(s.T(), e)
	assert.EqualValues(s.T(), 128, x)
	assert.EqualValues(s.T(), 256, y)

	x, y, e = s.distMap.FindDistance("10.10.0.1", "10.2.3.2")
	assert.NoError(s.T(), e)
	assert.EqualValues(s.T(), 1024, x)
	assert.EqualValues(s.T(), 1024, y)

	x, y, e = s.distMap.FindDistance("10.1.1.1", "cherami01-bar1")
	assert.NoError(s.T(), e)
	assert.EqualValues(s.T(), 0, x)
	assert.EqualValues(s.T(), 0, y)

	x, y, e = s.distMap.FindDistance("10.1.1.1", "cherami02-bar1")
	assert.NoError(s.T(), e)
	assert.EqualValues(s.T(), 1, x)
	assert.EqualValues(s.T(), 1, y)

	x, y, e = s.distMap.FindDistance("10.1.1.1", "cherami03-bar2")
	assert.NoError(s.T(), e)
	assert.EqualValues(s.T(), 128, x)
	assert.EqualValues(s.T(), 256, y)

	x, y, e = s.distMap.FindDistance("10.10.0.1", "cherami03-bar2")
	assert.NoError(s.T(), e)
	assert.EqualValues(s.T(), 1024, x)
	assert.EqualValues(s.T(), 1024, y)

	x, y, e = s.distMap.FindDistance("cherami01-bar1", "10.1.1.2")
	assert.NoError(s.T(), e)
	assert.EqualValues(s.T(), 0, x)
	assert.EqualValues(s.T(), 0, y)

	x, y, e = s.distMap.FindDistance("cherami01-bar1", "10.1.2.2")
	assert.NoError(s.T(), e)
	assert.EqualValues(s.T(), 1, x)
	assert.EqualValues(s.T(), 1, y)

	x, y, e = s.distMap.FindDistance("cherami01-bar1", "10.2.3.2")
	assert.NoError(s.T(), e)
	assert.EqualValues(s.T(), 128, x)
	assert.EqualValues(s.T(), 256, y)

	x, y, e = s.distMap.FindDistance("cherami01-foo1", "10.2.3.2")
	assert.NoError(s.T(), e)
	assert.EqualValues(s.T(), 1024, x)
	assert.EqualValues(s.T(), 1024, y)
}

func (s *DistanceSuite) TestFindResources() {
	_, e := s.distMap.FindResources(nil, []string{"10.1.1.1"}, "unknown", 1, 0, 1)
	assert.Error(s.T(), e)

	t, e := s.distMap.FindResources(nil, []string{"10.1.1.2"}, "host", 1, 0, 1)
	assert.NoError(s.T(), e)
	assert.Contains(s.T(), t, "cherami01-bar1")

	t, e = s.distMap.FindResources(nil, []string{"cherami01-bar1"}, "host", 1, 1, 4)
	assert.NoError(s.T(), e)
	assert.Contains(s.T(), t, "cherami02-bar1")

	t, e = s.distMap.FindResources(nil, []string{"10.1.1.1"}, "host", 2, 0, 2)
	assert.NoError(s.T(), e)
	assert.Contains(s.T(), t, "cherami01-bar1")
	assert.Contains(s.T(), t, "cherami02-bar1")

	t, e = s.distMap.FindResources(nil, []string{"10.1.1.2"}, "host", 2, 1, 2)
	assert.Error(s.T(), e)
	assert.Contains(s.T(), t, "cherami02-bar1")

	t, e = s.distMap.FindResources(nil, []string{"cherami01-bar1"}, "host", 2, 1, 257)
	assert.NoError(s.T(), e)
	assert.Contains(s.T(), t, "cherami02-bar1")
	assert.Contains(s.T(), t, "cherami03-bar2")

	t, e = s.distMap.FindResources(nil, []string{"cherami02-bar1", "cherami03-bar2"}, "host", 1, 1, 257)
	assert.NoError(s.T(), e)
	assert.Contains(s.T(), t, "cherami01-bar1")

	t, e = s.distMap.FindResources(nil, []string{"cherami01-foo1"}, "host", 1, 2, 3)
	assert.NoError(s.T(), e)
	assert.Contains(s.T(), t, "cherami02-foo1")

	t, e = s.distMap.FindResources(nil, []string{"foo1a-pod"}, "host", 2, 0, 3)
	assert.NoError(s.T(), e)
	assert.Contains(s.T(), t, "cherami01-foo1")
	assert.Contains(s.T(), t, "cherami02-foo1")
}

func benchmarkFindResources(b *testing.B, sourceResources []string, targetType string, count int, minDistance, maxDistance uint16) {
	m, _ := New("dist_test.yaml", bark.NewLoggerFromLogrus(log.New()))
	b.SetParallelism(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.FindResources(nil, sourceResources, targetType, count, minDistance, maxDistance)
	}
}

func BenchmarkFindResources_Zero_1_2_4(b *testing.B) {
	benchmarkFindResources(b, []string{}, "host", 1, 2, 4)
}

func BenchmarkFindResources_Zero_2_2_4(b *testing.B) {
	benchmarkFindResources(b, []string{}, "host", 2, 2, 4)
}

func BenchmarkFindResources_Zero_3_2_4(b *testing.B) {
	benchmarkFindResources(b, []string{}, "host", 3, 2, 4)
}

func BenchmarkFindResources_One_1_1_2(b *testing.B) {
	benchmarkFindResources(b, []string{"test215-foo1"}, "host", 1, 1, 2)
}

func BenchmarkFindResources_One_2_1_2(b *testing.B) {
	benchmarkFindResources(b, []string{"test215-foo1"}, "host", 2, 1, 2)
}

func BenchmarkFindResources_One_1_2_4(b *testing.B) {
	benchmarkFindResources(b, []string{"test215-foo1"}, "host", 1, 2, 4)
}

func BenchmarkFindResources_One_2_2_4(b *testing.B) {
	benchmarkFindResources(b, []string{"test215-foo1"}, "host", 2, 2, 4)
}

func BenchmarkFindResources_One_3_2_4(b *testing.B) {
	benchmarkFindResources(b, []string{"test215-foo1"}, "host", 3, 2, 4)
}

func BenchmarkFindResources_One_7_2_4(b *testing.B) {
	benchmarkFindResources(b, []string{"test215-foo1"}, "host", 7, 2, 4)
}

func BenchmarkFindResources_Two_1_1_2(b *testing.B) {
	benchmarkFindResources(b, []string{"test209-foo1", "test215-foo1"}, "host", 1, 1, 2)
}

func BenchmarkFindResources_Two_1_2_4(b *testing.B) {
	benchmarkFindResources(b, []string{"test209-foo1", "test215-foo1"}, "host", 1, 2, 4)
}
