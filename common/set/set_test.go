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

package set

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newTestSet(setType string, cap int) (s Set) {

	switch setType {
	case "SliceSet":
		s = NewSliceSet(0)
	case "SortedSet":
		s = NewSortedSet(0)
	case "MapSet":
		s = NewMapSet(0)
	case "SyncSet":
		s = NewSyncSet(New(0))
	}

	return s
}

func TestSet(t *testing.T) {

	testCases := []struct {
		set0, set1 string // set type
	}{
		{"SliceSet", "SliceSet"},
		{"SliceSet", "SortedSet"},
		{"SliceSet", "MapSet"},
		{"SortedSet", "SliceSet"},
		{"SortedSet", "SortedSet"},
		{"SortedSet", "MapSet"},
		{"MapSet", "SliceSet"},
		{"MapSet", "SortedSet"},
		{"MapSet", "MapSet"},
		{"SyncSet", "SyncSet"},
		{"SyncSet", "MapSet"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("{%v,%v}", tc.set0, tc.set1), func(t *testing.T) {

			s0 := newTestSet(tc.set0, 0)
			s1 := newTestSet(tc.set1, 0)

			assert.False(t, s0.Contains("foo"))
			assert.Equal(t, 0, s0.Count())

			s0.Insert("foo")
			assert.Equal(t, 1, s0.Count())
			assert.True(t, s0.Contains("foo"))

			assert.True(t, s1.Subset(s0))
			assert.False(t, s0.Subset(s1))
			assert.False(t, s1.Superset(s0))
			assert.True(t, s0.Superset(s1))
			assert.False(t, s1.Equals(s0))
			assert.False(t, s0.Equals(s1))

			s1.Insert("foo")
			assert.True(t, s1.Subset(s0))
			assert.True(t, s0.Subset(s1))
			assert.True(t, s1.Superset(s0))
			assert.True(t, s0.Superset(s1))
			assert.True(t, s1.Equals(s0))
			assert.True(t, s0.Equals(s1))

			s1.Insert("bar")
			assert.Equal(t, 2, s1.Count())
			assert.False(t, s1.Subset(s0))
			assert.True(t, s0.Subset(s1))
			assert.True(t, s1.Superset(s0))
			assert.False(t, s0.Superset(s1))
			assert.False(t, s1.Equals(s0))
			assert.False(t, s0.Equals(s1))

			s0.Insert("bar")
			assert.True(t, s1.Subset(s0))
			assert.True(t, s0.Subset(s1))
			assert.True(t, s1.Superset(s0))
			assert.True(t, s0.Superset(s1))
			assert.True(t, s1.Equals(s0))
			assert.True(t, s0.Equals(s1))

			s0.Clear()
			assert.Equal(t, 0, s0.Count())
		})
	}
}
