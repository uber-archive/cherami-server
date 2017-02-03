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
	"sort"

	"github.com/uber/cherami-thrift/.generated/go/metadata"
)

type extentStatsSorter struct {
	extents []*metadata.DestinationExtent
	cmpFunc func(a, b *metadata.DestinationExtent) bool
}

// Len implements sort.Interace
func (s *extentStatsSorter) Len() int {
	return len(s.extents)
}

// Swap implements sort.Interface.
func (s *extentStatsSorter) Swap(i, j int) {
	s.extents[i], s.extents[j] = s.extents[j], s.extents[i]
}

// Less implements sort.Interface
func (s *extentStatsSorter) Less(i, j int) bool {
	return s.cmpFunc(s.extents[i], s.extents[j])
}

// cmpExtentStatsByTime compares two extent stats
// by createdTime and returns true, if a is less
// than by
func cmpExtentStatsByTime(a, b *metadata.DestinationExtent) bool {
	return a.GetCreatedTimeMillis() < b.GetCreatedTimeMillis()
}

func sortExtentStatsByTime(extents []*metadata.DestinationExtent) {
	sorter := &extentStatsSorter{
		extents: extents,
		cmpFunc: cmpExtentStatsByTime,
	}
	sort.Sort(sorter)
}
