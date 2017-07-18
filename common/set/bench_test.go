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
	"math/rand"
	"testing"

	"github.com/google/uuid"
)

func BenchmarkSetInsert(b *testing.B) {

	for n := 1; n < 1048576; n *= 4 {

		var keys []string
		for i := 0; i < n; i++ {
			keys = append(keys, uuid.New().String())
		}

		for _, set := range []string{"SliceSet", "SortedSet", "MapSet", "SyncSet"} {

			s := newTestSet(set, n)

			b.Run(fmt.Sprintf("%s/%d", set, n), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					s.Insert(keys[rand.Intn(n)])
				}
			})
		}
	}
}

func BenchmarkSetContains(b *testing.B) {

	for n := 1; n < 65536; n *= 2 {

		var keys []string
		for i := 0; i < n; i++ {
			keys = append(keys, uuid.New().String())
		}

		for _, set := range []string{"SliceSet", "SortedSet", "MapSet", "SyncSet"} {

			s := newTestSet(set, n)

			for i := 0; i < n/2; i++ {
				s.Insert(keys[rand.Intn(n)])
			}

			b.Run(fmt.Sprintf("%s/%d", set, n), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					s.Contains(keys[rand.Intn(n)])
				}
			})
		}
	}
}
