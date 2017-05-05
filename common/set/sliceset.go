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

type sliceset struct {
	s []string
}

// NewSliceSet initializes a slice-based set
func NewSliceSet(cap int) Set {

	return &sliceset{
		s: make([]string, 0),
	}
}

// Clear clears the set
func (t *sliceset) Clear() {
	t.s = make([]string, 0)
}

// Empty checks if the set is empty
func (t *sliceset) Empty() bool {
	return len(t.s) == 0
}

// Count returns the size of the set
func (t *sliceset) Count() int {
	return len(t.s)
}

// Insert adds a key to the set
func (t *sliceset) Insert(key string) {

	for _, k := range t.s {
		if k == key {
			return
		}
	}

	t.s = append(t.s, key)
}

// Contains checks if the key exists in the set
func (t *sliceset) Contains(key string) bool {

	for _, k := range t.s {
		if k == key {
			return true
		}
	}

	return false
}

// Remove removes the key from the set
func (t *sliceset) Remove(key string) {

	for i, k := range t.s {

		if k == key {

			last := len(t.s) - 1

			if last >= 0 {
				t.s[i] = t.s[last]
			}

			t.s = t.s[:last]
			return
		}
	}

	return
}

// Keys returns the set of keys
func (t *sliceset) Keys() []string {

	m := make([]string, 0, len(t.s))

	for _, k := range t.s {
		m = append(m, k)
	}

	return m
}

// Equals compares the set against another set
func (t *sliceset) Equals(s Set) bool {

	if s.Count() != t.Count() {
		return false
	}

	for _, k := range s.Keys() {
		if !t.Contains(k) {
			return false
		}
	}

	return true
}

// Subset checks if this set is a subset of another
func (t *sliceset) Subset(s Set) bool {

	if s.Count() < t.Count() {
		return false
	}

	for _, k := range t.Keys() {
		if !s.Contains(k) {
			return false
		}
	}

	return true
}

// Superset checks if this set is a superset of another
func (t *sliceset) Superset(s Set) bool {

	if s.Count() > t.Count() {
		return false
	}

	for _, k := range s.Keys() {
		if !t.Contains(k) {
			return false
		}
	}

	return true
}
