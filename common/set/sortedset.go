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
	"sort"
)

type sortedset struct {
	s []string
}

// NewSortedSet initializes a sorted-slice based set
func NewSortedSet(cap int) Set {

	return &sortedset{
		s: make([]string, 0),
	}
}

// Clear clears the set
func (t *sortedset) Clear() {
	t.s = make([]string, 0)
}

// Empty checks if the set is empty
func (t *sortedset) Empty() bool {
	return len(t.s) == 0
}

// Count returns the size of the set
func (t *sortedset) Count() int {
	return len(t.s)
}

// Insert adds a key to the set
func (t *sortedset) Insert(key string) {

	i := sort.SearchStrings(t.s, key)

	if i < len(t.s) && t.s[i] == key {
		return // already contains key
	}

	// make space and insert at index 'i'
	t.s = append(t.s, "")
	copy(t.s[i+1:], t.s[i:])
	t.s[i] = key
}

// Contains checks if the key exists in the set
func (t *sortedset) Contains(key string) bool {

	i := sort.SearchStrings(t.s, key)
	return i < len(t.s) && t.s[i] == key
}

// Remove removes the key from the set
func (t *sortedset) Remove(key string) {

	i := sort.SearchStrings(t.s, key)

	if i == len(t.s) && t.s[i] != key {
		return // does not contain key
	}

	copy(t.s[i:], t.s[i+1:])
	t.s = t.s[:len(t.s)-1]
}

// Keys returns the set of keys
func (t *sortedset) Keys() []string {

	m := make([]string, 0, len(t.s))

	for _, k := range t.s {
		m = append(m, k)
	}

	return m
}

// Equals compares the set against another set
func (t *sortedset) Equals(s Set) bool {

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
func (t *sortedset) Subset(s Set) bool {

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
func (t *sortedset) Superset(s Set) bool {

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
