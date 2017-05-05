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

type mapset struct {
	m map[string]struct{}
}

// NewMapSet initializes a map-based set
func NewMapSet(cap int) Set {

	return &mapset{
		m: make(map[string]struct{}),
	}
}

// Clear clears the set
func (t *mapset) Clear() {
	t.m = make(map[string]struct{})
}

// Empty checks if the set is empty
func (t *mapset) Empty() bool {
	return len(t.m) == 0
}

// Count returns the size of the set
func (t *mapset) Count() int {
	return len(t.m)
}

// Insert adds a key to the set
func (t *mapset) Insert(key string) {
	t.m[key] = struct{}{}
}

// Contains checks if the key exists in the set
func (t *mapset) Contains(key string) bool {
	_, ok := t.m[key]
	return ok
}

// Remove removes the key from the set
func (t *mapset) Remove(key string) {
	delete(t.m, key)
}

// Keys returns the set of keys
func (t *mapset) Keys() []string {

	m := make([]string, 0, len(t.m))

	for k := range t.m {
		m = append(m, k)
	}

	return m
}

// Equals compares the set against another set
func (t *mapset) Equals(s Set) bool {

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
func (t *mapset) Subset(s Set) bool {

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
func (t *mapset) Superset(s Set) bool {

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
