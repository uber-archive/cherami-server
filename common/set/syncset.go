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

import "sync"

type syncset struct {
	sync.RWMutex
	set Set
}

// NewSyncSet initializes a map-based set
func NewSyncSet(set Set) Set {

	return &syncset{
		set: set,
	}
}

// Clear clears the set
func (t *syncset) Clear() {

	t.Lock()
	t.set.Clear()
	t.Unlock()
}

// Empty checks if the set is empty
func (t *syncset) Empty() bool {

	t.RLock()
	defer t.RUnlock()

	return t.set.Empty()
}

// Count returns the size of the set
func (t *syncset) Count() int {

	t.RLock()
	defer t.RUnlock()

	return t.set.Count()
}

// Insert adds a key to the set
func (t *syncset) Insert(key string) {

	t.Lock()
	t.set.Insert(key)
	t.Unlock()
}

// Contains checks if the key exists in the set
func (t *syncset) Contains(key string) bool {

	t.RLock()
	defer t.RUnlock()

	return t.set.Contains(key)
}

// Remove removes the key from the set
func (t *syncset) Remove(key string) {

	t.Lock()
	t.set.Remove(key)
	t.Unlock()
}

// Keys returns the set of keys
func (t *syncset) Keys() []string {

	t.RLock()
	defer t.RUnlock()

	return t.set.Keys()
}

// Equals compares the set against another set
func (t *syncset) Equals(s Set) bool {

	t.RLock()
	defer t.RUnlock()

	if s.Count() != t.set.Count() {
		return false
	}

	for _, k := range s.Keys() {
		if !t.set.Contains(k) {
			return false
		}
	}

	return true
}

// Subset checks if this set is a subset of another
func (t *syncset) Subset(s Set) bool {

	t.RLock()
	defer t.RUnlock()

	if s.Count() < t.set.Count() {
		return false
	}

	for _, k := range t.set.Keys() {
		if !s.Contains(k) {
			return false
		}
	}

	return true
}

// Superset checks if this set is a superset of another
func (t *syncset) Superset(s Set) bool {

	t.RLock()
	defer t.RUnlock()

	if s.Count() > t.set.Count() {
		return false
	}

	for _, k := range s.Keys() {
		if !t.set.Contains(k) {
			return false
		}
	}

	return true
}
