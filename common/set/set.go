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

// Set defines the interface for a set
type Set interface {
	// Clear clears the set
	Clear()
	// Empty checks if the set is empty
	Empty() bool
	// Count returns the size of the set
	Count() int
	// Insert adds a key to the set
	Insert(key string)
	// Contains checks if the key exists in the set
	Contains(key string) bool
	// Remove removes the key from the set
	Remove(key string)
	// Keys returns the set of keys
	Keys() []string
	// Equals compares the set against another set
	Equals(s Set) bool
	// Subset checks if this set is a subset of another
	Subset(s Set) bool
	// Superset checks if this set is a superset of another
	Superset(s Set) bool
}

// use a sliceset if 'cap' is greater than given constant
const sliceSetMax = 16

// New initializes a new set, picking the appropriate underlying
// implementation based on the specified expected capacity
func New(cap int) Set {

	// pick the underlying set implementation based on the specified expected
	// capacity; the thresholds are determined based on benchmarks.
	switch {
	case cap == 0:
		fallthrough

	case cap > sliceSetMax:
		return NewMapSet(cap)

	default:
		return NewSliceSet(cap)
	}
}

// NewConcurrent initializes a new concurrent set, picking the appropriate underlying
// implementation based on the specified expected capacity
func NewConcurrent(cap int) Set {

	return NewSyncSet(New(cap))
}
