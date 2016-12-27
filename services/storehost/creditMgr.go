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

package storehost

// this is a very naive implementation of a credit manager that
// supports credit-based flow control. this needs to be made more
// sophisticated eventually.

// currently, this gives out '5000' initial credits. every time
// credits are 'return'-ed on a credit-line, it is saved; and
// any/all saved credits are given out when 'borrow'.

const initialCredits = 5000

type (
	// CreditMgr defines the credit-manager interface
	CreditMgr interface {
		NewCreditLine() CreditLine
		Close() // close credit mgr
	}

	// CreditLine defines the credit-line interface
	CreditLine interface {
		Borrow() (credits int32)
		Return(credits int32)
		Close()
	}
)

type (
	creditMgr struct {
		total int32 // total credits
	}

	creditLine struct {
		credits int32
	}
)

// NewCreditMgr returns an instance of CreditMgr
func NewCreditMgr(totalCredits int32) CreditMgr {
	return &creditMgr{total: totalCredits}
}

func (t *creditMgr) Close() {
}

func (t *creditMgr) NewCreditLine() CreditLine {

	// FIXME: give initial credits
	return &creditLine{credits: initialCredits}
}

func (t *creditMgr) recomputeCredits() {
}

func (t *creditLine) Borrow() (credits int32) {

	// give back any credits we have got back on this credit-line
	c := t.credits
	t.credits = 0
	return c
}

func (t *creditLine) Return(credits int32) {

	// add credits back to credit-line
	t.credits += credits
}

func (t *creditLine) Close() {
}
