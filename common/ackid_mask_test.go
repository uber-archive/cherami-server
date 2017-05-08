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

package common

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type AckIDSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
}

func TestAckIDSuite(t *testing.T) {
	suite.Run(t, new(AckIDSuite))
}

func (s *AckIDSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func testCreateBitMaskUtil(startFromBit uint64, numBits uint64) uint64 {
	return ((1 << numBits) - 1) << startFromBit
}

// testCreateBitMask creates gives the masks for the session, ackMgr and the seqNo
func testCreateBitMask() (uint64, uint64, uint64) {
	sessionMask := testCreateBitMaskUtil(maxBits-sessionIDNumBits, sessionIDNumBits)                 // sessionId is the top 16 bits - 48 to 63 [48, 64)
	ackMgrMask := testCreateBitMaskUtil(maxBits-(sessionIDNumBits+ackMgrIDNumBits), ackMgrIDNumBits) // ackMgrBitMask is from bits 32 to 47 [32, 48)
	indexMask := testCreateBitMaskUtil(0, indexNumBits)                                              // seqNoBitMask is from bits 0 to 32 [0, 32)

	return sessionMask, ackMgrMask, indexMask
}

// TestAckIDValidateBitMasks is a simple test to validate the masks
// we use based on the bits.
func (s *AckIDSuite) TestAckIDValidateBitMasks() {
	sMask, aMask, iMask := testCreateBitMask()
	s.Equal(sMask, sessionIDMask)
	s.Equal(aMask, ackMgrIDMask)
	s.Equal(iMask, indexMask)
}
