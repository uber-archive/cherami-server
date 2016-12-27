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

package cassandra

import (
	"errors"
	"net"
	"regexp"
	"strings"
	"time"
)

var (
	ipAddrRegexStr = "[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}"
	ipAddrRegex    = regexp.MustCompile("^" + ipAddrRegexStr + "$")
)

var errNoAddr = errors.New("No ipv4 eth0 addr found")

// findEth0Addr returns the eth0 ipv4 address, if found
func findEth0Addr() (string, error) {

	iface, err := net.InterfaceByName("eth0")
	if err != nil {
		return "", err
	}

	addrs, err := iface.Addrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		tokens := strings.Split(addr.String(), "/")
		if len(tokens) != 2 {
			continue
		}
		if ipAddrRegex.MatchString(tokens[0]) {
			return tokens[0], nil
		}
	}

	return "", errNoAddr
}

// isQuit is a helper that returns true if
// the given channel is closed and false
// otherwise
func isQuit(quitC <-chan struct{}) bool {
	select {
	case <-quitC:
		return true
	default:
		return false
	}
}

// sleep is a helper that sleeps for the
// speficied duration as long as the companion
// channel is not closed during that time
func sleep(quitC <-chan struct{}, d time.Duration) {
	select {
	case <-time.After(d):
		return
	case <-quitC:
		return
	}
}
