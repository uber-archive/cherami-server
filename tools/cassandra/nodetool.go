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
	"bytes"
	"io"
	"net"
	"os/exec"
	"strings"
)

type (
	// NodeTool is an interface that Cassandra's
	// node tool wrappers must adhere to
	NodeTool interface {
		// CreateSnapshot captures a cassandra snapshot
		CreateSnapshot(name string, keyspace string) error
		// ClearSnapshot clears a specific cassandra snapshot
		ClearSnapshot(name string) error
		// ClearAllSnapshots clears all cassandra snapshots
		ClearAllSnapshots() error
		// DescKeyspace describes a given keyspace
		DescKeyspace(keyspace string) (string, error)
		// GetPeers returns the cassandra peers
		GetPeers() ([]string, error)
	}

	// nodeToolImpl is an implementatino
	// for NodeTool
	nodeToolImpl struct {
		serverName string
	}
)

const (
	cqlshBin    = "cqlsh"
	nodeToolBin = "nodetool"
)

// NewNodeTool returns a node too wrapper
func NewNodeTool(serverName string) NodeTool {
	return &nodeToolImpl{serverName: serverName}
}

// CreateSnapshot captures a cassandra snapshot
func (nt *nodeToolImpl) CreateSnapshot(name string, keyspace string) error {
	cmd := exec.Command(nodeToolBin, "snapshot", "-t", name, keyspace)
	ans, err := cmd.CombinedOutput()
	if err != nil {
		return newNodeToolError(string(ans), err)
	}
	return nil
}

// ClearSnapshot clears a specific cassandra snapshot
func (nt *nodeToolImpl) ClearSnapshot(name string) error {
	cmd := exec.Command(nodeToolBin, "clearsnapshot", name)
	ans, err := cmd.CombinedOutput()
	if err != nil {
		return newNodeToolError(string(ans), err)
	}
	return nil
}

// ClearAllSnapshots clears all cassandra snapshots
func (nt *nodeToolImpl) ClearAllSnapshots() error {
	cmd := exec.Command(nodeToolBin, "clearsnapshot")
	ans, err := cmd.CombinedOutput()
	if err != nil {
		return newNodeToolError(string(ans), err)
	}
	return nil
}

// GetPeers returns the cassandra peers
// This method also adds self to the list
func (nt *nodeToolImpl) GetPeers() ([]string, error) {

	cmd := exec.Command(cqlshBin, nt.serverName, "-e", "select peer from system.peers")
	stdout, err := cmd.StdoutPipe()
	stdoutBuff := new(bytes.Buffer)

	if err != nil {
		return nil, newNodeToolError("cqlsh exec failed", err)
	}
	if err = cmd.Start(); err != nil {
		return nil, newNodeToolError("cqlsh exec failed", err)
	}

	io.Copy(stdoutBuff, stdout)

	if err = cmd.Wait(); err != nil {
		return nil, newNodeToolError("cqlsh exec failed", err)
	}

	result := make([]string, 0, 4)

	cmd = exec.Command("grep", "-E", ipAddrRegexStr)
	cmd.Stdin = stdoutBuff
	ans, err := cmd.CombinedOutput()
	if err == nil {
		result = addrsToNames(string(ans), result)
	}

	if ipAddrRegex.MatchString(nt.serverName) {
		myName, err1 := net.LookupAddr(nt.serverName)
		if err1 == nil {
			result = append(result, myName[0])
		}
	} else {
		result = append(result, nt.serverName)
	}

	return result, nil
}

// DescKeyspace describes a given keyspace
func (nt *nodeToolImpl) DescKeyspace(keyspace string) (string, error) {
	cmd := exec.Command(cqlshBin, nt.serverName, "-e", "DESC "+keyspace)
	ans, err := cmd.CombinedOutput()
	if err != nil {
		return "", newNodeToolError("cqlsh exec failed", err)
	}
	return string(ans), nil
}

func addrsToNames(addrs string, result []string) []string {
	for _, addr := range strings.Split(addrs, "\n") {
		trimmed := strings.TrimSpace(addr)
		if len(trimmed) < 1 {
			continue
		}
		names, e := net.LookupAddr(trimmed)
		if e != nil || len(names) < 1 {
			// if we cannot resolve, just use the addr
			result = append(result, trimmed)
		}
		result = append(result, names[0])
	}
	return result
}
