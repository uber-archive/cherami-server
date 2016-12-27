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

type (
	// customError represents any user
	// defined error from this package
	customError struct {
		msg string
	}
	// ConfigError represents any error
	// for the Config handler
	ConfigError struct {
		customError
	}
	// TaskError represents any error
	// thrown by a task within this pkg
	TaskError struct {
		customError
	}
	// NodeToolError represents any
	// error thrown by the NodeTool
	NodeToolError struct {
		customError
	}
)

// Error implements the required error interface
func (e *customError) Error() string {
	return e.msg
}

func newCustomError(msg string, err error) customError {
	if err != nil {
		msg = msg + ":" + err.Error()
	}
	return customError{msg: msg}
}

func newConfigError(msg string, err error) error {
	return &ConfigError{
		customError: newCustomError(msg, err),
	}
}

func newTaskError(msg string, err error) error {
	return &TaskError{
		customError: newCustomError(msg, err),
	}
}

func newNodeToolError(msg string, err error) error {
	return &NodeToolError{
		customError: newCustomError(msg, err),
	}
}
