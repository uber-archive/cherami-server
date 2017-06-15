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

package configure

import (
	"io/ioutil"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"github.com/uber-common/bark"
)

// logHasBeenSetup ensures that log.Configure is called exactly once, to prevent the  race detector from triggering
var logHasBeenSetup int32

// LogConfiguration holds the log related config
type LogConfiguration struct {
	logger *logrus.Logger
	Level  string `yaml:"level"`
	Stdout bool   `yaml:"stdout"`
}

// NewCommonLogConfig instantiates the log config
func NewCommonLogConfig() *LogConfiguration {
	return &LogConfiguration{
		logger: logrus.StandardLogger(),
	}
}

// Configure configures the log configuration
func (r *LogConfiguration) Configure(cfg interface{}) {
	if atomic.CompareAndSwapInt32(&logHasBeenSetup, 0, 1) {
		// ParseLevel takes a string level and returns the Logrus log level constant.
		l, err := logrus.ParseLevel(r.Level)

		if err != nil {
			return
		}
		logrus.SetLevel(l)

		if !r.Stdout {
			logrus.SetOutput(ioutil.Discard)
		}
	}
}

// GetDefaultLogger returns the default logger
func (r *LogConfiguration) GetDefaultLogger() bark.Logger {
	return bark.NewLoggerFromLogrus(r.logger)
}
