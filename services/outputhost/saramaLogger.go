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

package outputhost

import (
	"github.com/Shopify/sarama"
	"github.com/uber-common/bark"
	"strings"
)

type saramaLogger struct {
	l bark.Logger
}

// NewSaramaLoggerFromBark provides a logger suitable for Sarama from a given bark logger
func NewSaramaLoggerFromBark(l bark.Logger, module string) sarama.StdLogger {
	return &saramaLogger{
		l: l.WithField(`module`, module),
	}
}

// Print prints the objects given
func (l *saramaLogger) Print(v ...interface{}) {
	if isError(v...) {
		l.l.Error(trim(v)...)
		return
	}
	l.l.Info(trim(v)...)
}

// Printf prints the objects given with the specified format
func (l *saramaLogger) Printf(format string, v ...interface{}) {
	if isError(format) || isError(v...) {
		l.l.Errorf(strings.TrimSpace(format), v...)
		return
	}
	l.l.Infof(strings.TrimSpace(format), v...)
}

// Println is the same as Print
func (l *saramaLogger) Println(v ...interface{}) {
	l.Print(v...)
}

// Try to detect the level of the log line
func isError(v ...interface{}) bool {
	if len(v) == 0 {
		return false
	}
	if _, ok := v[0].(error); ok {
		return true
	}
	if s, ok := v[0].(string); ok {
		s = strings.ToLower(s)
		if strings.Contains(s, `error`) || strings.Contains(s, `fail`) {
			return true
		}
	}
	return false
}

func trim(v ...interface{}) []interface{} {
	if len(v) == 0 {
		return v
	}
	if s, ok := v[0].(string); ok {
		v[0] = strings.TrimSpace(s)
	}
	return v
}
