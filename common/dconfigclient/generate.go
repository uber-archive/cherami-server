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

package dconfigclient

import (
	"fmt"
	"regexp"

	log "github.com/sirupsen/logrus"
)

// SetterInt is the setter function to generate the handler function
type SetterInt func(value int32)

// GetterInt is the function interface for handler dconfig value
type GetterInt func() int

// SetterString is the setter function to generate the handler function
type SetterString func(value string)

// GetterString is the function interface for handler dconfig value
type GetterString func() string

// GenerateIntHandler return a dconfig Handler for the dconfigKey
func GenerateIntHandler(dconfigKey string, setter SetterInt, getter GetterInt) Handler {
	handlerFunc := func(newConfig interface{}) {

		oldValue := getter()
		newValue, ok := newConfig.(int)
		if !ok {
			log.WithField("dconfigKey", dconfigKey).Error("Cannot get key from dconfig; just using the old value.")
		} else if oldValue == newValue {
			log.Info("oldValue is equal to newValue, do not need to update")
		} else {
			setter(int32(newValue))
		}
		log.WithFields(log.Fields{fmt.Sprintf("old%s", dconfigKey): oldValue, fmt.Sprintf("new%s", dconfigKey): getter()}).
			Info("new value vs old value for the dconfig key")
	}
	return handlerFunc
}

// GenerateIntMaxMinVerifier return a dconfig Verifier for the dconfigKey
func GenerateIntMaxMinVerifier(dconfigKey string, minV int, maxV int) Verifier {
	verifierFunc := func(_ interface{}, newV interface{}) bool {
		newValue, okNew := newV.(int)
		if !okNew {
			log.WithFields(log.Fields{"dconfigKey": dconfigKey, "newValue": fmt.Sprintf("%v", newV)}).Error("type-assertion to int failed for the dconfig key")
			return false
		}
		if newValue < minV && newValue > maxV {
			log.WithFields(log.Fields{"dconfigKey": dconfigKey, "newValue": fmt.Sprintf("%v", newValue), "min": fmt.Sprintf("%v", minV), "max": fmt.Sprintf("%v", maxV)}).Error("verify for the dconfigKey failed; new value doesn't fall between min and max")
			return false
		}
		return true
	}
	return verifierFunc
}

// GenerateStringRegexpVerifier return a dconfig Verifier for the dconfigKey
func GenerateStringRegexpVerifier(dconfigKey string, r *regexp.Regexp) Verifier {
	verifierFunc := func(_ interface{}, newV interface{}) bool {
		newValue, okNew := newV.(string)
		if !okNew {
			log.WithFields(log.Fields{"dconfigKey": dconfigKey, "newValue": fmt.Sprintf("%v", newV)}).Error("type-assertion to string failed for the dconfig key")
			return false
		}
		if !r.MatchString(newValue) {
			log.WithFields(log.Fields{"dconfigKey": dconfigKey, "newValue": fmt.Sprintf("%v", newValue), "regexp": r.String()}).Error("verify for the dconfigKey failed; new value doesn't match regular expression")
			return false
		}
		return true
	}
	return verifierFunc
}

// GenerateStringHandler return a dconfig Handler for the dconfigKey
func GenerateStringHandler(dconfigKey string, setter SetterString, getter GetterString) Handler {
	handlerFunc := func(newConfig interface{}) {

		oldValue := getter()
		newValue, ok := newConfig.(string)
		if !ok {
			log.WithField("dconfigKey", dconfigKey).Error("Cannot get key from dconfig; just using the old value.")
		} else if oldValue == newValue {
			log.Info("oldValue is equal to newValue, do not need to update")
		} else {
			setter(string(newValue))
		}
		log.WithFields(log.Fields{fmt.Sprintf("old%s", dconfigKey): oldValue, fmt.Sprintf("new%s", dconfigKey): getter()}).
			Info("new value vs old value for the dconfig key")
	}
	return handlerFunc
}
