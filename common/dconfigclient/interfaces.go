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

type (
	// Handler is user supplied handler functions, which will be
	// called back when the dynamic config updates.
	Handler func(newConfig interface{})

	// Verifier interface user supplied verifier functions to verify the value when updates
	Verifier func(oldValue interface{}, newValue interface{}) bool

	// Client is the interface used to manager dynamic config
	Client interface {
		// AddHandlers register all the handler for the dynamic config values
		AddHandlers(handlerMap map[string]Handler)

		// AddVerifiers register all the handler for the dynamic config values
		AddVerifiers(verifierMap map[string]Verifier)

		// Refresh the dynamic config client
		Refresh()

		// StartBackGroundRefresh start the back fround refresh for dynamic config
		StartBackGroundRefresh()

		GetOrDefault(fieldName string, defaultValue interface{}) interface{}
	}
)
