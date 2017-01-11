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

package frontendhost

import dconfig "github.com/uber/cherami-server/common/dconfigclient"

func (h *Frontend) registerInt() {
	// Add handler function for the dynamic config value
	handlerMap := make(map[string]dconfig.Handler)

	// nothing to be dynamically configured right now

	h.dClient.AddHandlers(handlerMap)
	// Add verify function for the dynamic config value
	verifierMap := make(map[string]dconfig.Verifier)
	h.dClient.AddVerifiers(verifierMap)
}

// LoadUconfig load the dynamic config values for key
func (h *Frontend) LoadUconfig() {
	// nothing to be loaded dynamically right now
}

// Manage do the work for uconfig
func (h *Frontend) dynamicConfigManage() {
	h.dClient.Refresh()
	h.LoadUconfig()
	h.registerInt()
	h.dClient.StartBackGroundRefresh()
}
