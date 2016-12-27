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

package replicator

import (
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
)

const (
	ukeyAuthoritativeZone = "replicator.AuthoritativeZone"
)

func (r *Replicator) registerUconfig() {
	handlerMap := make(map[string]dconfig.Handler)
	handlerMap[ukeyAuthoritativeZone] = dconfig.GenerateStringHandler(ukeyAuthoritativeZone, r.setAuthoritativeZone, r.getAuthoritativeZone)
	r.uconfigClient.AddHandlers(handlerMap)
}

func (r *Replicator) loadUconfig() {
	valueUcfg, ok := r.uconfigClient.GetOrDefault(ukeyAuthoritativeZone, r.defaultAuthoritativeZone).(string)
	if ok {
		r.setAuthoritativeZone(string(valueUcfg))
		r.logger.WithField(ukeyAuthoritativeZone, valueUcfg).Info(`Update the uconfig value`)
	} else {
		r.logger.WithField(ukeyAuthoritativeZone, valueUcfg).Error(`Cannot get value from uconfig`)
	}
}

func (r *Replicator) dynamicConfigManage() {
	r.uconfigClient.Refresh()
	r.loadUconfig()
	r.registerUconfig()
	r.uconfigClient.StartBackGroundRefresh()
}
