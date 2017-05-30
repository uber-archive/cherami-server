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

package inputhost

import (
	"github.com/uber/cherami-server/common"
	dconfig "github.com/uber/cherami-server/common/dconfigclient"
)

const (
	// UkeyHostOverall is uconfig key for HostOverallConnLimit
	UkeyHostOverall = "inputhost.HostOverallConnLimit"
	// UkeyHostPerSec is uconfig key for HostPerSecondConnLimit
	UkeyHostPerSec = "inputhost.HostPerSecondConnLimit"
	// UkeyMaxConnPerDest is uconfig key for HostMaxConnPerDestination
	UkeyMaxConnPerDest = "inputhost.HostMaxConnPerDestination"
	// UkeyExtMsgs is the uconfig key for HostPerExtentMsgsLimitPerSecond
	UkeyExtMsgs = "inputhost.HostPerExtentMsgsLimitPerSecond"
	// UkeyConnMsgs is the uconfig key for HostPerConnMsgsLimitPerSecond
	UkeyConnMsgs = "inputhost.HostPerConnMsgsLimitPerSecond"
	// UkeyTestShortExts is the uconfig key for TestShortExtentsByPath
	UkeyTestShortExts = "inputhost.TestShortExtentsByPath"
)

func (h *InputHost) registerDConfig() {
	// Add handler function for the dynamic config value
	handlerMap := make(map[string]dconfig.Handler)
	handlerMap[UkeyHostOverall] = dconfig.GenerateIntHandler(UkeyHostOverall, h.SetHostConnLimit, h.GetHostConnLimitOverall)
	handlerMap[UkeyHostPerSec] = dconfig.GenerateIntHandler(UkeyHostPerSec, h.SetHostConnLimitPerSecond, h.GetHostConnLimitPerSecond)
	handlerMap[UkeyMaxConnPerDest] = dconfig.GenerateIntHandler(UkeyMaxConnPerDest, h.SetMaxConnPerDest, h.GetMaxConnPerDest)
	handlerMap[UkeyExtMsgs] = dconfig.GenerateIntHandler(UkeyExtMsgs, h.SetExtMsgsLimitPerSecond, h.GetExtMsgsLimitPerSecond)
	handlerMap[UkeyConnMsgs] = dconfig.GenerateIntHandler(UkeyConnMsgs, h.SetConnMsgsLimitPerSecond, h.GetConnMsgsLimitPerSecond)
	handlerMap[UkeyTestShortExts] = dconfig.GenerateStringHandler(UkeyTestShortExts, h.SetTestShortExtentsByPath, h.GetTestShortExtentsByPath)
	h.dConfigClient.AddHandlers(handlerMap)
	// Add verify function for the dynamic config value
	verifierMap := make(map[string]dconfig.Verifier)
	verifierMap[UkeyHostOverall] = dconfig.GenerateIntMaxMinVerifier(UkeyHostOverall, 1, common.MaxHostOverallConn)
	verifierMap[UkeyHostPerSec] = dconfig.GenerateIntMaxMinVerifier(UkeyHostPerSec, 1, common.MaxHostPerSecondConn)
	verifierMap[UkeyMaxConnPerDest] = dconfig.GenerateIntMaxMinVerifier(UkeyMaxConnPerDest, 1, common.MaxHostMaxConnPerDestination)
	verifierMap[UkeyExtMsgs] = dconfig.GenerateIntMaxMinVerifier(UkeyExtMsgs, 1, common.MaxHostPerExtentMsgsLimitPerSecond)
	verifierMap[UkeyConnMsgs] = dconfig.GenerateIntMaxMinVerifier(UkeyConnMsgs, 1, common.MaxHostPerConnMsgsLimitPerSecond)
	verifierMap[UkeyTestShortExts] = dconfig.GenerateStringRegexpVerifier(UkeyTestShortExts, common.OverrideValueByPrefixJoinedValidatorRegexp)
	h.dConfigClient.AddVerifiers(verifierMap)
}

// uconfigManage do the work for uconfig
func (h *InputHost) dynamicConfigManage() {
	h.registerDConfig()
	h.dConfigClient.Refresh()
	h.dConfigClient.StartBackGroundRefresh()
}
