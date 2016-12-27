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
	"github.com/uber/cherami-server/common/configure"
)

// DconfigClient is the struct which implements the ClientInt interface
type DconfigClient struct {
	client struct{}
}

// NewDconfigClient return the dconfig client for the service
func NewDconfigClient(scfg configure.CommonServiceConfig, serviceName string) Client {
	clientImpl := &DconfigClient{}
	return clientImpl
}

// GetClient return the config client
func (uf *DconfigClient) GetClient() interface{} {
	return uf.client
}

// AddHandlers register all the handler for the dynamic config values
func (uf *DconfigClient) AddHandlers(handlerMap map[string]Handler) {
	//pass
}

// AddVerifiers register all the handler for the dynamic config values
func (uf *DconfigClient) AddVerifiers(verifierMap map[string]Verifier) {
	//pass
}

// Refresh refresh the dynamic config client
func (uf *DconfigClient) Refresh() {
	// pass
}

// StartBackGroundRefresh start the automatical refresh for dynamic config
func (uf *DconfigClient) StartBackGroundRefresh() {
	// pass
}

// GetOrDefault implements the corresponding method
func (uf *DconfigClient) GetOrDefault(fieldName string, defaultValue interface{}) interface{} {
	// pass
	return defaultValue
}
