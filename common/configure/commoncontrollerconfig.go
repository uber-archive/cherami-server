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

// ControllerConfig holds the config info related to our controller
type ControllerConfig struct {
	TopologyFile                     string `yaml:"TopologyFile"`
	MinInputToStoreDistance          uint16 `yaml:"MinInputToStoreDistance"`
	MaxInputToStoreDistance          uint16 `yaml:"MaxInputToStoreDistance"`
	MinInputToStoreFallbackDistance  uint16 `yaml:"MinInputToStoreFallbackDistance"`
	MaxInputToStoreFallbackDistance  uint16 `yaml:"MaxInputToStoreFallbackDistance"`
	MinOutputToStoreDistance         uint16 `yaml:"MinOutputToStoreDistance"`
	MaxOutputToStoreDistance         uint16 `yaml:"MaxOutputToStoreDistance"`
	MinOutputToStoreFallbackDistance uint16 `yaml:"MinOutputToStoreFallbackDistance"`
	MaxOutputToStoreFallbackDistance uint16 `yaml:"MaxOutputToStoreFallbackDistance"`
	MinStoreToStoreDistance          uint16 `yaml:"MinStoreToStoreDistance"`
	MaxStoreToStoreDistance          uint16 `yaml:"MaxStoreToStoreDistance"`
	MinStoreToStoreFallbackDistance  uint16 `yaml:"MinStoreToStoreFallbackDistance"`
	MaxStoreToStoreFallbackDistance  uint16 `yaml:"MaxStoreToStoreFallbackDistance"`
}

// NewCommonControllerConfig instantiates the controller config
func NewCommonControllerConfig() *ControllerConfig {
	return &ControllerConfig{}
}

// GetTopologyFile gets the topology file to be used for placement
func (r *ControllerConfig) GetTopologyFile() string {
	return r.TopologyFile
}

// GetMinInputToStoreDistance gets the minimum input<->store distance to use
func (r *ControllerConfig) GetMinInputToStoreDistance() uint16 {
	return r.MinInputToStoreDistance
}

// GetMaxInputToStoreDistance gets the maximum input<->store distance to use
func (r *ControllerConfig) GetMaxInputToStoreDistance() uint16 {
	return r.MaxInputToStoreDistance
}

// GetMinInputToStoreFallbackDistance gets the minimum fallback input<->store distance
func (r *ControllerConfig) GetMinInputToStoreFallbackDistance() uint16 {
	return r.MinInputToStoreFallbackDistance
}

// GetMaxInputToStoreFallbackDistance gets the maximum fallback input<->store distance
func (r *ControllerConfig) GetMaxInputToStoreFallbackDistance() uint16 {
	return r.MaxInputToStoreFallbackDistance
}

// GetMinOutputToStoreDistance gets the minimum output<->store distance to use
func (r *ControllerConfig) GetMinOutputToStoreDistance() uint16 {
	return r.MinOutputToStoreDistance
}

// GetMaxOutputToStoreDistance gets the maximum output<->store distance to use
func (r *ControllerConfig) GetMaxOutputToStoreDistance() uint16 {
	return r.MaxOutputToStoreDistance
}

// GetMinOutputToStoreFallbackDistance gets the minimum fallback output<->store distance
func (r *ControllerConfig) GetMinOutputToStoreFallbackDistance() uint16 {
	return r.MinOutputToStoreFallbackDistance
}

// GetMaxOutputToStoreFallbackDistance gets the maximum fallback output<->store distance
func (r *ControllerConfig) GetMaxOutputToStoreFallbackDistance() uint16 {
	return r.MaxOutputToStoreFallbackDistance
}

// GetMinStoreToStoreDistance gets the min store<->store distance
func (r *ControllerConfig) GetMinStoreToStoreDistance() uint16 {
	return r.MinStoreToStoreDistance
}

// GetMaxStoreToStoreDistance gets the max store<->store distance
func (r *ControllerConfig) GetMaxStoreToStoreDistance() uint16 {
	return r.MaxStoreToStoreDistance
}

// GetMinStoreToStoreFallbackDistance gets the minimum fallback store<->store distance
func (r *ControllerConfig) GetMinStoreToStoreFallbackDistance() uint16 {
	return r.MinStoreToStoreFallbackDistance
}

// GetMaxStoreToStoreFallbackDistance gets the maximum fallback store<->store distance
func (r *ControllerConfig) GetMaxStoreToStoreFallbackDistance() uint16 {
	return r.MaxStoreToStoreFallbackDistance
}
