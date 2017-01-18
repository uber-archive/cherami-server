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

package distance

import (
	"errors"
	"math/rand"

	"github.com/uber/cherami-server/common/configure"

	"github.com/uber-common/bark"
)

// A typical reasonable topology yaml config should consist of a entity tree
// with 3 levels: DC->rack->host. Nothing precludes declaring arbitrarily more
// levels, i.e. DC->pod->rack->host->ip.
//
// The algorithm for finding the distance between 2 resources is O(log(n)) where
// the depth of the entity tree is roughly around 3~5 for a typical topology.
// This logic uses O(n^2) space, but can be improved to O(n) by implementing a
// bloom filter.
//
// The algorithm to find m resources satisfying the [minDistance, maxDistance)
// requirements is O((m*log(n))^2) where m is 3 in order to find placement for 3
// replicas with minimum distance 2 to avoid positioning on the same rack, etc.

const (
	// ZeroDistance denotes the same resource.
	ZeroDistance = 0

	// InfiniteDistance denotes inifinite distance.
	InfiniteDistance = ^uint16(0)
)

// Map parses a yaml config containing a nested entity tree topology, describing
// distances between resources. A typical entity tree contains a collection of
// datacenter entities at the top layer, nested with collections of entities
// depicting racks at the next layer, followed by hosts. However, the number of
// levels can be arbitrary. See dist_test.go for additional examples.
type Map map[string]*entity

var (
	errSource = errors.New("Cannot find source")
	errTarget = errors.New("Cannot find target")
)

type override struct {
	source *entity

	Target   string `yaml:"target"`
	Distance uint16 `yaml:"distance"`
}

type entity struct {
	parent      *entity
	descendants map[string]struct{} // TODO: replace with bloom filter

	Resource  string      `yaml:"resource"`
	Type      string      `yaml:"type"`
	Distance  uint16      `yaml:"distance"`
	Entities  []*entity   `yaml:"entities"`
	Overrides []*override `yaml:"overrides"`
}

// New loads the distance map from the config file.
func New(path string, log bark.Logger) (m Map, err error) {
	var e entity
	if err = configure.NewCommonConfigure().LoadFile(&e, path); err == nil {
		m = make(Map)
		m.init(&e, log)
	}

	return
}

// Recursively initialize the distance map.
func (m Map) init(e *entity, log bark.Logger) (resources []string) {
	m[e.Resource] = e
	for _, f := range e.Entities {
		if f.Distance > e.Distance {
			log.WithFields(bark.Fields{"entity": f.Resource, "parent": e.Resource}).Fatal("Distance cannot be greater than its parent")
		}
		f.parent = e
		resources = append(resources, m.init(f, log)...)
	}

	e.descendants = make(map[string]struct{})
	for _, r := range resources {
		e.descendants[r] = struct{}{}
	}
	resources = append(resources, e.Resource)

	return
}

// Recursively compute the distance between source and target.
func (m Map) computeDistance(sourceEntity, targetEntity *entity, distance uint16, visitedResources map[string]struct{}) uint16 {
	// The following logic traverses the tree by finding the lowest common
	// ancestor. The overrides take precesdence over the parents.

	// Traverse override entries.
	for _, o := range sourceEntity.Overrides {
		if o.Target == targetEntity.Resource {
			return o.Distance
		}
		if _, ok := visitedResources[o.Target]; !ok {
			// Memoize visited resources.
			visitedResources[o.Target] = struct{}{}
			if e, okB := m[o.Target]; okB {
				if _, ok = e.descendants[targetEntity.Resource]; ok {
					return o.Distance
				}
			}
			delete(visitedResources, o.Target)
		}
	}

	// Traverse sibling entries.
	for _, e := range sourceEntity.parent.Entities {
		if e == targetEntity {
			return sourceEntity.parent.Distance
		}
		if _, ok := visitedResources[e.Resource]; !ok {
			// Memoize visited resources.
			visitedResources[e.Resource] = struct{}{}
			if _, ok = e.descendants[targetEntity.Resource]; ok {
				return sourceEntity.parent.Distance
			}
			delete(visitedResources, e.Resource)
		}
	}

	// Traverse parent entry.
	visitedResources[sourceEntity.parent.Resource] = struct{}{}
	return m.computeDistance(sourceEntity.parent, targetEntity, distance, visitedResources)
}

// Helper to lookup the forward and reverse distances.
func (m Map) lookupDistance(sourceEntity, targetEntity *entity) (forwardDistance, reverseDistance uint16) {
	if _, ok := sourceEntity.descendants[targetEntity.Resource]; !ok {
		if _, okB := targetEntity.descendants[sourceEntity.Resource]; !okB {
			forwardDistance = m.computeDistance(sourceEntity, targetEntity, ZeroDistance, map[string]struct{}{sourceEntity.Resource: {}})
			reverseDistance = m.computeDistance(targetEntity, sourceEntity, ZeroDistance, map[string]struct{}{targetEntity.Resource: {}})
			return
		}
	}

	return ZeroDistance, ZeroDistance
}

// FindDistance finds the forward and reverse distances.
func (m Map) FindDistance(sourceResource, targetResource string) (forwardDistance, reverseDistance uint16, err error) {
	if s, ok := m[sourceResource]; !ok {
		return InfiniteDistance, InfiniteDistance, errSource
	} else if t, okB := m[targetResource]; !okB {
		return InfiniteDistance, InfiniteDistance, errTarget
	} else {
		forwardDistance, reverseDistance = m.lookupDistance(s, t)
		return
	}
}

// Recursively traverse the tree to find a target resource which satisfies the
// [minDistance, maxDistance) requirements.
func (m Map) locateResource(sourceEntities []*entity, targetType string, minDistance, maxDistance uint16, currentEntity *entity, visitedResources map[string]struct{}, whitelistedResources map[string]struct{}) (*entity, error) {
	if currentEntity.Type == targetType {
		if _, ok := whitelistedResources[currentEntity.Resource]; ok || len(whitelistedResources) == 0 {
			// Check if the entity falls within the distance requirements from all of
			// the source resources.
			withinDistance := true
			for _, e := range sourceEntities {
				forwardDistance, reverseDistance := m.lookupDistance(currentEntity, e)
				if !(forwardDistance >= minDistance && forwardDistance < maxDistance) || !(reverseDistance >= minDistance && reverseDistance < maxDistance) {
					withinDistance = false
					break
				}
			}
			if withinDistance {
				// Memoize visited resources.
				visitedResources[currentEntity.Resource] = struct{}{}
				return currentEntity, nil
			}
		}
	} else if cnt := len(currentEntity.Entities); cnt > 0 {
		// Traverse descendant entities.
		eA := currentEntity.Entities[:rand.Intn(cnt)]
		eB := currentEntity.Entities[len(eA):]
		for _, eX := range [][]*entity{eB, eA} {
			for _, e := range eX {
				if _, ok := visitedResources[e.Resource]; !ok {
					if t, err := m.locateResource(sourceEntities, targetType, minDistance, maxDistance, e, visitedResources, whitelistedResources); err == nil {
						return t, nil
					}
				}
			}
		}
	}

	// Traverse override entities.
	for _, o := range currentEntity.Overrides {
		if o.Distance < maxDistance {
			if _, ok := visitedResources[o.Target]; !ok {
				if e, okB := m[o.Target]; okB {
					if t, err := m.locateResource(sourceEntities, targetType, minDistance, maxDistance, e, visitedResources, whitelistedResources); err == nil {
						return t, nil
					}
				}
			}
		}
	}

	// Traverse parent entity.
	if currentEntity.parent != nil && currentEntity.parent.Distance < maxDistance {
		if _, ok := visitedResources[currentEntity.parent.Resource]; !ok {
			// Memoize visited resources.
			visitedResources[currentEntity.Resource] = struct{}{}
			return m.locateResource(sourceEntities, targetType, minDistance, maxDistance, currentEntity.parent, visitedResources, whitelistedResources)
		}
	}

	return nil, errTarget
}

// FindResources finds a set of target resources of specified type with distance
// satisfying [minDistance, maxDistance) from each of the sources as well as
// between each target.
func (m Map) FindResources(poolResources, sourceResources []string, targetType string, count int, minDistance, maxDistance uint16) (targetResources []string, err error) {
	var sourceEntities []*entity
	var currentEntity *entity
	visitedResources := make(map[string]struct{})
	whitelistedResources := make(map[string]struct{})

	for _, r := range poolResources {
		if _, ok := m[r]; ok {
			whitelistedResources[r] = struct{}{}
		}
	}

	for _, s := range sourceResources {
		if e, ok := m[s]; ok {
			if e.parent != nil {
				sourceEntities = append(sourceEntities, e)
				if e.Type == targetType {
					// Memoize resources to skip.
					visitedResources[e.Resource] = struct{}{}
				}
			}
		} else {
			return nil, errSource
		}
	}

	if len(sourceEntities) > 0 {
		// Pick the random entity to start with.
		currentEntity = sourceEntities[rand.Intn(len(sourceEntities))]
		if currentEntity.Type == targetType {
			currentEntity = currentEntity.parent
		}
	} else {
		// No source resource supplied. Use the root as the source resource. Pick a
		// random entry and traverse its parent all the way up to the root.
		for _, e := range m {
			if count == 1 {
				// If only asking for one resource, then rely on fast pass by iterating
				// over the distance map.
				if e.Type != targetType {
					continue
				} else if _, ok := whitelistedResources[e.Resource]; ok || len(whitelistedResources) == 0 {
					return []string{e.Resource}, nil
				}
			}

			for e.parent != nil && e.Type != targetType {
				e = e.parent
			}
			if e.Type == targetType {
				if count == 1 {
					if _, ok := whitelistedResources[e.Resource]; ok || len(whitelistedResources) == 0 {
						return []string{e.Resource}, nil
					}
				}
				return nil, errTarget
			}
			currentEntity = e
			break
		}
	}

	for i := 0; i < count; i++ {
		if t, e := m.locateResource(sourceEntities, targetType, minDistance, maxDistance, currentEntity, visitedResources, whitelistedResources); e == nil {
			sourceEntities = append(sourceEntities, t)
			targetResources = append(targetResources, t.Resource)
			currentEntity = t.parent
		} else {
			err = e
			return
		}
	}

	return
}
