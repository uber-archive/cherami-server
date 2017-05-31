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

package common

import (
	"regexp"
	"sync"

	"github.com/uber/cherami-thrift/.generated/go/metadata"
)

type (
	// UUIDResolver maps UUIDs to IP addrs and vice-versa
	UUIDResolver interface {
		// Lookup returns the host addr corresponding to the uuid
		Lookup(uuid string) (string, error)
		// Reverse lookup returns the uuid corresponding to the host addr
		ReverseLookup(addr string) (string, error)
		// Clears the in-memory cache
		ClearCache()
	}

	// resolverImpl is an implementation of UUIDResolver that uses
	// cassandra as the underlying mapping store.
	resolverImpl struct {
		rwLock  sync.RWMutex
		cache   map[string]string
		mClient metadata.TChanMetadataService
	}
)

// NewUUIDResolver returns an instance of UUIDResolver
// that can be used to resovle host uuids to ip:port addresses
// and vice-versa. The returned resolver uses Cassandra as the backend
// store for persisting the mapping.  The resolver also
// maintains an in-memory cache for fast-lookups. Thread safe.
func NewUUIDResolver(mClient metadata.TChanMetadataService) UUIDResolver {
	instance := &resolverImpl{
		mClient: mClient,
		cache:   make(map[string]string),
	}
	return instance
}

// Resolve resolves the given uuid to a hostid
// On success, returns the host:port for the uuid
// On failure, error is returned
func (r *resolverImpl) Lookup(uuid string) (string, error) {

	if addr, ok := r.cacheGet(uuid); ok {
		return addr, nil
	}

	addr, err := r.mClient.UUIDToHostAddr(nil, uuid)
	if err == nil && len(addr) > 0 {
		r.cachePut(uuid, addr)
		r.cachePut(addr, uuid)
		return addr, nil
	}

	return "", err
}

// Resolve resolves the given addr to a uuid
// On success, returns the uuid for the addr
// On failure, error is returned
func (r *resolverImpl) ReverseLookup(addr string) (string, error) {

	if uuid, ok := r.cacheGet(addr); ok {
		return uuid, nil
	}

	uuid, err := r.mClient.HostAddrToUUID(nil, addr)
	if err == nil && len(uuid) > 0 {
		r.cachePut(uuid, addr)
		r.cachePut(addr, uuid)
		return uuid, nil
	}

	return "", err
}

// Clear caches clears the in-memory resolver cache
func (r *resolverImpl) ClearCache() {
	r.rwLock.Lock()
	defer r.rwLock.Unlock()
	r.cache = make(map[string]string)
}

func (r *resolverImpl) cacheGet(key string) (string, bool) {
	r.rwLock.RLock()
	defer r.rwLock.RUnlock()
	v, ok := r.cache[key]
	return v, ok
}

func (r *resolverImpl) cachePut(key string, value string) {
	r.rwLock.Lock()
	defer r.rwLock.Unlock()
	r.cache[key] = value
}

// Paths and consumer groups are of the form "/foo.bar/bax". Although we don't
// currently support "folders", relative paths, or other filesystem-like
// operations, it is best to enforce this style of naming up front in case we would
// like to in the future. We don't allow our clients to encroach directly on the
// root, so that destinations and consumer groups are at least grouped under a team
// or project name. We also require these names to have one letter character at least,
// so names like /./. aren't valid

// PathRegex regex for destination path
var PathRegex = regexp.MustCompile(`^/[\w.]*[[:alnum:]][\w.]*/[\w.]*[[:alnum:]][\w.]*$`)

// PathDLQRegex regex for dlq destination path
var PathDLQRegex = regexp.MustCompile(`^/[\w.]*[[:alnum:]][\w.]*/[\w.]*[[:alnum:]][\w.]*.dlq$`)

// PathRegexAllowUUID For special destinations (e.g. Dead letter queues) we allow a string UUID as path
var PathRegexAllowUUID, _ = regexp.Compile(`^(/[\w.]*[[:alnum:]][\w.]*/[\w.]*[[:alnum:]][\w.]*|[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12})$`)

// ConsumerGroupRegex regex for consumer group path
var ConsumerGroupRegex = PathRegex

// UUIDRegex regex for uuid
var UUIDRegex, _ = regexp.Compile(`^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$`)
