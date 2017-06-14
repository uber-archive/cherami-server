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
	"fmt"
	"strings"
)

const (
	resourceURNTemplateCreateDestination    = "urn:cherami:dst:%v:%v"
	resourceURNTemplateOperateDestination   = "urn:cherami:dst:%v:%v"
	resourceURNTemplateCreateConsumerGroup  = "urn:cherami:cg:%v:%v"
	resourceURNTemplateOperateConsumerGroup = "urn:cherami:cg:%v:%v:%v"
)

// GetResourceURNCreateDestination returns the resource URN to create destination, e.g. urn:cherami:dst:zone1_prod:/dst_prefix
// We use URN (Uniform Resource Name) like this: https://www.ietf.org/rfc/rfc2141.txt
func GetResourceURNCreateDestination(scommon SCommon, dstPath *string) string {
	var dstPathString string
	if dstPath == nil {
		dstPathString = ""
	} else {
		dstPathString = getPathRootName(dstPath)
	}
	return fmt.Sprintf(resourceURNTemplateCreateDestination, getTenancyLowerCase(scommon), strings.ToLower(dstPathString))
}

// GetResourceURNOperateDestination returns the resource URN to operate destination (read, delete), e.g. urn:cherami:dst:zone1_prod:/dst_prefix/dst1
// We use URN (Uniform Resource Name) like this: https://www.ietf.org/rfc/rfc2141.txt
func GetResourceURNOperateDestination(scommon SCommon, dstPath *string) string {
	var dstPathString string
	if dstPath == nil {
		dstPathString = ""
	} else {
		dstPathString = *dstPath
	}
	return fmt.Sprintf(resourceURNTemplateOperateDestination, getTenancyLowerCase(scommon), strings.ToLower(dstPathString))
}

// GetResourceURNCreateConsumerGroup returns the resource URN to create consumer group, e.g. urn:cherami:cg:zone1:/cg_prefix
// We use URN (Uniform Resource Name) like this: https://www.ietf.org/rfc/rfc2141.txt
func GetResourceURNCreateConsumerGroup(scommon SCommon, cgPath *string) string {
	var cgPathString string
	if cgPath == nil {
		cgPathString = ""
	} else {
		cgPathString = getPathRootName(cgPath)
	}
	return fmt.Sprintf(resourceURNTemplateCreateConsumerGroup, getTenancyLowerCase(scommon), strings.ToLower(cgPathString))
}

// GetResourceURNOperateConsumerGroup returns the resource URN to operate consumer group (read, delete), e.g. urn:cherami:cg:zone1:/dst_prefix/dst1:/cg_prefix/cg1
// We use URN (Uniform Resource Name) like this: https://www.ietf.org/rfc/rfc2141.txt
func GetResourceURNOperateConsumerGroup(scommon SCommon, dstPath *string, cgPath *string) string {
	var dstPathString string
	if dstPath == nil {
		dstPathString = ""
	} else {
		dstPathString = *dstPath
	}

	var cgPathString string
	if cgPath == nil {
		cgPathString = ""
	} else {
		cgPathString = *cgPath
	}
	return fmt.Sprintf(resourceURNTemplateOperateConsumerGroup, getTenancyLowerCase(scommon), strings.ToLower(dstPathString), strings.ToLower(cgPathString))
}

func getPathRootName(path *string) string {
	if path == nil || *path == "" {
		return ""
	}

	parts := strings.Split(*path, "/")

	if strings.HasPrefix(*path, "/") {
		return "/" + parts[1]
	}

	return parts[0]
}

func getTenancyLowerCase(scommon SCommon) string {
	deploymentName := scommon.GetConfig().GetDeploymentName()
	parts := strings.Split(deploymentName, "_")
	return strings.ToLower(parts[0])
}
