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
	resourceURNTemplateCreateDestination   = "urn:cherami:dst:%v:%v"
	resourceURNTemplateCreateConsumerGroup = "urn:cherami:dst:%v:%v"
)

// GetResourceURNCreateDestination returns the resource URN to create destination, e.g. urn:cherami:dst:zone1_prod:/prefix1
// We use URN (Uniform Resource Name) like this: https://www.ietf.org/rfc/rfc2141.txt
func GetResourceURNCreateDestination(scommon SCommon, dstPath *string) string {
	var dstPathString string
	if dstPath == nil {
		dstPathString = ""
	} else {
		dstPathString = getPathRootName(dstPath)
	}
	deploymentName := scommon.GetConfig().GetDeploymentName()
	return fmt.Sprintf(resourceURNTemplateCreateDestination, strings.ToLower(deploymentName), strings.ToLower(dstPathString))
}

// GetResourceURNCreateConsumerGroup returns the resource URN to create consumer group, e.g. urn:cherami:dst:zone1_prod:/dst1
// We use URN (Uniform Resource Name) like this: https://www.ietf.org/rfc/rfc2141.txt
func GetResourceURNCreateConsumerGroup(scommon SCommon, dstPath *string) string {
	var dstPathString string
	if dstPath == nil {
		dstPathString = ""
	} else {
		dstPathString = *dstPath
	}
	deploymentName := scommon.GetConfig().GetDeploymentName()
	return fmt.Sprintf(resourceURNTemplateCreateConsumerGroup, strings.ToLower(deploymentName), strings.ToLower(dstPathString))
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
