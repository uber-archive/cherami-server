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
	"context"
)

const (
	// SubjectTypeEmployee indiciates Employee
	SubjectTypeEmployee string = "Employee"
	// SubjectTypeService indiciates Service
	SubjectTypeService string = "Service"

	// OperationCreate indicates Create
	OperationCreate Operation = "Create"
	// OperationRead indicates Read
	OperationRead Operation = "Read"
	// OperationUpdate indicates Update
	OperationUpdate Operation = "Update"
	// OperationDelete indicates Delete
	OperationDelete Operation = "Delete"
)

type (
	// Subject is the entity like employee, service
	Subject struct {
		// Type is the entity type like Employee, Service
		Type string
		// Name is the entity name
		Name string
		// Groups contains the groups the entity belongs to
		Groups []string
	}

	// Operation is type for an user operation, e.g. CreateDestination, PublishDestination
	Operation string

	// Resource is type for a Cherami resource, e.g. destination, consumer group
	Resource string

	// AuthManager is interface to do auth
	AuthManager interface {
		// Authenticate checks user identity in the context, and return a subject instance containing the user name
		Authenticate(ctx context.Context) (Subject, error)
		// Authorize validates whether the user (subject) has the permission to do the operation on the resource.
		// It returns nil if the user has the permission, otherwise return error.
		Authorize(subject Subject, operation Operation, resource Resource) error
		// AddPermission adds permission
		AddPermission(subject Subject, operation Operation, resource Resource) error
	}

	// BypassAuthManager is a dummy implementation
	BypassAuthManager struct {
	}
)

// NewBypassAuthManager creates a dummy instance
func NewBypassAuthManager() AuthManager {
	return &BypassAuthManager{}
}

// Authenticate authenticates user
func (a *BypassAuthManager) Authenticate(ctx context.Context) (Subject, error) {
	subject := Subject{
		Type: SubjectTypeEmployee,
		Name: "bypass_auth_user",
	}
	return subject, nil
}

// Authorize authorizes user
func (a *BypassAuthManager) Authorize(subject Subject, operation Operation, resource Resource) error {
	return nil
}

// AddPermission adds permission
func (a *BypassAuthManager) AddPermission(subject Subject, operation Operation, resource Resource) error {
	return nil
}
