package main

import (
	"fmt"
	"regexp"

	"github.com/gocql/gocql"
	"github.com/uber/cherami-thrift/.generated/go/shared"
)

func getUUIDFilters(uuidStrings []string) (uuids []gocql.UUID) {

	for _, u := range uuidStrings {
		if uuid, err := gocql.ParseUUID(u); err == nil {
			uuids = append(uuids, uuid)
		}
	}

	return
}

func matchRegexpFilters(v string, filters []*regexp.Regexp) (matched bool) {

	if len(filters) == 0 {
		return true
	}

	for _, f := range filters {
		if f.MatchString(v) {
			return true
		}
	}

	return false
}

func matchIntFilters(v int, filters []int) (matched bool) {

	if len(filters) == 0 {
		return true
	}

	for _, f := range filters {
		if v == f {
			return true
		}
	}

	return false
}

func matchUUIDFilters(v gocql.UUID, filters []gocql.UUID) (matched bool) {

	if len(filters) == 0 {
		return true
	}

	for _, f := range filters {
		if v == f {
			return true
		}
	}

	return false
}

func matchFilters(v string, filters []string) (matched bool) {

	if len(filters) == 0 {
		return true
	}

	for _, f := range filters {
		if v == f {
			return true
		}
	}

	return false
}

func multiMatchWhereClause(column string, filters []string) (clause string) {

	switch len(filters) {
	case 0:
		break

	case 1:
		clause = fmt.Sprintf("%s=%s", column, filters[0])

	default: // > 1
		for i, d := range filters {
			if i == 0 {
				clause = fmt.Sprintf("%s IN ( ", column)
			} else {
				clause += ", "
			}

			clause += d
		}
		clause += " )"
	}

	return
}

func appendWhere(cql string, where []string) string {

	if len(where) > 0 {
		for i, w := range where {
			if i == 0 {
				cql += " WHERE "
			} else {
				cql += " AND "
			}
			cql += w
		}
	}

	return cql
}

func getDestStatus(status string) int {

	switch status {
	case "0":
		fallthrough
	case "open":
		fallthrough
	case "active":
		fallthrough
	case "enabled":
		return int(shared.DestinationStatus_ENABLED)
	case "1":
		fallthrough
	case "disabled":
		return int(shared.DestinationStatus_DISABLED)
	case "2":
		fallthrough
	case "sendonly":
		return int(shared.DestinationStatus_SENDONLY)
	case "3":
		fallthrough
	case "receiveonly":
		return int(shared.DestinationStatus_RECEIVEONLY)
	case "4":
		fallthrough
	case "deleting":
		return int(shared.DestinationStatus_DELETING)
	case "5":
		fallthrough
	case "deleted":
		return int(shared.DestinationStatus_DELETED)
	default:
		return -1
	}
}

func getCGStatus(status string) int {

	switch status {
	case "0":
		fallthrough
	case "open":
		fallthrough
	case "active":
		fallthrough
	case "enabled":
		return int(shared.ConsumerGroupStatus_ENABLED)
	case "1":
		fallthrough
	case "disabled":
		return int(shared.ConsumerGroupStatus_DISABLED)
	case "2":
		fallthrough
	case "deleted":
		return int(shared.ConsumerGroupStatus_DELETED)
	case "3":
		fallthrough
	case "deleting":
		return int(shared.ConsumerGroupStatus_DELETING)
	default:
		return -1
	}
}

func getExtStatus(status string) int {

	switch status {
	case "0":
		fallthrough
	case "open":
		fallthrough
	case "active":
		fallthrough
	case "enabled":
		return int(shared.ExtentStatus_OPEN)
	case "1":
		fallthrough
	case "sealed":
		return int(shared.ExtentStatus_SEALED)
	case "2":
		fallthrough
	case "consumed":
		return int(shared.ExtentStatus_CONSUMED)
	case "3":
		fallthrough
	case "archived":
		return int(shared.ExtentStatus_ARCHIVED)
	case "4":
		fallthrough
	case "deleted":
		return int(shared.ExtentStatus_DELETED)
	default:
		return -1
	}
}

func getCgxStatus(status string) int {

	switch status {
	case "0":
		fallthrough
	case "open":
		fallthrough
	case "active":
		fallthrough
	case "enabled":
		return int(shared.ConsumerGroupExtentStatus_OPEN)
	case "1":
		fallthrough
	case "consumed":
		return int(shared.ConsumerGroupExtentStatus_CONSUMED)
	case "2":
		fallthrough
	case "deleted":
		return int(shared.ConsumerGroupExtentStatus_DELETED)
	default:
		return -1
	}
}

func getSxStatus(status string) int {

	switch status {
	case "0":
		fallthrough
	case "created":
		return int(shared.ExtentReplicaStatus_CREATED)
	case "open":
		fallthrough
	case "active":
		fallthrough
	case "1":
		fallthrough
	case "enabled":
		return int(shared.ExtentReplicaStatus_OPEN)
	case "2":
		fallthrough
	case "sealed":
		return int(shared.ExtentReplicaStatus_SEALED)
	case "3":
		fallthrough
	case "deleted":
		return int(shared.ExtentReplicaStatus_DELETED)
	case "4":
		fallthrough
	case "corrupted":
		return int(shared.ExtentReplicaStatus_CORRUPTED)
	case "5":
		fallthrough
	case "missing":
		return int(shared.ExtentReplicaStatus_MISSING)
	default:
		return -1
	}
}

func getDestType(destType string) int {

	switch destType {
	case "0":
		fallthrough
	case "plain":
		return int(shared.DestinationType_PLAIN)
	case "1":
		fallthrough
	case "timer":
		return int(shared.DestinationType_TIMER)
	case "2":
		fallthrough
	case "log":
		return int(shared.DestinationType_LOG)
	case "3":
		fallthrough
	case "kafka":
		return int(shared.DestinationType_KAFKA)
	default:
		return -1
	}
}
