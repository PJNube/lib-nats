package notification

import (
	"fmt"
	"strings"
)

const (
	EventKeyword = "$event"
	SysKeyword   = "$sys"

	BeKeyword         = "be"
	CoreKeyword       = "core"
	DeleteRoleKeyword = "delete.role"
	CheckRoleKeyword  = "check.role"
)

func BuildSubject(profile, vendor string, extName ...string) string {
	if len(extName) == 0 {
		return strings.ToLower(fmt.Sprintf("local.%s.%s.%s.%s", EventKeyword, profile, vendor, SysKeyword))
	}
	return strings.ToLower(fmt.Sprintf("local.%s.%s.%s.%s", EventKeyword, profile, vendor, extName[0]))
}

func BuildLocalEventSysSubject(profile, vendor, subject string) string {
	return fmt.Sprintf("local.%s.%s.%s.%s.%s", EventKeyword, profile, vendor, SysKeyword, subject)
}
