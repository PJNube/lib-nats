package notification

import (
	"fmt"
	"strings"
)

const (
	EventKeyword        = "$event"
	SysKeyword          = "$sys"
	NotificationKeyword = "$notification"

	BeProfile    = "be"
	VendorPjnube = "pjnube"

	DeleteRoleSubject = "delete.role"
)

func BuildSubject(profile, vendor string, extName ...string) string {
	if len(extName) == 0 {
		return strings.ToLower(fmt.Sprintf("local.%s.%s.%s.%s", EventKeyword, profile, vendor, SysKeyword))
	}
	return strings.ToLower(fmt.Sprintf("local.%s.%s.%s.%s", EventKeyword, profile, vendor, extName[0]))
}

func BuildNotificationSysSubject(profile, vendor, subject string) string {
	return fmt.Sprintf("local.%s.%s.%s.%s.%s", NotificationKeyword, profile, vendor, SysKeyword, subject)
}
