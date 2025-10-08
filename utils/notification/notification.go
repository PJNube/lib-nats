package notification

import (
	"fmt"
	"strings"
)

const (
	EventKeyword = "$event"
	SysKeyword   = "$sys"
)

func BuildSubject(profile, vendor string, extName ...string) string {
	if len(extName) == 0 {
		return strings.ToLower(fmt.Sprintf("local.%s.%s.%s.%s", EventKeyword, profile, vendor, SysKeyword))
	}
	return strings.ToLower(fmt.Sprintf("local.%s.%s.%s.%s", EventKeyword, profile, vendor, extName[0]))
}
