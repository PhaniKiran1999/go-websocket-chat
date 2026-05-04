package utils

import "fmt"

func ConstructRedisChannelKey(serverID string) string {
	return fmt.Sprintf("server-channel:%s:", serverID)
}
