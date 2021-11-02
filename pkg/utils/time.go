package utils

import "time"

func UnixMilliLocal(milli int64) time.Time {
	return time.UnixMilli(milli).Local()
}
