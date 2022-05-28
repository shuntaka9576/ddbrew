package ddbrew

import (
	"fmt"
)

const KB_UNIT = 1000
const MB_UNIT = 1000000
const GB_UNIT = 1000000000

func PrittyPrintBytes(size int) *string {
	var bytestring string
	bytestring = fmt.Sprintf("%.2f B", float64(size))

	if size >= KB_UNIT && size < MB_UNIT {
		bytestring = fmt.Sprintf("%.2f KB", float64(size)/KB_UNIT)
	} else if size >= MB_UNIT && size < GB_UNIT {
		bytestring = fmt.Sprintf("%.2f MB", float64(size)/MB_UNIT)

	} else if size >= GB_UNIT {
		bytestring = fmt.Sprintf("%.2f GB", float64(size)/GB_UNIT)
	}

	return &bytestring
}
