package ddbrew

import (
	"errors"
	"strconv"
)

const (
	WU_UNIT = 1000 // 1KB
	RU_UNIT = 4000 // 4KB
)

type ItemResult struct {
	Size      int
	ReadUnit  int
	WriteUnit int
}

func itemSizeRoundUp(size int, unitSize int) int {
	remainder := size % unitSize
	quotient := size / unitSize

	if remainder > 0 {
		return (quotient + 1) * unitSize
	} else {
		return quotient * unitSize
	}
}

func itemReadSizeRoundUp(size int) int {
	return itemSizeRoundUp(size, RU_UNIT)
}

func itemWriteSizeRoundUp(size int) int {
	return itemSizeRoundUp(size, WU_UNIT)
}

func getRuSize(size int) int {
	return itemReadSizeRoundUp(size) / RU_UNIT
}

func getWuSize(size int) int {
	return itemWriteSizeRoundUp(size) / WU_UNIT
}

func GetItemSizeByJSON(json map[string]any) (*ItemResult, error) {
	size, err := getItemSizeByJSON(json)
	if err != nil {
		return nil, err
	}
	ru := getRuSize(size)
	wu := getWuSize(size)

	return &ItemResult{
		Size:      size,
		ReadUnit:  ru,
		WriteUnit: wu,
	}, nil
}

func getItemSizeByJSON(json any) (int, error) {
	var sum int

	switch vt := json.(type) {
	case map[string]any:
		for k, v := range vt {
			glen, err := getItemSizeByJSON(v)
			if err != nil {
				return 0, nil
			}
			sum += glen
			sum += len(k)
		}
	case string:
		l := len(vt)
		return l, nil
	case float64:
		l := len(strconv.FormatFloat(vt, 'f', -1, 64))
		return l, nil
	case bool:
		l := 1
		return l, nil
	case nil:
		l := 1
		return l, nil
	case []interface{}:
		for _, v := range vt {
			glen, err := getItemSizeByJSON(v)
			if err != nil {
				return 0, err
			}
			sum += glen
		}
	default:
		return 0, errors.New("unexpected type error")
	}

	return sum, nil
}
