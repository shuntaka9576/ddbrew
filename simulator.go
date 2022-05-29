package ddbrew

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

type DDBMode = string

var (
	OnDemand    = DDBMode("OnDemand")
	Provisioned = DDBMode("Provisioned")
)

const (
	ITEM_SIZE_LIMIT = 400000 // 400KB
)

type SimulateOpt struct {
	Reader io.Reader
	Mode   DDBMode
}

type SimulateResult struct {
	ConsumeRRU    *int
	ConsumeWRU    *int
	ConsumeRCU    *int
	ConsumeWCU    *int
	TotalItemSize int
}

func Simulate(opt *SimulateOpt) (*SimulateResult, error) {
	reader := bufio.NewReader(opt.Reader)

	var totalItemSize float64
	rusum, wusum := 0, 0
	for {
		l, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
		}
		tl := strings.TrimSpace(l)

		var pjl map[string]any
		err = json.Unmarshal([]byte(tl), &pjl)

		if err != nil {
			return nil, err
		}

		itemResult, err := GetItemSizeByJSON(pjl)
		if err != nil {
			return nil, err
		}

		if itemResult.Size > ITEM_SIZE_LIMIT {
			return nil, err
		}

		totalItemSize = totalItemSize + float64(itemResult.Size)
		rusum += itemResult.ReadUnit
		wusum += itemResult.WriteUnit
	}

	switch opt.Mode {
	case OnDemand:
		return &SimulateResult{
			ConsumeWRU:    &wusum,
			ConsumeRRU:    &rusum,
			TotalItemSize: int(totalItemSize),
		}, nil
	case Provisioned:
		return &SimulateResult{
			ConsumeWCU:    &wusum,
			ConsumeRCU:    &rusum,
			TotalItemSize: int(totalItemSize),
		}, nil
	default:
		return nil, errors.New("invalid DynamoDB mode")
	}
}
