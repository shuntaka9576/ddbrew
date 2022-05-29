package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/pkg/errors"
	"github.com/shuntaka9576/ddbrew"
)

type DeleteOption struct {
	TableName string
	FilePath  string
	DryRun    bool
	Limit     int
}

func Delete(ctx context.Context, opt *DeleteOption) error {
	var f *os.File
	f, err := os.Open(opt.FilePath)
	if err != nil {
		return err
	}

	tinfo, err := ddbrew.DdbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &opt.TableName,
	})
	if err != nil {
		return errors.Wrap(ErrorDescribeTable, err.Error())
	}

	mode := ddbrew.GetDDBMode(tinfo)

	if opt.DryRun {
		result, err := ddbrew.Simulate(&ddbrew.SimulateOpt{Reader: f, Mode: *mode})
		if err != nil {
			return err
		}

		size := ddbrew.PrittyPrintBytes(result.TotalItemSize)
		fmt.Printf("Total item size: %s\n", *size)
		if mode == &ddbrew.Provisioned {
			fmt.Printf("Total to consume: %d WCU\n", *result.ConsumeWCU)
		} else if mode == &ddbrew.OnDemand {
			fmt.Printf("Total to consume: %d WRU\n", *result.ConsumeWRU)
		}

		return nil
	}

	var limitUnit *int = nil
	if opt.Limit > 0 {
		limitUnit = &opt.Limit
	}

	err = ddbrew.Delete(ctx, &ddbrew.DeleteOption{
		TableName: opt.TableName,
		File:      f,
		LimitUnit: limitUnit,
	})

	if err != nil {
		return err
	}

	return nil
}