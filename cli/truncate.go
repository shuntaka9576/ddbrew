package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/pkg/errors"
	"github.com/shuntaka9576/ddbrew"
)

type TruncateOption struct {
	TableName string
	FilePath  string
	DryRun    bool
	Limit     int
	Stdin     string
}

func (c *TruncateOption) validate() error {
	if c.FilePath == "" && c.Stdin == "" || c.FilePath != "" && c.Stdin != "" {
		return ErrorOptInputError
	}

	return nil
}

func Truncate(ctx context.Context, opt *TruncateOption) error {
	err := opt.validate()
	if err != nil {
		return err
	}

	var f *os.File
	var re io.Reader
	if opt.FilePath != "" {
		f, _ = os.Open(opt.FilePath)
		re = f
	} else if opt.Stdin != "" {
		re = strings.NewReader(opt.Stdin)
	}

	tinfo, err := ddbrew.DdbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &opt.TableName,
	})
	if err != nil {
		return errors.Wrap(ErrorDescribeTable, err.Error())
	}

	mode := ddbrew.GetDDBMode(tinfo)

	reader := bufio.NewReader(re)
	if opt.DryRun {
		result, err := ddbrew.Simulate(&ddbrew.SimulateOpt{Reader: reader, Mode: *mode})
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

	err = ddbrew.Truncate(ctx, &ddbrew.TruncateOption{
		TableName: opt.TableName,
		Reader:    *reader,
		LimitUnit: limitUnit,
	})

	if err != nil {
		return err
	}

	return nil
}
