package cli

import (
	"context"
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
	table := &ddbrew.Table{}
	table.Init(tinfo)

	var limitUnit *int = nil
	if opt.Limit > 0 {
		limitUnit = &opt.Limit
	}

	err = ddbrew.Delete(ctx, &ddbrew.DeleteOption{
		Table:     table,
		File:      f,
		LimitUnit: limitUnit,
	})

	if err != nil {
		return err
	}

	return nil
}
