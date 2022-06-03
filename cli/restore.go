package cli

import (
	"context"
	"os"

	"github.com/shuntaka9576/ddbrew"
)

type RestoreOption struct {
	TableName string
	FilePath  string
	DryRun    bool
	Limit     int
}

func Restore(ctx context.Context, opt *RestoreOption) error {
	var f *os.File
	f, err := os.Open(opt.FilePath)
	if err != nil {
		return err
	}

	table := ddbrew.Table{
		Name: opt.TableName,
	}

	var limitUnit *int = nil
	if opt.Limit > 0 {
		limitUnit = &opt.Limit
	}

	err = ddbrew.Restore(ctx, &ddbrew.RestoreOption{
		Table:     &table,
		File:      f,
		LimitUnit: limitUnit,
	})

	if err != nil {
		return err
	}

	return nil
}
