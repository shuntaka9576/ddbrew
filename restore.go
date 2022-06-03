package ddbrew

import (
	"context"
	"os"
)

type RestoreOption struct {
	Table     *Table
	File      *os.File
	LimitUnit *int
}

func Restore(ctx context.Context, opt *RestoreOption) error {
	return batchWriteWithConsle(&BatchWriteConsoleOpt{
		Table:     opt.Table,
		File:      opt.File,
		DDBAction: DDB_ACTION_PUT,
		LimitUnit: opt.LimitUnit,
	})
}
