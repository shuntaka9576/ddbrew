package ddbrew

import (
	"context"
	"os"
)

type DeleteOption struct {
	Table     *Table
	File      *os.File
	LimitUnit *int
}

func Delete(ctx context.Context, opt *DeleteOption) error {
	return batchWriteWithConsle(&BatchWriteConsoleOpt{
		Table:     opt.Table,
		File:      opt.File,
		DDBAction: DDB_ACTION_DELETE,
		LimitUnit: opt.LimitUnit,
	})
}
