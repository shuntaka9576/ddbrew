package cli

import (
	"context"

	"github.com/shuntaka9576/ddbrew"
)

type BackupOption struct {
	TableName string
	FilePath  string
	Limit     int
}

func Backup(ctx context.Context, opt *BackupOption) error {
	var limit *int
	if opt.Limit > 0 {
		limit = &opt.Limit
	}

	err := ddbrew.Backup(ctx, &ddbrew.BackupOption{
		TableName: opt.TableName,
		FilePath:  opt.FilePath,
		Limit:     limit,
	})

	if err != nil {
		return err
	}

	return nil
}
