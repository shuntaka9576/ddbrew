package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/alecthomas/kong"
	"github.com/shuntaka9576/ddbrew"
	"github.com/shuntaka9576/ddbrew/cli"
)

var CLI struct {
	Local  string `short:"L" name:"local" help:"Specify DynamoDB local endpoint. (ex: http://localhost:8000)"`
	Backup struct {
		TableName string `arg:"" name:"tableName" help:"Specify table name to backup."`
		FilePath  string `short:"f" name:"file" help:"Specify the file path to output (default backup_tableName_yyyymmdd-HHMMSS.jsonl)."`
		Limit     int    `short:"l" help:"Limit the number of reads per second to the specified number (units are automatically determined as RCUs for provisioned tables and RRUs for on-demand tables)."`
	} `cmd:"" help:"backup DynamoDB table."`
	Restore struct {
		TableName string `arg:"" name:"tableName" help:"Specifies table name to restore."`
		FilePath  string `short:"f" name:"file" help:"Specify the jsonline file containing the table data to be restored."`
		DryRun    bool   `short:"d" name:"dry-run" help:"Simulate WRUs/WCUs to consume."`
		Limit     int    `short:"l" name:"limit" help:"Limit the number of writes per second by the specified number (the unit is automatically determined as RCU for provisioned tables and RRU for on-demand tables)."`
	} `cmd:"" help:"Restore DynamoDB table."`
	Truncate struct {
		TableName string `arg:"" name:"tableName" help:"Specifies table name to truncate."`
		FilePath  string `short:"f" name:"file" required:"" help:"Specify the jsonline file containing the table data to be truncate"`
		DryRun    bool   `short:"d" name:"dry-run" help:"Simulate WRUs/WCUs to consume."`
		Limit     int    `short:"l" name:"limit" help:"Limit the number of request units if the target table is on-demand, or capacity units if the target table is provisioned."`
	} `cmd:"" help:"Truncate DynamoDB table."`
}

func main() {
	kontext := kong.Parse(&CLI)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmdErrCh := make(chan error)

	ddbrew.InitClient(&ddbrew.DDBClientOption{
		Local: CLI.Local,
	})

	go func() {
		switch kontext.Command() {
		case "backup <tableName>":
			cmdErrCh <- cli.Backup(ctx, &cli.BackupOption{
				TableName: CLI.Backup.TableName,
				FilePath:  CLI.Backup.FilePath,
				Limit:     CLI.Backup.Limit,
			})
		case "restore <tableName>":
			cmdErrCh <- cli.Restore(ctx, &cli.RestoreOption{
				TableName: CLI.Restore.TableName,
				FilePath:  CLI.Restore.FilePath,
				DryRun:    CLI.Restore.DryRun,
				Limit:     CLI.Restore.Limit,
			})
		case "truncate <tableName>":
			cmdErrCh <- cli.Truncate(ctx, &cli.TruncateOption{
				TableName: CLI.Truncate.TableName,
				FilePath:  CLI.Truncate.FilePath,
				DryRun:    CLI.Truncate.DryRun,
				Limit:     CLI.Truncate.Limit,
			})
		}
	}()

LOOP:
	for {
		select {
		case err := <-cmdErrCh:
			if err != nil {
				switch {
				case errors.Is(err, cli.ErrorOptInputError):
					fmt.Fprintf(os.Stderr, "must specify --filepath or --stdin option")
				case errors.Is(err, cli.ErrorDescribeTable):
					fmt.Fprintf(os.Stderr, "descirebe table error: %s", err)
				default:
					fmt.Fprintf(os.Stderr, "%s", err)
				}

				cancel()
			}

			break LOOP
		case <-ctx.Done():
			break LOOP
		}
	}
}
