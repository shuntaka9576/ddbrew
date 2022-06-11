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

type Globals struct {
	Local   string          `short:"L" name:"local" help:"Specify DynamoDB local endpoint. ex: (http://)localhost:8000"`
	Version cli.VersionFlag `short:"v" name:"version" help:"print the version."`
}

var CLI struct {
	Globals
	Backup struct {
		TableName string `arg:"" name:"tableName" help:"Specify table name to backup."`
		File      string `short:"f" name:"file" help:"Specify the file path to output (default backup_tableName_yyyymmdd-HHMMSS.jsonl)."`
		Limit     int    `short:"l" help:"Limit the number of reads per second to the specified number (units are automatically determined as RCUs for provisioned tables and RRUs for on-demand tables)."`
	} `cmd:"" help:"Backup DynamoDB table."`
	Restore struct {
		TableName string `arg:"" name:"tableName" help:"Specifies table name to restore."`
		File      string `short:"f" name:"file" required:"" help:"Specify the jsonline file containing the table data to be restored."`
		DryRun    bool   `short:"d" name:"dry-run" help:"Simulate WRUs/WCUs to consume."`
		Limit     int    `short:"l" name:"limit" help:"Limit the number of writes per second by the specified number (the unit is automatically determined as RCU for provisioned tables and RRU for on-demand tables)."`
	} `cmd:"" help:"Restore DynamoDB table."`
	Delete struct {
		TableName string `arg:"" name:"tableName" help:"Specifies table name to delete."`
		File      string `short:"f" name:"file" required:"" help:"Specify the jsonline file containing the table data to be delete."`
		DryRun    bool   `short:"d" name:"dry-run" help:"Simulate WRUs/WCUs to consume."`
		Limit     int    `short:"l" name:"limit" help:"Limit the number of request units if the target table is on-demand, or capacity units if the target table is provisioned."`
	} `cmd:"" help:"Delete DynamoDB table."`
}

func main() {
	kontext := kong.Parse(&CLI,
		kong.Name("ddbrew"),
		kong.Description("Simple DynamoDB utility"),
	)

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
				FilePath:  CLI.Backup.File,
				Limit:     CLI.Backup.Limit,
			})
		case "restore <tableName>":
			cmdErrCh <- cli.Write(ctx, &cli.WriteOption{
				TableName: CLI.Restore.TableName,
				FilePath:  CLI.Restore.File,
				DryRun:    CLI.Restore.DryRun,
				Limit:     CLI.Restore.Limit,
				Action:    ddbrew.DDB_ACTION_PUT,
			})
		case "delete <tableName>":
			cmdErrCh <- cli.Write(ctx, &cli.WriteOption{
				TableName: CLI.Delete.TableName,
				FilePath:  CLI.Delete.File,
				DryRun:    CLI.Delete.DryRun,
				Limit:     CLI.Delete.Limit,
				Action:    ddbrew.DDB_ACTION_DELETE,
			})
		}
	}()

LOOP:
	for {
		select {
		case err := <-cmdErrCh:
			if err != nil {
				switch {
				case errors.Is(err, cli.ErrorDescribeTable):
					fmt.Fprintf(os.Stderr, "descirebe table error: %s", err)
				default:
					fmt.Fprintf(os.Stderr, "%s", err)
				}

				cancel()
				os.Exit(1)
			}

			break LOOP
		case <-ctx.Done():
			break LOOP
		}
	}

	os.Exit(0)
}
