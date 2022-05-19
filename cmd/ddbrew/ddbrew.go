package main

import (
	"context"
	"fmt"
	"os"

	"github.com/alecthomas/kong"
	"github.com/shuntaka9576/ddbrew"
)

var CLI struct {
	Local  string `help:"Specify DynamoDB local endpoint. (ex: http://localhost:8000)"`
	Backup struct {
		TableName    string `arg:"" name:"tableName" help:"Specify table name to backup."`
		Output       string `short:"o" name:"Specify the file path to output (default backup_tableName_yyyymmdd-HHMMSS.jsonl)."`
		ScanLimit    int    `short:"l" name:"scan-limit" help:"Number of records to be scanned in one interval."`
		ScanInterval int    `short:"i" name:"scan-interval" help:"Specify the time for one interval (ms)."`
		Stdout       bool   `short:"s" name:"output to stdout"`
	} `cmd:"" help:"backup DynamoDB table."`
	Restore struct {
		TableName string `arg:"" name:"tableName" help:"Specifies table name to restore."`
		FilePath  string `short:"f" name:"filepath" required:"" help:"Specify the jsonline file containing the table data to be restored."`
		DryRun    bool   `short:"d" help:"Calculate the number of records to be written and RRUs to be consumed in the interval (sampling the first few records of input data)."`
		Procs     int    `short:"p" help:"Specifies the number of parallel BatchWriteRequests within one interval (default runtime.NumCPUs())."`
	} `cmd:"" help:"Restore DynamoDB table."`
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
			cmdErrCh <- ddbrew.Backup(ctx, &ddbrew.BackupOption{
				TableName:    CLI.Backup.TableName,
				ScanLimit:    CLI.Backup.ScanLimit,
				ScanInterval: CLI.Backup.ScanInterval,
				Output:       CLI.Backup.Output,
				Stdout:       CLI.Backup.Stdout,
			})
		case "restore <tableName>":
			cmdErrCh <- ddbrew.Restore(ctx, &ddbrew.RestoreOption{
				TableName: CLI.Restore.TableName,
				FilePath:  CLI.Restore.FilePath,
				DryRun:    CLI.Restore.DryRun,
				Procs:     CLI.Restore.Procs,
			})
		}
	}()

LOOP:
	for {
		select {
		case err := <-cmdErrCh:
			if err != nil {
				fmt.Fprintf(os.Stderr, "exec error: %s", err)

				cancel()
			}

			break LOOP
		case <-ctx.Done():
			break LOOP
		}
	}
}
