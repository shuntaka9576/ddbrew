package cli

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/pkg/errors"
	"github.com/shuntaka9576/ddbrew"
	"github.com/shuntaka9576/ddbrew/ui"
)

type WriteOption struct {
	TableName string
	FilePath  string
	DryRun    bool
	Limit     int
	Action    ddbrew.DDBAction
}

func Write(ctx context.Context, opt *WriteOption) error {

	f, err := os.Open(opt.FilePath)
	if err != nil {
		return err
	}
	lines := countLines(f)

	info, err := ddbrew.DdbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &opt.TableName,
	})
	if err != nil {
		return errors.Wrap(ErrorDescribeTable, err.Error())
	}

	table := ddbrew.Table{}
	table.Init(info)

	if opt.DryRun {
		result, err := ddbrew.Simulate(&ddbrew.SimulateOpt{Reader: f, Mode: table.Mode})
		if err != nil {
			return err
		}

		size := ddbrew.PrittyPrintBytes(result.TotalItemSize)
		fmt.Printf("Total item size: %s\n", *size)

		switch table.Mode {
		case ddbrew.Provisioned:
			fmt.Printf("Total to consume: %d WCU\n", *result.ConsumeWCU)
		case ddbrew.OnDemand:
			fmt.Printf("Total to consume: %d WRU\n", *result.ConsumeWRU)
		}

		return nil
	}

	var limitUnit *int = nil
	if opt.Limit > 0 {
		limitUnit = &opt.Limit
	}

	results := make(chan *ddbrew.BatchResult)
	done := make(chan struct{})
	remainCount := int64(0)

	writer := ddbrew.BatchWriter{
		Table:       &table,
		File:        f,
		DDBAction:   opt.Action,
		LimitUnit:   limitUnit,
		RemainCount: &remainCount,
		Results:     results,
		Done:        done,
	}

	err = writer.BatchWrite(ctx)
	if err != nil {
		return err
	}

	finCh := make(chan struct{})
	p := tea.NewProgram(ui.InitModel(&ui.Option{
		LineCount:   lines,
		RemainCount: &remainCount,
		FinCh:       finCh,
	}), tea.WithOutput(os.Stderr))

	// FIXME: use error group
	go func() {
		if err := p.Start(); err != nil {
			os.Exit(1)
		}
	}()

	isUnprocessed := false
	var unprocessedRecordFile *os.File

LOOP:
	for {
		select {
		case result := <-results:
			atomic.AddInt64(&remainCount, -1)

			if result.Error != nil {
				return result.Error
			}

			if !isUnprocessed && len(result.Content.UnprocessedRecord) > 0 {
				isUnprocessed = true

				ufile := fmt.Sprintf("unprocessed_record_%s_%s.jsonl",
					opt.TableName,
					time.Now().Format("20060102-150405"))
				unprocessedRecordFile, err = os.Create(ufile)
				if err != nil {
					return err
				}

				defer unprocessedRecordFile.Close()
				p.Send(ui.BatchMsg{
					SuccessCount:        result.Content.SuccessCount,
					UnprocessedCount:    len(result.Content.UnprocessedRecord),
					UnprocessedFileName: unprocessedRecordFile.Name(),
				})
			} else {
				p.Send(ui.BatchMsg{
					SuccessCount:     result.Content.SuccessCount,
					UnprocessedCount: len(result.Content.UnprocessedRecord),
				})
			}

			if len(result.Content.UnprocessedRecord) > 0 {
				for _, record := range result.Content.UnprocessedRecord {
					unprocessedRecordFile.Write([]byte(record + "\n"))
				}
			}
		case <-done:
			if remainCount == 0 {
				fmt.Println("break")
				break LOOP
			}
		}
	}

	<-finCh

	return nil
}
