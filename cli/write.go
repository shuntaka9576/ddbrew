package cli

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/pkg/errors"
	"github.com/shuntaka9576/ddbrew"
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

	successNum, unprocessedNum := 0, 0
	var unprocessedRecordFile *os.File
	isUnprocessed := false

	for {
		select {
		case result := <-results:
			atomic.AddInt64(&remainCount, -1)

			successNum += result.Content.SuccessCount
			unprocessedNum += len(result.Content.UnprocessedRecord)
			progress := int(float64(successNum) / float64(lines) * 100)

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
			}

			if len(result.Content.UnprocessedRecord) > 0 {
				for _, record := range result.Content.UnprocessedRecord {
					unprocessedRecordFile.Write([]byte(record + "\n"))
				}
			}

			if isUnprocessed {
				fmt.Fprintf(os.Stderr, "\rSuccess: %d(%d%%) Unprocessed(%s): %d",
					successNum,
					progress,
					unprocessedRecordFile.Name(),
					unprocessedNum)
			} else {
				fmt.Fprintf(os.Stderr, "\rSuccess: %d(%d%%)", successNum, progress)
			}

			if result.Error != nil {
				return result.Error
			}
		case <-done:
			if remainCount == 0 {
				return nil
			}
		}
	}
}
