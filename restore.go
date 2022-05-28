package ddbrew

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
)

const (
	DEFAULT_SAMPLING_SIZE    int = 10
	BATCH_WRITE_PER_RECORD   int = 25
	BATCH_WRITE_PER_CAPACITY int = 1000000 // 1MB
)

type RestoreOption struct {
	TableName string
	Reader    bufio.Reader
	LimitUnit *int
}

func Restore(ctx context.Context, opt *RestoreOption) error {
	tasks := make(chan Task)
	results := make(chan Result)
	writeItems := make(chan *WriteItem)
	tableName := opt.TableName

	procs := runtime.NumCPU()
	for i := 0; i < procs; i++ {
		go worker(i, tasks, results)
	}

	inputDone := make(chan struct{})
	var remainedCount int64
	readLine, readDone := 0, false

	type WriteRequestPerSec = [][]types.WriteRequest

	wo := &WriteOrchestrator{
		Ctx:           ctx,
		TableName:     tableName,
		WriteItems:    writeItems,
		Tasks:         tasks,
		LimitUnit:     opt.LimitUnit,
		remainedCount: &remainedCount,
		inputDone:     inputDone,
	}

	go wo.Run()

	go func() {
		for {
			jl, err := opt.Reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					writeItems <- nil
					readDone = true

					break
				}
			}

			jm := map[string]any{}
			json.Unmarshal([]byte(strings.TrimSpace(jl)), &jm)
			result, err := GetItemSizeByJSON(jm)
			if err != nil {
				continue
			}

			attributeMap, err := attributevalue.MarshalMap(jm)
			if err != nil {
				continue
			}

			req := types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: attributeMap,
				},
			}
			writeItems <- &WriteItem{
				tableName: opt.TableName,
				item:      req,
				ru:        result.ReadUnit,
				wu:        result.WriteUnit,
				size:      result.Size,
			}
			readLine += 1

			for wo.queueSize != nil && *wo.queueSize >= MAX_ORCHESTRATOR_QUEUE_SIZE {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	writeRecordLength := 0
	unprocessedFlag := false
	unprocessedLength := 0
	var unprocessedRecordFile *os.File

	for {
		select {
		case result := <-results:
			writeRecordLength += result.Count()

			if !unprocessedFlag {
				if readDone {
					fmt.Fprintf(os.Stderr, "\rwrite record: %d(%d%%)", writeRecordLength, int(float64(writeRecordLength)/float64(readLine)*100))
				} else {
					fmt.Fprintf(os.Stderr, "\rwrite record: %d", writeRecordLength)
				}
			} else {
				if readDone {
					fmt.Fprintf(os.Stderr, "\rwrite record: %d(%d%%), unprocessed record: %d", writeRecordLength, int(float64(writeRecordLength)/float64(readLine)*100), unprocessedLength)
				} else {
					fmt.Fprintf(os.Stderr, "\rwrite record: %d, unprocessed record(%s): %d", writeRecordLength, unprocessedRecordFile.Name(), unprocessedLength)
				}
			}

			if !unprocessedFlag && len(result.UnprocessedItems()) > 0 {
				unprocessedFlag = true
				filePath := fmt.Sprintf("unprocessed_record_%s_%s.jsonl", opt.TableName, time.Now().Format("20060102-150405"))
				unprocessedRecordFile, _ = os.Create(filePath)
				defer unprocessedRecordFile.Close()
			}

			if len(result.UnprocessedItems()) > 0 {
				for _, record := range result.UnprocessedItems() {
					unprocessedRecordFile.Write([]byte(record + "\n"))
					unprocessedLength += 1
				}
			}

			if result.Error() != nil {
				return errors.Wrap(result.Error(), "batch write err")
			} else {
				atomic.AddInt64(&remainedCount, -1)
			}
		case <-inputDone:
			if remainedCount == 0 {
				return nil
			}
		}
	}
}
