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
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
)

type DeleteOption struct {
	TableName string
	File      *os.File
	LimitUnit *int
}

func Delete(ctx context.Context, opt *DeleteOption) error {
	tasks := make(chan Task)
	results := make(chan Result)
	writeItems := make(chan *WriteItem)
	tableName := opt.TableName

	tinfo, _ := DdbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &opt.TableName,
	})

	var tableKeys []string
	for _, keyav := range tinfo.Table.KeySchema {
		tableKeys = append(tableKeys, *keyav.AttributeName)
	}

	procs := runtime.NumCPU()
	for i := 0; i < procs; i++ {
		go worker(i, tasks, results)
	}

	inputDone := make(chan struct{})
	var remainedCount int64

	wo := (&WriteOrchestrator{
		Ctx:           ctx,
		TaskType:      TASK_TYPE_DELETE,
		TableName:     tableName,
		WriteItems:    writeItems,
		Tasks:         tasks,
		LimitUnit:     opt.LimitUnit,
		remainedCount: &remainedCount,
		inputDone:     inputDone,
	})

	readLine := countLines(opt.File)

	opt.File.Seek(0, 0)
	reader := bufio.NewReader(opt.File)

	go func() {
		for {
			jl, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					writeItems <- nil

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
			mappingTableKeyAv := map[string]types.AttributeValue{}
			for _, key := range tableKeys {
				mappingTableKeyAv[key] = attributeMap[key]
			}

			req := types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: mappingTableKeyAv,
				},
			}

			writeItems <- &WriteItem{
				tableName: opt.TableName,
				item:      req,
				ru:        result.ReadUnit,
				wu:        result.WriteUnit,
				size:      result.Size,
			}

			for wo.queueSize != nil && *wo.queueSize >= MAX_ORCHESTRATOR_QUEUE_SIZE {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	go wo.Run()

	writeRecordLength := 0
	unprocessedFlag := false
	unprocessedLength := 0
	var unprocessedRecordFile *os.File

	for {
		select {
		case result := <-results:
			writeRecordLength += result.Count()

			if !unprocessedFlag {
				fmt.Fprintf(os.Stderr, "\rdelete record: %d(%d%%)", writeRecordLength, int(float64(writeRecordLength)/float64(readLine)*100))
			} else {
				fmt.Fprintf(os.Stderr, "\rdelte record: %d(%d%%), unprocessed record(%s): %d", writeRecordLength, int(float64(writeRecordLength)/float64(readLine)*100), unprocessedRecordFile.Name(), unprocessedLength)
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