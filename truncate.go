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

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
)

type TruncateOption struct {
	TableName string
	FilePath  string
}

func Trunate(ctx context.Context, opt *TruncateOption) error {
	f, err := os.Open(opt.FilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	procs := runtime.NumCPU()

	headInfo, err := head(f, DEFAULT_SAMPLING_SIZE)
	if err != nil {
		return err
	}

	jsonLineSize := len(headInfo.Lines) / headInfo.LineConut
	perBatchRecordSize := BATCH_WRITE_LIMIT_PER_REQ
	perBatchRecordSizeExp := BATCH_WRITE_CAPACITY / jsonLineSize

	if perBatchRecordSizeExp < BATCH_WRITE_LIMIT_PER_REQ {
		perBatchRecordSize = perBatchRecordSizeExp
	}

	tasks := make(chan Task)
	results := make(chan Result)

	for i := 0; i < procs; i++ {
		go worker(i, tasks, results)
	}

	reader := bufio.NewReader(f)

	var remainedCount int64
	readLine := 0
	inputDone := make(chan struct{})

	tinfo, _ := ddbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &opt.TableName,
	})

	var tableKeys []string
	for _, keyav := range tinfo.Table.KeySchema {
		tableKeys = append(tableKeys, *keyav.AttributeName)
	}

	go func() {
		notifyTask := func(wreq []types.WriteRequest) {
			batchReq := map[string][]types.WriteRequest{}
			batchReq[opt.TableName] = wreq
			input := &dynamodb.BatchWriteItemInput{
				RequestItems: batchReq,
			}

			tasks <- &RestoreTask{
				tableName: opt.TableName,
				req:       input,
				ctx:       ctx,
			}
			atomic.AddInt64(&remainedCount, 1)
		}

		var wreqs []types.WriteRequest

		for {
			jl, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					notifyTask(wreqs)

					break
				}
			}

			tjl := strings.TrimSpace(jl)
			m := make(map[string]interface{})
			err = json.Unmarshal([]byte(tjl), &m)
			if err != nil {
				fmt.Printf("json marshal err %s\n", err)

				continue
			}

			attributeMap, err := attributevalue.MarshalMap(m)
			if err != nil {
				fmt.Printf("marshal error %s\n", err)

				continue
			}

			mappingTableKeyAv := map[string]types.AttributeValue{}
			for _, key := range tableKeys {
				mappingTableKeyAv[key] = attributeMap[key]
			}

			wreqs = append(wreqs, types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: mappingTableKeyAv,
				},
			})

			if len(wreqs) == perBatchRecordSize-1 || err == io.EOF {
				notifyTask(wreqs)

				wreqs = nil
			}
			readLine += 1
		}

		close(inputDone)
	}()

	writed := 0
	done := false
	for {
		select {
		case result := <-results:
			writed += result.Count()
			if done {
				fmt.Printf("\rreadLine:(done)%d, deleteLine: %d", readLine, writed)
			} else {
				fmt.Printf("\rreadLine: %d, deleteLine: %d", readLine, writed)
			}

			if result.Error() != nil {
				return errors.Wrap(result.Error(), "batch write err")
			} else {
				atomic.AddInt64(&remainedCount, -1)
			}
		case <-inputDone:
			done = true
			if remainedCount == 0 {
				return nil
			}
		}
	}
}

type TruncateResult struct {
	count int
	error error
}

func (t *TruncateResult) Error() error {
	return t.error
}

func (t *TruncateResult) Count() int {
	return t.count
}

type TruncateTask struct {
	tableName string
	req       *dynamodb.BatchWriteItemInput
	ctx       context.Context
}

func (t *TruncateTask) Run() Result {
	r, err := ddbClient.BatchWriteItem(t.ctx, t.req)
	result := &RestoreResult{}

	if err != nil {
		result.error = err
	}

	if err == nil {
		if r != nil && len(r.UnprocessedItems) > 0 {
			result.count = len(t.req.RequestItems[t.tableName]) - len(r.UnprocessedItems)
		} else {
			result.count = len(t.req.RequestItems[t.tableName])
		}
	}

	return result
}
