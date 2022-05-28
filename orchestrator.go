package ddbrew

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	MAX_ORCHESTRATOR_QUEUE_SIZE int = 10
)

type WriteItem struct {
	tableName string
	item      types.WriteRequest
	ru        int
	wu        int
	size      int
	eol       bool
}

type WriteOrchestrator struct {
	Ctx           context.Context
	TableName     string
	WriteItems    <-chan *WriteItem
	Tasks         chan<- Task
	LimitUnit     *int
	remainedCount *int64
	inputDone     chan<- struct{}
	queueSize     *int
}

type WriteRequestPerSec = [][]types.WriteRequest

const WRITE_INTERVAL = 1 * time.Second

func (w *WriteOrchestrator) Run() {
	ticker := time.NewTicker(WRITE_INTERVAL)
	batchReqSize := 0

	wrs := []types.WriteRequest{}
	consumeRu := 0
	writeRequestPerSec := WriteRequestPerSec{}
	writeRequestListPerSec := []WriteRequestPerSec{}
	stopReadLine := false
	isTaskNotFound := false

	for {
	LOOP:
		select {
		case wi := <-w.WriteItems:
			if w.LimitUnit == nil {
				if wi != nil {
					if len(wrs) == BATCH_WRITE_PER_RECORD-1 || batchReqSize >= BATCH_WRITE_PER_CAPACITY {
						requestItems := map[string][]types.WriteRequest{}
						requestItems[w.TableName] = wrs
						req := &dynamodb.BatchWriteItemInput{
							RequestItems: requestItems,
						}

						w.Tasks <- &RestoreTask{
							tableName: w.TableName,
							req:       req,
							ctx:       w.Ctx,
						}
						wrs = nil

						atomic.AddInt64(w.remainedCount, 1)
					}

					wrs = append(wrs, wi.item)
				} else {
					requestItems := map[string][]types.WriteRequest{}
					requestItems[w.TableName] = wrs
					req := &dynamodb.BatchWriteItemInput{
						RequestItems: requestItems,
					}
					w.Tasks <- &RestoreTask{
						tableName: w.TableName,
						req:       req,
						ctx:       w.Ctx,
					}
					atomic.AddInt64(w.remainedCount, 1)

					close(w.inputDone)
					break LOOP
				}

				batchReqSize += wi.size
			} else {
				if wi != nil {
					if len(wrs) == BATCH_WRITE_PER_RECORD-1 || batchReqSize >= BATCH_WRITE_PER_CAPACITY || consumeRu >= *w.LimitUnit {
						writeRequestPerSec = append(writeRequestPerSec, wrs)
						wrs = nil
					}

					if consumeRu >= *w.LimitUnit || isTaskNotFound {
						isTaskNotFound = false
						writeRequestListPerSec = append(writeRequestListPerSec, writeRequestPerSec)
						writeRequestPerSec = nil
						consumeRu = 0
					}

					wrs = append(wrs, wi.item)
					batchReqSize += wi.size
					consumeRu += wi.ru
				} else {
					if len(wrs) > 0 {
						writeRequestPerSec = append(writeRequestPerSec, wrs)
						writeRequestListPerSec = append(writeRequestListPerSec, writeRequestPerSec)
					}

					stopReadLine = true
				}
			}
		case <-ticker.C:
			if len(writeRequestListPerSec) > 0 {
				writeRequestPerSec = writeRequestListPerSec[0]
				writeRequestListPerSec = writeRequestListPerSec[1:]

				for _, writeRequest := range writeRequestPerSec {
					requestItems := map[string][]types.WriteRequest{}
					requestItems[w.TableName] = writeRequest

					req := &dynamodb.BatchWriteItemInput{
						RequestItems: requestItems,
					}
					w.Tasks <- &RestoreTask{
						tableName: w.TableName,
						req:       req,
						ctx:       w.Ctx,
					}
					atomic.AddInt64(w.remainedCount, 1)
				}

				if stopReadLine && len(writeRequestListPerSec) == 0 {
					close(w.inputDone)

					break LOOP
				}
			} else {
				isTaskNotFound = true
			}

			queueSize := len(writeRequestListPerSec)
			w.queueSize = &queueSize
		}
	}
}
