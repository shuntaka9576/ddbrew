package ddbrew

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type BatchTask struct {
	tableName string
	jsonLines []string
	ctx       context.Context
}

type BatchResult struct {
	Value            int64
	Task             BatchTask
	Err              error
	UnprocessedItems map[string][]types.WriteRequest
}

func batchWorker(id int, tasks <-chan BatchTask, results chan<- BatchResult) {
	for t := range tasks {
		batchReq := map[string][]types.WriteRequest{}
		var reqs []types.WriteRequest

		for _, jl := range t.jsonLines {
			m := make(map[string]interface{})
			err := json.Unmarshal([]byte(jl), &m)
			if err != nil {
				fmt.Printf("json marshal err %s\n", err)

				continue // skip record
			}

			attributeMap, err := attributevalue.MarshalMap(m)
			if err != nil {
				fmt.Printf("marshal error %s\n", err)

				continue // skip record
			}

			reqs = append(reqs, types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: attributeMap,
				},
			})
		}

		batchReq[t.tableName] = reqs

		result := BatchResult{
			Task: t,
		}

		input := &dynamodb.BatchWriteItemInput{
			RequestItems: batchReq,
		}

		writeResult, err := ddbClient.BatchWriteItem(t.ctx, input)
		if err != nil {
			result.Err = err
		}

		if writeResult != nil && len(writeResult.UnprocessedItems) > 0 {
			result.UnprocessedItems = writeResult.UnprocessedItems
		}

		results <- result
	}
}
