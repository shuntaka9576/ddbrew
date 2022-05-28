package ddbrew

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type RestoreResult struct {
	count            int
	unprocessedItems []string
	error            error
}

func (t *RestoreResult) Error() error {
	return t.error
}

func (t *RestoreResult) Count() int {
	return t.count
}

func (t *RestoreResult) UnprocessedItems() []string {
	return t.unprocessedItems
}

type RestoreTask struct {
	tableName string
	req       *dynamodb.BatchWriteItemInput
	ctx       context.Context
}

func (t *RestoreTask) Run() Result {
	r, err := DdbClient.BatchWriteItem(t.ctx, t.req)
	result := &RestoreResult{}

	if err != nil {
		result.error = err
	}

	if err == nil {
		if r != nil && len(r.UnprocessedItems[t.tableName]) > 0 {
			for _, item := range r.UnprocessedItems[t.tableName] {
				parsedJl := map[string]interface{}{}
				_ = attributevalue.UnmarshalMap(item.PutRequest.Item, &parsedJl)
				jsonByte, _ := json.Marshal(parsedJl)
				result.unprocessedItems = append(result.unprocessedItems, string(jsonByte))
			}
		}
	}

	result.count = len(t.req.RequestItems[t.tableName])

	return result
}
