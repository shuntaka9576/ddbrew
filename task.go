package ddbrew

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

const (
	TASK_TYPE_RESTORE = "TASK_NAME_RESTORE"
	TASK_TYPE_DELETE  = "TASK_TYPE_DELETE"
)

type Result struct {
	count            int
	unprocessedItems []string
	error            error
}

func (t *Result) Error() error {
	return t.error
}

func (t *Result) Count() int {
	return t.count
}

func (t *Result) UnprocessedItems() []string {
	return t.unprocessedItems
}

type Task struct {
	taskType  string
	tableName string
	req       *dynamodb.BatchWriteItemInput
	ctx       context.Context
}

func (t *Task) Run() Result {
	r, err := DdbClient.BatchWriteItem(t.ctx, t.req)
	result := Result{}

	if err != nil {
		result.error = err
	}

	if err == nil {
		if r != nil && len(r.UnprocessedItems[t.tableName]) > 0 {
			for _, item := range r.UnprocessedItems[t.tableName] {
				parsedJl := map[string]interface{}{}

				if t.taskType == TASK_TYPE_RESTORE {
					err = attributevalue.UnmarshalMap(item.PutRequest.Item, &parsedJl)
				} else if t.taskType == TASK_TYPE_DELETE {
					err = attributevalue.UnmarshalMap(item.DeleteRequest.Key, &parsedJl)
				}
				if err != nil {
					result.error = err
				}

				jsonByte, err := json.Marshal(parsedJl)
				if err != nil {
					result.error = err
				}
				result.unprocessedItems = append(result.unprocessedItems, string(jsonByte))
			}
		}
	}

	result.count = len(t.req.RequestItems[t.tableName])

	return result
}
