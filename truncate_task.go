package ddbrew

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

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
	r, err := DdbClient.BatchWriteItem(t.ctx, t.req)
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
