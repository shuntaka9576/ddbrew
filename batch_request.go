package ddbrew

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type BatchRequest struct {
	TableName     string
	WriteRequests []types.WriteRequest
	totalWU       int
	ByteSize      int
	Retry         int
}

func (b *BatchRequest) AddWriteRequest(ddbItem DDBItem) error {
	if ddbItem.ByteSize >= WRITE_LIMIT_BYTE_SIZE {
		return errors.New("byte size over")
	}

	b.WriteRequests = append(b.WriteRequests, ddbItem.WriteReuest())
	b.totalWU += ddbItem.Unit
	b.ByteSize += ddbItem.ByteSize

	return nil
}

func (b *BatchRequest) Reset() {
	b.WriteRequests = nil
	b.totalWU = 0
}

func (b *BatchRequest) Number() int {
	return len(b.WriteRequests)
}

func (b *BatchRequest) Size() int {
	return b.ByteSize
}

func (b *BatchRequest) BatchWriteItemInput() *dynamodb.BatchWriteItemInput {
	requestItems := map[string][]types.WriteRequest{}

	requestItems[b.TableName] = b.WriteRequests

	return &dynamodb.BatchWriteItemInput{
		RequestItems: requestItems,
	}
}
