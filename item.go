package ddbrew

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DDBItem struct {
	Action         DDBAction
	AttributeValue map[string]types.AttributeValue
	Keys           []string
	Unit           int
	ByteSize       int
}

func (d *DDBItem) WriteReuest() types.WriteRequest {
	switch d.Action {
	case DDB_ACTION_DELETE:
		return d.DeleteRequest()
	case DDB_ACTION_PUT:
		return d.PutRequest()
	default:
		panic("invalid")
	}
}

func (d *DDBItem) PutRequest() types.WriteRequest {
	return types.WriteRequest{
		PutRequest: &types.PutRequest{
			Item: d.AttributeValue,
		},
	}
}

func (d *DDBItem) DeleteRequest() types.WriteRequest {
	mappingTableKeyAv := map[string]types.AttributeValue{}
	for _, key := range d.Keys {
		mappingTableKeyAv[key] = d.AttributeValue[key]
	}

	return types.WriteRequest{
		DeleteRequest: &types.DeleteRequest{
			Key: mappingTableKeyAv,
		},
	}
}
