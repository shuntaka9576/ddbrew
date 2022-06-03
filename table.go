package ddbrew

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type DDBMode = string

var (
	OnDemand    = DDBMode("OnDemand")
	Provisioned = DDBMode("Provisioned")
)

type Table struct {
	Name string
	Keys []string
	Mode DDBMode
}

func (t *Table) Init(output *dynamodb.DescribeTableOutput) {
	t.Name = *output.Table.TableName

	for _, keyav := range output.Table.KeySchema {
		t.Keys = append(t.Keys, *keyav.AttributeName)
	}

	t.Mode = func() DDBMode {
		if output.Table.BillingModeSummary.BillingMode == "PAY_PER_REQUEST" {
			return OnDemand
		}
		return Provisioned
	}()
}

func (t *Table) DDBItemFromString(jsonString string, action DDBAction) (item DDBItem, err error) {
	pJson := map[string]interface{}{}
	err = json.Unmarshal([]byte(jsonString), &pJson)
	if err != nil {
		return item, err
	}

	size, err := GetItemSizeByJSON(pJson)
	if err != nil {
		return item, err
	}

	attribute, err := attributevalue.MarshalMap(pJson)
	if err != nil {
		return item, err
	}

	var unit int
	switch action {
	case DDB_ACTION_PUT:
		unit = size.WriteUnit
	case DDB_ACTION_DELETE:
		unit = size.WriteUnit
	}

	item = DDBItem{
		AttributeValue: attribute,
		Unit:           unit,
		Action:         action,
		Keys:           t.Keys,
		ByteSize:       size.Size,
	}

	return item, nil
}
