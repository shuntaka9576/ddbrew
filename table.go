package ddbrew

import "github.com/aws/aws-sdk-go-v2/service/dynamodb"

func GetDDBMode(o *dynamodb.DescribeTableOutput) *DDBMode {
	if o.Table.BillingModeSummary == nil {
		return &Provisioned
	} else if o.Table.BillingModeSummary.BillingMode == "PAY_PER_REQUEST" {
		return &OnDemand
	}

	return nil
}
