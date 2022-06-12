package ddbrew

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type DDBClient struct {
	*dynamodb.Client
}

var DdbClient *DDBClient

type DDBClientOption struct {
	Local string
}

func checkAndFixURLSchema(endpoint string) string {
	if strings.HasPrefix(endpoint, "https://") || strings.HasPrefix(endpoint, "http://") {
		return endpoint
	}

	return "http://" + endpoint
}

func InitClient(opt *DDBClientOption) error {

	var cfg aws.Config
	var err error

	endpoint := checkAndFixURLSchema(opt.Local)

	if opt.Local != "" {
		cfg, err = config.LoadDefaultConfig(context.TODO(),
			config.WithEndpointResolver(aws.EndpointResolverFunc(
				func(service, region string) (aws.Endpoint, error) {
					return aws.Endpoint{URL: endpoint}, nil
				})))
		if err != nil {
			log.Fatalf("unable to load SDK config, %v", err)
		}
	} else {
		cfg, err = config.LoadDefaultConfig(context.TODO())
		if err != nil {
			log.Fatalf("unable to load SDK config, %v", err)
		}
	}

	DdbClient = &DDBClient{
		Client: dynamodb.NewFromConfig(cfg),
	}

	return nil
}

type BatchWriteOutput struct {
	SuccessCount      int
	UnprocessedRecord []string
}

func (d *DDBClient) BatchWrite(ctx context.Context, req BatchRequest) (output BatchWriteOutput, err error) {
	res, err := d.BatchWriteItem(ctx, req.BatchWriteItemInput())

	if err != nil {
		return output, err
	}

	unprocessedNum := len(res.UnprocessedItems[req.TableName])

	var reTryResult BatchWriteOutput
	if unprocessedNum > 0 {
		if req.Retry > 0 {
			time.Sleep(1 * time.Second)

			retryReq := BatchRequest{
				TableName:     req.TableName,
				WriteRequests: res.UnprocessedItems[req.TableName],
				Retry:         req.Retry - 1,
			}

			reTryResult, err = d.BatchWrite(ctx, retryReq)
			if err != nil {
				return output, err
			}
		} else if req.Retry == 0 {
			for _, item := range res.UnprocessedItems[req.TableName] {
				parsedJl := map[string]interface{}{}

				if item.PutRequest != nil {
					err = attributevalue.UnmarshalMap(item.PutRequest.Item, &parsedJl)
					if err != nil {
						continue
					}

				}

				if item.DeleteRequest != nil {
					err = attributevalue.UnmarshalMap(item.DeleteRequest.Key, &parsedJl)
					if err != nil {
						continue
					}
				}

				jsonByte, err := json.Marshal(parsedJl)
				if err != nil {
					return output, err
				}

				output.UnprocessedRecord = append(output.UnprocessedRecord, string(jsonByte))
				output.SuccessCount = req.Number() - unprocessedNum

				return output, err
			}
		} else {
			return output, errors.New("undefined retry value")
		}
	}

	output.SuccessCount = req.Number() - unprocessedNum + reTryResult.SuccessCount
	output.UnprocessedRecord = reTryResult.UnprocessedRecord

	return output, nil
}
