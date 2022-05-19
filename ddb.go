package ddbrew

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type DDBClient struct {
	*dynamodb.Client
}

var ddbClient *DDBClient

type DDBClientOption struct {
	Local string
}

func InitClient(opt *DDBClientOption) {

	var cfg aws.Config
	var err error

	if opt.Local != "" {
		cfg, err = config.LoadDefaultConfig(context.TODO(),
			config.WithEndpointResolver(aws.EndpointResolverFunc(
				func(service, region string) (aws.Endpoint, error) {
					return aws.Endpoint{URL: opt.Local}, nil
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

	ddbClient = &DDBClient{
		Client: dynamodb.NewFromConfig(cfg),
	}
}
