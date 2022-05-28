package ddbrew

import (
	"context"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
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
