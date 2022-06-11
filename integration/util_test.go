package integration_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	ONDEMAND_TABLE_NAME = "DdbrewPrimaryOnDemand"
	RECORD_NUM          = 1000
	TESTDATA_PATH       = "./testdata/1.jsonl"
)

type Record struct {
	Pk        string `json:"pk"`
	Sk        string `json:"sk"`
	Number    int    `json:"number"`
	CreatedAt int64  `json:"createdAt"`
	UpdatedAt int64  `json:"updatedAt"`
}

func createBackupTestData(tableName string) error {
	client := InitClient(&ClientOption{})

	f, err := os.Open(TESTDATA_PATH)
	if err != nil {
		return err
	}

	reader := bufio.NewReader(f)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		pJson := map[string]interface{}{}
		json.Unmarshal([]byte(line), &pJson)

		av, err := attributevalue.MarshalMap(pJson)
		if err != nil {
			return err
		}

		_, err = client.PutItem(context.TODO(), &dynamodb.PutItemInput{
			TableName: &tableName,
			Item:      av,
		})

		if err != nil {
			return err
		}
	}

	return nil
}

func cleanBackupTestData(tableName string) error {
	client := InitClient(&ClientOption{})

	f, err := os.Open(TESTDATA_PATH)
	if err != nil {
		return err
	}

	reader := bufio.NewReader(f)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		pJson := map[string]interface{}{}
		json.Unmarshal([]byte(line), &pJson)

		av, err := attributevalue.MarshalMap(pJson)
		if err != nil {
			return err
		}

		var keys = map[string]types.AttributeValue{}
		keys["pk"] = av["pk"]

		_, err = client.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
			TableName: &tableName,
			Key:       keys,
		})

		if err != nil {
			return err
		}
	}

	return nil
}

type ClientOption struct {
	Local string
}

func InitClient(opt *ClientOption) *dynamodb.Client {
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

	return dynamodb.NewFromConfig(cfg)
}

func checkAndFixURLSchema(endpoint string) string {
	if strings.HasPrefix(endpoint, "https://") || strings.HasPrefix(endpoint, "http://") {
		return endpoint
	}

	return "http://" + endpoint
}
