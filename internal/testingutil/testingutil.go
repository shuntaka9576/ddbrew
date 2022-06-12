package testingutil

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
)

const (
	RECORD_NUM    = 1000
	TESTDATA_PATH = "./testdata/1.jsonl"
)

var (
	onDemandTableName = "DdbrewPrimaryOnDemand"
	pkName            = "pk"
	skName            = "sk"
)

type Record struct {
	Pk        string `json:"pk"`
	Sk        string `json:"sk"`
	Number    int    `json:"number"`
	CreatedAt int64  `json:"createdAt"`
	UpdatedAt int64  `json:"updatedAt"`
}

type TableOption struct {
	Mode      types.BillingMode
	Secondary bool
}

func CreateTable(opt *TableOption) (tableName string, err error) {
	client := InitClient(&ClientOption{})
	keySchema := []types.KeySchemaElement{
		{
			AttributeName: &pkName,
			KeyType:       types.KeyTypeHash,
		},
	}

	var input *dynamodb.CreateTableInput

	if opt.Mode == types.BillingModePayPerRequest && !opt.Secondary {
		tableName = onDemandTableName
		input = &dynamodb.CreateTableInput{
			TableName:   &tableName,
			KeySchema:   keySchema,
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: &pkName, AttributeType: types.ScalarAttributeTypeS},
			},
		}
	} else {
		return "", errors.New("create not supported")
	}

	_, err = client.CreateTable(context.TODO(), input)
	if err != nil {
		return "", err
	}

	for {
		describe, err := client.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{TableName: &onDemandTableName})
		if err != nil {
			return "", err
		}

		if describe.Table.TableStatus == types.TableStatusActive {
			fmt.Printf("\rtable created")

			break
		} else if describe.Table.TableStatus == types.TableStatusCreating {
			fmt.Printf("\rtable creating...")
		}

		time.Sleep(500 * time.Millisecond)
	}

	return onDemandTableName, nil
}

func DeleteTable() error {
	client := InitClient(&ClientOption{})
	input := &dynamodb.DeleteTableInput{
		TableName: &onDemandTableName,
	}

	_, err := client.DeleteTable(context.TODO(), input)
	if err != nil {
		return err
	}

	for {
		describe, err := client.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{TableName: &onDemandTableName})
		if err != nil {
			var rnfe *types.ResourceNotFoundException
			if errors.As(err, &rnfe) {
				return nil
			} else {
				return err
			}
		}

		if describe.Table.TableStatus == types.TableStatusDeleting {
			fmt.Printf("\rtable deleting...")
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func CreateBackupTestData() error {
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
			TableName: &onDemandTableName,
			Item:      av,
		})

		if err != nil {
			return err
		}
	}

	return nil
}

func CleanBackupTestData() error {
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
			TableName: &onDemandTableName,
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
