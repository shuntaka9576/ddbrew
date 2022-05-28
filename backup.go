package ddbrew

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/pkg/errors"
)

var marks = []string{"|", "/", "-", "\\"}

func mark(i int) string {
	return marks[i%4]
}

const LIMIT_SCAN_INTERVAL = 1 * time.Second

type BackupOption struct {
	TableName string
	FilePath  string
	Limit     *int
}

func Backup(ctx context.Context, opt *BackupOption) error {
	filePath := fmt.Sprintf("backup_%s_%s.jsonl", opt.TableName, time.Now().Format("20060102-150405"))
	if opt.FilePath != "" {
		filePath = opt.FilePath
	}

	f, err := os.Create(filePath)
	if err != nil {
		fmt.Printf("create file error %s\n", err)

		return err
	}
	defer f.Close()

	fmt.Fprintf(os.Stderr, "created %s\n", f.Name())

	writer := io.MultiWriter(f)

	params := &dynamodb.ScanInput{
		TableName: &opt.TableName,
	}

	doneCh := make(chan struct{})
	errCh := make(chan error)
	valueCh := make(chan *dynamodb.ScanOutput)
	notifyCount, scanCount := 0, 0

	go func() {
		for {
			scanData := <-valueCh
			for _, item := range scanData.Items {
				parsedJl := map[string]interface{}{}
				_ = attributevalue.UnmarshalMap(item, &parsedJl)
				jsonByte, _ := json.Marshal(parsedJl)

				fmt.Fprintf(writer, string(jsonByte)+"\n")
			}

			scanCount += int(scanData.Count)
			notifyCount += 1
			fmt.Printf("\rscaned records: %s %d", mark(notifyCount), scanCount)
			if len(scanData.LastEvaluatedKey) == 0 {
				fmt.Println()
				doneCh <- struct{}{}
			}
		}
	}()

	go func() {
		if opt.Limit == nil {
			for {
				if opt.Limit == nil {
					res, err := DdbClient.Scan(ctx, params)
					if err != nil {
						errCh <- err

						break
					}

					valueCh <- res

					if len(res.LastEvaluatedKey) > 0 {
						params.ExclusiveStartKey = res.LastEvaluatedKey
					} else {
						break
					}
				}
			}
		} else {
			limitRU := int32(*opt.Limit)
			params.Limit = &limitRU

			ticker := time.NewTicker(LIMIT_SCAN_INTERVAL)
			for {
				select {
				case <-ticker.C:
					limitRU = int32(*opt.Limit)
				default:
					if limitRU > 0 {
						res, err := DdbClient.Scan(ctx, params)
						if err != nil {
							errCh <- err

							break
						}
						limitRU -= res.ScannedCount

						valueCh <- res

						if len(res.LastEvaluatedKey) > 0 {
							params.ExclusiveStartKey = res.LastEvaluatedKey
						}
					}
				}
			}
		}
	}()

	for {
		select {
		case <-doneCh:
			fmt.Println("backuped")

			return nil
		case err := <-errCh:

			return errors.Wrap(err, "backup error")
		}
	}
}
