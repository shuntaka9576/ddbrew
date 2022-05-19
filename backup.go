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

type BackupOption struct {
	TableName    string
	ScanLimit    int
	ScanInterval int
	Output       string
	Stdout       bool
}

func Backup(ctx context.Context, opt *BackupOption) error {
	filePath := fmt.Sprintf("backup_%s_%s.jsonl", opt.TableName, time.Now().Format("20060102-150405"))
	if opt.Output != "" {
		filePath = opt.Output
	}

	f, err := os.Create(filePath)
	if err != nil {
		fmt.Printf("create file error %s\n", err)

		return err
	}
	defer f.Close()

	fmt.Fprintf(os.Stderr, "created %s\n", f.Name())

	writer := io.MultiWriter(f)

	if opt.Stdout {
		writer = io.MultiWriter(os.Stdout, f)
	}

	var limit *int32 = nil
	tmpLimit := int32(opt.ScanLimit)

	if opt.ScanLimit > 0 {
		limit = &tmpLimit
	}

	params := &dynamodb.ScanInput{
		TableName: &opt.TableName,
		Limit:     limit,
	}

	doneCh := make(chan struct{})
	errCh := make(chan error)
	valueCh := make(chan *dynamodb.ScanOutput)
	notifyCount, scanCount := 0, 0

	go func() {
		for {
			scanData := <-valueCh
			for _, v := range scanData.Items {
				parsedJl := map[string]interface{}{}
				_ = attributevalue.UnmarshalMap(v, &parsedJl)
				jsonByte, _ := json.Marshal(parsedJl)

				fmt.Fprintf(writer, string(jsonByte)+"\n")
			}

			if !opt.Stdout {
				scanCount += int(scanData.Count)
				notifyCount += 1
				fmt.Printf("\rscaned records: %s %d", mark(notifyCount), scanCount)
			}

			if len(scanData.LastEvaluatedKey) == 0 {
				fmt.Println()
				doneCh <- struct{}{}
			}
		}
	}()

	go func() {
		for {
			res, err := ddbClient.Scan(ctx, params)
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

			time.Sleep(time.Duration(opt.ScanInterval) * time.Millisecond)
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
