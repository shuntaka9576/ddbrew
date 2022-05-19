package ddbrew

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync/atomic"

	"github.com/pkg/errors"
)

var (
	DEFAULT_SAMPLING_SIZE     int = 10
	BATCH_WRITE_LIMIT_PER_REQ int = 25
	BATCH_WRITE_CAPACITY      int = 1000000 // 1MB
	WRU_UNIT                  int = 1000    // 1KB
)

type RestoreOption struct {
	TableName string
	FilePath  string
	DryRun    bool
	Procs     int
}

func Restore(ctx context.Context, opt *RestoreOption) error {
	f, err := os.Open(opt.FilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	maxProcs := runtime.NumCPU()
	if opt.Procs == 0 {
		opt.Procs = maxProcs
	} else if opt.Procs > maxProcs {
		return errors.New("The number of specified parallels exceeds rutime.CPUs().")
	}

	headInfo, err := head(f, DEFAULT_SAMPLING_SIZE)
	if err != nil {
		return err
	}

	info, _ := f.Stat()

	jsonLineSize := len(headInfo.Lines) / headInfo.LineConut
	perBatchRecordSize := BATCH_WRITE_LIMIT_PER_REQ
	perBatchRecordSizeExp := BATCH_WRITE_CAPACITY / jsonLineSize

	if perBatchRecordSizeExp < BATCH_WRITE_LIMIT_PER_REQ {
		perBatchRecordSize = perBatchRecordSizeExp
	}

	writeTimePerInterval := perBatchRecordSize * opt.Procs

	if opt.DryRun {
		printProcess(int(info.Size()), perBatchRecordSize, jsonLineSize, perBatchRecordSizeExp, writeTimePerInterval, opt.Procs)

		return nil
	}

	tasks := make(chan BatchTask)
	results := make(chan BatchResult)

	for i := 0; i < opt.Procs; i++ {
		go batchWorker(i, tasks, results)
	}

	reader := bufio.NewReader(f)

	var remainedCount int64
	inputDone := make(chan struct{})

	go func() {
		var lines []string

		notifyTask := func() {
			tasks <- BatchTask{
				tableName: opt.TableName,
				jsonLines: lines,
				ctx:       ctx,
			}
			atomic.AddInt64(&remainedCount, 1)
		}

		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					notifyTask()
					break
				}
			}
			lines = append(lines, strings.TrimSpace(line))

			if len(lines) == perBatchRecordSize-1 || err == io.EOF {
				notifyTask()

				lines = nil
			}
		}

		close(inputDone)
	}()

	for {
		select {
		case result := <-results:
			if result.Err != nil {

				return errors.Wrap(result.Err, "batch write err")
			} else {
				atomic.AddInt64(&remainedCount, -1)
			}

			if len(result.UnprocessedItems) > 0 {
				for _, v := range result.UnprocessedItems {
					fmt.Printf("%v\n", v)
				}
			}
		case <-inputDone:
			if remainedCount == 0 {
				return nil
			}
		}
	}
}

func printProcess(size int, perBatchRecordSize int, jsonLineSize int, perBatchRecordSizeExp int, writeTimePerInterval int, procs int) {
	fmt.Printf("------ Result and calc process -------\n")
	fmt.Printf("Note:\n")
	fmt.Printf("* It contains the filed name and schema. Actually excluded. Therefore, it is just a suggestion.\n")
	fmt.Printf("* The request command writes BatchWriteRequest in parallel using goroutine.\n")
	fmt.Printf("* Currently, only on-demand mode is supported..\n")
	fmt.Printf("The following are the indicators of the results of the dry run.\n")
	fmt.Println()

	fmt.Printf("* Number of times a BatchWriteRequest can be written in one BatchWriteRequest\n")
	fmt.Printf("   %d times = Batch write limit(1MB) / jsonLineSizeAverage(%d byte)\n", perBatchRecordSizeExp, perBatchRecordSizeExp)
	if perBatchRecordSizeExp > BATCH_WRITE_LIMIT_PER_REQ {
		fmt.Printf("   --> Since this is more than 25 times, the BatchWriteResquest recording limit of 25 times is adopted.\n")
	}

	fmt.Printf("* Number of writes per interval\n")
	fmt.Printf("   %d (times) = Number of records that can be written per 1 MB (%d times) * runtime.NumCPUs(%d cpus)\n", writeTimePerInterval, perBatchRecordSize, procs)

	fmt.Printf("* WRUs consumed per interval(If the appropriate data exists)\n")
	fmt.Printf("   %d (WRU) = jsonLineSizeAverage(%d byte) * perBatchWriteReq(%d records) * runtime.NumCPUs(%d cpus)/1WRU(%d byte)\n", jsonLineSize*perBatchRecordSize*procs/WRU_UNIT, jsonLineSize, perBatchRecordSize, procs, WRU_UNIT)

	fmt.Printf("* WRUs consumed when all files are written\n")
	fmt.Printf("   %d (WRU) = fileSize(%d byte) / WRU(%d byte)\n", size/WRU_UNIT, size, WRU_UNIT)

	fmt.Println()
	fmt.Printf("- Batch write requests are limited to 1 MB or less and 25 records or less.\n")
	fmt.Printf("-----------------------------------\n")
}
