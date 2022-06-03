package ddbrew

import (
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"time"
)

type BatchWriteConsoleOpt struct {
	Table     *Table
	File      *os.File
	DDBAction DDBAction
	LimitUnit *int
}

type BatchResult struct {
	Content *BatchWriteOutput
	Error   error
}

func batchWriteWithConsle(opt *BatchWriteConsoleOpt) error {
	generator := BatchRequestGenerator{}
	generator.Init(&BatchRequestGeneratorOption{
		File:   opt.File,
		Action: opt.DDBAction,
		Table:  opt.Table,
	})

	results := make(chan *BatchResult)
	done := make(chan struct{})
	reqs := make(chan BatchRequest)

	procs := runtime.NumCPU()

	for i := 0; i < procs; i++ {
		go worker(reqs, results)
	}

	var remainCount int64 = 0

	if opt.LimitUnit == nil {
		go func() {
			for {
				batchReq, err := generator.generate(0)
				if err != nil {
					if batchReq.Number() == 0 && err == ErrBatchEOF {
						close(done)

						break
					}
				}
				reqs <- batchReq
				atomic.AddInt64(&remainCount, 1)
			}
		}()
	} else {
		go func() {
			ticker := time.NewTicker(1 * time.Second)

			for {
				<-ticker.C
				limit := *opt.LimitUnit

				for {
					reqUnitSize := DEFAULT_UNIT_SIZE
					if limit < reqUnitSize {
						reqUnitSize = limit
					}

					batchReq, err := generator.generate(reqUnitSize)
					if err != nil {
						if batchReq.Number() == 0 && err == ErrBatchEOF {
							close(done)

							break
						}
					}
					reqs <- batchReq
					atomic.AddInt64(&remainCount, 1)

					limit -= batchReq.totalWU
					if limit == 0 {
						break
					}
				}
			}
		}()
	}

	successNum, unprocessedNum := 0, 0
	for {
		select {
		case result := <-results:
			if result != nil {
				atomic.AddInt64(&remainCount, -1)

				if result.Content != nil {
					successNum += result.Content.SuccessCount
					unprocessedNum += len(result.Content.UnprocessedRecord)

					fmt.Printf("\r%d, %d", successNum, unprocessedNum)
				}

				if result.Error != nil {
					return result.Error
				}
			}
		case <-done:
			if remainCount == 0 {
				return nil
			}
		}
	}

}
