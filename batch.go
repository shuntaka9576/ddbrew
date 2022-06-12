package ddbrew

import (
	"context"
	"os"
	"runtime"
	"sync/atomic"
	"time"
)

const (
	RETRY_NUMBER = 3
)

type BatchWriter struct {
	Table       *Table
	File        *os.File
	DDBAction   DDBAction
	LimitUnit   *int
	RemainCount *int64
	Results     chan *BatchResult
	Done        chan struct{}
}

type BatchResult struct {
	Content BatchWriteOutput
	Error   error
}

func (b *BatchWriter) BatchWrite(ctx context.Context) error {
	generator := BatchRequestGenerator{}
	generator.Init(&BatchRequestGeneratorOption{
		File:   b.File,
		Action: b.DDBAction,
		Table:  b.Table,
	})

	reqs := make(chan BatchRequest)

	procs := runtime.NumCPU()

	for i := 0; i < procs; i++ {
		go worker(ctx, reqs, b.Results)
	}

	if b.LimitUnit == nil {
		go func() {
		LOOP:
			for {
				select {
				case <-ctx.Done():
					break LOOP
				default:
					batchReq, err := generator.generate(0)
					batchReq.Retry = RETRY_NUMBER

					if err != nil {
						if batchReq.Number() == 0 && err == ErrBatchEOF {
							close(b.Done)

							break LOOP
						}
					}
					reqs <- batchReq
					atomic.AddInt64(b.RemainCount, 1)
				}
			}
		}()
	} else {
		go func() {
			ticker := time.NewTicker(1 * time.Second)

		LOOP:
			for {
				select {
				case <-ctx.Done():
					break LOOP
				case <-ticker.C:
					limit := *b.LimitUnit

					for {
						reqUnitSize := DEFAULT_UNIT_SIZE
						if limit < reqUnitSize {
							reqUnitSize = limit
						}

						batchReq, err := generator.generate(reqUnitSize)
						if err != nil {
							if batchReq.Number() == 0 && err == ErrBatchEOF {
								close(b.Done)

								break LOOP
							}
						}
						reqs <- batchReq
						atomic.AddInt64(b.RemainCount, 1)

						limit -= batchReq.totalWU
						if limit == 0 {
							break
						}
					}
				}
			}
		}()
	}

	return nil
}
