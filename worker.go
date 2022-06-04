package ddbrew

import (
	"context"
)

func worker(ctx context.Context, reqs <-chan BatchRequest, results chan<- *BatchResult) {
	for req := range reqs {
		res, err := DdbClient.BatchWrite(ctx, req)

		results <- &BatchResult{
			Content: res,
			Error:   err,
		}
	}
}
