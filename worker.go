package ddbrew

import (
	"context"
)

func worker(reqs <-chan BatchRequest, results chan<- *BatchResult) {
	for req := range reqs {
		res, err := DdbClient.BatchWrite(context.TODO(), req)

		results <- &BatchResult{
			Content: res,
			Error:   err,
		}
	}
}
