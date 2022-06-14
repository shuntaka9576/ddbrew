package timer

import (
	"time"
)

type Timer struct {
	startTime time.Time
}

func (t *Timer) Start() {
	t.startTime = time.Now()
}

func (t *Timer) Estimated(recordCount int, writedCount int) string {
	now := time.Now()
	diff := now.Sub(t.startTime)

	perWriteMills := diff / time.Duration(writedCount)
	// pp.Println(perWriteMills)
	remainCount := recordCount - writedCount
	estimateDuration := perWriteMills * time.Duration(remainCount)

	return estimateDuration.String()
}
