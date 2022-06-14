package timer

import (
	"fmt"
	"testing"
)

func TestTimer(t *testing.T) {
	ti := &Timer{}
	ti.Start()

	a := ti.Estimated(100000, 100)
	fmt.Println(a)
}
