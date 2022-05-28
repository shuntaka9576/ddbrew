package ddbrew

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestSimulate(t *testing.T) {
	type input struct {
		ioType string
		value  string
		mode   string
	}

	var tests = []struct {
		input input
		want  *SimulateResult
	}{
		{input{"file", "./testdata/1.jsonl", "OnDemand"}, &SimulateResult{
			ConsumeRRU:    ptr(58),
			ConsumeWRU:    ptr(58),
			ConsumeRCU:    nil,
			ConsumeWCU:    nil,
			TotalItemSize: 5510,
		}},
		{input{"file", "./testdata/1.jsonl", "Provisioned"}, &SimulateResult{
			ConsumeRRU:    nil,
			ConsumeWRU:    nil,
			ConsumeRCU:    ptr(58),
			ConsumeWCU:    ptr(58),
			TotalItemSize: 5510,
		}},
		{input{"string", "{\"foo\":\"hoge\"}\n{\"foo\":\"hoge\"}", "OnDemand"}, &SimulateResult{
			ConsumeRRU:    ptr(1),
			ConsumeWRU:    ptr(1),
			ConsumeRCU:    nil,
			ConsumeWCU:    nil,
			TotalItemSize: 7,
		}},
		{input{"byte", "{\"foo\":\"hoge\"}\n{\"foo\":\"hoge\"}", "OnDemand"}, &SimulateResult{
			ConsumeRRU:    ptr(1),
			ConsumeWRU:    ptr(1),
			ConsumeRCU:    nil,
			ConsumeWCU:    nil,
			TotalItemSize: 7,
		}},
	}

	for _, test := range tests {
		var reader io.Reader
		switch test.input.ioType {
		case "file":
			reader, _ = os.Open(test.input.value)
		case "string":
			reader = strings.NewReader(test.input.value)
		case "byte":
			reader = bytes.NewReader([]byte(test.input.value))
		}
		br := bufio.NewReader(reader)

		got, err := Simulate(&SimulateOpt{Reader: br, Mode: test.input.mode})
		if err != nil {
			t.Errorf("got error\n")
		}

		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("error want %v got %v\n", test.want, *got)
		}
	}
}

func ptr[T any](v T) *T {
	return &v
}
