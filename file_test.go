package ddbrew

import (
	"bytes"
	"os"
	"testing"
)

func TestHead(t *testing.T) {
	type input struct {
		filePath string
		N        int
	}
	var tests = []struct {
		input input
		want  int
	}{
		{input{"./testdata/1.jsonl", 1}, 1},
		{input{"./testdata/1.jsonl", 3}, 3},
		{input{"./testdata/1.jsonl", 5}, 5},
		{input{"./testdata/1.jsonl", 10}, 10},
		{input{"./testdata/1.jsonl", 50}, 50},
		{input{"./testdata/1.jsonl", 58}, 58},
		{input{"./testdata/1.jsonl", 60}, 58},
		{input{"./testdata/1.jsonl", 100}, 58},
	}

	for _, test := range tests {
		f, _ := os.Open(test.input.filePath)
		defer f.Close()

		got, _ := head(f, test.input.N)
		gotLf := bytes.Count(got.Lines, []byte("\n"))

		if gotLf != test.want {
			t.Errorf("error want lines %d got %d\n", test.want, gotLf)
		}

		if got.LineConut != test.want {
			t.Errorf("error want lines count %d got %d\n", test.want, got.LineConut)
		}
	}
}
