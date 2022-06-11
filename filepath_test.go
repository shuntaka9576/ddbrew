package ddbrew

import (
	"os"
	"testing"
)

func Test_replacePathTildeOK(t *testing.T) {
	var tests = []struct {
		input string
		want  string
	}{
		{
			input: "/Users/foo/repos/github.com/shuntaka9576/ddbrew/a.jsonl",
			want:  "~/repos/github.com/shuntaka9576/ddbrew/a.jsonl",
		},
	}

	os.Setenv("HOME", "/Users/foo")

	for _, test := range tests {

		got, err := replacePathTilde(test.input)

		if err != nil {
			t.Error(err)
		}

		if got != test.want {
			t.Errorf("got %s, want %s", got, test.want)
		}
	}
}

func Test_replacePathTildeNG_HOMEisNotDefined(t *testing.T) {
	var tests = []struct {
		input string
		want  string
	}{
		{
			input: "/Users/foo/repos/github.com/shuntaka9576/ddbrew",
			want:  "$HOME is not defined",
		},
	}

	os.Setenv("HOME", "")

	for _, test := range tests {
		_, err := replacePathTilde(test.input)
		if err.Error() != test.want {
			t.Errorf("got %s, want %s", err.Error(), test.want)
		}
	}
}

func Test_replacePathTildeNG_pathNotIncludeHome(t *testing.T) {
	var tests = []struct {
		input string
		want  string
	}{
		{
			input: "/etc/foo.jsonl",
			want:  "replace faild",
		},
	}

	os.Setenv("HOME", "/Users/foo")

	for _, test := range tests {
		_, err := replacePathTilde(test.input)
		if err.Error() != test.want {
			t.Errorf("got %s, want %s", err.Error(), test.want)
		}
	}
}
