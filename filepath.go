package ddbrew

import (
	"errors"
	"os"
	"strings"
)

func replacePathTilde(path string) (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	if strings.HasPrefix(path, homeDir) {
		path = strings.Replace(path, homeDir, "~", 1)

		return path, err
	}

	return "", errors.New("replace faild")
}
