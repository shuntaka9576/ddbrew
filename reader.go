package ddbrew

import (
	"bufio"
	"io"
)

func countLines(reader io.Reader) (lines int) {
	re := bufio.NewReader(reader)

	for {
		_, err := re.ReadString('\n')
		if err != nil {
			if err == io.EOF {

				break
			}
		}
		lines += 1
	}

	return lines
}
