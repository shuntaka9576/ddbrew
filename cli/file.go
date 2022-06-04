package cli

import (
	"bufio"
	"io"
	"os"
)

func countLines(f *os.File) (lines int) {
	defer f.Seek(0, 0)

	re := bufio.NewReader(f)

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
