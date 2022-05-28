package ddbrew

import (
	"bytes"
	"os"
)

var READ_BUF_SIZE int = 4000

type HeadResult struct {
	LineConut int
	Lines     []byte
}

func head(f *os.File, n int) (*HeadResult, error) {
	defer f.Seek(0, 0)

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := int(info.Size())

	readLinePos := 0
	readPos := 0
	bufferLineNumber := 0
	var headLinesBuffer []byte

	for readLinePos <= n {
		var buf []byte

		var readSize int
		if size-readPos-READ_BUF_SIZE >= 0 {
			readSize = READ_BUF_SIZE
		} else {
			readSize = size - readPos
		}

		buf = make([]byte, readSize)

		f.Seek(0, readPos)

		_, err := f.Read(buf)
		if err != nil {
			return nil, err
		}

		lfCount := bytes.Count(buf, []byte("\n"))

		wantLines := n - readLinePos - 1
		if lfCount >= n-readLinePos {
			for i := 0; i <= len(buf)-1; i++ {

				if buf[i] == 10 {
					bufferLineNumber += 1

					if wantLines == 0 {
						headLinesBuffer = append(headLinesBuffer, buf[:i+1]...)

						break
					}
					wantLines -= 1
				}
			}
		} else {
			headLinesBuffer = append(headLinesBuffer, buf...)
			bufferLineNumber += lfCount
		}

		if size == readPos+readSize {
			break
		} else {
			readPos += readSize
			readLinePos += lfCount
		}
	}

	return &HeadResult{
		LineConut: bufferLineNumber,
		Lines:     headLinesBuffer,
	}, nil
}
