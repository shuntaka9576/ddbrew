package ddbrew

import (
	"errors"
	"os"
)

var (
	ErrBatchEOF = errors.New("BatchEOF")
)

type BatchRequestGeneratorOption struct {
	File   *os.File
	Action DDBAction
	Table  *Table
}

type BatchRequestGenerator struct {
	file   *os.File
	action DDBAction
	unit   int
	pos    int
	table  *Table
}

func (b *BatchRequestGenerator) Init(opt *BatchRequestGeneratorOption) *BatchRequestGenerator {
	b.file = opt.File
	b.action = opt.Action
	b.table = opt.Table

	return b
}

const (
	BATCH_RECORD_NUM_LIMIT     = 25
	RECORD_BYTE_SIZE_LIMIT     = 1000000 // 1MB
	READ_BUF_SIZE              = 1000000 // 4KB
	LF_BYTE                    = 10
	DEFAULT_UNIT_SIZE      int = 100 // 4(WWU) * 25 = 100(WWU)
)

func (b *BatchRequestGenerator) generate(unit int) (BatchRequest, error) {
	if unit == 0 {
		unit = DEFAULT_UNIT_SIZE
	}

	req := BatchRequest{
		TableName: b.table.Name,
	}

	info, err := b.file.Stat()
	if err != nil {
		return req, err
	}
	endpos := int(info.Size())

	for {
		var readSize int
		if endpos-b.pos-READ_BUF_SIZE >= 0 {
			readSize = READ_BUF_SIZE
		} else {
			readSize = endpos - b.pos
		}

		if readSize == 0 {
			return req, ErrBatchEOF
		}

		buf := make([]byte, readSize)
		_, err := b.file.Seek(int64(b.pos), 0)
		if err != nil {

			return req, err
		}

		_, err = b.file.Read(buf)
		if err != nil {
			return req, err
		}

		lastLfPos, lineStartPos := 0, 0
		isUnitOver := false
		for i := 0; i <= len(buf)-1; i++ {
			if buf[i] == LF_BYTE {
				lastLfPos = i

				item, err := b.table.DDBItemFromString(string(buf[lineStartPos:lastLfPos]), b.action)
				if err != nil {
					return req, err
				}

				if unit-item.Unit < 0 {
					isUnitOver = true

					break
				}

				unit -= item.Unit
				lineStartPos = lastLfPos + 1

				err = req.AddWriteRequest(item)
				if err != nil {
					return req, err
				}

				if req.Number() == BATCH_RECORD_NUM_LIMIT {
					break
				}
			}
		}

		if buf[len(buf)-1] != LF_BYTE {
			b.pos += lineStartPos

			return req, err
		}

		if req.Number() == BATCH_RECORD_NUM_LIMIT || isUnitOver {
			b.pos += lastLfPos + 1

			return req, err
		}

		if endpos == b.pos+readSize {
			b.pos += readSize

			return req, ErrBatchEOF
		}
	}
}
