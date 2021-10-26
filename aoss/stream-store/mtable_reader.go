package streamstore

import (
	"io"

	"github.com/pkg/errors"
)

var _ SectionReader = (*mtableReader)(nil)

type mtableReader struct {
	chunks *Chunks
	offset int64
}

func (reader *mtableReader) Close() error {
	return nil
}
func (reader *mtableReader) Offset() (begin int64, end int64) {
	return reader.chunks.From, reader.chunks.To
}

func (reader *mtableReader) Seek(offset int64, whence int) (int64, error) {
	newOffset := offset
	if whence == io.SeekStart {
	} else if whence == io.SeekCurrent {
		newOffset = reader.offset + offset
	} else if whence == io.SeekEnd {
		return 0, errors.New("mtable blocks reader no support `Seek` from `SeekEnd` of stream")
	} else {
		return 0, errors.New("`Seek` argument error")
	}

	if newOffset < reader.chunks.From {
		return 0, errors.WithStack(ErrOutOfOffsetRangeBegin)
	} else if newOffset > reader.chunks.To {
		return 0, errors.WithStack(ErrOutOfOffsetRangeEnd)
	}

	reader.offset = newOffset
	return newOffset, nil
}

func (reader *mtableReader) Read(p []byte) (n int, err error) {
	n, err = reader.chunks.ReadAt(p, reader.offset)
	if err != nil {
		if err == io.EOF {
			return
		}
		return 0, errors.WithStack(err)
	}
	reader.offset += int64(n)
	return
}
