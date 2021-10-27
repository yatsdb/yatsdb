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

var errSeekInvalidOffset = errors.New("Seek: invalid offset")
var errSeekInvalidWhence = errors.New("Seek: invalid whence")

func (reader *mtableReader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart {
	} else if whence == io.SeekCurrent {
		offset += reader.offset
	} else if whence == io.SeekEnd {
		return 0, errors.New("no support seek whence `io.SeekEnd`")
	} else {
		return 0, errSeekInvalidWhence
	}

	if offset < reader.chunks.From {
		return 0, errors.WithStack(errSeekInvalidOffset)
	}
	reader.offset = offset
	return offset, nil
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
