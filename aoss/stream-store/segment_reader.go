package streamstore

import (
	"io"
	"os"

	"github.com/pkg/errors"
	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
)

var _ SectionReader = (*segmentReader)(nil)

type segmentReader struct {
	soffset streamstorepb.StreamOffset
	f       *os.File
	offset  int64
}

func (reader *segmentReader) Close() error {
	if err := reader.f.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
func (reader *segmentReader) Offset() (begin int64, end int64) {
	return reader.soffset.From, reader.soffset.To
}

func (reader *segmentReader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart {

	} else if whence == io.SeekCurrent {
		offset += reader.offset
	} else if whence == io.SeekEnd {
		return 0, errors.New("segment block reader no support Seek from end of stream")
	} else {
		return 0, errors.New("`Seek` argument error")
	}
	if offset < reader.soffset.From {
		return 0, errSeekInvalidOffset
	}
	reader.offset = offset
	_, err := reader.f.Seek(offset-reader.soffset.From+reader.soffset.Offset, 0)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return reader.offset, nil
}

func (reader *segmentReader) Read(p []byte) (n int, err error) {
	if reader.offset >= reader.soffset.To {
		return 0, io.EOF
	}
	remain := reader.soffset.To - reader.offset
	if int(remain) < len(p) {
		p = p[:remain]
	}
	n, err = reader.f.Read(p)
	if err != nil {
		if err == io.EOF {
			return
		}
		return 0, errors.WithStack(err)
	}
	reader.offset += int64(n)
	return
}
