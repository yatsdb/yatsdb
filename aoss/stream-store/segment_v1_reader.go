package streamstore

import (
	"errors"
	"io"

	"github.com/yatsdb/yatsdb/pkg/utils"
)

type SegmentV1Reader struct {
	data   []byte
	offset int64
	meta   StreamSegmentOffset
	ref    *utils.Ref
}

func (reader *SegmentV1Reader) Read(p []byte) (n int, err error) {
	if int(reader.offset) >= len(reader.data) {
		return 0, io.EOF
	}
	n = copy(p, reader.data[reader.offset:])
	reader.offset += int64(n)
	return n, nil
}

func (reader *SegmentV1Reader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekEnd {
		return 0, errors.New("segmentV1Reader no support Seek from end")
	} else if whence == io.SeekCurrent {
		curr := reader.meta.From + reader.offset
		curr += offset
		offset = curr - reader.meta.From
	} else if whence == io.SeekStart {
		offset = offset - reader.meta.From
	} else {
		return 0, errors.New("`Seek` whence unknown")
	}
	if offset < 0 {
		return 0, errors.New("Seek: invalid offset")
	}
	reader.offset = offset
	return reader.meta.From + offset, nil
}

func (reader *SegmentV1Reader) Close() error {
	reader.ref.DecRef()
	return nil
}

//Offset return stream offset[from,to)
func (reader *SegmentV1Reader) Offset() (from int64, to int64) {
	return reader.meta.From, reader.meta.To
}
