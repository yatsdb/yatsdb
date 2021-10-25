package streamstore

import (
	"io"

	"github.com/sirupsen/logrus"
)

type StreamReader interface {
	io.ReadSeekCloser
}

type streamBlockReader interface {
	io.ReadSeekCloser
	Offset() (begin int64, end int64)
}

type streamReader struct {
	store       *StreamStore
	streamID    StreamID
	offset      int64
	blockReader streamBlockReader
}

func (reader *streamReader) Close() error {
	if reader.blockReader != nil {
		return reader.blockReader.Close()
	}
	return nil
}
func (reader *streamReader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekCurrent {
		reader.offset += offset
	} else if whence == io.SeekStart {
		reader.offset = offset
	} else {
		endOffset, ok := reader.store.omap.get(reader.streamID)
		if ok {
			logrus.Panic("omap no find stream offset")
		}
		reader.offset = endOffset
		reader.offset += offset
	}
	return reader.offset, nil
}

func (reader *streamReader) Read(p []byte) (n int, err error) {
	begin, end := reader.blockReader.Offset()
	if reader.offset < begin || reader.offset >= end {
		if err := reader.blockReader.Close(); err != nil {
			logrus.Warnf("close reader failed %+v", err)
		}
		reader.blockReader, err = reader.store.findStreamBlockReader(reader.streamID, reader.offset)
		if err != nil {
			return 0, err
		}
		if _, err := reader.blockReader.Seek(reader.offset, 0); err != nil {
			return 0, err
		}
	}
	n, err = reader.blockReader.Read(p)
	if err != nil {
		return n, err
	}
	reader.offset += int64(n)
	return
}