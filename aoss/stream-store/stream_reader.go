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
	streamID StreamID
	store    *StreamStore

	currReader streamBlockReader

	offset int64
}

func (reader *streamReader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekCurrent {
		reader.offset += offset
	} else if whence == io.SeekStart {
		reader.offset = offset
	} else {
		endOffset, ok := reader.store.omap.get(reader.streamID)
		if !ok {
			logrus.Panic("omap no find stream offset")
		}
		reader.offset = endOffset
		reader.offset += offset
	}
	return reader.offset, nil
}

func (reader *streamReader) findStreamReader() (streamBlockReader, error) {
	var err error
	reader.currReader, err = reader.store.findStreamBlockReader(reader.streamID, reader.offset)
	if err != nil {
		return nil, err
	}
	return reader.currReader, err
}

func (reader *streamReader) Read(p []byte) (n int, err error) {
	if reader.currReader != nil {
		if begin, end := reader.currReader.Offset(); reader.offset >= end || reader.offset < begin {
			reader.currReader, err = reader.findStreamReader()
			if err != nil {
				return 0, err
			}
		}
	}
	n, err = reader.currReader.Read(p)
	if err != nil {
		return n, err
	}
	reader.offset += int64(n)
	return
}
