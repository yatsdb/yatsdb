package streamstore

import (
	"io"

	"github.com/sirupsen/logrus"
)

type StreamReader interface {
	io.ReadSeekCloser
}

type SectionReader interface {
	io.ReadSeekCloser
	//Offset return stream offset[from,to)
	Offset() (from int64, to int64)
}

type streamReader struct {
	store         *StreamStore
	streamID      StreamID
	offset        int64
	sectionReader SectionReader
}

func (reader *streamReader) Close() error {
	if reader.sectionReader != nil {
		return reader.sectionReader.Close()
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
		if !ok {
			logrus.Panic("omap no find stream offset")
		}
		reader.offset = endOffset
		reader.offset += offset
	}
	return reader.offset, nil
}

func (reader *streamReader) Read(p []byte) (n int, err error) {
	begin, end := reader.sectionReader.Offset()
	if reader.offset < begin || reader.offset >= end {
		if err := reader.sectionReader.Close(); err != nil {
			logrus.Warnf("close reader failed %+v", err)
		}
		reader.sectionReader, err = reader.store.newStreamSectionReader(reader.streamID, reader.offset)
		if err != nil {
			return 0, err
		}
		if _, err := reader.sectionReader.Seek(reader.offset, 0); err != nil {
			return 0, err
		}
	}
	n, err = reader.sectionReader.Read(p)
	if err != nil {
		return n, err
	}
	reader.offset += int64(n)
	return
}
