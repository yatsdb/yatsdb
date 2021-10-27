package aoss

import (
	"io"

	filestreamstore "github.com/yatsdb/yatsdb/aoss/file-stream-store"
	streamstore "github.com/yatsdb/yatsdb/aoss/stream-store"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
)

type StreamID = invertedindex.StreamID

type StreamAppender interface {
	Append(streamID StreamID, data []byte, fn func(offset int64, err error))
}

type StreamReader interface {
	NewReader(streamID StreamID) (io.ReadSeekCloser, error)
}

type StreamStore interface {
	io.Closer
	StreamAppender
	StreamReader
}

var _ StreamStore = (*filestreamstore.FileStreamStore)(nil)
var _ StreamStore = (*streamstore.StreamStore)(nil)
