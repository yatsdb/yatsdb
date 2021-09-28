package aoss

import (
	"io"
)

type StreamAppender interface {
	Append(streamID uint64, data []byte, fn func(offset int64, err error))
}

type StreamReader interface {
	NewReader(streamID uint64) io.ReadSeekCloser
}

type InvertedIndex interface {
	StreamAppender
	StreamReader
}
