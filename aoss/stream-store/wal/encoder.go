package wal

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
)

type Encoder interface {
	Encode(streamstorepb.EntryTyper) error
	Reset(writer io.Writer)
}

type encoder struct {
	io.Writer
}

func newEncoder(writer io.Writer) Encoder {
	return &encoder{
		Writer: writer,
	}
}

func (e *encoder) Reset(writer io.Writer) {
	e.Writer = writer
}
func (e *encoder) Encode(entry streamstorepb.EntryTyper) error {
	size := entry.Size()
	buffer := make([]byte, size+4+1)
	binary.BigEndian.PutUint32(buffer, uint32(size))
	buffer = buffer[4:]
	buffer[0] = entry.Type()
	buffer = buffer[1:]
	if _, err := entry.MarshalTo(buffer); err != nil {
		return errors.WithStack(err)
	}
	if _, err := e.Write(buffer); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
