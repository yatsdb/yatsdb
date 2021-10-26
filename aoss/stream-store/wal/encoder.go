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

func newEncoder(writer io.Writer) *encoder {
	return &encoder{
		Writer: writer,
	}
}

func (e *encoder) Reset(writer io.Writer) {
	e.Writer = writer
}
func (e *encoder) Encode(entry streamstorepb.EntryTyper) error {
	size := entry.Size()
	buf := make([]byte, size+4+1)
	data := buf
	binary.BigEndian.PutUint32(buf, uint32(size))
	buf = buf[4:]
	buf[0] = entry.Type()
	buf = buf[1:]
	if _, err := entry.MarshalTo(buf); err != nil {
		return errors.WithStack(err)
	}
	if _, err := e.Write(data); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
