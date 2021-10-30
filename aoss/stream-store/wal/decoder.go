package wal

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
)

type Decoder interface {
	Decode() (streamstorepb.EntryTyper, error)
}

type decoder struct {
	io.Reader
}

func newDecoder(reader io.Reader) Decoder {
	return &decoder{
		Reader: reader,
	}
}

func (d *decoder) Decode() (streamstorepb.EntryTyper, error) {
	var sizeBuffer [5]byte
	if _, err := io.ReadFull(d, sizeBuffer[:]); err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, errors.WithStack(err)
	}
	var size = binary.BigEndian.Uint32(sizeBuffer[:4])
	var buffer = make([]byte, size)
	if _, err := io.ReadFull(d, buffer[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, err
		}
		return nil, errors.WithStack(err)
	}
	entryTyper := streamstorepb.NewEntryTyper(sizeBuffer[4])
	if err := entryTyper.Unmarshal(buffer); err != nil {
		return nil, errors.WithStack(err)
	}
	return entryTyper, nil
}
