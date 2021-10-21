package wal

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
)

type EntryIteractor interface {
	Next() (streamstorepb.Entry, error)
}

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
		return nil, err
	}
	var size = binary.BigEndian.Uint32(sizeBuffer[:])
	var buffer = make([]byte, size)
	if _, err := io.ReadFull(d, buffer[:]); err != nil {
		return nil, err
	}
	entryTyper := streamstorepb.NewEntryTyper(sizeBuffer[4])
	if err := entryTyper.Unmarshal(buffer); err != nil {
		return nil, errors.WithStack(err)
	}
	return entryTyper, nil
}

type entryIteractor struct {
	entries []streamstorepb.Entry
	Decoder
}

func newEntryIteractor(d Decoder) EntryIteractor {
	return &entryIteractor{
		Decoder: d,
	}
}

func (i *entryIteractor) Next() (streamstorepb.Entry, error) {
	if len(i.entries) != 0 {
		entry := i.entries[0]
		i.entries = i.entries[1:]
		return entry, nil
	}
	for {
		entryTyper, err := i.Decode()
		if err != nil {
			return streamstorepb.Entry{}, err
		}
		switch val := entryTyper.(type) {
		case *streamstorepb.EntryBatch:
			i.entries = val.Entries
			return i.Next()
		default:
			continue
		}
	}
}
