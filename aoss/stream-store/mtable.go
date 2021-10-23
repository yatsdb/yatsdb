package streamstore

import (
	"io"

	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
)

type block struct {
	begin  uint64
	offset int
	bytes  []byte
}

type Blocks struct {
	StreamID     StreamID
	firstEntryID uint64
	lastEntryID  uint64

	blocks []block
}

func (blocks *Blocks) Write(data []byte) {

}

type MTable interface {
	firstEntryID() uint64
	lastEntryID() uint64

	Write(streamstorepb.Entry) (offset int64)
	//table size
	Size() int
	//flush to segment
	io.WriterTo
	//read stream from mtables
	ReadAt(streamID StreamID, data []byte, offset int64) (n int, err error)
}

func newMTable(OffsetMap OffsetMap) MTable {
	panic("not implemented")
}
