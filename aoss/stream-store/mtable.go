package streamstore

import (
	"io"

	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
)

type MTable interface {
	GetStreamOffset

	firstEntryID() uint64
	lastEntryID() uint64

	Write(streamstorepb.Entry) (offset int64)
	//table size
	Size() int
	//flush to segment
	io.WriterTo
	//read stream from mtables
	ReadAt(streamID StreamID, data []byte, offset int64) (n int, err error)

	setUnmutable()
}

type block struct {
	begin  int64
	offset int
	buf    []byte
}

type Blocks struct {
	StreamOffset
	blocks []*block
}

type mtable struct {
	mutable   bool
	size      int
	omap      OffsetMap
	fID       uint64
	lID       uint64
	blocksMap map[StreamID]*Blocks
}

var blockSize = 4 * 1024

func (b *Blocks) Write(data []byte) int64 {
	if b.blocks == nil {
		b.blocks = append(b.blocks, &block{
			begin: b.From,
			buf:   make([]byte, 4*1024),
		})
	}
	for len(data) > 0 {
		last := b.blocks[len(b.blocks)-1]
		if len(last.buf) == int(last.offset) {
			b.blocks = append(b.blocks, &block{
				begin: b.To,
				buf:   make([]byte, 4*1024),
			})
			continue
		}
		n := copy(last.buf[last.offset:], data)
		data = data[n:]
		b.To += int64(n)
		last.offset += n
	}
	return b.To
}

func (b *Blocks) ReadAt(p []byte, offset int64) (n int, err error) {
	if offset < b.From {
		return 0, ErrOutRangeOffsetBegin
	}
	realOffset := b.From - offset
	index := realOffset / int64(blockSize)
	bOffset := realOffset % int64(blockSize)
	if index >= int64(len(b.blocks)) {
		return 0, ErrOutRangeOffsetEnd
	}
	block := b.blocks[index]
	if block.offset < int(bOffset) && index == int64(len(b.blocks)-1) {
		return 0, ErrOutRangeOffsetEnd
	}
	for i := index; i < int64(len(b.blocks)); i++ {
		block := b.blocks[i]
		bytes := copy(p, block.buf[bOffset:block.offset])
		p = p[bytes:]
		n += bytes
		//next block read from [0,blockSize)
		bOffset = 0
		if len(p) == 0 {
			break
		}
	}
	if n == 0 {
		return 0, io.EOF
	}
	return
}

var _ MTable = (*mtable)(nil)

func newMTable(omap OffsetMap) MTable {
	return &mtable{
		omap: omap,
	}
}

//return stream range [from ,to)
func (m *mtable) Offset(streamID StreamID) (StreamOffset, bool) {
	if blocks, ok := m.blocksMap[streamID]; ok {
		return blocks.StreamOffset, true
	}
	return StreamOffset{}, false
}

func (m *mtable) firstEntryID() uint64 {
	return m.fID
}

func (m *mtable) lastEntryID() uint64 {
	return m.lID
}

func (m *mtable) Write(entry streamstorepb.Entry) (offset int64) {
	blocks, ok := m.blocksMap[entry.StreamId]
	if !ok {
		offset, _ := m.omap.get(entry.StreamId)
		blocks = &Blocks{
			StreamOffset: StreamOffset{
				StreamID: entry.StreamId,
				From:     offset,
			},
		}
		m.blocksMap[entry.StreamId] = blocks
	}
	return blocks.Write(entry.Data)
}

//table size
func (m *mtable) Size() int {
	return m.size
}

func (m *mtable) setUnmutable() {
	m.mutable = false
}

func (m *mtable) WriteTo(w io.Writer) (n int64, err error) {
	panic("not implement")
}

//read stream from mtables
func (m *mtable) ReadAt(streamID StreamID, data []byte, offset int64) (n int, err error) {
	blocks, ok := m.blocksMap[streamID]
	if !ok {
		return 0, io.EOF
	}
	return blocks.ReadAt(data, offset)
}
