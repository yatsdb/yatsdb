package streamstore

import (
	"encoding/binary"
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
)

type MTable interface {
	GetStreamOffset

	FirstEntryID() uint64

	//
	LastEntryID() uint64

	//Write write entry to memory,and return end of stream
	Write(streamstorepb.Entry) (offset int64)
	//table size
	Size() int
	//flush to segment
	WriteToSegment(wser io.WriteSeeker) error

	NewReader(streamID StreamID) (SectionReader, error)
}

type chunk struct {
	begin  int64
	offset int
	buf    []byte
}

type Chunks struct {
	sync.RWMutex
	StreamOffset
	chunks []*chunk
}

type mtable struct {
	sync.RWMutex
	size         int
	omap         OffsetMap
	fristEntryID uint64
	lastEntryID  uint64
	chunksMap    map[StreamID]*Chunks
}

var blockSize = 4 * 1024

func (b *Chunks) WriteTo(w io.Writer) (n int64, err error) {
	b.RLock()
	for _, block := range b.chunks {
		if r, err := w.Write(block.buf[:block.offset]); err != nil {
			return 0, errors.WithStack(err)
		} else {
			n += int64(r)
		}
	}
	b.RUnlock()
	return
}
func (b *Chunks) Write(data []byte) int64 {
	b.Lock()
	if b.chunks == nil {
		b.chunks = append(b.chunks, &chunk{
			begin: b.From,
			buf:   make([]byte, blockSize),
		})
	}
	for len(data) > 0 {
		last := b.chunks[len(b.chunks)-1]
		if len(last.buf) == int(last.offset) {
			b.chunks = append(b.chunks, &chunk{
				begin: b.To,
				buf:   make([]byte, blockSize),
			})
			continue
		}
		n := copy(last.buf[last.offset:], data)
		data = data[n:]
		b.To += int64(n)
		last.offset += n
	}
	b.Unlock()
	return b.To
}

func (b *Chunks) ReadAt(p []byte, offset int64) (n int, err error) {
	if offset < b.From {
		return 0, ErrOutOfOffsetRangeBegin
	}
	b.RLock()
	realOffset := offset - b.From
	index := realOffset / int64(blockSize)
	bOffset := realOffset % int64(blockSize)
	if index >= int64(len(b.chunks)) {
		b.RUnlock()
		return 0, io.EOF
	}
	block := b.chunks[index]
	if block.offset < int(bOffset) && index == int64(len(b.chunks)-1) {
		b.RUnlock()
		return 0, io.EOF
	}
	for i := index; i < int64(len(b.chunks)); i++ {
		block := b.chunks[i]
		bytes := copy(p, block.buf[bOffset:block.offset])
		p = p[bytes:]
		n += bytes
		//next block read from [0,blockSize)
		bOffset = 0
		if len(p) == 0 {
			break
		}
	}
	b.RUnlock()
	if n == 0 {
		return 0, io.EOF
	}
	return
}

var _ MTable = (*mtable)(nil)

func newMTable(omap OffsetMap) MTable {
	return &mtable{
		omap:      omap,
		chunksMap: make(map[invertedindex.StreamID]*Chunks, 1024*1024),
	}
}

//return stream range [from ,to)
func (m *mtable) Offset(streamID StreamID) (StreamOffset, bool) {
	if blocks, ok := m.chunksMap[streamID]; ok {
		return blocks.StreamOffset, true
	}
	return StreamOffset{}, false
}

func (m *mtable) FirstEntryID() uint64 {
	return m.fristEntryID
}

func (m *mtable) LastEntryID() uint64 {
	return m.lastEntryID
}

func (m *mtable) Write(entry streamstorepb.Entry) (offset int64) {
	m.Lock()
	chunks, ok := m.chunksMap[entry.StreamId]
	if !ok {
		offset, _ := m.omap.get(entry.StreamId)
		chunks = &Chunks{
			StreamOffset: StreamOffset{
				StreamID: entry.StreamId,
				From:     offset,
				To:       offset,
			},
		}
		m.chunksMap[entry.StreamId] = chunks
	}
	m.size += len(entry.Data)
	if m.fristEntryID == 0 {
		m.fristEntryID = entry.ID
	}
	m.lastEntryID = entry.ID
	m.Unlock()
	return chunks.Write(entry.Data)
}

//table size
func (m *mtable) Size() int {
	return m.size
}

func (m *mtable) WriteToSegment(ws io.WriteSeeker) error {
	if _, err := ws.Write(make([]byte, 4)); err != nil {
		return errors.WithStack(err)
	}
	var offset int64
	offset = 4
	var footer = streamstorepb.SegmentFooter{
		CreateTS:      time.Now().UnixNano(),
		StreamOffsets: map[uint64]streamstorepb.StreamOffset{},
		FirstEntryId:  m.fristEntryID,
		LastEntryId:   m.lastEntryID,
	}
	for streamID, blocks := range m.chunksMap {
		footer.StreamOffsets[uint64(streamID)] = streamstorepb.StreamOffset{
			StreamId: streamID,
			From:     blocks.From,
			To:       blocks.To,
			Offset:   offset,
		}
		n, err := blocks.WriteTo(ws)
		if err != nil {
			return err
		}
		offset += int64(n)
	}
	data, err := footer.Marshal()
	if err != nil {
		return errors.WithStack(err)
	}
	if _, err := ws.Write(data); err != nil {
		return errors.WithStack(err)
	}
	var header = make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(offset))
	if _, err := ws.Seek(0, 0); err != nil {
		return errors.WithStack(err)
	}
	if _, err := ws.Write(header); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (m *mtable) NewReader(streamID StreamID) (SectionReader, error) {
	m.RLock()
	bs, ok := m.chunksMap[streamID]
	if !ok {
		m.RUnlock()
		return nil, errors.New("no find stream blocks")
	}
	m.RUnlock()
	return &mtableReader{
		chunks: bs,
		offset: bs.From,
	}, nil
}
