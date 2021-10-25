package streamstore

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
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
	writeSegment(f *os.File) error
	//read stream from mtables
	ReadAt(streamID StreamID, data []byte, offset int64) (n int, err error)

	setUnmutable()

	newStreamBlockReader(streamID StreamID) (streamBlockReader, error)
}

type block struct {
	begin  int64
	offset int
	buf    []byte
}

type Blocks struct {
	disableRWLocker
	StreamOffset
	mtx    sync.RWMutex
	blocks []*block
}

type mtable struct {
	disableRWLocker
	size      int
	omap      OffsetMap
	fID       uint64
	lID       uint64
	blocksMap map[StreamID]*Blocks
}

var blockSize = 4 * 1024

func (b *Blocks) WriteTo(w io.Writer) (n int, err error) {
	b.RLock()
	for _, block := range b.blocks {
		if r, err := w.Write(block.buf[:block.offset]); err != nil {
			return 0, errors.WithStack(err)
		} else {
			n += r
		}
	}
	b.RUnlock()
}
func (b *Blocks) Write(data []byte) int64 {
	b.Lock()
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
	b.Unlock()
	return b.To
}

func (b *Blocks) ReadAt(p []byte, offset int64) (n int, err error) {
	if offset < b.From {
		return 0, ErrOutOfOffsetRangeBegin
	}
	b.RLock()
	realOffset := b.From - offset
	index := realOffset / int64(blockSize)
	bOffset := realOffset % int64(blockSize)
	if index >= int64(len(b.blocks)) {
		b.RUnlock()
		return 0, ErrOutOfOffsetRangeEnd
	}
	block := b.blocks[index]
	if block.offset < int(bOffset) && index == int64(len(b.blocks)-1) {
		b.RUnlock()
		return 0, ErrOutOfOffsetRangeEnd
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
	b.RUnlock()
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
	m.Lock()
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
	m.Unlock()
	return blocks.Write(entry.Data)
}

//table size
func (m *mtable) Size() int {
	return m.size
}

func (m *mtable) setUnmutable() {
	m.setDisable()
	for _, blocks := range m.blocksMap {
		blocks.setDisable()
	}
}

func (m *mtable) writeSegment(f *os.File) error {
	if _, err := f.Write(make([]byte, 4)); err != nil {
		return errors.WithStack(err)
	}
	var offset int64
	offset = 4
	var footer = streamstorepb.SegmentFooter{
		CreateTS:      time.Now().UnixNano(),
		StreamOffsets: map[uint64]streamstorepb.StreamOffset{},
		FirstEntryId:  m.fID,
		LastEntryId:   m.lID,
	}
	for streamID, blocks := range m.blocksMap {
		footer.StreamOffsets[uint64(streamID)] = streamstorepb.StreamOffset{
			StreamId: streamID,
			From:     blocks.From,
			To:       blocks.To,
			Offset:   offset,
		}
		n, err := blocks.WriteTo(f)
		if err != nil {
			return err
		}
		offset += int64(n)
	}
	data, err := footer.Marshal()
	if err != nil {
		return errors.WithStack(err)
	}
	if _, err := f.Write(data); err != nil {
		return errors.WithStack(err)
	}
	var header = make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(offset))
	if _, err := f.WriteAt(header, 0); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

//read stream from mtables
func (m *mtable) ReadAt(streamID StreamID, data []byte, offset int64) (n int, err error) {
	m.RLock()
	blocks, ok := m.blocksMap[streamID]
	if !ok {
		m.RUnlock()
		return 0, io.EOF
	}
	m.RUnlock()
	return blocks.ReadAt(data, offset)
}

func (m *mtable) newStreamBlockReader(streamID StreamID) (streamBlockReader, error) {
	m.RLock()
	bs, ok := m.blocksMap[streamID]
	if !ok {
		m.RUnlock()
		return nil, errors.New("no find stream blocks")
	}
	m.RUnlock()
	return &mtableBlockReader{
		blocks: bs,
		offset: bs.From,
	}, nil
}

type disableRWLocker struct {
	mtx     sync.RWMutex
	disable bool
}

func (m *disableRWLocker) RLock() {
	if !m.disable {
		m.mtx.RLock()
	}
}

func (m *disableRWLocker) RUnlock() {
	if !m.disable {
		m.mtx.RUnlock()
	}
}
func (m *disableRWLocker) Lock() {
	if !m.disable {
		m.mtx.Lock()
	}
}
func (m *disableRWLocker) Unlock() {
	if !m.disable {
		m.mtx.Unlock()
	}
}
func (m *disableRWLocker) setDisable() {
	m.mtx.Lock()
	m.disable = true
	m.mtx.Unlock()
}
