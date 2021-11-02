package streamstore

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
)

type Segment interface {
	GetStreamOffset
	FirstEntryID() uint64
	LastEntryID() uint64
	CreateTS() time.Time
	GetStreamOffsets() []StreamOffset

	NewReader(streamID StreamID) (SectionReader, error)

	Size() int64
	Filename() string

	io.Closer

	SetDeleteOnClose(bool)
}

type segment struct {
	footer streamstorepb.SegmentFooter
	f      *os.File
	//streambaseOffset head size
	streambaseOffset int64

	ref        int32
	delOnClose bool
}

var _ Segment = (*segment)(nil)

func newSegment(f *os.File) (*segment, error) {
	var buf = make([]byte, 4)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, errors.WithStack(err)
	}
	offset := binary.BigEndian.Uint32(buf)

	if _, err := f.Seek(int64(offset), 0); err != nil {
		return nil, errors.WithStack(err)
	}

	buf, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var header streamstorepb.SegmentFooter
	if err := header.Unmarshal(buf); err != nil {
		return nil, errors.WithStack(err)
	}

	return &segment{
		footer:           header,
		f:                f,
		streambaseOffset: 4,
		ref:              1,
	}, nil
}

//return stream range [from ,to)
func (s *segment) Offset(streamID StreamID) (StreamOffset, bool) {
	offset, ok := s.footer.StreamOffsets[uint64(streamID)]
	if ok {
		return StreamOffset{
			StreamID: streamID,
			From:     offset.From,
			To:       offset.To,
		}, ok
	}
	return StreamOffset{}, false
}

func (s *segment) FirstEntryID() uint64 {
	return s.footer.FirstEntryId
}

func (s *segment) LastEntryID() uint64 {
	return s.footer.LastEntryId
}

func (s *segment) CreateTS() time.Time {
	return time.Unix(s.footer.CreateTS/1e9, s.footer.CreateTS%1e9)
}

func (s *segment) GetStreamOffsets() []StreamOffset {
	var offsets = make([]StreamOffset, 0, len(s.footer.StreamOffsets))
	for _, offset := range s.footer.StreamOffsets {
		offsets = append(offsets, StreamOffset{
			StreamID: offset.StreamId,
			From:     offset.From,
			To:       offset.To,
		})
	}
	return offsets
}
func (s *segment) NewReader(streamID StreamID) (SectionReader, error) {
	offset, ok := s.footer.StreamOffsets[uint64(streamID)]
	if !ok {
		return nil, errors.New("no find streamID in segment")
	}
	//inc ref+1
	for {
		ref := atomic.LoadInt32(&s.ref)
		if ref <= 0 {
			return nil, fmt.Errorf("segment is closed")
		}
		if atomic.CompareAndSwapInt32(&s.ref, ref, ref+1) {
			break
		}
	}
	return &segmentReader{
		segment: s,
		soffset: offset,
		offset:  offset.From,
	}, nil
}

func (s *segment) Size() int64 {
	stat, err := s.f.Stat()
	if err != nil {
		logrus.WithError(err).
			Panicf("get file stat failed")
	}
	return stat.Size()
}
func (s *segment) Filename() string {
	return s.f.Name()
}

func (s *segment) SetDeleteOnClose(val bool) {
	s.delOnClose = val
}
func (s *segment) Close() error {
	for {
		ret := atomic.LoadInt32(&s.ref)
		if ret <= 0 {
			panic("ref error")
		}
		if atomic.CompareAndSwapInt32(&s.ref, ret, ret-1) {
			if ret-1 > 0 {
				return nil
			}
			break
		}
	}
	if err := s.f.Close(); err != nil {
		return errors.WithStack(err)
	}
	if err := os.Remove(s.Filename()); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func WriteSegment(m *mtable, ws io.WriteSeeker) error {
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
