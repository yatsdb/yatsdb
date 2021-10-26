package streamstore

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"syscall"
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
}

type segment struct {
	footer streamstorepb.SegmentFooter
	f      *os.File
	//streambaseOffset head size
	streambaseOffset int64
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
	fd, err := syscall.Dup(int(s.f.Fd()))
	if err != nil {
		return nil, errors.WithMessage(err, "dup file failed")
	}
	newFile := os.NewFile(uintptr(fd), s.f.Name())
	if _, err := newFile.Seek(offset.Offset, 0); err != nil {
		_ = newFile.Close()
		return nil, errors.WithMessage(err, "seek failed")
	}
	return &segmentReader{
		soffset: offset,
		offset:  offset.Offset,
		f:       newFile,
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

func (s *segment) Close() error {
	if err := s.f.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
