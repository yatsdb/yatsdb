package streamstore

import (
	"encoding/binary"
	"io"
	"sort"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/sirupsen/logrus"
	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
	"github.com/yatsdb/yatsdb/pkg/utils"
)

type StreamSegmentOffset struct {
	StreamID StreamID
	From     int64
	To       int64
	Offset   int64
}

const SSOffsetSize = int(unsafe.Sizeof(StreamSegmentOffset{}))

type SegmentV1 struct {
	header    streamstorepb.SegmentV1Header
	SSOffsets []StreamSegmentOffset
	mfile     *fileutil.MmapFile
	filename  string
	filesize  int64

	ref *utils.Ref
}

var validSegmentSize = true

func openSegmentV1(filename string) (*SegmentV1, error) {
	mfile, err := fileutil.OpenMmapFile(filename)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	data := mfile.Bytes()
	if len(data) < 4 {
		return nil, errors.New("segment file format error")
	}
	filesize := len(data)
	headerSize := binary.BigEndian.Uint32(data)
	data = data[4:]
	if len(data) < int(headerSize) {
		return nil, errors.New("segment file format error")
	}

	var header streamstorepb.SegmentV1Header
	if err := header.Unmarshal(data[:headerSize]); err != nil {
		return nil, errors.Wrap(err, "unmarshal segment Header failed")
	}
	data = data[headerSize:]

	var segment = &SegmentV1{
		header:    header,
		SSOffsets: []StreamSegmentOffset{},
		mfile:     mfile,
		filesize:  int64(filesize),
		filename:  filename,
	}

	segment.ref = utils.NewRef(segment.refRelease)
	if len(data) > 0 {
		utils.UnsafeSlice(unsafe.Pointer(&segment.SSOffsets),
			unsafe.Pointer(&data[0]), int(header.StreamCount))
		if len(segment.SSOffsets) != 0 {
			ssoffsetSize := SSOffsetSize * len(segment.SSOffsets)
			data = data[ssoffsetSize:]
		}
	}
	if validSegmentSize {
		var size int64
		for _, offset := range segment.SSOffsets {
			size += offset.To - offset.From
		}
		if size != int64(len(data)) {
			return nil, errors.New("segment data size invalid")
		}
	}
	return segment, nil
}

//return stream range [from ,to)
func (segment *SegmentV1) Offset(streamID StreamID) (StreamOffset, bool) {
	offset, ok := segment.offset(streamID)
	if ok {
		return StreamOffset{
			StreamID: streamID,
			From:     offset.From,
			To:       offset.To,
		}, true
	}
	return StreamOffset{}, false
}
func (segment *SegmentV1) offset(streamID StreamID) (StreamSegmentOffset, bool) {
	i := sort.Search(int(segment.header.StreamCount), func(i int) bool {
		return streamID <= segment.SSOffsets[i].StreamID
	})
	if i < int(segment.header.StreamCount) &&
		streamID == segment.SSOffsets[i].StreamID {
		return segment.SSOffsets[i], true
	}
	return StreamSegmentOffset{}, false
}

func (segment *SegmentV1) FirstEntryID() uint64 {
	return segment.header.FirstEntryId
}

func (segment *SegmentV1) LastEntryID() uint64 {
	return segment.header.LastEntryId
}

func (segment *SegmentV1) CreateTS() time.Time {
	return utils.UnixMilliLocal(segment.header.GetCreateTs())
}

func (segment *SegmentV1) GetStreamOffsets() []StreamOffset {
	var offsets []StreamOffset
	for _, offset := range segment.SSOffsets {
		offsets = append(offsets, StreamOffset{
			StreamID: offset.StreamID,
			From:     offset.From,
			To:       offset.To,
		})
	}
	return offsets
}

func (segment *SegmentV1) NewReader(streamID StreamID) (SectionReader, error) {
	meta, ok := segment.offset(streamID)
	if !ok {
		return nil, errors.New("no find streamID")
	}
	if !segment.ref.Inc() {
		return nil, errors.New("segment close")
	}
	size := meta.To - meta.From
	return &SegmentV1Reader{
		data:   segment.mfile.Bytes()[meta.Offset : meta.Offset+size],
		offset: 0,
		meta:   meta,
		ref:    segment.ref,
	}, nil
}

func (segment *SegmentV1) Size() int64 {
	return segment.filesize
}

func (segment *SegmentV1) Filename() string {
	return segment.filename
}

func (segment *SegmentV1) refRelease() {
	if err := segment.mfile.Close(); err != nil {
		logrus.WithFields(logrus.Fields{
			"filename": segment.filename,
			"err":      err,
		}).Panic("close segment failed")
	}
}
func (segment *SegmentV1) Close() error {
	segment.ref.DecRef()
	return nil
}

func WriteSegmentV1(m *mtable, ws io.WriteSeeker) error {
	var header = streamstorepb.SegmentV1Header{
		Merges:       0,
		FirstEntryId: m.fristEntryID,
		LastEntryId:  m.lastEntryID,
		CreateTs:     time.Now().UnixMilli(),
		StreamCount:  uint64(len(m.chunksMap)),
	}
	HData, err := header.Marshal()
	if err != nil {
		return errors.WithStack(err)
	}
	var HSizeBuf = make([]byte, 4)
	binary.BigEndian.PutUint32(HSizeBuf, uint32(len(HData)))
	if _, err := ws.Seek(0, 0); err != nil {
		return errors.WithStack(err)
	}
	//write header size
	if _, err := ws.Write(HSizeBuf); err != nil {
		return errors.WithStack(err)
	}
	//write header
	if _, err := ws.Write(HData); err != nil {
		return errors.WithStack(err)
	}
	var offset int64
	offset += 4
	offset += int64(len(HData))

	var offsetIndexes []StreamSegmentOffset
	for streamID, blocks := range m.chunksMap {
		offsetIndexes = append(offsetIndexes, StreamSegmentOffset{
			StreamID: streamID,
			From:     blocks.From,
			To:       blocks.To,
		})
	}
	sort.Slice(offsetIndexes, func(i, j int) bool {
		return offsetIndexes[i].StreamID < offsetIndexes[j].StreamID
	})
	indexOffset := offset
	//offset index
	indexSize := int64(len(offsetIndexes) * SSOffsetSize)
	indexBuf := make([]byte, indexSize)
	//skip index block
	offset += indexSize
	if _, err := ws.Seek(offset, 0); err != nil {
		return errors.WithStack(err)
	}
	for i := 0; i < len(offsetIndexes); i++ {
		meta := &offsetIndexes[i]
		meta.Offset = offset
		blocks := m.chunksMap[meta.StreamID]
		n, err := blocks.WriteTo(ws)
		if err != nil {
			return err
		}
		offset += int64(n)
		*(*StreamSegmentOffset)(unsafe.Pointer(&indexBuf[i*int(SSOffsetSize)])) = *meta
	}

	if _, err := ws.Seek(indexOffset, 0); err != nil {
		return errors.WithStack(err)
	}
	if _, err := ws.Write(indexBuf); err != nil {
		return errors.WithStack(err)
	}

	return nil
}
