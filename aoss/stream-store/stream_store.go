package streamstore

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
	"github.com/yatsdb/yatsdb/aoss/stream-store/wal"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
)

type StreamID = invertedindex.StreamID

type appendEntry struct {
	entry streamstorepb.Entry
	fn    AppendCallbackFn
}

type StreamStore struct {
	Options
	ctx           context.Context
	cancel        context.CancelFunc
	wal           wal.Wal
	appendEntryCh chan appendEntry

	mtableMtx sync.Mutex
	mtable    MTable
	omap      OffsetMap
	//mTables tables
	mTables *[]MTable

	callbackCh chan func()

	flushTableCh chan MTable

	segmentLocker sync.RWMutex
	segments      []Segment
}

type AppendCallbackFn = func(offset int64, err error)

func (ss *StreamStore) Append(streamID StreamID, data []byte, fn AppendCallbackFn) {
	ss.wal.Write(streamstorepb.Entry{
		StreamId: streamID,
		Data:     data,
	}, func(ID uint64, err error) {
		ss.appendEntry(streamstorepb.Entry{
			StreamId: streamID,
			Data:     data,
			ID:       ID,
		}, fn)
	})
}

func (ss *StreamStore) appendEntry(entry streamstorepb.Entry, fn AppendCallbackFn) {
	select {
	case ss.appendEntryCh <- appendEntry{
		entry: entry,
		fn:    fn,
	}:
	case <-ss.ctx.Done():
		fn(0, ss.ctx.Err())
	}
}

func (ss *StreamStore) startCallbackRoutine() {
	go func() {
		for {
			select {
			case cb := <-ss.callbackCh:
				cb()
			case <-ss.ctx.Done():
				return
			}
		}
	}()
}

func (ss *StreamStore) asyncCallback(fn func()) {
	select {
	case ss.callbackCh <- fn:
	case <-ss.ctx.Done():
		return
	}
}

const (
	segmentTempExt = ".segment.temp"
	segmentExt     = ".segment"
)

func (ss *StreamStore) flushMTable(mtable MTable) string {
	begin := time.Now()
	tempfile := filepath.Join(ss.Options.SegmentDir, strconv.FormatUint(mtable.firstEntryID(), 10)+segmentTempExt)
	f, err := os.Create(tempfile)
	if err != nil {
		logrus.Panicf("create file failed %s", err.Error())
	}
	if _, err := mtable.WriteTo(f); err != nil {
		logrus.Panicf("write mtable to file %s failed %+v", tempfile, err)
	}

	filename := filepath.Join(ss.Options.SegmentDir, strconv.FormatUint(mtable.firstEntryID(), 10)+segmentExt)
	if err := os.Rename(tempfile, filename); err != nil {
		logrus.Panicf("remove %s to %s failed %s", tempfile, filename, err.Error())
	}
	logrus.Infof("flush mtable to %s success toke times %s", filename, time.Since(begin))
	return filename
}

func (ss *StreamStore) openSegment(filename string) (Segment, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, errors.Errorf("open file %s failed %s", filename, err.Error())
	}
	return newSegment(f), nil
}

func (ss *StreamStore) updateSegmentIndex(segment Segment) {

}
func (ss *StreamStore) startFlushMTableRoutine() {
	go func() {
		for {
			select {
			case mtable := <-ss.flushTableCh:
				filename := ss.flushMTable(mtable)
				segment, err := ss.openSegment(filename)
				if err != nil {
					logrus.Panicf("open segment failed %+v", err)
				}
				ss.updateSegmentIndex(segment)
			case <-ss.ctx.Done():
				return
			}
		}
	}()
}

func (ss *StreamStore) asyncFlushMtable(mtable MTable) {
	select {
	case ss.flushTableCh <- mtable:
	case <-ss.ctx.Done():
	}
}

func (ss *StreamStore) writeEntry(entry appendEntry) int64 {
	offset := ss.mtable.Write(entry.entry)
	ss.omap.set(entry.entry.StreamId, offset)
	if ss.mtable.Size() < ss.MaxMemTableSize {
		return offset
	}
	unmutTable := ss.mtable
	ss.mtable = newMTable(ss.omap)

	mTables := (*[]MTable)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&ss.mTables))))
	newTables := append(append([]MTable{}, *mTables...), ss.mtable)
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&ss.mTables)), unsafe.Pointer(&newTables))

	ss.asyncFlushMtable(unmutTable)
	return offset
}
func (ss *StreamStore) startWriteEntryRoutine() {
	go func() {
		for {
			select {
			case entry := <-ss.appendEntryCh:
				offset := ss.writeEntry(entry)
				ss.asyncCallback(func() {
					entry.fn(offset, nil)
				})
			case <-ss.ctx.Done():
				return
			}
		}
	}()
}

func (ss *StreamStore) findStreamBlockReader(streamID StreamID, offset int64) (streamBlockReader, error) {
	ss.segmentLocker.RLock()
	i := SearchSegments(ss.segments, streamID, offset)
	if i != -1 {
		segment := ss.segments[i]
		ss.segmentLocker.RUnlock()
		return newSegmentBlockReader(streamID, segment), nil
	}
	ss.segmentLocker.RUnlock()

	mTables := *(*[]MTable)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&ss.mTables))))
	i = SearchMTables(mTables, streamID, offset)
	if i != -1 {
		return newMtableBlockReader(streamID, mTables[i]), nil
	}
	return nil, io.EOF
}

func (ss *StreamStore) NewStreamReader() (StreamReader, error) {
	panic("not implement")
}
