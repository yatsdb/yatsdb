package streamstore

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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

var (
	ErrOutOfOffsetRangeBegin = errors.New("out of offset range begin")
	ErrOutOfOffsetRangeEnd   = errors.New("out of offset range end")
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

	wg sync.WaitGroup
}

type AppendCallbackFn = func(offset int64, err error)

func Open(options Options) (*StreamStore, error) {
	ctx, cancel := context.WithCancel(context.Background())
	var ss = &StreamStore{
		Options:       options,
		ctx:           ctx,
		cancel:        cancel,
		appendEntryCh: make(chan appendEntry, 1024),
		mtableMtx:     sync.Mutex{},
		omap:          newOffsetMap(),
		mTables:       &[]MTable{},
		callbackCh:    make(chan func(), 64*1024),
		flushTableCh:  make(chan MTable, 4),
		segmentLocker: sync.RWMutex{},
	}
	for _, dir := range []string{options.SegmentDir, options.WalOptions.Dir} {
		if err := os.MkdirAll(dir, 0777); err != nil {
			if err != os.ErrExist {
				return nil, errors.WithStack(err)
			}
		}
	}
	err := filepath.Walk(options.SegmentDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, segmentTempExt) {
			if err := os.Remove(path); err != nil {
				return errors.WithStack(err)
			}
			logrus.Infof("delete segment temp file %s", path)
			return nil
		} else if strings.HasSuffix(path, segmentExt) {
			f, err := os.Open(path)
			if err != nil {
				return errors.WithStack(err)
			}
			segment, err := newSegment(f)
			if err != nil {
				logrus.Panicf("newSegment %s failed %+v", path, err)
			}
			ss.segments = append(ss.segments, segment)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(ss.segments, func(i, j int) bool {
		return ss.segments[i].LastEntryID() < ss.segments[j].LastEntryID()
	})

	var lastEntryID uint64
	for _, segment := range ss.segments {
		for _, soffset := range segment.GetStreamOffsets() {
			ss.omap.set(soffset.StreamID, soffset.To)
		}
		lastEntryID = segment.LastEntryID()
	}

	ss.mtable = newMTable(ss.omap)
	ss.appendMtable(ss.mtable)
	ss.wg.Add(3)
	ss.startWriteEntryRoutine()
	ss.startFlushMTableRoutine()
	ss.startCallbackRoutine()

	var wg sync.WaitGroup
	var reloadCount int64
	var begin = time.Now()
	ss.wal, err = wal.Reload(options.WalOptions, func(e streamstorepb.Entry) error {
		if e.ID <= lastEntryID {
			return nil
		}
		wg.Add(1)
		reloadCount++
		ss.appendEntryCh <- appendEntry{
			entry: e,
			fn: func(offset int64, err error) {
				wg.Done()
				if err != nil {
					logrus.Panicf("append entry failed %+v", err)
				}
			},
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	wg.Wait()
	logrus.Infof("reload wal success count %d take time %s", reloadCount, time.Since(begin))
	return ss, nil
}

func (ss *StreamStore) AppendSync(streamID StreamID, data []byte) (offset int64, err error) {
	ch := make(chan struct {
		offset int64
		err    error
	})
	ss.Append(streamID, data, func(offset int64, err error) {
		ch <- struct {
			offset int64
			err    error
		}{
			offset: offset,
			err:    err,
		}
	})
	res := <-ch
	return res.offset, res.err
}
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
		defer ss.wg.Done()
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

func (ss *StreamStore) createSegment(mtable MTable) string {
	begin := time.Now()
	tempfile := filepath.Join(ss.Options.SegmentDir,
		strconv.FormatUint(mtable.LastEntryID(), 10)+segmentTempExt)
	f, err := os.Create(tempfile)
	if err != nil {
		logrus.WithError(err).Panicf("create file failed")
	}
	if err := mtable.WriteToSegment(f); err != nil {
		logrus.WithField("filename", tempfile).
			WithError(err).Panicf("write segment failed")
	}
	if err := f.Close(); err != nil {
		logrus.WithField("filename", tempfile).
			WithError(err).Panicf("close segment failed")
	}
	filename := filepath.Join(ss.Options.SegmentDir,
		strconv.FormatUint(mtable.FirstEntryID(), 10)+segmentExt)
	if err := os.Rename(tempfile, filename); err != nil {
		logrus.WithField("from", tempfile).
			WithField("to", filename).
			WithError(err).Panicf("rename failed")
	}
	logrus.WithField("filename", filename).
		WithField("elapsed", time.Since(begin)).
		Infof("flush MTable to segment success")
	return filename
}

func (ss *StreamStore) clearFirstSegmentWithLock() {
	first := ss.segments[0]
	copy(ss.segments, ss.segments[1:])
	ss.segments[len(ss.segments)-1] = nil
	ss.segments = ss.segments[:len(ss.segments)-1]

	filename := first.Filename()
	if err := first.Close(); err != nil {
		logrus.WithField("filename", filename).
			Panicf("close segment failed %s", err.Error())
	}
	if err := os.Remove(filename); err != nil {
		logrus.WithField("filename", filename).
			Panicf("remove segment failed")
	}
	logrus.WithField("filename", filename).Infof("clear segment file")
}
func (ss *StreamStore) clearSegments() {
	ss.segmentLocker.Lock()
	defer ss.segmentLocker.Unlock()

	for len(ss.segments) > 0 {
		if time.Since(ss.segments[0].CreateTS()) > ss.Retention.Time {
			ss.clearFirstSegmentWithLock()
			continue
		}
		break
	}

	for len(ss.segments) > 0 {
		var size int64
		for _, s := range ss.segments {
			size += s.Size()
		}
		if size > ss.Retention.Size {
			ss.clearFirstSegmentWithLock()
			continue
		}
		break
	}
}
func (ss *StreamStore) flushMTable(mtable MTable) {
	filename := ss.createSegment(mtable)
	segment, err := ss.openSegment(filename)
	if err != nil {
		logrus.Panicf("open segment failed %+v", err)
	}
	ss.updateSegments(segment)
	ss.clearMTable()
	ss.wal.ClearLogFiles(mtable.LastEntryID())
	ss.clearSegments()
}

func (ss *StreamStore) openSegment(filename string) (Segment, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, errors.Errorf("open file %s failed %s", filename, err.Error())
	}
	return newSegment(f)
}

func (ss *StreamStore) updateSegments(segment Segment) {
	ss.segmentLocker.Lock()
	ss.segments = append(ss.segments, segment)
	ss.segmentLocker.Unlock()
}

//clearMTable remove mtable reduce memory using
func (ss *StreamStore) clearMTable() {
	mTables := ss.getMtables()
	if len(mTables) > ss.MaxMTables {
		newTables := append([]MTable{}, (mTables)[1:]...)
		ss.updateTables(newTables)
	}
}

func (ss *StreamStore) startFlushMTableRoutine() {
	go func() {
		defer ss.wg.Done()
		for {
			select {
			case mtable := <-ss.flushTableCh:
				ss.flushMTable(mtable)
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

func (ss *StreamStore) updateTables(tables []MTable) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&ss.mTables)), unsafe.Pointer(&tables))
}
func (ss *StreamStore) getMtables() []MTable {
	return *(*[]MTable)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&ss.mTables))))
}
func (ss *StreamStore) appendMtable(mtable MTable) {
	mTables := (*[]MTable)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&ss.mTables))))
	newTables := append(append([]MTable{}, *mTables...), mtable)
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&ss.mTables)), unsafe.Pointer(&newTables))
}

func (ss *StreamStore) writeEntry(entry appendEntry) int64 {
	offset := ss.mtable.Write(entry.entry)
	ss.omap.set(entry.entry.StreamId, offset)
	if ss.mtable.Size() < ss.MaxMemTableSize {
		return offset
	}
	logrus.WithField("stream_count", ss.mtable.StreamCount()).
		WithField("alloc_size", ss.mtable.ChunkAllocSize()).
		WithField("size", ss.mtable.Size()).
		Infof("mtable stat")
	unmutTable := ss.mtable
	ss.mtable = newMTable(ss.omap)
	ss.appendMtable(ss.mtable)
	ss.asyncFlushMtable(unmutTable)
	return offset
}
func (ss *StreamStore) startWriteEntryRoutine() {
	go func() {
		defer ss.wg.Done()
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

func (ss *StreamStore) newReader(streamID StreamID, offset int64) (SectionReader, error) {
	mTables := ss.getMtables()
	if i := SearchMTables(mTables, streamID, offset); i != -1 {
		return mTables[i].NewReader(streamID)
	}

	ss.segmentLocker.RLock()
	if i := SearchSegments(ss.segments, streamID, offset); i != -1 {
		segment := ss.segments[i]
		ss.segmentLocker.RUnlock()
		return segment.NewReader(streamID)
	}
	ss.segmentLocker.RUnlock()
	return nil, io.EOF
}

func (ss *StreamStore) NewReader(streamID StreamID) (io.ReadSeekCloser, error) {
	if _, ok := ss.omap.get(streamID); !ok {
		return nil, io.EOF
	}
	blockReader, err := ss.newReader(streamID, 0)
	if err != nil {
		return nil, err
	}
	from, _ := blockReader.Offset()
	return &streamReader{
		streamID:      streamID,
		store:         ss,
		sectionReader: blockReader,
		offset:        from,
	}, nil
}

func (ss *StreamStore) Close() error {
	if err := ss.wal.Close(); err != nil {
		return err
	}
	ss.cancel()
	ss.wg.Wait()
	for _, segment := range ss.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	ss.updateTables([]MTable{})
	return nil
}
