package streamstore

import (
	"context"
	"fmt"
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
	"github.com/yatsdb/yatsdb/aoss/stream-store/wal"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
	"github.com/yatsdb/yatsdb/pkg/metrics"
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

	segments *[]Segment

	wg             sync.WaitGroup
	mergeSegmentCh chan interface{}
}

type AppendCallbackFn = func(offset int64, err error)

func Open(options Options) (*StreamStore, error) {
	if options.BlockSize < 320 {
		options.BlockSize = 320
	}
	blockSize = options.BlockSize
	if options.CallbackRoutines == 0 {
		options.CallbackRoutines = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	var ss = &StreamStore{
		Options:        options,
		ctx:            ctx,
		cancel:         cancel,
		appendEntryCh:  make(chan appendEntry, 64*1024),
		mtableMtx:      sync.Mutex{},
		omap:           newOffsetMap(),
		mTables:        &[]MTable{},
		segments:       &[]Segment{},
		callbackCh:     make(chan func(), 64*1024),
		flushTableCh:   make(chan MTable, 4),
		wg:             sync.WaitGroup{},
		mergeSegmentCh: make(chan interface{}, 1),
	}
	tmpSegments := ss.getSegments()
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
			segment, err := openSegmentV1(path)
			if err != nil {
				logrus.Panicf("newSegment %s failed %+v", path, err)
			}
			tmpSegments = append(tmpSegments, segment)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(tmpSegments, func(i, j int) bool {
		if tmpSegments[i].FirstEntryID() == tmpSegments[j].FirstEntryID() {
			return tmpSegments[i].LastEntryID() > tmpSegments[j].LastEntryID()
		}
		return tmpSegments[i].FirstEntryID() < tmpSegments[j].FirstEntryID()
	})

	var segments []Segment
	for _, segment := range tmpSegments {
		if segments == nil {
			segments = append(segments, segment)
			continue
		}
		if segments[len(segments)-1].FirstEntryID() <= segment.FirstEntryID() &&
			segment.LastEntryID() <= segments[len(segments)-1].LastEntryID() {
			logrus.WithFields(logrus.Fields{
				"filename": segment.Filename(),
			}).Info("delete segment by merged")
			continue
		}
		segments = append(segments, segment)
	}

	if !ss.updateSegments(ss.getSegmentsPointor(), segments) {
		logrus.Panic("updateSegments failed")
	}

	var lastEntryID uint64
	for _, segment := range ss.getSegments() {
		for _, soffset := range segment.GetStreamOffsets() {
			ss.omap.set(soffset.StreamID, soffset.To)
		}
		lastEntryID = segment.LastEntryID()
	}

	ss.wg.Add(2 + ss.CallbackRoutines)

	ss.mtable = newMTable(ss.omap)
	ss.appendMtable(ss.mtable)
	ss.startWriteEntryRoutine()
	ss.startFlushMTableRoutine()
	ss.startMergeSegmentRoutine()
	for i := 0; i < ss.CallbackRoutines; i++ {
		ss.startCallbackRoutine()
	}

	metrics.SegmentSize = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "yatsdb",
		Subsystem: "stream_store",
		Name:      "segment_files",
		Help:      "size of yatsdb stream store segment files",
	}, func() float64 {
		var size int64
		for _, segment := range ss.getSegments() {
			size += segment.Size()
		}
		return float64(size)
	})

	metrics.SegmentFiles = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "yatsdb",
		Subsystem: "stream_store",
		Name:      "segment_files",
		Help:      "size of yatsdb stream store segment files",
	}, func() float64 {
		return float64(len(ss.getSegments()))
	})

	metrics.MTables = prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: "yatsdb",
		Subsystem: "stream_store",
		Name:      "mtables",
		Help:      "total mtables size",
	}, func() float64 {
		return float64(len(ss.getMtables()))
	})

	metrics.OMapLen = prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: "yatsdb",
		Subsystem: "stream_store",
		Name:      "offset_map",
		Help:      "size of offset_map",
	}, func() float64 {
		return float64(ss.omap.size())
	})

	var wg sync.WaitGroup
	var reloadCount int64
	var begin = time.Now()
	ss.wal, err = wal.Reload(options.WalOptions, func(typ streamstorepb.EntryTyper) error {
		e := typ.(*streamstorepb.Entry)
		if e.ID <= lastEntryID {
			return nil
		}
		wg.Add(1)
		reloadCount++
		ss.appendEntryCh <- appendEntry{
			entry: *e,
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

type EntryWithFn struct {
	streamstorepb.Entry
	fn func(ID uint64, err error)
}

func (entry *EntryWithFn) SetID(ID uint64) {
	entry.ID = ID
}
func (entry *EntryWithFn) Fn(ID uint64, err error) {
	entry.fn(ID, err)
}

func (ss *StreamStore) Append(streamID StreamID, data []byte, fn AppendCallbackFn) {
	ss.wal.Write(&EntryWithFn{
		streamstorepb.Entry{
			StreamId: streamID,
			Data:     data,
		}, func(ID uint64, err error) {
			ss.appendEntry(streamstorepb.Entry{
				StreamId: streamID,
				Data:     data,
				ID:       ID,
			}, fn)
		},
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

func FormatSegmentName(first, last uint64) string {
	return fmt.Sprintf("%d-%d", first, last)
}
func parseEntryID(filename string) (from int64, to int64, err error) {
	token := strings.Split(filepath.Base(filename), ".")[0]
	tokens := strings.Split(token, "-")
	if len(tokens) != 2 {
		return 0, 0, errors.Errorf("filename %s format error", filename)
	}
	from, err = strconv.ParseInt(tokens[0], 10, 64)
	if err != nil {
		return 0, 0, errors.Errorf("parse from %s error %s", tokens[0], err.Error())
	}
	to, err = strconv.ParseInt(tokens[1], 10, 64)
	if err != nil {
		return 0, 0, errors.Errorf("parse to %s error %s", tokens[0], err.Error())
	}
	return
}

func (ss *StreamStore) createSegment(mtable MTable) string {
	begin := time.Now()
	name := FormatSegmentName(mtable.FirstEntryID(), mtable.LastEntryID())
	tempfile := filepath.Join(ss.Options.SegmentDir, name+segmentTempExt)
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
	filename := filepath.Join(ss.Options.SegmentDir, name+segmentExt)
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

func (ss *StreamStore) deleteFirstSegment() {
	var segment Segment
	for {
		segmentPointer := ss.getSegmentsPointor()
		segments := *(*[]Segment)(segmentPointer)
		if len(segments) > 0 {
			segment = segments[0]
			if ss.updateSegments(segmentPointer, append([]Segment{}, segments[1:]...)) {
				break
			}
		}
	}

	segment.SetDeleteOnClose(true)
	if err := segment.Close(); err != nil {
		logrus.WithField("filename", segment.Filename()).
			Panicf("close segment failed %s", err.Error())
	}
}

func (ss *StreamStore) flushMTable(mtable MTable) {
	filename := ss.createSegment(mtable)
	segment, err := ss.openSegment(filename)
	if err != nil {
		logrus.Panicf("open segment failed %+v", err)
	}
	ss.appendSegment(segment)
	ss.clearMTable()
	ss.wal.ClearLogFiles(mtable.LastEntryID())
	ss.notifyMergeSegments()
}

func (ss *StreamStore) openSegment(filename string) (Segment, error) {
	return openSegmentV1(filename)
}

func (ss *StreamStore) getSegments() []Segment {
	return *(*[]Segment)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&ss.segments))))
}
func (ss *StreamStore) getSegmentsPointor() unsafe.Pointer {
	return atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&ss.segments)))
}
func (ss *StreamStore) updateSegments(pointor unsafe.Pointer, segments []Segment) bool {
	return atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&ss.segments)),
		pointor, unsafe.Pointer(&segments))
}
func (ss *StreamStore) appendSegment(segment Segment) {
	for {
		segments := ss.getSegmentsPointor()
		newSegments := append(*(*[]Segment)(segments), segment)
		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&ss.segments)),
			segments, unsafe.Pointer(&newSegments)) {
			break
		}
	}
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
	ss.updateTables(append(ss.getMtables(), mtable))
}

func (ss *StreamStore) writeEntry(entry appendEntry) int64 {
	offset := ss.mtable.Write(entry.entry)
	ss.omap.set(entry.entry.StreamId, offset)
	if ss.mtable.Size() < int(ss.MaxMemTableSize) {
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
func (ss *StreamStore) clearSegments() {
	for {
		segments := ss.getSegments()
		if len(segments) == 0 {
			return
		}
		if time.Since(segments[0].CreateTS()) > ss.Retention.Time {
			ss.deleteFirstSegment()
			continue
		}
		break
	}

	for {
		var size int64
		for _, segment := range ss.getSegments() {
			size += segment.Size()
		}
		if size > int64(ss.Retention.Size) {
			ss.deleteFirstSegment()
			continue
		}
		break
	}
}
func (ss *StreamStore) mergeSegments() {
	var size int64
	var toMergeSegments []*SegmentV1
	for _, segment := range ss.getSegments() {
		segmentV1 := segment.(*SegmentV1)
		if int64(segmentV1.header.Merges) > 1 {
			logrus.WithFields(logrus.Fields{
				"segment": segment.Filename(),
				"merges":  segmentV1.header.Merges,
			}).Info("skip merged segment")
			continue
		}
		size += segment.Size()
		toMergeSegments = append(toMergeSegments, segmentV1)
	}

	if size < int64(ss.Options.MinMergedSegmentSize) ||
		len(toMergeSegments) <= 1 {
		return
	}

	name := FormatSegmentName(toMergeSegments[0].FirstEntryID(),
		toMergeSegments[len(toMergeSegments)-1].LastEntryID())
	tempfile := filepath.Join(ss.SegmentDir, name+segmentTempExt)
	f, err := os.Create(tempfile)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"filename": tempfile,
			"err":      err,
		}).Panicf("create temp segment failed")
	}
	if err := MergeSegments(f, toMergeSegments...); err != nil {
		logrus.WithFields(logrus.Fields{
			"filename": tempfile,
			"err":      err.Error(),
		}).Panicf("mergeSegments failed")
	}

	if err := f.Close(); err != nil {
		logrus.WithFields(logrus.Fields{
			"filename": tempfile,
			"err":      err.Error(),
		}).Panic("close file failed")
	}
	filename := filepath.Join(ss.SegmentDir, name+segmentExt)
	if err := os.Rename(tempfile, filename); err != nil {
		logrus.WithFields(logrus.Fields{
			"from": tempfile,
			"to":   filename,
			"err":  err.Error(),
		}).Panic("rename failed")
	}
	mergedSegment, err := openSegmentV1(filename)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"filename": filename,
			"err":      err.Error(),
		}).Panic("openSegmentV1 failed")
	}
	for {
		segmentsPointer := ss.getSegmentsPointor()
		segments := *(*[]Segment)(segmentsPointer)
		var newSegments []Segment
		var toDeleteSegment []Segment
		for _, segment := range segments {
			if mergedSegment.FirstEntryID() <= segment.FirstEntryID() &&
				segment.LastEntryID() <= mergedSegment.LastEntryID() {
				toDeleteSegment = append(toDeleteSegment, segment)
				continue
			}
			newSegments = append(newSegments, segment)
		}

		newSegments = append(newSegments, mergedSegment)

		sort.Slice(newSegments, func(i, j int) bool {
			return newSegments[i].LastEntryID() < newSegments[j].LastEntryID()
		})

		if !ss.updateSegments(segmentsPointer, newSegments) {
			continue
		}

		for _, segment := range toDeleteSegment {
			segment.SetDeleteOnClose(true)
			if err := segment.Close(); err != nil {
				logrus.WithFields(logrus.Fields{
					"filename": segment.Filename(),
					"err":      err,
				}).Panic("close Segment failed")
			}
		}
		break
	}
}
func (ss *StreamStore) notifyMergeSegments() {
	select {
	case ss.mergeSegmentCh <- struct{}{}:
	default:
	}
}
func (ss *StreamStore) startMergeSegmentRoutine() {
	go func() {
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()
		for {
			select {
			case <-ss.mergeSegmentCh:
			case <-ticker.C:
			case <-ss.ctx.Done():
				return
			}
			ss.clearSegments()
			ss.mergeSegments()
		}
	}()
}
func (ss *StreamStore) startWriteEntryRoutine() {
	go func() {
		defer ss.wg.Done()
		for {
			select {
			case entry := <-ss.appendEntryCh:
				offset := ss.writeEntry(entry)
				offset -= int64(len(entry.entry.Data))
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
	segments := ss.getSegments()
	if i := SearchMTables1(mTables, streamID, offset); i != -1 {
		return mTables[i].NewReader(streamID)
	}
	if i := SearchSegments(segments, streamID, offset); i != -1 {
		segment := segments[i]
		return segment.NewReader(streamID)
	}
	if i := SearchMTables2(mTables, streamID, offset); i != -1 {
		return mTables[i].NewReader(streamID)
	}
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
	for _, segment := range ss.getSegments() {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	ss.updateTables([]MTable{})
	return nil
}
