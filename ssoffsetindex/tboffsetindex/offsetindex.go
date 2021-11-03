package tboffsetindex

import (
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/sirupsen/logrus"
	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
	"github.com/yatsdb/yatsdb/aoss/stream-store/wal"
	"github.com/yatsdb/yatsdb/pkg/metrics"
	"github.com/yatsdb/yatsdb/pkg/utils"
	"github.com/yatsdb/yatsdb/ssoffsetindex"
)

type DB struct {
	Options
	offsetTables       *[]STOffsetTable
	flushTables        *[]STOffsetTable
	FileSTOffsetTables *[]FileSTOffsetTable

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

type STOffset struct {
	//metrics stream ID
	StreamId ssoffsetindex.StreamID
	//Offset stream offset
	Offset int64
}
type FileSTOffsetTable struct {
	TimeStamp struct {
		From int64
		To   int64
	}
	STOffsets     []STOffset
	mfile         *fileutil.MmapFile
	filename      string
	header        streamstorepb.OffsetIndexFileTableHeader
	ref           *int32
	deleteOnClose bool
}

type STOffsetTable struct {
	WalDir    string
	Timestamp struct {
		From int64
		To   int64
	}
	Offsets      map[ssoffsetindex.StreamID]int64
	wal          wal.Wal
	tablesLocker *sync.Mutex
}

type STOffsetEntry struct {
	streamstorepb.StreamTimeStampOffset
	fn func(uint64, error)
}

func (entry *STOffsetEntry) SetID(ID uint64) {
	entry.ID = ID
}
func (entry *STOffsetEntry) Fn(ID uint64, err error) {
	entry.fn(ID, err)
}

/*
|     10 Minute     |                |
*/

func (table *FileSTOffsetTable) IncRef() bool {
	for {
		ref := atomic.LoadInt32(table.ref)
		if ref < 0 {
			logrus.WithFields(logrus.Fields{
				"filename": table.filename,
				"ref":      ref,
			}).Panic("ref error")
		}
		if ref == 0 {
			return false
		}
		if atomic.CompareAndSwapInt32(table.ref, ref, ref+1) {
			return true
		}
	}
}

func (table *FileSTOffsetTable) DecRef() {
	for {
		ref := atomic.LoadInt32(table.ref)
		if ref <= 0 {
			logrus.WithFields(logrus.Fields{
				"filename": table.filename,
				"ref":      ref,
			}).Panic("ref error")
		}

		if !atomic.CompareAndSwapInt32(table.ref, ref, ref-1) {
			continue
		}
		if ref-1 == 0 {
			if err := table.mfile.Close(); err != nil {
				logrus.WithFields(logrus.Fields{
					"filename": table.filename,
					"err":      err,
				}).Panic("close file table failed")
			}
			if table.deleteOnClose {
				if err := os.Remove(table.filename); err != nil {
					logrus.WithFields(logrus.Fields{
						"filename": table.filename,
						"err":      err,
					}).Panic("remove table table error")
				}
				logrus.WithFields(logrus.Fields{
					"filename":  table.filename,
					"timestamp": table.TimeStamp,
				}).Infof("delete file table success")
			} else {
				logrus.WithFields(logrus.Fields{
					"filename":  table.filename,
					"timestamp": table.TimeStamp,
				}).Infof("close file table success")
			}
		}
		break
	}
}

func (db *DB) getFileSTOffsetTables() []FileSTOffsetTable {
	return *(*[]FileSTOffsetTable)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&db.FileSTOffsetTables))))
}
func (db *DB) setFileSTOffsetTable(tables []FileSTOffsetTable) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&db.FileSTOffsetTables)), unsafe.Pointer(&tables))
}

func (db *DB) getFlushTables() []STOffsetTable {
	return *(*[]STOffsetTable)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&db.flushTables))))
}
func (db *DB) setFlushTables(tables []STOffsetTable) {
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Timestamp.From < tables[j].Timestamp.From
	})
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&db.flushTables)), unsafe.Pointer(&tables))
}

func (db *DB) getOffsetTables() []STOffsetTable {
	return *(*[]STOffsetTable)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&db.offsetTables))))
}

func (db *DB) setOffsetTables(tables []STOffsetTable) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&db.offsetTables)), unsafe.Pointer(&tables))
}

func (db *DB) flushOffsetMap() bool {
	now := time.Now()
	offsetTables := db.getOffsetTables()
	var timestamp struct {
		From int64
		To   int64
	}
	if len(offsetTables) == 0 {
		timestamp.From = now.Add(-db.Options.OffsetTableInterval / 2).UnixMilli()
		timestamp.To = now.Add(db.OffsetTableInterval / 2).UnixMilli()
		logrus.Info("offsetTable empty")
	} else {
		lastTable := offsetTables[len(offsetTables)-1]
		toTS := time.UnixMilli(lastTable.Timestamp.To).Local()
		if toTS.After(now) && toTS.Sub(now) > db.OffsetTableInterval/2 {
			logrus.WithFields(logrus.Fields{
				"toTS":                     toTS,
				"now":                      now,
				"toTS.Sub(now)":            toTS.Sub(now),
				"db.OffsetTableInterval/2": db.OffsetTableInterval / 2,
			}).Info("offset table ok")
			return false
		}
		if toTS.Before(now) {
			logrus.WithFields(logrus.Fields{
				"from": time.UnixMilli(lastTable.Timestamp.From).Local(),
				"to":   time.UnixMilli(lastTable.Timestamp.To).Local(),
				"now":  now,
			}).Info("offsetTable all timeout")
			timestamp.From = now.Add(-db.Options.OffsetTableInterval / 2).UnixMilli()
			timestamp.To = now.Add(db.OffsetTableInterval / 2).UnixMilli()
		} else {
			timestamp.From = time.UnixMilli(lastTable.Timestamp.To).Local().UnixMilli()
			timestamp.To = timestamp.From + db.OffsetTableInterval.Milliseconds()
		}
	}

	tsName := strconv.FormatInt(timestamp.From, 10) + "-" +
		strconv.FormatInt(timestamp.To, 10)
	dir := filepath.Join(db.WalDir, tsName)
	_ = os.RemoveAll(dir)
	_ = os.Mkdir(dir, 0777)
	newWal, err := wal.Reload(wal.DefaultOption(dir), nil)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"dir": dir,
			"err": err,
		}).Panic("create wal failed")
	}
	offsetTables = append(append([]STOffsetTable{}, offsetTables...), STOffsetTable{
		Timestamp:    timestamp,
		wal:          newWal,
		WalDir:       dir,
		tablesLocker: &sync.Mutex{},
		Offsets:      make(map[ssoffsetindex.StreamID]int64, 64*1024),
	})

	logrus.WithFields(logrus.Fields{
		"wal":               dir,
		"timestamp.from":    time.UnixMilli(timestamp.From).Local(),
		"timestamp.to":      time.UnixMilli(timestamp.To).Local(),
		"offset.table.size": len(offsetTables),
	}).Info("create new offset map")

	if len(offsetTables) > 2 {
		first := offsetTables[0]
		flushTables := append(append([]STOffsetTable{}, db.getFlushTables()...), first)
		db.setFlushTables(flushTables)
		offsetTables = append([]STOffsetTable{}, offsetTables[1:]...)
		db.setOffsetTables(offsetTables)
		go db.flushFileOffsetTable(first)
	} else {
		db.setOffsetTables(offsetTables)
	}
	return true
}

func (db *DB) clearFileTables() {
	fileTables := db.getFileSTOffsetTables()
	if len(fileTables) == 0 {
		return
	}
	ts := time.UnixMilli(fileTables[0].header.CreateTs).Local()
	if time.Now().Sub(ts) < db.Retention.Time {
		return
	}
	var remain []FileSTOffsetTable
	for i, table := range fileTables {
		ts := time.UnixMilli(fileTables[0].header.CreateTs).Local()
		if time.Now().Sub(ts) >= db.Retention.Time {
			logrus.WithFields(logrus.Fields{
				"filename":  table.filename,
				"create_ts": ts,
			}).Info("to delete fileTable")
			table.deleteOnClose = true
			table.DecRef()
			continue
		}
		remain = append(remain, fileTables[i:]...)
		break
	}
	db.setFileSTOffsetTable(remain)
}
func (db *DB) startTickerRoutine() {
	ticker := time.NewTicker(db.TickerInterval)
	db.wg.Add(1)
	go func() {
		defer func() {
			db.wg.Done()
			ticker.Stop()
		}()
		for {
			select {
			case <-ticker.C:
			case <-db.ctx.Done():
				return
			}
			for db.flushOffsetMap() {
			}
			db.clearFileTables()
		}
	}()
}

const (
	tableExt    = ".table"
	TableTmpExt = ".table.tmp"
)

func (db *DB) flushFileOffsetTable(table STOffsetTable) {
	var wg sync.WaitGroup
	wg.Add(1)
	table.wal.Write(&STOffsetEntry{
		StreamTimeStampOffset: streamstorepb.StreamTimeStampOffset{},
		fn: func(entryID uint64, err error) {
			if err != nil {
				logrus.WithError(err).Panic("wal write failed")
			}
			wg.Done()
		},
	})
	wg.Wait()

	if err := table.wal.Close(); err != nil {
		logrus.WithFields(logrus.Fields{
			"dir": table.WalDir,
			"err": err,
		}).Panic("close wal failed")
	}
	tsName := strconv.FormatInt(table.Timestamp.From, 10) + "-" +
		strconv.FormatInt(table.Timestamp.To, 10)
	tmpfile := filepath.Join(db.FileTableDir, tsName+TableTmpExt)

	f, err := os.Create(tmpfile)
	if err != nil {
		logrus.WithFields(logrus.Fields{"filename": tmpfile, "err": err}).
			Panic("create file failed")
	}

	var offsetEntries []STOffset
	for streamID, offset := range table.Offsets {
		offsetEntries = append(offsetEntries, STOffset{
			StreamId: streamID,
			Offset:   int64(offset),
		})
	}

	sort.Slice(offsetEntries, func(i, j int) bool {
		return offsetEntries[i].StreamId < offsetEntries[j].StreamId
	})

	header := streamstorepb.OffsetIndexFileTableHeader{
		Ver:      "v1",
		CreateTs: time.Now().UnixMilli(),
		Count:    int32(len(table.Offsets)),
		TsFrom:   table.Timestamp.From,
		TsTo:     table.Timestamp.To,
	}
	data, _ := header.Marshal()
	var headerLen [4]byte
	binary.BigEndian.PutUint32(headerLen[:], uint32(len(data)))
	if _, err := f.Write(headerLen[:]); err != nil {
		logrus.WithFields(logrus.Fields{"filename": tmpfile, "err": err}).
			Panic("write file failed")
	}
	if _, err := f.Write(data); err != nil {
		logrus.WithFields(logrus.Fields{"filename": tmpfile, "err": err}).
			Panic("write file failed")
	}

	data = make([]byte, int(unsafe.Sizeof(offsetEntries[0]))*len(offsetEntries))
	for index, offset := range offsetEntries {
		*(*STOffset)(unsafe.Pointer(&data[index*int(unsafe.Sizeof(offset))])) = offset
	}

	if _, err := f.Write(data); err != nil {
		logrus.WithFields(logrus.Fields{"filename": tmpfile, "err": err}).
			Panic("write file failed")
	}
	if err := f.Close(); err != nil {
		logrus.WithFields(logrus.Fields{"filename": tmpfile, "err": err}).
			Panic("close file failed")
	}
	tablefile := filepath.Join(db.FileTableDir, tsName+tableExt)
	if err := os.Rename(tmpfile, tablefile); err != nil {
		logrus.WithFields(logrus.Fields{"old": tmpfile, "new": tablefile, "err": err}).
			Panic("close file failed")
	}

	if err := os.RemoveAll(table.WalDir); err != nil {
		logrus.WithFields(logrus.Fields{
			"wal_dir": table.WalDir,
			"err":     err,
		}).Panic("remove wal dir failed")
	}

	if err := db.openTablefile(tablefile); err != nil {
		logrus.WithFields(logrus.Fields{
			"filename": tablefile,
			"err":      err.Error(),
		}).Panic("openTablefile error")
	}
	logrus.WithFields(logrus.Fields{
		"tableFile":      tablefile,
		"wal":            table.WalDir,
		"timestamp.from": time.UnixMilli(table.Timestamp.From).Local(),
		"timestamp.to":   time.UnixMilli(table.Timestamp.To).Local(),
		"entryCount":     len(table.Offsets),
	}).Infof("create file table success")
}

func parseTimestamp(filename string) (from int64, to int64, err error) {
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

func unsafeSlice(slice, data unsafe.Pointer, len int) {
	s := (*reflect.SliceHeader)(slice)
	s.Data = uintptr(data)
	s.Cap = len
	s.Len = len
}

func (db *DB) openTablefile(filename string) error {
	from, to, err := parseTimestamp(filename)
	if err != nil {
		return err
	}
	mfile, err := fileutil.OpenMmapFile(filename)
	if err != nil {
		return errors.WithStack(err)
	}
	data := mfile.Bytes()
	if len(data) < 4 {
		return errors.New("file table format error")
	}

	headerLen := binary.BigEndian.Uint32(data)
	data = data[4:]

	if len(data) < int(headerLen) {
		return errors.New("file table format error")
	}
	var header streamstorepb.OffsetIndexFileTableHeader
	if err := header.Unmarshal(data[:headerLen]); err != nil {
		return errors.WithMessage(err, "unmarshal header failed")
	}
	data = data[headerLen:]

	var ref int32 = 1
	table := FileSTOffsetTable{
		TimeStamp: struct {
			From int64
			To   int64
		}{
			From: from,
			To:   to,
		},
		mfile:    mfile,
		filename: filename,
		header:   header,
		ref:      &ref,
	}
	if len(data) > 0 {
		unsafeSlice(unsafe.Pointer(&table.STOffsets),
			unsafe.Pointer(&data[0]), int(header.Count))
	}
	fileSTOffsetTables := db.getFileSTOffsetTables()

	if len(fileSTOffsetTables) != 0 {
		if fileSTOffsetTables[len(fileSTOffsetTables)-1].TimeStamp.To > table.TimeStamp.To {
			fileSTOffsetTables = append(append([]FileSTOffsetTable{}, fileSTOffsetTables...), table)
			sort.Slice(fileSTOffsetTables, func(i, j int) bool {
				return fileSTOffsetTables[i].TimeStamp.To < fileSTOffsetTables[j].TimeStamp.To
			})
		} else {
			fileSTOffsetTables = append(append([]FileSTOffsetTable{}, fileSTOffsetTables...), table)
		}
	} else {
		fileSTOffsetTables = append(append([]FileSTOffsetTable{}, fileSTOffsetTables...), table)
	}

	db.setFileSTOffsetTable(fileSTOffsetTables)
	flushTables := db.getFlushTables()
	var remainFlushtables []STOffsetTable
	for _, offsetTable := range flushTables {
		if offsetTable.Timestamp.To != table.TimeStamp.To {
			remainFlushtables = append(remainFlushtables, offsetTable)
			break
		}
	}
	db.setFlushTables(remainFlushtables)
	return nil
}

func (db *DB) GetStreamTimestampOffset(streamID ssoffsetindex.StreamID, timestampMS int64, LE bool) (int64, error) {
	offsetTables := db.getOffsetTables()
	flushTables := db.getFlushTables()
	fileSTOffsetTables := db.getFileSTOffsetTables()

	for i := len(offsetTables) - 1; i >= 0; i-- {
		table := offsetTables[i]
		table.tablesLocker.Lock()
		if table.Timestamp.From <= timestampMS {
			if offset, ok := table.Offsets[streamID]; ok {
				table.tablesLocker.Unlock()
				return offset, nil
			}
		}
		table.tablesLocker.Unlock()
	}

	for i := len(flushTables) - 1; i >= 0; i-- {
		table := flushTables[i]
		if table.Timestamp.From <= timestampMS {
			if offset, ok := table.Offsets[streamID]; ok {
				return offset, nil
			}
		}
	}

	i := sort.Search(len(fileSTOffsetTables), func(i int) bool {
		return timestampMS <= fileSTOffsetTables[i].TimeStamp.From
	})
	if i >= len(fileSTOffsetTables) {
		i = len(fileSTOffsetTables) - 1
	}
	for ; i >= 0; i-- {
		fileTable := fileSTOffsetTables[i]
		if !fileTable.IncRef() {
			continue
		}
		if fileTable.TimeStamp.From <= timestampMS {
			offsets := fileTable.STOffsets
			k := sort.Search(len(offsets), func(j int) bool {
				return streamID <= offsets[j].StreamId
			})
			if k < len(offsets) && streamID == offsets[k].StreamId {
				fileTable.DecRef()
				return offsets[k].Offset, nil
			}
		}
		fileTable.DecRef()
	}
	return 0, ssoffsetindex.ErrNoFindOffset
}

func (db *DB) SetStreamTimestampOffset(entry ssoffsetindex.SeriesStreamOffset, callback func(err error)) {
	for _, table := range db.getOffsetTables() {
		table.tablesLocker.Lock()
		if table.Timestamp.From <= entry.TimestampMS && entry.TimestampMS < table.Timestamp.To {
			if _, ok := table.Offsets[entry.StreamID]; ok {
				table.tablesLocker.Unlock()
				callback(nil)
				return
			} else {
				table.Offsets[entry.StreamID] = entry.Offset
				table.tablesLocker.Unlock()
				metrics.UpdateOffsetIndexCount.Inc()
				table.wal.Write(&STOffsetEntry{
					StreamTimeStampOffset: streamstorepb.StreamTimeStampOffset{
						StreamId:    0,
						TimestampMS: 0,
						Offset:      0,
						ID:          0,
					},
					fn: func(entryID uint64, err error) {
						callback(err)
					},
				})
				return
			}
		}
		table.tablesLocker.Unlock()
	}
	callback(fmt.Errorf("timestamp error error"))
}

func (db *DB) Close() error {
	db.cancel()
	for _, fileTable := range db.getFileSTOffsetTables() {
		fileTable.DecRef()
	}
	for _, offset := range db.getOffsetTables() {
		if err := offset.wal.Close(); err != nil {
			return err
		}
	}
	return nil
}

func Open(options Options) (*DB, error) {
	if options.TickerInterval == 0 {
		options.TickerInterval = time.Second
	}
	if err := utils.MkdirAll(options.WalDir, options.FileTableDir); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	var db = DB{
		ctx:                ctx,
		cancel:             cancel,
		Options:            options,
		offsetTables:       &[]STOffsetTable{},
		flushTables:        &[]STOffsetTable{},
		FileSTOffsetTables: &[]FileSTOffsetTable{},
	}
	if err := filepath.Walk(options.FileTableDir,
		func(path string, info fs.FileInfo, err error) error {
			if err != nil {
				return errors.WithStack(err)
			}
			if info.IsDir() {
				return nil
			}
			if strings.HasSuffix(path, TableTmpExt) {
				logrus.WithFields(logrus.Fields{
					"filename": path,
				}).Info("delete file table tmp file")
				return nil
			}
			if strings.HasSuffix(path, tableExt) {
				if err := db.openTablefile(path); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
		return nil, err
	}
	f, err := os.Open(options.WalDir)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	dirs, err := f.ReadDir(0)
	if err != nil {
		return nil, errors.WithStack(err)
	}

loop:
	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}

		path := filepath.Join(options.WalDir, dir.Name())
		from, to, err := parseTimestamp(path)
		if err != nil {
			return nil, err
		}
		for _, filetable := range db.getFileSTOffsetTables() {
			if filetable.TimeStamp.From == from && filetable.TimeStamp.To == to {
				if err := os.RemoveAll(path); err != nil {
					return nil, errors.WithStack(err)
				}
				logrus.WithFields(logrus.Fields{
					"wal dir":   path,
					"filetable": filetable.filename,
				}).Info("delete offset map wal")
				continue loop
			}
		}

		table := STOffsetTable{
			WalDir: path,
			Timestamp: struct {
				From int64
				To   int64
			}{
				From: from,
				To:   to,
			},
			Offsets:      map[ssoffsetindex.StreamID]int64{},
			wal:          nil,
			tablesLocker: &sync.Mutex{},
		}
		var begin = time.Now()
		table.wal, err = wal.Reload(wal.DefaultOption(path),
			func(et streamstorepb.EntryTyper) error {
				entry := et.(*streamstorepb.StreamTimeStampOffset)
				table.Offsets[entry.StreamId] = entry.Offset
				return nil
			})
		if err != nil {
			return nil, err
		}
		*db.offsetTables = append(*db.offsetTables, table)
		logrus.WithFields(logrus.Fields{
			"wal":     path,
			"count":   len(table.Offsets),
			"elapsed": time.Since(begin),
		}).Info("offset map reload success")
	}

	offsetTables := db.getOffsetTables()
	sort.Slice(offsetTables, func(i, j int) bool {
		return offsetTables[i].Timestamp.From < offsetTables[j].Timestamp.From
	})
	db.setOffsetTables(offsetTables)

	for db.flushOffsetMap() {
	}
	db.startTickerRoutine()
	return &db, nil
}
