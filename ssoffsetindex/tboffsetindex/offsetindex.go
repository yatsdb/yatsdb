package tboffsetindex

import (
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
	"github.com/yatsdb/yatsdb/ssoffsetindex"
)

type DB struct {
	Options
	offsetTables       *[]STOffsetTable
	flushTables        *[]STOffsetTable
	FileSTOffsetTables *[]FileSTOffsetTable
}

type Options struct {
	FileTableDir        string
	WalDir              string
	OffsetTableInterval time.Duration
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
	STOffsets []STOffset
	mfile     *fileutil.MmapFile
	filename  string
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
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&db.flushTables)), unsafe.Pointer(&tables))
}

func (db *DB) getOffsetTables() []STOffsetTable {
	return *(*[]STOffsetTable)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&db.offsetTables))))
}

func (db *DB) setOffsetTables(tables []STOffsetTable) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&db.offsetTables)), unsafe.Pointer(&tables))
}

func (db *DB) flushOffsetMap() {
	now := time.Now()
	offsetTables := db.getOffsetTables()
	lastTable := offsetTables[len(offsetTables)-1]
	toTS := time.UnixMilli(lastTable.Timestamp.To).Local()
	if toTS.After(now) && toTS.Sub(now) < db.OffsetTableInterval/2 {
		return
	}

	timestamp := struct {
		From int64
		To   int64
	}{
		From: lastTable.Timestamp.To,
		To:   lastTable.Timestamp.To + db.OffsetTableInterval.Milliseconds(),
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
		"wal":            dir,
		"timestamp.from": time.UnixMilli(timestamp.From).Local(),
		"timestamp.to":   time.UnixMilli(timestamp.To).Local(),
	}).Info("create new offset map")

	if len(offsetTables) > 2 {
		first := offsetTables[0]
		flushTables := append(append([]STOffsetTable{}, db.getFlushTables()...), first)
		db.setFlushTables(flushTables)
		offsetTables = append([]STOffsetTable{}, offsetTables[1:]...)
		db.setOffsetTables(offsetTables)
		db.flushFileOffsetTable(first)
	} else {
		db.setOffsetTables(offsetTables)
	}
}

func (db *DB) startFlushTableRoutine() {
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
			}
			db.flushOffsetMap()
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
		logrus.WithFields(logrus.Fields{"filename": tmpfile, "err": err}).Panic("create file failed")
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

	var data = make([]byte, int(unsafe.Sizeof(offsetEntries[0]))*len(offsetEntries))
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
		"timestamp.from": table.Timestamp.From,
		"timestamp.to":   table.Timestamp.To,
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
	}
	size := len(mfile.Bytes()) / int(unsafe.Sizeof(STOffset{}))
	data := mfile.Bytes()
	unsafeSlice(unsafe.Pointer(&table.STOffsets), unsafe.Pointer(&data[0]), size)

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

	if len(fileSTOffsetTables) != 0 {
		if timestampMS <= fileSTOffsetTables[len(fileSTOffsetTables)-1].TimeStamp.From {
			i := sort.Search(len(fileSTOffsetTables), func(i int) bool {
				return timestampMS <= fileSTOffsetTables[i].TimeStamp.From
			})
			for ; i >= 0; i-- {
				offsets := fileSTOffsetTables[i].STOffsets
				k := sort.Search(len(offsets), func(j int) bool {
					return streamID <= offsets[j].StreamId
				})
				if k < len(offsets) && streamID == offsets[k].StreamId {
					return fileSTOffsetTables[i].TimeStamp.From, nil
				}
			}
		}
	}

	for i := len(flushTables) - 1; i >= 0; i++ {
		if flushTables[i].Timestamp.To <= timestampMS {
			if offset, ok := flushTables[i].Offsets[streamID]; ok {
				return offset, nil
			}
		}
	}
	for _, table := range offsetTables {
		table.tablesLocker.Lock()
		if table.Timestamp.From <= timestampMS && timestampMS < table.Timestamp.To {
			if offset, ok := table.Offsets[streamID]; ok {
				table.tablesLocker.Unlock()
				return offset, nil
			}
		}
		table.tablesLocker.Unlock()
	}
	return 0, ssoffsetindex.ErrNoFindOffset
}

func (db *DB) SetStreamTimestampOffset(entry ssoffsetindex.SeriesStreamOffset, callback func(err error)) {
	for _, table := range db.getOffsetTables() {
		table.tablesLocker.Lock()
		if table.Timestamp.From <= entry.TimestampMS && entry.TimestampMS < table.Timestamp.To {
			if _, ok := table.Offsets[entry.StreamID]; ok {
				table.tablesLocker.Lock()
				return
			} else {
				table.Offsets[entry.StreamID] = entry.Offset
				table.tablesLocker.Lock()
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
		table.tablesLocker.Lock()
	}
}

func Reload(options Options) (*DB, error) {
	var db = DB{
		Options:            options,
		offsetTables:       &[]STOffsetTable{},
		flushTables:        &[]STOffsetTable{},
		FileSTOffsetTables: &[]FileSTOffsetTable{},
	}
	if err := filepath.Walk(options.FileTableDir,
		func(path string, info fs.FileInfo, err error) error {
			if err != nil {
				return err
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

	if err := filepath.WalkDir(options.WalDir,
		func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				return nil
			}
			from, to, err := parseTimestamp(path)
			if err != nil {
				return err
			}
			for _, filetable := range db.getFileSTOffsetTables() {
				if filetable.TimeStamp.From == from && filetable.TimeStamp.To == to {
					if err := os.RemoveAll(path); err != nil {
						return errors.WithStack(err)
					}
					logrus.WithFields(logrus.Fields{
						"wal dir":   path,
						"filetable": filetable.filename,
					}).Info("delete offset map wal")
					return nil
				}
			}

			table := STOffsetTable{
				WalDir: "",
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
				return err
			}
			*db.offsetTables = append(*db.offsetTables, table)
			logrus.WithFields(logrus.Fields{
				"wal":     path,
				"count":   len(table.Offsets),
				"elapsed": time.Since(begin),
			}).Info("offset map reload success")
			return nil
		}); err != nil {
		return nil, err
	}
	db.startFlushTableRoutine()
	return &db, nil
}
