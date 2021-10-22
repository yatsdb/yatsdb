package wal

import (
	"context"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/pkg/errors"

	"github.com/sirupsen/logrus"
	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
)

type Wal interface {
	Write(entry streamstorepb.Entry, fn func(ID uint64, err error))
	Close() error
	ClearLogFiles(ID uint64)
}

type Entry struct {
	entry streamstorepb.Entry
	fn    func(uint64, error)
}

var _ Wal = (*wal)(nil)

type LogFile interface {
	io.Writer
	io.Closer
	Sync() error
	Size() int64
	SetFirstEntryID(ID uint64)
	SetLastEntryID(ID uint64)
	GetFirstEntryID() uint64
	GetLastEntryID() uint64
	Rename() error
	Filename() string
}

type EntriesSync struct {
	entries []Entry
	lf      LogFile
}

type wal struct {
	Options
	entryCh chan Entry
	ctx     context.Context

	syncEntryCh chan EntriesSync
	LogFileCh   chan LogFile

	lastLogFile LogFile

	logFiles       []LogFile
	logFilesLocker sync.Mutex

	createLogIndex uint64
	entryID        uint64
}

func (wal *wal) Close() error {
	return nil
}
func (wal *wal) ClearLogFiles(ID uint64) {
	wal.logFilesLocker.Lock()
	defer wal.logFilesLocker.Unlock()
	for i, lf := range wal.logFiles[:len(wal.logFiles)-1] {
		if lf.GetLastEntryID() <= ID {
			if err := lf.Close(); err != nil {
				logrus.Panicf("close logFile failed %+v", err)
			}
			if err := os.Remove(lf.Filename()); err != nil {
				logrus.Panicf("remove logFile failed %+v", err)
			}
		} else {
			copy(wal.logFiles, wal.logFiles[i:])
			wal.logFiles = wal.logFiles[:len(wal.logFiles)-i]
			break
		}
	}
}
func (wal *wal) startSyncEntriesGoroutine() {
	go func() {
		var last *EntriesSync
		for {
			var count int
			var entries []EntriesSync
			if last == nil {
				select {
				case sync := <-wal.syncEntryCh:
					entries = append(entries, sync)
				case <-wal.ctx.Done():
					syncEntriesCb(entries, wal.ctx.Err())
					return
				}
			} else {
				entries = append(entries, *last)
				last = nil
				count++
			}
		batchLoop:
			for {
				select {
				case entry := <-wal.syncEntryCh:
					if entry.lf != entries[len(entries)-1].lf {
						last = &entry
						break batchLoop
					}
					entries = append(entries, entry)
					count += len(entry.entries)
					if count < wal.SyncBatchSize {
						continue
					}
				case <-wal.ctx.Done():
					syncEntriesCb(entries, wal.ctx.Err())
					return
				default:
				}
				break batchLoop
			}
			lf := entries[len(entries)-1].lf
			if err := lf.Sync(); err != nil {
				logrus.Panicf("sync file failed %s", err.Error())
			}
			syncEntriesCb(entries, nil)
			if lf.Size() > wal.MaxLogSize {
				if err := lf.Rename(); err != nil {
					logrus.Panicf("rename failed %+v", err)
				}
			}
		}
	}()
}

func syncEntriesCb(entriesSynces []EntriesSync, err error) {
	for _, sync := range entriesSynces {
		for _, entry := range sync.entries {
			if err != nil {
				entry.fn(0, err)
			} else {
				entry.fn(entry.entry.ID, nil)
			}
		}
	}
}

func entriesCb(entries []Entry, err error) {
	for _, entry := range entries {
		if err != nil {
			entry.fn(0, err)
		}
	}
}

func (wal *wal) nextEntryID() uint64 {
	wal.entryID++
	return wal.entryID
}
func (wal *wal) startWriteEntryGoroutine() {
	go func() {
		var encoder = newEncoder(nil)
		file, err := wal.getLogFile()
		if err != nil {
			logrus.Panicf("get log file failed %+v", err)
		}
		for {
			var entries []Entry
			select {
			case entry := <-wal.entryCh:
				entry.entry.ID = wal.nextEntryID()
				entries = append(entries, entry)
			case <-wal.ctx.Done():
				entriesCb(entries, wal.ctx.Err())
				return
			}
			for {
				select {
				case entry := <-wal.entryCh:
					entry.entry.ID = wal.nextEntryID()
					entries = append(entries, entry)
					if len(entries) < wal.BatchSize {
						continue
					}
				default:
				}
				break
			}

			var batch streamstorepb.EntryBatch
			batch.Entries = make([]streamstorepb.Entry, 0, len(entries))
			for _, entry := range entries {
				batch.Entries = append(batch.Entries, entry.entry)
			}

			file.SetFirstEntryID(batch.Entries[0].ID)
			encoder.Reset(file)
			if err := encoder.Encode(&batch); err != nil {
				logrus.Panicf("encode entries failed %+v", err)
				continue
			}

			select {
			case wal.syncEntryCh <- EntriesSync{
				entries: entries,
				lf:      file,
			}:
			case <-wal.ctx.Done():
				entriesCb(entries, err)
				return
			}

			if file.Size() > wal.MaxLogSize {
				file.SetLastEntryID(batch.Entries[len(batch.Entries)-1].ID)
				file, err = wal.nextLog()
				if err != nil {
					if err == wal.ctx.Err() {
						entriesCb(entries, err)
						return
					}
					logrus.Panicf("get next log file failed %+v", err)
				}
			}
		}
	}()
}

func (wal *wal) CreateLogFile() (LogFile, error) {
	wal.createLogIndex++
	filename := filepath.Join(wal.Options.Dir, strconv.FormatUint(wal.createLogIndex, 10)+logExt)
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &logFile{
		f: f,
	}, nil
}
func (wal *wal) startCreatLogFileRoutine() {
	wal.createLogIndex = math.MaxUint64 / 2
	go func() {
		for {
			file, err := wal.CreateLogFile()
			if err != nil {
				logrus.Panicf("CreatLogFile failed %s", err.Error())
				continue
			}
			select {
			case wal.LogFileCh <- file:
			case <-wal.ctx.Done():
				return
			}
		}
	}()
}
func (wal *wal) nextLog() (LogFile, error) {
	wal.lastLogFile = nil
	return wal.getLogFile()
}
func (wal *wal) getLogFile() (LogFile, error) {
	if wal.lastLogFile != nil {
		return wal.lastLogFile, nil
	} else {
		select {
		case logFile := <-wal.LogFileCh:
			wal.lastLogFile = logFile
			wal.logFilesLocker.Lock()
			wal.logFiles = append(wal.logFiles, wal.lastLogFile)
			wal.logFilesLocker.Unlock()
		case <-wal.ctx.Done():
			return nil, wal.ctx.Err()
		}
		return wal.lastLogFile, nil
	}
}

func (wal *wal) Write(entry streamstorepb.Entry, fn func(entryID uint64, err error)) {
	select {
	case wal.entryCh <- Entry{
		entry: entry,
		fn:    fn,
	}:
	case <-wal.ctx.Done():
		fn(0, wal.ctx.Err())
	}
}
