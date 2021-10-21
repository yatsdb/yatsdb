package wal

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/pkg/errors"

	"github.com/sirupsen/logrus"
	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
)

type Wal interface {
	Write(entry streamstorepb.Entry, fn func(err error))
	Close() error
	ClearLogFiles(ID uint64)
}

type Entry struct {
	entry streamstorepb.Entry
	fn    func(error)
}

var _ Wal = (*wal)(nil)

type Options struct {
	SyncWrite     bool
	SyncBatchSize int
	MaxLogSize    int64

	Dir       string
	BatchSize int
	// TruncateLast truncate last log file when error happen
	TruncateLast bool
}

type Syncer interface {
	Sync() error
}

type LogFile interface {
	io.Writer
	Syncer
	Size() int64
	SetCloseOnSync()
	SetFirstEntryID(ID uint64)
	SetLastEntryID(ID uint64)
}

type EntriesSync struct {
	entries []Entry
	syncer  Syncer
}

type wal struct {
	Options
	entryCh chan Entry
	ctx     context.Context

	syncEntryCh chan EntriesSync
	LogFileCh   chan LogFile

	currLogFile LogFile

	closedFiles []LogFile

	createLogIndex int64
}

func (wal *wal) Close() error {
	return nil
}
func (wal *wal) ClearLogFiles(ID uint64) {

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
				case sync := <-wal.syncEntryCh:
					if sync.syncer == entries[len(entries)-1].syncer {
						last = &sync
						break batchLoop
					}
					entries = append(entries, sync)
					count += len(sync.entries)
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
			if err := entries[len(entries)-1].syncer.Sync(); err != nil {
				logrus.Warnf("sync file failed %s", err.Error())
				syncEntriesCb(entries, err)
			}
		}
	}()
}

func syncEntriesCb(entriesSynces []EntriesSync, err error) {
	for _, sync := range entriesSynces {
		for _, entry := range sync.entries {
			entry.fn(err)
		}
	}
}

func entriesCb(entries []Entry, err error) {
	for _, entry := range entries {
		entry.fn(err)
	}
}

func (wal *wal) startWriteEntryGoroutine() {
	go func() {
		var encoder = newEncoder(nil)
		for {
			var entries []Entry
			select {
			case entry := <-wal.entryCh:
				entries = append(entries, entry)
			case <-wal.ctx.Done():
				entriesCb(entries, wal.ctx.Err())
				return
			}
			for {
				select {
				case entry := <-wal.entryCh:
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

			file, err := wal.getLogFile()
			if err != nil {
				entriesCb(entries, err)
				continue
			}
			file.SetFirstEntryID(batch.Entries[0].ID)
			encoder.Reset(file)
			if err := encoder.Encode(&batch); err != nil {
				entriesCb(entries, err)
				continue
			}
			if file.Size() > wal.MaxLogSize {
				file.SetLastEntryID(batch.Entries[len(batch.Entries)-1].ID)
				file.SetCloseOnSync()
				wal.nextLog()
			}
			select {
			case wal.syncEntryCh <- EntriesSync{
				entries: entries,
				syncer:  file,
			}:
			case <-wal.ctx.Done():
				entriesCb(entries, err)
				return
			}
		}
	}()
}

func (wal *wal) CreateLogFile() (LogFile, error) {
	wal.createLogIndex++
	filename := filepath.Join(wal.Options.Dir, strconv.FormatInt(wal.createLogIndex, 10)+logExt)
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &logFile{
		f: f,
	}, nil
}
func (wal *wal) startCreatLogFileRoutine() {
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
func (wal *wal) nextLog() {
	wal.currLogFile = nil
}
func (wal *wal) getLogFile() (LogFile, error) {
	if wal.currLogFile != nil {
		return wal.currLogFile, nil
	} else {
		select {
		case logFile := <-wal.LogFileCh:
			wal.currLogFile = logFile
		case <-wal.ctx.Done():
			return nil, wal.ctx.Err()
		}
		return wal.currLogFile, nil
	}
}

func (wal *wal) Write(entry streamstorepb.Entry, fn func(err error)) {
	select {
	case wal.entryCh <- Entry{
		entry: entry,
		fn:    fn,
	}:
	case <-wal.ctx.Done():
		fn(wal.ctx.Err())
	}
}
