package wal

import (
	"context"
	"errors"
	"io"

	"github.com/sirupsen/logrus"
	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
)

type Wal interface {
	Write(writeStream streamstorepb.Entry, fn func(err error))
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
}

type Syncer interface {
	Sync() error
}

type LogFile interface {
	io.Writer
	Syncer
	Size() int64
	SetCloseOnSync()
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
				case <-wal.ctx.Done():
					syncEntriesCb(entries, wal.ctx.Err())
					return
				default:
				}
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
			data, err := batch.Marshal()
			if err != nil {
				logrus.Panicf("Marshal failed %+v", err)
			}
			file, err := wal.getLogFile()
			if err != nil {
				entriesCb(entries, err)
				continue
			}
			if _, err := file.Write(data); err != nil {
				entriesCb(entries, err)
				continue
			}
			if file.Size() > wal.MaxLogSize {
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

func (wal *wal) CreatLogFile() (LogFile, error) {
	return nil, errors.New("not implement")
}
func (wal *wal) startCreatLogFileRoutine() {
	go func() {
		for {
			file, err := wal.CreatLogFile()
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
