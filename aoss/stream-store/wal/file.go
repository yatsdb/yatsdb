package wal

import (
	"os"
	"sync/atomic"

	"github.com/pkg/errors"
)

const (
	logExt = ".wal"
)

type logFile struct {
	f            *os.File
	size         int64
	closeOnSync  bool
	firstEntryID uint64
	lastEntryID  uint64
}

var _ LogFile = (*logFile)(nil)

func initLogFile(f *os.File, firstEntryID uint64, lastEntryID uint64) (*logFile, error) {
	if _, err := f.Seek(0, 2); err != nil {
		return nil, errors.WithStack(err)
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	lf := &logFile{
		firstEntryID: firstEntryID,
		lastEntryID:  lastEntryID,
		f:            f,
		size:         stat.Size(),
	}

	return lf, nil
}

func (lf *logFile) Write(p []byte) (n int, err error) {
	n, err = lf.f.Write(p)
	atomic.AddInt64(&lf.size, int64(n))
	return n, err
}
func (lf *logFile) Size() int64 {
	return lf.size
}
func (lf *logFile) SetCloseOnSync() {
	lf.closeOnSync = true
}

func (lf *logFile) Sync() error {
	if lf.closeOnSync {
		return lf.f.Close()
	}
	return lf.f.Sync()
}

func (lf *logFile) SetFirstEntryID(ID uint64) {
	if lf.firstEntryID == 0 {
		lf.firstEntryID = ID
	}
}

func (lf *logFile) SetLastEntryID(ID uint64) {
	lf.lastEntryID = ID
}
