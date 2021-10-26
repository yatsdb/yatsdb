package wal

import (
	"os"
	"path/filepath"
	"strconv"

	"github.com/pkg/errors"
)

const (
	logExt = ".wal"
)

type logFile struct {
	filename     string
	f            *os.File
	size         int64
	firstEntryID uint64
	lastEntryID  uint64
}

var logFileFlag = os.O_CREATE | os.O_RDWR
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
		filename:     f.Name(),
		firstEntryID: firstEntryID,
		lastEntryID:  lastEntryID,
		f:            f,
		size:         stat.Size(),
	}

	return lf, nil
}

func (lf *logFile) Write(p []byte) (n int, err error) {
	n, err = lf.f.Write(p)
	lf.size += int64(n)
	return n, err
}
func (lf *logFile) Size() int64 {
	return lf.size
}

func (lf *logFile) Sync() error {
	if err := lf.f.Sync(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (lf *logFile) SetFirstEntryID(ID uint64) {
	if lf.firstEntryID == 0 {
		lf.firstEntryID = ID
	}
}

func (lf *logFile) SetLastEntryID(ID uint64) {
	lf.lastEntryID = ID
}
func (lf *logFile) Rename() error {
	if filepath.Base(lf.f.Name()) != strconv.FormatUint(lf.lastEntryID, 10)+logExt {
		path := filepath.Join(filepath.Dir(lf.f.Name()), strconv.FormatUint(lf.lastEntryID, 10)+logExt)
		if err := os.Rename(lf.f.Name(), path); err != nil {
			return errors.WithStack(err)
		}
		lf.filename = path
	}
	return nil
}
func (lf *logFile) Filename() string {
	return lf.filename
}

func (lf *logFile) GetFirstEntryID() uint64 {
	return lf.firstEntryID
}

func (lf *logFile) GetLastEntryID() uint64 {
	return lf.lastEntryID
}
func (lf *logFile) Close() error {
	if err := lf.f.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
