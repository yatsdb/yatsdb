package wal

import (
	"os"
	"sync/atomic"
)

const (
	logExt = ".wal"
)

type File struct {
	f           *os.File
	size        int64
	closeOnSync bool
}

func (file *File) Write(p []byte) (n int, err error) {
	n, err = file.f.Write(p)
	atomic.AddInt64(&file.size, int64(n))
	return n, err
}

func (file *File) SetCloseOnSync() {
	file.closeOnSync = true
}

func (file *File) Sync() error {
	if file.closeOnSync {
		return file.f.Close()
	}
	return file.f.Sync()
}
