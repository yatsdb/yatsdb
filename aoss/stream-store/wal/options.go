package wal

import "github.com/yatsdb/yatsdb/pkg/utils"

type Options struct {
	SyncWrite     bool
	SyncBatchSize int
	MaxLogSize    utils.Bytes

	Dir       string
	BatchSize int
	// TruncateLast truncate last log file when error happen
	TruncateLast bool
}

func DefaultOption(dir string) Options {
	return Options{
		SyncWrite:     true,
		SyncBatchSize: 64 * 1024,
		MaxLogSize:    64 * 1024 * 1024,
		Dir:           dir,
		BatchSize:     64 * 1024,
		TruncateLast:  true,
	}
}
