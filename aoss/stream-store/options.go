package streamstore

import (
	"path/filepath"

	"github.com/yatsdb/yatsdb/aoss/stream-store/wal"
)

type Options struct {
	WalOptions      wal.Options
	MaxMemTableSize int
	MaxTables       int
	SegmentDir      string
}

func DefaultOptionsWithDir(dir string) Options {
	return Options{
		WalOptions: wal.Options{
			SyncWrite:     true,
			SyncBatchSize: 1024,
			MaxLogSize:    128 * 1024 * 1024,
			Dir:           filepath.Join(dir, "wals"),
			BatchSize:     1024,
			TruncateLast:  true,
		},
		MaxMemTableSize: 256 * 1024 * 1024,
		MaxTables:       4,
		SegmentDir:      filepath.Join(dir, "segments"),
	}
}
