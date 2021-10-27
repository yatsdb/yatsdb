package streamstore

import (
	"path/filepath"
	"time"

	"github.com/yatsdb/yatsdb/aoss/stream-store/wal"
)

type Options struct {
	WalOptions      wal.Options `json:"wal_options,omitempty"`
	MaxMemTableSize int         `json:"max_mem_table_size,omitempty"`
	MaxMTables      int         `json:"max_mtables,omitempty"`
	SegmentDir      string      `json:"segment_dir,omitempty"`
	Retention       struct {
		Time time.Duration `json:"retention,omitempty"`
		Size int64         `json:"size,omitempty"`
	} `json:"retention,omitempty"`
}

func DefaultOptionsWithDir(dir string) Options {
	if dir == "" {
		dir = "data"
	}
	return Options{
		WalOptions: wal.Options{
			SyncWrite:     true,
			SyncBatchSize: 1024,
			MaxLogSize:    128 << 20,
			Dir:           filepath.Join(dir, "wals"),
			BatchSize:     1024,
			TruncateLast:  true,
		},
		MaxMemTableSize: 256 << 20,
		MaxMTables:      4,
		SegmentDir:      filepath.Join(dir, "segments"),
		Retention: struct {
			Time time.Duration `json:"retention,omitempty"`
			Size int64         `json:"size,omitempty"`
		}{
			Time: time.Hour * 24 * 30,
			Size: 100 << 30,
		},
	}
}
