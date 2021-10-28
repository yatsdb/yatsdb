package streamstore

import (
	"path/filepath"
	"time"

	"github.com/yatsdb/yatsdb/aoss/stream-store/wal"
)

type Options struct {
	WalOptions       wal.Options `yaml:"wal_options,omitempty" json:"wal_options,omitempty"`
	MaxMemTableSize  int         `yaml:"max_mem_table_size,omitempty" json:"max_mem_table_size,omitempty"`
	MaxMTables       int         `yaml:"max_mtables,omitempty" json:"max_m_tables,omitempty"`
	SegmentDir       string      `yaml:"segment_dir,omitempty" json:"segment_dir,omitempty"`
	CallbackRoutines int         `yaml:"callback_routines,omitempty" json:"callback_routines,omitempty"`
	Retention        struct {
		Time time.Duration `yaml:"retention,omitempty" json:"time,omitempty"`
		Size int64         `yaml:"size,omitempty" json:"size,omitempty"`
	} `yaml:"retention,omitempty" json:"retention,omitempty"`
}

func DefaultOptionsWithDir(dir string) Options {
	if dir == "" {
		dir = "data"
	}
	return Options{
		WalOptions: wal.Options{
			SyncWrite:     true,
			SyncBatchSize: 1024,
			MaxLogSize:    1 << 20,
			Dir:           filepath.Join(dir, "wals"),
			BatchSize:     1024,
			TruncateLast:  true,
		},
		MaxMemTableSize:  512 << 20,
		MaxMTables:       1,
		CallbackRoutines: 4,
		SegmentDir:       filepath.Join(dir, "segments"),
		Retention: struct {
			Time time.Duration `yaml:"retention,omitempty" json:"time,omitempty"`
			Size int64         `yaml:"size,omitempty" json:"size,omitempty"`
		}{
			Time: time.Hour * 24 * 30,
			Size: 100 << 30,
		},
	}
}
