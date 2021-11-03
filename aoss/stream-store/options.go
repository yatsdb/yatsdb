package streamstore

import (
	"path/filepath"
	"time"

	"github.com/yatsdb/yatsdb/aoss/stream-store/wal"
	"github.com/yatsdb/yatsdb/pkg/utils"
)

type Options struct {
	WalOptions       wal.Options `yaml:"wal_options" json:"wal_options,omitempty"`
	MaxMemTableSize  utils.Bytes `yaml:"max_mem_table_size" json:"max_mem_table_size,omitempty"`
	MaxMTables       int         `yaml:"max_mtables" json:"max_m_tables,omitempty"`
	SegmentDir       string      `yaml:"segment_dir" json:"segment_dir,omitempty"`
	CallbackRoutines int         `yam:"callback_routines" json:"callback_routines,omitempty"`
	BlockSize        int         `yaml:"block_size,omitempty" json:"block_size,omitempty"`
	Retention        struct {
		Time time.Duration `yaml:"retention" json:"time,omitempty"`
		Size utils.Bytes   `yaml:"size" json:"size,omitempty"`
	} `yaml:"retention" json:"retention,omitempty"`

	MinMergedSegmentSize utils.Bytes `yaml:"max_merged_segment_size" json:"max_merged_segment_size,omitempty"`
}

func DefaultOptionsWithDir(dir string) Options {
	if dir == "" {
		dir = "data"
	}
	return Options{
		WalOptions:           wal.DefaultOption(filepath.Join(dir, "wals")),
		MaxMemTableSize:      512 << 20,
		MinMergedSegmentSize: 1 << 32, //4GiB
		MaxMTables:           1,
		BlockSize:            640,
		CallbackRoutines:     4,
		SegmentDir:           filepath.Join(dir, "segments"),
		Retention: struct {
			Time time.Duration `yaml:"retention" json:"time,omitempty"`
			Size utils.Bytes   `yaml:"size" json:"size,omitempty"`
		}{
			Time: time.Hour * 24 * 30,
			Size: 100 << 30,
		},
	}
}
