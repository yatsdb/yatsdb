package streamstore

import "github.com/yatsdb/yatsdb/aoss/stream-store/wal"

type Options struct {
	WalOptions      wal.Options
	MaxMemTableSize int
	MaxTables       int
	SegmentDir      string
}
