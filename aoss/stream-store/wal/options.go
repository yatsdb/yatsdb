package wal

type Options struct {
	SyncWrite     bool
	SyncBatchSize int
	MaxLogSize    int64

	Dir       string
	BatchSize int
	// TruncateLast truncate last log file when error happen
	TruncateLast bool
}

func DefaultOption(dir string) Options {
	return Options{
		SyncWrite:     true,
		SyncBatchSize: 4 * 1024,
		MaxLogSize:    64 * 1024 * 1024,
		Dir:           dir,
		BatchSize:     1024,
		TruncateLast:  true,
	}
}
