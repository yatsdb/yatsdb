package yatsdb

import (
	"path/filepath"

	filestreamstore "github.com/yatsdb/yatsdb/aoss/file-stream-store"
	streamstore "github.com/yatsdb/yatsdb/aoss/stream-store"
)

type Options struct {
	BadgerDBStoreDir       string                                 `yaml:"badger_db_store_dir,omitempty"`
	EnableStreamStore      bool                                   `yaml:"enable_stream_store,omitempty"`
	ReadGorutines          int                                    `yaml:"read_gorutines,omitempty"`
	StreamStoreOptions     streamstore.Options                    `yaml:"stream_store_options,omitempty"`
	FileStreamStoreOptions filestreamstore.FileStreamStoreOptions `yaml:"file_stream_store_options,omitempty"`
}

//DefaultOptions return default options with store path
func DefaultOptions(path string) Options {
	return Options{
		BadgerDBStoreDir:   filepath.Join(path, "index"),
		EnableStreamStore:  true,
		ReadGorutines:      128,
		StreamStoreOptions: streamstore.DefaultOptionsWithDir(path),
		FileStreamStoreOptions: filestreamstore.FileStreamStoreOptions{
			Dir:            filepath.Join(path, "filestreamstore"),
			SyncWrite:      false,
			WriteGorutines: 32,
		},
	}
}
