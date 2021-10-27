package yatsdb

import (
	"path/filepath"

	filestreamstore "github.com/yatsdb/yatsdb/aoss/file-stream-store"
	streamstore "github.com/yatsdb/yatsdb/aoss/stream-store"
)

type Options struct {
	BadgerDBStoreDir       string                                 `json:"badger_db_store_dir,omitempty"`
	EnableStreamStore      bool                                   `json:"enable_stream_store,omitempty"`
	ReadGorutines          int                                    `json:"read_gorutines,omitempty"`
	StreamStoreOptions     streamstore.Options                    `json:"stream_store_options,omitempty"`
	FileStreamStoreOptions filestreamstore.FileStreamStoreOptions `json:"file_stream_store_options,omitempty"`
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
