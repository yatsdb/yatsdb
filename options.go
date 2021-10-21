package yatsdb

import "github.com/yatsdb/yatsdb/aoss"

type Options struct {
	BadgerDBStoreDir       string                      `json:"badger_db_store_dir,omitempty"`
	FileStreamStoreOptions aoss.FileStreamStoreOptions `json:"file_stream_store_options,omitempty"`
	ReadGorutines          int                         `json:"read_gorutines,omitempty"`
}
