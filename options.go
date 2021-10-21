package yatsdb

import filestreamstore "github.com/yatsdb/yatsdb/aoss/file-stream-store"

type Options struct {
	BadgerDBStoreDir       string                                 `json:"badger_db_store_dir,omitempty"`
	FileStreamStoreOptions filestreamstore.FileStreamStoreOptions `json:"file_stream_store_options,omitempty"`
	ReadGorutines          int                                    `json:"read_gorutines,omitempty"`
}
