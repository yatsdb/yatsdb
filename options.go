package yatsdb

type Options struct {
	BadgerDBStoreDir   string `json:"badger_db_store_dir,omitempty"`
	FileStreamStoreDir string `json:"file_stream_store_dir,omitempty"`
}
