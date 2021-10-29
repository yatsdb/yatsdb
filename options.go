package yatsdb

import (
	"io/ioutil"
	"path/filepath"

	"github.com/pkg/errors"
	filestreamstore "github.com/yatsdb/yatsdb/aoss/file-stream-store"
	streamstore "github.com/yatsdb/yatsdb/aoss/stream-store"
	"gopkg.in/yaml.v3"
)

type Options struct {
	BadgerDBStoreDir       string                                 `yaml:"badger_db_store_dir"`
	EnableStreamStore      bool                                   `yaml:"enable_stream_store"`
	ReadGorutines          int                                    `yaml:"read_gorutines"`
	StreamStoreOptions     streamstore.Options                    `yaml:"stream_store_options"`
	FileStreamStoreOptions filestreamstore.FileStreamStoreOptions `yaml:"file_stream_store_options"`
	Debug                  struct {
		DumpReadRequestResponse bool `yaml:"dump_read_request_response"`
		LogWriteStat            bool `yaml:"log_write_stat"`
	} `yaml:"debug"`
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

//ParseConfig parse config from file
func ParseConfig(filepath string) (Options, error) {
	var opts = DefaultOptions("")
	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		return opts, errors.WithStack(err)
	}
	if err := yaml.Unmarshal(data, &opts); err != nil {
		return opts, errors.WithStack(err)
	}
	return opts, nil
}

func WriteConfig(opts Options, filepath string) error {
	data, err := yaml.Marshal(opts)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := ioutil.WriteFile(filepath, data, 0666); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
