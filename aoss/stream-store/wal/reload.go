package wal

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
)

func Reload(options Options, fn func(streamstorepb.Entry)) (Wal, error) {
	type FileInfo struct {
		Path string
		Size int64
		f    *os.File
	}

	var fileInfos []FileInfo
	err := filepath.Walk(options.Dir, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, logExt) {
			f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0666)
			if err != nil {
				return errors.WithStack(err)
			}
			fileInfos = append(fileInfos, FileInfo{
				f:    f,
				Path: path,
				Size: info.Size(),
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return nil, errors.New("not implement")
}
