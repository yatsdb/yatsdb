package wal

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
)

func Reload(options Options, fn func(streamstorepb.Entry) error) (Wal, error) {
	type FileInfo struct {
		firstID uint64
		lastID  uint64
		Path    string
		Size    int64
		f       *os.File
	}

	var fileInfos []*FileInfo
	err := filepath.Walk(options.Dir, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, logExt) {
			// delete empty file
			if info.Size() == 0 {
				if err := os.Remove(path); err != nil {
					return errors.WithStack(err)
				}
				logrus.Infof("delete empty logFile %s", path)
				return nil
			}
			f, err := os.OpenFile(path, os.O_RDWR, 0666)
			if err != nil {
				return errors.WithStack(err)
			}
			fileInfos = append(fileInfos, &FileInfo{
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

	sort.Slice(fileInfos, func(i, j int) bool {
		if len(fileInfos[i].Path) != len(fileInfos[j].Path) {
			return len(fileInfos[i].Path) < len(fileInfos[j].Path)
		}
		return fileInfos[i].Path < fileInfos[j].Path
	})

	var w wal
	var lastFileInfo *FileInfo
	for i, fileInfo := range fileInfos {

		//range entries
		iter := newEntryIteractor(newDecoder(fileInfo.f))
		for {
			//last success write offset
			offset, err := fileInfo.f.Seek(0, 0)
			if err != nil {
				return nil, err
			}
			entry, err := iter.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				if err == io.ErrUnexpectedEOF && i == len(fileInfos)-1 && options.TruncateLast {
					if err := fileInfo.f.Truncate(offset); err != nil {
						return nil, errors.WithStack(err)
					}
				} else {
					return nil, errors.WithStack(err)
				}
				break
			}
			if err := fn(entry); err != nil {
				return nil, err
			}
			if fileInfo.firstID == 0 {
				fileInfo.firstID = entry.ID
			}
			fileInfo.lastID = entry.ID
		}
		//to fix logFile name error
		if filepath.Base(fileInfo.Path) != strconv.FormatUint(fileInfo.lastID, 10)+logExt {
			path := filepath.Join(filepath.Dir(fileInfo.Path), strconv.FormatUint(fileInfo.lastID, 10)+logExt)
			if err := os.Rename(fileInfo.Path, path); err != nil {
				return nil, errors.WithStack(err)
			}
			fileInfo.Path = path
		}
		//close files
		if i < len(fileInfos)-1 || fileInfo.Size == options.MaxLogSize {
			if err := fileInfo.f.Close(); err != nil {
				return nil, errors.WithStack(err)
			}
			w.closedFiles = append(w.closedFiles, &logFile{
				size:         fileInfo.Size,
				firstEntryID: fileInfo.firstID,
				lastEntryID:  fileInfo.lastID,
			})
		} else {
			lastFileInfo = fileInfo
		}
	}

	//init wal currLogFile
	if lastFileInfo != nil {
		var err error
		w.currLogFile, err = initLogFile(lastFileInfo.f, lastFileInfo.firstID, lastFileInfo.lastID)
		if err != nil {
			return nil, err
		}
	}

	w.startCreatLogFileRoutine()
	w.startSyncEntriesGoroutine()
	w.startWriteEntryGoroutine()

	return &w, nil
}
