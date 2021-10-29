package wal

import (
	"context"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
)

func Reload(options Options, fn func(streamstorepb.Entry) error) (*wal, error) {
	type FileInfo struct {
		firstID uint64
		lastID  uint64
		Path    string
		Size    int64
		f       *os.File
	}

	var fileInfos []*FileInfo
	err := filepath.Walk(options.Dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
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
	ctx, cancel := context.WithCancel(context.Background())
	var w = wal{
		ctx:            ctx,
		cancel:         cancel,
		Options:        options,
		entryCh:        make(chan Entry, options.BatchSize),
		syncEntryCh:    make(chan EntriesSync, options.SyncBatchSize),
		LogFileCh:      make(chan LogFile),
		lastLogFile:    nil,
		logFiles:       []LogFile{},
		logFilesLocker: sync.Mutex{},
		createLogIndex: math.MaxUint64 / 2,
	}
	var lastFileInfo *FileInfo
	for i, fileInfo := range fileInfos {
		logrus.WithField("filename", fileInfo.f.Name()).Info("reload wal log")
		//range entries
		iter := newEntryIteractor(newDecoder(fileInfo.f))
		for {
			//last success write offset
			offset, err := fileInfo.f.Seek(0, 1)
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
			w.entryID = entry.ID
		}

		//close files
		if i < len(fileInfos)-1 || fileInfo.Size >= options.MaxLogSize {
			lf, err := initLogFile(fileInfo.f, fileInfo.firstID, fileInfo.lastID)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			if err := lf.Rename(); err != nil {
				return nil, errors.WithStack(err)
			}
			w.logFiles = append(w.logFiles, lf)
		} else {
			lastFileInfo = fileInfo
		}
	}

	//init wal currLogFile
	if lastFileInfo != nil {
		var err error
		w.lastLogFile, err = initLogFile(lastFileInfo.f, lastFileInfo.firstID, lastFileInfo.lastID)
		if err != nil {
			return nil, err
		}
		w.logFiles = append(w.logFiles, w.lastLogFile)

		base := filepath.Base(w.lastLogFile.Filename())
		index, err := strconv.ParseUint(base[:len(base)-len(logExt)], 10, 64)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		w.createLogIndex = index
	}

	w.startCreatLogFileRoutine()
	w.startSyncEntriesGoroutine()
	w.startWriteEntryGoroutine()

	logrus.Infof("reload wal success last entry ID %d", w.entryID)

	return &w, nil
}
