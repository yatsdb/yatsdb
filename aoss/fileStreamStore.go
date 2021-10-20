package aoss

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/pkg/errors"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
)

type fileStream struct {
	sync.Mutex
	filepath string
	f        *os.File
}

type FileStreamStore struct {
	fileStreams map[StreamID]*fileStream
	mtx         *sync.Mutex
	baseDir     string

	pipelines chan interface{}
}

var fileStreamExt = ".stream"

func OpenFileStreamStore(dir string) (*FileStreamStore, error) {
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		if err != os.ErrExist {
			return nil, errors.WithStack(err)
		}
	}
	fileStreams := make(map[StreamID]*fileStream)

	filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, fileStreamExt) {
			f, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
			if err != nil {
				return errors.WithStack(err)
			}
			filename := filepath.Base(path)
			filename = filename[:len(filename)-len(fileStreamExt)]
			streamID, err := strconv.ParseUint(filename, 10, 64)
			if err != nil {
				return errors.WithMessagef(err, "parse filename failed %s", filename)
			}
			fileStreams[invertedindex.StreamID(streamID)] = &fileStream{
				filepath: filename,
				f:        f,
			}
		}
		return nil
	})

	return &FileStreamStore{
		mtx:         &sync.Mutex{},
		fileStreams: fileStreams,
		baseDir:     dir,
		pipelines:   make(chan interface{}, 128),
	}, nil
}

func (fsStore *FileStreamStore) NewReader(streamID StreamID) (io.ReadSeekCloser, error) {
	fsStore.mtx.Lock()
	fs := fsStore.fileStreams[invertedindex.StreamID(streamID)]
	if fs == nil {
		fsStore.mtx.Unlock()
		return nil, errors.New("no find stream file")
	}

	fd := fs.f.Fd()
	fd2, err := syscall.Dup(int(fd))
	if err != nil {
		fsStore.mtx.Unlock()
		return nil, errors.WithStack(err)
	}
	reader := os.NewFile(uintptr(fd2), fs.filepath)
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		_ = reader.Close()
		fsStore.mtx.Unlock()
		return nil, err
	}
	fsStore.mtx.Unlock()
	return reader, nil
}

func (fsStore *FileStreamStore) Append(streamID StreamID, data []byte, fn func(offset int64, err error)) {
	fsStore.mtx.Lock()
	fs := fsStore.fileStreams[invertedindex.StreamID(streamID)]
	if fs == nil {
		path := strings.Join([]string{fsStore.baseDir, strconv.FormatUint(uint64(streamID), 10) + fileStreamExt}, "/")
		f, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			fsStore.mtx.Unlock()
			go fn(0, errors.WithStack(err))
			return
		}
		fs = &fileStream{
			filepath: path,
			f:        f,
		}
		fsStore.fileStreams[invertedindex.StreamID(streamID)] = fs
	}
	fsStore.mtx.Unlock()

	fsStore.pipelines <- struct{}{}

	go func() {
		defer func() {
			<-fsStore.pipelines
		}()
		fs.Mutex.Lock()
		//current file size
		info, err := fs.f.Stat()
		if err != nil {
			fs.Mutex.Lock()
			fn(0, errors.WithStack(err))
			return
		}
		if _, err := fs.f.Write(data); err != nil {
			fs.Mutex.Unlock()
			fn(0, errors.WithStack(err))
		} else {
			if err := fs.f.Sync(); err != nil {
				fs.Mutex.Unlock()
				fn(0, errors.WithStack(err))
				return
			}
			fs.Mutex.Unlock()
			fn(info.Size(), nil)
		}
	}()
}

func (fsStore *FileStreamStore) Close() error {
	fsStore.mtx.Lock()
	for _, fs := range fsStore.fileStreams {
		fs.Mutex.Lock()
		_ = fs.f.Close()
		fs.Mutex.Unlock()
	}
	fsStore.fileStreams = make(map[invertedindex.StreamID]*fileStream)
	fsStore.mtx.Unlock()
	return nil
}
