package aoss

import (
	"os"
	"sync"
	"testing"

	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
)

func TestOpenFileStreamStore(t *testing.T) {
	t.Cleanup(func() {
		_ = os.RemoveAll(t.Name())
	})
	type args struct {
		dir string
	}
	tests := []struct {
		name    string
		args    args
		want    *FileStreamStore
		wantErr bool
	}{
		{
			args: args{
				dir: t.Name(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := OpenFileStreamStore(tt.args.dir)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenFileStreamStore() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				t.Errorf("OpenFileStreamStore() return nil")
			}
		})
	}
}

func TestFileStreamStore_Append(t *testing.T) {
	t.Cleanup(func() {
		_ = os.RemoveAll(t.Name())
	})

	fsStore, err := OpenFileStreamStore(t.Name())
	if err != nil {
		t.Fatalf(err.Error())
	}

	type fields struct {
		mtx         *sync.Mutex
		fileStreams map[StreamID]*fileStream
		baseDir     string
		pipelines   chan interface{}
	}
	type args struct {
		streamID StreamID
		data     []byte
		fn       func(offset int64, err error)
	}
	f := fields{
		mtx:         fsStore.mtx,
		fileStreams: fsStore.fileStreams,
		baseDir:     fsStore.baseDir,
		pipelines:   fsStore.pipelines,
	}

	var wg sync.WaitGroup

	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			fields: f,
			args: args{
				streamID: invertedindex.StreamID(1),
				data:     []byte("hello"),
				fn: func(offset int64, err error) {
					if err != nil {
						t.Errorf("append stream failed %+v", err)
					}
					wg.Done()
				},
			},
		},
		{
			fields: f,
			args: args{
				streamID: invertedindex.StreamID(1),
				data:     []byte("hello"),
				fn: func(offset int64, err error) {
					if err != nil {
						t.Errorf("append stream failed %+v", err)
					}
					wg.Done()
				},
			},
		},
		{
			fields: f,
			args: args{
				streamID: invertedindex.StreamID(1),
				data:     []byte("hello"),
				fn: func(offset int64, err error) {
					if err != nil {
						t.Errorf("append stream failed %+v", err)
					}
					wg.Done()
				},
			},
		},
		{
			fields: f,
			args: args{
				streamID: invertedindex.StreamID(1),
				data:     []byte("hello"),
				fn: func(offset int64, err error) {
					if err != nil {
						t.Errorf("append stream failed %+v", err)
					}
					wg.Done()
				},
			},
		},
		{
			fields: f,
			args: args{
				streamID: invertedindex.StreamID(2),
				data:     []byte("hello"),
				fn: func(offset int64, err error) {
					if err != nil {
						t.Errorf("append stream failed %+v", err)
					}
					wg.Done()
				},
			},
		},
	}
	for _, tt := range tests {
		wg.Add(1)
		t.Run(tt.name, func(t *testing.T) {
			fsStore := &FileStreamStore{
				mtx:         tt.fields.mtx,
				fileStreams: tt.fields.fileStreams,
				baseDir:     tt.fields.baseDir,
				pipelines:   tt.fields.pipelines,
			}
			fsStore.Append(tt.args.streamID, tt.args.data, tt.args.fn)
		})
	}

	wg.Wait()
}
