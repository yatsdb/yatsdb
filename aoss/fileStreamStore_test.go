package aoss

import (
	"bytes"
	"io/ioutil"
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

func TestFileStreamStore_NewReader(t *testing.T) {
	t.Cleanup(func() {
		_ = os.RemoveAll(t.Name())
	})

	fsStore, err := OpenFileStreamStore(t.Name())
	if err != nil {
		t.Fatalf(err.Error())
	}

	appendStream := func(streamID StreamID, data []byte) {
		var wg sync.WaitGroup
		wg.Add(1)
		fsStore.Append(streamID, data, func(offset int64, err error) {
			if err != nil {
				t.Fatalf(err.Error())
			}
			wg.Done()
		})
		wg.Wait()
	}

	bytesWrites := map[StreamID]string{}
	type streamAppend struct {
		streamID StreamID
		data     []byte
		repeated int
	}

	for _, sa := range []*streamAppend{
		{
			streamID: 1,
			data:     []byte("hello1"),
			repeated: 10,
		},
		{
			streamID: 2,
			data:     []byte("2222222-"),
			repeated: 10,
		},
		{
			streamID: 3,
			data:     []byte("3333333-"),
			repeated: 10,
		},
	} {
		for i := 0; i < sa.repeated; i++ {
			appendStream(sa.streamID, sa.data)
			bytesWrites[sa.streamID] += string(sa.data)
		}
	}

	type fields struct {
		fileStreams map[StreamID]*fileStream
		mtx         *sync.Mutex
		baseDir     string
		pipelines   chan interface{}
	}

	type args struct {
		streamID StreamID
	}
	f := fields{
		fileStreams: fsStore.fileStreams,
		mtx:         fsStore.mtx,
		baseDir:     fsStore.baseDir,
		pipelines:   fsStore.pipelines,
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{
			fields: f,
			args: args{
				streamID: 1,
			},
			want: []byte(bytesWrites[1]),
		},
		{
			fields: f,
			args: args{
				streamID: 2,
			},
			want: []byte(bytesWrites[2]),
		},
		{
			fields: f,
			args: args{
				streamID: 3,
			},
			want: []byte(bytesWrites[3]),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsStore := &FileStreamStore{
				fileStreams: tt.fields.fileStreams,
				mtx:         tt.fields.mtx,
				baseDir:     tt.fields.baseDir,
				pipelines:   tt.fields.pipelines,
			}
			reader, err := fsStore.NewReader(tt.args.streamID)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileStreamStore.NewReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			data, err := ioutil.ReadAll(reader)
			if err != nil {
				t.Errorf("readAll failed %+v", err)
				return
			}

			if !bytes.Equal(data, tt.want) {
				t.Fatalf("data no equal want:\ndata:%s\nwant:%s",
					string(data), string(tt.want))
			}
		})
	}
}
