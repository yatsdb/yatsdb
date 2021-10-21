package aoss

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
	"gopkg.in/stretchr/testify.v1/assert"
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
			got, err := OpenFileStreamStore(FileStreamStoreOptions{
				Dir:            tt.args.dir,
				SyncWrite:      true,
				WriteGorutines: 128,
			})
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

	fsStore, err := OpenFileStreamStore(FileStreamStoreOptions{
		Dir:            t.Name(),
		SyncWrite:      false,
		WriteGorutines: 128,
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	if fsStore.Dir == "" {
		t.Fatal("fsStore.Dir empty")
	}

	type args struct {
		streamID StreamID
		data     []byte
		fn       func(offset int64, err error)
	}

	var wg sync.WaitGroup

	tests := []struct {
		name string
		args args
	}{
		{
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
			fsStore.Append(tt.args.streamID, tt.args.data, tt.args.fn)
		})
	}

	wg.Wait()
}

func TestFileStreamStore_NewReader(t *testing.T) {
	t.Cleanup(func() {
		_ = os.RemoveAll(t.Name())
	})

	fsStore, err := OpenFileStreamStore(FileStreamStoreOptions{
		Dir:            t.Name(),
		SyncWrite:      false,
		WriteGorutines: 12,
	})
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

	type args struct {
		streamID StreamID
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			args: args{
				streamID: 1,
			},
			want: []byte(bytesWrites[1]),
		},
		{
			args: args{
				streamID: 2,
			},
			want: []byte(bytesWrites[2]),
		},
		{
			args: args{
				streamID: 3,
			},
			want: []byte(bytesWrites[3]),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

func TestFileStreamStore_Close(t *testing.T) {

	t.Cleanup(func() {
		_ = os.RemoveAll(t.Name())
	})

	fsStore, err := OpenFileStreamStore(FileStreamStoreOptions{
		Dir:            t.Name(),
		SyncWrite:      false,
		WriteGorutines: 128,
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	for i := 0; i < 100; i++ {
		for j := 0; j < 100; j++ {
			_, err = fsStore.AppendSync(invertedindex.StreamID(j), []byte(fmt.Sprintf("hello world %d", j)))
			assert.NoError(t, err)
		}
	}

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := fsStore.Close(); (err != nil) != tt.wantErr {
				t.Errorf("FileStreamStore.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
