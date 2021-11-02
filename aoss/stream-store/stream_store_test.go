package streamstore

import (
	"bytes"
	"fmt"
	"io/fs"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
	"gopkg.in/stretchr/testify.v1/assert"
)

func TestStreamStore_appendMtable(t *testing.T) {
	type fields struct {
		mTables *[]MTable
	}
	type args struct {
		mtable MTable
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "",
			fields: fields{
				mTables: &[]MTable{},
			},
			args: args{
				mtable: newMTable(nil),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ss := &StreamStore{
				mTables: tt.fields.mTables,
			}
			ss.appendMtable(tt.args.mtable)
			tables := ss.getMtables()
			assert.Equal(t, 1, len(tables))

			ss.updateTables([]MTable{})
			assert.Equal(t, 1, len(tables))
			assert.Equal(t, 0, len(ss.getMtables()))
		})
	}
}

func TestOpen(t *testing.T) {
	t.Cleanup(func() {
		_ = os.RemoveAll(t.Name())
	})
	type args struct {
		options Options
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "",
			args: args{
				options: DefaultOptionsWithDir(t.Name()),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Open(tt.args.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.NotNil(t, got)
		})
	}
}

func TestStreamStore_Append(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	t.Cleanup(func() {
		//	_ = os.RemoveAll(t.Name())
	})
	opts := DefaultOptionsWithDir(t.Name())
	opts.MaxMemTableSize = 128 << 10
	opts.MaxMTables = 3
	ss, err := Open(opts)
	assert.NoError(t, err)

	var sCount = 3333
	var streamSize = 3333
	var bufferMap = make(map[StreamID]*bytes.Buffer)

	var wg sync.WaitGroup
	t.Run("append", func(t *testing.T) {
		for i := 0; i < sCount; i++ {
			remain := streamSize
			for remain > 0 {
				wg.Add(1)
				buf := make([]byte, rand.Intn(64)+16)
				if len(buf) > remain {
					buf = buf[:remain]
				}
				remain -= len(buf)
				_, err := rand.Read(buf)
				assert.NoError(t, err)
				if bufferMap[StreamID(i)] == nil {
					bufferMap[StreamID(i)] = bytes.NewBuffer(nil)
				}
				bufferMap[StreamID(i)].Write(buf)
				ss.Append(StreamID(i), buf, func(offset int64, err error) {
					assert.NoError(t, err)
					wg.Done()
				})
			}
		}
		wg.Wait()
	})
	wg.Wait()

	t.Run("streamReader", func(t *testing.T) {
		for i := 0; i < sCount; i++ {
			reader, err := ss.NewReader(invertedindex.StreamID(i))
			assert.NoError(t, err)

			data, err := ioutil.ReadAll(reader)
			assert.NoError(t, err)
			assert.True(t, bytes.Equal(data, bufferMap[invertedindex.StreamID(i)].Bytes()))
		}
		for !(len(ss.getMtables()) <= opts.MaxMTables) {
			fmt.Println(len(ss.getMtables()))
			time.Sleep(time.Second)
		}
	})

	ss.Options.MinMergedSegmentSize = 10 << 20

	ss.mergeSegments()

	return

	t.Run("close", func(t *testing.T) {
		assert.NoError(t, ss.Close())
	})

	t.Run("reload", func(t *testing.T) {
		ss, err = Open(opts)
		assert.NoError(t, err)
	})

	t.Run("reader", func(t *testing.T) {
		for i := 0; i < sCount; i++ {
			reader, err := ss.NewReader(invertedindex.StreamID(i))
			assert.NoError(t, err)

			data, err := ioutil.ReadAll(reader)
			assert.NoError(t, err)
			assert.True(t, bytes.Equal(data, bufferMap[invertedindex.StreamID(i)].Bytes()))
		}
	})

	t.Run("close", func(t *testing.T) {
		assert.NoError(t, ss.Close())
	})

	t.Run("open/clearSegment", func(t *testing.T) {
		opts.Retention.Time = time.Millisecond
		ss, err = Open(opts)
		assert.NoError(t, err)

		ss.clearSegments()

		assert.Equal(t, len(ss.getSegments()), 0)
		assert.NoError(t, filepath.Walk(opts.SegmentDir, func(path string, info fs.FileInfo, err error) error {
			assert.NoError(t, err)
			if info.IsDir() {
				return nil
			}
			assert.Failf(t, "segment dir should empty,file", path)
			return nil
		}))
	})
}
