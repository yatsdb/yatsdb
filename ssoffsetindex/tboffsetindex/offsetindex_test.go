package tboffsetindex

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/sirupsen/logrus"
	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
	"github.com/yatsdb/yatsdb/aoss/stream-store/wal"
	"github.com/yatsdb/yatsdb/ssoffsetindex"
	"gopkg.in/stretchr/testify.v1/assert"
)

func TestDB_getFileSTOffsetTables(t *testing.T) {
	type fields struct {
		Options            Options
		offsetTables       *[]STOffsetTable
		flushTables        *[]STOffsetTable
		FileSTOffsetTables *[]FileSTOffsetTable
	}
	tests := []struct {
		name   string
		fields fields
		want   []FileSTOffsetTable
	}{
		{
			name: "",
			fields: fields{
				Options: Options{},
				FileSTOffsetTables: &[]FileSTOffsetTable{
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{1, 2},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   2,
							},
						},
						filename: "1",
					},
				},
			},
			want: []FileSTOffsetTable{
				{
					TimeStamp: struct {
						From int64
						To   int64
					}{1, 2},
					STOffsets: []STOffset{
						{
							StreamId: 1,
							Offset:   2,
						},
					},
					filename: "1",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				Options:            tt.fields.Options,
				offsetTables:       tt.fields.offsetTables,
				flushTables:        tt.fields.flushTables,
				FileSTOffsetTables: tt.fields.FileSTOffsetTables,
			}
			if got := db.getFileSTOffsetTables(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DB.getFileSTOffsetTables() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_setFileSTOffsetTable(t *testing.T) {
	type fields struct {
		Options            Options
		FileSTOffsetTables *[]FileSTOffsetTable
	}
	type args struct {
		tables []FileSTOffsetTable
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "",
			fields: fields{
				Options:            Options{},
				FileSTOffsetTables: &[]FileSTOffsetTable{},
			},
			args: args{
				tables: []FileSTOffsetTable{
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{1, 2},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   2,
							},
						},
						filename: "1",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				Options:            tt.fields.Options,
				FileSTOffsetTables: tt.fields.FileSTOffsetTables,
			}
			db.setFileSTOffsetTable(tt.args.tables)
			assert.NotNil(t, db.getFileSTOffsetTables())
		})
	}
}

func TestDB_getFlushTables(t *testing.T) {
	type fields struct {
		Options            Options
		offsetTables       *[]STOffsetTable
		flushTables        *[]STOffsetTable
		FileSTOffsetTables *[]FileSTOffsetTable
	}
	tests := []struct {
		name   string
		fields fields
		want   []STOffsetTable
	}{
		{
			name: "",
			fields: fields{
				Options:      Options{},
				offsetTables: &[]STOffsetTable{},
				flushTables: &[]STOffsetTable{
					{
						WalDir: "1",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 1,
							To:   2,
						},
						Offsets:      map[ssoffsetindex.StreamID]int64{1: 1},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
				},
				FileSTOffsetTables: &[]FileSTOffsetTable{},
			},
			want: []STOffsetTable{
				{
					WalDir: "1",
					Timestamp: struct {
						From int64
						To   int64
					}{
						From: 1,
						To:   2,
					},
					Offsets:      map[ssoffsetindex.StreamID]int64{1: 1},
					wal:          nil,
					tablesLocker: &sync.Mutex{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				Options:            tt.fields.Options,
				offsetTables:       tt.fields.offsetTables,
				flushTables:        tt.fields.flushTables,
				FileSTOffsetTables: tt.fields.FileSTOffsetTables,
			}
			if got := db.getFlushTables(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DB.getFlushTables() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_setFlushTables(t *testing.T) {
	type fields struct {
		Options            Options
		offsetTables       *[]STOffsetTable
		flushTables        *[]STOffsetTable
		FileSTOffsetTables *[]FileSTOffsetTable
	}
	type args struct {
		tables []STOffsetTable
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "",
			fields: fields{
				Options:            Options{},
				offsetTables:       &[]STOffsetTable{},
				flushTables:        &[]STOffsetTable{},
				FileSTOffsetTables: &[]FileSTOffsetTable{},
			},
			args: args{
				tables: []STOffsetTable{
					{
						WalDir: "1",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 1,
							To:   2,
						},
						Offsets:      map[ssoffsetindex.StreamID]int64{1: 1},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				Options:            tt.fields.Options,
				offsetTables:       tt.fields.offsetTables,
				flushTables:        tt.fields.flushTables,
				FileSTOffsetTables: tt.fields.FileSTOffsetTables,
			}
			db.setFlushTables(tt.args.tables)
			assert.Equal(t, db.getFlushTables(), tt.args.tables)
		})
	}
}

func TestDB_getOffsetTables(t *testing.T) {
	type fields struct {
		Options            Options
		offsetTables       *[]STOffsetTable
		flushTables        *[]STOffsetTable
		FileSTOffsetTables *[]FileSTOffsetTable
	}
	tests := []struct {
		name   string
		fields fields
		want   []STOffsetTable
	}{
		{
			name: "",
			fields: fields{
				Options: Options{},
				offsetTables: &[]STOffsetTable{
					{
						WalDir: "1",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 1,
							To:   2,
						},
						Offsets:      map[ssoffsetindex.StreamID]int64{1: 1},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
				},
				flushTables:        &[]STOffsetTable{},
				FileSTOffsetTables: &[]FileSTOffsetTable{},
			},
			want: []STOffsetTable{
				{
					WalDir: "1",
					Timestamp: struct {
						From int64
						To   int64
					}{
						From: 1,
						To:   2,
					},
					Offsets:      map[ssoffsetindex.StreamID]int64{1: 1},
					wal:          nil,
					tablesLocker: &sync.Mutex{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				Options:            tt.fields.Options,
				offsetTables:       tt.fields.offsetTables,
				flushTables:        tt.fields.flushTables,
				FileSTOffsetTables: tt.fields.FileSTOffsetTables,
			}
			if got := db.getOffsetTables(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DB.getOffsetTables() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_setOffsetTables(t *testing.T) {
	type fields struct {
		Options            Options
		offsetTables       *[]STOffsetTable
		flushTables        *[]STOffsetTable
		FileSTOffsetTables *[]FileSTOffsetTable
	}
	type args struct {
		tables []STOffsetTable
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "",
			fields: fields{
				Options:            Options{},
				offsetTables:       &[]STOffsetTable{},
				flushTables:        &[]STOffsetTable{},
				FileSTOffsetTables: &[]FileSTOffsetTable{},
			},
			args: args{
				tables: []STOffsetTable{
					{
						WalDir: "1",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 1,
							To:   2,
						},
						Offsets:      map[ssoffsetindex.StreamID]int64{1: 1},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				Options:            tt.fields.Options,
				offsetTables:       tt.fields.offsetTables,
				flushTables:        tt.fields.flushTables,
				FileSTOffsetTables: tt.fields.FileSTOffsetTables,
			}
			db.setOffsetTables(tt.args.tables)
			assert.Equal(t, db.getOffsetTables(), tt.args.tables)
		})
	}
}

func TestDB_flushFileOffsetTable(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})
	_ = os.MkdirAll(t.Name()+"/1-2", 0777)
	type fields struct {
		Options            Options
		offsetTables       *[]STOffsetTable
		flushTables        *[]STOffsetTable
		FileSTOffsetTables *[]FileSTOffsetTable
	}
	type args struct {
		table STOffsetTable
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "",
			fields: fields{
				Options: Options{
					FileTableDir:        t.Name(),
					WalDir:              t.Name(),
					OffsetTableInterval: 0,
				},
				offsetTables:       &[]STOffsetTable{},
				flushTables:        &[]STOffsetTable{},
				FileSTOffsetTables: &[]FileSTOffsetTable{},
			},
			args: args{
				table: STOffsetTable{
					WalDir: t.Name() + "/1-2",
					Timestamp: struct {
						From int64
						To   int64
					}{
						From: 1,
						To:   2,
					},
					Offsets: map[ssoffsetindex.StreamID]int64{1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 1, 7: 1, 8: 1, 9: 1, 10: 2},
					wal: func() wal.Wal {
						w, err := wal.Reload(wal.DefaultOption(t.Name()+"/1-2"), func(et streamstorepb.EntryTyper) error {
							panic("empty dir")
						})
						if err != nil {
							t.Fatal(err.Error())
						}
						return w
					}(),
					tablesLocker: &sync.Mutex{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				Options:            tt.fields.Options,
				offsetTables:       tt.fields.offsetTables,
				flushTables:        tt.fields.flushTables,
				FileSTOffsetTables: tt.fields.FileSTOffsetTables,
			}
			db.flushFileOffsetTable(tt.args.table)

			filetables := db.getFileSTOffsetTables()
			assert.True(t, len(filetables) == 1)

			filetable := filetables[0]
			var offsets []STOffset
			for k, v := range tt.args.table.Offsets {
				offsets = append(offsets, STOffset{
					StreamId: k,
					Offset:   v,
				})
			}
			sort.Slice(offsets, func(i, j int) bool {
				return offsets[i].StreamId < offsets[j].StreamId
			})
			assert.Equal(t, filetable.STOffsets, offsets)
		})
	}
}

func Test_unsafeSlice(t *testing.T) {
	var offsets []STOffset
	for i := 0; i < 100; i++ {
		offsets = append(offsets, STOffset{
			StreamId: ssoffsetindex.StreamID(i),
			Offset:   int64(i),
		})
	}
	var data = make([]byte, int(unsafe.Sizeof(offsets[0]))*len(offsets))
	for index, offset := range offsets {
		*(*STOffset)(unsafe.Pointer(&data[index*int(unsafe.Sizeof(offset))])) = offset
	}
	var offsets2 []STOffset
	unsafeSlice(unsafe.Pointer(&offsets2), unsafe.Pointer(&data[0]), int(len(data)/int(unsafe.Sizeof(STOffset{}))))
	assert.Equal(t, offsets2,
		offsets)
}

func TestDB_flushOffsetMap(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})
	type fields struct {
		Options            Options
		offsetTables       *[]STOffsetTable
		flushTables        *[]STOffsetTable
		FileSTOffsetTables *[]FileSTOffsetTable
	}
	now := time.Now().Add(time.Second)
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "",
			fields: fields{
				Options: Options{
					FileTableDir:        t.Name(),
					WalDir:              t.Name(),
					OffsetTableInterval: time.Minute * 10,
				},
				offsetTables: &[]STOffsetTable{
					{
						WalDir: func() string {
							dir := t.Name() +
								fmt.Sprintf("/%d-%d",
									now.Add(-time.Minute*10).UnixMilli(),
									now.UnixMilli())
							os.MkdirAll(dir, 0777)
							return dir
						}(),
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: now.Add(-time.Minute * 10).UnixMilli(),
							To:   now.UnixMilli(),
						},
						Offsets: map[ssoffsetindex.StreamID]int64{
							1:  1,
							2:  2,
							3:  3,
							4:  4,
							5:  5,
							6:  6,
							7:  7,
							8:  8,
							9:  9,
							10: 10},
						wal: func() wal.Wal {
							w, err := wal.Reload(wal.DefaultOption(func() string {
								dir := t.Name() +
									fmt.Sprintf("/%d-%d",
										now.Add(-time.Minute*10).UnixMilli(),
										now.UnixMilli())
								os.MkdirAll(dir, 0777)
								return dir
							}()), func(et streamstorepb.EntryTyper) error {
								panic("empty dir")
							})
							if err != nil {
								t.Fatal(err.Error())
							}
							return w
						}(),
						tablesLocker: &sync.Mutex{},
					},
					{
						WalDir: "",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: now.UnixMilli(),
							To:   now.Add(time.Minute * 10).UnixMilli(),
						},
						Offsets:      map[ssoffsetindex.StreamID]int64{},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
				},
				flushTables:        &[]STOffsetTable{},
				FileSTOffsetTables: &[]FileSTOffsetTable{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				Options:            tt.fields.Options,
				offsetTables:       tt.fields.offsetTables,
				flushTables:        tt.fields.flushTables,
				FileSTOffsetTables: tt.fields.FileSTOffsetTables,
			}
			db.flushOffsetMap()

			offsetTables := db.getOffsetTables()
			assert.True(t, len(offsetTables) == 2)
			assert.Equal(t, offsetTables[len(offsetTables)-1].Timestamp.From, now.Add(time.Minute*10).UnixMilli())

			fileTables := db.getFileSTOffsetTables()
			assert.Equal(t, 1, len(fileTables))

		})
	}
}

func Test_parseTimestamp(t *testing.T) {
	type args struct {
		filename string
	}
	tests := []struct {
		name     string
		args     args
		wantFrom int64
		wantTo   int64
		wantErr  bool
	}{
		{
			name: "",
			args: args{
				filename: "1-2.table",
			},
			wantFrom: 1,
			wantTo:   2,
			wantErr:  false,
		},
		{
			name: "",
			args: args{
				filename: "data/1-2",
			},
			wantFrom: 1,
			wantTo:   2,
			wantErr:  false,
		},
		{
			name: "",
			args: args{
				filename: "data/1-2.log",
			},
			wantFrom: 1,
			wantTo:   2,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFrom, gotTo, err := parseTimestamp(tt.args.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTimestamp() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotFrom != tt.wantFrom {
				t.Errorf("parseTimestamp() gotFrom = %v, want %v", gotFrom, tt.wantFrom)
			}
			if gotTo != tt.wantTo {
				t.Errorf("parseTimestamp() gotTo = %v, want %v", gotTo, tt.wantTo)
			}
		})
	}
}

func createWal(dir string) wal.Wal {
	_ = os.MkdirAll(dir, 0777)
	w, err := wal.Reload(wal.DefaultOption(dir), nil)
	if err != nil {
		panic(err.Error())
	}
	return w
}

func TestDB_SetStreamTimestampOffset(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})
	type fields struct {
		Options            Options
		offsetTables       *[]STOffsetTable
		flushTables        *[]STOffsetTable
		FileSTOffsetTables *[]FileSTOffsetTable
	}
	type args struct {
		entry    ssoffsetindex.SeriesStreamOffset
		callback func(err error)
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "",
			fields: fields{
				Options: Options{},
				offsetTables: &[]STOffsetTable{
					{
						WalDir: t.Name() + "/1-10",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 1,
							To:   10,
						},
						Offsets:      map[ssoffsetindex.StreamID]int64{},
						wal:          createWal(t.Name() + "/1-10"),
						tablesLocker: &sync.Mutex{},
					},
				},
				flushTables:        &[]STOffsetTable{},
				FileSTOffsetTables: &[]FileSTOffsetTable{},
			},
			args: args{
				entry: ssoffsetindex.SeriesStreamOffset{
					StreamID:    1,
					TimestampMS: 1,
					Offset:      1,
				},
				callback: func(err error) {
					assert.NoError(t, err)
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				Options:            tt.fields.Options,
				offsetTables:       tt.fields.offsetTables,
				flushTables:        tt.fields.flushTables,
				FileSTOffsetTables: tt.fields.FileSTOffsetTables,
			}
			db.SetStreamTimestampOffset(tt.args.entry, tt.args.callback)
			assert.Equal(t, db.getOffsetTables()[0].Offsets[tt.args.entry.StreamID], tt.args.entry.Offset)
		})
	}
}

func TestDB_GetStreamTimestampOffset(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	newRef := func() *int32 {
		var ref int32
		ref = 1
		return &ref
	}
	type fields struct {
		Options            Options
		offsetTables       *[]STOffsetTable
		flushTables        *[]STOffsetTable
		FileSTOffsetTables *[]FileSTOffsetTable
	}
	type args struct {
		streamID    ssoffsetindex.StreamID
		timestampMS int64
		LE          bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "fileSTOffsetTables_ts_1",
			fields: fields{
				Options:      Options{},
				offsetTables: &[]STOffsetTable{},
				flushTables:  &[]STOffsetTable{},
				FileSTOffsetTables: &[]FileSTOffsetTable{
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 1,
							To:   100,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   1,
							},
							{
								StreamId: 2,
								Offset:   2,
							},
							{
								StreamId: 3,
								Offset:   3,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "1-100.table",
						ref:      newRef(),
					},
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 100,
							To:   200,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   10,
							},
							{
								StreamId: 2,
								Offset:   20,
							},
							{
								StreamId: 3,
								Offset:   30,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "100-200.table",
						ref:      newRef(),
					},
				},
			},
			args: args{
				streamID:    3,
				timestampMS: 1,
			},
			want:    3,
			wantErr: false,
		},
		{

			name: "fileSTOffsetTables_ts_0",
			fields: fields{
				Options:      Options{},
				offsetTables: &[]STOffsetTable{},
				flushTables:  &[]STOffsetTable{},
				FileSTOffsetTables: &[]FileSTOffsetTable{
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 1,
							To:   100,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   1,
							},
							{
								StreamId: 2,
								Offset:   2,
							},
							{
								StreamId: 3,
								Offset:   3,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "1-100.table",
						ref:      newRef(),
					},
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 100,
							To:   200,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   10,
							},
							{
								StreamId: 2,
								Offset:   20,
							},
							{
								StreamId: 3,
								Offset:   30,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "100-200.table",
						ref:      newRef(),
					},
				},
			},
			args: args{
				streamID:    3,
				timestampMS: 0,
			},
			want:    0,
			wantErr: true,
		},
		{

			name: "fileSTOffsetTables_ts_99",
			fields: fields{
				Options:      Options{},
				offsetTables: &[]STOffsetTable{},
				flushTables:  &[]STOffsetTable{},
				FileSTOffsetTables: &[]FileSTOffsetTable{
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 1,
							To:   100,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   1,
							},
							{
								StreamId: 2,
								Offset:   2,
							},
							{
								StreamId: 3,
								Offset:   3,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "1-100.table",
						ref:      newRef(),
					},
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 100,
							To:   200,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   10,
							},
							{
								StreamId: 2,
								Offset:   20,
							},
							{
								StreamId: 3,
								Offset:   30,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "100-200.table",
						ref:      newRef(),
					},
				},
			},
			args: args{
				streamID:    3,
				timestampMS: 99,
			},
			want:    3,
			wantErr: false,
		},
		{

			name: "fileSTOffsetTables_ts_100",
			fields: fields{
				Options:      Options{},
				offsetTables: &[]STOffsetTable{},
				flushTables:  &[]STOffsetTable{},
				FileSTOffsetTables: &[]FileSTOffsetTable{
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 1,
							To:   100,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   1,
							},
							{
								StreamId: 2,
								Offset:   2,
							},
							{
								StreamId: 3,
								Offset:   3,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "1-100.table",
						ref:      newRef(),
					},
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 100,
							To:   200,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   10,
							},
							{
								StreamId: 2,
								Offset:   20,
							},
							{
								StreamId: 3,
								Offset:   30,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "100-200.table",
						ref:      newRef(),
					},
				},
			},
			args: args{
				streamID:    3,
				timestampMS: 100,
			},
			want:    30,
			wantErr: false,
		},
		{

			name: "fileSTOffsetTables_ts_200",
			fields: fields{
				Options:      Options{},
				offsetTables: &[]STOffsetTable{},
				flushTables:  &[]STOffsetTable{},
				FileSTOffsetTables: &[]FileSTOffsetTable{
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 1,
							To:   100,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   1,
							},
							{
								StreamId: 2,
								Offset:   2,
							},
							{
								StreamId: 3,
								Offset:   3,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "1-100.table",
						ref:      newRef(),
					},
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 100,
							To:   200,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   10,
							},
							{
								StreamId: 2,
								Offset:   20,
							},
							{
								StreamId: 3,
								Offset:   30,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "100-200.table",
						ref:      newRef(),
					},
				},
			},
			args: args{
				streamID:    3,
				timestampMS: 100,
			},
			want:    30,
			wantErr: false,
		},
		{

			name: "fileSTOffsetTables_ts_1000",
			fields: fields{
				Options:      Options{},
				offsetTables: &[]STOffsetTable{},
				flushTables:  &[]STOffsetTable{},
				FileSTOffsetTables: &[]FileSTOffsetTable{
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 1,
							To:   100,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   1,
							},
							{
								StreamId: 2,
								Offset:   2,
							},
							{
								StreamId: 3,
								Offset:   3,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "1-100.table",
						ref:      newRef(),
					},
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 100,
							To:   200,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   10,
							},
							{
								StreamId: 2,
								Offset:   20,
							},
							{
								StreamId: 3,
								Offset:   30,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "100-200.table",
						ref:      newRef(),
					},
				},
			},
			args: args{
				streamID:    3,
				timestampMS: 1000,
			},
			want:    30,
			wantErr: false,
		},
		{

			name: "flushTables_ts_200",
			fields: fields{
				Options:      Options{},
				offsetTables: &[]STOffsetTable{},
				flushTables: &[]STOffsetTable{
					{
						WalDir: "",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 200,
							To:   300,
						},
						Offsets: map[ssoffsetindex.StreamID]int64{
							1: 100,
							2: 200,
							3: 300,
						},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
				},
				FileSTOffsetTables: &[]FileSTOffsetTable{
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 1,
							To:   100,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   1,
							},
							{
								StreamId: 2,
								Offset:   2,
							},
							{
								StreamId: 3,
								Offset:   3,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "1-100.table",
						ref:      newRef(),
					},
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 100,
							To:   200,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   10,
							},
							{
								StreamId: 2,
								Offset:   20,
							},
							{
								StreamId: 3,
								Offset:   30,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "100-200.table",
						ref:      newRef(),
					},
				},
			},
			args: args{
				streamID:    3,
				timestampMS: 1000,
			},
			want:    300,
			wantErr: false,
		},
		{

			name: "flushTables_ts_300",
			fields: fields{
				Options:      Options{},
				offsetTables: &[]STOffsetTable{},
				flushTables: &[]STOffsetTable{
					{
						WalDir: "",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 200,
							To:   300,
						},
						Offsets: map[ssoffsetindex.StreamID]int64{
							1: 100,
							2: 200,
							3: 300,
						},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
				},
				FileSTOffsetTables: &[]FileSTOffsetTable{
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 1,
							To:   100,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   1,
							},
							{
								StreamId: 2,
								Offset:   2,
							},
							{
								StreamId: 3,
								Offset:   3,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "1-100.table",
						ref:      newRef(),
					},
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 100,
							To:   200,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   10,
							},
							{
								StreamId: 2,
								Offset:   20,
							},
							{
								StreamId: 3,
								Offset:   30,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "100-200.table",
						ref:      newRef(),
					},
				},
			},
			args: args{
				streamID:    3,
				timestampMS: 1000,
			},
			want:    300,
			wantErr: false,
		},
		{

			name: "flushTables_ts_400",
			fields: fields{
				Options:      Options{},
				offsetTables: &[]STOffsetTable{},
				flushTables: &[]STOffsetTable{
					{
						WalDir: "",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 200,
							To:   300,
						},
						Offsets: map[ssoffsetindex.StreamID]int64{
							1: 100,
							2: 200,
							3: 300,
						},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
					{
						WalDir: "",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 300,
							To:   400,
						},
						Offsets: map[ssoffsetindex.StreamID]int64{
							1: 1000,
							2: 2000,
							3: 3000,
						},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
				},
				FileSTOffsetTables: &[]FileSTOffsetTable{
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 1,
							To:   100,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   1,
							},
							{
								StreamId: 2,
								Offset:   2,
							},
							{
								StreamId: 3,
								Offset:   3,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "1-100.table",
						ref:      newRef(),
					},
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 100,
							To:   200,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   10,
							},
							{
								StreamId: 2,
								Offset:   20,
							},
							{
								StreamId: 3,
								Offset:   30,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "100-200.table",
						ref:      newRef(),
					},
				},
			},
			args: args{
				streamID:    3,
				timestampMS: 1000,
			},
			want:    3000,
			wantErr: false,
		},
		{

			name: "offsetTables_ts_500",
			fields: fields{
				Options: Options{},
				offsetTables: &[]STOffsetTable{
					{
						WalDir: "",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 500,
							To:   600,
						},
						Offsets: map[ssoffsetindex.StreamID]int64{
							1: 10000,
							2: 20000,
							3: 30000,
						},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
				},
				flushTables: &[]STOffsetTable{
					{
						WalDir: "",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 200,
							To:   300,
						},
						Offsets: map[ssoffsetindex.StreamID]int64{
							1: 100,
							2: 200,
							3: 300,
						},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
					{
						WalDir: "",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 300,
							To:   400,
						},
						Offsets: map[ssoffsetindex.StreamID]int64{
							1: 1000,
							2: 2000,
							3: 3000,
						},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
				},
				FileSTOffsetTables: &[]FileSTOffsetTable{
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 1,
							To:   100,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   1,
							},
							{
								StreamId: 2,
								Offset:   2,
							},
							{
								StreamId: 3,
								Offset:   3,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "1-100.table",
						ref:      newRef(),
					},
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 100,
							To:   200,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   10,
							},
							{
								StreamId: 2,
								Offset:   20,
							},
							{
								StreamId: 3,
								Offset:   30,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "100-200.table",
						ref:      newRef(),
					},
				},
			},
			args: args{
				streamID:    3,
				timestampMS: 500,
			},
			want:    30000,
			wantErr: false,
		},
		{

			name: "offsetTables_ts_600",
			fields: fields{
				Options: Options{},
				offsetTables: &[]STOffsetTable{
					{
						WalDir: "",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 500,
							To:   600,
						},
						Offsets: map[ssoffsetindex.StreamID]int64{
							1: 10000,
							2: 20000,
							3: 30000,
						},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
					{

						WalDir: "",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 600,
							To:   700,
						},
						Offsets: map[ssoffsetindex.StreamID]int64{
							1: 11000,
							2: 22000,
							3: 33000,
						},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
				},
				flushTables: &[]STOffsetTable{
					{
						WalDir: "",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 200,
							To:   300,
						},
						Offsets: map[ssoffsetindex.StreamID]int64{
							1: 100,
							2: 200,
							3: 300,
						},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
					{
						WalDir: "",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 300,
							To:   400,
						},
						Offsets: map[ssoffsetindex.StreamID]int64{
							1: 1000,
							2: 2000,
							3: 3000,
						},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
				},
				FileSTOffsetTables: &[]FileSTOffsetTable{
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 1,
							To:   100,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   1,
							},
							{
								StreamId: 2,
								Offset:   2,
							},
							{
								StreamId: 3,
								Offset:   3,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "1-100.table",
						ref:      newRef(),
					},
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 100,
							To:   200,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   10,
							},
							{
								StreamId: 2,
								Offset:   20,
							},
							{
								StreamId: 3,
								Offset:   30,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "100-200.table",
						ref:      newRef(),
					},
				},
			},
			args: args{
				streamID:    3,
				timestampMS: 600,
			},
			want:    33000,
			wantErr: false,
		},
		{

			name: "offsetTables_ts_700",
			fields: fields{
				Options: Options{},
				offsetTables: &[]STOffsetTable{
					{
						WalDir: "",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 500,
							To:   600,
						},
						Offsets: map[ssoffsetindex.StreamID]int64{
							1: 10000,
							2: 20000,
							3: 30000,
						},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
					{

						WalDir: "",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 600,
							To:   700,
						},
						Offsets: map[ssoffsetindex.StreamID]int64{
							1: 11000,
							2: 22000,
							3: 33000,
						},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
				},
				flushTables: &[]STOffsetTable{
					{
						WalDir: "",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 200,
							To:   300,
						},
						Offsets: map[ssoffsetindex.StreamID]int64{
							1: 100,
							2: 200,
							3: 300,
						},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
					{
						WalDir: "",
						Timestamp: struct {
							From int64
							To   int64
						}{
							From: 300,
							To:   400,
						},
						Offsets: map[ssoffsetindex.StreamID]int64{
							1: 1000,
							2: 2000,
							3: 3000,
						},
						wal:          nil,
						tablesLocker: &sync.Mutex{},
					},
				},
				FileSTOffsetTables: &[]FileSTOffsetTable{
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 1,
							To:   100,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   1,
							},
							{
								StreamId: 2,
								Offset:   2,
							},
							{
								StreamId: 3,
								Offset:   3,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "1-100.table",
						ref:      newRef(),
					},
					{
						TimeStamp: struct {
							From int64
							To   int64
						}{
							From: 100,
							To:   200,
						},
						STOffsets: []STOffset{
							{
								StreamId: 1,
								Offset:   10,
							},
							{
								StreamId: 2,
								Offset:   20,
							},
							{
								StreamId: 3,
								Offset:   30,
							},
						},
						mfile:    &fileutil.MmapFile{},
						filename: "100-200.table",
						ref:      newRef(),
					},
				},
			},
			args: args{
				streamID:    3,
				timestampMS: 600,
			},
			want:    33000,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				Options:            tt.fields.Options,
				offsetTables:       tt.fields.offsetTables,
				flushTables:        tt.fields.flushTables,
				FileSTOffsetTables: tt.fields.FileSTOffsetTables,
			}
			got, err := db.GetStreamTimestampOffset(tt.args.streamID, tt.args.timestampMS, tt.args.LE)
			if (err != nil) != tt.wantErr {
				t.Errorf("DB.GetStreamTimestampOffset() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DB.GetStreamTimestampOffset() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOpen(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})
	opts := Options{
		FileTableDir:        t.Name(),
		WalDir:              t.Name(),
		OffsetTableInterval: time.Second * 3,
		Retention: struct {
			Time time.Duration `yaml:"retention"`
		}{
			Time: time.Hour * 10000,
		},
		TickerInterval: time.Millisecond * 100,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	var scount = 100

	assert.True(t, len(db.getOffsetTables()) > 0)

	for i := 0; i < scount; i++ {
		var wg sync.WaitGroup
		wg.Add(1)
		db.SetStreamTimestampOffset(ssoffsetindex.SeriesStreamOffset{
			StreamID:    ssoffsetindex.StreamID(i),
			TimestampMS: time.Now().UnixMilli(),
			Offset:      int64(i),
		}, func(err error) {
			assert.NoError(t, err)
			wg.Done()
		})
		wg.Wait()
	}
	time.Sleep(time.Second * 5)

	for i := 0; i < scount; i++ {
		offset, err := db.GetStreamTimestampOffset(ssoffsetindex.StreamID(i),
			time.Now().UnixMilli(), false)
		assert.NoError(t, err)
		assert.Equal(t, offset, int64(i))
	}
	assert.NoError(t, db.Close())

	db, err = Open(opts)
	assert.NoError(t, err)

	for i := 0; i < scount; i++ {
		offset, err := db.GetStreamTimestampOffset(ssoffsetindex.StreamID(i),
			time.Now().UnixMilli(), false)
		assert.NoError(t, err)
		assert.Equal(t, offset, int64(i))
	}

}
