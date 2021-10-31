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
