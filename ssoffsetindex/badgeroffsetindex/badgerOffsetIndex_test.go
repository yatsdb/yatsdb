package badgeroffsetindex

import (
	"context"
	"math"
	"os"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v3"
	badgerbatcher "github.com/yatsdb/yatsdb/badger-batcher"
	. "github.com/yatsdb/yatsdb/ss-offsetindex"
	"gotest.tools/v3/assert"
)

func TestOpenStreamTimestampOffsetIndex(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})
	type args struct {
		ctx       context.Context
		storePath string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			args: args{
				ctx:       context.Background(),
				storePath: t.Name(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := OpenStreamTimestampOffsetIndex(tt.args.ctx, tt.args.storePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenStreamTimestampOffsetIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				t.Errorf("OpenStreamTimestampOffsetIndex return nil")
			}
		})
	}
}

func TestSeriesStreamOffsetIndex_SetStreamTimestampOffset(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})

	indexDB, err := OpenStreamTimestampOffsetIndex(context.Background(), t.Name())
	if err != nil {
		t.Fatal(err.Error())
	}

	type fields struct {
		db      *badger.DB
		batcher *badgerbatcher.BadgerDBBatcher
	}
	type args struct {
		offset SeriesStreamOffset
		fn     func(err error)
	}

	f := fields{
		db:      indexDB.db,
		batcher: indexDB.batcher,
	}

	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			fields: f,
			args: args{offset: SeriesStreamOffset{
				StreamID:    1,
				TimestampMS: 1,
				Offset:      1,
			},
				fn: func(err error) { assert.NilError(t, err) }},
		},
		{
			fields: f,
			args: args{offset: SeriesStreamOffset{
				StreamID:    1,
				TimestampMS: 2,
				Offset:      2,
			},
				fn: func(err error) { assert.NilError(t, err) }},
		},
		{
			fields: f,
			args: args{offset: SeriesStreamOffset{
				StreamID:    1,
				TimestampMS: 2,
				Offset:      2,
			},
				fn: func(err error) { assert.NilError(t, err) }},
		},
		{
			fields: f,
			args: args{offset: SeriesStreamOffset{
				StreamID:    2,
				TimestampMS: 1,
				Offset:      1,
			},
				fn: func(err error) { assert.NilError(t, err) }},
		},
		{
			fields: f,
			args: args{offset: SeriesStreamOffset{
				StreamID:    2,
				TimestampMS: 2,
				Offset:      2,
			},
				fn: func(err error) { assert.NilError(t, err) }},
		},
	}
	var wg sync.WaitGroup
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index := &SeriesStreamOffsetIndex{
				db:      tt.fields.db,
				batcher: tt.fields.batcher,
			}
			index.SetStreamTimestampOffset(tt.args.offset, func(err error) {
				wg.Done()
				tt.args.fn(err)
			})
		})
	}
	wg.Wait()
}

func TestSeriesStreamOffsetIndex_GetStreamTimestampOffset(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})

	indexDB, err := OpenStreamTimestampOffsetIndex(context.Background(), t.Name())
	if err != nil {
		t.Fatal(err.Error())
	}

	SetStreamTimestampOffset := func(offset SeriesStreamOffset) {
		var wg sync.WaitGroup
		wg.Add(1)
		indexDB.SetStreamTimestampOffset(offset, func(err error) {
			if err != nil {
				t.Fatal(err.Error())
			}
			wg.Done()
		})
		wg.Wait()
	}
	SetStreamTimestampOffset(SeriesStreamOffset{StreamID: 1, TimestampMS: 1, Offset: 1})
	SetStreamTimestampOffset(SeriesStreamOffset{StreamID: 1, TimestampMS: 2, Offset: 2})
	SetStreamTimestampOffset(SeriesStreamOffset{StreamID: 1, TimestampMS: 3, Offset: 3})

	SetStreamTimestampOffset(SeriesStreamOffset{StreamID: 2, TimestampMS: 1, Offset: 1})
	SetStreamTimestampOffset(SeriesStreamOffset{StreamID: 2, TimestampMS: 2, Offset: 2})
	SetStreamTimestampOffset(SeriesStreamOffset{StreamID: 2, TimestampMS: 3, Offset: 3})

	type fields struct {
		db      *badger.DB
		batcher *badgerbatcher.BadgerDBBatcher
	}
	type args struct {
		streamID    StreamID
		timestampMS int64
		LE          bool
	}
	f := fields{
		db:      indexDB.db,
		batcher: indexDB.batcher,
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int64
		wantErr bool
	}{
		{
			fields: f,
			args: args{
				streamID:    1,
				timestampMS: 0,
				LE:          false,
			},
			want: 1,
		},
		{
			fields: f,
			args: args{
				streamID:    1,
				timestampMS: 1,
				LE:          false,
			},
			want: 1,
		},
		{
			fields: f,
			args: args{
				streamID:    1,
				timestampMS: 2,
				LE:          false,
			},
			want: 2,
		},
		{
			fields: f,
			args: args{
				streamID:    1,
				timestampMS: 3,
				LE:          false,
			},
			want: 3,
		},
		{
			fields: f,
			args: args{
				streamID:    1,
				timestampMS: 4,
				LE:          true,
			},
			want: 3,
		},
		{
			fields: f,
			args: args{
				streamID:    1,
				timestampMS: math.MaxInt64,
				LE:          true,
			},
			want: 3,
		},

		//stream2
		{
			fields: f,
			args: args{
				streamID:    2,
				timestampMS: 0,
				LE:          false,
			},
			want: 1,
		},
		{
			fields: f,
			args: args{
				streamID:    2,
				timestampMS: 1,
				LE:          false,
			},
			want: 1,
		},
		{
			fields: f,
			args: args{
				streamID:    2,
				timestampMS: 2,
				LE:          false,
			},
			want: 2,
		},
		{
			fields: f,
			args: args{
				streamID:    2,
				timestampMS: 3,
				LE:          false,
			},
			want: 3,
		},
		{
			fields: f,
			args: args{
				streamID:    2,
				timestampMS: 4,
				LE:          true,
			},
			want: 3,
		},
		{
			fields: f,
			args: args{
				streamID:    2,
				timestampMS: math.MaxInt64,
				LE:          true,
			},
			want: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index := &SeriesStreamOffsetIndex{
				db:      tt.fields.db,
				batcher: tt.fields.batcher,
			}
			got, err := index.GetStreamTimestampOffset(tt.args.streamID, tt.args.timestampMS, tt.args.LE)
			if (err != nil) != tt.wantErr {
				t.Errorf("SeriesStreamOffsetIndex.GetStreamTimestampOffset() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SeriesStreamOffsetIndex.GetStreamTimestampOffset() = %v, want %v", got, tt.want)
			}
		})
	}
}
