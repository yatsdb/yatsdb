package invertedindex

import (
	"context"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/prometheus/prometheus/prompb"
	badgerbatcher "github.com/yatsdb/yatsdb/badger-batcher"
)

func TestOpenBadgerIndex(t *testing.T) {
	dbpath := t.Name()
	t.Cleanup(func() {
		os.RemoveAll(dbpath)
	})
	type args struct {
		ctx  context.Context
		path string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			args:    args{ctx: context.Background(), path: dbpath},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := OpenBadgerIndex(tt.args.ctx, tt.args.path); (err != nil) != tt.wantErr {
				t.Errorf("OpenBadgerIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestBadgerIndex_Insert(t *testing.T) {

	dbpath := t.Name()
	db, err := OpenBadgerIndex(context.Background(), dbpath)
	if err != nil {
		t.Fatalf(err.Error())
	}
	t.Cleanup(func() {
		os.RemoveAll(dbpath)
	})

	type fields struct {
		db              *badger.DB
		batcher         *badgerbatcher.BadgerDBBatcher
		streamIDsLocker *sync.Mutex
		streamIDs       map[StreamID]bool
	}
	type args struct {
		streamMetric StreamMetric
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			fields: fields{db: db.db, batcher: db.batcher, streamIDsLocker: db.streamIDsLocker, streamIDs: db.streamIDs},
			args: args{streamMetric: StreamMetric{
				Labels: []prompb.Label{
					{Name: "a", Value: "b"},
					{Name: "i", Value: "a"},
				},
				StreamID: 1,
			}},
		},
		{
			fields: fields{db: db.db, batcher: db.batcher, streamIDsLocker: db.streamIDsLocker, streamIDs: db.streamIDs},
			args: args{streamMetric: StreamMetric{
				Labels: []prompb.Label{
					{Name: "a", Value: "b"},
					{Name: "i", Value: "a"},
					{Name: "j", Value: "a"},
				},
				StreamID: 2,
			}},
		},
		{
			fields: fields{db: db.db, batcher: db.batcher, streamIDsLocker: db.streamIDsLocker, streamIDs: db.streamIDs},
			args: args{streamMetric: StreamMetric{
				Labels: []prompb.Label{
					{Name: "a", Value: "b"},
					{Name: "i", Value: "a"},
					{Name: "j", Value: "a"},
				},
				StreamID: 3,
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index := &BadgerIndex{
				db:              tt.fields.db,
				batcher:         tt.fields.batcher,
				streamIDsLocker: tt.fields.streamIDsLocker,
				streamIDs:       tt.fields.streamIDs,
			}
			if err := index.Insert(tt.args.streamMetric); (err != nil) != tt.wantErr {
				t.Errorf("BadgerIndex.Insert() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBadgerIndex_Matches(t *testing.T) {

	dbpath := t.Name()
	db, err := OpenBadgerIndex(context.Background(), dbpath)
	if err != nil {
		t.Fatalf(err.Error())
	}
	t.Cleanup(func() {
		os.RemoveAll(dbpath)
	})

	var streamMetrics = []StreamMetric{
		{Labels: []prompb.Label{{Name: "n", Value: "1"}}, StreamID: 1},
		{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
		{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}, StreamID: 3},
		{Labels: []prompb.Label{{Name: "n", Value: "2"}}, StreamID: 4},
		{Labels: []prompb.Label{{Name: "n", Value: "2.5"}}, StreamID: 5},
	}

	for _, metric := range streamMetrics {
		if err := db.Insert(metric); err != nil {
			t.Fatal(err.Error())
		}
	}

	type fields struct {
		db              *badger.DB
		batcher         *badgerbatcher.BadgerDBBatcher
		streamIDsLocker *sync.Mutex
		streamIDs       map[StreamID]bool
	}
	type args struct {
		labelMatchers []*prompb.LabelMatcher
	}

	f := fields{db: db.db, batcher: db.batcher, streamIDsLocker: db.streamIDsLocker, streamIDs: db.streamIDs}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []StreamMetric
		wantErr bool
	}{
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}}, StreamID: 1},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}, StreamID: 3},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_EQ, Name: "i", Value: "a"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_EQ, Name: "i", Value: "missing"},
			}},
			want: nil,
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "missing", Value: ""},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}}, StreamID: 1},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}, StreamID: 3},
				{Labels: []prompb.Label{{Name: "n", Value: "2"}}, StreamID: 4},
				{Labels: []prompb.Label{{Name: "n", Value: "2.5"}}, StreamID: 5},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_NEQ, Name: "n", Value: "1"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "2"}}, StreamID: 4},
				{Labels: []prompb.Label{{Name: "n", Value: "2.5"}}, StreamID: 5},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_NEQ, Name: "i", Value: ""},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}, StreamID: 3},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_NEQ, Name: "missing", Value: ""},
			}},
			want: nil,
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_NEQ, Name: "i", Value: "a"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}}, StreamID: 1},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}, StreamID: 3},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_NEQ, Name: "i", Value: ""},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}, StreamID: 3},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_RE, Name: "n", Value: "^1$"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}}, StreamID: 1},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}, StreamID: 3},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_RE, Name: "i", Value: "^a$"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_RE, Name: "i", Value: "^a?$"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}}, StreamID: 1},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_RE, Name: "i", Value: "^$"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}}, StreamID: 1},
				{Labels: []prompb.Label{{Name: "n", Value: "2"}}, StreamID: 4},
				{Labels: []prompb.Label{{Name: "n", Value: "2.5"}}, StreamID: 5},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_RE, Name: "i", Value: "^$"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}}, StreamID: 1},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_RE, Name: "i", Value: "^.*$"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}}, StreamID: 1},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}, StreamID: 3},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_RE, Name: "i", Value: "^.+$"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}, StreamID: 3},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_NRE, Name: "n", Value: "^1$"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "2"}}, StreamID: 4},
				{Labels: []prompb.Label{{Name: "n", Value: "2.5"}}, StreamID: 5},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_NRE, Name: "i", Value: "^a?"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}, StreamID: 3},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_NRE, Name: "i", Value: "^$"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}, StreamID: 3},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_NRE, Name: "i", Value: "^.*$"},
			}},
			want: nil,
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_NRE, Name: "i", Value: "^.+$"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}}, StreamID: 1},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_NRE, Name: "i", Value: "b"},
				{Type: prompb.LabelMatcher_RE, Name: "i", Value: "^(b|a).*$"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_RE, Name: "n", Value: "1|2"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}}, StreamID: 1},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}, StreamID: 3},
				{Labels: []prompb.Label{{Name: "n", Value: "2"}}, StreamID: 4},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_RE, Name: "i", Value: "a|b"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}, StreamID: 3},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_RE, Name: "n", Value: "x2|2"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "2"}}, StreamID: 4},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_RE, Name: "n", Value: "2|2\\.5"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "2"}}, StreamID: 4},
				{Labels: []prompb.Label{{Name: "n", Value: "2.5"}}, StreamID: 5},
			},
		},
		{
			fields: f,
			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_RE, Name: "i", Value: "c||d"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}}, StreamID: 1},
				{Labels: []prompb.Label{{Name: "n", Value: "2"}}, StreamID: 4},
				{Labels: []prompb.Label{{Name: "n", Value: "2.5"}}, StreamID: 5},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index := &BadgerIndex{
				db:              tt.fields.db,
				batcher:         tt.fields.batcher,
				streamIDsLocker: tt.fields.streamIDsLocker,
				streamIDs:       tt.fields.streamIDs,
			}
			got, err := index.Matches(tt.args.labelMatchers...)
			if (err != nil) != tt.wantErr {
				t.Errorf("BadgerIndex.Matches() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			sort.Slice(got, func(i, j int) bool {
				return got[i].StreamID < got[j].StreamID
			})
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BadgerIndex.Matches() = %v, want %v", JS(got), JS(tt.want))
			}
		})
	}
}
