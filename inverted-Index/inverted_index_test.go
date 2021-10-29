package invertedindex

import (
	"context"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/prometheus/prometheus/prompb"
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
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})
	dbpath := t.Name()
	index, err := OpenBadgerIndex(context.Background(), dbpath)
	if err != nil {
		t.Fatalf(err.Error())
	}
	t.Cleanup(func() {
		os.RemoveAll(dbpath)
	})

	type args struct {
		streamMetric StreamMetric
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{

			args: args{streamMetric: StreamMetric{
				Labels: []prompb.Label{
					{Name: "a", Value: "b"},
					{Name: "i", Value: "a"},
				},
				StreamID: 1,
			}},
		},
		{

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
			if err := index.Insert(tt.args.streamMetric); (err != nil) != tt.wantErr {
				t.Errorf("BadgerIndex.Insert() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBadgerIndex_Matches(t *testing.T) {

	dbpath := t.Name()
	index, err := OpenBadgerIndex(context.Background(), dbpath)
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
		if err := index.Insert(metric); err != nil {
			t.Fatal(err.Error())
		}
	}

	type args struct {
		labelMatchers []*prompb.LabelMatcher
	}

	tests := []struct {
		name    string
		args    args
		want    []StreamMetric
		wantErr bool
	}{
		{

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

			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_EQ, Name: "i", Value: "a"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
			},
		},
		{

			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_EQ, Name: "i", Value: "missing"},
			}},
			want: nil,
		},
		{

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

			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_NEQ, Name: "n", Value: "1"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "2"}}, StreamID: 4},
				{Labels: []prompb.Label{{Name: "n", Value: "2.5"}}, StreamID: 5},
			},
		},
		{

			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_NEQ, Name: "i", Value: ""},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}, StreamID: 3},
			},
		},
		{

			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_NEQ, Name: "missing", Value: ""},
			}},
			want: nil,
		},
		{

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

			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_RE, Name: "i", Value: "^a$"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
			},
		},
		{

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

			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_RE, Name: "i", Value: "^$"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}}, StreamID: 1},
			},
		},
		{

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

			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_NRE, Name: "n", Value: "^1$"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "2"}}, StreamID: 4},
				{Labels: []prompb.Label{{Name: "n", Value: "2.5"}}, StreamID: 5},
			},
		},
		{

			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_NRE, Name: "i", Value: "^a?"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}, StreamID: 3},
			},
		},
		{

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

			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_NRE, Name: "i", Value: "^.*$"},
			}},
			want: nil,
		},
		{

			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
				{Type: prompb.LabelMatcher_NRE, Name: "i", Value: "^.+$"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}}, StreamID: 1},
			},
		},
		{

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

			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_RE, Name: "i", Value: "a|b"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}, StreamID: 2},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}, StreamID: 3},
			},
		},
		{

			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_RE, Name: "n", Value: "x2|2"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "2"}}, StreamID: 4},
			},
		},
		{

			args: args{labelMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_RE, Name: "n", Value: "2|2\\.5"},
			}},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "2"}}, StreamID: 4},
				{Labels: []prompb.Label{{Name: "n", Value: "2.5"}}, StreamID: 5},
			},
		},
		{

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
