package yatsdb

import (
	"context"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
	filestreamstore "github.com/yatsdb/yatsdb/aoss/file-stream-store"
)

func TestOpenTSDB(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
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
			args: args{
				options: Options{
					BadgerDBStoreDir: t.Name() + "/badgerdbstore",
					FileStreamStoreOptions: filestreamstore.FileStreamStoreOptions{
						Dir: t.Name() + "/fileStreamStore",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := OpenTSDB(tt.args.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenTSDB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				t.Errorf("OpenTSDB nil")
			}
		})
	}
}

func Test_tsdb_WriteSamples(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	t.Cleanup(func() {
		//	os.RemoveAll(t.Name())
	})

	tsdb, err := OpenTSDB(Options{
		BadgerDBStoreDir: t.Name() + "/badgerdbstore",
		FileStreamStoreOptions: filestreamstore.FileStreamStoreOptions{
			Dir: t.Name() + "/fileStreamStore",
		},
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	type args struct {
		request *prompb.WriteRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			args: args{
				request: &prompb.WriteRequest{
					Timeseries: []prompb.TimeSeries{
						{
							Labels: []prompb.Label{{Name: "n", Value: "1"}},
							Samples: []prompb.Sample{
								{Timestamp: 1, Value: 1},
								{Timestamp: 2, Value: 2},
								{Timestamp: 3, Value: 3},
								{Timestamp: 4, Value: 4}},
						},
						{
							Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
							Samples: []prompb.Sample{
								{Timestamp: 1, Value: 1},
								{Timestamp: 2, Value: 2},
								{Timestamp: 3, Value: 3},
								{Timestamp: 4, Value: 4}},
						},
						{
							Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
							Samples: []prompb.Sample{
								{Timestamp: 1, Value: 1},
								{Timestamp: 2, Value: 2},
								{Timestamp: 3, Value: 3},
								{Timestamp: 4, Value: 4}},
						},
						{
							Labels: []prompb.Label{{Name: "n", Value: "2"}},
							Samples: []prompb.Sample{
								{Timestamp: 1, Value: 1},
								{Timestamp: 2, Value: 2},
								{Timestamp: 3, Value: 3},
								{Timestamp: 4, Value: 4}},
						},
						{
							Labels: []prompb.Label{{Name: "n", Value: "2.5"}},
							Samples: []prompb.Sample{
								{Timestamp: 1, Value: 1},
								{Timestamp: 2, Value: 2},
								{Timestamp: 3, Value: 3},
								{Timestamp: 4, Value: 4}},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tsdb.WriteSamples(tt.args.request); (err != nil) != tt.wantErr {
				t.Errorf("tsdb.WriteSamples() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

}

func Test_tsdb_ReadSimples(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})

	tsdb, err := OpenTSDB(Options{
		BadgerDBStoreDir: t.Name() + "/badgerdbstore",
		FileStreamStoreOptions: filestreamstore.FileStreamStoreOptions{
			Dir: t.Name() + "/fileStreamStore",
		},
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = tsdb.WriteSamples(&prompb.WriteRequest{Timeseries: []prompb.TimeSeries{
		{
			Labels: []prompb.Label{{Name: "n", Value: "1"}},
			Samples: []prompb.Sample{
				{Timestamp: 1, Value: 1},
				{Timestamp: 2, Value: 2},
				{Timestamp: 3, Value: 3},
				{Timestamp: 4, Value: 4}},
		},
		{
			Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
			Samples: []prompb.Sample{
				{Timestamp: 1, Value: 1},
				{Timestamp: 2, Value: 2},
				{Timestamp: 3, Value: 3},
				{Timestamp: 4, Value: 4}},
		},
		{
			Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
			Samples: []prompb.Sample{
				{Timestamp: 1, Value: 1},
				{Timestamp: 2, Value: 2},
				{Timestamp: 3, Value: 3},
				{Timestamp: 4, Value: 4}},
		},
		{
			Labels: []prompb.Label{{Name: "n", Value: "2"}},
			Samples: []prompb.Sample{
				{Timestamp: 1, Value: 1},
				{Timestamp: 2, Value: 2},
				{Timestamp: 3, Value: 3},
				{Timestamp: 4, Value: 4}},
		},
		{
			Labels: []prompb.Label{{Name: "n", Value: "2.5"}},
			Samples: []prompb.Sample{
				{Timestamp: 1, Value: 1},
				{Timestamp: 2, Value: 2},
				{Timestamp: 3, Value: 3},
				{Timestamp: 4, Value: 4}},
		},
	}})
	if err != nil {
		t.Fatal(err.Error())
	}

	type args struct {
		req *prompb.ReadRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *prompb.ReadResponse
		wantErr bool
	}{
		{
			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
							},
							StartTimestampMs: 0,
							EndTimestampMs:   5,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
									{Timestamp: 3, Value: 3},
									{Timestamp: 4, Value: 4},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
									{Timestamp: 3, Value: 3},
									{Timestamp: 4, Value: 4},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
									{Timestamp: 3, Value: 3},
									{Timestamp: 4, Value: 4},
								},
							},
						},
					},
				},
			},
		},
		{
			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   1,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
								},
							},
						},
					},
				},
			},
		},
		{
			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
							},
							StartTimestampMs: 5,
							EndTimestampMs:   5,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
							},
						},
					},
				},
			},
		},
		{
			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
							},
							StartTimestampMs: 4,
							EndTimestampMs:   500,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels:  []prompb.Label{{Name: "n", Value: "1"}},
								Samples: []prompb.Sample{{Timestamp: 4, Value: 4}},
							},
							{
								Labels:  []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
								Samples: []prompb.Sample{{Timestamp: 4, Value: 4}},
							},
							{
								Labels:  []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
								Samples: []prompb.Sample{{Timestamp: 4, Value: 4}},
							},
						},
					},
				},
			},
		},
		{
			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
								{Type: prompb.LabelMatcher_EQ, Name: "i", Value: "a"},
							},
							StartTimestampMs: 0,
							EndTimestampMs:   500,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
									{Timestamp: 3, Value: 3},
									{Timestamp: 4, Value: 4},
								},
							},
						},
					},
				},
			},
		},
		{
			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
								{Type: prompb.LabelMatcher_EQ, Name: "i", Value: "missing"},
							},
							StartTimestampMs: 0,
							EndTimestampMs:   500,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{},
				},
			},
		},
		{
			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "missing", Value: ""},
							},
							StartTimestampMs: 0,
							EndTimestampMs:   500,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
									{Timestamp: 3, Value: 3},
									{Timestamp: 4, Value: 4},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
									{Timestamp: 3, Value: 3},
									{Timestamp: 4, Value: 4},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
									{Timestamp: 3, Value: 3},
									{Timestamp: 4, Value: 4},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "2"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
									{Timestamp: 3, Value: 3},
									{Timestamp: 4, Value: 4}},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "2.5"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
									{Timestamp: 3, Value: 3},
									{Timestamp: 4, Value: 4}},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_NEQ, Name: "n", Value: "1"},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "2"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "2.5"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_NEQ, Name: "i", Value: ""},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_NEQ, Name: "missing", Value: ""},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
								{Type: prompb.LabelMatcher_NEQ, Name: "i", Value: "a"},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
								{Type: prompb.LabelMatcher_NEQ, Name: "i", Value: ""},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{
			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_RE, Name: "n", Value: "^1$"},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
								{Type: prompb.LabelMatcher_RE, Name: "i", Value: "^a$"},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
								{Type: prompb.LabelMatcher_RE, Name: "i", Value: "^a?$"},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_RE, Name: "i", Value: "^$"},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "2"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "2.5"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
								{Type: prompb.LabelMatcher_RE, Name: "i", Value: "^$"},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{
			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
								{Type: prompb.LabelMatcher_RE, Name: "i", Value: "^.*$"},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
								{Type: prompb.LabelMatcher_RE, Name: "i", Value: "^.+$"},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_NRE, Name: "n", Value: "^1$"},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "2"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "2.5"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
								{Type: prompb.LabelMatcher_NRE, Name: "i", Value: "^a?"},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
								{Type: prompb.LabelMatcher_NRE, Name: "i", Value: "^$"},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
								{Type: prompb.LabelMatcher_NRE, Name: "i", Value: "^.*$"},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
								{Type: prompb.LabelMatcher_NRE, Name: "i", Value: "^.+$"},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"},
								{Type: prompb.LabelMatcher_NRE, Name: "i", Value: "b"},
								{Type: prompb.LabelMatcher_RE, Name: "i", Value: "^(b|a).*$"},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_RE, Name: "n", Value: "1|2"},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "2"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_RE, Name: "i", Value: "a|b"},
							},
							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_RE, Name: "n", Value: "x2|2"},
							},

							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "2"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_RE, Name: "n", Value: "2|2\\.5"},
							},

							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "2"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "2.5"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
		{

			args: args{
				req: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Type: prompb.LabelMatcher_RE, Name: "i", Value: "c||d"},
							},

							StartTimestampMs: 1,
							EndTimestampMs:   2,
						},
					},
				},
			},
			want: &prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{{Name: "n", Value: "1"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "2"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
							{
								Labels: []prompb.Label{{Name: "n", Value: "2.5"}},
								Samples: []prompb.Sample{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: 2},
								},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tsdb.ReadSamples(context.Background(), tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("tsdb.ReadSimples() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			SortQueryResponse(got)
			SortQueryResponse(tt.want)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("tsdb.ReadSimples() = \n%v\n, want \n%v\n", JS(got), JS(tt.want))
			}
		})
	}
}

func SortQueryResponse(response *prompb.ReadResponse) {
	for index := range response.Results {
		sort.Slice(response.Results[index].Timeseries, func(i, j int) bool {
			return JS(response.Results[index].Timeseries[i]) < JS(response.Results[index].Timeseries[j])
		})
	}
}
