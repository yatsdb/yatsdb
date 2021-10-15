package invertedindex

import (
	"reflect"
	"testing"

	"github.com/prometheus/prometheus/prompb"
)

func TestLabelMatchersSort(t *testing.T) {
	type args struct {
		matchers []*prompb.LabelMatcher
	}
	tests := []struct {
		name string
		args args
		want []*prompb.LabelMatcher
	}{
		{
			args: args{
				matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  "b",
						Value: "",
					},
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  "a",
						Value: "1",
					},
				},
			},
			want: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "a",
					Value: "1",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "b",
					Value: "",
				},
			},
		},
		{
			args: args{
				matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_RE,
						Name:  "b",
						Value: "a||c",
					},
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  "a",
						Value: "1",
					},
				},
			},
			want: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "a",
					Value: "1",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "b",
					Value: "a||c",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LabelMatchersSort(tt.args.matchers); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LabelMatchersSort() = %v, want %v", got, tt.want)
			}
		})
	}
}
