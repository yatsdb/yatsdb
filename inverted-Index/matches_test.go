package invertedindex

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/prometheus/prometheus/prompb"
)

func TestMetricsMatches(t *testing.T) {

	var streamMetrics = []StreamMetric{
		{Labels: []prompb.Label{{Name: "n", Value: "1"}}},
		{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}},
		{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}},
		{Labels: []prompb.Label{{Name: "n", Value: "2"}}},
		{Labels: []prompb.Label{{Name: "n", Value: "2.5"}}},
	}

	type args struct {
		metrics  []StreamMetric
		matchers []*Matcher
	}
	tests := []struct {
		name string
		args args
		want []StreamMetric
	}{
		{
			name: "Simple equals.",
			args: args{
				metrics:  streamMetrics,
				matchers: []*Matcher{NewMatcher(prompb.LabelMatcher{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"})},
			},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}}},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}},
			},
		},

		{
			name: "Simple equals.",
			args: args{
				metrics: streamMetrics,
				matchers: []*Matcher{NewMatcher(prompb.LabelMatcher{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"}),
					NewMatcher(prompb.LabelMatcher{Type: prompb.LabelMatcher_EQ, Name: "i", Value: "a"})},
			},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}},
			},
		},
		{
			name: "Simple equals.",
			args: args{
				metrics: streamMetrics,
				matchers: []*Matcher{NewMatcher(prompb.LabelMatcher{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"}),
					NewMatcher(prompb.LabelMatcher{Type: prompb.LabelMatcher_EQ, Name: "i", Value: "missing"})},
			},
			want: []StreamMetric{},
		},

		{
			name: "Simple equals.",
			args: args{
				metrics:  streamMetrics,
				matchers: []*Matcher{NewMatcher(prompb.LabelMatcher{Type: prompb.LabelMatcher_EQ, Name: "missing", Value: ""})},
			},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}}},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}},
				{Labels: []prompb.Label{{Name: "n", Value: "2"}}},
				{Labels: []prompb.Label{{Name: "n", Value: "2.5"}}},
			},
		},
		{
			name: "Not equals.",
			args: args{
				metrics:  streamMetrics,
				matchers: []*Matcher{NewMatcher(prompb.LabelMatcher{Type: prompb.LabelMatcher_NEQ, Name: "n", Value: "1"})},
			},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "2"}}},
				{Labels: []prompb.Label{{Name: "n", Value: "2.5"}}},
			},
		},
		{
			name: "Not equals.",
			args: args{
				metrics:  streamMetrics,
				matchers: []*Matcher{NewMatcher(prompb.LabelMatcher{Type: prompb.LabelMatcher_NEQ, Name: "i", Value: ""})},
			},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}},
			},
		},
		{
			name: "Not equals.",
			args: args{
				metrics:  streamMetrics,
				matchers: []*Matcher{NewMatcher(prompb.LabelMatcher{Type: prompb.LabelMatcher_NEQ, Name: "missing", Value: ""})},
			},
			want: []StreamMetric{},
		},
		{
			name: "Not equals.",
			args: args{
				metrics: streamMetrics,
				matchers: []*Matcher{NewMatcher(prompb.LabelMatcher{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"}),
					NewMatcher(prompb.LabelMatcher{Type: prompb.LabelMatcher_NEQ, Name: "i", Value: "a"})},
			},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}}},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}},
			},
		},
		{
			name: "Not equals.",
			args: args{
				metrics: streamMetrics,
				matchers: []*Matcher{NewMatcher(prompb.LabelMatcher{Type: prompb.LabelMatcher_EQ, Name: "n", Value: "1"}),
					NewMatcher(prompb.LabelMatcher{Type: prompb.LabelMatcher_NEQ, Name: "i", Value: ""})},
			},
			want: []StreamMetric{
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "a"}}},
				{Labels: []prompb.Label{{Name: "n", Value: "1"}, {Name: "i", Value: "b"}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MetricsMatches(tt.args.metrics, tt.args.matchers...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MetricsMatches() = %+v, want %+v", JS(got), JS(tt.want))
			}
		})
	}
}

func JS(obj interface{}) string {
	data, _ := json.MarshalIndent(obj, "", "    ")
	return string(data)
}
