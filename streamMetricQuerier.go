package yatsdb

import (
	"github.com/prometheus/prometheus/prompb"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
	ssoffsetindex "github.com/yatsdb/yatsdb/ssoffsetindex"
)

type streamMetricQuerier struct {
	streamTimestampOffsetGetter ssoffsetindex.StreamTimestampOffsetGetter
	metricMatcher               invertedindex.IndexMatcher
}

var _ StreamMetricQuerier = (*streamMetricQuerier)(nil)

func (querier *streamMetricQuerier) QueryStreamMetric(query *prompb.Query) ([]*StreamMetricOffset, error) {
	var offset []*StreamMetricOffset
	streamMetrics, err := querier.metricMatcher.Matches(query.Matchers...)
	if err != nil {
		return nil, err
	}
	for _, metric := range streamMetrics {
		offsetStart, err := querier.streamTimestampOffsetGetter.GetStreamTimestampOffset(metric.StreamID, query.StartTimestampMs, false)
		if err != nil {
			if err != ssoffsetindex.ErrNoFindOffset {
				return nil, err
			}
		}
		offset = append(offset, &StreamMetricOffset{
			StreamMetric:     metric,
			Offset:           offsetStart,
			StartTimestampMs: query.StartTimestampMs,
			EndTimestampMs:   query.EndTimestampMs,
		})
	}
	return offset, nil
}
