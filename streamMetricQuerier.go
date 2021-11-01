package yatsdb

import (
	"time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
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
	begin := time.Now()
	streamMetrics, err := querier.metricMatcher.Matches(query.Matchers...)
	if err != nil {
		return nil, err
	}
	logrus.WithFields(logrus.Fields{
		"count":   len(streamMetrics),
		"elapsed": time.Since(begin),
	}).Info("matches metrics success")
	begin = time.Now()
	//Todo multi goroutine to get offset
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
	logrus.WithFields(logrus.Fields{
		"count":   len(streamMetrics),
		"elapsed": time.Since(begin),
	}).Info("GetStreamTimestampOffset success")
	return offset, nil
}
