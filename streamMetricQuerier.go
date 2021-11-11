package yatsdb

import (
	"math"
	"sync"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
	ssoffsetindex "github.com/yatsdb/yatsdb/ssoffsetindex"
)

type streamMetricQuerier struct {
	streamTimestampOffsetGetter ssoffsetindex.StreamTimestampOffsetGetter
	metricMatcher               invertedindex.IndexMatcher
	getOffsetCh                 chan interface{}
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
	var wg sync.WaitGroup
	for _, metric := range streamMetrics {
		SMOffset := &StreamMetricOffset{
			StreamMetric: metric,
			Query:        query,
		}
		querier.getOffsetCh <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
				<-querier.getOffsetCh
			}()
			offsetStart, err := querier.streamTimestampOffsetGetter.
				GetStreamTimestampOffset(SMOffset.StreamID, query.StartTimestampMs, false)
			if err != nil {
				if err != ssoffsetindex.ErrNoFindOffset {
					logrus.WithError(err).
						Panicf("get steam timestamp offset failed")
				}
				//EOF
				SMOffset.Offset = math.MaxInt64
			} else {
				SMOffset.Offset = offsetStart
			}
		}()
		offset = append(offset, SMOffset)
	}
	logrus.WithFields(logrus.Fields{
		"count":   len(streamMetrics),
		"elapsed": time.Since(begin),
	}).Info("GetStreamTimestampOffset success")
	return offset, nil
}
