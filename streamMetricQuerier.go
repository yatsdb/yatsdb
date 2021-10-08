package yatsdb

import (
	"fmt"

	"github.com/prometheus/prometheus/prompb"
)

type streamMetricQuerier struct {
	StreamMetricQuerier
}

var _ StreamMetricQuerier = (*streamMetricQuerier)(nil)

func (querier *streamMetricQuerier) QueryStreamMetric(query *prompb.Query) ([]*StreamMetricOffset, error) {
	return nil, fmt.Errorf("not implemented")
}
