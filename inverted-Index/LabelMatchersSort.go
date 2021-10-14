package invertedindex

import (
	"sort"

	"github.com/prometheus/prometheus/prompb"
)

func LabelMatchersSort(matchers []*prompb.LabelMatcher) {
	sort.Slice(matchers, func(i, j int) bool {
		if matchers[i].Type == prompb.LabelMatcher_EQ {
			return matchers[i].Value != ""
		}
		return true
	})
}
