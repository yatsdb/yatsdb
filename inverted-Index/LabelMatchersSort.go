package invertedindex

import (
	"sort"

	"github.com/prometheus/prometheus/prompb"
)

func LabelMatchersSort(matchers []*prompb.LabelMatcher) []*prompb.LabelMatcher {
	sort.Slice(matchers, func(i, j int) bool {
		return !NewMatcher(matchers[i]).matchEmpty
	})
	return matchers
}
