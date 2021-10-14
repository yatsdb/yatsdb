package invertedindex

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
)

func MetricsMatches(metrics []StreamMetric, matchers ...*Matcher) []StreamMetric {
	var result = make([]StreamMetric, 0, len(metrics)/2)
	for _, metric := range metrics {
		if metricMatches(metric, matchers...) {
			result = append(result, metric)
		}
	}
	return result
}

func metricMatches(metric StreamMetric, matchers ...*Matcher) bool {
	for _, matcher := range matchers {
		if !matcher.Matches(metric) {
			return false
		}
	}
	return true
}

type Matcher struct {
	matchEmpty    bool
	labelsMatcher *labels.Matcher
}

func NewMatcher(labelMatcher prompb.LabelMatcher) *Matcher {
	matcher := &Matcher{
		labelsMatcher: labels.MustNewMatcher(labels.MatchType(labelMatcher.Type),
			labelMatcher.Name, labelMatcher.Value),
	}
	matcher.matchEmpty = matcher.labelsMatcher.Matches("")
	return matcher
}

func (matcher *Matcher) Matches(metric StreamMetric) bool {
	switch matcher.labelsMatcher.Type {
	case labels.MatchEqual:
		var findLabel = false
		for _, label := range metric.Labels {
			if matcher.labelsMatcher.Name == label.Name {
				findLabel = true
				if matcher.labelsMatcher.Matches(label.Value) {
					return true
				}
			}
		}
		// l=""
		// If the matchers for a labelname selects an empty value, it selects all
		// the series which don't have the label name set too. See:
		// https://github.com/prometheus/prometheus/issues/3575 and
		// https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
		return matcher.matchEmpty && !findLabel
	case labels.MatchNotEqual, labels.MatchNotRegexp:
		var findLabel = false
		for _, label := range metric.Labels {
			if matcher.labelsMatcher.Name == label.Name {
				findLabel = true
				if matcher.labelsMatcher.Matches(label.Value) {
					continue
				}
				return false
			}
		}
		return findLabel || matcher.matchEmpty
	case labels.MatchRegexp:
		var findLabel = false
		for _, label := range metric.Labels {
			if matcher.labelsMatcher.Name == label.Name {
				findLabel = true
				if matcher.labelsMatcher.Matches(label.Value) {
					return true
				}
			}
		}
		return !findLabel && matcher.matchEmpty
	}
	return false
}
