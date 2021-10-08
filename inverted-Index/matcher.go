package invertedindex

import "github.com/prometheus/prometheus/prompb"

type Matcher struct {
	laberMatchers []*prompb.LabelMatcher
}

func (matcher *Matcher) Match(key string, value string) bool {
	for _, laberMatcher := range matcher.laberMatchers {
		switch laberMatcher.Type {
		case prompb.LabelMatcher_EQ:
			if laberMatcher.Name != key || laberMatcher.Value != value {
				return false
			}

		}
	}
	return false
}
