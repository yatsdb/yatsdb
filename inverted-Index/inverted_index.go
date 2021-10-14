package invertedindex

import (
	"encoding/binary"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
	badgerbatcher "github.com/yatsdb/yatsdb/badger-batcher"
)

type StreamID uint64

type StreamMetric struct {
	Labels []prompb.Label `json:"labels,omitempty"`
	//streamID  metric stream
	StreamID StreamID `json:"stream_id,omitempty"`
	//Offset to read
}

type IndexInserter interface {
	Insert(streamMetric StreamMetric) error
}

type IndexMatcher interface {
	Matcher(matcher ...*prompb.LabelMatcher) ([]StreamMetric, error)
}

type Index struct {
	IndexInserter
	IndexMatcher
}

type BadgerIndex struct {
	db      *badger.DB
	batcher *badgerbatcher.BadgerDBBatcher
}

var (
	invertedKeyPrefix = "$0"
	metricKeyPrefix   = "$1"
	sep               = []byte(`\xff`)
)

/*
key format: |$0|sep|lable_name|sep|lable_value|stream_id
name sep value sep streamIO
*/
func (index *BadgerIndex) Insert(streamMetric StreamMetric) error {
	if err := index.update(func(txn *badger.Txn) error {
		var IDBuf = make([]byte, 8)
		binary.BigEndian.PutUint64(IDBuf, uint64(streamMetric.StreamID))
		for _, label := range streamMetric.Labels {
			buf := make([]byte, len(invertedKeyPrefix)+len(sep)+len(label.Name)+len(label.Value)+len(sep)+8)
			key := buf
			//copy name
			n := copy(buf, invertedKeyPrefix+string(sep)+label.Name)
			buf = buf[n:]
			//copy sep
			n = copy(buf, sep)
			buf = buf[n:]
			//copy label value
			n = copy(buf, label.Value)
			buf = buf[n:]
			//copy ID
			n = copy(buf, IDBuf)
			buf = buf[n:]
			if len(buf) > 0 {
				panic("encode label error")
			}
			if err := txn.Set(key, IDBuf); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		logrus.Errorf("db update failed %+v", err)
		return err
	}
	return nil
}

func (index *BadgerIndex) Matcher(matchers ...*prompb.LabelMatcher) ([]StreamMetric, error) {
	LabelMatchersSort(matchers)
	firstMatcher := matchers[0]
	index.db.View(func(txn *badger.Txn) error {
		matcher := NewMatcher(*firstMatcher)
		if matcher.matchEmpty &&
			(matcher.labelsMatcher.Type == labels.MatchEqual || matcher.labelsMatcher.Type == labels.MatchRegexp) {
			// l=""
			// If the matchers for a labelname selects an empty value, it selects all
			// the series which don't have the label name set too. See:
			// https://github.com/prometheus/prometheus/issues/3575 and
			// https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555

		} else {
		}
		return nil
	})

	return nil, fmt.Errorf("not implemented")
}

func (index *BadgerIndex) update(fn func(txn *badger.Txn) error) error {
	var errs = make(chan error)
	index.batcher.Update(badgerbatcher.BadgerOP{
		Op: fn,
		Commit: func(err error) {
			errs <- err
		},
	})
	return <-errs
}
