package invertedindex

import (
	"context"
	"encoding/binary"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
	badgerbatcher "github.com/yatsdb/yatsdb/badger-batcher"
)

var (
	invertedKeyPrefix = "$0"
	metricKeyPrefix   = "$1"
	sep               = []byte(`\xff`)
)

type IndexInserter interface {
	Insert(streamMetric StreamMetric) error
}

type IndexMatcher interface {
	Matches(matcher ...*prompb.LabelMatcher) ([]StreamMetric, error)
}

type Index struct {
	IndexInserter
	IndexMatcher
}

type BadgerIndex struct {
	db              *badger.DB
	batcher         *badgerbatcher.BadgerDBBatcher
	streamIDsLocker *sync.Mutex
	streamIDs       map[StreamID]bool
}

func OpenBadgerIndex(ctx context.Context, path string) (*BadgerIndex, error) {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	go func() {
		for {
			select {
			case <-time.After(time.Minute * 5):
				db.RunValueLogGC(0.5)
			case <-ctx.Done():
				return
			}
		}
	}()
	return &BadgerIndex{
		db:              db,
		batcher:         badgerbatcher.NewBadgerDBBatcher(ctx, 32, db).Start(),
		streamIDsLocker: &sync.Mutex{},
		streamIDs:       make(map[StreamID]bool, 1024),
	}, nil
}

func (index *BadgerIndex) loadOrStoreStreamID(ID StreamID) bool {
	index.streamIDsLocker.Lock()
	_, ok := index.streamIDs[ID]
	if !ok {
		index.streamIDs[ID] = true
	}
	index.streamIDsLocker.Unlock()
	return ok
}

/*
key format: |$0|sep|lable_name|sep|lable_value|stream_id
name sep value sep streamIO
*/
func (index *BadgerIndex) Insert(streamMetric StreamMetric) error {
	if index.loadOrStoreStreamID(streamMetric.StreamID) {
		return nil
	}
	if err := index.update(func(txn *badger.Txn) error {
		var IDBuf = make([]byte, 8)
		binary.BigEndian.PutUint64(IDBuf, uint64(streamMetric.StreamID))
		for _, label := range streamMetric.Labels {
			buf := make([]byte, len(invertedKeyPrefix)+
				len(sep)+len(label.Name)+
				len(sep)+len(label.Value)+
				len(sep)+8)
			key := buf
			//copy name
			n := copy(buf, invertedKeyPrefix)
			buf = buf[n:]
			//copy sep
			n = copy(buf, sep)
			buf = buf[n:]
			//copy label Name
			n = copy(buf, []byte(label.Name))
			buf = buf[n:]

			//copy sep
			n = copy(buf, sep)
			buf = buf[n:]
			//copy label value
			n = copy(buf, []byte(label.Value))
			buf = buf[n:]

			//copy sep
			n = copy(buf, sep)
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
		data, err := streamMetric.Marshal()
		if err != nil {
			return errors.WithStack(err)
		}
		if err := txn.Set([]byte(string(metricKeyPrefix+string(IDBuf))), data); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}); err != nil {
		logrus.Errorf("insert metrics %+v failed %+v", streamMetric, err)
		return err
	}
	return nil
}

func (index *BadgerIndex) Matches(labelMatchers ...*prompb.LabelMatcher) ([]StreamMetric, error) {
	var result []StreamMetric
	LabelMatchersSort(labelMatchers)
	err := index.db.View(func(txn *badger.Txn) error {
		matchers := NewMatchers(labelMatchers...)
		firstMatcher := matchers[0]
		if firstMatcher.matchEmpty &&
			(firstMatcher.labelsMatcher.Type == labels.MatchEqual || firstMatcher.labelsMatcher.Type == labels.MatchRegexp) {
			// l=""
			// If the matchers for a labelname selects an empty value, it selects all
			// the series which don't have the label name set too. See:
			// https://github.com/prometheus/prometheus/issues/3575 and
			// https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555

			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte(metricKeyPrefix)
			iter := txn.NewIterator(opts)
			defer iter.Close()
			for iter.Rewind(); iter.Valid(); iter.Next() {
				var metric StreamMetric
				if err := iter.Item().Value(func(val []byte) error {
					if err := metric.Unmarshal(val); err != nil {
						return errors.WithStack(err)
					}
					return nil
				}); err != nil {
					return err
				}
				if MetricMatches(metric, matchers...) {
					result = append(result, metric)
				}
			}
			return nil
		} else {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte(invertedKeyPrefix + string(sep) + firstMatcher.labelsMatcher.Name)

			if firstMatcher.labelsMatcher.Type == labels.MatchEqual {
				opts.Prefix = []byte(invertedKeyPrefix + string(sep) +
					firstMatcher.labelsMatcher.Name + string(sep) +
					firstMatcher.labelsMatcher.Value + string(sep))
				matchers = matchers[1:]
			}
			var streamIDs []StreamID
			iter := txn.NewIterator(opts)
			defer iter.Close()
			for iter.Rewind(); iter.Valid(); iter.Next() {
				if err := iter.Item().Value(func(val []byte) error {
					if len(val) != 8 {
						return errors.Errorf("value size %d error", len(val))
					}
					streamIDs = append(streamIDs, StreamID(binary.BigEndian.Uint64(val)))
					return nil
				}); err != nil {
					return err
				}
			}

			for _, streamID := range streamIDs {
				var buffer [8]byte
				binary.BigEndian.PutUint64(buffer[:], uint64(streamID))
				item, err := txn.Get([]byte(metricKeyPrefix + string(buffer[:])))
				if err != nil {
					return errors.WithStack(err)
				}
				var metric StreamMetric
				if err := item.Value(func(val []byte) error {
					if err := metric.Unmarshal(val); err != nil {
						return errors.WithStack(err)
					}
					return nil
				}); err != nil {
					return err
				}
				if MetricMatches(metric, matchers...) {
					result = append(result, metric)
				}
			}
			return nil
		}
	})

	if err != nil {
		return nil, err
	}
	return result, nil
}

func (index *BadgerIndex) update(fn func(txn *badger.Txn) error) error {
	var errs = make(chan error, 1)
	index.batcher.Update(badgerbatcher.BadgerOP{
		Op: fn,
		Commit: func(err error) {
			errs <- err
		},
	})
	return <-errs
}
