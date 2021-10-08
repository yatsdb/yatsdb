package invertedindex

import (
	"encoding/binary"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
	badgerbatcher "github.com/yatsdb/yatsdb/badger-batcher"
)

type StreamID uint64

type StreamMetric struct {
	Labels []prompb.Label
	//streamID  metric stream
	StreamID StreamID
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
	sep = []byte(`\xff`)
)

/*
key format:
name sep value sep streamIO
*/
func (index *BadgerIndex) Insert(streamMetric StreamMetric) error {
	if err := index.update(func(txn *badger.Txn) error {
		var IDBuf = make([]byte, 8)
		binary.BigEndian.PutUint64(IDBuf, uint64(streamMetric.StreamID))
		for _, label := range streamMetric.Labels {
			buf := make([]byte, len(label.Name)+len(label.Value)+len(sep)+8)
			key := buf
			//copy name
			n := copy(buf, label.Name)
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

func (index *BadgerIndex) Matcher(matcher ...*prompb.LabelMatcher) ([]StreamMetric, error) {
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
