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
type IndexInserter interface {
	Insert(labels prompb.Labels, ID StreamID) error
}

type IndexMatcher interface {
	Matcher(matcher ...*prompb.LabelMatcher) ([]StreamID, error)
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

//support RBP filter  https://github.com/RoaringBitmap/gocroaring
func (index *BadgerIndex) Insert(labels prompb.Labels, ID StreamID) error {
	if err := index.update(func(txn *badger.Txn) error {
		var IDBuf = make([]byte, 8)
		binary.BigEndian.PutUint64(IDBuf, uint64(ID))
		for _, label := range labels.Labels {
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

func (index *BadgerIndex) Matcher(matcher ...*prompb.LabelMatcher) ([]StreamID, error) {
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
