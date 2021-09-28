package invertedindex

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/prometheus/prometheus/prompb"
	badgerbatcher "github.com/yatsdb/yatsdb/badger-batcher"
)

type StreamID uint64
type IndexInserter interface {
	Insert(labels prompb.Labels, ID StreamID) error
}

type IndexMatcher interface {
}

type Index struct {
	IndexInserter
	IndexMatcher
}

type BadgerIndex struct {
	db      *badger.DB
	batcher *badgerbatcher.BadgerDBBatcher
}

//support RBP filter  https://github.com/RoaringBitmap/gocroaring
func (index *BadgerIndex) Insert(labels prompb.Labels, ID StreamID) error {
	return nil
}
