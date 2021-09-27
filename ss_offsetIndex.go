package yatsdb

import (
	"encoding/binary"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
)

type SeriesStreamOffsetIndex struct {
	OffsetIndexUpdater
	db      *badger.DB
	batcher *BadgerDBBatcher
}

const (
	STOffsetPrefix    = "0$"
	STOffsetPrefixLen = len(STOffsetPrefix)
)

func OpenStreamTimestampOffsetIndex(storePath string) (*SeriesStreamOffsetIndex, error) {
	return nil, fmt.Errorf("not implemented")
}

func (index *SeriesStreamOffsetIndex) SetStreamTimestampOffset(offset SeriesStreamOffset, callback func(err error)) {
	index.batcher.Update(BadgerOP{
		Op: func(txn *badger.Txn) error {
			key := make([]byte, 16+STOffsetPrefixLen)
			value := make([]byte, 8)

			copy(key, STOffsetPrefix)
			key = key[STOffsetPrefixLen:]

			binary.BigEndian.PutUint64(key, uint64(offset.StreamID))
			key = key[8:]
			binary.BigEndian.PutUint64(key, uint64(offset.TimestampMS))

			binary.BigEndian.PutUint64(value, uint64(offset.Offset))
			if err := txn.Set(key, value); err != nil {
				return errors.WithStack(err)
			}
			return nil
		},
		Commit: callback,
	})
}
