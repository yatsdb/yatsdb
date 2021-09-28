package yatsdb

import (
	"context"
	"encoding/binary"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	badgerbatcher "github.com/yatsdb/yatsdb/badger-batcher"
)

type SeriesStreamOffsetIndex struct {
	db      *badger.DB
	batcher *badgerbatcher.BadgerDBBatcher
	cancel  context.CancelFunc
}

const (
	STOffsetPrefix    = "0$"
	STOffsetPrefixLen = len(STOffsetPrefix)
)

func OpenStreamTimestampOffsetIndex(ctx context.Context, storePath string) (*SeriesStreamOffsetIndex, error) {
	db, err := badger.Open(badger.DefaultOptions(storePath))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	batcher := badgerbatcher.NewBadgerDBBatcher(ctx, 64, db)
	batcher.Start()
	return &SeriesStreamOffsetIndex{
		db:      db,
		batcher: batcher,
		cancel:  cancel,
	}, nil
}

func (index *SeriesStreamOffsetIndex) Close() {
	index.cancel()
}
func (index *SeriesStreamOffsetIndex) SetStreamTimestampOffset(offset SeriesStreamOffset, fn func(err error)) {
	index.batcher.Update(badgerbatcher.BadgerOP{
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
		Commit: fn,
	})
}
