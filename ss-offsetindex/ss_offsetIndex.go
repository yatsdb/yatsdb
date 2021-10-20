package ssoffsetindex

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	badgerbatcher "github.com/yatsdb/yatsdb/badger-batcher"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
)

type StreamID = invertedindex.StreamID

type StreamTimestampOffsetGetter interface {
	//LE less or equal
	GetStreamTimestampOffset(streamID StreamID, timestampMS int64, LE bool) (int64, error)
}
type OffsetIndexUpdater interface {
	SetStreamTimestampOffset(offset SeriesStreamOffset, callback func(err error))
}

//SeriesStreamOffset
type SeriesStreamOffset struct {
	//metrics stream ID
	StreamID StreamID
	//TimestampMS time series samples timestamp
	TimestampMS int64
	//Offset stream offset
	Offset int64
}

type OffsetDB interface {
	StreamTimestampOffsetGetter
	OffsetIndexUpdater
}

type SeriesStreamOffsetIndex struct {
	db      *badger.DB
	batcher *badgerbatcher.BadgerDBBatcher
}

const (
	STOffsetPrefix  = "0$"
	offsetPrefixLen = len(STOffsetPrefix)
)

func NewSeriesStreamOffsetIndex(db *badger.DB, batcher *badgerbatcher.BadgerDBBatcher) *SeriesStreamOffsetIndex {
	return &SeriesStreamOffsetIndex{
		db:      db,
		batcher: batcher,
	}
}

func OpenStreamTimestampOffsetIndex(ctx context.Context, storePath string) (*SeriesStreamOffsetIndex, error) {
	db, err := badger.Open(badger.DefaultOptions(storePath))
	if err != nil {
		return nil, err
	}
	batcher := badgerbatcher.NewBadgerDBBatcher(ctx, 64, db)
	batcher.Start()
	go func() {
		for {
			select {
			case <-time.After(time.Minute * 5):
				_ = db.RunValueLogGC(0.5)
			case <-ctx.Done():
				return
			}
		}
	}()
	return &SeriesStreamOffsetIndex{
		db:      db,
		batcher: batcher,
	}, nil
}

var ErrNoFindOffset = errors.New("streamID offset no find ")

func (index *SeriesStreamOffsetIndex) GetStreamTimestampOffset(streamID StreamID, timestampMS int64, LE bool) (int64, error) {
	var offset int64
	err := index.db.View(func(txn *badger.Txn) error {
		keyBuffer := make([]byte, 16+offsetPrefixLen)
		key := keyBuffer

		copy(keyBuffer, STOffsetPrefix)
		keyBuffer = keyBuffer[offsetPrefixLen:]
		binary.BigEndian.PutUint64(keyBuffer, uint64(streamID))
		keyBuffer = keyBuffer[8:]
		binary.BigEndian.PutUint64(keyBuffer, uint64(timestampMS))

		opts := badger.DefaultIteratorOptions
		opts.Prefix = key[:offsetPrefixLen+8]
		opts.Reverse = LE

		iter := txn.NewIterator(opts)
		defer iter.Close()

		iter.Seek(key)
		if iter.Valid() {
			iter.Item()
			key := iter.Item().Key()
			if len(key) != 16+offsetPrefixLen {
				return errors.New("key length error")
			}
			keyStreamID := int64(binary.BigEndian.Uint64(key[offsetPrefixLen:]))
			if keyStreamID != int64(streamID) {
				return ErrNoFindOffset
			}
			return iter.Item().Value(func(val []byte) error {
				if len(val) != 8 {
					return errors.Errorf("badgerDB value error")
				}
				offset = int64(binary.BigEndian.Uint64(val))
				return nil
			})
		}
		return ErrNoFindOffset
	})
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func (index *SeriesStreamOffsetIndex) SetStreamTimestampOffset(offset SeriesStreamOffset, fn func(err error)) {
	index.batcher.Update(badgerbatcher.BadgerOP{
		Op: func(txn *badger.Txn) error {
			keyBuffer := make([]byte, 16+offsetPrefixLen)
			key := keyBuffer
			value := make([]byte, 8)

			n := copy(keyBuffer, STOffsetPrefix)
			keyBuffer = keyBuffer[n:]

			binary.BigEndian.PutUint64(keyBuffer, uint64(offset.StreamID))
			keyBuffer = keyBuffer[8:]
			binary.BigEndian.PutUint64(keyBuffer, uint64(offset.TimestampMS))

			binary.BigEndian.PutUint64(value, uint64(offset.Offset))
			if err := txn.Set(key, value); err != nil {
				return errors.WithStack(err)
			}
			return nil
		},
		Commit: fn,
	})
}
