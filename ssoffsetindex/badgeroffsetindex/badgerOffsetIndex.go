package badgeroffsetindex

import (
	"context"
	"encoding/binary"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	badgerbatcher "github.com/yatsdb/yatsdb/badger-batcher"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
	"github.com/yatsdb/yatsdb/pkg/metrics"
	. "github.com/yatsdb/yatsdb/ssoffsetindex"
)

type SeriesStreamOffsetIndex struct {
	db      *badger.DB
	batcher *badgerbatcher.BadgerDBBatcher

	streamLastTSMapLocker sync.Mutex
	streamLastTSMap       map[StreamID]int64
}

const (
	STOffsetPrefix  = "0$"
	offsetPrefixLen = len(STOffsetPrefix)
)

func NewSeriesStreamOffsetIndex(db *badger.DB, batcher *badgerbatcher.BadgerDBBatcher) *SeriesStreamOffsetIndex {
	return &SeriesStreamOffsetIndex{
		db:                    db,
		batcher:               batcher,
		streamLastTSMapLocker: sync.Mutex{},
		streamLastTSMap:       map[invertedindex.StreamID]int64{},
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
		opts.PrefetchSize = 0
		opts.PrefetchValues = false

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
	var update = false
	index.streamLastTSMapLocker.Lock()
	if (offset.TimestampMS - index.streamLastTSMap[offset.StreamID]) > 1000*300 {
		index.streamLastTSMap[offset.StreamID] = offset.TimestampMS
		update = true
	}
	index.streamLastTSMapLocker.Unlock()
	if !update {
		fn(nil)
		return
	}

	metrics.UpdateOffsetIndexCount.Inc()

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
