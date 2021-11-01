package wal

import (
	"os"
	"sync"
	"testing"

	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
	"gopkg.in/stretchr/testify.v1/assert"
)

type TestEntry struct {
	streamstorepb.Entry
	fn func(ID uint64, err error)
}

func (entry *TestEntry) SetID(ID uint64) {
	entry.ID = ID
}
func (entry *TestEntry) Fn(ID uint64, err error) {
	entry.fn(ID, err)
}

func Test_wal(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})
	assert.NoError(t, os.MkdirAll(t.Name(), 0777))
	opts := DefaultOption(t.Name())
	opts.MaxLogSize = 10
	opts.BatchSize = 10
	opts.SyncBatchSize = 10
	wal, err := Reload(opts, func(e streamstorepb.EntryTyper) error {
		return nil
	})
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		nextID := uint64(i + 1)
		wg.Add(1)
		wal.Write(&TestEntry{
			Entry: streamstorepb.Entry{
				Data: []byte("hello"),
			},
			fn: func(ID uint64, err error) {
				assert.Exactly(t, ID, nextID)
				assert.NoError(t, err)
				wg.Done()
			}})
	}

	wg.Wait()
	assert.NoError(t, wal.Close())

	var nextID = uint64(1)
	wal, err = Reload(opts, func(typ streamstorepb.EntryTyper) error {
		e := typ.(*streamstorepb.Entry)
		assert.Equal(t, nextID, e.GetID())
		assert.Equal(t, e.StreamId, streamstorepb.StreamID(0))
		assert.Equal(t, e.Data, []byte("hello"))
		nextID++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, nextID, uint64(1001))

	wg = sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		nextID := uint64(i + 1001)
		wg.Add(1)
		wal.Write(&TestEntry{
			streamstorepb.Entry{Data: []byte("hello")}, func(ID uint64, err error) {
				assert.Exactly(t, ID, nextID)
				assert.NoError(t, err)
				wg.Done()
			},
		})
	}
	wg.Wait()
	assert.NoError(t, wal.Close())

	//reload wal
	nextID = uint64(1)
	wal, err = Reload(opts, func(typ streamstorepb.EntryTyper) error {
		e := typ.(*streamstorepb.Entry)
		assert.Equal(t, nextID, e.ID)
		assert.Equal(t, e.StreamId, streamstorepb.StreamID(0))
		assert.Equal(t, e.Data, []byte("hello"))
		nextID++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, nextID, uint64(2001))

	wg = sync.WaitGroup{}
	wg.Add(1)
	wal.Write(&TestEntry{
		streamstorepb.Entry{Data: []byte("hello")},
		func(ID uint64, err error) {
			assert.Exactly(t, ID, nextID)
			assert.NoError(t, err)
			wg.Done()
		},
	})
	wal.ClearLogFiles(1000)
	nextID = wal.logFiles[0].GetFirstEntryID()
	assert.NoError(t, wal.Close())

	wal, err = Reload(opts, func(typ streamstorepb.EntryTyper) error {
		e := typ.(*streamstorepb.Entry)
		assert.Equal(t, nextID, e.ID)
		assert.Equal(t, e.StreamId, streamstorepb.StreamID(0))
		assert.Equal(t, e.Data, []byte("hello"))
		nextID++
		return nil
	})

	assert.NoError(t, wal.Close())

}
