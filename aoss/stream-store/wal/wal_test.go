package wal

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
	"gopkg.in/stretchr/testify.v1/assert"
)

func Test_wal_Write(t *testing.T) {
	t.Cleanup(func() {
		//os.RemoveAll(t.Name())
	})
	assert.NoError(t, os.MkdirAll(t.Name(), 0777))
	opts := DefaultOption(t.Name())
	opts.MaxLogSize = 10
	wal, err := Reload(context.Background(), opts, func(e streamstorepb.Entry) error {
		fmt.Println("reload", e.ID)
		return nil
	})
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	wal.Write(streamstorepb.Entry{}, func(ID uint64, err error) {
		fmt.Println("write callback", ID)
		assert.Exactly(t, ID, uint64(5))
		assert.NoError(t, err)
		wg.Done()
	})

	wg.Wait()
}
