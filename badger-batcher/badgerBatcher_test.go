package badgerbatcher

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/sirupsen/logrus"
)

func TestBadgerDBBatcher_Update(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})
	logrus.SetLevel(logrus.DebugLevel)
	db, err := badger.Open(badger.DefaultOptions(t.Name()))
	if err != nil {
		t.Fatalf(err.Error())
	}
	batcher := NewBadgerDBBatcher(context.Background(), 1024, db).Start()

	type args struct {
		Op BadgerOP
	}

	tests := []struct {
		name string
		args args
	}{
		{
			args: args{
				Op: BadgerOP{
					Op: func(txn *badger.Txn) error {
						return txn.Set([]byte("1"), []byte("1"))
					},
					Commit: func(err error) {
						if err != nil {
							t.Fatalf(err.Error())
						}
					},
				},
			},
		},
		{
			args: args{
				Op: BadgerOP{
					Op: func(txn *badger.Txn) error {
						return txn.Set([]byte("2"), []byte("1"))
					},
					Commit: func(err error) {
						if err != nil {
							t.Fatalf(err.Error())
						}
					},
				},
			},
		},
		{
			args: args{
				Op: BadgerOP{
					Op: func(txn *badger.Txn) error {
						return txn.Set([]byte("2"), []byte("1"))
					},
					Commit: func(err error) {
						if err != nil {
							t.Fatalf(err.Error())
						}
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg sync.WaitGroup
			wg.Add(1)
			tt.args.Op.Commit = func(err error) {
				wg.Done()
			}
			batcher.Update(tt.args.Op)
			wg.Wait()
		})
	}

}
