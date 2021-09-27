package yatsdb

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/sirupsen/logrus"
)

type BadgerOP struct {
	Op     func(txn *badger.Txn) error
	Commit func(err error)
}
type BadgerDBBatcher struct {
	batchSize int
	db        *badger.DB
	opCh      chan BadgerOP
}

func (batcher *BadgerDBBatcher) Update(Op BadgerOP) {
	batcher.opCh <- Op
}
func (batcher *BadgerDBBatcher) Start() {
	go func() {
		for {
			ops := append([]BadgerOP{}, <-batcher.opCh)
			for {
				select {
				case op := <-batcher.opCh:
					ops = append([]BadgerOP{}, op)
					if len(ops) < batcher.batchSize {
						continue
					}
				default:
				}
				break
			}
			var toCommits = make([]func(err error), 0, len(ops))
			if err := batcher.db.Update(func(txn *badger.Txn) error {
				for _, op := range ops {
					if err := op.Op(txn); err != nil {
						op.Commit(err)
					} else {
						toCommits = append(toCommits, op.Commit)
					}
				}
				return nil
			}); err != nil {
				logrus.Errorf("badger commit failed %+v", err)
				for _, commit := range toCommits {
					commit(err)
				}
				continue
			}
			for _, commit := range toCommits {
				commit(nil)
			}
		}
	}()
}
