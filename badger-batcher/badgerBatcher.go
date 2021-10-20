package badgerbatcher

import (
	"context"

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
	ctx       context.Context
	opCh      chan BadgerOP
}

func NewBadgerDBBatcher(ctx context.Context, maxBatchSize int, db *badger.DB) *BadgerDBBatcher {
	if maxBatchSize == 0 {
		maxBatchSize = 32
	}
	return &BadgerDBBatcher{
		batchSize: maxBatchSize,
		db:        db,
		ctx:       ctx,
		opCh:      make(chan BadgerOP, maxBatchSize),
	}
}

func (batcher *BadgerDBBatcher) Update(Op BadgerOP) {
	logrus.Debugf("batcher Update")
	select {
	case batcher.opCh <- Op:
	case <-batcher.ctx.Done():
		logrus.Warnf("context cancel %s", batcher.ctx.Err())
		Op.Commit(batcher.ctx.Err())
	}
}
func (batcher *BadgerDBBatcher) Start() *BadgerDBBatcher {
	go func() {
		for {
			var ops []BadgerOP
			select {
			case <-batcher.ctx.Done():
				return
			case op := <-batcher.opCh:
				ops = append(ops, op)
				//batch update
				for {
					select {
					case op := <-batcher.opCh:
						ops = append(ops, op)
						if len(ops) < batcher.batchSize {
							continue
						}
					default:
					}
					break
				}
			}
			var toCommits = make([]func(err error), 0, len(ops))
			if err := batcher.db.Update(func(txn *badger.Txn) error {
				for _, op := range ops {
					if err := op.Op(txn); err != nil {
						logrus.Warnf("db update failed %+v", err)
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
				logrus.Debugf("batcher committed")
			}
		}
	}()
	return batcher
}
