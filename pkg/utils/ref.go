package utils

import (
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

type Ref struct {
	fn func()
	n  int32
}

func NewRef(fn func()) *Ref {
	return &Ref{
		fn: fn,
		n:  1,
	}
}

func (ref *Ref) Inc() bool {
	for {
		n := atomic.LoadInt32(&ref.n)
		if n < 0 {
			logrus.WithFields(logrus.Fields{
				"ref": n,
			}).Panic("ref error")
		}
		if n == 0 {
			return false
		}
		if atomic.CompareAndSwapInt32(&ref.n, n, n+1) {
			return true
		}
	}
}
func (ref *Ref) DecRef() {
	for {
		n := atomic.LoadInt32(&ref.n)
		if n <= 0 {
			logrus.WithFields(logrus.Fields{
				"ref.n": n,
			}).Panic("ref error")
		}
		if !atomic.CompareAndSwapInt32(&ref.n, n, n-1) {
			continue
		}
		if n-1 == 0 {
			if ref.fn != nil {
				ref.fn()
			}
		}
		break
	}
}
