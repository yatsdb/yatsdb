package streamstore

import (
	"sync"
)

type OffsetMap interface {
	set(id StreamID, offset int64)
	get(id StreamID) (offset int64, ok bool)
}

func newOffsetMap() OffsetMap {
	return &OffsetMapWithLocker{
		offsets: map[StreamID]int64{},
		locker:  sync.RWMutex{},
	}
}

type OffsetMapWithLocker struct {
	offsets map[StreamID]int64
	locker  sync.RWMutex
}

func (omap *OffsetMapWithLocker) set(id StreamID, offset int64) {
	omap.locker.Lock()
	omap.offsets[id] = offset
	omap.locker.Unlock()
}

func (omap *OffsetMapWithLocker) get(id StreamID) (offset int64, ok bool) {
	omap.locker.RLock()
	offset, ok = omap.offsets[id]
	omap.locker.RUnlock()
	return
}
