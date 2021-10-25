package streamstore

import (
	"os"
	"time"
)

type Segment interface {
	GetStreamOffset
	FirstEntryID() uint64
	LastEntryID() uint64
	CreateTS() time.Time
	GetStreamOffsets() []StreamOffset
}

func newSegment(f *os.File) Segment {
	panic("not implement")
}
