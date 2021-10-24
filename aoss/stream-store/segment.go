package streamstore

import "io"

type Segment interface {
	GetStreamOffset

	FirstEntryID() uint64
	LastEntryID() uint64
}

func newSegment(rsc io.ReadSeekCloser) Segment {
	panic("not implement")
}
