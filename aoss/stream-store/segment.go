package streamstore

import "io"

type Segment interface {
}

func newSegment(rsc io.ReadSeekCloser) Segment {
	panic("not implement")
}
