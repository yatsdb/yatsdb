package ssoffsetindex

import (
	"errors"

	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
)

type StreamID = invertedindex.StreamID

var ErrNoFindOffset = errors.New("streamID offset no find ")

type StreamTimestampOffsetGetter interface {
	//LE less or equal
	GetStreamTimestampOffset(streamID StreamID, timestampMS int64, LE bool) (int64, error)
}
type OffsetIndexUpdater interface {
	SetStreamTimestampOffset(offset SeriesStreamOffset, callback func(err error))
}

//SeriesStreamOffset
type SeriesStreamOffset struct {
	//metrics stream ID
	StreamID StreamID
	//TimestampMS time series samples timestamp
	TimestampMS int64
	//Offset stream offset
	Offset int64
}

type OffsetIndexDB interface {
	StreamTimestampOffsetGetter
	OffsetIndexUpdater
}
