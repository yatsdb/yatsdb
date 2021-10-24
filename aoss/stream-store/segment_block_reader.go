package streamstore

var _ streamBlockReader = (*segmentBlockReader)(nil)

type segmentBlockReader struct {
	segment  Segment
	streamID StreamID
}

func newSegmentBlockReader(streamID StreamID, segment Segment) *segmentBlockReader {
	return &segmentBlockReader{
		segment:  segment,
		streamID: streamID,
	}
}

func (reader *segmentBlockReader) Close() error {
	panic("not implement")
}
func (reader *segmentBlockReader) Offset() (begin int64, end int64) {
	panic("not implement")
}

func (reader *segmentBlockReader) Seek(offset int64, whence int) (int64, error) {
	panic("not implement")
}

func (reader *segmentBlockReader) Read(p []byte) (n int, err error) {
	panic("not implement")
}
