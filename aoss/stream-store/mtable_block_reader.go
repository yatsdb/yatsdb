package streamstore

var _ streamBlockReader = (*mtableBlockReader)(nil)

type mtableBlockReader struct {
	mtable   MTable
	streamID StreamID
}

func newMtableBlockReader(streamID StreamID, mtable MTable) *mtableBlockReader {
	return &mtableBlockReader{
		mtable:   mtable,
		streamID: streamID,
	}
}

func (reader *mtableBlockReader) Close() error {
	panic("not implement")
}
func (reader *mtableBlockReader) Offset() (begin int64, end int64) {
	panic("not implement")
}

func (reader *mtableBlockReader) Seek(offset int64, whence int) (int64, error) {
	panic("not implement")
}

func (reader *mtableBlockReader) Read(p []byte) (n int, err error) {
	panic("not implement")
}
