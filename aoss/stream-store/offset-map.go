package streamstore

type OffsetMap interface {
	set(id StreamID, offset int64)
}
