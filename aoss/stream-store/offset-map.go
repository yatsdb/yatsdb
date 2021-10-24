package streamstore

type OffsetMap interface {
	set(id StreamID, offset int64)
	get(id StreamID) (offset int64, ok bool)
}
