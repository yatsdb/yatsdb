package streamstorepb

import "fmt"

type Type = byte

const (
	EntryType                 Type = 1
	StreamTimeStampOffsetType Type = 2
)

type EntryTyper interface {
	Type() Type
	GetID() uint64
	MarshalTo(buffer []byte) (int, error)
	Unmarshal(data []byte) error
	Size() int
}

func (*Entry) Type() Type {
	return EntryType
}

func (*StreamTimeStampOffset) Type() Type {
	return StreamTimeStampOffsetType
}

func NewEntryTyper(typ Type) EntryTyper {
	switch typ {
	case EntryType:
		return new(Entry)
	case StreamTimeStampOffsetType:
		return new(StreamTimeStampOffset)
	default:
		panic(fmt.Sprintf("unknown type %d", typ))
	}
}
