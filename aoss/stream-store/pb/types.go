package streamstorepb

import "fmt"

type Type = byte

const (
	EntryType Type = 1
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

func NewEntryTyper(typ Type) EntryTyper {
	switch typ {
	case EntryType:
		return new(Entry)
	default:
		panic(fmt.Sprintf("unknown type %d", typ))
	}
}
