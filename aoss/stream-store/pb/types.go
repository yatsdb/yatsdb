package streamstorepb

import "fmt"

type EntryType = byte

const (
	EntryBatchType EntryType = 1
)

type EntryTyper interface {
	Type() EntryType
	MarshalTo(buffer []byte) (int, error)
	Unmarshal(data []byte) error
	Size() int
}

func (*EntryBatch) Type() EntryType {
	return EntryBatchType
}

func NewEntryTyper(entryType EntryType) EntryTyper {
	switch entryType {
	case EntryBatchType:
		return new(EntryBatch)
	default:
		panic(fmt.Sprintf("unknown type %d", entryType))
	}
}
