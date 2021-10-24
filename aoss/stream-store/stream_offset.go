package streamstore

type GetStreamOffset interface {
	//return stream range [from ,to)
	Offset(streamID StreamID) (from int64, to int64, ok bool)
}

func SearchSegments(segment []Segment, streamID StreamID, offset int64) int {
	for i := 0; i < len(segment); i++ {
		_, end, ok := segment[i].Offset(streamID)
		if !ok {
			continue
		}
		if offset < end {
			return i
		}
	}
	return -1
}

func SearchMTables(mtables []MTable, streamID StreamID, offset int64) int {
	for i := 0; i < len(mtables); i++ {
		_, end, ok := mtables[i].Offset(streamID)
		if !ok {
			continue
		}
		if offset < end {
			return i
		}
	}
	return -1
}
