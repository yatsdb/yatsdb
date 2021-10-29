package streamstore

type GetStreamOffset interface {
	//return stream range [from ,to)
	Offset(streamID StreamID) (StreamOffset, bool)
}

//StreamOffset
type StreamOffset struct {
	StreamID StreamID
	From     int64
	To       int64
}

func SearchSegments(segment []Segment, streamID StreamID, offset int64) int {
	for i := 0; i < len(segment); i++ {
		soffset, ok := segment[i].Offset(streamID)
		if !ok {
			continue
		}
		if soffset.From <= offset && offset < soffset.To {
			return i
		}
	}
	return -1
}

func SearchMTables1(mtables []MTable, streamID StreamID, offset int64) int {
	for i := 0; i < len(mtables); i++ {
		soffset, ok := mtables[i].Offset(streamID)
		if !ok {
			continue
		}
		if soffset.From <= offset && offset < soffset.To {
			return i
		}
	}
	return -1
}
func SearchMTables2(mtables []MTable, streamID StreamID, offset int64) int {
	for i := 0; i < len(mtables); i++ {
		soffset, ok := mtables[i].Offset(streamID)
		if !ok {
			continue
		}
		if offset < soffset.To {
			return i
		}
	}
	return -1
}
