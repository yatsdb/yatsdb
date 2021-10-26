package streamstorepb

import invertedindex "github.com/yatsdb/yatsdb/inverted-Index"

type StreamID = invertedindex.StreamID

type StreamOffsetMap map[StreamID]StreamOffsetMap
