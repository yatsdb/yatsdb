package streamstore

import (
	"crypto/rand"
	"io"
	"io/ioutil"
	"os"
	"testing"

	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
	"gopkg.in/stretchr/testify.v1/assert"
)

func TestWriteSegmentV1(t *testing.T) {
	table := newMTable(newOffsetMap())

	var sCount = 10000
	data := make([]byte, 1024)
	_, err := rand.Read(data)
	assert.NoError(t, err)

	var entryID uint64 = 1010120
	firstEntryID := entryID
	lastEntryID := entryID
	for i := 0; i < sCount; i++ {
		table.Write(streamstorepb.Entry{
			StreamId: invertedindex.StreamID(i),
			Data:     data,
			ID:       entryID,
		})
		lastEntryID = entryID
		entryID++
	}
	filename := t.Name() + "/segment.seg"
	os.MkdirAll(t.Name(), 0777)
	f, err := os.Create(filename)
	assert.NoError(t, err)
	err = table.WriteToSegment(f)
	assert.NoError(t, err)
	assert.NoError(t, f.Close())

	segment, err := openSegmentV1(filename)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, segment.Filename(), filename)
	stat, err := os.Stat(filename)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assert.Equal(t, segment.Size(), stat.Size())

	assert.Equal(t, segment.FirstEntryID(), firstEntryID)
	assert.Equal(t, segment.LastEntryID(), lastEntryID)
	assert.Equal(t, len(segment.GetStreamOffsets()), sCount)

	for i := 0; i < sCount; i++ {
		reader, err := segment.NewReader(invertedindex.StreamID(i))
		assert.NoError(t, err)

		buf := make([]byte, len(data))
		n, err := reader.Read(buf)
		assert.NoError(t, err)
		assert.Equal(t, buf, data)
		assert.Equal(t, len(data), int(n))

		_, err = reader.Read(make([]byte, 0))
		assert.Equal(t, io.EOF, err)
		assert.NoError(t, reader.Close())

		//test ioutil.ReadAll()
		reader, err = segment.NewReader(invertedindex.StreamID(i))
		buf, err = ioutil.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, data, buf)

		_, err = reader.Read(make([]byte, 0))
		assert.Equal(t, io.EOF, err)
		assert.NoError(t, reader.Close())

	}
	assert.NoError(t, segment.Close())
	assert.False(t, segment.ref.Inc())

}
