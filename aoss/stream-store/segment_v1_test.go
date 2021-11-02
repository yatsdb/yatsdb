package streamstore

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"io"
	"io/ioutil"
	mrand "math/rand"
	"os"
	"reflect"
	"strconv"
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

func Test_mergeSSOffsets(t *testing.T) {
	type args struct {
		offsetss [][]StreamSegmentOffset
	}
	tests := []struct {
		name string
		args args
		want []StreamSegmentOffset
	}{
		{
			name: "",
			args: args{
				offsetss: [][]StreamSegmentOffset{
					{
						StreamSegmentOffset{
							StreamID: 1,
							From:     0,
							To:       10,
							Offset:   0,
						},
						StreamSegmentOffset{
							StreamID: 2,
							From:     0,
							To:       20,
							Offset:   0,
						},
						StreamSegmentOffset{
							StreamID: 3,
							From:     0,
							To:       50,
							Offset:   0,
						},
					},
					{
						StreamSegmentOffset{
							StreamID: 1,
							From:     10,
							To:       20,
							Offset:   0,
						},
						StreamSegmentOffset{
							StreamID: 2,
							From:     20,
							To:       40,
							Offset:   0,
						},
						StreamSegmentOffset{
							StreamID: 4,
							From:     0,
							To:       50,
							Offset:   0,
						},
					},
					{
						StreamSegmentOffset{
							StreamID: 1,
							From:     20,
							To:       50,
							Offset:   0,
						},
						StreamSegmentOffset{
							StreamID: 2,
							From:     40,
							To:       50,
							Offset:   0,
						},
						StreamSegmentOffset{
							StreamID: 5,
							From:     0,
							To:       50,
							Offset:   0,
						},
					},
				},
			},
			want: []StreamSegmentOffset{
				{
					StreamID: 1,
					From:     0,
					To:       50,
					Offset:   0,
				},
				{
					StreamID: 2,
					From:     0,
					To:       50,
					Offset:   0,
				},
				{
					StreamID: 3,
					From:     0,
					To:       50,
					Offset:   0,
				},
				{
					StreamID: 4,
					From:     0,
					To:       50,
					Offset:   0,
				},
				{
					StreamID: 5,
					From:     0,
					To:       50,
					Offset:   0,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mergeSSOffsets(tt.args.offsetss...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mergeSSOffsets() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMergeSegments(t *testing.T) {

	_ = os.MkdirAll(t.Name(), 0777)
	t.Cleanup(func() {
		_ = os.RemoveAll(t.Name())
	})

	omap := newOffsetMap()
	var sCount = 33
	var entryID = uint64(1)
	firstEntryID := entryID

	lastEntryID := entryID
	var segments []*SegmentV1
	for i := 0; i < 33; i++ {
		table := newMTable(omap)
		for streamID := 0; streamID < sCount; streamID++ {
			if mrand.Intn(sCount) > sCount/2 {
				size := mrand.Intn(1280)
				buf := make([]byte, size)
				_, err := rand.Read(buf)
				assert.NoError(t, err)
				offset := table.Write(streamstorepb.Entry{
					StreamId: invertedindex.StreamID(streamID),
					Data:     buf,
					ID:       uint64(entryID),
				})
				omap.set(invertedindex.StreamID(streamID), offset)
				lastEntryID = entryID
				entryID++
			}
		}
		filename := t.Name() + "/" + strconv.Itoa(i) + ".segment"
		f, err := os.Create(filename)
		if err != nil {
			t.Fatal(err.Error())
		}
		assert.NoError(t, table.WriteToSegment(f))
		f.Close()
		s, err := openSegmentV1(filename)
		if err != nil {
			t.Fatal(err.Error())
		}
		segments = append(segments, s)
	}

	filename := t.Name() + "/merged.segment"
	f, err := os.Create(filename)
	if err != nil {
		t.Fatal(err.Error())
	}
	assert.NoError(t, MergeSegments(f, segments...))
	assert.NoError(t, f.Close())

	mergeSegment, err := openSegmentV1(filename)
	assert.NoError(t, err)

	assert.Equal(t, mergeSegment.FirstEntryID(), uint64(firstEntryID))
	assert.Equal(t, mergeSegment.LastEntryID(), uint64(lastEntryID))
	assert.Equal(t, mergeSegment.CreateTS(), segments[len(segments)-1].CreateTS())

	for i := 0; i < sCount; i++ {
		t.Run("check_stream_data_"+strconv.Itoa(i), func(t *testing.T) {
			hash := md5.New()
			var size int
			for _, segment := range segments {
				reader, err := segment.NewReader(invertedindex.StreamID(i))
				if err != nil {
					continue
				}
				data, err := ioutil.ReadAll(reader)
				assert.NoError(t, err)
				assert.NoError(t, reader.Close())
				size += len(data)
				assert.NoError(t, err)
				hash.Write(data)
			}
			if size == 0 {
				return
			}
			md5Sum := hex.EncodeToString(hash.Sum(nil))

			offset, ok := mergeSegment.offset(invertedindex.StreamID(i))
			assert.True(t, ok)
			assert.True(t, offset.To-offset.From == int64(size))

			reader, err := mergeSegment.NewReader(invertedindex.StreamID(i))
			assert.NoError(t, err)

			data, err := ioutil.ReadAll(reader)
			assert.NoError(t, reader.Close())
			assert.NoError(t, err)
			hash = md5.New()
			hash.Write(data)
			assert.Equal(t, md5Sum, hex.EncodeToString(hash.Sum(nil)))
		})
	}
	for _, segment := range segments {
		segment.SetDeleteOnClose(true)
		assert.NoError(t, segment.Close())
	}
}
