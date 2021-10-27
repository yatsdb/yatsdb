package streamstore

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
	"gopkg.in/stretchr/testify.v1/assert"
)

func TestChunks_Write(t *testing.T) {
	blockSize = 128
	b := Chunks{}
	type args struct {
		data []byte
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "",
			args: args{
				data: make([]byte, 64),
			},
			want: 64,
		},
		{
			name: "",
			args: args{
				data: make([]byte, 64),
			},
			want: 128,
		},
		{
			name: "",
			args: args{
				data: make([]byte, 64*1024),
			},
			want: 128 + 1024*64,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := b.Write(tt.args.data); got != tt.want {
				t.Errorf("Chunks.Write() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChunks_WriteTo(t *testing.T) {
	blockSize = 11
	rand.Seed(time.Now().Unix())
	b := &Chunks{}
	buf := make([]byte, 123456)
	_, err := rand.Read(buf)
	assert.NoError(t, err)
	assert.True(t, b.Write(buf) == 123456)

	tests := []struct {
		name    string
		wantN   int64
		wantW   string
		wantErr bool
	}{
		{
			name:    "",
			wantN:   123456,
			wantW:   string(buf),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			gotN, err := b.WriteTo(w)
			if (err != nil) != tt.wantErr {
				t.Errorf("Chunks.WriteTo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotN != tt.wantN {
				t.Errorf("Chunks.WriteTo() = %v, want %v", gotN, tt.wantN)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("Chunks.WriteTo() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func TestChunks_ReadAt(t *testing.T) {
	blockSize = 11

	b := &Chunks{}

	rand.Seed(time.Now().Unix())
	buf := make([]byte, 123456)
	_, err := rand.Read(buf)
	assert.NoError(t, err)
	assert.True(t, b.Write(buf) == 123456)

	type args struct {
		p      []byte
		offset int64
	}
	tests := []struct {
		name    string
		args    args
		wantN   int
		wantErr bool
	}{
		{
			name: "",
			args: args{
				p:      make([]byte, 123456),
				offset: 0,
			},
			wantN:   123456,
			wantErr: false,
		},
		{
			name: "",
			args: args{
				p:      make([]byte, 12345),
				offset: 0,
			},
			wantN:   12345,
			wantErr: false,
		},
		{
			name: "",
			args: args{
				p:      make([]byte, 123456),
				offset: 1000,
			},
			wantN:   122456,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotN, err := b.ReadAt(tt.args.p, tt.args.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("Chunks.ReadAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotN != tt.wantN {
				t.Errorf("Chunks.ReadAt() = %v, want %v", gotN, tt.wantN)
			}
		})
	}

	reader := io.NewSectionReader(b, 0, 123456)

	all, err := ioutil.ReadAll(reader)
	assert.NoError(t, err)
	assert.True(t, bytes.Compare(all, buf) == 0)

	b = &Chunks{}

	b.From = 12345
	b.To = 12345

	assert.True(t, b.Write(buf) == int64(123456+12345))

	_, err = b.ReadAt(make([]byte, 0), 1)
	assert.Equal(t, err, ErrOutOfOffsetRangeBegin)

	_, err = b.ReadAt(make([]byte, 1), 123456+12345)
	assert.Equal(t, err, io.EOF)

	n := b.Write([]byte("helloworld"))
	assert.True(t, n == int64(123456+12345+10))

	buf = make([]byte, 10)
	ret, err := b.ReadAt(buf, 123456+12345)
	assert.NoError(t, err)
	assert.True(t, ret == 10)
	assert.True(t, string(buf) == "helloworld")
}

func Test_newMTable(t *testing.T) {
	type args struct {
		omap OffsetMap
	}
	omap := newOffsetMap()
	tests := []struct {
		name string
		args args
		want MTable
	}{
		{
			name: "",
			args: args{
				omap: omap,
			},
			want: &mtable{
				size:         0,
				omap:         omap,
				fristEntryID: 0,
				lastEntryID:  0,
				chunksMap:    map[invertedindex.StreamID]*Chunks{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotNil(t, newMTable(tt.args.omap))
		})
	}
}

func Test_mtable_Write(t *testing.T) {
	blockSize = 11
	type args struct {
		entry streamstorepb.Entry
	}
	omap := newOffsetMap()
	omap.set(1, 128)
	m := newMTable(omap)

	tests := []struct {
		name       string
		args       args
		wantOffset int64
	}{
		{
			name: "",
			args: args{
				entry: streamstorepb.Entry{
					StreamId: 1,
					Data:     make([]byte, 128),
					ID:       1,
				},
			},
			wantOffset: 256,
		},
		{
			name: "",
			args: args{
				entry: streamstorepb.Entry{
					StreamId: 1,
					Data:     make([]byte, 128),
					ID:       2,
				},
			},
			wantOffset: 256 + 128,
		},
		{
			name: "",
			args: args{
				entry: streamstorepb.Entry{
					StreamId: 1,
					Data:     make([]byte, 128),
					ID:       99999,
				},
			},
			wantOffset: 512,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotOffset := m.Write(tt.args.entry); gotOffset != tt.wantOffset {
				t.Errorf("mtable.Write() = %v, want %v", gotOffset, tt.wantOffset)
			}
		})
	}

	size := m.Size()
	assert.Equal(t, 128*3, size)

	assert.Equal(t, m.FirstEntryID(), uint64(1))
	assert.Equal(t, m.LastEntryID(), uint64(99999))

	offset, ok := m.Offset(1)
	assert.True(t, ok)

	assert.Equal(t, offset, StreamOffset{
		StreamID: 1,
		From:     128,
		To:       512,
	})

	reader, err := m.NewReader(1)
	assert.NoError(t, err)

	n, err := reader.Read(make([]byte, 1024))
	assert.NoError(t, err)
	assert.Equal(t, n, 128*3)

	ret, err := reader.Seek(0, 1)
	assert.NoError(t, err)

	assert.Equal(t, int64(512), ret)

	_, err = reader.Read(make([]byte, 10))
	assert.Equal(t, err, io.EOF)

}

func Test_mtable_WriteSegment(t *testing.T) {
	rand.Seed(time.Now().Unix())
	blockSize = 321
	omap := newOffsetMap()
	table := newMTable(omap)
	data := make([]byte, 11111)
	_, err := rand.Read(data)
	assert.NoError(t, err)
	var lastEntryID = rand.Uint64()
	var firstEID = lastEntryID

	var sCount = 2221

	for i := 0; i < sCount; i++ {
		omap.set(StreamID(i), int64(i))
		wBuf := data
		for len(wBuf) > 0 {
			n := rand.Intn(len(wBuf) + 1)
			buf := wBuf[:n]
			wBuf = wBuf[n:]
			table.Write(streamstorepb.Entry{
				StreamId: StreamID(i),
				Data:     buf,
				ID:       uint64(lastEntryID),
			})
			lastEntryID++
		}
		offset, ok := table.Offset(StreamID(i))
		assert.True(t, ok)
		assert.Equal(t, offset.From, int64(i))
		assert.Equal(t, offset.To, int64(i+len(data)))
	}

	assert.Equal(t, table.FirstEntryID(), uint64(firstEID))
	assert.Equal(t, table.LastEntryID(), lastEntryID-1)

	f, err := os.CreateTemp("", "segment")
	t.Cleanup(func() {
		f.Close()
		os.Remove(f.Name())
	})

	assert.NoError(t, table.WriteToSegment(f))
	assert.NoError(t, f.Sync())

	sf, err := os.Open(f.Name())
	assert.NoError(t, err)
	assert.NotNil(t, sf)

	segment, err := newSegment(sf)
	assert.NoError(t, err)

	assert.True(t, time.Now().After(segment.CreateTS()), time.Now().Sub(segment.CreateTS()) < time.Second)

	assert.Equal(t, segment.FirstEntryID(), table.FirstEntryID())
	assert.Equal(t, segment.LastEntryID(), table.LastEntryID())

	assert.True(t, segment.Size() > int64(table.Size()))

	for _, offset := range segment.GetStreamOffsets() {
		assert.Equal(t, int(offset.From), int(offset.StreamID))
		assert.Equal(t, int(offset.To), int(offset.StreamID)+len(data))

		reader, err := segment.NewReader(offset.StreamID)
		assert.NoError(t, err)

		buffer := make([]byte, len(data))
		_, err = io.ReadFull(reader, buffer)
		assert.NoError(t, err)

		assert.True(t, bytes.Compare(buffer, data) == 0)

		_, err = reader.Read(make([]byte, 1))
		assert.Equal(t, err, io.EOF)

		randOffset := rand.Intn(len(data))

		seekOffset := offset.From + int64(randOffset)
		n, err := reader.Seek(seekOffset, 0)
		assert.NoError(t, err)
		assert.Equal(t, seekOffset, n)

		all, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, len(all), len(data)-randOffset)
		assert.True(t, bytes.Compare(all, data[randOffset:]) == 0)
	}
	assert.Equal(t, len(segment.GetStreamOffsets()), sCount)

	assert.NoError(t, err)
	assert.NotNil(t, f)
}
