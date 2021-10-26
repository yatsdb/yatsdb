package streamstore

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"

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
