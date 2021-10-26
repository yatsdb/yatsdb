package wal

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
	"gopkg.in/stretchr/testify.v1/assert"
)

func Test_decoder_Decode(t *testing.T) {
	var buffer = bytes.NewBuffer(nil)
	encoder := newEncoder(buffer)

	assert.NoError(t, encoder.Encode(&streamstorepb.EntryBatch{
		Entries: []streamstorepb.Entry{
			{
				StreamId: 1,
				Data:     []byte("hello1"),
				ID:       1,
			},
			{
				StreamId: 2,
				Data:     []byte("hello2"),
				ID:       2,
			},
			{
				StreamId: 3,
				Data:     []byte("hello3"),
				ID:       3,
			},
		},
	}))

	assert.NoError(t, encoder.Encode(&streamstorepb.EntryBatch{
		Entries: []streamstorepb.Entry{
			{
				StreamId: 4,
				Data:     []byte("hello4"),
				ID:       4,
			},
			{
				StreamId: 5,
				Data:     []byte("hello5"),
				ID:       5,
			},
			{
				StreamId: 6,
				Data:     []byte("hello6"),
				ID:       6,
			},
		},
	}))

	type fields struct {
		Reader io.Reader
	}
	tests := []struct {
		name    string
		fields  fields
		want    streamstorepb.EntryTyper
		wantErr bool
	}{
		{
			name: "",
			fields: fields{
				Reader: buffer,
			},
			want: &streamstorepb.EntryBatch{
				Entries: []streamstorepb.Entry{
					{
						StreamId: 1,
						Data:     []byte("hello1"),
						ID:       1,
					},
					{
						StreamId: 2,
						Data:     []byte("hello2"),
						ID:       2,
					},
					{
						StreamId: 3,
						Data:     []byte("hello3"),
						ID:       3,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "",
			fields: fields{
				Reader: buffer,
			},
			want: &streamstorepb.EntryBatch{
				Entries: []streamstorepb.Entry{
					{
						StreamId: 4,
						Data:     []byte("hello4"),
						ID:       4,
					},
					{
						StreamId: 5,
						Data:     []byte("hello5"),
						ID:       5,
					},
					{
						StreamId: 6,
						Data:     []byte("hello6"),
						ID:       6,
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &decoder{
				Reader: tt.fields.Reader,
			}
			got, err := d.Decode()
			if (err != nil) != tt.wantErr {
				t.Errorf("decoder.Decode() error = %+v, wantErr %+v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("decoder.Decode() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func Test_entryIteractor_Next(t *testing.T) {

	var buffer = bytes.NewBuffer(nil)
	encoder := newEncoder(buffer)

	assert.NoError(t, encoder.Encode(&streamstorepb.EntryBatch{
		Entries: []streamstorepb.Entry{
			{
				StreamId: 1,
				Data:     []byte("hello1"),
				ID:       1,
			},
			{
				StreamId: 2,
				Data:     []byte("hello2"),
				ID:       2,
			},
			{
				StreamId: 3,
				Data:     []byte("hello3"),
				ID:       3,
			},
		},
	}))

	assert.NoError(t, encoder.Encode(&streamstorepb.EntryBatch{
		Entries: []streamstorepb.Entry{
			{
				StreamId: 4,
				Data:     []byte("hello4"),
				ID:       4,
			},
			{
				StreamId: 5,
				Data:     []byte("hello5"),
				ID:       5,
			},
			{
				StreamId: 6,
				Data:     []byte("hello6"),
				ID:       6,
			},
		},
	}))

	iter := &entryIteractor{Decoder: newDecoder(buffer)}

	tests := []struct {
		name    string
		want    streamstorepb.Entry
		wantErr bool
	}{

		{
			want: streamstorepb.Entry{
				StreamId: 1,
				Data:     []byte("hello1"),
				ID:       1,
			},
			wantErr: false,
		},
		{
			want: streamstorepb.Entry{
				StreamId: 2,
				Data:     []byte("hello2"),
				ID:       2,
			},
			wantErr: false,
		},
		{
			want: streamstorepb.Entry{
				StreamId: 3,
				Data:     []byte("hello3"),
				ID:       3,
			},
			wantErr: false,
		},
		{
			want: streamstorepb.Entry{
				StreamId: 4,
				Data:     []byte("hello4"),
				ID:       4,
			},
			wantErr: false,
		},
		{
			want: streamstorepb.Entry{
				StreamId: 5,
				Data:     []byte("hello5"),
				ID:       5,
			},
			wantErr: false,
		},
		{
			want: streamstorepb.Entry{
				StreamId: 6,
				Data:     []byte("hello6"),
				ID:       6,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := iter.Next()
			if (err != nil) != tt.wantErr {
				t.Errorf("entryIteractor.Next() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("entryIteractor.Next() = %v, want %v", JS(got), JS(tt.want))
			}
		})
	}
}
